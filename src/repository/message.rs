use anyhow::Result;
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use tokio_postgres::Row;

use super::db::{col_opt_i32, col_opt_i64, col_opt_str, exec_drop, exec_rows};
use crate::utils::ulid::{bin_to_ulid, id_to_vec, ulid_to_vec};
use crate::{models::message::Message, proto::message::ConversationItem};

pub struct MessageRepository {
    pool: Pool,
}

impl MessageRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    // to_char → ISO8601, COALESCE untuk nullable timestamps
    fn msg_cols() -> &'static str {
        r#"m.id, m.sender_id, m.recipient_id, m.content, m.msg_type, m.order_id,
       m.media_url, m.media_mime, m.media_size, m.media_duration, m.media_thumb,
       m.sent_at, m.delivered_at, m.read_at,
       u.name AS sender_name, u.avatar_url AS sender_avatar,
       COALESCE(o.status, '') AS status_order"#
    }

    fn row_to_message(row: &Row) -> Result<Message> {
        let id_bytes: Vec<u8> = row.try_get("id")?;
        let sender_bytes: Vec<u8> = row.try_get("sender_id")?;
        let recipient_bytes: Vec<u8> = row.try_get("recipient_id")?;
        let order_bytes: Vec<u8> = row.try_get("order_id")?;

        Ok(Message {
            id: bin_to_ulid(id_bytes)?,
            sender_id: bin_to_ulid(sender_bytes)?,
            recipient_id: bin_to_ulid(recipient_bytes)?,
            order_id: bin_to_ulid(order_bytes)?,
            content: row.try_get("content")?,
            msg_type: row.try_get("msg_type")?,
            media_url: col_opt_str(row, "media_url")?,
            media_mime: col_opt_str(row, "media_mime")?,
            media_size: col_opt_i64(row, "media_size"),
            media_duration: col_opt_i32(row, "media_duration"),
            media_thumb: col_opt_str(row, "media_thumb")?,
            sent_at: row.try_get("sent_at")?,
            delivered_at: row.try_get("delivered_at")?,
            read_at: row.try_get("read_at")?,
            sender_name: col_opt_str(row, "sender_name")?,
            sender_avatar: col_opt_str(row, "sender_avatar")?,
            status_order: col_opt_str(row, "status_order")?,
        })
    }

    pub async fn save(&self, msg: &mut Message) -> Result<()> {
        let id_b = ulid_to_vec(&msg.id)?;
        let sender_b = id_to_vec(&msg.sender_id)?;
        let recipient_b = id_to_vec(&msg.recipient_id)?;
        let order_b = id_to_vec(&msg.order_id)?;

        // tokio-postgres tidak bisa bind &str ke TIMESTAMPTZ.
        // sent_at di-set DEFAULT (NOW()) sehingga tidak perlu di-bind.
        exec_drop(
            &self.pool,
            r#"INSERT INTO messages
               (id, sender_id, recipient_id, order_id, content, msg_type,
                media_url, media_mime, media_size, media_duration, media_thumb)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)"#,
            &[
                &id_b,
                &sender_b,
                &recipient_b,
                &order_b,
                &msg.content.as_str(),
                &msg.msg_type.as_str(),
                &msg.media_url.as_deref(),
                &msg.media_mime.as_deref(),
                &msg.media_size,
                &msg.media_duration,
                &msg.media_thumb.as_deref(),
            ],
        )
        .await
    }

    pub async fn get_conversations(&self, user_id: &str) -> Result<Vec<ConversationItem>> {
        let user_b = id_to_vec(user_id)?;

        // PostgreSQL CTE — WITH clause: DISTINCT ON lebih efisien dari ROW_NUMBER() di Postgres
        let q = r#"
        WITH conversation_latest AS (
        SELECT * FROM (
            SELECT
                CASE WHEN m.sender_id = $1 THEN m.recipient_id ELSE m.sender_id END AS peer_id,
                m.content,
                m.msg_type,
                m.sent_at,
                m.order_id,
                ROW_NUMBER() OVER (
                    PARTITION BY CASE WHEN m.sender_id = $1 THEN m.recipient_id ELSE m.sender_id END
                    ORDER BY m.sent_at DESC
                ) AS rn
            FROM messages m
            WHERE m.sender_id = $1 OR m.recipient_id = $1
        ) t WHERE rn = 1
    ),
    unread_counts AS (
        SELECT sender_id, COUNT(*) AS unread_count
        FROM messages
        WHERE recipient_id = $1 AND read_at IS NULL
        GROUP BY sender_id
    )
    SELECT
        cl.peer_id,
        COALESCE(u.name, '') AS peer_name,
        COALESCE(u.avatar_url, '') AS peer_avatar,
        COALESCE(cl.content, '') AS last_message,
        COALESCE(cl.msg_type, '') AS last_msg_type,
        cl.sent_at AS last_sent_at,
        cl.order_id,
        COALESCE(o.status, '') AS order_status,
        COALESCE(uc.unread_count, 0) AS unread_count
    FROM conversation_latest cl
    LEFT JOIN users u ON u.id = cl.peer_id
    LEFT JOIN orders o ON o.id = cl.order_id
    LEFT JOIN unread_counts uc ON uc.sender_id = cl.peer_id
    ORDER BY cl.sent_at DESC
        "#;

        let rows = exec_rows(&self.pool, q, &[&user_b]).await?;

        rows.iter()
            .map(|r| -> Result<ConversationItem> {
                let peer_bytes: Vec<u8> = r.try_get("peer_id")?;
                let order_bytes: Vec<u8> = r.try_get("order_id")?;
                let unread: i64 = r.try_get("unread_count")?;
                let last_sent_at: DateTime<Utc> = r.try_get("last_sent_at")?;
                Ok(ConversationItem {
                    peer_id: bin_to_ulid(peer_bytes)?,
                    peer_name: r.try_get("peer_name")?,
                    peer_avatar: r.try_get("peer_avatar")?,
                    last_message: r.try_get("last_message")?,
                    last_message_type: r.try_get("last_msg_type")?,
                    last_seen_at: last_sent_at.to_rfc3339(),
                    unread_count: unread as u32,
                    order_id: bin_to_ulid(order_bytes)?,
                    order_status: r.try_get("order_status")?,
                })
            })
            .collect()
    }

    pub async fn get_history(
        &self,
        user_id: &str,
        peer_id: &str,
        limit: i32,
        _before_id: Option<&str>,
    ) -> Result<Vec<Message>> {
        let user_b = id_to_vec(user_id)?;
        let peer_b = id_to_vec(peer_id)?;
        let limit = (limit as i64).clamp(1, 100);

        let q = format!(
            r#"(SELECT {cols}
            FROM messages m
            JOIN users u ON m.sender_id = u.id
            JOIN orders o ON m.order_id = o.id
            WHERE m.sender_id = $1 AND m.recipient_id = $2
            ORDER BY m.sent_at DESC
            LIMIT $3)
           UNION ALL
           (SELECT {cols}
            FROM messages m
            JOIN users u ON m.sender_id = u.id
            JOIN orders o ON m.order_id = o.id
            WHERE m.sender_id = $2 AND m.recipient_id = $1
            ORDER BY m.sent_at DESC
            LIMIT $3)
           ORDER BY sent_at DESC
           LIMIT $3"#,
            cols = Self::msg_cols()
        );

        let rows = exec_rows(&self.pool, &q, &[&user_b, &peer_b, &limit]).await?;
        let mut msgs = rows
            .iter()
            .map(Self::row_to_message)
            .collect::<Result<Vec<_>>>()?;
        msgs.reverse();
        Ok(msgs)
    }

    pub async fn mark_read(&self, reader_id: &str, sender_id: &str) -> Result<()> {
        let reader_b = id_to_vec(reader_id)?;
        let sender_b = id_to_vec(sender_id)?;
        exec_drop(
            &self.pool,
            "UPDATE messages SET read_at=NOW() WHERE recipient_id=$1 AND sender_id=$2 AND read_at IS NULL",
            &[&reader_b, &sender_b],
        )
        .await
    }

    pub async fn mark_delivered(&self, msg_id: &str) -> Result<()> {
        let msg_b = id_to_vec(msg_id)?;
        exec_drop(
            &self.pool,
            "UPDATE messages SET delivered_at=NOW() WHERE id=$1 AND delivered_at IS NULL",
            &[&msg_b],
        )
        .await
    }
}
