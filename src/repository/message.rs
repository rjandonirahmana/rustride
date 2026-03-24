use anyhow::Result;
use mysql_async::{from_value, prelude::Queryable, Pool, Row};

use super::db::{col, col_opt_i32, col_opt_i64, col_opt_str, exec_drop, exec_rows};
use crate::utils::ulid::{bin_to_ulid, ulid_to_bytes};
use crate::{models::message::Message, proto::message::ConversationItem};

pub struct MessageRepository {
    pool: Pool,
}

impl MessageRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn msg_cols() -> &'static str {
        r"m.id, m.sender_id, m.recipient_id, m.content, m.msg_type, m.order_id,
          m.media_url, m.media_mime, m.media_size, m.media_duration, m.media_thumb,
          IFNULL(DATE_FORMAT(m.sent_at,       '%Y-%m-%dT%H:%i:%sZ'), '0001-01-01T00:00:00Z') AS sent_at,
          IFNULL(DATE_FORMAT(m.delivered_at,  '%Y-%m-%dT%H:%i:%sZ'), '0001-01-01T00:00:00Z') AS delivered_at,
          IFNULL(DATE_FORMAT(m.read_at,       '%Y-%m-%dT%H:%i:%sZ'), '0001-01-01T00:00:00Z') AS read_at,
          u.name       AS sender_name,
          u.avatar_url AS sender_avatar,
          IFNULL(o.status, '') AS status_order"
    }

    fn row_to_message(row: &Row) -> Result<Message> {
        Ok(Message {
            id: bin_to_ulid(from_value(col(row, "id")?))?,
            sender_id: bin_to_ulid(from_value(col(row, "sender_id")?))?,
            recipient_id: bin_to_ulid(from_value(col(row, "recipient_id")?))?,
            order_id: bin_to_ulid(from_value(col(row, "order_id")?))?,
            content: from_value(col(row, "content")?),
            msg_type: from_value(col(row, "msg_type")?),
            media_url: col_opt_str(row, "media_url"),
            media_mime: col_opt_str(row, "media_mime"),
            media_size: col_opt_i64(row, "media_size"),
            media_duration: col_opt_i32(row, "media_duration"),
            media_thumb: col_opt_str(row, "media_thumb"),
            sent_at: from_value(col(row, "sent_at")?),
            delivered_at: col_opt_str(row, "delivered_at"),
            read_at: col_opt_str(row, "read_at"),
            sender_name: col_opt_str(row, "sender_name"),
            sender_avatar: col_opt_str(row, "sender_avatar"),
            status_order: col_opt_str(row, "status_order"),
        })
    }

    pub async fn save(&self, msg: &mut Message) -> Result<()> {
        let id_b = ulid_to_bytes(&msg.id)?;
        let sender_b = ulid_to_bytes(&msg.sender_id)?;
        let recipient_b = ulid_to_bytes(&msg.recipient_id)?;
        let order_b = ulid_to_bytes(&msg.order_id)?;

        exec_drop(
            &self.pool,
            r"INSERT INTO messages
              (id, sender_id, recipient_id, order_id, content, msg_type,
               media_url, media_mime, media_size, media_duration, media_thumb, sent_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                &id_b[..],
                &sender_b[..],
                &recipient_b[..],
                &order_b[..],
                &msg.content,
                &msg.msg_type,
                &msg.media_url,
                &msg.media_mime,
                msg.media_size,
                msg.media_duration,
                &msg.media_thumb,
                &msg.sent_at,
            ),
        )
        .await
    }

    pub async fn get_conversations(&self, user_id: &str) -> Result<Vec<ConversationItem>> {
        let user_id_b = ulid_to_bytes(user_id)?;

        // peer_id di result set masih BINARY(16) — dikonversi di bawah
        let q = r"
            SELECT
                peer_id,
                peer_name,
                peer_avatar,
                last_message,
                last_msg_type,
                last_sent_at,
                order_id,
                order_status,
                (
                    SELECT COUNT(*)
                    FROM messages
                    WHERE recipient_id = ? AND sender_id = peer_id AND read_at IS NULL
                ) AS unread_count
            FROM (
                SELECT
                    CASE WHEN m.sender_id = ? THEN m.recipient_id ELSE m.sender_id END AS peer_id,
                    IFNULL(u.name,       '') AS peer_name,
                    IFNULL(u.avatar_url, '') AS peer_avatar,
                    IFNULL(m.content,    '') AS last_message,
                    IFNULL(m.msg_type,   '') AS last_msg_type,
                    IFNULL(DATE_FORMAT(m.sent_at, '%Y-%m-%dT%H:%i:%sZ'), '0001-01-01T00:00:00Z') AS last_sent_at,
                    m.order_id,
                    IFNULL(o.status, '') AS order_status,
                    ROW_NUMBER() OVER (
                        PARTITION BY CASE WHEN m.sender_id = ? THEN m.recipient_id ELSE m.sender_id END
                        ORDER BY m.sent_at DESC
                    ) AS rn
                FROM messages m
                JOIN users u ON u.id = CASE WHEN m.sender_id = ? THEN m.recipient_id ELSE m.sender_id END
                JOIN orders o ON m.order_id = o.id
                WHERE m.sender_id = ? OR m.recipient_id = ?
            ) t
            WHERE rn = 1
            ORDER BY last_sent_at DESC";

        let ub = &user_id_b[..];
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<Row> = conn.exec(q, (ub, ub, ub, ub, ub, ub)).await?;

        rows.iter()
            .map(|r| -> Result<ConversationItem> {
                Ok(ConversationItem {
                    peer_id: bin_to_ulid(from_value(col(r, "peer_id")?))?,
                    peer_name: from_value(col(r, "peer_name")?),
                    peer_avatar: from_value(col(r, "peer_avatar")?),
                    last_message: from_value(col(r, "last_message")?),
                    last_message_type: from_value(col(r, "last_msg_type")?),
                    last_seen_at: from_value(col(r, "last_sent_at")?),
                    unread_count: from_value(col(r, "unread_count")?),
                    order_id: bin_to_ulid(from_value(col(r, "order_id")?))?,
                    order_status: from_value(col(r, "order_status")?),
                })
            })
            .collect()
    }

    pub async fn get_history(
        &self,
        sender_id: &str,
        reader_id: &str,
        limit: i32,
        _before_id: Option<&str>,
    ) -> Result<Vec<Message>> {
        let sender_b = ulid_to_bytes(sender_id)?;
        let reader_b = ulid_to_bytes(reader_id)?;

        let q = format!(
            r"SELECT {} FROM messages m
              JOIN users u ON m.sender_id = u.id
              JOIN orders o ON m.order_id = o.id
              WHERE (m.sender_id = ? AND m.recipient_id = ?)
                 OR (m.sender_id = ? AND m.recipient_id = ?)
              ORDER BY m.sent_at DESC
              LIMIT ?",
            Self::msg_cols()
        );
        let rows: Vec<Row> = exec_rows(
            &self.pool,
            &q,
            (
                &sender_b[..],
                &reader_b[..],
                &reader_b[..],
                &sender_b[..],
                limit,
            ),
        )
        .await?;

        let mut msgs: Vec<Message> = rows
            .into_iter()
            .map(|r| Self::row_to_message(&r))
            .collect::<Result<_>>()?;
        msgs.reverse(); // oldest first
        Ok(msgs)
    }

    pub async fn mark_read(&self, reader_id: &str, sender_id: &str) -> Result<()> {
        let reader_b = ulid_to_bytes(reader_id)?;
        let sender_b = ulid_to_bytes(sender_id)?;
        exec_drop(
            &self.pool,
            "UPDATE messages SET read_at = NOW(3) WHERE recipient_id = ? AND sender_id = ? AND read_at IS NULL",
            (&reader_b[..], &sender_b[..]),
        )
        .await
    }

    pub async fn mark_delivered(&self, msg_id: &str) -> Result<()> {
        let msg_id_b = ulid_to_bytes(msg_id)?;
        exec_drop(
            &self.pool,
            "UPDATE messages SET delivered_at = NOW(3) WHERE id = ? AND delivered_at IS NULL",
            (&msg_id_b[..],),
        )
        .await
    }
}
