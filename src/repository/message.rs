use anyhow::Result;
use mysql_async::{from_value, Row};

use super::db::{col, col_opt_i32, col_opt_i64, col_opt_str, exec_drop, exec_rows};
use crate::models::message::Message;
use mysql_async::Pool;

pub struct MessageRepository {
    pool: Pool,
}

impl MessageRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn row_to_message(row: &Row) -> Result<Message> {
        Ok(Message {
            id: from_value(col(row, "id")?),
            sender_id: from_value(col(row, "sender_id")?),
            recipient_id: from_value(col(row, "recipient_id")?),
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
            sender_avatar: col_opt_str(row, "sender_avatar"),
            sender_name: col_opt_str(row, "sender_name"),
        })
    }

    fn msg_cols() -> &'static str {
        r"m.id, m.sender_id, m.recipient_id, m.content, m.msg_type,
          m.media_url, m.media_mime, m.media_size, m.media_duration, m.media_thumb,
          IFNULL(DATE_FORMAT(m.sent_at, '%Y-%m-%dT%H:%i:%sZ'), '0001-01-01T00:00:00Z') AS sent_at, IFNULL(DATE_FORMAT(m.delivered_at, '%Y-%m-%dT%H:%i:%sZ'), '0001-01-01T00:00:00Z') AS delivered_at, IFNULL(DATE_FORMAT(m.read_at, '%Y-%m-%dT%H:%i:%sZ'), '0001-01-01T00:00:00Z') AS read_at, u.name AS sender_name, u.avatar_url AS sender_avatar"
    }

    pub async fn save(&self, msg: &mut Message) -> Result<()> {
        exec_drop(
            &self.pool,
            r"INSERT INTO messages
              (id, sender_id, recipient_id, content, msg_type,
               media_url, media_mime, media_size, media_duration, media_thumb, sent_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                &msg.id,
                &msg.sender_id,
                &msg.recipient_id,
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

    pub async fn get_history(
        &self,
        sender_id: &str,
        reader_id: &str,
        limit: i32,
        before_id: Option<&str>,
    ) -> Result<Vec<Message>> {
        let q = format!(
            r"SELECT {} FROM messages m JOIN users u ON m.sender_id = u.id
                  WHERE (m.sender_id = ? AND m.recipient_id = ?) OR (m.sender_id = ? AND m.recipient_id = ?)
                  ORDER BY m.sent_at DESC LIMIT ?",
            Self::msg_cols()
        );
        let rows: Vec<Row> = exec_rows(
            &self.pool,
            &q,
            (sender_id, reader_id, reader_id, sender_id, limit),
        )
        .await?;

        let mut msgs: Vec<Message> = rows
            .into_iter()
            .map(|r| Self::row_to_message(&r))
            .collect::<Result<Vec<_>>>()?;
        msgs.reverse(); // oldest first
        Ok(msgs)
    }

    pub async fn mark_read(&self, reader_id: &str, sender_id: &str) -> Result<()> {
        exec_drop(
            &self.pool,
            "UPDATE messages SET read_at = NOW(3) WHERE recipient_id = ? AND sender_id = ? AND read_at IS NULL",
            (reader_id, sender_id),
        )
        .await
    }

    pub async fn mark_delivered(&self, msg_id: &str) -> Result<()> {
        exec_drop(
            &self.pool,
            "UPDATE messages SET delivered_at = NOW(3) WHERE id = ? AND delivered_at IS NULL",
            (msg_id,),
        )
        .await
    }
}
