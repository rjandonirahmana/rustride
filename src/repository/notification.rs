use anyhow::Result;
use async_trait::async_trait;
use mysql_async::Pool;
use mysql_async::{from_value, Row};

use super::db::{col, col_opt_str, exec_drop, exec_rows};

// ── Model ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Notification {
    pub id: i64,
    pub user_id: String,
    pub notif_type: String,
    pub title: String,
    pub body: String,
    pub ref_order_id: Option<String>,
    pub ref_user_id: Option<String>,
    pub meta: Option<String>,
    pub is_read: bool,
    pub read_at: Option<String>,
    pub created_at: String,
}

// ── Repository ────────────────────────────────────────────────────────────────

pub struct NotificationRepository {
    pool: Pool,
}
#[async_trait]
pub trait NotificationRepositorytrait: Send + Sync {
    async fn create(
        &self,
        user_id: &str,
        notif_type: &str,
        title: &str,
        body: &str,
        ref_order_id: Option<&str>,
        ref_user_id: Option<&str>,
        meta: Option<&str>,
    ) -> Result<i64>;

    async fn list(
        &self,
        user_id: &str,
        limit: i32,
        before_id: Option<i64>,
        unread_only: bool,
    ) -> Result<Vec<Notification>>;

    async fn mark_read(&self, notif_id: i64, user_id: &str) -> Result<()>;
    async fn mark_all_read(&self, user_id: &str) -> Result<()>;
    async fn unread_count(&self, user_id: &str) -> Result<i64>;
}

impl NotificationRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn row_to_notification(row: &Row) -> Result<Notification> {
        Ok(Notification {
            id: from_value(col(row, "id")?),
            user_id: from_value(col(row, "user_id")?),
            notif_type: from_value(col(row, "type")?),
            title: from_value(col(row, "title")?),
            body: from_value(col(row, "body")?),
            ref_order_id: col_opt_str(row, "ref_order_id"),
            ref_user_id: col_opt_str(row, "ref_user_id"),
            meta: col_opt_str(row, "meta"),
            is_read: {
                let v: u8 = from_value(col(row, "is_read")?);
                v != 0
            },
            read_at: col_opt_str(row, "read_at"),
            created_at: from_value(col(row, "created_at")?),
        })
    }

    // ── Create ────────────────────────────────────────────────────────────────
}

#[async_trait]
impl NotificationRepositorytrait for NotificationRepository {
    async fn create(
        &self,
        user_id: &str,
        notif_type: &str,
        title: &str,
        body: &str,
        ref_order_id: Option<&str>,
        ref_user_id: Option<&str>,
        meta: Option<&str>,
    ) -> Result<i64> {
        let id = uuid::Uuid::new_v4().to_string();
        exec_drop(
            &self.pool,
            r"INSERT INTO notifications
                (id, user_id, type, title, body, ref_order_id, ref_user_id, meta)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                &id,
                user_id,
                notif_type,
                title,
                body,
                ref_order_id,
                ref_user_id,
                meta,
            ),
        )
        .await?;

        // Ambil auto_increment id yang baru di-insert
        let rows: Vec<Row> = exec_rows(
            &self.pool,
            "SELECT id FROM notifications WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        .await?;

        let notif_id: i64 = rows
            .first()
            .map(|r| from_value(col(r, "id").unwrap()))
            .unwrap_or(0);

        Ok(notif_id)
    }

    // ── List ──────────────────────────────────────────────────────────────────

    async fn list(
        &self,
        user_id: &str,
        limit: i32,
        before_id: Option<i64>,
        unread_only: bool,
    ) -> Result<Vec<Notification>> {
        let rows: Vec<Row> = match (unread_only, before_id) {
            (true, Some(bid)) => {
                exec_rows(
                    &self.pool,
                    r"SELECT id, user_id, type, title, body,
                         ref_order_id, ref_user_id, meta,
                         is_read, read_at,
                         DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') AS created_at
                  FROM notifications
                  WHERE user_id = ? AND is_read = 0 AND id < ?
                  ORDER BY id DESC LIMIT ?",
                    (user_id, bid, limit),
                )
                .await?
            }

            (true, None) => {
                exec_rows(
                    &self.pool,
                    r"SELECT id, user_id, type, title, body,
                         ref_order_id, ref_user_id, meta,
                         is_read, read_at,
                         DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') AS created_at
                  FROM notifications
                  WHERE user_id = ? AND is_read = 0
                  ORDER BY id DESC LIMIT ?",
                    (user_id, limit),
                )
                .await?
            }

            (false, Some(bid)) => {
                exec_rows(
                    &self.pool,
                    r"SELECT id, user_id, type, title, body,
                         ref_order_id, ref_user_id, meta,
                         is_read, read_at,
                         DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') AS created_at
                  FROM notifications
                  WHERE user_id = ? AND id < ?
                  ORDER BY id DESC LIMIT ?",
                    (user_id, bid, limit),
                )
                .await?
            }

            (false, None) => {
                exec_rows(
                    &self.pool,
                    r"SELECT id, user_id, type, title, body,
                         ref_order_id, ref_user_id, meta,
                         is_read, read_at,
                         DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') AS created_at
                  FROM notifications
                  WHERE user_id = ?
                  ORDER BY id DESC LIMIT ?",
                    (user_id, limit),
                )
                .await?
            }
        };

        rows.iter().map(Self::row_to_notification).collect()
    }

    // ── Mark read ─────────────────────────────────────────────────────────────

    async fn mark_read(&self, notif_id: i64, user_id: &str) -> Result<()> {
        exec_drop(
            &self.pool,
            "UPDATE notifications SET is_read = 1, read_at = NOW(3) WHERE id = ? AND user_id = ?",
            (notif_id, user_id),
        )
        .await
    }

    async fn mark_all_read(&self, user_id: &str) -> Result<()> {
        exec_drop(
            &self.pool,
            "UPDATE notifications SET is_read = 1, read_at = NOW(3) WHERE user_id = ? AND is_read = 0",
            (user_id,),
        )
        .await
    }

    async fn unread_count(&self, user_id: &str) -> Result<i64> {
        let rows: Vec<Row> = exec_rows(
            &self.pool,
            "SELECT COUNT(*) AS cnt FROM notifications WHERE user_id = ? AND is_read = 0",
            (user_id,),
        )
        .await?;

        Ok(rows
            .first()
            .map(|r| from_value(col(r, "cnt").unwrap()))
            .unwrap_or(0))
    }
}
