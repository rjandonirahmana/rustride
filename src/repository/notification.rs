use anyhow::Result;
use async_trait::async_trait;
use mysql_async::{from_value, Pool, Row};

use super::db::{col, col_opt_str, exec_drop, exec_rows};
use crate::utils::ulid::{bin_to_ulid, bin_to_ulid_opt, ulid_to_bytes};

// ── Model ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Notification {
    pub id: i64,         // BIGINT AUTO_INCREMENT — tetap i64
    pub user_id: String, // BINARY(16) → ULID string
    pub notif_type: String,
    pub title: String,
    pub body: String,
    pub ref_order_id: Option<String>, // BINARY(16) nullable → ULID string
    pub ref_user_id: Option<String>,  // BINARY(16) nullable → ULID string
    pub meta: Option<String>,
    pub is_read: bool,
    pub read_at: Option<String>,
    pub created_at: String,
}

// ── Trait ─────────────────────────────────────────────────────────────────────

#[async_trait]
pub trait NotificationRepositoryTrait: Send + Sync {
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

// ── MySQL implementasi ────────────────────────────────────────────────────────

pub struct NotificationRepository {
    pool: Pool,
}

impl NotificationRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn row_to_notification(row: &Row) -> Result<Notification> {
        Ok(Notification {
            id: from_value(col(row, "id")?),
            user_id: bin_to_ulid(from_value(col(row, "user_id")?))?,
            notif_type: from_value(col(row, "type")?),
            title: from_value(col(row, "title")?),
            body: from_value(col(row, "body")?),
            ref_order_id: bin_to_ulid_opt(col(row, "ref_order_id")?)?,
            ref_user_id: bin_to_ulid_opt(col(row, "ref_user_id")?)?,
            meta: col_opt_str(row, "meta"),
            is_read: {
                let v: u8 = from_value(col(row, "is_read")?);
                v != 0
            },
            read_at: col_opt_str(row, "read_at"),
            created_at: from_value(col(row, "created_at")?),
        })
    }

    // SELECT cols yang sama dipakai di semua list query
    fn notif_cols() -> &'static str {
        r"id, user_id, type, title, body,
          ref_order_id, ref_user_id, meta,
          is_read, read_at,
          DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') AS created_at"
    }
}

#[async_trait]
impl NotificationRepositoryTrait for NotificationRepository {
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
        let user_id_b = ulid_to_bytes(user_id)?;

        // ref_order_id & ref_user_id nullable BINARY(16) — konversi hanya kalau Some
        let ref_order_b: Option<[u8; 16]> = ref_order_id.map(ulid_to_bytes).transpose()?;
        let ref_user_b: Option<[u8; 16]> = ref_user_id.map(ulid_to_bytes).transpose()?;

        exec_drop(
            &self.pool,
            r"INSERT INTO notifications
                (user_id, type, title, body, ref_order_id, ref_user_id, meta)
              VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                &user_id_b[..],
                notif_type,
                title,
                body,
                ref_order_b.as_ref().map(|b| b.as_slice()),
                ref_user_b.as_ref().map(|b| b.as_slice()),
                meta,
            ),
        )
        .await?;

        // Ambil AUTO_INCREMENT id yang baru saja diinsert
        let rows: Vec<Row> = exec_rows(
            &self.pool,
            "SELECT id FROM notifications WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (&user_id_b[..],),
        )
        .await?;

        let notif_id: i64 = rows
            .first()
            .map(|r| from_value(col(r, "id").unwrap()))
            .unwrap_or(0);

        Ok(notif_id)
    }

    async fn list(
        &self,
        user_id: &str,
        limit: i32,
        before_id: Option<i64>,
        unread_only: bool,
    ) -> Result<Vec<Notification>> {
        let user_id_b = ulid_to_bytes(user_id)?;
        let cols = Self::notif_cols();

        let rows: Vec<Row> = match (unread_only, before_id) {
            (true, Some(bid)) => exec_rows(
                &self.pool,
                &format!("SELECT {} FROM notifications WHERE user_id = ? AND is_read = 0 AND id < ? ORDER BY id DESC LIMIT ?", cols),
                (&user_id_b[..], bid, limit),
            ).await?,
            (true, None) => exec_rows(
                &self.pool,
                &format!("SELECT {} FROM notifications WHERE user_id = ? AND is_read = 0 ORDER BY id DESC LIMIT ?", cols),
                (&user_id_b[..], limit),
            ).await?,
            (false, Some(bid)) => exec_rows(
                &self.pool,
                &format!("SELECT {} FROM notifications WHERE user_id = ? AND id < ? ORDER BY id DESC LIMIT ?", cols),
                (&user_id_b[..], bid, limit),
            ).await?,
            (false, None) => exec_rows(
                &self.pool,
                &format!("SELECT {} FROM notifications WHERE user_id = ? ORDER BY id DESC LIMIT ?", cols),
                (&user_id_b[..], limit),
            ).await?,
        };

        rows.iter().map(Self::row_to_notification).collect()
    }

    async fn mark_read(&self, notif_id: i64, user_id: &str) -> Result<()> {
        let user_id_b = ulid_to_bytes(user_id)?;
        exec_drop(
            &self.pool,
            "UPDATE notifications SET is_read = 1, read_at = NOW(3) WHERE id = ? AND user_id = ?",
            (notif_id, &user_id_b[..]),
        )
        .await
    }

    async fn mark_all_read(&self, user_id: &str) -> Result<()> {
        let user_id_b = ulid_to_bytes(user_id)?;
        exec_drop(
            &self.pool,
            "UPDATE notifications SET is_read = 1, read_at = NOW(3) WHERE user_id = ? AND is_read = 0",
            (&user_id_b[..],),
        )
        .await
    }

    async fn unread_count(&self, user_id: &str) -> Result<i64> {
        let user_id_b = ulid_to_bytes(user_id)?;
        let rows: Vec<Row> = exec_rows(
            &self.pool,
            "SELECT COUNT(*) AS cnt FROM notifications WHERE user_id = ? AND is_read = 0",
            (&user_id_b[..],),
        )
        .await?;
        Ok(rows
            .first()
            .map(|r| from_value(col(r, "cnt").unwrap()))
            .unwrap_or(0))
    }
}
