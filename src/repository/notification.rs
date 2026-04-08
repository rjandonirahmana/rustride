use anyhow::Result;
use async_trait::async_trait;
use deadpool_postgres::Pool;
use tokio_postgres::Row;

use super::db::{col_opt_str, exec_drop, exec_first, exec_rows};
use crate::utils::ulid::{bin_to_ulid, bin_to_ulid_opt, id_to_vec};

// ── notifications.id adalah GENERATED ALWAYS AS IDENTITY ─────────────────────
// Tidak boleh insert id manual — PostgreSQL generate sendiri.
// Pakai RETURNING id untuk dapat id yang baru dibuat.

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

pub struct NotificationRepository {
    pool: Pool,
}

impl NotificationRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn notif_cols() -> &'static str {
        // "type" di-quote karena reserved word di SQL
        r#"id, user_id, "type" AS notif_type, title, body,
           ref_order_id, ref_user_id, meta::TEXT AS meta,
           is_read,
           to_char(read_at    AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS read_at,
           to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at"#
    }

    fn row_to_notification(row: &Row) -> Result<Notification> {
        let uid_bytes: Vec<u8> = row.try_get("user_id")?;
        let ref_order_bytes: Option<Vec<u8>> = row.try_get("ref_order_id")?;
        let ref_user_bytes: Option<Vec<u8>> = row.try_get("ref_user_id")?;
        Ok(Notification {
            id: row.try_get("id")?,
            user_id: bin_to_ulid(uid_bytes)?,
            notif_type: row.try_get("notif_type")?,
            title: row.try_get("title")?,
            body: row.try_get("body")?,
            ref_order_id: bin_to_ulid_opt(ref_order_bytes)?,
            ref_user_id: bin_to_ulid_opt(ref_user_bytes)?,
            meta: col_opt_str(row, "meta")?,
            is_read: row.try_get("is_read")?,
            read_at: col_opt_str(row, "read_at")?,
            created_at: row.try_get("created_at")?,
        })
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
        let user_b = id_to_vec(user_id)?;

        // Optional BYTEA — ulid_to_vec hanya kalau Some
        let ref_order_b: Option<Vec<u8>> = ref_order_id.map(id_to_vec).transpose()?;
        let ref_user_b: Option<Vec<u8>> = ref_user_id.map(id_to_vec).transpose()?;

        // id TIDAK diinsert — GENERATED ALWAYS AS IDENTITY
        // meta::jsonb — cast dari text
        let row = exec_first(
            &self.pool,
            r#"INSERT INTO notifications
               (user_id, "type", title, body, ref_order_id, ref_user_id, meta)
               VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
               RETURNING id"#,
            &[
                &user_b, // &Vec<u8> — BYTEA
                &notif_type,
                &title,
                &body,
                &ref_order_b, // Option<Vec<u8>> — nullable BYTEA
                &ref_user_b,  // Option<Vec<u8>> — nullable BYTEA
                &meta,        // Option<&str> — nullable TEXT → jsonb
            ],
        )
        .await?;

        Ok(row
            .as_ref()
            .and_then(|r| r.try_get("id").ok())
            .unwrap_or(0i64))
    }

    async fn list(
        &self,
        user_id: &str,
        limit: i32,
        before_id: Option<i64>,
        unread_only: bool,
    ) -> Result<Vec<Notification>> {
        let user_b = id_to_vec(user_id)?;
        let cols = Self::notif_cols();
        let limit_i64 = limit as i64;

        let rows = match (unread_only, before_id) {
            (true,  Some(bid)) => exec_rows(&self.pool,
                &format!("SELECT {cols} FROM notifications WHERE user_id=$1 AND is_read=false AND id<$2 ORDER BY id DESC LIMIT $3"),
                &[&user_b, &bid, &limit_i64]).await?,
            (true,  None) => exec_rows(&self.pool,
                &format!("SELECT {cols} FROM notifications WHERE user_id=$1 AND is_read=false ORDER BY id DESC LIMIT $2"),
                &[&user_b, &limit_i64]).await?,
            (false, Some(bid)) => exec_rows(&self.pool,
                &format!("SELECT {cols} FROM notifications WHERE user_id=$1 AND id<$2 ORDER BY id DESC LIMIT $3"),
                &[&user_b, &bid, &limit_i64]).await?,
            (false, None) => exec_rows(&self.pool,
                &format!("SELECT {cols} FROM notifications WHERE user_id=$1 ORDER BY id DESC LIMIT $2"),
                &[&user_b, &limit_i64]).await?,
        };

        rows.iter().map(Self::row_to_notification).collect()
    }

    async fn mark_read(&self, notif_id: i64, user_id: &str) -> Result<()> {
        let user_b = id_to_vec(user_id)?;
        exec_drop(
            &self.pool,
            "UPDATE notifications SET is_read=true, read_at=NOW() WHERE id=$1 AND user_id=$2",
            &[&notif_id, &user_b],
        )
        .await
    }

    async fn mark_all_read(&self, user_id: &str) -> Result<()> {
        let user_b = id_to_vec(user_id)?;
        exec_drop(&self.pool,
            "UPDATE notifications SET is_read=true, read_at=NOW() WHERE user_id=$1 AND is_read=false",
            &[&user_b]).await
    }

    async fn unread_count(&self, user_id: &str) -> Result<i64> {
        let user_b = id_to_vec(user_id)?;
        let row = exec_first(
            &self.pool,
            "SELECT COUNT(*)::BIGINT AS cnt FROM notifications WHERE user_id=$1 AND is_read=false",
            &[&user_b],
        )
        .await?;
        Ok(row
            .as_ref()
            .and_then(|r| r.try_get("cnt").ok())
            .unwrap_or(0i64))
    }
}
