use anyhow::Result;
use async_trait::async_trait;
use mysql_async::{from_value, prelude::Queryable, Pool, Row};

use super::db::{col, col_opt_f32, col_opt_i32, col_opt_str, exec_rows};

// ── Models ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DriverOrderItem {
    pub order_id: String,
    pub status: String,
    pub rider_name: String,
    pub pickup_address: String,
    pub dest_address: String,
    pub distance_km: f32,
    pub duration_min: i32,
    pub fare: i64,
    pub driver_earning: i64,
    pub tip: i64,
    pub service_type: String,
    pub started_at: String,
    pub completed_at: String,
    pub cancel_reason: String,
    pub rating: f32,
    pub rating_comment: String,
}

#[derive(Debug, Clone)]
pub struct DailySummary {
    pub total_orders: i32,
    pub cancelled_orders: i32,
    pub gross_earnings: i64,
    pub platform_fee: i64,
    pub net_earnings: i64,
    pub tips: i64,
    pub online_minutes: i32,
    pub distance_km: f32,
    pub avg_rating: f32,
    pub peak_hour: String,
}

impl Default for DailySummary {
    fn default() -> Self {
        Self {
            total_orders: 0,
            cancelled_orders: 0,
            gross_earnings: 0,
            platform_fee: 0,
            net_earnings: 0,
            tips: 0,
            online_minutes: 0,
            distance_km: 0.0,
            avg_rating: 0.0,
            peak_hour: String::new(),
        }
    }
}

#[derive(Debug)]
pub struct OrderHistoryFilter {
    pub date_from: Option<String>, // "YYYY-MM-DD"
    pub date_to: Option<String>,
    pub status: Option<String>, // "completed" | "cancelled" | None = semua
    pub page: u32,
    pub limit: u32,
}

// ── Trait ─────────────────────────────────────────────────────────────────────

#[async_trait]
pub trait DriverRepository: Send + Sync {
    /// Ambil semua order hari ini milik driver.
    async fn get_today_orders(&self, driver_id: &str) -> Result<Vec<DriverOrderItem>>;

    /// Ambil summary hari ini dari driver_daily_summary.
    /// Return None kalau belum ada baris (driver belum pernah online hari ini).
    async fn get_today_summary(&self, driver_id: &str) -> Result<Option<DailySummary>>;

    /// History order dengan filter + pagination.
    async fn get_order_history(
        &self,
        driver_id: &str,
        filter: OrderHistoryFilter,
    ) -> Result<(Vec<DriverOrderItem>, u32)>; // (items, total_count)

    /// Detail satu order beserta route points.
    async fn get_order_detail(
        &self,
        driver_id: &str,
        order_id: &str,
    ) -> Result<Option<DriverOrderDetail>>;
}

#[derive(Debug, Clone)]
pub struct DriverOrderDetail {
    pub item: DriverOrderItem,
    pub pickup_lat: f64,
    pub pickup_lng: f64,
    pub dest_lat: f64,
    pub dest_lng: f64,
    pub rider_phone: String,
    pub rider_avatar: String,
    pub rider_rating: f32,
}

// ── MySQL implementasi ────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct MySqlDriverRepository {
    pool: Pool,
}

impl MySqlDriverRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    /// Kolom SELECT untuk daftar order driver.
    /// Platform fee dihitung inline (20%).
    fn order_item_cols() -> &'static str {
        r#"
            o.id                                                        AS order_id,
            o.status,
            COALESCE(u.name, '')                                        AS rider_name,
            o.pickup_address,
            o.dest_address,
            COALESCE(o.distance_km, 0)                                  AS distance_km,
            COALESCE(
                TIMESTAMPDIFF(MINUTE, o.started_at, o.completed_at), 0
            )                                                           AS duration_min,
            COALESCE(o.fare_final, o.fare_estimate)                     AS fare,
            FLOOR(COALESCE(o.fare_final, o.fare_estimate) * 0.80)       AS driver_earning,
            COALESCE(
                (SELECT SUM(r.tip_amount) FROM ratings r WHERE r.order_id = o.id
                 AND r.target_id = o.driver_id), 0
            )                                                           AS tip,
            o.service_type,
            COALESCE(DATE_FORMAT(o.started_at,   '%Y-%m-%dT%H:%i:%sZ'), '') AS started_at,
            COALESCE(DATE_FORMAT(o.completed_at, '%Y-%m-%dT%H:%i:%sZ'), '') AS completed_at,
            COALESCE(o.cancel_reason, '')                               AS cancel_reason,
            COALESCE(
                (SELECT r.score FROM ratings r WHERE r.order_id = o.id
                 AND r.target_id = o.driver_id LIMIT 1), 0
            )                                                           AS rating,
            COALESCE(
                (SELECT r.comment FROM ratings r WHERE r.order_id = o.id
                 AND r.target_id = o.driver_id LIMIT 1), ''
            )                                                           AS rating_comment
        "#
    }

    fn row_to_item(row: &Row) -> Result<DriverOrderItem> {
        Ok(DriverOrderItem {
            order_id: from_value(col(row, "order_id")?),
            status: from_value(col(row, "status")?),
            rider_name: from_value(col(row, "rider_name")?),
            pickup_address: from_value(col(row, "pickup_address")?),
            dest_address: from_value(col(row, "dest_address")?),
            distance_km: {
                let v: f64 = from_value(col(row, "distance_km")?);
                v as f32
            },
            duration_min: from_value(col(row, "duration_min")?),
            fare: from_value(col(row, "fare")?),
            driver_earning: from_value(col(row, "driver_earning")?),
            tip: from_value(col(row, "tip")?),
            service_type: from_value(col(row, "service_type")?),
            started_at: from_value(col(row, "started_at")?),
            completed_at: from_value(col(row, "completed_at")?),
            cancel_reason: from_value(col(row, "cancel_reason")?),
            rating: {
                let v: f64 = from_value(col(row, "rating")?);
                v as f32
            },
            rating_comment: from_value(col(row, "rating_comment")?),
        })
    }
}

#[async_trait]
impl DriverRepository for MySqlDriverRepository {
    // ── Today orders ──────────────────────────────────────────────────────────
    async fn get_today_orders(&self, driver_id: &str) -> Result<Vec<DriverOrderItem>> {
        let q = format!(
            r#"SELECT {}
               FROM orders o
               LEFT JOIN users u ON u.id = o.rider_id
               WHERE o.driver_id = ?
                 AND o.status IN ('completed', 'cancelled')
                 AND DATE(COALESCE(o.completed_at, o.cancelled_at)) = CURDATE()
               ORDER BY COALESCE(o.completed_at, o.cancelled_at) DESC"#,
            Self::order_item_cols()
        );
        let rows = exec_rows(&self.pool, &q, (driver_id,)).await?;
        rows.iter().map(Self::row_to_item).collect()
    }

    // ── Today summary ─────────────────────────────────────────────────────────
    async fn get_today_summary(&self, driver_id: &str) -> Result<Option<DailySummary>> {
        let mut conn = self.pool.get_conn().await?;
        let row: Option<Row> = conn
            .exec_first(
                r#"SELECT
                       total_orders,
                       cancelled_orders,
                       gross_earnings,
                       platform_fee,
                       net_earnings,
                       tips,
                       online_minutes,
                       distance_km,
                       avg_rating,
                       COALESCE(peak_hour, '') AS peak_hour
                   FROM driver_daily_summary
                   WHERE driver_id = ? AND summary_date = CURDATE()"#,
                (driver_id,),
            )
            .await?;

        let Some(r) = row else { return Ok(None) };

        Ok(Some(DailySummary {
            total_orders: from_value(col(&r, "total_orders")?),
            cancelled_orders: from_value(col(&r, "cancelled_orders")?),
            gross_earnings: from_value(col(&r, "gross_earnings")?),
            platform_fee: from_value(col(&r, "platform_fee")?),
            net_earnings: from_value(col(&r, "net_earnings")?),
            tips: from_value(col(&r, "tips")?),
            online_minutes: from_value(col(&r, "online_minutes")?),
            distance_km: {
                let v: f64 = from_value(col(&r, "distance_km")?);
                v as f32
            },
            avg_rating: {
                let v: f64 = from_value(col(&r, "avg_rating")?);
                v as f32
            },
            peak_hour: from_value(col(&r, "peak_hour")?),
        }))
    }

    // ── Order history ─────────────────────────────────────────────────────────
    async fn get_order_history(
        &self,
        driver_id: &str,
        filter: OrderHistoryFilter,
    ) -> Result<(Vec<DriverOrderItem>, u32)> {
        let mut conn = self.pool.get_conn().await?;
        let limit = filter.limit.max(1).min(100);
        let offset = (filter.page.saturating_sub(1)) * limit;

        // Build WHERE clauses dinamis
        let mut where_parts = vec![
            "o.driver_id = ?".to_string(),
            "o.status IN ('completed', 'cancelled')".to_string(),
        ];
        let mut binds: Vec<mysql_async::Value> = vec![driver_id.into()];

        if let Some(ref df) = filter.date_from {
            where_parts.push("DATE(COALESCE(o.completed_at, o.cancelled_at)) >= ?".to_string());
            binds.push(df.as_str().into());
        }
        if let Some(ref dt) = filter.date_to {
            where_parts.push("DATE(COALESCE(o.completed_at, o.cancelled_at)) <= ?".to_string());
            binds.push(dt.as_str().into());
        }
        if let Some(ref st) = filter.status {
            where_parts.push("o.status = ?".to_string());
            binds.push(st.as_str().into());
        }

        let where_sql = where_parts.join(" AND ");

        // Count total
        let count_q = format!("SELECT COUNT(*) FROM orders o WHERE {}", where_sql);
        let total: u32 = conn.exec_first(&count_q, binds.clone()).await?.unwrap_or(0);

        // Fetch page
        binds.push((limit as u64).into());
        binds.push((offset as u64).into());

        let data_q = format!(
            r#"SELECT {}
               FROM orders o
               LEFT JOIN users u ON u.id = o.rider_id
               WHERE {}
               ORDER BY COALESCE(o.completed_at, o.cancelled_at) DESC
               LIMIT ? OFFSET ?"#,
            Self::order_item_cols(),
            where_sql
        );

        let rows: Vec<Row> = conn.exec(&data_q, binds).await?;
        let items = rows
            .iter()
            .map(Self::row_to_item)
            .collect::<Result<Vec<_>>>()?;
        Ok((items, total))
    }

    // ── Order detail ──────────────────────────────────────────────────────────
    async fn get_order_detail(
        &self,
        driver_id: &str,
        order_id: &str,
    ) -> Result<Option<DriverOrderDetail>> {
        let mut conn = self.pool.get_conn().await?;
        let row: Option<Row> = conn
            .exec_first(
                format!(
                    r#"SELECT {},
                           o.pickup_lat, o.pickup_lng, o.dest_lat, o.dest_lng,
                           COALESCE(u.phone, '')      AS rider_phone,
                           COALESCE(u.avatar_url, '') AS rider_avatar,
                           COALESCE(
                               (SELECT AVG(r2.score) FROM ratings r2
                                WHERE r2.target_id = o.rider_id), 0
                           ) AS rider_rating
                       FROM orders o
                       LEFT JOIN users u ON u.id = o.rider_id
                       WHERE o.id = ? AND o.driver_id = ?"#,
                    Self::order_item_cols()
                ),
                (order_id, driver_id),
            )
            .await?;

        let Some(r) = row else { return Ok(None) };

        Ok(Some(DriverOrderDetail {
            item: Self::row_to_item(&r)?,
            pickup_lat: from_value(col(&r, "pickup_lat")?),
            pickup_lng: from_value(col(&r, "pickup_lng")?),
            dest_lat: from_value(col(&r, "dest_lat")?),
            dest_lng: from_value(col(&r, "dest_lng")?),
            rider_phone: from_value(col(&r, "rider_phone")?),
            rider_avatar: from_value(col(&r, "rider_avatar")?),
            rider_rating: {
                let v: f64 = from_value(col(&r, "rider_rating")?);
                v as f32
            },
        }))
    }
}
