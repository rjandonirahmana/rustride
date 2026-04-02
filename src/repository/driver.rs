use anyhow::Result;
use async_trait::async_trait;
use deadpool_postgres::Pool;
use tokio_postgres::Row;

use super::db::{col_opt_str, exec_rows, f32_col, get_conn};
use crate::utils::ulid::{bin_to_ulid, id_to_vec};

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

#[derive(Debug, Clone, Default)]
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

#[derive(Debug)]
pub struct OrderHistoryFilter {
    pub date_from: Option<String>,
    pub date_to: Option<String>,
    pub status: Option<String>,
    pub service_type: Option<String>,
    pub page: u32,
    pub limit: u32,
}

#[derive(Debug, Default)]
pub struct EarningsResult {
    pub gross_earnings: i64,
    pub net_earnings: i64,
    pub tips: i64,
    pub trip_count: i32,
    pub cancel_count: i32,
    pub distance_km: f32,
    pub online_minutes: i64,
    pub daily: Vec<DailyEarningRow>,
}

#[derive(Debug)]
pub struct DailyEarningRow {
    pub date: String,
    pub net_earnings: i64,
    pub trip_count: i32,
    pub distance_km: f32,
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

// ── Trait ─────────────────────────────────────────────────────────────────────

#[async_trait]
pub trait DriverRepository: Send + Sync {
    async fn get_today_orders(&self, driver_id: &str) -> Result<Vec<DriverOrderItem>>;
    async fn get_today_summary(&self, driver_id: &str) -> Result<Option<DailySummary>>;
    async fn get_order_history(
        &self,
        driver_id: &str,
        filter: OrderHistoryFilter,
    ) -> Result<(Vec<DriverOrderItem>, u32)>;
    async fn get_order_detail(
        &self,
        driver_id: &str,
        order_id: &str,
    ) -> Result<Option<DriverOrderDetail>>;
    async fn get_earnings(
        &self,
        driver_id: &str,
        date_from: &str,
        date_to: &str,
    ) -> Result<EarningsResult>;
}

// ── PostgreSQL implementasi ───────────────────────────────────────────────────

#[derive(Clone)]
pub struct PgDriverRepository {
    pool: Pool,
}

impl PgDriverRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    // Ambil o.id sebagai BYTEA langsung — decode ke ULID via bin_to_ulid.
    // Tidak pakai encode(o.id,'hex') di SQL.
    fn order_item_cols() -> &'static str {
        r#"
            o.id                                                                AS order_id,
            o.status,
            COALESCE(u.name, '')                                                AS rider_name,
            o.pickup_address,
            o.dest_address,
            COALESCE(o.distance_km::FLOAT8, 0)                                 AS distance_km,
            COALESCE(EXTRACT(EPOCH FROM (o.completed_at - o.started_at))::INT / 60, 0) AS duration_min,
            COALESCE(o.fare_final, o.fare_estimate)                             AS fare,
            FLOOR(COALESCE(o.fare_final, o.fare_estimate) * 0.80)::BIGINT       AS driver_earning,
            COALESCE(
                (SELECT SUM(r.tip_amount) FROM ratings r
                 WHERE r.order_id = o.id AND r.target_id = o.driver_id), 0
            )::BIGINT                                                            AS tip,
            o.service_type,
            COALESCE(to_char(o.started_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '') AS started_at,
            COALESCE(to_char(o.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '') AS completed_at,
            COALESCE(o.cancel_reason, '')                                        AS cancel_reason,
            COALESCE(
                (SELECT r.score::FLOAT8 FROM ratings r
                 WHERE r.order_id = o.id AND r.target_id = o.driver_id LIMIT 1), 0
            )                                                                    AS rating,
            COALESCE(
                (SELECT r.comment FROM ratings r
                 WHERE r.order_id = o.id AND r.target_id = o.driver_id LIMIT 1), ''
            )                                                                    AS rating_comment
        "#
    }

    fn row_to_item(row: &Row) -> Result<DriverOrderItem> {
        // order_id datang sebagai BYTEA langsung
        let id_bytes: Vec<u8> = row.try_get("order_id")?;
        let order_id = bin_to_ulid(id_bytes)?;

        Ok(DriverOrderItem {
            order_id,
            status: row.try_get("status")?,
            rider_name: row.try_get("rider_name")?,
            pickup_address: row.try_get("pickup_address")?,
            dest_address: row.try_get("dest_address")?,
            distance_km: f32_col(row, "distance_km")?,
            duration_min: row.try_get("duration_min")?,
            fare: row.try_get("fare")?,
            driver_earning: row.try_get("driver_earning")?,
            tip: row.try_get("tip")?,
            service_type: row.try_get("service_type")?,
            started_at: row.try_get("started_at")?,
            completed_at: row.try_get("completed_at")?,
            cancel_reason: row.try_get("cancel_reason")?,
            rating: f32_col(row, "rating")?,
            rating_comment: row.try_get("rating_comment")?,
        })
    }
}

#[async_trait]
impl DriverRepository for PgDriverRepository {
    async fn get_today_orders(&self, driver_id: &str) -> Result<Vec<DriverOrderItem>> {
        let driver_b = id_to_vec(driver_id)?;
        let q = format!(
            r#"SELECT {}
               FROM orders o
               LEFT JOIN users u ON u.id = o.rider_id
               WHERE o.driver_id = $1
                 AND o.status IN ('completed','cancelled')
                 AND DATE(COALESCE(o.completed_at, o.cancelled_at) AT TIME ZONE 'UTC') = CURRENT_DATE
               ORDER BY COALESCE(o.completed_at, o.cancelled_at) DESC"#,
            Self::order_item_cols()
        );
        let rows = exec_rows(&self.pool, &q, &[&driver_b]).await?;
        rows.iter().map(Self::row_to_item).collect()
    }

    async fn get_today_summary(&self, driver_id: &str) -> Result<Option<DailySummary>> {
        let driver_b = id_to_vec(driver_id)?;
        let row = exec_rows(
            &self.pool,
            r#"SELECT total_orders, cancelled_orders, gross_earnings, platform_fee,
                      net_earnings, tips, online_minutes,
                      distance_km::FLOAT8 AS distance_km,
                      avg_rating::FLOAT8 AS avg_rating,
                      COALESCE(peak_hour, '') AS peak_hour
               FROM driver_daily_summary
               WHERE driver_id = $1 AND summary_date = CURRENT_DATE"#,
            &[&driver_b],
        )
        .await?;

        let Some(r) = row.into_iter().next() else {
            return Ok(None);
        };
        // DB schema: total_orders/cancelled_orders/online_minutes = SMALLINT → i16
        //            gross_earnings/platform_fee/net_earnings/tips = INTEGER → i32
        Ok(Some(DailySummary {
            total_orders: r.try_get::<_, i16>("total_orders")? as i32,
            cancelled_orders: r.try_get::<_, i16>("cancelled_orders")? as i32,
            gross_earnings: r.try_get::<_, i32>("gross_earnings")? as i64,
            platform_fee: r.try_get::<_, i32>("platform_fee")? as i64,
            net_earnings: r.try_get::<_, i32>("net_earnings")? as i64,
            tips: r.try_get::<_, i32>("tips")? as i64,
            online_minutes: r.try_get::<_, i16>("online_minutes")? as i32,
            distance_km: f32_col(&r, "distance_km")?,
            avg_rating: f32_col(&r, "avg_rating")?,
            peak_hour: r.try_get("peak_hour")?,
        }))
    }

    async fn get_order_history(
        &self,
        driver_id: &str,
        filter: OrderHistoryFilter,
    ) -> Result<(Vec<DriverOrderItem>, u32)> {
        let driver_b = id_to_vec(driver_id)?;
        let limit = (filter.limit.max(1).min(100)) as i64;
        let offset = (filter.page.saturating_sub(1) as i64) * limit;

        let mut where_parts: Vec<String> = vec![
            "o.driver_id = $1".into(),
            "o.status IN ('completed','cancelled')".into(),
        ];
        let mut param_idx = 2usize;

        let mut date_from_str = String::new();
        let mut date_to_str = String::new();
        let mut status_str = String::new();
        let mut svc_str = String::new();

        if let Some(ref df) = filter.date_from {
            date_from_str = df.clone();
            where_parts.push(format!(
                "DATE(COALESCE(o.completed_at, o.cancelled_at) AT TIME ZONE 'UTC') >= ${}",
                param_idx
            ));
            param_idx += 1;
        }
        if let Some(ref dt) = filter.date_to {
            date_to_str = dt.clone();
            where_parts.push(format!(
                "DATE(COALESCE(o.completed_at, o.cancelled_at) AT TIME ZONE 'UTC') <= ${}",
                param_idx
            ));
            param_idx += 1;
        }
        if let Some(ref st) = filter.status {
            status_str = st.clone();
            where_parts.push(format!("o.status = ${}", param_idx));
            param_idx += 1;
        }
        if let Some(ref svc) = filter.service_type {
            svc_str = svc.clone();
            where_parts.push(format!("o.service_type = ${}", param_idx));
            param_idx += 1;
        }

        let where_sql = where_parts.join(" AND ");

        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&driver_b];
        if !date_from_str.is_empty() {
            params.push(&date_from_str);
        }
        if !date_to_str.is_empty() {
            params.push(&date_to_str);
        }
        if !status_str.is_empty() {
            params.push(&status_str);
        }
        if !svc_str.is_empty() {
            params.push(&svc_str);
        }

        // COUNT
        let count_q = format!(
            "SELECT COUNT(*)::BIGINT AS cnt FROM orders o WHERE {}",
            where_sql
        );
        let conn = get_conn(&self.pool).await?;
        let count_rows = conn.query(&count_q, params.as_slice()).await?;
        let total: i64 = count_rows
            .first()
            .and_then(|r| r.try_get("cnt").ok())
            .unwrap_or(0);

        // DATA
        let limit_idx = param_idx;
        let offset_idx = param_idx + 1;
        params.push(&limit);
        params.push(&offset);

        let data_q = format!(
            r#"SELECT {cols}
               FROM orders o
               LEFT JOIN users u ON u.id = o.rider_id
               WHERE {where}
               ORDER BY COALESCE(o.completed_at, o.cancelled_at) DESC
               LIMIT ${limit_idx} OFFSET ${offset_idx}"#,
            cols      = Self::order_item_cols(),
            where     = where_sql,
            limit_idx = limit_idx,
            offset_idx= offset_idx,
        );
        let rows = conn.query(&data_q, params.as_slice()).await?;
        let items = rows.iter().map(Self::row_to_item).collect::<Result<_>>()?;
        Ok((items, total as u32))
    }

    async fn get_order_detail(
        &self,
        driver_id: &str,
        order_id: &str,
    ) -> Result<Option<DriverOrderDetail>> {
        let driver_b = id_to_vec(driver_id)?;
        let order_b = id_to_vec(order_id)?;
        let q = format!(
            r#"SELECT {cols},
                      o.pickup_lat::FLOAT8, o.pickup_lng::FLOAT8,
                      o.dest_lat::FLOAT8,   o.dest_lng::FLOAT8,
                      COALESCE(u.phone,      '') AS rider_phone,
                      COALESCE(u.avatar_url, '') AS rider_avatar,
                      COALESCE(
                          (SELECT AVG(r2.score::FLOAT8) FROM ratings r2 WHERE r2.target_id = o.rider_id), 0
                      )::FLOAT8 AS rider_rating
               FROM orders o
               LEFT JOIN users u ON u.id = o.rider_id
               WHERE o.id = $1 AND o.driver_id = $2"#,
            cols = Self::order_item_cols()
        );

        let rows = exec_rows(&self.pool, &q, &[&order_b, &driver_b]).await?;
        let Some(r) = rows.into_iter().next() else {
            return Ok(None);
        };

        Ok(Some(DriverOrderDetail {
            item: Self::row_to_item(&r)?,
            pickup_lat: r.try_get("pickup_lat")?,
            pickup_lng: r.try_get("pickup_lng")?,
            dest_lat: r.try_get("dest_lat")?,
            dest_lng: r.try_get("dest_lng")?,
            rider_phone: r.try_get("rider_phone")?,
            rider_avatar: r.try_get("rider_avatar")?,
            rider_rating: f32_col(&r, "rider_rating")?,
        }))
    }

    async fn get_earnings(
        &self,
        driver_id: &str,
        date_from: &str,
        date_to: &str,
    ) -> Result<EarningsResult> {
        let driver_b = id_to_vec(driver_id)?;
        let conn = get_conn(&self.pool).await?;

        let agg = conn
            .query_one(
                r#"SELECT
                       COALESCE(SUM(COALESCE(fare_final, fare_estimate)), 0)::BIGINT              AS gross_earnings,
                       COALESCE(SUM(FLOOR(COALESCE(fare_final, fare_estimate) * 0.80)), 0)::BIGINT AS net_earnings,
                       COALESCE(
                           (SELECT SUM(r.tip_amount) FROM ratings r
                            INNER JOIN orders o2 ON o2.id = r.order_id
                            WHERE o2.driver_id = $1
                              AND DATE(COALESCE(o2.completed_at, o2.cancelled_at) AT TIME ZONE 'UTC') BETWEEN $2 AND $3
                              AND r.target_id = $1), 0
                       )::BIGINT                                                                   AS tips,
                       COUNT(CASE WHEN status = 'completed' THEN 1 END)::INT                      AS trip_count,
                       COUNT(CASE WHEN status = 'cancelled' THEN 1 END)::INT                      AS cancel_count,
                       COALESCE(SUM(CASE WHEN status='completed' THEN distance_km::FLOAT8 ELSE 0 END), 0)::FLOAT8 AS distance_km
                   FROM orders
                   WHERE driver_id = $1
                     AND DATE(COALESCE(completed_at, cancelled_at) AT TIME ZONE 'UTC') BETWEEN $2 AND $3"#,
                &[&driver_b, &date_from, &date_to],
            )
            .await?;

        let online_minutes: i64 = conn
            .query_one(
                "SELECT COALESCE(SUM(online_minutes), 0)::BIGINT AS om FROM driver_daily_summary WHERE driver_id=$1 AND summary_date BETWEEN $2 AND $3",
                &[&driver_b, &date_from, &date_to],
            )
            .await?
            .try_get("om")?;

        let daily_rows = conn
            .query(
                r#"SELECT
                       DATE(COALESCE(completed_at, cancelled_at) AT TIME ZONE 'UTC')::TEXT         AS date,
                       COALESCE(SUM(FLOOR(COALESCE(fare_final, fare_estimate) * 0.80)), 0)::BIGINT AS net_earnings,
                       COUNT(CASE WHEN status = 'completed' THEN 1 END)::INT                       AS trip_count,
                       COALESCE(SUM(CASE WHEN status='completed' THEN distance_km::FLOAT8 ELSE 0 END), 0)::FLOAT8 AS distance_km
                   FROM orders
                   WHERE driver_id = $1
                     AND DATE(COALESCE(completed_at, cancelled_at) AT TIME ZONE 'UTC') BETWEEN $2 AND $3
                   GROUP BY DATE(COALESCE(completed_at, cancelled_at) AT TIME ZONE 'UTC')
                   ORDER BY 1 ASC"#,
                &[&driver_b, &date_from, &date_to],
            )
            .await?;

        let daily = daily_rows
            .iter()
            .map(|r| -> Result<DailyEarningRow> {
                Ok(DailyEarningRow {
                    date: r.try_get("date")?,
                    net_earnings: r.try_get("net_earnings")?,
                    trip_count: r.try_get("trip_count")?,
                    distance_km: f32_col(r, "distance_km")?,
                })
            })
            .collect::<Result<_>>()?;

        Ok(EarningsResult {
            gross_earnings: agg.try_get("gross_earnings")?,
            net_earnings: agg.try_get("net_earnings")?,
            tips: agg.try_get("tips")?,
            trip_count: agg.try_get("trip_count")?,
            cancel_count: agg.try_get("cancel_count")?,
            distance_km: f32_col(&agg, "distance_km")?,
            online_minutes,
            daily,
        })
    }
}
