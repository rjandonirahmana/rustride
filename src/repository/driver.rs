use anyhow::Result;
use async_trait::async_trait;
use mysql_async::{from_value, prelude::Queryable, Pool, Row};

use super::db::{col, exec_rows};
use crate::{repository::db::f32_col, utils::ulid::ulid_to_bytes};

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

// ── MySQL implementasi ────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct MySqlDriverRepository {
    pool: Pool,
}

impl MySqlDriverRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    // order_id di sini adalah kolom BINARY(16) — diformat pakai HEX() supaya
    // row_to_item bisa baca sebagai string lalu dikonversi ke ULID
    fn order_item_cols() -> &'static str {
        r#"
            HEX(o.id)                                                    AS order_id,
            o.status,
            COALESCE(u.name, '')                                         AS rider_name,
            o.pickup_address,
            o.dest_address,
            COALESCE(o.distance_km, 0)                                   AS distance_km,
            COALESCE(TIMESTAMPDIFF(MINUTE, o.started_at, o.completed_at), 0) AS duration_min,
            COALESCE(o.fare_final, o.fare_estimate)                      AS fare,
            FLOOR(COALESCE(o.fare_final, o.fare_estimate) * 0.80)        AS driver_earning,
            COALESCE(
                (SELECT SUM(r.tip_amount) FROM ratings r
                 WHERE r.order_id = o.id AND r.target_id = o.driver_id), 0
            )                                                            AS tip,
            o.service_type,
            COALESCE(DATE_FORMAT(o.started_at,   '%Y-%m-%dT%H:%i:%sZ'), '') AS started_at,
            COALESCE(DATE_FORMAT(o.completed_at, '%Y-%m-%dT%H:%i:%sZ'), '') AS completed_at,
            COALESCE(o.cancel_reason, '')                                AS cancel_reason,
            COALESCE(
                (SELECT r.score FROM ratings r
                 WHERE r.order_id = o.id AND r.target_id = o.driver_id LIMIT 1), 0
            )                                                            AS rating,
            COALESCE(
                (SELECT r.comment FROM ratings r
                 WHERE r.order_id = o.id AND r.target_id = o.driver_id LIMIT 1), ''
            )                                                            AS rating_comment
        "#
    }

    fn row_to_item(row: &Row) -> Result<DriverOrderItem> {
        // order_id datang sebagai hex string 32 char dari HEX(o.id)
        // konversi ke ULID via hex → bytes → Ulid
        let order_id_hex: String = from_value(col(row, "order_id")?);
        let order_id = hex_to_ulid(&order_id_hex)?;

        Ok(DriverOrderItem {
            order_id,
            status: from_value(col(row, "status")?),
            rider_name: from_value(col(row, "rider_name")?),
            pickup_address: from_value(col(row, "pickup_address")?),
            dest_address: from_value(col(row, "dest_address")?),
            distance_km: f32_col(row, "distance_km")?,
            duration_min: from_value(col(row, "duration_min")?),
            fare: from_value(col(row, "fare")?),
            driver_earning: from_value(col(row, "driver_earning")?),
            tip: from_value(col(row, "tip")?),
            service_type: from_value(col(row, "service_type")?),
            started_at: from_value(col(row, "started_at")?),
            completed_at: from_value(col(row, "completed_at")?),
            cancel_reason: from_value(col(row, "cancel_reason")?),
            rating: f32_col(row, "rating")?,
            rating_comment: from_value(col(row, "rating_comment")?),
        })
    }
}

// driver_repository khusus: order_id di result set adalah HEX string (bukan raw bytes)
// karena dipakai di subquery correlated yang susah diubah — pakai HEX() di sini saja,
// bukan di semua repo. Ini exception yang terjustifikasi.
fn hex_to_ulid(hex: &str) -> Result<String> {
    use ulid::Ulid;
    let bytes = (0..32)
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect::<std::result::Result<Vec<u8>, _>>()
        .map_err(|e| anyhow::anyhow!("Invalid hex: {}", e))?;
    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("Expected 16 bytes"))?;
    Ok(Ulid::from_bytes(arr).to_string())
}

#[async_trait]
impl DriverRepository for MySqlDriverRepository {
    async fn get_today_orders(&self, driver_id: &str) -> Result<Vec<DriverOrderItem>> {
        let driver_b = ulid_to_bytes(driver_id)?;
        let q = format!(
            r#"SELECT {}
               FROM orders o
               LEFT JOIN users u ON u.id = o.rider_id
               WHERE o.driver_id = ?
                 AND o.status IN ('completed','cancelled')
                 AND DATE(COALESCE(o.completed_at, o.cancelled_at)) = CURDATE()
               ORDER BY COALESCE(o.completed_at, o.cancelled_at) DESC"#,
            Self::order_item_cols()
        );
        let rows = exec_rows(&self.pool, &q, (&driver_b[..],)).await?;
        rows.iter().map(Self::row_to_item).collect()
    }

    async fn get_today_summary(&self, driver_id: &str) -> Result<Option<DailySummary>> {
        let driver_b = ulid_to_bytes(driver_id)?;
        let mut conn = self.pool.get_conn().await?;
        let row: Option<Row> = conn
            .exec_first(
                r#"SELECT total_orders, cancelled_orders, gross_earnings, platform_fee,
                          net_earnings, tips, online_minutes, distance_km, avg_rating,
                          COALESCE(peak_hour, '') AS peak_hour
                   FROM driver_daily_summary
                   WHERE driver_id = ? AND summary_date = CURDATE()"#,
                (&driver_b[..],),
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
            distance_km: f32_col(&r, "distance_km")?,
            avg_rating: f32_col(&r, "avg_rating")?,
            peak_hour: from_value(col(&r, "peak_hour")?),
        }))
    }

    async fn get_order_history(
        &self,
        driver_id: &str,
        filter: OrderHistoryFilter,
    ) -> Result<(Vec<DriverOrderItem>, u32)> {
        let driver_b = ulid_to_bytes(driver_id)?;
        let mut conn = self.pool.get_conn().await?;
        let limit = filter.limit.max(1).min(100);
        let offset = filter.page.saturating_sub(1) * limit;

        let mut where_parts: Vec<String> = vec![
            "o.driver_id = ?".into(),
            "o.status IN ('completed','cancelled')".into(),
        ];
        let mut binds: Vec<mysql_async::Value> = vec![mysql_async::Value::Bytes(driver_b.to_vec())];

        if let Some(ref df) = filter.date_from {
            where_parts.push("DATE(COALESCE(o.completed_at, o.cancelled_at)) >= ?".into());
            binds.push(df.as_str().into());
        }
        if let Some(ref dt) = filter.date_to {
            where_parts.push("DATE(COALESCE(o.completed_at, o.cancelled_at)) <= ?".into());
            binds.push(dt.as_str().into());
        }
        if let Some(ref st) = filter.status {
            where_parts.push("o.status = ?".into());
            binds.push(st.as_str().into());
        }
        if let Some(ref svc) = filter.service_type {
            where_parts.push("o.service_type = ?".into());
            binds.push(svc.as_str().into());
        }

        let where_sql = where_parts.join(" AND ");

        let count_q = format!("SELECT COUNT(*) FROM orders o WHERE {}", where_sql);
        let total: u32 = conn.exec_first(&count_q, binds.clone()).await?.unwrap_or(0);

        let mut page_binds = binds;
        page_binds.push((limit as u64).into());
        page_binds.push((offset as u64).into());

        let data_q = format!(
            r#"SELECT {}
               FROM orders o
               LEFT JOIN users u ON u.id = o.rider_id
               WHERE {}
               ORDER BY COALESCE(o.completed_at, o.cancelled_at) DESC
               LIMIT ? OFFSET ?"#,
            Self::order_item_cols(),
            where_sql,
        );
        let rows: Vec<Row> = conn.exec(&data_q, page_binds).await?;
        let items = rows.iter().map(Self::row_to_item).collect::<Result<_>>()?;
        Ok((items, total))
    }

    async fn get_order_detail(
        &self,
        driver_id: &str,
        order_id: &str,
    ) -> Result<Option<DriverOrderDetail>> {
        let driver_b = ulid_to_bytes(driver_id)?;
        let order_b = ulid_to_bytes(order_id)?;
        let mut conn = self.pool.get_conn().await?;
        let row: Option<Row> = conn
            .exec_first(
                format!(
                    r#"SELECT {},
                              o.pickup_lat, o.pickup_lng, o.dest_lat, o.dest_lng,
                              COALESCE(u.phone,      '') AS rider_phone,
                              COALESCE(u.avatar_url, '') AS rider_avatar,
                              COALESCE(
                                  (SELECT AVG(r2.score) FROM ratings r2 WHERE r2.target_id = o.rider_id), 0
                              ) AS rider_rating
                       FROM orders o
                       LEFT JOIN users u ON u.id = o.rider_id
                       WHERE o.id = ? AND o.driver_id = ?"#,
                    Self::order_item_cols()
                ),
                (&order_b[..], &driver_b[..]),
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
            rider_rating: f32_col(&r, "rider_rating")?,
        }))
    }

    async fn get_earnings(
        &self,
        driver_id: &str,
        date_from: &str,
        date_to: &str,
    ) -> Result<EarningsResult> {
        let driver_b = ulid_to_bytes(driver_id)?;
        let db = &driver_b[..];
        let mut conn = self.pool.get_conn().await?;

        let agg_row: Option<Row> = conn
            .exec_first(
                r#"SELECT
                       COALESCE(SUM(COALESCE(fare_final, fare_estimate)), 0)                    AS gross_earnings,
                       COALESCE(SUM(FLOOR(COALESCE(fare_final, fare_estimate) * 0.80)), 0)      AS net_earnings,
                       COALESCE(
                           (SELECT SUM(r.tip_amount) FROM ratings r
                            INNER JOIN orders o2 ON o2.id = r.order_id
                            WHERE o2.driver_id = ?
                              AND DATE(COALESCE(o2.completed_at, o2.cancelled_at)) BETWEEN ? AND ?
                              AND r.target_id = ?), 0
                       )                                                                         AS tips,
                       COUNT(CASE WHEN status = 'completed' THEN 1 END)                         AS trip_count,
                       COUNT(CASE WHEN status = 'cancelled' THEN 1 END)                         AS cancel_count,
                       COALESCE(SUM(CASE WHEN status = 'completed' THEN distance_km ELSE 0 END), 0) AS distance_km
                   FROM orders
                   WHERE driver_id = ?
                     AND DATE(COALESCE(completed_at, cancelled_at)) BETWEEN ? AND ?"#,
                (db, date_from, date_to, db, db, date_from, date_to),
            )
            .await?;

        let (gross_earnings, net_earnings, tips, trip_count, cancel_count, distance_km) =
            match agg_row {
                Some(r) => (
                    from_value::<i64>(col(&r, "gross_earnings")?),
                    from_value::<i64>(col(&r, "net_earnings")?),
                    from_value::<i64>(col(&r, "tips")?),
                    from_value::<i32>(col(&r, "trip_count")?),
                    from_value::<i32>(col(&r, "cancel_count")?),
                    f32_col(&r, "distance_km")?,
                ),
                None => (0, 0, 0, 0, 0, 0.0),
            };

        let online_minutes: i64 = conn
            .exec_first(
                "SELECT COALESCE(SUM(online_minutes), 0) FROM driver_daily_summary WHERE driver_id = ? AND summary_date BETWEEN ? AND ?",
                (db, date_from, date_to),
            )
            .await?
            .unwrap_or(0);

        let daily_rows: Vec<Row> = conn
            .exec(
                r#"SELECT
                       DATE(COALESCE(completed_at, cancelled_at))                               AS date,
                       COALESCE(SUM(FLOOR(COALESCE(fare_final, fare_estimate) * 0.80)), 0)      AS net_earnings,
                       COUNT(CASE WHEN status = 'completed' THEN 1 END)                         AS trip_count,
                       COALESCE(SUM(CASE WHEN status = 'completed' THEN distance_km ELSE 0 END), 0) AS distance_km
                   FROM orders
                   WHERE driver_id = ?
                     AND DATE(COALESCE(completed_at, cancelled_at)) BETWEEN ? AND ?
                   GROUP BY DATE(COALESCE(completed_at, cancelled_at))
                   ORDER BY DATE(COALESCE(completed_at, cancelled_at)) ASC"#,
                (db, date_from, date_to),
            )
            .await?;

        let daily = daily_rows
            .iter()
            .map(|r| -> Result<DailyEarningRow> {
                Ok(DailyEarningRow {
                    date: {
                        let val = col(r, "date")?.clone();
                        // mysql_async Date type tidak bisa langsung ke String
                        // convert via chrono NaiveDate atau manual
                        match val {
                            mysql_async::Value::Date(y, m, d, ..) => {
                                format!("{:04}-{:02}-{:02}", y, m, d)
                            }
                            _ => from_value::<String>(val),
                        }
                    },
                    net_earnings: from_value(col(r, "net_earnings")?),
                    trip_count: from_value::<i64>(col(r, "trip_count")?) as i32,
                    distance_km: f32_col(r, "distance_km")?,
                })
            })
            .collect::<Result<_>>()?;

        Ok(EarningsResult {
            gross_earnings,
            net_earnings,
            tips,
            trip_count,
            cancel_count,
            distance_km,
            online_minutes,
            daily,
        })
    }
}
