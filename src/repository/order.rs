use anyhow::{Ok, Result};
use async_trait::async_trait;
use mysql_async::{from_value, params, prelude::Queryable, Pool, Row};

use super::db::{col, col_opt_f32, col_opt_i32, exec_drop, exec_rows};
use crate::models::order::Order;
use crate::utils::ulid::{bin_to_ulid, bin_to_ulid_opt, new_ulid, ulid_to_bytes};

// 0 = searching
// 1 = driver_accepted
// 2 = driver_arrived
// 3 = on_trip
// 4 = completed
// 5 = cancelled

// ── Trait ─────────────────────────────────────────────────────────────────────

#[async_trait]
pub trait OrderRepository: Send + Sync {
    async fn create(&self, o: &NewOrder) -> Result<Order>;
    async fn find_by_id(&self, id: &str) -> Result<Option<Order>>;
    async fn find_active_for_rider(&self, rider_id: &str) -> Result<Option<Order>>;
    async fn get_nearby_searching(
        &self,
        service_type: &str,
        lat: f64,
        lng: f64,
        radius_km: f64,
    ) -> Result<Vec<Order>>;
    async fn assign_driver(&self, order_id: &str, driver_id: &str) -> Result<()>;
    async fn update_status(&self, order_id: &str, status: &str) -> Result<()>;
    async fn complete(&self, order_id: &str, distance_km: f32, fare_final: i32) -> Result<()>;
    async fn cancel(&self, order_id: &str, reason: Option<&str>) -> Result<()>;
    async fn find_active_between(&self, rider_id: &str, driver_id: &str) -> Result<Option<Order>>;
    async fn assign_driver_atomic(&self, order_id: &str, driver_id: &str) -> Result<u64>;
    async fn submit_rating(
        &self,
        order_id: &str,
        poster_id: &str,
        target_id: &str,
        rating: u8,
        tip_amount: f64,
        comment: &str,
    ) -> Result<()>;
    async fn find_active_for_driver(&self, driver_id: &str) -> Result<Option<Order>>;
    async fn get_by_user_id(&self, user_id: &str, limit: i64) -> Result<Vec<Order>>;
}

/// DTO untuk membuat order baru
#[derive(Debug, Clone)]
pub struct NewOrder {
    pub rider_id: String,
    pub pickup_lat: f64,
    pub pickup_lng: f64,
    pub pickup_address: String,
    pub dest_lat: f64,
    pub dest_lng: f64,
    pub dest_address: String,
    pub fare_estimate: i32,
    pub service_type: String,
}

// ── MySQL implementasi ────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct MySqlOrderRepository {
    pool: Pool,
}

impl MySqlOrderRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn order_cols() -> &'static str {
        // Kolom BINARY(16) dibaca as-is — konversi ke ULID string di row_to_order
        r#"o.id, o.rider_id, o.driver_id, o.status,
           o.pickup_lat, o.pickup_lng, o.pickup_address,
           o.dest_lat, o.dest_lng, o.dest_address,
           o.distance_km, o.fare_estimate, o.fare_final, o.service_type,
           DATE_FORMAT(CONVERT_TZ(o.created_at,'+00:00','+00:00'),'%Y-%m-%dT%H:%i:%sZ') AS created_at_fmt,
           u.name AS rider_name"#
    }

    fn row_to_order(row: &Row) -> Result<Order> {
        Ok(Order {
            id: bin_to_ulid(from_value(col(row, "id")?))?,
            rider_id: bin_to_ulid(from_value(col(row, "rider_id")?))?,
            driver_id: bin_to_ulid_opt(col(row, "driver_id")?)?,
            status: from_value(col(row, "status")?),
            pickup_lat: from_value(col(row, "pickup_lat")?),
            pickup_lng: from_value(col(row, "pickup_lng")?),
            pickup_address: from_value(col(row, "pickup_address")?),
            dest_lat: from_value(col(row, "dest_lat")?),
            dest_lng: from_value(col(row, "dest_lng")?),
            dest_address: from_value(col(row, "dest_address")?),
            distance_km: col_opt_f32(row, "distance_km"),
            fare_estimate: from_value(col(row, "fare_estimate")?),
            fare_final: col_opt_i32(row, "fare_final"),
            service_type: from_value(col(row, "service_type")?),
            created_at: from_value(col(row, "created_at_fmt")?),
            rider_name: from_value(col(row, "rider_name")?),
        })
    }
}

#[async_trait]
impl OrderRepository for MySqlOrderRepository {
    async fn create(&self, o: &NewOrder) -> Result<Order> {
        let id = new_ulid();
        let id_b = ulid_to_bytes(&id)?;
        let rider_b = ulid_to_bytes(&o.rider_id)?;

        exec_drop(
            &self.pool,
            r#"INSERT INTO orders
               (id, rider_id, status, pickup_lat, pickup_lng, pickup_address,
                dest_lat, dest_lng, dest_address, fare_estimate, service_type)
               VALUES (?, ?, 'searching', ?, ?, ?, ?, ?, ?, ?, ?)"#,
            (
                &id_b[..],
                &rider_b[..],
                o.pickup_lat,
                o.pickup_lng,
                o.pickup_address.as_str(),
                o.dest_lat,
                o.dest_lng,
                o.dest_address.as_str(),
                o.fare_estimate,
                o.service_type.as_str(),
            ),
        )
        .await?;

        self.find_by_id(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Failed to create order"))
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<Order>> {
        let id_b = ulid_to_bytes(id)?;
        let q = format!(
            "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id WHERE o.id = ?",
            Self::order_cols()
        );
        let rows = exec_rows(&self.pool, &q, (&id_b[..],)).await?;
        rows.into_iter()
            .next()
            .map(|r| Self::row_to_order(&r))
            .transpose()
    }

    async fn find_active_for_rider(&self, rider_id: &str) -> Result<Option<Order>> {
        let rider_b = ulid_to_bytes(rider_id)?;
        let mut conn = self.pool.get_conn().await?;
        let row: Option<Row> = conn
            .exec_first(
                format!(
                    // IN lebih optimal dari NOT IN — cardinality jelas, optimizer bisa pakai idx_rider_status
                    "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id 
                    WHERE o.rider_id = ? AND o.status NOT IN ('cancelled', 'completed')",
                    Self::order_cols()
                ),
                (&rider_b[..],),
            )
            .await?;
        row.map(|r| Self::row_to_order(&r)).transpose()
    }

    async fn find_active_for_driver(&self, driver_id: &str) -> Result<Option<Order>> {
        let driver_b = ulid_to_bytes(driver_id)?;
        let mut conn = self.pool.get_conn().await?;
        let row: Option<Row> = conn
            .exec_first(
                format!(
                    "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id
                     WHERE o.driver_id = ? AND o.status NOT IN ('cancelled', 'completed')",
                    Self::order_cols()
                ),
                (&driver_b[..],),
            )
            .await?;
        row.map(|r| Self::row_to_order(&r)).transpose()
    }

    async fn find_active_between(&self, rider_id: &str, driver_id: &str) -> Result<Option<Order>> {
        let rider_b = ulid_to_bytes(rider_id)?;
        let driver_b = ulid_to_bytes(driver_id)?;

        let q = format!(
            "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id
             WHERE o.rider_id  = :rider_id
               AND o.driver_id = :driver_id
               AND o.status IN ('driver_accepted','driver_arrived','on_trip')
             LIMIT 1",
            Self::order_cols()
        );

        let mut conn = self.pool.get_conn().await?;
        let row: Option<Row> = conn
            .exec_first(
                &q,
                params! {
                    "rider_id"  => &rider_b[..],
                    "driver_id" => &driver_b[..],
                },
            )
            .await?;
        row.map(|r| Self::row_to_order(&r)).transpose()
    }

    async fn assign_driver_atomic(&self, order_id: &str, driver_id: &str) -> Result<u64> {
        let order_b = ulid_to_bytes(order_id)?;
        let driver_b = ulid_to_bytes(driver_id)?;
        let mut conn = self.pool.get_conn().await?;
        let result = conn
            .exec_iter(
                "UPDATE orders
                 SET driver_id = ?, status = 'driver_accepted', accepted_at = NOW(3)
                 WHERE id = ? AND status = 'searching'",
                (&driver_b[..], &order_b[..]),
            )
            .await?;
        Ok(result.affected_rows())
    }

    async fn get_nearby_searching(
        &self,
        service_type: &str,
        lat: f64,
        lng: f64,
        radius_km: f64,
    ) -> Result<Vec<Order>> {
        // Bounding box dulu → index idx_search_geo bisa dipakai
        // Baru haversine untuk presisi — jauh lebih cepat dari full scan
        let lat_delta = radius_km / 111.0; // 1 derajat lat ≈ 111 km
        let lng_delta = radius_km / (111.0 * lat.to_radians().cos());

        let q = format!(
            r#"SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id
               WHERE o.status       = 'searching'
                 AND o.service_type = ?
                 AND o.pickup_lat BETWEEN ? AND ?
                 AND o.pickup_lng BETWEEN ? AND ?
                 AND (6371 * 2 * ASIN(SQRT(
                       POWER(SIN(RADIANS(? - o.pickup_lat) / 2), 2) +
                       COS(RADIANS(?)) * COS(RADIANS(o.pickup_lat)) *
                       POWER(SIN(RADIANS(? - o.pickup_lng) / 2), 2)
                     ))) <= ?
               ORDER BY o.created_at ASC"#,
            Self::order_cols()
        );
        let rows = exec_rows(
            &self.pool,
            &q,
            (
                service_type,
                lat - lat_delta,
                lat + lat_delta, // BETWEEN pickup_lat
                lng - lng_delta,
                lng + lng_delta, // BETWEEN pickup_lng
                lat,
                lat,
                lng, // haversine args
                radius_km,
            ),
        )
        .await?;
        rows.into_iter().map(|r| Self::row_to_order(&r)).collect()
    }

    async fn assign_driver(&self, order_id: &str, driver_id: &str) -> Result<()> {
        let order_b = ulid_to_bytes(order_id)?;
        let driver_b = ulid_to_bytes(driver_id)?;
        exec_drop(
            &self.pool,
            "UPDATE orders SET driver_id = ?, status = 'driver_found', driver_found_at = NOW(3) WHERE id = ?",
            (&driver_b[..], &order_b[..]),
        )
        .await
    }

    async fn update_status(&self, order_id: &str, status: &str) -> Result<()> {
        let order_b = ulid_to_bytes(order_id)?;
        let q = match status {
            "rider_accepted" => "UPDATE orders SET status = ?, accepted_at = NOW(3) WHERE id = ?",
            "driver_accepted" => "UPDATE orders SET status = ?, accepted_at = NOW(3) WHERE id = ?",
            "driver_arrived" => "UPDATE orders SET status = ?, arrived_at  = NOW(3) WHERE id = ?",
            "on_trip" => "UPDATE orders SET status = ?, started_at  = NOW(3) WHERE id = ?",
            _ => "UPDATE orders SET status = ? WHERE id = ?",
        };
        exec_drop(&self.pool, q, (status, &order_b[..])).await
    }

    async fn complete(&self, order_id: &str, distance_km: f32, fare_final: i32) -> Result<()> {
        let order_b = ulid_to_bytes(order_id)?;
        exec_drop(
            &self.pool,
            "UPDATE orders SET status = 'completed', completed_at = NOW(3), distance_km = ?, fare_final = ? WHERE id = ?",
            (distance_km, fare_final, &order_b[..]),
        )
        .await
    }

    async fn cancel(&self, order_id: &str, reason: Option<&str>) -> Result<()> {
        let order_b = ulid_to_bytes(order_id)?;
        exec_drop(
            &self.pool,
            "UPDATE orders SET status = 'cancelled', cancelled_at = NOW(3), cancel_reason = ? WHERE id = ?",
            (reason, &order_b[..]),
        )
        .await
    }

    async fn get_by_user_id(&self, user_id: &str, limit: i64) -> Result<Vec<Order>> {
        let user_b = ulid_to_bytes(user_id)?;
        // ORDER BY created_at DESC + LIMIT → pakai idx_rider_created, no filesort
        let q = format!(
            "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id
             WHERE o.rider_id = ?
             ORDER BY o.created_at DESC
             LIMIT ?",
            Self::order_cols()
        );
        let rows: Vec<Row> = exec_rows(&self.pool, &q, (&user_b[..], limit)).await?;
        rows.iter().map(Self::row_to_order).collect()
    }

    async fn submit_rating(
        &self,
        order_id: &str,
        poster_id: &str,
        target_id: &str,
        rating: u8,
        tip_amount: f64,
        comment: &str,
    ) -> Result<()> {
        let id = new_ulid();
        let id_b = ulid_to_bytes(&id)?;
        let order_b = ulid_to_bytes(order_id)?;
        let poster_b = ulid_to_bytes(poster_id)?;
        let target_b = ulid_to_bytes(target_id)?;

        exec_drop(
            &self.pool,
            r#"INSERT INTO ratings
               (id, order_id, poster_id, target_id, score, comment, tip_amount, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))"#,
            (
                &id_b[..],
                &order_b[..],
                &poster_b[..],
                &target_b[..],
                rating,
                comment,
                tip_amount,
            ),
        )
        .await
    }
}
