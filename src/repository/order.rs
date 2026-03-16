use anyhow::{Ok, Result};
use async_trait::async_trait;
use mysql_async::{from_value, params, prelude::Queryable, Pool, Row};
use uuid::Uuid;

use super::db::{col, col_opt_f32, col_opt_i32, col_opt_str, exec_drop, exec_rows};
use crate::models::order::Order;

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
    async fn find_active_between(
        &self,
        rider_id: &str,
        driver_id: &str,
    ) -> anyhow::Result<Option<Order>>;
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
    async fn get_by_user_id(&self, user_id: &str) -> Result<Vec<Order>>;
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
        r#"o.id, o.rider_id, o.driver_id, o.status,
           o.pickup_lat, o.pickup_lng, o.pickup_address,
           o.dest_lat, o.dest_lng, o.dest_address,
           o.distance_km, o.fare_estimate, o.fare_final, o.service_type,
           DATE_FORMAT(CONVERT_TZ(o.created_at,'+00:00','+00:00'),'%Y-%m-%dT%H:%i:%sZ') AS created_at_fmt, u.name as rider_name"#
    }

    fn row_to_order(row: &Row) -> Result<Order> {
        Ok(Order {
            id: from_value(col(row, "id")?),
            rider_id: from_value(col(row, "rider_id")?),
            driver_id: col_opt_str(row, "driver_id"),
            status: from_value(col(row, "status")?),
            pickup_lat: {
                let v: f64 = from_value(col(row, "pickup_lat")?);
                v
            },
            pickup_lng: {
                let v: f64 = from_value(col(row, "pickup_lng")?);
                v
            },
            pickup_address: from_value(col(row, "pickup_address")?),
            dest_lat: {
                let v: f64 = from_value(col(row, "dest_lat")?);
                v
            },
            dest_lng: {
                let v: f64 = from_value(col(row, "dest_lng")?);
                v
            },
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
        let id = Uuid::new_v4().to_string();

        exec_drop(
            &self.pool,
            r#"INSERT INTO orders
               (id, rider_id, status, pickup_lat, pickup_lng, pickup_address,
                dest_lat, dest_lng, dest_address, fare_estimate, service_type)
               VALUES (?, ?, 'searching', ?, ?, ?, ?, ?, ?, ?, ?)"#,
            (
                &id,
                &o.rider_id,
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
        match self.find_by_id(&id).await? {
            Some(o) => {
                return Ok(o);
            }
            None => anyhow::bail!("Failed to create order"),
        }
    }

    async fn find_active_between(
        &self,
        rider_id: &str,
        driver_id: &str,
    ) -> anyhow::Result<Option<Order>> {
        let q = format!(
            "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id
         WHERE o.rider_id = :rider_id 
           AND o.driver_id = :driver_id 
           AND o.status IN ('driver_accepted', 'driver_arrived', 'on_trip') 
         LIMIT 1",
            Self::order_cols()
        );

        let mut conn = self.pool.get_conn().await?;

        let row: Option<mysql_async::Row> = conn
            .exec_first(
                &q,
                params! {
                    "rider_id" => rider_id,
                    "driver_id" => driver_id,
                },
            )
            .await?;

        row.map(|r| Self::row_to_order(&r)).transpose()
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<Order>> {
        let q = format!(
            "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id WHERE o.id = ?",
            Self::order_cols()
        );
        let rows = exec_rows(&self.pool, &q, (id,)).await?;
        rows.into_iter()
            .next()
            .map(|r| Self::row_to_order(&r))
            .transpose()
    }

    async fn find_active_for_rider(&self, rider_id: &str) -> Result<Option<Order>> {
        let mut conn = self.pool.get_conn().await?;
        let row: Option<mysql_async::Row> = conn
            .exec_first(
                format!(
                    "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id WHERE o.rider_id = ? AND o.status NOT IN ('completed', 'cancelled', 'searching')",
                    Self::order_cols()
                ),
                (rider_id,),
            )
            .await?;
        row.map(|r| Self::row_to_order(&r)).transpose()
    }

    async fn find_active_for_driver(&self, driver_id: &str) -> Result<Option<Order>> {
        let mut conn = self.pool.get_conn().await?;
        let row: Option<mysql_async::Row> = conn
            .exec_first(
             format!("SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id WHERE o.driver_id = ? AND o.status NOT IN ('completed', 'cancelled', 'searching')", Self::order_cols()),
                (driver_id,),
            )
            .await?;
        row.map(|r| Self::row_to_order(&r)).transpose()
    }
    async fn assign_driver_atomic(&self, order_id: &str, driver_id: &str) -> Result<u64> {
        let mut conn = self.pool.get_conn().await?;
        let result = conn
            .exec_iter(
                "UPDATE orders 
             SET driver_id = ?, status = 'driver_accepted', accepted_at = NOW(3) 
             WHERE id = ? AND status = 'searching'",
                (driver_id, order_id),
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
        let q = format!(
            r#"SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id
               WHERE o.status = 'searching'
               AND o.service_type = ?
               AND (
                 6371 * 2 * ASIN(SQRT(
                   POWER(SIN(RADIANS(? - o.pickup_lat) / 2), 2) +
                   COS(RADIANS(?)) * COS(RADIANS(o.pickup_lat)) *
                   POWER(SIN(RADIANS(? - o.pickup_lng) / 2), 2)
                 ))
               ) <= ?
               ORDER BY o.created_at ASC"#,
            Self::order_cols()
        );
        let rows = exec_rows(&self.pool, &q, (service_type, lat, lat, lng, radius_km)).await?;
        rows.into_iter().map(|r| Self::row_to_order(&r)).collect()
    }

    async fn assign_driver(&self, order_id: &str, driver_id: &str) -> Result<()> {
        exec_drop(
            &self.pool,
            "UPDATE orders SET driver_id = ?, status = 'driver_found', driver_found_at = NOW(3) WHERE id = ?",
            (driver_id, order_id),
        )
        .await?;
        Ok(())
    }

    async fn update_status(&self, order_id: &str, status: &str) -> Result<()> {
        let q = match status {
            "rider_accepted" => "UPDATE orders SET status = ?, accepted_at = NOW(3) WHERE id = ?",
            "driver_accepted" => "UPDATE orders SET status = ?, accepted_at = NOW(3) WHERE id = ?",
            "driver_arrived" => "UPDATE orders SET status = ?, arrived_at = NOW(3) WHERE id = ?",
            "on_trip" => "UPDATE orders SET status = ?, started_at = NOW(3) WHERE id = ?",
            _ => "UPDATE orders SET status = ? WHERE id = ?",
        };
        exec_drop(&self.pool, q, (status, order_id)).await?;
        Ok(())
    }

    async fn complete(&self, order_id: &str, distance_km: f32, fare_final: i32) -> Result<()> {
        exec_drop(
            &self.pool,
            "UPDATE orders SET status = 'completed', completed_at = NOW(3), distance_km = ?, fare_final = ? WHERE id = ?",
            (distance_km, fare_final, order_id),
        )
        .await?;
        Ok(())
    }

    async fn cancel(&self, order_id: &str, reason: Option<&str>) -> Result<()> {
        exec_drop(
            &self.pool,
            "UPDATE orders SET status = 'cancelled', cancelled_at = NOW(3), cancel_reason = ? WHERE id = ?",
            (reason, order_id),
        )
        .await?;
        Ok(())
    }

    async fn get_by_user_id(&self, user_id: &str) -> Result<Vec<Order>> {
        let q = format!(
            "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id
         WHERE o.rider_id = ?",
            Self::order_cols()
        );
        let rows: Vec<Row> = exec_rows(&self.pool, &q, (user_id,)).await?;

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
        let id = Uuid::new_v4().to_string();
        let q = r#"INSERT INTO ratings
(id, order_id, poster_id, target_id, score, comment, tip_amount, created_at)
VALUES(?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))"#;
        exec_drop(
            &self.pool,
            q,
            (
                id, order_id, poster_id, target_id, rating, comment, tip_amount,
            ),
        )
        .await?;
        Ok(())
    }
}
