use anyhow::{Ok, Result};
use async_trait::async_trait;
use deadpool_postgres::Pool;
use tokio_postgres::Row;

use super::db::{col_opt_f32, col_opt_i32, exec_drop, exec_first, exec_rows, get_conn};
use crate::models::order::Order;
use crate::utils::ulid::{bin_to_ulid, bin_to_ulid_opt, id_to_vec, new_ulid, ulid_to_vec};

#[async_trait]
pub trait OrderRepository: Send + Sync {
    async fn create(&self, o: &NewOrder) -> Result<Order>;
    async fn find_by_id(&self, id: &str) -> Result<Option<Order>>;
    async fn find_active_for_rider(&self, rider_id: &str) -> Result<Option<Order>>;
    async fn find_active_for_driver(&self, driver_id: &str) -> Result<Option<Order>>;
    async fn find_active_between(&self, rider_id: &str, driver_id: &str) -> Result<Option<Order>>;
    async fn get_nearby_searching(
        &self,
        service_type: &str,
        lat: f64,
        lng: f64,
        radius_km: f64,
    ) -> Result<Vec<Order>>;
    async fn assign_driver(&self, order_id: &str, driver_id: &str) -> Result<()>;
    async fn assign_driver_atomic(&self, order_id: &str, driver_id: &str) -> Result<u64>;
    async fn update_status(&self, order_id: &str, status: &str) -> Result<()>;
    async fn complete(&self, order_id: &str, distance_km: f32, fare_final: i32) -> Result<()>;
    async fn cancel(&self, order_id: &str, reason: Option<&str>) -> Result<()>;
    async fn submit_rating(
        &self,
        order_id: &str,
        poster_id: &str,
        target_id: &str,
        rating: u8,
        tip_amount: f64,
        comment: &str,
    ) -> Result<()>;
    async fn get_by_user_id(&self, user_id: &str, limit: i64) -> Result<Vec<Order>>;
}

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

#[derive(Clone)]
pub struct PgOrderRepository {
    pool: Pool,
}

impl PgOrderRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    // Ambil BYTEA langsung — decode ke ULID via bin_to_ulid di row_to_order.
    // Tidak pakai encode(id,'hex') di SQL.
    fn order_cols() -> &'static str {
        r#"o.id,
       o.rider_id,
       o.driver_id,
       o.status,
       o.pickup_lat::FLOAT8 AS pickup_lat, o.pickup_lng::FLOAT8 AS pickup_lng, o.pickup_address,
       o.dest_lat::FLOAT8   AS dest_lat,   o.dest_lng::FLOAT8   AS dest_lng,   o.dest_address,
       o.distance_km::FLOAT8 AS distance_km,
       o.fare_estimate, o.fare_final,
       o.service_type::TEXT AS service_type,
       o.created_at AS created_at,
       u.name AS rider_name, u2.name AS driver_name"#
    }

    fn row_to_order(row: &Row) -> Result<Order> {
        let id_bytes: Vec<u8> = row.try_get("id")?;
        let rider_bytes: Vec<u8> = row.try_get("rider_id")?;
        let driver_bytes: Option<Vec<u8>> = row.try_get("driver_id")?;

        Ok(Order {
            id: bin_to_ulid(id_bytes)?,
            rider_id: bin_to_ulid(rider_bytes)?,
            driver_id: bin_to_ulid_opt(driver_bytes)?,
            status: row.try_get("status")?,
            pickup_lat: row.try_get("pickup_lat")?,
            pickup_lng: row.try_get("pickup_lng")?,
            pickup_address: row.try_get("pickup_address")?,
            dest_lat: row.try_get("dest_lat")?,
            dest_lng: row.try_get("dest_lng")?,
            dest_address: row.try_get("dest_address")?,
            distance_km: col_opt_f32(row, "distance_km"),
            fare_estimate: row.try_get("fare_estimate")?,
            fare_final: col_opt_i32(row, "fare_final"),
            service_type: row.try_get("service_type")?,
            created_at: row.try_get("created_at")?,
            rider_name: row.try_get("rider_name")?,
            driver_name: row.try_get("driver_name")?,
        })
    }
}

#[async_trait]
impl OrderRepository for PgOrderRepository {
    async fn create(&self, o: &NewOrder) -> Result<Order> {
        let id = new_ulid();
        let id_b = ulid_to_vec(&id)?;

        let rider_bytes = id_to_vec(&o.rider_id)?;
        tracing::info!(
            "Inserting new order into database for user {} (rider_id={:?})",
            o.pickup_address,
            &rider_bytes,
        );

        let conn = get_conn(&self.pool).await?;

        let rows_affected = conn
            .execute(
                r#"INSERT INTO orders
               (id, rider_id, status, pickup_lat, pickup_lng, pickup_address,
                dest_lat, dest_lng, dest_address, fare_estimate, service_type)
               VALUES ($1, $2, 'searching',
                       $3::FLOAT8::NUMERIC, $4::FLOAT8::NUMERIC, $5,
                       $6::FLOAT8::NUMERIC, $7::FLOAT8::NUMERIC, $8,
                       $9, $10::text::vehicle_type)"#,
                &[
                    &id_b,
                    &rider_bytes,
                    &o.pickup_lat,
                    &o.pickup_lng,
                    &o.pickup_address.as_str(),
                    &o.dest_lat,
                    &o.dest_lng,
                    &o.dest_address.as_str(),
                    &o.fare_estimate,
                    &o.service_type.as_str(),
                ],
            )
            .await?;

        anyhow::ensure!(rows_affected == 1, "Expected 1 row inserted");

        self.find_by_id(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Order created but not found: {}", id))
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<Order>> {
        let id_b = id_to_vec(id)?;
        let q = format!(
            "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id LEFT JOIN users u2 ON o.driver_id = u2.id WHERE o.id = $1",
            Self::order_cols()
        );
        exec_first(&self.pool, &q, &[&id_b])
            .await?
            .map(|r| Self::row_to_order(&r))
            .transpose()
    }

    async fn find_active_for_rider(&self, rider_id: &str) -> Result<Option<Order>> {
        let rider_bytes = id_to_vec(rider_id)?;
        let q = format!(
            "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id
            LEFT JOIN users u2 ON o.driver_id = u2.id
            WHERE o.rider_id = $1 AND o.status NOT IN ('cancelled','completed') LIMIT 1",
            Self::order_cols()
        );
        exec_first(&self.pool, &q, &[&rider_bytes])
            .await?
            .map(|r| Self::row_to_order(&r))
            .transpose()
    }

    async fn find_active_for_driver(&self, driver_id: &str) -> Result<Option<Order>> {
        let driver_bytes = id_to_vec(driver_id)?;
        let q = format!(
            "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id
            LEFT JOIN users u2 ON o.driver_id = u2.id
            WHERE o.driver_id = $1 AND o.status NOT IN ('cancelled','completed') LIMIT 1",
            Self::order_cols()
        );
        exec_first(&self.pool, &q, &[&driver_bytes])
            .await?
            .map(|r| Self::row_to_order(&r))
            .transpose()
    }

    async fn find_active_between(&self, rider_id: &str, driver_id: &str) -> Result<Option<Order>> {
        let rider_bytes = id_to_vec(rider_id)?;
        let driver_bytes = id_to_vec(driver_id)?;
        let q = format!(
            "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id
             LEFT JOIN users u2 ON o.driver_id = u2.id
             WHERE o.rider_id = $1 AND o.driver_id = $2
            AND o.status IN ('driver_accepted','driver_arrived','on_trip') LIMIT 1",
            Self::order_cols()
        );
        exec_first(&self.pool, &q, &[&rider_bytes, &driver_bytes])
            .await?
            .map(|r| Self::row_to_order(&r))
            .transpose()
    }

    async fn get_nearby_searching(
        &self,
        service_type: &str,
        lat: f64,
        lng: f64,
        radius_km: f64,
    ) -> Result<Vec<Order>> {
        if service_type.is_empty() {
            anyhow::bail!("service_type tidak boleh kosong");
        }
        if service_type != "motor" && service_type != "mobil" {
            anyhow::bail!("service_type tidak boleh selain motor mobil");
        }
        let lat_delta = radius_km / 111.0;
        let lng_delta = radius_km / (111.0 * lat.to_radians().cos());
        let q = format!(
            r#"SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id
                LEFT JOIN users u2 ON o.driver_id = u2.id
               WHERE o.status = 'searching'
                 AND o.service_type::TEXT = $1 
                 AND o.pickup_lat::FLOAT8 BETWEEN $2 AND $3
                 AND o.pickup_lng::FLOAT8 BETWEEN $4 AND $5
                 AND (6371 * 2 * asin(sqrt(
                       power(sin(radians($6 - o.pickup_lat::FLOAT8) / 2), 2) +
                       cos(radians($7)) * cos(radians(o.pickup_lat::FLOAT8)) *
                       power(sin(radians($8 - o.pickup_lng::FLOAT8) / 2), 2)
                     ))) <= $9
               ORDER BY o.created_at ASC"#,
            Self::order_cols()
        );
        let rows = exec_rows(
            &self.pool,
            &q,
            &[
                &service_type,
                &(lat - lat_delta),
                &(lat + lat_delta),
                &(lng - lng_delta),
                &(lng + lng_delta),
                &lat,
                &lat,
                &lng,
                &radius_km,
            ],
        )
        .await?;
        rows.iter().map(Self::row_to_order).collect()
    }

    async fn assign_driver(&self, order_id: &str, driver_id: &str) -> Result<()> {
        let order_b = id_to_vec(order_id)?;
        let driver_b = id_to_vec(driver_id)?;
        exec_drop(&self.pool,
            "UPDATE orders SET driver_id=$1, status='driver_found', driver_found_at=NOW() WHERE id=$2",
            &[&driver_b, &order_b]).await
    }

    async fn assign_driver_atomic(&self, order_id: &str, driver_id: &str) -> Result<u64> {
        let order_b = id_to_vec(order_id)?;
        let driver_b = id_to_vec(driver_id)?;
        let conn = get_conn(&self.pool).await?;
        let n = conn
            .execute(
                "UPDATE orders SET driver_id=$1, status='driver_accepted', accepted_at=NOW()
             WHERE id=$2 AND status='searching'",
                &[&driver_b, &order_b],
            )
            .await?;
        Ok(n)
    }

    async fn update_status(&self, order_id: &str, status: &str) -> Result<()> {
        let order_b = id_to_vec(order_id)?;
        let q = match status {
            "rider_accepted" | "driver_accepted" => {
                "UPDATE orders SET status=$1, accepted_at=NOW() WHERE id=$2"
            }
            "driver_arrived" => "UPDATE orders SET status=$1, arrived_at=NOW()  WHERE id=$2",
            "on_trip" => "UPDATE orders SET status=$1, started_at=NOW()  WHERE id=$2",
            _ => "UPDATE orders SET status=$1 WHERE id=$2",
        };
        exec_drop(&self.pool, q, &[&status, &order_b]).await
    }

    async fn complete(&self, order_id: &str, distance_km: f32, fare_final: i32) -> Result<()> {
        let order_b = id_to_vec(order_id)?;
        let dist_f64 = distance_km as f64;
        exec_drop(&self.pool,
            "UPDATE orders SET status='completed', completed_at=NOW(), distance_km=$1::FLOAT8::NUMERIC, fare_final=$2 WHERE id=$3",
            &[&dist_f64, &fare_final, &order_b]).await
    }

    async fn cancel(&self, order_id: &str, reason: Option<&str>) -> Result<()> {
        let order_b = id_to_vec(order_id)?;
        exec_drop(&self.pool,
            "UPDATE orders SET status='cancelled', cancelled_at=NOW(), cancel_reason=$1 WHERE id=$2",
            &[&reason, &order_b]).await
    }

    async fn get_by_user_id(&self, user_id: &str, limit: i64) -> Result<Vec<Order>> {
        let user_b = id_to_vec(user_id)?;
        let q = format!(
            "SELECT {} FROM orders o JOIN users u ON o.rider_id = u.id
            LEFT JOIN users u2 ON o.driver_id = u2.id
            WHERE o.rider_id = $1 ORDER BY o.created_at DESC LIMIT $2",
            Self::order_cols()
        );
        let rows = exec_rows(&self.pool, &q, &[&user_b, &limit]).await?;
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
        let id_b = ulid_to_vec(&new_ulid())?;
        let order_b = id_to_vec(order_id)?;
        let poster_b = id_to_vec(poster_id)?;
        let target_b = id_to_vec(target_id)?;
        let score = rating as i16; // SMALLINT
        let tip_i32 = tip_amount as i32; // INTEGER
        exec_drop(
            &self.pool,
            "INSERT INTO ratings (id, order_id, poster_id, target_id, score, comment, tip_amount)
             VALUES ($1,$2,$3,$4,$5,$6,$7)",
            &[
                &id_b, &order_b, &poster_b, &target_b, &score, &comment, &tip_i32,
            ],
        )
        .await
    }
}
