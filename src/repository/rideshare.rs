use anyhow::Result;
use async_trait::async_trait;
use deadpool_postgres::Pool;
use tokio_postgres::Row;

use super::db::{col_opt_f64, col_opt_i32, col_opt_str, exec_drop, exec_rows};
use crate::utils::ulid::{bin_to_ulid, id_to_vec, new_ulid, ulid_to_vec};

#[derive(Debug, Clone)]
pub struct RideshareTrip {
    pub id: String,
    pub host_order_id: String,
    pub driver_id: String,
    pub service_type: String,
    pub route_start_lat: f64,
    pub route_start_lng: f64,
    pub route_end_lat: f64,
    pub route_end_lng: f64,
    pub route_polyline: Option<String>,
    pub max_passengers: i32,
    pub current_passengers: i32,
    pub status: String,
    pub join_deadline: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct RidesharePassenger {
    pub id: String,
    pub trip_id: String,
    pub rider_id: String,
    pub pickup_lat: f64,
    pub pickup_lng: f64,
    pub pickup_address: String,
    pub dest_lat: f64,
    pub dest_lng: f64,
    pub dest_address: String,
    pub fare_estimate: i32,
    pub fare_final: Option<i32>,
    pub distance_km: Option<f64>,
    pub status: String,
    pub requested_at: String,
    pub accepted_at: Option<String>,
    pub picked_up_at: Option<String>,
    pub dropped_off_at: Option<String>,
    pub cancelled_at: Option<String>,
    pub cancel_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RideshareOpenItem {
    pub id: String,
    pub host_order_id: String,
    pub driver_id: String,
    pub service_type: String,
    pub route_start_lat: f64,
    pub route_start_lng: f64,
    pub route_end_lat: f64,
    pub route_end_lng: f64,
    pub route_polyline: Option<String>,
    pub seats_available: i64,
    pub join_deadline: Option<String>,
    pub driver_name: String,
    pub driver_avatar: Option<String>,
    pub vehicle_plate: String,
    pub vehicle_model: String,
    pub vehicle_color: String,
    pub driver_rating: f64,
}

#[async_trait]
pub trait RideshareRepositoryTrait: Send + Sync {
    async fn create_trip(
        &self,
        host_order_id: &str,
        driver_id: &str,
        service_type: &str,
        start_lat: f64,
        start_lng: f64,
        end_lat: f64,
        end_lng: f64,
        max_passengers: i32,
        join_deadline_s: Option<i64>,
    ) -> Result<String>;
    async fn find_trip_by_id(&self, trip_id: &str) -> Result<Option<RideshareTrip>>;
    async fn find_trip_by_order(&self, order_id: &str) -> Result<Option<RideshareTrip>>;
    async fn update_trip_status(&self, trip_id: &str, status: &str) -> Result<()>;
    async fn list_open_trips(
        &self,
        service_type: &str,
        min_lat: f64,
        max_lat: f64,
        min_lng: f64,
        max_lng: f64,
    ) -> Result<Vec<RideshareOpenItem>>;
    async fn create_passenger(
        &self,
        trip_id: &str,
        rider_id: &str,
        pickup_lat: f64,
        pickup_lng: f64,
        pickup_address: &str,
        dest_lat: f64,
        dest_lng: f64,
        dest_address: &str,
        fare_estimate: i32,
    ) -> Result<String>;
    async fn find_passenger_by_id(&self, passenger_id: &str) -> Result<Option<RidesharePassenger>>;
    async fn list_passengers(&self, trip_id: &str) -> Result<Vec<RidesharePassenger>>;
    async fn update_passenger_status(
        &self,
        passenger_id: &str,
        status: &str,
        reason: Option<&str>,
        fare_final: Option<i32>,
        distance_km: Option<f64>,
    ) -> Result<()>;
    async fn passenger_exists(&self, trip_id: &str, rider_id: &str) -> Result<bool>;
}

pub struct RideshareRepository {
    pool: Pool,
}

impl RideshareRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn row_to_trip(row: &Row) -> Result<RideshareTrip> {
        let id_bytes: Vec<u8> = row.try_get("id")?;
        let order_bytes: Vec<u8> = row.try_get("host_order_id")?;
        let driver_bytes: Vec<u8> = row.try_get("driver_id")?;
        Ok(RideshareTrip {
            id: bin_to_ulid(id_bytes)?,
            host_order_id: bin_to_ulid(order_bytes)?,
            driver_id: bin_to_ulid(driver_bytes)?,
            service_type: row.try_get("service_type")?, // sudah TEXT dari ::TEXT cast
            route_start_lat: row.try_get("route_start_lat")?,
            route_start_lng: row.try_get("route_start_lng")?,
            route_end_lat: row.try_get("route_end_lat")?,
            route_end_lng: row.try_get("route_end_lng")?,
            route_polyline: col_opt_str(row, "route_polyline")?,
            max_passengers: row.try_get::<_, i16>("max_passengers")? as i32,
            current_passengers: row.try_get::<_, i16>("current_passengers")? as i32,
            status: row.try_get("status")?,
            join_deadline: col_opt_str(row, "join_deadline")?,
            created_at: row.try_get("created_at")?,
        })
    }

    fn row_to_passenger(row: &Row) -> Result<RidesharePassenger> {
        let id_bytes: Vec<u8> = row.try_get("id")?;
        let trip_bytes: Vec<u8> = row.try_get("trip_id")?;
        let rider_bytes: Vec<u8> = row.try_get("rider_id")?;
        Ok(RidesharePassenger {
            id: bin_to_ulid(id_bytes)?,
            trip_id: bin_to_ulid(trip_bytes)?,
            rider_id: bin_to_ulid(rider_bytes)?,
            pickup_lat: row.try_get("pickup_lat")?,
            pickup_lng: row.try_get("pickup_lng")?,
            pickup_address: row.try_get("pickup_address")?,
            dest_lat: row.try_get("dest_lat")?,
            dest_lng: row.try_get("dest_lng")?,
            dest_address: row.try_get("dest_address")?,
            fare_estimate: row.try_get("fare_estimate")?,
            fare_final: col_opt_i32(row, "fare_final"),
            distance_km: col_opt_f64(row, "distance_km"),
            status: row.try_get("status")?,
            requested_at: row.try_get("requested_at")?,
            accepted_at: col_opt_str(row, "accepted_at")?,
            picked_up_at: col_opt_str(row, "picked_up_at")?,
            dropped_off_at: col_opt_str(row, "dropped_off_at")?,
            cancelled_at: col_opt_str(row, "cancelled_at")?,
            cancel_reason: col_opt_str(row, "cancel_reason")?,
        })
    }

    fn row_to_open_item(row: &Row) -> Result<RideshareOpenItem> {
        let id_bytes: Vec<u8> = row.try_get("id")?;
        let order_bytes: Vec<u8> = row.try_get("host_order_id")?;
        let driver_bytes: Vec<u8> = row.try_get("driver_id")?;
        Ok(RideshareOpenItem {
            id: bin_to_ulid(id_bytes)?,
            host_order_id: bin_to_ulid(order_bytes)?,
            driver_id: bin_to_ulid(driver_bytes)?,
            service_type: row.try_get("service_type")?,
            route_start_lat: row.try_get("route_start_lat")?,
            route_start_lng: row.try_get("route_start_lng")?,
            route_end_lat: row.try_get("route_end_lat")?,
            route_end_lng: row.try_get("route_end_lng")?,
            route_polyline: col_opt_str(row, "route_polyline")?,
            seats_available: row.try_get("seats_available")?,
            join_deadline: col_opt_str(row, "join_deadline")?,
            driver_name: row.try_get("driver_name")?,
            driver_avatar: col_opt_str(row, "driver_avatar")?,
            vehicle_plate: row.try_get("vehicle_plate")?,
            vehicle_model: row.try_get("vehicle_model")?,
            vehicle_color: row.try_get("vehicle_color")?,
            driver_rating: row.try_get("driver_rating")?,
        })
    }
}

#[async_trait]
impl RideshareRepositoryTrait for RideshareRepository {
    async fn create_trip(
        &self,
        host_order_id: &str,
        driver_id: &str,
        service_type: &str,
        start_lat: f64,
        start_lng: f64,
        end_lat: f64,
        end_lng: f64,
        max_passengers: i32,
        join_deadline_s: Option<i64>,
    ) -> Result<String> {
        let trip_id = new_ulid();
        let trip_b = ulid_to_vec(&trip_id)?;
        let host_order_b = id_to_vec(host_order_id)?;
        let driver_b = id_to_vec(driver_id)?;
        let max_p = max_passengers as i16; // SMALLINT

        exec_drop(
            &self.pool,
            r#"INSERT INTO rideshare_trips
               (id, host_order_id, driver_id, service_type,
                route_start_lat, route_start_lng, route_end_lat, route_end_lng,
                max_passengers, join_deadline)
               VALUES ($1,$2,$3,$4::text::vehicle_type,
                       $5::FLOAT8::NUMERIC, $6::FLOAT8::NUMERIC,
                       $7::FLOAT8::NUMERIC, $8::FLOAT8::NUMERIC,
                       $9,
                 CASE WHEN $10::BIGINT IS NOT NULL
                      THEN NOW() + ($10 * INTERVAL '1 second')
                      ELSE NULL END)"#,
            &[
                &trip_b,
                &host_order_b,
                &driver_b,
                &service_type,
                &start_lat,
                &start_lng,
                &end_lat,
                &end_lng,
                &max_p,
                &join_deadline_s,
            ],
        )
        .await?;
        Ok(trip_id)
    }

    async fn find_trip_by_id(&self, trip_id: &str) -> Result<Option<RideshareTrip>> {
        let trip_b = id_to_vec(trip_id)?;
        let rows = exec_rows(&self.pool,
            r#"SELECT id, host_order_id, driver_id,
                      service_type::TEXT AS service_type,
                      route_start_lat::FLOAT8, route_start_lng::FLOAT8,
                      route_end_lat::FLOAT8,   route_end_lng::FLOAT8,
                      route_polyline, max_passengers, current_passengers, status,
                      to_char(join_deadline AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS join_deadline,
                      to_char(created_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
               FROM rideshare_trips WHERE id = $1"#,
            &[&trip_b]).await?;
        rows.into_iter()
            .next()
            .map(|r| Self::row_to_trip(&r))
            .transpose()
    }

    async fn find_trip_by_order(&self, order_id: &str) -> Result<Option<RideshareTrip>> {
        let order_b = id_to_vec(order_id)?;
        let rows = exec_rows(&self.pool,
            r#"SELECT id, host_order_id, driver_id,
                      service_type::TEXT AS service_type,
                      route_start_lat::FLOAT8, route_start_lng::FLOAT8,
                      route_end_lat::FLOAT8,   route_end_lng::FLOAT8,
                      route_polyline, max_passengers, current_passengers, status,
                      to_char(join_deadline AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS join_deadline,
                      to_char(created_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
               FROM rideshare_trips WHERE host_order_id = $1"#,
            &[&order_b]).await?;
        rows.into_iter()
            .next()
            .map(|r| Self::row_to_trip(&r))
            .transpose()
    }

    async fn update_trip_status(&self, trip_id: &str, status: &str) -> Result<()> {
        let trip_b = id_to_vec(trip_id)?;
        let extra = match status {
            "in_progress" => ", started_at = NOW()",
            "completed" => ", completed_at = NOW()",
            _ => "",
        };
        let sql = format!(
            "UPDATE rideshare_trips SET status = $1{} WHERE id = $2",
            extra
        );
        exec_drop(&self.pool, &sql, &[&status, &trip_b]).await
    }

    async fn list_open_trips(
        &self,
        service_type: &str,
        min_lat: f64,
        max_lat: f64,
        min_lng: f64,
        max_lng: f64,
    ) -> Result<Vec<RideshareOpenItem>> {
        // Pakai view rideshare_trips_open yang ada di schema
        // service_type di view sudah vehicle_type ENUM — cast ke TEXT untuk compare
        let rows = exec_rows(&self.pool,
            r#"SELECT id, host_order_id, driver_id,
                      service_type::TEXT AS service_type,
                      route_start_lat::FLOAT8, route_start_lng::FLOAT8,
                      route_end_lat::FLOAT8,   route_end_lng::FLOAT8,
                      route_polyline,
                      seats_available::BIGINT,
                      to_char(join_deadline AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS join_deadline,
                      driver_name, driver_avatar,
                      vehicle_plate, vehicle_model, vehicle_color,
                      driver_rating::FLOAT8
               FROM rideshare_trips_open
               WHERE service_type::TEXT = $1
                 AND route_start_lat::FLOAT8 BETWEEN $2 AND $3
                 AND route_start_lng::FLOAT8 BETWEEN $4 AND $5
                 AND seats_available > 0
               ORDER BY id DESC LIMIT 50"#,
            &[&service_type, &min_lat, &max_lat, &min_lng, &max_lng]).await?;
        rows.iter().map(Self::row_to_open_item).collect()
    }

    async fn create_passenger(
        &self,
        trip_id: &str,
        rider_id: &str,
        pickup_lat: f64,
        pickup_lng: f64,
        pickup_address: &str,
        dest_lat: f64,
        dest_lng: f64,
        dest_address: &str,
        fare_estimate: i32,
    ) -> Result<String> {
        let passenger_id = new_ulid();
        let passenger_b = ulid_to_vec(&passenger_id)?;
        let trip_b = id_to_vec(trip_id)?;
        let rider_b = id_to_vec(rider_id)?;
        exec_drop(
            &self.pool,
            r#"INSERT INTO rideshare_passengers
               (id, trip_id, rider_id, pickup_lat, pickup_lng, pickup_address,
                dest_lat, dest_lng, dest_address, fare_estimate)
               VALUES ($1,$2,$3,
                       $4::FLOAT8::NUMERIC, $5::FLOAT8::NUMERIC, $6,
                       $7::FLOAT8::NUMERIC, $8::FLOAT8::NUMERIC, $9,
                       $10)"#,
            &[
                &passenger_b,
                &trip_b,
                &rider_b,
                &pickup_lat,
                &pickup_lng,
                &pickup_address,
                &dest_lat,
                &dest_lng,
                &dest_address,
                &fare_estimate,
            ],
        )
        .await?;
        Ok(passenger_id)
    }

    async fn find_passenger_by_id(&self, passenger_id: &str) -> Result<Option<RidesharePassenger>> {
        let p_b = id_to_vec(passenger_id)?;
        let rows = exec_rows(&self.pool,
            r#"SELECT id, trip_id, rider_id,
                      pickup_lat::FLOAT8, pickup_lng::FLOAT8, pickup_address,
                      dest_lat::FLOAT8,   dest_lng::FLOAT8,   dest_address,
                      fare_estimate, fare_final, distance_km::FLOAT8, status,
                      to_char(requested_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
                      to_char(accepted_at    AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS accepted_at,
                      to_char(picked_up_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS picked_up_at,
                      to_char(dropped_off_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS dropped_off_at,
                      to_char(cancelled_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS cancelled_at,
                      cancel_reason
               FROM rideshare_passengers WHERE id = $1"#,
            &[&p_b]).await?;
        rows.into_iter()
            .next()
            .map(|r| Self::row_to_passenger(&r))
            .transpose()
    }

    async fn list_passengers(&self, trip_id: &str) -> Result<Vec<RidesharePassenger>> {
        let trip_b = id_to_vec(trip_id)?;
        let rows = exec_rows(&self.pool,
            r#"SELECT id, trip_id, rider_id,
                      pickup_lat::FLOAT8, pickup_lng::FLOAT8, pickup_address,
                      dest_lat::FLOAT8,   dest_lng::FLOAT8,   dest_address,
                      fare_estimate, fare_final, distance_km::FLOAT8, status,
                      to_char(requested_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
                      to_char(accepted_at    AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS accepted_at,
                      to_char(picked_up_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS picked_up_at,
                      to_char(dropped_off_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS dropped_off_at,
                      to_char(cancelled_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS cancelled_at,
                      cancel_reason
               FROM rideshare_passengers WHERE trip_id = $1"#,
            &[&trip_b]).await?;
        rows.iter().map(Self::row_to_passenger).collect()
    }

    async fn update_passenger_status(
        &self,
        passenger_id: &str,
        status: &str,
        reason: Option<&str>,
        fare_final: Option<i32>,
        distance_km: Option<f64>,
    ) -> Result<()> {
        let p_b = id_to_vec(passenger_id)?;
        exec_drop(
            &self.pool,
            r#"UPDATE rideshare_passengers
               SET status        = $1,
                   cancel_reason = COALESCE($2, cancel_reason),
                   fare_final    = COALESCE($3, fare_final),
                   distance_km   = COALESCE($4::FLOAT8::NUMERIC, distance_km)
               WHERE id = $5"#,
            &[&status, &reason, &fare_final, &distance_km, &p_b],
        )
        .await?;

        let ts_col = match status {
            "accepted" => Some("accepted_at"),
            "picked_up" => Some("picked_up_at"),
            "dropped_off" => Some("dropped_off_at"),
            "cancelled" | "rejected" => Some("cancelled_at"),
            _ => None,
        };
        if let Some(col) = ts_col {
            let sql = format!(
                "UPDATE rideshare_passengers SET {} = NOW() WHERE id = $1",
                col
            );
            exec_drop(&self.pool, &sql, &[&p_b]).await?;
        }
        Ok(())
    }

    async fn passenger_exists(&self, trip_id: &str, rider_id: &str) -> Result<bool> {
        let trip_b = id_to_vec(trip_id)?;
        let rider_b = id_to_vec(rider_id)?;
        let rows = exec_rows(
            &self.pool,
            r#"SELECT COUNT(*)::BIGINT AS cnt FROM rideshare_passengers
               WHERE trip_id = $1 AND rider_id = $2
                 AND status NOT IN ('rejected','cancelled')"#,
            &[&trip_b, &rider_b],
        )
        .await?;
        let cnt: i64 = rows
            .first()
            .and_then(|r| r.try_get("cnt").ok())
            .unwrap_or(0);
        Ok(cnt > 0)
    }
}
