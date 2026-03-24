use anyhow::Result;
use async_trait::async_trait;
use mysql_async::{from_value, Pool, Row};

use super::db::{col, col_opt_f64, col_opt_i32, col_opt_str, exec_drop, exec_rows};
use crate::utils::ulid::{bin_to_ulid, new_ulid, ulid_to_bytes};

// ── Models ────────────────────────────────────────────────────────────────────

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

// ── Trait ─────────────────────────────────────────────────────────────────────

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

// ── MySQL implementasi ────────────────────────────────────────────────────────

pub struct RideshareRepository {
    pool: Pool,
}

impl RideshareRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn row_to_trip(row: &Row) -> Result<RideshareTrip> {
        Ok(RideshareTrip {
            id: bin_to_ulid(from_value(col(row, "id")?))?,
            host_order_id: bin_to_ulid(from_value(col(row, "host_order_id")?))?,
            driver_id: bin_to_ulid(from_value(col(row, "driver_id")?))?,
            service_type: from_value(col(row, "service_type")?),
            route_start_lat: from_value(col(row, "route_start_lat")?),
            route_start_lng: from_value(col(row, "route_start_lng")?),
            route_end_lat: from_value(col(row, "route_end_lat")?),
            route_end_lng: from_value(col(row, "route_end_lng")?),
            route_polyline: col_opt_str(row, "route_polyline"),
            max_passengers: from_value(col(row, "max_passengers")?),
            current_passengers: from_value(col(row, "current_passengers")?),
            status: from_value(col(row, "status")?),
            join_deadline: col_opt_str(row, "join_deadline"),
            created_at: from_value(col(row, "created_at")?),
        })
    }

    fn row_to_passenger(row: &Row) -> Result<RidesharePassenger> {
        Ok(RidesharePassenger {
            id: bin_to_ulid(from_value(col(row, "id")?))?,
            trip_id: bin_to_ulid(from_value(col(row, "trip_id")?))?,
            rider_id: bin_to_ulid(from_value(col(row, "rider_id")?))?,
            pickup_lat: from_value(col(row, "pickup_lat")?),
            pickup_lng: from_value(col(row, "pickup_lng")?),
            pickup_address: from_value(col(row, "pickup_address")?),
            dest_lat: from_value(col(row, "dest_lat")?),
            dest_lng: from_value(col(row, "dest_lng")?),
            dest_address: from_value(col(row, "dest_address")?),
            fare_estimate: from_value(col(row, "fare_estimate")?),
            fare_final: col_opt_i32(row, "fare_final"),
            distance_km: col_opt_f64(row, "distance_km"),
            status: from_value(col(row, "status")?),
            requested_at: from_value(col(row, "requested_at")?),
            accepted_at: col_opt_str(row, "accepted_at"),
            picked_up_at: col_opt_str(row, "picked_up_at"),
            dropped_off_at: col_opt_str(row, "dropped_off_at"),
            cancelled_at: col_opt_str(row, "cancelled_at"),
            cancel_reason: col_opt_str(row, "cancel_reason"),
        })
    }

    fn row_to_open_item(row: &Row) -> Result<RideshareOpenItem> {
        Ok(RideshareOpenItem {
            id: bin_to_ulid(from_value(col(row, "id")?))?,
            host_order_id: bin_to_ulid(from_value(col(row, "host_order_id")?))?,
            driver_id: bin_to_ulid(from_value(col(row, "driver_id")?))?,
            service_type: from_value(col(row, "service_type")?),
            route_start_lat: from_value(col(row, "route_start_lat")?),
            route_start_lng: from_value(col(row, "route_start_lng")?),
            route_end_lat: from_value(col(row, "route_end_lat")?),
            route_end_lng: from_value(col(row, "route_end_lng")?),
            route_polyline: col_opt_str(row, "route_polyline"),
            seats_available: from_value(col(row, "seats_available")?),
            join_deadline: col_opt_str(row, "join_deadline"),
            driver_name: from_value(col(row, "driver_name")?),
            driver_avatar: col_opt_str(row, "driver_avatar"),
            vehicle_plate: from_value(col(row, "vehicle_plate")?),
            vehicle_model: from_value(col(row, "vehicle_model")?),
            vehicle_color: from_value(col(row, "vehicle_color")?),
            driver_rating: from_value(col(row, "driver_rating")?),
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
        let trip_id_b = ulid_to_bytes(&trip_id)?;
        let host_order_id_b = ulid_to_bytes(host_order_id)?;
        let driver_id_b = ulid_to_bytes(driver_id)?;

        exec_drop(
            &self.pool,
            r"INSERT INTO rideshare_trips
                (id, host_order_id, driver_id, service_type,
                 route_start_lat, route_start_lng, route_end_lat, route_end_lng,
                 max_passengers, join_deadline)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
                CASE WHEN ? IS NOT NULL THEN DATE_ADD(NOW(3), INTERVAL ? SECOND) ELSE NULL END)",
            (
                &trip_id_b[..],
                &host_order_id_b[..],
                &driver_id_b[..],
                service_type,
                start_lat,
                start_lng,
                end_lat,
                end_lng,
                max_passengers,
                join_deadline_s,
                join_deadline_s,
            ),
        )
        .await?;
        Ok(trip_id)
    }

    async fn find_trip_by_id(&self, trip_id: &str) -> Result<Option<RideshareTrip>> {
        let trip_id_b = ulid_to_bytes(trip_id)?;
        let rows: Vec<Row> = exec_rows(
            &self.pool,
            r"SELECT id, host_order_id, driver_id, service_type,
                     route_start_lat, route_start_lng, route_end_lat, route_end_lng,
                     route_polyline, max_passengers, current_passengers, status,
                     join_deadline, created_at
              FROM rideshare_trips WHERE id = ?",
            (&trip_id_b[..],),
        )
        .await?;
        rows.first().map(Self::row_to_trip).transpose()
    }

    async fn find_trip_by_order(&self, order_id: &str) -> Result<Option<RideshareTrip>> {
        let order_id_b = ulid_to_bytes(order_id)?;
        let rows: Vec<Row> = exec_rows(
            &self.pool,
            r"SELECT id, host_order_id, driver_id, service_type,
                     route_start_lat, route_start_lng, route_end_lat, route_end_lng,
                     route_polyline, max_passengers, current_passengers, status,
                     join_deadline, created_at
              FROM rideshare_trips WHERE host_order_id = ?",
            (&order_id_b[..],),
        )
        .await?;
        rows.first().map(Self::row_to_trip).transpose()
    }

    async fn update_trip_status(&self, trip_id: &str, status: &str) -> Result<()> {
        let trip_id_b = ulid_to_bytes(trip_id)?;
        let extra = match status {
            "in_progress" => ", started_at = NOW(3)",
            "completed" => ", completed_at = NOW(3)",
            _ => "",
        };
        let sql = format!(
            "UPDATE rideshare_trips SET status = ?{} WHERE id = ?",
            extra
        );
        exec_drop(&self.pool, &sql, (status, &trip_id_b[..])).await
    }

    async fn list_open_trips(
        &self,
        service_type: &str,
        min_lat: f64,
        max_lat: f64,
        min_lng: f64,
        max_lng: f64,
    ) -> Result<Vec<RideshareOpenItem>> {
        let rows: Vec<Row> = exec_rows(
            &self.pool,
            r"SELECT id, host_order_id, driver_id, service_type,
                     route_start_lat, route_start_lng, route_end_lat, route_end_lng,
                     route_polyline, seats_available, join_deadline,
                     driver_name, driver_avatar,
                     vehicle_plate, vehicle_model, vehicle_color, driver_rating
              FROM rideshare_trips_open
              WHERE service_type = ?
                AND route_start_lat BETWEEN ? AND ?
                AND route_start_lng BETWEEN ? AND ?
                AND seats_available > 0
              ORDER BY id DESC LIMIT 50",
            (service_type, min_lat, max_lat, min_lng, max_lng),
        )
        .await?;
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
        let passenger_id_b = ulid_to_bytes(&passenger_id)?;
        let trip_id_b = ulid_to_bytes(trip_id)?;
        let rider_id_b = ulid_to_bytes(rider_id)?;

        exec_drop(
            &self.pool,
            r"INSERT INTO rideshare_passengers
                (id, trip_id, rider_id,
                 pickup_lat, pickup_lng, pickup_address,
                 dest_lat, dest_lng, dest_address, fare_estimate)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                &passenger_id_b[..],
                &trip_id_b[..],
                &rider_id_b[..],
                pickup_lat,
                pickup_lng,
                pickup_address,
                dest_lat,
                dest_lng,
                dest_address,
                fare_estimate,
            ),
        )
        .await?;
        Ok(passenger_id)
    }

    async fn find_passenger_by_id(&self, passenger_id: &str) -> Result<Option<RidesharePassenger>> {
        let passenger_id_b = ulid_to_bytes(passenger_id)?;
        let rows: Vec<Row> = exec_rows(
            &self.pool,
            r"SELECT id, trip_id, rider_id,
                     pickup_lat, pickup_lng, pickup_address,
                     dest_lat, dest_lng, dest_address,
                     fare_estimate, fare_final, distance_km, status,
                     requested_at, accepted_at, picked_up_at, dropped_off_at,
                     cancelled_at, cancel_reason
              FROM rideshare_passengers WHERE id = ?",
            (&passenger_id_b[..],),
        )
        .await?;
        rows.first().map(Self::row_to_passenger).transpose()
    }

    async fn list_passengers(&self, trip_id: &str) -> Result<Vec<RidesharePassenger>> {
        let trip_id_b = ulid_to_bytes(trip_id)?;
        let rows: Vec<Row> = exec_rows(
            &self.pool,
            r"SELECT id, trip_id, rider_id,
                     pickup_lat, pickup_lng, pickup_address,
                     dest_lat, dest_lng, dest_address,
                     fare_estimate, fare_final, distance_km, status,
                     requested_at, accepted_at, picked_up_at, dropped_off_at,
                     cancelled_at, cancel_reason
              FROM rideshare_passengers WHERE trip_id = ?",
            (&trip_id_b[..],),
        )
        .await?;
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
        let passenger_id_b = ulid_to_bytes(passenger_id)?;

        exec_drop(
            &self.pool,
            r"UPDATE rideshare_passengers
              SET status        = ?,
                  cancel_reason = COALESCE(?, cancel_reason),
                  fare_final    = COALESCE(?, fare_final),
                  distance_km   = COALESCE(?, distance_km)
              WHERE id = ?",
            (status, reason, fare_final, distance_km, &passenger_id_b[..]),
        )
        .await?;

        let ts_col = match status {
            "accepted" => Some("accepted_at"),
            "picked_up" => Some("picked_up_at"),
            "dropped_off" => Some("dropped_off_at"),
            "cancelled" | "rejected" => Some("cancelled_at"),
            _ => None,
        };
        if let Some(col_name) = ts_col {
            let sql = format!(
                "UPDATE rideshare_passengers SET {} = NOW(3) WHERE id = ?",
                col_name
            );
            exec_drop(&self.pool, &sql, (&passenger_id_b[..],)).await?;
        }
        Ok(())
    }

    async fn passenger_exists(&self, trip_id: &str, rider_id: &str) -> Result<bool> {
        let trip_id_b = ulid_to_bytes(trip_id)?;
        let rider_id_b = ulid_to_bytes(rider_id)?;
        let rows: Vec<Row> = exec_rows(
            &self.pool,
            r"SELECT COUNT(*) AS cnt FROM rideshare_passengers
              WHERE trip_id = ? AND rider_id = ?
                AND status NOT IN ('rejected','cancelled')",
            (&trip_id_b[..], &rider_id_b[..]),
        )
        .await?;
        let cnt: i64 = rows
            .first()
            .map(|r| from_value(col(r, "cnt").unwrap()))
            .unwrap_or(0);
        Ok(cnt > 0)
    }
}
