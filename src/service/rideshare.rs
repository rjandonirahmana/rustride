use std::sync::Arc;

use crate::{
    connections::{ConnectionManager, Priority},
    proto::ridehailing::{
        server_event::Payload as Sp, PassengerRequestEvent, PassengerStatusEvent, RideshareItem,
        RideshareStatusEvent, ServerEvent,
    },
    repository::{
        notification::NotificationRepositoryTrait,
        rideshare::{RideshareOpenItem, RideshareRepositoryTrait},
        user::UserRepository,
    },
};

// ── Haversine helper ──────────────────────────────────────────────────────────

pub fn haversine_km(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    const R: f64 = 6371.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlng / 2.0).sin().powi(2);
    R * 2.0 * a.sqrt().atan2((1.0 - a).sqrt())
}

/// Hitung fare sederhana per rider berdasarkan jarak pickup→dest mereka.
/// Base: 5000 + 2500/km (motor), 7000 + 3500/km (mobil)
fn estimate_fare(dist_km: f64, service_type: &str) -> i32 {
    let (base, per_km): (f64, f64) = match service_type {
        "mobil" => (7000.0, 3500.0),
        _ => (5000.0, 2500.0),
    };
    ((base + per_km * dist_km) as i32).max(8000)
}

/// Cek apakah rute rider (pickup→dest) searah dengan rute driver (start→end).
/// Pakai dot-product vektor — jika sudut < 60° dianggap searah.
fn is_route_compatible(
    driver_start: (f64, f64),
    driver_end: (f64, f64),
    rider_pickup: (f64, f64),
    rider_dest: (f64, f64),
) -> bool {
    let driver_vec = (driver_end.0 - driver_start.0, driver_end.1 - driver_start.1);
    let rider_vec = (rider_dest.0 - rider_pickup.0, rider_dest.1 - rider_pickup.1);

    let dot = driver_vec.0 * rider_vec.0 + driver_vec.1 * rider_vec.1;
    let mag_d = (driver_vec.0.powi(2) + driver_vec.1.powi(2)).sqrt();
    let mag_r = (rider_vec.0.powi(2) + rider_vec.1.powi(2)).sqrt();

    if mag_d < 1e-9 || mag_r < 1e-9 {
        return false;
    }

    let cos_angle = (dot / (mag_d * mag_r)).clamp(-1.0, 1.0);
    // cos(60°) = 0.5 → terima jika sudut ≤ 60°
    cos_angle >= 0.5
}

// ── Service ───────────────────────────────────────────────────────────────────

pub struct RideshareService<
    RR: RideshareRepositoryTrait,
    NR: NotificationRepositoryTrait,
    UR: UserRepository,
> {
    pub rideshare_repo: Arc<RR>,
    pub notif_repo: Arc<NR>,
    pub user_repo: Arc<UR>,
    pub connections: Arc<ConnectionManager>,
}

impl<RR, NR, UR> RideshareService<RR, NR, UR>
where
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
    UR: UserRepository + 'static,
{
    // ── Driver: buka trip nebeng ──────────────────────────────────────────────
    pub async fn open_trip(
        &self,
        driver_id: &str,
        order_id: &str,
        start_lat: f64,
        start_lng: f64,
        end_lat: f64,
        end_lng: f64,
        service_type: &str,
        max_passengers: i32,
        join_deadline_s: Option<i64>,
    ) -> anyhow::Result<String> {
        // Cek belum ada trip terbuka untuk order ini
        if let Some(_existing) = self.rideshare_repo.find_trip_by_order(order_id).await? {
            anyhow::bail!("Trip sudah dibuka untuk order ini");
        }

        let trip_id = self
            .rideshare_repo
            .create_trip(
                order_id,
                driver_id,
                service_type,
                start_lat,
                start_lng,
                end_lat,
                end_lng,
                max_passengers,
                join_deadline_s,
            )
            .await?;

        tracing::info!(driver_id, trip_id, "Rideshare trip dibuka");
        Ok(trip_id)
    }

    // ── Rider: browse trip searah ─────────────────────────────────────────────
    pub async fn browse_trips(
        &self,
        rider_pickup_lat: f64,
        rider_pickup_lng: f64,
        rider_dest_lat: f64,
        rider_dest_lng: f64,
        service_type: &str,
        radius_km: f64,
    ) -> anyhow::Result<Vec<RideshareItem>> {
        // Bounding box ±radius
        let lat_deg = radius_km / 111.0;
        let lng_deg = radius_km
            / (111.0 * rider_pickup_lat.to_radians().cos())
                .abs()
                .max(0.01);

        let raw = self
            .rideshare_repo
            .list_open_trips(
                service_type,
                rider_pickup_lat - lat_deg,
                rider_pickup_lat + lat_deg,
                rider_pickup_lng - lng_deg,
                rider_pickup_lng + lng_deg,
            )
            .await?;

        let mut result = Vec::new();
        for trip in raw {
            // Filter presisi: arah rute harus kompatibel
            if !is_route_compatible(
                (trip.route_start_lat, trip.route_start_lng),
                (trip.route_end_lat, trip.route_end_lng),
                (rider_pickup_lat, rider_pickup_lng),
                (rider_dest_lat, rider_dest_lng),
            ) {
                continue;
            }

            // Jarak rider ke start driver
            let dist_to_driver = haversine_km(
                rider_pickup_lat,
                rider_pickup_lng,
                trip.route_start_lat,
                trip.route_start_lng,
            );

            // Fare rider ini: pickup → dest
            let rider_dist = haversine_km(
                rider_pickup_lat,
                rider_pickup_lng,
                rider_dest_lat,
                rider_dest_lng,
            );
            let fare = estimate_fare(rider_dist, service_type);

            result.push(trip_to_proto(&trip, fare, dist_to_driver as f32));
        }

        Ok(result)
    }

    // ── Rider: request nebeng ─────────────────────────────────────────────────
    pub async fn join_trip(
        &self,
        rider_id: &str,
        rider_name: &str,
        rider_avatar: &str,
        trip_id: &str,
        pickup_lat: f64,
        pickup_lng: f64,
        pickup_address: &str,
        dest_lat: f64,
        dest_lng: f64,
        dest_address: &str,
    ) -> anyhow::Result<(String, i32)> {
        // (passenger_id, fare_estimate)
        let trip = self
            .rideshare_repo
            .find_trip_by_id(trip_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Trip tidak ditemukan"))?;

        if trip.status != "open" {
            anyhow::bail!("Trip sudah penuh atau tidak tersedia");
        }

        if self
            .rideshare_repo
            .passenger_exists(trip_id, rider_id)
            .await?
        {
            anyhow::bail!("Kamu sudah request trip ini");
        }

        let dist = haversine_km(pickup_lat, pickup_lng, dest_lat, dest_lng);
        let fare = estimate_fare(dist, &trip.service_type);

        let passenger_id = self
            .rideshare_repo
            .create_passenger(
                trip_id,
                rider_id,
                pickup_lat,
                pickup_lng,
                pickup_address,
                dest_lat,
                dest_lng,
                dest_address,
                fare,
            )
            .await?;

        // Jarak rider ke pickup driver (untuk info driver)
        let dist_to_driver = haversine_km(
            pickup_lat,
            pickup_lng,
            trip.route_start_lat,
            trip.route_start_lng,
        );

        // Kirim notif ke driver
        self.connections.send(
            &trip.driver_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::PassengerRequest(PassengerRequestEvent {
                    passenger_id: passenger_id.clone(),
                    trip_id: trip_id.to_string(),
                    rider_id: rider_id.to_string(),
                    rider_name: rider_name.to_string(),
                    rider_avatar: rider_avatar.to_string(),
                    pickup_lat,
                    pickup_lng,
                    pickup_address: pickup_address.to_string(),
                    dest_lat,
                    dest_lng,
                    dest_address: dest_address.to_string(),
                    fare_estimate: fare,
                    distance_km: dist_to_driver as f32,
                })),
            }),
            Priority::Normal,
        );

        // Simpan notif ke DB untuk driver
        let _ = self
            .notif_repo
            .create(
                &trip.driver_id,
                "nebeng_request",
                "Ada penumpang ingin nebeng",
                &format!("{} ingin nebeng ke {}", rider_name, dest_address),
                None,
                Some(rider_id),
                None,
            )
            .await;

        tracing::info!(rider_id, trip_id, passenger_id, "Rider request nebeng");
        Ok((passenger_id, fare))
    }

    // ── Driver: terima passenger ──────────────────────────────────────────────
    pub async fn accept_passenger(
        &self,
        driver_id: &str,
        trip_id: &str,
        passenger_id: &str,
    ) -> anyhow::Result<()> {
        let trip = self
            .rideshare_repo
            .find_trip_by_id(trip_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Trip tidak ditemukan"))?;

        if trip.driver_id != driver_id {
            anyhow::bail!("Bukan trip kamu");
        }

        let passenger = self
            .rideshare_repo
            .find_passenger_by_id(passenger_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Passenger tidak ditemukan"))?;

        self.rideshare_repo
            .update_passenger_status(passenger_id, "accepted", None, None, None)
            .await?;

        // Notify rider
        self.connections.send(
            &passenger.rider_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::PassengerStatus(PassengerStatusEvent {
                    passenger_id: passenger_id.to_string(),
                    trip_id: trip_id.to_string(),
                    status: "accepted".to_string(),
                    reason: String::new(),
                    fare_estimate: passenger.fare_estimate,
                })),
            }),
            Priority::Normal,
        );

        // Broadcast status trip (mungkin berubah ke "full")
        self.broadcast_trip_status(trip_id, Some(passenger_id), "accepted")
            .await;

        // Notif DB ke rider
        let _ = self
            .notif_repo
            .create(
                &passenger.rider_id,
                "nebeng_accepted",
                "Request nebeng diterima!",
                "Driver menerima permintaan nebengmu. Bersiaplah!",
                None,
                Some(driver_id),
                None,
            )
            .await;

        Ok(())
    }

    // ── Driver: tolak passenger ───────────────────────────────────────────────
    pub async fn reject_passenger(
        &self,
        driver_id: &str,
        trip_id: &str,
        passenger_id: &str,
        reason: Option<&str>,
    ) -> anyhow::Result<()> {
        let trip = self
            .rideshare_repo
            .find_trip_by_id(trip_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Trip tidak ditemukan"))?;

        if trip.driver_id != driver_id {
            anyhow::bail!("Bukan trip kamu");
        }

        let passenger = self
            .rideshare_repo
            .find_passenger_by_id(passenger_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Passenger tidak ditemukan"))?;

        self.rideshare_repo
            .update_passenger_status(passenger_id, "rejected", reason, None, None)
            .await?;

        self.connections.send(
            &passenger.rider_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::PassengerStatus(PassengerStatusEvent {
                    passenger_id: passenger_id.to_string(),
                    trip_id: trip_id.to_string(),
                    status: "rejected".to_string(),
                    reason: reason.unwrap_or("").to_string(),
                    fare_estimate: 0,
                })),
            }),
            Priority::Normal,
        );

        let _ = self
            .notif_repo
            .create(
                &passenger.rider_id,
                "nebeng_rejected",
                "Request nebeng ditolak",
                reason.unwrap_or("Driver tidak dapat menerimamu saat ini."),
                None,
                Some(driver_id),
                None,
            )
            .await;

        Ok(())
    }

    // ── Driver: pickup passenger ──────────────────────────────────────────────
    pub async fn pickup_passenger(
        &self,
        driver_id: &str,
        trip_id: &str,
        passenger_id: &str,
    ) -> anyhow::Result<()> {
        self.assert_driver_owns_trip(driver_id, trip_id).await?;
        let passenger = self.get_passenger(passenger_id).await?;

        self.rideshare_repo
            .update_passenger_status(passenger_id, "picked_up", None, None, None)
            .await?;

        self.connections.send(
            &passenger.rider_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::PassengerStatus(PassengerStatusEvent {
                    passenger_id: passenger_id.to_string(),
                    trip_id: trip_id.to_string(),
                    status: "picked_up".to_string(),
                    reason: String::new(),
                    fare_estimate: passenger.fare_estimate,
                })),
            }),
            Priority::Normal,
        );

        self.broadcast_trip_status(trip_id, Some(passenger_id), "picked_up")
            .await;
        Ok(())
    }

    // ── Driver: drop passenger ────────────────────────────────────────────────
    pub async fn drop_passenger(
        &self,
        driver_id: &str,
        trip_id: &str,
        passenger_id: &str,
        distance_km: f64,
    ) -> anyhow::Result<()> {
        self.assert_driver_owns_trip(driver_id, trip_id).await?;
        let passenger = self.get_passenger(passenger_id).await?;

        let fare_final = estimate_fare(distance_km, "mobil"); // fallback; idealnya pakai service_type trip

        self.rideshare_repo
            .update_passenger_status(
                passenger_id,
                "dropped_off",
                None,
                Some(fare_final),
                Some(distance_km),
            )
            .await?;

        self.connections.send(
            &passenger.rider_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::PassengerStatus(PassengerStatusEvent {
                    passenger_id: passenger_id.to_string(),
                    trip_id: trip_id.to_string(),
                    status: "dropped_off".to_string(),
                    reason: String::new(),
                    fare_estimate: fare_final,
                })),
            }),
            Priority::Normal,
        );

        // Notif selesai
        let _ = self
            .notif_repo
            .create(
                &passenger.rider_id,
                "order_completed",
                "Perjalanan nebeng selesai",
                &format!("Kamu sudah tiba di tujuan. Fare: Rp {}", fare_final),
                None,
                Some(driver_id),
                None,
            )
            .await;

        self.broadcast_trip_status(trip_id, Some(passenger_id), "dropped_off")
            .await;
        Ok(())
    }

    // ── Rider: cancel nebeng ──────────────────────────────────────────────────
    pub async fn cancel_passenger(
        &self,
        rider_id: &str,
        trip_id: &str,
        passenger_id: &str,
        reason: Option<&str>,
    ) -> anyhow::Result<()> {
        let passenger = self.get_passenger(passenger_id).await?;

        if passenger.rider_id != rider_id {
            anyhow::bail!("Bukan requestmu");
        }

        self.rideshare_repo
            .update_passenger_status(passenger_id, "cancelled", reason, None, None)
            .await?;

        // Notify driver
        let trip = self
            .rideshare_repo
            .find_trip_by_id(trip_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Trip tidak ditemukan"))?;

        self.broadcast_trip_status(trip_id, Some(passenger_id), "cancelled")
            .await;

        // Kirim update ke driver supaya list penumpang segar
        self.connections.send(
            &trip.driver_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::RideshareStatus(RideshareStatusEvent {
                    trip_id: trip_id.to_string(),
                    status: trip.status.clone(),
                    seats_available: (trip.max_passengers - trip.current_passengers + 1) as i32,
                    passenger_id: passenger_id.to_string(),
                    passenger_status: "cancelled".to_string(),
                })),
            }),
            Priority::Normal,
        );

        Ok(())
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    async fn assert_driver_owns_trip(&self, driver_id: &str, trip_id: &str) -> anyhow::Result<()> {
        let trip = self
            .rideshare_repo
            .find_trip_by_id(trip_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Trip tidak ditemukan"))?;
        if trip.driver_id != driver_id {
            anyhow::bail!("Bukan trip kamu");
        }
        Ok(())
    }

    async fn get_passenger(
        &self,
        passenger_id: &str,
    ) -> anyhow::Result<crate::repository::rideshare::RidesharePassenger> {
        self.rideshare_repo
            .find_passenger_by_id(passenger_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Passenger tidak ditemukan"))
    }

    async fn broadcast_trip_status(
        &self,
        trip_id: &str,
        passenger_id: Option<&str>,
        passenger_status: &str,
    ) {
        if let Ok(Some(trip)) = self.rideshare_repo.find_trip_by_id(trip_id).await {
            let seats = (trip.max_passengers - trip.current_passengers) as i32;
            let event = ServerEvent {
                payload: Some(Sp::RideshareStatus(RideshareStatusEvent {
                    trip_id: trip_id.to_string(),
                    status: trip.status.clone(),
                    seats_available: seats,
                    passenger_id: passenger_id.unwrap_or("").to_string(),
                    passenger_status: passenger_status.to_string(),
                })),
            };
            // Broadcast ke semua passenger yang accepted
            if let Ok(passengers) = self.rideshare_repo.list_passengers(trip_id).await {
                for p in passengers
                    .iter()
                    .filter(|p| p.status == "accepted" || p.status == "picked_up")
                {
                    self.connections
                        .send(&p.rider_id, Arc::new(event.clone()), Priority::Normal);
                }
            }
        }
    }
}

// ── Proto converter ───────────────────────────────────────────────────────────

fn trip_to_proto(trip: &RideshareOpenItem, fare: i32, dist_to_driver: f32) -> RideshareItem {
    RideshareItem {
        trip_id: trip.id.clone(),
        driver_id: trip.driver_id.clone(),
        driver_name: trip.driver_name.clone(),
        driver_avatar: trip.driver_avatar.clone().unwrap_or_default(),
        vehicle_plate: trip.vehicle_plate.clone(),
        vehicle_model: trip.vehicle_model.clone(),
        vehicle_color: trip.vehicle_color.clone(),
        driver_rating: trip.driver_rating as f32,
        service_type: trip.service_type.clone(),
        route_start_lat: trip.route_start_lat,
        route_start_lng: trip.route_start_lng,
        route_end_lat: trip.route_end_lat,
        route_end_lng: trip.route_end_lng,
        seats_available: trip.seats_available as i32,
        fare_estimate: fare,
        distance_km: dist_to_driver,
        join_deadline: trip.join_deadline.clone().unwrap_or_default(),
    }
}
