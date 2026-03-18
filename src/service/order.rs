use anyhow::{bail, Result};
/// OrderService — seluruh business logic order + driver matching.
///
/// Tidak ada dependency ke tonic/gRPC sama sekali.
/// Dipanggil dari gRPC handler, bisa juga dari REST handler.
use std::{collections::HashSet, sync::Arc};

use crate::{
    connections::{ConnectionManager, Priority},
    location::{haversine_m, LocationService},
    models::order::{NearbyOrder, Order},
    proto::ridehailing::{
        server_event::Payload as Sp, DriverInfo, DriverLocationEvent, FareEstimateEvent,
        NewOrderOffer, Order as cpOrder, OrderStatusEvent, ServerEvent,
    },
    repository::{
        order::{NewOrder, OrderRepository},
        user::UserRepository,
    },
};

// ── Helpers ───────────────────────────────────────────────────────────────────

pub fn calculate_fare(distance_km: f64, service_type: &str) -> i32 {
    let (base, per_km): (i32, i32) = match service_type {
        "mobil" => (15_000, 4_000),
        _ => (8_000, 2_500),
    };
    let raw = base + (distance_km * per_km as f64) as i32;
    // Bulatkan ke ribuan
    ((raw + 999) / 1000) * 1000
}

pub fn order_to_proto(o: &Order) -> crate::proto::ridehailing::Order {
    crate::proto::ridehailing::Order {
        id: o.id.clone(),
        rider_id: o.rider_id.clone(),
        driver_id: o.driver_id.clone().unwrap_or_default(),
        status: o.status.clone(),
        pickup_lat: o.pickup_lat,
        pickup_lng: o.pickup_lng,
        pickup_address: o.pickup_address.clone(),
        dest_lat: o.dest_lat,
        dest_lng: o.dest_lng,
        dest_address: o.dest_address.clone(),
        fare_estimate: o.fare_estimate,
        service_type: o.service_type.clone(),
        created_at: o.created_at.clone(),
        rider_name: o.rider_name.clone(),
    }
}

fn status_event(
    order_id: &str,
    status: &str,
    service_tye: &str,
    fare_estimate: &i32,
) -> Arc<ServerEvent> {
    Arc::new(ServerEvent {
        payload: Some(Sp::OrderStatus(OrderStatusEvent {
            order_id: order_id.to_string(),
            status: status.to_string(),
            driver: None,
            service_type: service_tye.to_string(),
            fare_estimate: *fare_estimate,
        })),
    })
}

// ── OrderService ──────────────────────────────────────────────────────────────

pub struct OrderService<OR: OrderRepository, UR: UserRepository> {
    pub order_repo: Arc<OR>,
    pub user_repo: Arc<UR>,
    pub location: LocationService,
    pub connections: Arc<ConnectionManager>,
}

impl<OR: OrderRepository, UR: UserRepository> Clone for OrderService<OR, UR> {
    fn clone(&self) -> Self {
        Self {
            order_repo: self.order_repo.clone(),
            user_repo: self.user_repo.clone(),
            location: self.location.clone(),
            connections: self.connections.clone(),
        }
    }
}

impl<OR, UR> OrderService<OR, UR>
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
{
    pub fn new(
        order_repo: Arc<OR>,
        user_repo: Arc<UR>,
        location: LocationService,
        connections: Arc<ConnectionManager>,
    ) -> Self {
        Self {
            order_repo,
            user_repo,
            location,
            connections,
        }
    }

    // ── Rider: buat order ─────────────────────────────────────────────────────
    pub async fn create_order(
        &self,
        rider_id: &str,
        pickup_lat: f64,
        pickup_lng: f64,
        pickup_address: String,
        dest_lat: f64,
        dest_lng: f64,
        dest_address: String,
        service_type: &str,
    ) -> Result<Order> {
        if self
            .order_repo
            .find_active_for_rider(rider_id)
            .await?
            .is_some()
        {
            bail!("Masih ada order yang aktif");
        }

        let dist_km = haversine_m(pickup_lat, pickup_lng, dest_lat, dest_lng) / 1000.0;
        let fare = calculate_fare(dist_km, service_type);
        let order = &NewOrder {
            rider_id: rider_id.to_string(),
            pickup_lat,
            pickup_lng,
            pickup_address,
            dest_lat,
            dest_lng,
            dest_address,
            fare_estimate: fare,
            service_type: service_type.to_string(),
        };

        let order = self.order_repo.create(order).await?;

        // Push status "searching" ke rider
        self.connections.send(
            &rider_id,
            status_event(
                &order.id,
                "searching",
                &order.service_type,
                &order.fare_estimate,
            ),
            Priority::Normal,
        );

        // Spawn background driver matching
        let svc = self.clone();
        let o = order.clone();
        tokio::spawn(async move { svc.search_driver_loop(o).await });
        // Di create_order, setelah spawn search_driver_loop
        let svc2 = self.clone();
        let order_notif = order.clone();
        tokio::spawn(async move {
            let _ = svc2.notify_nearby_drivers(&order_notif).await;
        });

        Ok(order)
    }

    // di order.rs
    pub async fn has_active_order_with(&self, rider_id: &str, driver_id: &str) -> bool {
        match self
            .order_repo
            .find_active_between(rider_id, driver_id)
            .await
        {
            Ok(Some(order)) => {
                // Hanya boleh chat saat order sudah ada driver (bukan searching/cancelled)
                matches!(
                    order.status.as_str(),
                    "driver_accepted" | "driver_arrived" | "on_trip"
                )
            }
            _ => false,
        }
    }

    /// Dipanggil saat user disconnect — cancel order yang masih aktif
    /// Return Ok(Some(order_id)) jika ada yang di-cancel, Ok(None) jika tidak ada
    pub async fn cancel_active_order_on_disconnect(&self, user_id: &str) -> Result<Option<String>> {
        // Cari order aktif milik user ini (sebagai rider atau driver)
        let order = match self.order_repo.find_active_for_rider(user_id).await? {
            Some(o) => o,
            None => return Ok(None),
        };

        // Hanya cancel kalau status belum terminal
        if !matches!(
            order.status.as_str(),
            "completed" | "cancelled" | "searching"
        ) {
            return Ok(None);
        }

        let order_id = order.id.clone();
        let reason = "User terputus dari koneksi";

        self.order_repo.cancel(&order_id, Some(reason)).await?;

        // Bersihkan Redis kalau driver yang disconnect
        if order.driver_id.as_deref() == Some(user_id) {
            let _ = self.location.clear_driver_order(user_id).await;
            // Notify rider
            self.connections.send(
                &order.rider_id,
                Arc::new(ServerEvent {
                    payload: Some(Sp::OrderStatus(OrderStatusEvent {
                        order_id: order_id.clone(),
                        status: "cancelled".to_string(),
                        driver: None,
                        service_type: order.service_type.clone(),
                        fare_estimate: order.fare_estimate,
                    })),
                }),
                Priority::Normal,
            );
        } else {
            // Rider yang disconnect — notify driver kalau ada
            if let Some(ref driver_id) = order.driver_id {
                let _ = self.location.clear_driver_order(driver_id).await;
                self.connections.send(
                    driver_id,
                    Arc::new(ServerEvent {
                        payload: Some(Sp::OrderStatus(OrderStatusEvent {
                            order_id: order_id.clone(),
                            status: "cancelled".to_string(),
                            driver: None,
                            service_type: order.service_type.clone(),
                            fare_estimate: order.fare_estimate,
                        })),
                    }),
                    Priority::Normal,
                );
            }
        }

        Ok(Some(order_id))
    }
    // Di OrderService — setelah create_order, notify nearby drivers ada order baru
    pub async fn notify_nearby_drivers(&self, order: &Order) -> Result<()> {
        // Ambil driver terdekat dari Redis
        let nearby = self
            .location
            .find_nearby_drivers(order.pickup_lat, order.pickup_lng, &order.service_type)
            .await
            .unwrap_or_default();

        // Filter hanya yang online + tidak sedang order
        let mut eligible_ids = vec![];
        for (driver_id, _) in &nearby {
            if !self.connections.is_connected(driver_id).await {
                continue;
            }
            if self
                .location
                .get_driver_order(driver_id)
                .await
                .unwrap_or(None)
                .is_some()
            {
                continue;
            }
            eligible_ids.push(driver_id.clone().to_string());
        }

        if eligible_ids.is_empty() {
            return Ok(());
        }

        // Kirim notif "ada order baru di sekitar kamu"
        let event = ServerEvent {
            payload: Some(Sp::NewOrder(NewOrderOffer {
                order: Some(cpOrder {
                    id: order.id.clone(),
                    rider_id: order.rider_id.clone(),
                    driver_id: "".to_string(),
                    status: order.status.clone(),
                    pickup_address: order.pickup_address.clone(),
                    pickup_lat: order.pickup_lat,
                    pickup_lng: order.pickup_lng,
                    dest_address: order.dest_address.clone(),
                    dest_lat: order.dest_lat,
                    dest_lng: order.dest_lng,
                    fare_estimate: order.fare_estimate,
                    service_type: order.service_type.clone(),
                    created_at: order.created_at.clone(),
                    rider_name: order.rider_name.clone(),
                }),
                distance_to_pickup_m: order.distance_km.unwrap_or_default(),
                eta_to_pickup_min: 1000.0,
                timeout_secs: 100,
            })),
        };

        self.connections
            .send_to_drivers(eligible_ids, Arc::new(event), Priority::Normal);
        Ok(())
    }

    async fn search_driver_loop(&self, order: Order) {
        const OFFER_TIMEOUT: u64 = 15;
        const MIN_RATING: f32 = 3.0;
        const RETRY_DELAY_SECS: u64 = 5;

        let mut offered: HashSet<String> = HashSet::new();
        let mut round = 0u32;

        loop {
            round += 1;
            tracing::info!(order_id = %order.id, round, "Search driver round");

            // ── Cek order masih searching ─────────────────────────────────────
            match self.order_repo.find_by_id(&order.id).await {
                Ok(Some(o)) if o.status != "searching" => {
                    tracing::info!(order_id = %order.id, status = %o.status, "Order tidak lagi searching, stop loop");
                    return;
                }
                Ok(None) => return,
                Err(e) => {
                    tracing::error!("find_by_id error: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_DELAY_SECS)).await;
                    continue;
                }
                _ => {}
            }

            let nearby = match self
                .location
                .find_nearby_drivers(order.pickup_lat, order.pickup_lng, &order.service_type)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("find_nearby_drivers: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_DELAY_SECS)).await;
                    continue;
                }
            };

            // ── Reset offered setiap 3 round supaya driver bisa ditawari lagi ─
            // (driver yang reject mungkin sudah mau nerima sekarang)
            if round % 3 == 0 && !offered.is_empty() {
                tracing::info!(order_id = %order.id, "Reset offered list setelah 3 round");
                offered.clear();
            }

            struct DriverCandidate {
                driver_id: String,
                dist_m: f64,
                score: f32,
            }

            let mut candidates: Vec<DriverCandidate> = vec![];

            for (driver_id, dist_m) in nearby {
                if offered.contains(&driver_id) {
                    continue;
                }
                if !self.connections.is_connected(&driver_id).await {
                    continue;
                }
                if self
                    .location
                    .get_driver_order(&driver_id)
                    .await
                    .unwrap_or(None)
                    .is_some()
                {
                    continue;
                }

                let (_user, profile) = match self.user_repo.find_driver_by_id(&driver_id).await {
                    Ok(Some(v)) => v,
                    _ => continue,
                };

                if profile.rating < MIN_RATING {
                    continue;
                }
                if profile.vehicle_type != order.service_type {
                    continue;
                }

                let max_dist = 5000.0_f32;
                let dist_score = (1.0 - (dist_m as f32 / max_dist)).max(0.0);
                let rating_score = profile.rating / 5.0;
                let score = (dist_score * 0.6) + (rating_score * 0.4);

                candidates.push(DriverCandidate {
                    driver_id,
                    dist_m,
                    score,
                });
            }

            candidates.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let best = match candidates.into_iter().next() {
                Some(c) => c,
                None => {
                    tracing::warn!(
                        order_id = %order.id,
                        round,
                        "Tidak ada driver kandidat, retry dalam {}s",
                        RETRY_DELAY_SECS
                    );
                    // Notify rider masih mencari
                    self.connections.send(
                        &order.rider_id,
                        Arc::new(ServerEvent {
                            payload: Some(Sp::OrderStatus(OrderStatusEvent {
                                order_id: order.id.clone(),
                                status: "searching".to_string(),
                                driver: None,
                                service_type: order.service_type.clone(),
                                fare_estimate: order.fare_estimate,
                            })),
                        }),
                        Priority::Normal,
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_DELAY_SECS)).await;
                    continue;
                }
            };

            let eta_min = (best.dist_m / 1000.0 / 30.0 * 60.0) as f32;
            offered.insert(best.driver_id.clone());

            tracing::info!(
                order_id = %order.id,
                driver_id = %best.driver_id,
                score = %best.score,
                round,
                "Offer ke driver"
            );

            self.connections.send(
                &best.driver_id,
                Arc::new(ServerEvent {
                    payload: Some(Sp::NewOrder(NewOrderOffer {
                        order: Some(order_to_proto(&order)),
                        distance_to_pickup_m: best.dist_m as f32,
                        eta_to_pickup_min: eta_min,
                        timeout_secs: OFFER_TIMEOUT as u32,
                    })),
                }),
                Priority::Normal,
            );

            // Tunggu response driver
            tokio::time::sleep(tokio::time::Duration::from_secs(OFFER_TIMEOUT + 2)).await;

            // Cek lagi setelah tunggu
            match self.order_repo.find_by_id(&order.id).await {
                Ok(Some(o)) if o.status != "searching" => {
                    tracing::info!(order_id = %order.id, status = %o.status, "Order taken/cancelled");
                    return;
                }
                Ok(None) => return,
                _ => {}
            }

            // Lanjut ke round berikutnya tanpa batas
        }
    }

    // ── Driver: terima order ──────────────────────────────────────────────────
    pub async fn driver_accept(&self, driver_id: &str, order_id: &str) -> Result<Order> {
        let order = self
            .order_repo
            .find_by_id(order_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Order tidak ditemukan"))?;

        if order.status != "searching" {
            bail!("Order sudah tidak tersedia (status: {})", order.status);
        }
        if self.location.get_driver_order(driver_id).await?.is_some() {
            bail!("Driver sedang mengerjakan order lain");
        }

        // ── Atomic: assign + update status dalam 1 query ──────────────────────
        let affected = self
            .order_repo
            .assign_driver_atomic(order_id, driver_id) // UPDATE WHERE status = 'searching'
            .await?;

        if affected == 0 {
            // Sudah diambil driver lain duluan (race condition)
            bail!("Order sudah diambil driver lain");
        }

        // Setelah DB berhasil, baru set Redis
        self.location.set_driver_order(driver_id, order_id).await?;

        let driver_info = self.build_driver_info(driver_id).await;

        // Notify rider
        self.connections.send(
            &order.rider_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::OrderStatus(OrderStatusEvent {
                    order_id: order_id.to_string(),
                    status: "driver_found".to_string(),
                    driver: driver_info.clone(),
                    service_type: order.service_type.clone(),
                    fare_estimate: order.fare_estimate,
                })),
            }),
            Priority::Normal,
        );

        // Notify driver
        self.connections.send(
            driver_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::OrderStatus(OrderStatusEvent {
                    order_id: order_id.to_string(),
                    status: "driver_accepted".to_string(),
                    driver: driver_info,
                    service_type: order.service_type,
                    fare_estimate: order.fare_estimate,
                })),
            }),
            Priority::Normal,
        );

        let updated = self.order_repo.find_by_id(order_id).await?.unwrap();
        Ok(updated)
    }

    // ── Driver: tiba di pickup ────────────────────────────────────────────────
    pub async fn driver_arrived(&self, driver_id: &str, order_id: &str) -> Result<()> {
        let order = self
            .get_order_for_driver(driver_id, order_id, "driver_accepted")
            .await?;
        self.order_repo
            .update_status(order_id, "driver_arrived")
            .await?;
        self.connections.send(
            &order.rider_id,
            status_event(
                order_id,
                "driver_arrived",
                &order.service_type,
                &order.fare_estimate,
            ),
            Priority::Normal,
        );
        self.connections.send(
            driver_id,
            status_event(
                order_id,
                "driver_arrived",
                &order.service_type,
                &order.fare_estimate,
            ),
            Priority::Normal,
        );
        Ok(())
    }

    // ── Driver: mulai trip ────────────────────────────────────────────────────
    pub async fn start_trip(&self, driver_id: &str, order_id: &str) -> Result<()> {
        let order = self
            .get_order_for_driver(driver_id, order_id, "driver_arrived")
            .await?;
        self.order_repo.update_status(order_id, "on_trip").await?;
        self.connections.send(
            &order.rider_id,
            status_event(
                order_id,
                "on_trip",
                &order.service_type,
                &order.fare_estimate,
            ),
            Priority::Normal,
        );
        self.connections.send(
            driver_id,
            status_event(
                order_id,
                "on_trip",
                &order.service_type,
                &order.fare_estimate,
            ),
            Priority::Normal,
        );
        Ok(())
    }

    // ── Driver: selesai trip ──────────────────────────────────────────────────
    pub async fn complete_trip(&self, driver_id: &str, order_id: &str, dist_km: f32) -> Result<()> {
        let order = self
            .get_order_for_driver(driver_id, order_id, "on_trip")
            .await?;
        let fare = calculate_fare(dist_km as f64, &order.service_type);
        self.order_repo.complete(order_id, dist_km, fare).await?;
        let _ = self.location.clear_driver_order(driver_id).await;
        self.connections.send(
            &order.rider_id,
            status_event(
                order_id,
                "completed",
                &order.service_type,
                &order.fare_estimate,
            ),
            Priority::Normal,
        );
        self.connections.send(
            driver_id,
            status_event(
                order_id,
                "completed",
                &order.service_type,
                &order.fare_estimate,
            ),
            Priority::Normal,
        );
        Ok(())
    }

    // ── Cancel (rider atau driver) ────────────────────────────────────────────
    pub async fn cancel_order(
        &self,
        user_id: &str,
        role: &str,
        order_id: &str,
        reason: Option<String>,
    ) -> Result<()> {
        let order = self
            .order_repo
            .find_by_id(order_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Order tidak ditemukan"))?;

        let is_owner = match role {
            "rider" => order.rider_id == user_id,
            "driver" => order.driver_id.as_deref() == Some(user_id),
            _ => false,
        };
        if !is_owner {
            bail!("Bukan order kamu");
        }
        if matches!(order.status.as_str(), "completed" | "cancelled") {
            bail!("Order sudah {}", order.status);
        }

        self.order_repo.cancel(order_id, reason.as_deref()).await?;

        if let Some(ref driver_id) = order.driver_id {
            let _ = self.location.clear_driver_order(driver_id).await;
            if role == "rider" {
                self.connections.send(
                    driver_id,
                    status_event(
                        order_id,
                        "cancelled",
                        &order.service_type,
                        &order.fare_estimate,
                    ),
                    Priority::Normal,
                );
            }
        }
        if role == "driver" {
            self.connections.send(
                &order.rider_id,
                status_event(
                    order_id,
                    "cancelled",
                    &order.service_type,
                    &order.fare_estimate,
                ),
                Priority::Normal,
            );
        }
        self.connections.send(
            user_id,
            status_event(
                order_id,
                "cancelled",
                &order.service_type,
                &order.fare_estimate,
            ),
            Priority::Normal,
        );
        Ok(())
    }

    // ── Browse order (driver pull) ────────────────────────────────────────────
    pub async fn get_nearby_orders(
        &self,
        lat: f64,
        lng: f64,
        service_type: &str,
        radius_km: Option<f32>,
    ) -> Result<Vec<NearbyOrder>> {
        let r = radius_km.unwrap_or(5.0) as f64;
        let orders = self
            .order_repo
            .get_nearby_searching(service_type, lat, lng, r)
            .await?;
        Ok(orders
            .into_iter()
            .map(|o| {
                let dist = haversine_m(lat, lng, o.pickup_lat, o.pickup_lng) as f32;
                let eta = dist / 1000.0 / 30.0 * 60.0;
                NearbyOrder {
                    distance_to_pickup_m: dist,
                    eta_to_pickup_min: eta,
                    order: o,
                }
            })
            .collect())
    }
    pub async fn broadcast_driver_location(
        &self,
        _driver_id: &str,
        order_id: &str,
        lat: f64,
        lng: f64,
        heading: f32,
    ) -> Result<()> {
        let order = self
            .order_repo
            .find_by_id(order_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Order tidak ditemukan"))?;
        self.connections.send(
            &order.rider_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::DriverLoc(DriverLocationEvent {
                    order_id: order_id.to_string(),
                    lat,
                    lng,
                    heading,
                })),
            }),
            Priority::Normal,
        );
        Ok(())
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    async fn get_order_for_driver(
        &self,
        driver_id: &str,
        order_id: &str,
        required_status: &str,
    ) -> Result<Order> {
        let order = self
            .order_repo
            .find_by_id(order_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Order tidak ditemukan"))?;
        if order.driver_id.as_deref() != Some(driver_id) {
            bail!("Bukan order kamu");
        }
        if order.status != required_status {
            bail!(
                "Status order tidak valid: {} (butuh: {})",
                order.status,
                required_status
            );
        }
        Ok(order)
    }

    async fn build_driver_info(&self, driver_id: &str) -> Option<DriverInfo> {
        let (user, profile) = self.user_repo.find_driver_by_id(driver_id).await.ok()??;
        Some(DriverInfo {
            user_id: user.id,
            name: user.name,
            phone: user.phone,
            vehicle_plate: profile.vehicle_plate,
            vehicle_model: profile.vehicle_model,
            vehicle_color: profile.vehicle_color,
            rating: profile.rating,
        })
    }

    pub async fn get_active_order_for_user(&self, user_id: &str) -> Result<Option<Order>> {
        if let Some(order) = self.order_repo.find_active_for_rider(user_id).await? {
            return Ok(Some(order));
        }

        Ok(None)
    }

    pub async fn get_active_order_for_driver(&self, driver_id: &str) -> Result<Option<Order>> {
        if let Some(order) = self.order_repo.find_active_for_driver(driver_id).await? {
            return Ok(Some(order));
        }

        Ok(None)
    }

    pub async fn estimate_fare(
        &self,
        pickup_lat: f64,
        pickup_lng: f64,
        dest_lat: f64,
        dest_lng: f64,
        service_type: &str,
    ) -> Result<FareEstimateEvent> {
        let distance_m = haversine_m(pickup_lat, pickup_lng, dest_lat, dest_lng);
        let distance_km = distance_m / 1000.0;
        let fare = calculate_fare(distance_km, service_type);

        // Estimasi durasi — kecepatan rata-rata di kota
        let avg_speed_kmh: f64 = match service_type {
            "mobil" => 25.0,
            _ => 30.0,
        };
        let duration_min = ((distance_km / avg_speed_kmh) * 60.0).ceil() as i32;

        Ok(FareEstimateEvent {
            fare_estimate: fare,
            distance_km: distance_km as f32,
            duration_min,
            service_type: service_type.to_string(),
        })
    }

    pub async fn submit_rating(
        &self,
        user_id: &str,
        role: &str,
        order_id: &str,
        tip_amount: f64,
        rating: u8,
        comment: &str,
    ) -> Result<()> {
        if role == "driver" {
            bail!("Invalid role");
        }
        let order = self
            .order_repo
            .find_by_id(order_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Order tidak ditemukan"))?;

        let is_allowed = match role {
            "rider" => order.rider_id == user_id,
            "driver" => order.driver_id.as_deref() == Some(user_id),
            _ => false,
        };

        if order.driver_id.is_none() {
            bail!("Belum ada driver, belum bisa kasih rating");
        }

        if !is_allowed {
            bail!("Bukan order kamu");
        }
        if order.status != "completed" {
            bail!("Order belum selesai");
        }

        self.order_repo
            .submit_rating(
                order_id,
                user_id,
                order.driver_id.as_deref().unwrap_or(""),
                rating,
                tip_amount,
                comment,
            )
            .await?;
        Ok(())
    }
}
