use anyhow::{bail, Result};
/// OrderService — seluruh business logic order + driver matching.
///
/// Tidak ada dependency ke tonic/gRPC sama sekali.
/// Dipanggil dari gRPC handler, bisa juga dari REST handler.
use std::{collections::HashSet, sync::Arc};

use crate::{
    connections::{ConnectionManager, Priority},
    grpc::order::is_order_active,
    location::{haversine_m, LocationService},
    models::order::{NearbyOrder, Order},
    proto::{
        order::{DriverInfo, FareEstimateEvent, Order as cpOrder},
        ridehailing::{
            server_event::Payload as Sp, DriverLocationEvent, NewOrderOffer, OrderStatusEvent,
            ServerEvent,
        },
    },
    repository::{
        order::{NewOrder, OrderRepository},
        user::UserRepository,
    },
    utils::ulid::id_to_vec,
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

pub fn order_to_proto(o: &Order) -> crate::proto::order::Order {
    crate::proto::order::Order {
        order_id: o.id.clone(),
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

        driver: None, // diisi terpisah oleh caller yang punya akses user_repo
    }
}

fn status_event(
    order_id: &str,
    status: &str,
    service_type: &str,
    fare_estimate: &i32,
    rider_id: &str,
    rider_name: &str,
) -> Arc<ServerEvent> {
    Arc::new(ServerEvent {
        payload: Some(Sp::OrderStatus(OrderStatusEvent {
            order_id: order_id.to_string(),
            status: status.to_string(),
            driver: None,
            service_type: service_type.to_string(),
            fare_estimate: *fare_estimate,
            rider_id: rider_id.to_string(),
            rider_name: rider_name.to_string(),
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
                &order.rider_id,
                &order.rider_name,
            ),
            Priority::Normal,
        );

        // Spawn background driver matching:
        // 1. notify_nearby_drivers — push NewOrder langsung ke semua driver terdekat (immediate)
        // 2. search_driver_loop — fallback loop jika tidak ada yang accept dalam timeout
        let svc = self.clone();
        let o = order.clone();
        let rider_id_clone = rider_id.to_string();
        tokio::spawn(async move {
            // Push order ke semua driver terdekat yang available — ini yang membuat
            // data order muncul di driver secara realtime tanpa harus pull/browse
            if let Err(e) = svc.notify_nearby_drivers(&o).await {
                tracing::warn!(order_id = %o.id, error = %e, "notify_nearby_drivers gagal, lanjut ke search_driver_loop");
            }
            svc.search_driver_loop(o, &rider_id_clone).await;
        });

        Ok(order)
    }

    /// Dipanggil saat user disconnect — cancel order yang masih aktif
    /// Return Ok(Some(order_id)) jika ada yang di-cancel, Ok(None) jika tidak ada
    pub async fn cancel_active_order_on_disconnect(&self, user_id: &str) -> Result<Option<String>> {
        // Cari order aktif milik user ini (sebagai rider atau driver)
        let order = match self.order_repo.find_active_for_rider(user_id).await? {
            Some(o) => o,
            None => return Ok(None),
        };

        tracing::info!(user_id, order_id = %order.id, "User disconnect, cek cancel order aktif");

        if order.status != "searching" {
            return Ok(None);
        }
        let order_id = order.id.clone();
        let reason = "User terputus dari koneksi";

        self.order_repo.cancel(&order_id, Some(reason)).await?;

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

        // Filter hanya yang online + tidak sedang order, sambil simpan jarak
        let mut eligible: Vec<(String, f64)> = vec![];
        for (driver_id, dist_m) in &nearby {
            if !self.connections.is_connected(driver_id).await? {
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
            eligible.push((driver_id.clone().to_string(), *dist_m));
        }

        if eligible.is_empty() {
            return Ok(());
        }

        // Kirim notif individual per driver dengan jarak yang akurat
        for (driver_id, dist_m) in eligible {
            let eta_min = (dist_m / 1000.0 / 30.0 * 60.0) as f32;
            let event = ServerEvent {
                payload: Some(Sp::NewOrder(NewOrderOffer {
                    order: Some(cpOrder {
                        order_id: order.id.clone(),
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
                        driver: None,
                    }),
                    distance_to_pickup_m: dist_m as f32,
                    eta_to_pickup_min: eta_min,
                    timeout_secs: 30,
                })),
            };
            self.connections
                .send(&driver_id, Arc::new(event), Priority::Normal);
        }
        Ok(())
    }

    async fn search_driver_loop(&self, order: Order, rider_id: &str) {
        tracing::info!(order_id = %order.id, "Mulai search_driver_loop untuk order");
        const OFFER_TIMEOUT: u64 = 15;
        const MIN_RATING: f32 = 3.0;
        const RETRY_DELAY_SECS: u64 = 5;

        let mut offered: HashSet<String> = HashSet::new();
        let mut round = 0u32;

        loop {
            round += 1;
            tracing::info!(order_id = %order.id, round, "Search driver round");

            match self.connections.is_connected(rider_id).await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::info!(order_id = %order.id, "Rider sudah disconnect, stop loop");
                    return;
                }
                Err(e) => {
                    tracing::error!("is_connected error: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_DELAY_SECS)).await;
                    return;
                }
            }

            // ── PINDAH KE ATAS — cek status SEBELUM apapun ───────────────────
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

            // ── HAPUS notify_nearby_drivers dari sini ─────────────────────────
            // Sudah di-handle: initial call di create_order, dan per-driver offer di bawah

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

            tracing::info!(
                order_id = %order.id,
                round,
                count = nearby.len(),
                service_type = %order.service_type,
                pickup_lat = order.pickup_lat,
                pickup_lng = order.pickup_lng,
                "find_nearby_drivers result"
            );

            // Reset offered list setiap 3 round ATAU kalau semua driver nearby
            // sudah pernah di-offer (driver reconnect setelah offline perlu
            // mendapat offer ulang tanpa menunggu 3 round penuh)
            let all_offered =
                !nearby.is_empty() && nearby.iter().all(|(id, _)| offered.contains(id));
            if (round % 3 == 0 || all_offered) && !offered.is_empty() {
                tracing::info!(
                    order_id = %order.id,
                    round,
                    all_offered,
                    "Reset offered list"
                );
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
                    tracing::debug!(driver_id = %driver_id, "skip: sudah pernah di-offer");
                    continue;
                }

                match self.connections.is_connected(rider_id).await {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::info!(order_id = %order.id, "Rider sudah disconnect, stop loop");
                        return;
                    }
                    Err(e) => {
                        tracing::error!("is_connected error: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_DELAY_SECS))
                            .await;
                        return;
                    }
                }

                if self
                    .location
                    .get_driver_order(&driver_id)
                    .await
                    .unwrap_or(None)
                    .is_some()
                {
                    tracing::info!(driver_id = %driver_id, "skip: driver sudah punya order aktif di Redis");
                    continue;
                }

                let (_user, profile) = match self.user_repo.find_driver_by_id(&driver_id).await {
                    Ok(Some(v)) => v,
                    Ok(None) => {
                        tracing::warn!(driver_id = %driver_id, "skip: driver tidak ditemukan di DB");
                        continue;
                    }
                    Err(e) => {
                        tracing::error!(driver_id = %driver_id, error = %e, "skip: gagal fetch driver profile");
                        continue;
                    }
                };

                if profile.rating < MIN_RATING {
                    tracing::info!(driver_id = %driver_id, rating = profile.rating, min = MIN_RATING, "skip: rating terlalu rendah");
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
                                status: order.status.to_string(),
                                driver: None,
                                service_type: order.service_type.clone(),
                                fare_estimate: order.fare_estimate,
                                rider_id: order.rider_id.clone(),
                                rider_name: order.rider_name.clone(),
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

            // Poll setiap 1 detik selama OFFER_TIMEOUT:
            // - Kalau order sudah tidak searching (driver accept) → stop
            // - Kalau driver offline → langsung next round, cari driver lain
            // - Kalau timeout habis → next round (driver tidak respond)
            let mut elapsed = 0u64;
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                elapsed += 1;

                // Cek order masih searching
                match self.order_repo.find_by_id(&order.id).await {
                    Ok(Some(o)) if o.status != "searching" => {
                        tracing::info!(order_id = %order.id, status = %o.status, "Order accepted, stop loop");
                        return;
                    }
                    Ok(None) => return,
                    Err(e) => {
                        tracing::error!("find_by_id poll error: {}", e);
                        break;
                    }
                    _ => {}
                }

                // Kalau driver offline → langsung cari driver lain, jangan tunggu
                match self.connections.is_connected(&best.driver_id).await {
                    Ok(false) => {
                        tracing::info!(
                            order_id = %order.id,
                            driver_id = %best.driver_id,
                            elapsed,
                            "Driver offline saat menunggu respons, cari driver lain"
                        );
                        // Reset offered supaya kalau tidak ada driver lain,
                        // driver ini bisa di-offer ulang saat sudah online lagi
                        offered.remove(&best.driver_id);
                        break;
                    }
                    Ok(true) => {}
                    Err(_) => break,
                }

                if elapsed >= OFFER_TIMEOUT {
                    tracing::info!(
                        order_id = %order.id,
                        driver_id = %best.driver_id,
                        "Driver tidak respond dalam timeout, cari driver lain"
                    );
                    break;
                }
            }
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
        self.location
            .set_driver_order(driver_id, order_id, &order.service_type)
            .await?;

        let driver_info = self.build_driver_info(driver_id).await;

        // Notify rider
        self.connections.send(
            &order.rider_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::OrderStatus(OrderStatusEvent {
                    order_id: order_id.to_string(),
                    status: "driver_accepted".to_string(),
                    driver: driver_info.clone(),
                    service_type: order.service_type.clone(),
                    fare_estimate: order.fare_estimate,
                    rider_id: order.rider_id.clone(),
                    rider_name: order.rider_name.clone(),
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
                    rider_id: order.rider_id,
                    rider_name: order.rider_name,
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
                &order.rider_id,
                &order.rider_name,
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
                &order.rider_id,
                &order.rider_name,
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
                &order.rider_id,
                &order.rider_name,
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
                &order.rider_id,
                &order.rider_name,
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
                &order.rider_id,
                &order.rider_name,
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
                &order.rider_id,
                &order.rider_name,
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
            "driver" => order.driver_id.as_ref() == Some(&user_id.to_string()),
            _ => false,
        };

        if !is_owner {
            bail!(
        "Bukan order kamu. role: {}, user_id: {}, order.rider_id: {}, order.driver_id: {:?}",
        role, user_id, order.rider_id, order.driver_id
    );
        }
        if matches!(order.status.as_str(), "completed" | "cancelled") {
            bail!("Order sudah {}", order.status);
        }

        if role == "rider" && order.status != "searching" {
            bail!(
                "Order tidak bisa dicancel karena sudah {} oleh driver",
                order.status
            );
        }

        // Driver: hanya bisa cancel jika status 'driver_accepted'
        if role == "driver" && order.status != "driver_accepted" {
            bail!(
                "Order tidak bisa dicancel karena status masih {}",
                order.status
            );
        }

        self.order_repo.cancel(order_id, reason.as_deref()).await?;

        if role == "driver" {
            self.connections.send(
                &order.rider_id,
                status_event(
                    order_id,
                    "cancelled",
                    &order.service_type,
                    &order.fare_estimate,
                    &order.rider_id,
                    &order.rider_name,
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
                &order.rider_id,
                &order.rider_name,
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
        if role != "rider" {
            bail!("Hanya rider yang bisa memberi rating");
        }
        let order = self
            .order_repo
            .find_by_id(order_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Order tidak ditemukan"))?;
        if order.rider_id != user_id {
            bail!("Bukan order kamu");
        }
        if order.driver_id.is_none() {
            bail!("Belum ada driver, belum bisa kasih rating");
        }
        if order.status != "completed" {
            bail!("Order belum selesai");
        }
        self.order_repo
            .submit_rating(
                order_id,
                user_id,
                order.driver_id.as_deref().unwrap(),
                rating,
                tip_amount,
                comment,
            )
            .await
    }
}
