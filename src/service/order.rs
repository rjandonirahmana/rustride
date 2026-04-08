use anyhow::{bail, Result};
/// OrderService — seluruh business logic order + driver matching.
///
/// Tidak ada dependency ke tonic/gRPC sama sekali.
/// Dipanggil dari gRPC handler, bisa juga dari REST handler.
use std::{collections::HashSet, sync::Arc, time::Duration, time::Instant};
use tokio::sync::Semaphore;

use crate::{
    connections::{ConnectionManager, Priority},
    location::{haversine_m, LocationService},
    models::order::{NearbyOrder, Order},
    proto::{
        order::{DriverInfo, FareEstimateEvent},
        ridehailing::{
            server_event::Payload as Sp, DriverLocationEvent, NewOrderOffer, OrderStatusEvent,
            ServerEvent,
        },
    },
    repository::{
        order::{NewOrder, OrderRepository},
        user::UserRepository,
    },
};

// ── Konstanta ─────────────────────────────────────────────────────────────────

/// Maksimum waktu pencarian driver sebelum order otomatis dibatalkan.
const MAX_SEARCH_DURATION_SECS: u64 = 300; // 5 menit

/// Timeout per offer ke satu driver.
const OFFER_TIMEOUT_SECS: u64 = 15;

/// Rating minimum driver yang boleh mendapat offer.
const MIN_RATING: f32 = 3.0;

/// Delay sebelum retry saat tidak ada driver kandidat.
/// 2 detik untuk pengalaman rider yang lebih responsif.
const RETRY_DELAY_SECS: u64 = 2;

/// Maksimum order yang dikembalikan oleh get_nearby_orders (driver pull).
const NEARBY_ORDER_LIMIT: usize = 50;

/// Batas maksimum concurrent search task.
/// Mencegah task leak saat ribuan order dibuat sekaligus —
/// task yang tidak mendapat slot akan antre secara async (tidak blocking).
const MAX_CONCURRENT_SEARCH: usize = 1000;

// ── Helpers ───────────────────────────────────────────────────────────────────

pub fn calculate_fare(distance_km: f64, service_type: &str) -> i32 {
    let (base, per_km): (i32, i32) = match service_type {
        "mobil" => (15_000, 4_000),
        _ => (8_000, 2_500),
    };
    let raw = base + (distance_km * per_km as f64) as i32;
    // Bulatkan ke ribuan terdekat
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
        created_at: o.created_at.clone().to_rfc3339(),
        driver: None, // diisi terpisah oleh caller yang punya akses user_repo
    }
}

/// Buat ServerEvent menggunakan status yang ada di `order`.
fn make_status_event(order: &Order, driver: Option<DriverInfo>) -> Arc<ServerEvent> {
    make_status_event_with(order, &order.status.clone(), driver)
}

/// Buat ServerEvent dengan status yang di-override secara eksplisit.
/// Digunakan setelah update DB di mana `order` masih memegang status lama.
fn make_status_event_override(
    order: &Order,
    status: &str,
    driver: Option<DriverInfo>,
) -> Arc<ServerEvent> {
    make_status_event_with(order, status, driver)
}

/// Implementasi tunggal untuk menghindari duplikasi struct OrderStatusEvent.
fn make_status_event_with(
    order: &Order,
    status: &str,
    driver: Option<DriverInfo>,
) -> Arc<ServerEvent> {
    Arc::new(ServerEvent {
        payload: Some(Sp::OrderStatus(OrderStatusEvent {
            order_id: order.id.to_string(),
            status: status.to_string(),
            driver,
            service_type: order.service_type.to_string(),
            fare_estimate: order.fare_estimate,
            rider_id: order.rider_id.to_string(),
            rider_name: order.rider_name.to_string(),
            dest_lat: order.dest_lat,
            dest_lng: order.dest_lng,
            pickup_lat: order.pickup_lat,
            pickup_lng: order.pickup_lng,
        })),
    })
}

// ── Tipe bantu internal ───────────────────────────────────────────────────────

struct DriverCandidate {
    driver_id: String,
    dist_m: f64,
    score: f32,
}

/// Cache status driver per round untuk mengurangi jumlah round-trip ke Redis.
struct DriverState {
    is_online: bool,
    has_active_order: bool,
}

// ── OrderService ──────────────────────────────────────────────────────────────

pub struct OrderService<OR: OrderRepository, UR: UserRepository> {
    pub order_repo: Arc<OR>,
    pub user_repo: Arc<UR>,
    pub location: LocationService,
    pub connections: Arc<ConnectionManager>,
    /// Semaphore untuk membatasi concurrent search task agar tidak terjadi task leak.
    search_semaphore: Arc<Semaphore>,
}

impl<OR: OrderRepository, UR: UserRepository> Clone for OrderService<OR, UR> {
    fn clone(&self) -> Self {
        Self {
            order_repo: self.order_repo.clone(),
            user_repo: self.user_repo.clone(),
            location: self.location.clone(),
            connections: self.connections.clone(),
            search_semaphore: self.search_semaphore.clone(),
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
            search_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_SEARCH)),
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
        let new_order = &NewOrder {
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

        let order = self.order_repo.create(new_order).await?;

        // Push status "searching" ke rider
        self.connections
            .send(rider_id, make_status_event(&order, None), Priority::Normal);

        // Spawn background driver matching:
        // 1. notify_nearby_drivers — push NewOrder langsung ke semua driver terdekat
        // 2. search_driver_loop    — fallback loop jika tidak ada yang accept
        //
        // Semaphore memastikan tidak ada lebih dari MAX_CONCURRENT_SEARCH task aktif.
        // acquire_owned() memindahkan permit ke dalam task sehingga dilepas otomatis
        // saat task selesai (drop).
        let svc = self.clone();
        let o = order.clone();
        let rider_id_clone = rider_id.to_string();
        tokio::spawn(async move {
            let _permit = match svc.search_semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    tracing::error!(order_id = %o.id, "Semaphore closed, search task dibatalkan");
                    return;
                }
            };

            if let Err(e) = svc.notify_nearby_drivers(&o).await {
                tracing::warn!(
                    order_id = %o.id,
                    error = %e,
                    "notify_nearby_drivers gagal, lanjut ke search_driver_loop"
                );
            }
            svc.search_driver_loop(o, &rider_id_clone).await;
            // _permit di-drop di sini → slot semaphore dibebaskan
        });

        Ok(order)
    }

    /// Dipanggil saat rider disconnect — cancel order yang masih "searching".
    /// Return Ok(Some(order_id)) jika ada yang di-cancel, Ok(None) jika tidak ada.
    pub async fn cancel_active_order_on_disconnect(&self, user_id: &str) -> Result<Option<String>> {
        let order = match self.order_repo.find_active_for_rider(user_id).await? {
            Some(o) => o,
            None => return Ok(None),
        };

        tracing::info!(user_id, order_id = %order.id, "Rider disconnect, cek cancel order aktif");

        if order.status != "searching" {
            return Ok(None);
        }

        let order_id = order.id.clone();
        self.order_repo
            .cancel(&order_id, Some("User terputus dari koneksi"))
            .await?;

        Ok(Some(order_id))
    }

    /// Dipanggil saat driver disconnect — cancel order "driver_accepted" dan
    /// notify rider supaya tidak stuck menunggu driver yang sudah offline.
    pub async fn cancel_active_order_on_driver_disconnect(
        &self,
        driver_id: &str,
    ) -> Result<Option<String>> {
        let order = match self.order_repo.find_active_for_driver(driver_id).await? {
            Some(o) => o,
            None => return Ok(None),
        };

        tracing::info!(driver_id, order_id = %order.id, "Driver disconnect, cek cancel order aktif");

        // Hanya cancel jika status masih driver_accepted (belum mulai trip)
        if order.status != "driver_accepted" {
            return Ok(None);
        }

        let order_id = order.id.clone();
        self.order_repo
            .cancel(&order_id, Some("Driver terputus dari koneksi"))
            .await?;

        // Gunakan data order yang sudah ada — tidak perlu query ulang ke DB
        self.connections.send(
            &order.rider_id,
            make_status_event_override(&order, "cancelled", None),
            Priority::Normal,
        );

        let _ = self.location.clear_driver_order(driver_id).await;

        Ok(Some(order_id))
    }

    /// Segera push order ke semua driver terdekat yang available saat order baru dibuat.
    pub async fn notify_nearby_drivers(&self, order: &Order) -> Result<()> {
        let nearby = self
            .location
            .find_nearby_drivers(order.pickup_lat, order.pickup_lng, &order.service_type)
            .await
            .unwrap_or_default();

        // Fetch state semua driver sekaligus untuk mengurangi round-trip Redis
        let driver_states = self.fetch_driver_states(&nearby).await;

        let eligible: Vec<(String, f64)> = nearby
            .iter()
            .filter_map(|(driver_id, dist_m)| {
                let state = driver_states.get(driver_id.as_str())?;
                if state.is_online && !state.has_active_order {
                    Some((driver_id.clone(), *dist_m))
                } else {
                    None
                }
            })
            .collect();

        if eligible.is_empty() {
            return Ok(());
        }

        for (driver_id, dist_m) in eligible {
            let eta_min = (dist_m / 1000.0 / 30.0 * 60.0) as f32;
            let event = Arc::new(ServerEvent {
                payload: Some(Sp::NewOrder(NewOrderOffer {
                    order: Some(order_to_proto(order)),
                    distance_to_pickup_m: dist_m as f32,
                    eta_to_pickup_min: eta_min,
                    timeout_secs: OFFER_TIMEOUT_SECS as u32,
                })),
            });
            self.connections.send(&driver_id, event, Priority::Normal);
        }
        Ok(())
    }

    // ── Background: loop pencarian driver ────────────────────────────────────

    async fn search_driver_loop(&self, order: Order, rider_id: &str) {
        tracing::info!(order_id = %order.id, "Mulai search_driver_loop");

        let deadline = Instant::now() + Duration::from_secs(MAX_SEARCH_DURATION_SECS);
        let mut offered: HashSet<String> = HashSet::new();
        let mut round = 0u32;

        loop {
            round += 1;

            // ── 1. Cek batas waktu global ─────────────────────────────────────
            if Instant::now() >= deadline {
                tracing::warn!(order_id = %order.id, "Waktu pencarian habis, cancel order");
                if let Err(e) = self
                    .order_repo
                    .cancel(&order.id, Some("Waktu pencarian driver habis"))
                    .await
                {
                    tracing::error!(order_id = %order.id, error = %e, "Gagal cancel order timeout");
                }
                // Notify rider — gunakan data order yang ada, tidak perlu query ulang
                self.connections.send(
                    rider_id,
                    make_status_event_override(&order, "cancelled", None),
                    Priority::Normal,
                );
                return;
            }

            // ── 2. Cek koneksi rider ──────────────────────────────────────────
            match self.connections.is_connected(rider_id).await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::info!(order_id = %order.id, "Rider disconnect, stop loop");
                    return;
                }
                Err(e) => {
                    tracing::error!(order_id = %order.id, error = %e, "is_connected error");
                    tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
                    continue;
                }
            }

            // ── 3. Cek status order ───────────────────────────────────────────
            match self.order_repo.find_by_id(&order.id).await {
                Ok(Some(o)) if o.status != "searching" => {
                    tracing::info!(order_id = %order.id, status = %o.status, "Order sudah tidak searching, stop loop");
                    return;
                }
                Ok(None) => {
                    tracing::info!(order_id = %order.id, "Order tidak ditemukan, stop loop");
                    return;
                }
                Err(e) => {
                    tracing::error!(order_id = %order.id, error = %e, "find_by_id error");
                    tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
                    continue;
                }
                _ => {}
            }

            // ── 4. Fetch driver terdekat ──────────────────────────────────────
            let nearby = match self
                .location
                .find_nearby_drivers(order.pickup_lat, order.pickup_lng, &order.service_type)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(order_id = %order.id, error = %e, "find_nearby_drivers error");
                    tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
                    continue;
                }
            };

            tracing::info!(
                order_id = %order.id,
                round,
                count = nearby.len(),
                "find_nearby_drivers result"
            );

            // Reset offered list setiap 3 round ATAU jika semua driver sudah di-offer
            let all_offered =
                !nearby.is_empty() && nearby.iter().all(|(id, _)| offered.contains(id));
            if (round % 3 == 0 || all_offered) && !offered.is_empty() {
                tracing::info!(order_id = %order.id, round, all_offered, "Reset offered list");
                offered.clear();
            }

            // ── 5. Fetch state semua driver sekaligus (batch Redis) ───────────
            // Mengurangi round-trip Redis dari O(N driver) menjadi O(1) batch per round.
            let driver_states = self.fetch_driver_states(&nearby).await;

            // ── 6. Filter & scoring kandidat ─────────────────────────────────
            let candidates = self
                .build_driver_candidates(&order, &nearby, &offered, &driver_states)
                .await;

            let best = match candidates.into_iter().next() {
                Some(c) => c,
                None => {
                    tracing::warn!(
                        order_id = %order.id,
                        round,
                        "Tidak ada kandidat driver, retry dalam {}s",
                        RETRY_DELAY_SECS
                    );
                    // Notify rider masih mencari
                    self.connections.send(
                        &order.rider_id,
                        make_status_event(&order, None),
                        Priority::Normal,
                    );
                    tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
                    continue;
                }
            };

            // ── 7. Kirim offer ke driver terpilih ─────────────────────────────
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
                        timeout_secs: OFFER_TIMEOUT_SECS as u32,
                    })),
                }),
                Priority::Normal,
            );

            // ── 8. Poll respons driver ────────────────────────────────────────
            match self
                .wait_for_driver_response(&order, &best.driver_id, OFFER_TIMEOUT_SECS)
                .await
            {
                PollResult::Accepted => return,
                PollResult::OrderGone => return,
                PollResult::DriverOffline => {
                    // Hapus dari offered supaya driver ini bisa dapat offer lagi
                    // saat reconnect tanpa harus tunggu reset 3 round
                    offered.remove(&best.driver_id);
                }
                PollResult::Timeout | PollResult::Error => {
                    // Lanjut ke driver berikutnya di round berikutnya
                }
            }
        }
    }

    /// Fetch status online + active_order untuk semua driver dalam satu pass.
    ///
    /// Hasilnya di-cache per round sehingga `build_driver_candidates` tidak perlu
    /// query Redis lagi secara individual — penting saat ada banyak driver di radius.
    async fn fetch_driver_states(
        &self,
        nearby: &[(String, f64)],
    ) -> std::collections::HashMap<String, DriverState> {
        let mut states = std::collections::HashMap::with_capacity(nearby.len());

        for (driver_id, _) in nearby {
            let is_online = self
                .connections
                .is_connected(driver_id)
                .await
                .unwrap_or(false);
            let has_active_order = self
                .location
                .get_driver_order(driver_id)
                .await
                .unwrap_or(None)
                .is_some();

            states.insert(
                driver_id.clone(),
                DriverState {
                    is_online,
                    has_active_order,
                },
            );
        }

        states
    }

    /// Filter dan score semua kandidat driver dari hasil `find_nearby_drivers`.
    /// Menggunakan `driver_states` yang sudah di-fetch sebelumnya agar tidak ada
    /// query Redis ganda per driver per round.
    async fn build_driver_candidates(
        &self,
        order: &Order,
        nearby: &[(String, f64)],
        offered: &HashSet<String>,
        driver_states: &std::collections::HashMap<String, DriverState>,
    ) -> Vec<DriverCandidate> {
        let mut candidates: Vec<DriverCandidate> = vec![];

        for (driver_id, dist_m) in nearby {
            if offered.contains(driver_id) {
                tracing::debug!(driver_id = %driver_id, "skip: sudah pernah di-offer");
                continue;
            }

            // Gunakan state yang sudah di-cache — tidak ada query Redis di sini
            match driver_states.get(driver_id.as_str()) {
                Some(state) if !state.is_online => {
                    tracing::debug!(driver_id = %driver_id, "skip: driver offline");
                    continue;
                }
                Some(state) if state.has_active_order => {
                    tracing::debug!(driver_id = %driver_id, "skip: driver sudah punya order aktif");
                    continue;
                }
                None => {
                    tracing::warn!(driver_id = %driver_id, "skip: state tidak ada di cache");
                    continue;
                }
                _ => {}
            }

            // Fetch profil driver dari DB (rating, tipe kendaraan)
            let (_user, profile) = match self.user_repo.find_driver_by_id(driver_id).await {
                Ok(Some(v)) => v,
                Ok(None) => {
                    tracing::warn!(driver_id = %driver_id, "skip: driver tidak ditemukan di DB");
                    continue;
                }
                Err(e) => {
                    tracing::error!(driver_id = %driver_id, error = %e, "gagal fetch driver profile");
                    continue;
                }
            };

            if profile.rating < MIN_RATING {
                tracing::debug!(driver_id = %driver_id, rating = profile.rating, "skip: rating terlalu rendah");
                continue;
            }
            if profile.vehicle_type != order.service_type {
                tracing::debug!(driver_id = %driver_id, "skip: tipe kendaraan tidak sesuai");
                continue;
            }

            let max_dist = 5000.0_f32;
            let dist_score = (1.0 - (*dist_m as f32 / max_dist)).max(0.0);
            let rating_score = profile.rating / 5.0;
            let score = (dist_score * 0.6) + (rating_score * 0.4);

            candidates.push(DriverCandidate {
                driver_id: driver_id.clone(),
                dist_m: *dist_m,
                score,
            });
        }

        // Sort descending berdasarkan score (jarak + rating)
        candidates.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        candidates
    }

    /// Poll respons driver setelah offer dikirim.
    /// Cek setiap 1 detik: apakah order sudah tidak searching, driver offline, atau timeout.
    async fn wait_for_driver_response(
        &self,
        order: &Order,
        driver_id: &str,
        timeout_secs: u64,
    ) -> PollResult {
        let mut elapsed = 0u64;

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            elapsed += 1;

            // Cek apakah order sudah berubah status (accepted/cancelled)
            match self.order_repo.find_by_id(&order.id).await {
                Ok(Some(o)) if o.status != "searching" => {
                    tracing::info!(order_id = %order.id, status = %o.status, "Order berubah status saat menunggu driver");
                    return PollResult::Accepted;
                }
                Ok(None) => return PollResult::OrderGone,
                Err(e) => {
                    tracing::error!(order_id = %order.id, error = %e, "find_by_id poll error");
                    return PollResult::Error;
                }
                _ => {}
            }

            // Cek driver masih online — jika offline langsung cari driver lain
            match self.connections.is_connected(driver_id).await {
                Ok(false) => {
                    tracing::info!(
                        order_id = %order.id,
                        driver_id = %driver_id,
                        elapsed,
                        "Driver offline saat menunggu respons"
                    );
                    return PollResult::DriverOffline;
                }
                Ok(true) => {}
                Err(_) => return PollResult::Error,
            }

            if elapsed >= timeout_secs {
                tracing::info!(
                    order_id = %order.id,
                    driver_id = %driver_id,
                    "Driver tidak respond dalam timeout"
                );
                return PollResult::Timeout;
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

        // ── Urutan aman: set Redis dulu, baru update DB ───────────────────────
        // Jika DB gagal atau order sudah diambil driver lain, Redis di-rollback.
        self.location
            .set_driver_order(driver_id, order_id, &order.service_type)
            .await?;

        let affected = match self
            .order_repo
            .assign_driver_atomic(order_id, driver_id)
            .await
        {
            Ok(n) => n,
            Err(e) => {
                let _ = self.location.clear_driver_order(driver_id).await;
                return Err(e);
            }
        };

        if affected == 0 {
            // Order sudah diambil driver lain (race condition) — rollback Redis
            let _ = self.location.clear_driver_order(driver_id).await;
            bail!("Order sudah diambil driver lain");
        }

        let driver_info = self.build_driver_info(driver_id).await;
        if driver_info.is_none() {
            tracing::warn!(
                driver_id,
                "build_driver_info gagal — notifikasi dikirim tanpa info driver"
            );
        }

        let accepted_event =
            make_status_event_override(&order, "driver_accepted", driver_info.clone());
        self.connections
            .send(&order.rider_id, accepted_event.clone(), Priority::Normal);
        self.connections
            .send(driver_id, accepted_event, Priority::Normal);

        let updated = self
            .order_repo
            .find_by_id(order_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Order tidak ditemukan setelah assign"))?;
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
        let event = make_status_event_override(&order, "driver_arrived", None);
        self.connections
            .send(&order.rider_id, event.clone(), Priority::Normal);
        self.connections.send(driver_id, event, Priority::Normal);
        Ok(())
    }

    // ── Driver: mulai trip ────────────────────────────────────────────────────
    pub async fn start_trip(&self, driver_id: &str, order_id: &str) -> Result<()> {
        let order = self
            .get_order_for_driver(driver_id, order_id, "driver_arrived")
            .await?;
        self.order_repo.update_status(order_id, "on_trip").await?;
        let event = make_status_event_override(&order, "on_trip", None);
        self.connections
            .send(&order.rider_id, event.clone(), Priority::Normal);
        self.connections.send(driver_id, event, Priority::Normal);
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
        let event = make_status_event_override(&order, "completed", None);
        self.connections
            .send(&order.rider_id, event.clone(), Priority::Normal);
        self.connections.send(driver_id, event, Priority::Normal);
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
                role,
                user_id,
                order.rider_id,
                order.driver_id
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
        // Driver hanya bisa cancel saat driver_accepted (belum mulai trip)
        if role == "driver" && order.status != "driver_accepted" {
            bail!(
                "Order tidak bisa dicancel karena status masih {}",
                order.status
            );
        }

        self.order_repo.cancel(order_id, reason.as_deref()).await?;

        let event = make_status_event_override(&order, "cancelled", None);

        if role == "driver" {
            // Bersihkan Redis driver lalu notify rider
            let _ = self.location.clear_driver_order(user_id).await;
            self.connections
                .send(&order.rider_id, event.clone(), Priority::Normal);
        }
        self.connections.send(user_id, event, Priority::Normal);
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

        let mut result: Vec<NearbyOrder> = orders
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
            .collect();

        // Sort terdekat dulu, batasi jumlah hasil agar tidak membanjiri client
        result.sort_by(|a, b| {
            a.distance_to_pickup_m
                .partial_cmp(&b.distance_to_pickup_m)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        result.truncate(NEARBY_ORDER_LIMIT);

        Ok(result)
    }

    /// Broadcast lokasi driver ke rider yang aktif dalam order.
    /// Verifikasi driver_id untuk mencegah spoofing lokasi dari pihak tidak berwenang.
    pub async fn broadcast_driver_location(
        &self,
        driver_id: &str,
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

        // Verifikasi pengirim adalah driver yang terassign ke order ini
        if order.driver_id.as_deref() != Some(driver_id) {
            bail!("Tidak diizinkan: bukan driver untuk order ini");
        }

        // Update lokasi hanya relevan saat driver dalam perjalanan
        if !matches!(
            order.status.as_str(),
            "driver_accepted" | "driver_arrived" | "on_trip"
        ) {
            bail!("Order tidak dalam status yang memerlukan update lokasi");
        }

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
        self.order_repo.find_active_for_rider(user_id).await
    }

    pub async fn get_active_order_for_driver(&self, driver_id: &str) -> Result<Option<Order>> {
        self.order_repo.find_active_for_driver(driver_id).await
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
        if tip_amount < 0.0 {
            bail!("Tip tidak boleh negatif");
        }
        if rating == 0 || rating > 5 {
            bail!("Rating harus antara 1–5");
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

// ── PollResult ────────────────────────────────────────────────────────────────

/// Hasil dari polling respons driver setelah offer dikirim.
#[derive(Debug)]
enum PollResult {
    /// Order sudah tidak searching — driver accept atau order dicancel
    Accepted,
    /// Order tidak lagi ada di DB
    OrderGone,
    /// Driver terputus saat menunggu
    DriverOffline,
    /// Driver tidak merespons dalam batas waktu
    Timeout,
    /// Error saat polling (DB atau Redis)
    Error,
}
