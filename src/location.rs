// ===== LOCATION SERVICE - PRODUCTION v2.1 =====
//
// Key design:
//   geo:{service_type}              → Redis GEO sorted set, semua driver (cleanup & stats)
//   geo:{service_type}:ready        → Redis GEO sorted set, hanya driver available
//   {driver_id}:loc                 → SETEX 30s, proto bytes (source of truth online)
//   {driver_id}:order               → SETEX 4h, order_id jika driver busy
//   {driver_id}:svc                 → SETEX 30s, service_type aktif
//
// Invariant geo:ready:
//   Driver masuk :ready  → heartbeat, HANYA jika tidak sedang busy (cek order)
//   Driver keluar :ready → set_driver_order (ZREM atomic dengan SET order)
//   Driver re-masuk :ready → heartbeat berikutnya setelah order clear
//
// Race condition heartbeat vs order:
//   Diselesaikan dengan cek EXISTS {driver}:order di heartbeat.
//   Mahal? Ya, tapi hanya 1 GET per heartbeat per driver — jauh lebih murah
//   dari double dispatch. Trade-off yang benar untuk ride-hailing.
//
// Cluster-safe: semua key per-driver pakai hashtag {driver_id} agar
// multi-key ops berada di slot yang sama.

use anyhow::{Context, Result};
use prost::Message;
use redis::{aio::ConnectionManager, AsyncCommands};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::{models::order::DriverLocation, proto::order::DriverLocationMeta};

// ── Constants ─────────────────────────────────────────────────────────────────

const GEO_PREFIX: &str = "geo:";

const META_TTL_SECS: u64 = 30;
const ORDER_TTL_SECS: u64 = 3600 * 4;

const SEARCH_RADIUS_KM: f64 = 5.0;
const SEARCH_RADIUS_EXPAND_KM: f64 = 10.0;
const MIN_DRIVERS: usize = 3;
const MAX_DRIVERS: usize = 10;

// +5 buffer untuk absorb driver offline/leak yang lolos GEOSEARCH :ready.
// Tidak lagi *3 karena :ready sudah pre-filter busy drivers.
const CANDIDATE_FETCH: usize = MAX_DRIVERS + 5;

// Safety net EXISTS check hanya untuk N kandidat teratas.
const EXISTS_SAFETY_CHECK_N: usize = 3;

// ── LocationService ───────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct LocationService {
    redis: ConnectionManager,
}

impl LocationService {
    pub fn new(redis: ConnectionManager) -> Self {
        Self { redis }
    }

    // ── Key Helpers ───────────────────────────────────────────────────────────

    #[inline]
    fn geo_key(service_type: &str) -> String {
        format!("{}{}", GEO_PREFIX, service_type)
    }

    #[inline]
    fn geo_ready_key(service_type: &str) -> String {
        format!("{}{}:ready", GEO_PREFIX, service_type)
    }

    #[inline]
    fn loc_key(driver_id: &str) -> String {
        format!("{{{}}}:loc", driver_id)
    }

    #[inline]
    fn order_key(driver_id: &str) -> String {
        format!("{{{}}}:order", driver_id)
    }

    #[inline]
    fn svc_key(driver_id: &str) -> String {
        format!("{{{}}}:svc", driver_id)
    }

    // ── Validation ────────────────────────────────────────────────────────────

    fn validate_coords(lat: f64, lng: f64) -> Result<()> {
        if lat.is_nan() || lng.is_nan() || lat.is_infinite() || lng.is_infinite() {
            anyhow::bail!("NaN/Inf coordinates: lat={lat}, lng={lng}");
        }
        if !(-90.0..=90.0).contains(&lat) {
            anyhow::bail!("Invalid latitude: {lat}");
        }
        if !(-180.0..=180.0).contains(&lng) {
            anyhow::bail!("Invalid longitude: {lng}");
        }
        Ok(())
    }

    fn validate_driver_id(driver_id: &str) -> Result<()> {
        if driver_id.is_empty() || driver_id.len() > 128 {
            anyhow::bail!("Invalid driver_id length: {}", driver_id.len());
        }
        Ok(())
    }

    // ── Update Location ───────────────────────────────────────────────────────

    /// Atomic pipeline dengan order-aware :ready insertion.
    ///
    /// Flow:
    ///   1. Cek service_type lama → jika beda, ZREM dari kedua geo key lama
    ///   2. Cek EXISTS {driver}:order → jika busy, skip GEOADD :ready
    ///   3. GEOADD ke geo global (selalu)
    ///   4. GEOADD ke geo:ready (hanya jika tidak busy)
    ///   5. SETEX loc + SETEX svc
    ///
    /// Kenapa GET order di heartbeat?
    ///
    /// Alternatif: `{driver}:was_ready` flag menambah key baru dengan
    /// lifetime yang perlu di-manage tersendiri (cleanup, TTL, dll).
    /// Satu GET per heartbeat lebih predictable dan tidak ada state ekstra.
    ///
    /// Cost: 1 GET + 1 pipeline per heartbeat (~5 detik per driver).
    /// Untuk 10.000 driver aktif: 2.000 GET/detik — acceptable untuk
    /// mencegah double dispatch yang cost-nya jauh lebih tinggi.
    pub async fn update_driver_location(
        &self,
        driver_id: &str,
        lat: f64,
        lng: f64,
        heading: Option<f32>,
        speed: Option<f32>,
        service_type: &str,
    ) -> Result<()> {
        Self::validate_coords(lat, lng)?;
        Self::validate_driver_id(driver_id)?;

        let mut redis = self.redis.clone();
        let svc_key = Self::svc_key(driver_id);
        let loc_key = Self::loc_key(driver_id);
        let order_key = Self::order_key(driver_id);
        let new_geo_key = Self::geo_key(service_type);
        let new_ready_key = Self::geo_ready_key(service_type);

        // Dua GET sekaligus: svc lama + order status
        // Ini satu-satunya roundtrip sebelum pipeline utama
        let (old_svc, has_order): (Option<String>, bool) = {
            let mut pipe = redis::pipe();
            pipe.get(&svc_key).exists(&order_key);
            let (svc, ord): (Option<String>, i64) =
                pipe.query_async(&mut redis).await.unwrap_or_else(|e| {
                    tracing::error!(
                        driver = driver_id,
                        error = ?e,
                        "redis failed fetching svc/order state, assuming not busy"
                    );
                    (None, 0)
                });
            (svc, ord == 1)
        };

        let switched = old_svc
            .as_deref()
            .map(|s| s != service_type)
            .unwrap_or(false);

        let meta = DriverLocationMeta {
            lat,
            lng,
            heading: heading.unwrap_or_default() as f64,
            speed: speed.unwrap_or_default() as f64,
            ts: chrono::Utc::now().timestamp_millis(),
        };
        let bytes = meta.encode_to_vec();

        let mut pipe = redis::pipe();
        pipe.atomic();

        if switched {
            let old_svc_str = old_svc.as_deref().unwrap();
            pipe.cmd("ZREM")
                .arg(Self::geo_key(old_svc_str))
                .arg(driver_id)
                .cmd("ZREM")
                .arg(Self::geo_ready_key(old_svc_str))
                .arg(driver_id);

            tracing::debug!(
                driver = driver_id,
                from = old_svc_str,
                to = service_type,
                "driver switched service type"
            );
        }

        // Selalu update geo global dan metadata
        pipe.cmd("GEOADD")
            .arg(&new_geo_key)
            .arg(lng)
            .arg(lat)
            .arg(driver_id)
            .cmd("SETEX")
            .arg(&loc_key)
            .arg(META_TTL_SECS)
            .arg(&bytes)
            .cmd("SETEX")
            .arg(&svc_key)
            .arg(META_TTL_SECS)
            .arg(service_type);

        // :ready hanya jika tidak sedang busy
        if !has_order {
            pipe.cmd("GEOADD")
                .arg(&new_ready_key)
                .arg(lng)
                .arg(lat)
                .arg(driver_id);
        } else {
            tracing::debug!(
                driver = driver_id,
                "heartbeat skipped :ready insertion — driver has active order"
            );
        }

        pipe.query_async(&mut redis)
            .await
            .map(|_: ()| ())
            .context("Failed to update driver location")?;

        Ok(())
    }

    // ── Remove Driver ─────────────────────────────────────────────────────────

    pub async fn remove_driver(&self, driver_id: &str, service_type: &str) -> Result<()> {
        Self::validate_driver_id(driver_id)?;

        redis::pipe()
            .atomic()
            .cmd("ZREM")
            .arg(Self::geo_key(service_type))
            .arg(driver_id)
            .cmd("ZREM")
            .arg(Self::geo_ready_key(service_type))
            .arg(driver_id)
            .cmd("DEL")
            .arg(Self::loc_key(driver_id))
            .cmd("DEL")
            .arg(Self::svc_key(driver_id))
            .query_async(&mut self.redis.clone())
            .await
            .map(|_: ()| ())
            .context("Failed to remove driver")
    }

    // ── Find Nearby Drivers ───────────────────────────────────────────────────

    /// Alur:
    ///   1. GEOSEARCH di :ready (sudah exclude busy + offline sebagian besar)
    ///   2. Expand jika hasil < MIN_DRIVERS
    ///   3. MGET batch loc → filter driver offline (TTL expired)
    ///   4. EXISTS safety check untuk top-N → filter race condition leak
    ///   5. Collect MAX_DRIVERS pertama
    pub async fn find_nearby_drivers(
        &self,
        lat: f64,
        lng: f64,
        service_type: &str,
    ) -> Result<Vec<(String, f64)>> {
        Self::validate_coords(lat, lng)?;

        // Hard timeout 4s — GEOSEARCH besar tidak boleh blok search_driver_loop
        tokio::time::timeout(
            std::time::Duration::from_secs(4),
            self.find_nearby_drivers_inner(lat, lng, service_type),
        )
        .await
        .map_err(|_| {
            tracing::error!(
                service_type,
                lat,
                lng,
                "find_nearby_drivers timed out (>4s)"
            );
            anyhow::anyhow!("find_nearby_drivers timeout")
        })?
    }

    async fn find_nearby_drivers_inner(
        &self,
        lat: f64,
        lng: f64,
        service_type: &str,
    ) -> Result<Vec<(String, f64)>> {
        // 1 koneksi untuk seluruh flow — tidak buka clone baru di setiap sub-call
        let mut redis = self.redis.clone();

        let mut candidates =
            Self::geosearch_ready_conn(&mut redis, lat, lng, service_type, SEARCH_RADIUS_KM)
                .await?;

        if candidates.len() < MIN_DRIVERS {
            candidates = Self::geosearch_ready_conn(
                &mut redis,
                lat,
                lng,
                service_type,
                SEARCH_RADIUS_EXPAND_KM,
            )
            .await?;
        }

        if candidates.is_empty() {
            return Ok(vec![]);
        }

        // MGET loc — filter offline
        let loc_keys: Vec<String> = candidates.iter().map(|(id, _)| Self::loc_key(id)).collect();

        let loc_data: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(&loc_keys)
            .query_async(&mut redis)
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = ?e, "redis MGET loc failed in find_nearby_drivers");
                vec![None; candidates.len()]
            });

        let online_candidates: Vec<(&str, f64)> = candidates
            .iter()
            .enumerate()
            .filter_map(|(i, (id, dist_m))| {
                if loc_data.get(i).and_then(|v| v.as_ref()).is_some() {
                    Some((id.as_str(), *dist_m))
                } else {
                    None
                }
            })
            .collect();

        if online_candidates.is_empty() {
            return Ok(vec![]);
        }

        // EXISTS safety check — hanya top-N
        let safety_n = EXISTS_SAFETY_CHECK_N.min(online_candidates.len());
        let top_n = &online_candidates[..safety_n];

        let mut pipe = redis::pipe();
        for &(id, _) in top_n {
            pipe.cmd("EXISTS").arg(Self::order_key(id));
        }

        let order_exists: Vec<i64> = pipe.query_async(&mut redis).await.unwrap_or_else(|e| {
            tracing::error!(error = ?e, "redis EXISTS order failed in safety check");
            vec![0; safety_n]
        });

        let busy_ids: std::collections::HashSet<&str> = top_n
            .iter()
            .zip(order_exists.iter())
            .filter(|(_, &exists)| exists == 1)
            .map(|(&(id, _), _)| id)
            .collect();

        if !busy_ids.is_empty() {
            tracing::warn!(
                count = busy_ids.len(),
                "safety check caught leak: driver in :ready with active order"
            );
        }

        let mut result = Vec::with_capacity(MAX_DRIVERS);
        for &(driver_id, dist_m) in &online_candidates {
            if busy_ids.contains(driver_id) {
                continue;
            }
            result.push((driver_id.to_string(), dist_m));
            if result.len() >= MAX_DRIVERS {
                break;
            }
        }

        Ok(result)
    }

    // Terima &mut ConnectionManager agar bisa share 1 koneksi dengan caller
    async fn geosearch_ready_conn(
        redis: &mut ConnectionManager,
        lat: f64,
        lng: f64,
        service_type: &str,
        radius_km: f64,
    ) -> Result<Vec<(String, f64)>> {
        let geo_key = Self::geo_ready_key(service_type);

        let raw: Vec<Vec<String>> = redis::cmd("GEOSEARCH")
            .arg(&geo_key)
            .arg("FROMLONLAT")
            .arg(lng)
            .arg(lat)
            .arg("BYRADIUS")
            .arg(radius_km)
            .arg("km")
            .arg("ASC")
            .arg("COUNT")
            .arg(CANDIDATE_FETCH)
            .arg("WITHDIST")
            .query_async(redis)
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, service_type, radius_km, "GEOSEARCH failed");
                anyhow::anyhow!("GEOSEARCH failed: {e}")
            })?;

        Ok(raw
            .into_iter()
            .filter_map(|item| {
                if item.len() != 2 {
                    return None;
                }
                let dist_m = item[1].parse::<f64>().ok()? * 1000.0;
                Some((item[0].clone(), dist_m))
            })
            .collect())
    }

    // ── Get Driver Location ───────────────────────────────────────────────────

    pub async fn get_driver_location(&self, driver_id: &str) -> Result<Option<DriverLocation>> {
        Self::validate_driver_id(driver_id)?;

        let bytes: Option<Vec<u8>> = self
            .redis
            .clone()
            .get(Self::loc_key(driver_id))
            .await
            .context("Failed to get driver location")?;

        let Some(b) = bytes else { return Ok(None) };

        let meta =
            DriverLocationMeta::decode(&b[..]).context("Failed to decode DriverLocationMeta")?;

        Ok(Some(DriverLocation {
            driver_id: driver_id.to_string(),
            lat: meta.lat,
            lng: meta.lng,
            heading: Some(meta.heading as f32),
            speed: Some(meta.speed as f32),
            updated_at: meta.ts,
        }))
    }

    // ── Fare Estimation ───────────────────────────────────────────────────────

    pub async fn estimate_price(
        &self,
        pickup_lat: f64,
        pickup_lng: f64,
        dest_lat: f64,
        dest_lng: f64,
        service_type: &str,
    ) -> Result<Option<f64>> {
        let drivers = self
            .find_nearby_drivers(pickup_lat, pickup_lng, service_type)
            .await?;

        let Some((_, driver_to_pickup_m)) = drivers.first() else {
            return Ok(None);
        };

        let trip_m = haversine_m(pickup_lat, pickup_lng, dest_lat, dest_lng);
        let total_m = driver_to_pickup_m + trip_m;
        let duration_sec = (total_m / 8.0) as u64;

        Ok(Some(Self::calculate_fare(
            total_m,
            duration_sec,
            service_type,
            1.0,
        )))
    }

    fn calculate_fare(distance_m: f64, duration_sec: u64, service_type: &str, surge: f64) -> f64 {
        let (base, per_km, per_min) = match service_type {
            "mobil" => (5000.0, 3000.0, 500.0),
            "motor" => (3000.0, 1500.0, 300.0),
            "food" => (2000.0, 1000.0, 200.0),
            _ => (0.0, 0.0, 0.0),
        };

        let fare = base + (distance_m / 1000.0) * per_km + (duration_sec as f64 / 60.0) * per_min;
        (fare * surge).max(8000.0)
    }

    // ── Order Tracking ────────────────────────────────────────────────────────

    /// Atomic: SET order + ZREM dari :ready.
    ///
    /// Ini satu-satunya tempat driver keluar dari :ready secara eksplisit.
    /// Heartbeat akan skip re-insert selama order key masih hidup.
    pub async fn set_driver_order(
        &self,
        driver_id: &str,
        order_id: &str,
        service_type: &str,
    ) -> Result<()> {
        Self::validate_driver_id(driver_id)?;
        if order_id.is_empty() {
            anyhow::bail!("order_id cannot be empty");
        }

        redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(Self::order_key(driver_id))
            .arg(order_id)
            .arg("EX")
            .arg(ORDER_TTL_SECS)
            .cmd("ZREM")
            .arg(Self::geo_ready_key(service_type))
            .arg(driver_id)
            .query_async(&mut self.redis.clone())
            .await
            .map(|_: ()| ())
            .context("Failed to set driver order")?;

        tracing::debug!(
            driver = driver_id,
            order = order_id,
            "driver assigned, removed from :ready"
        );
        Ok(())
    }

    pub async fn get_driver_order(&self, driver_id: &str) -> Result<Option<String>> {
        Self::validate_driver_id(driver_id)?;
        self.redis
            .clone()
            .get(Self::order_key(driver_id))
            .await
            .context("Failed to get driver order")
    }

    /// Hanya DEL order key.
    ///
    /// Driver re-enter :ready via heartbeat berikutnya (~5 detik).
    /// Tidak perlu GEOADD di sini karena: (1) koordinat yang tersimpan
    /// mungkin sudah stale, (2) heartbeat selalu bawa koordinat terbaru.
    pub async fn clear_driver_order(&self, driver_id: &str) -> Result<()> {
        Self::validate_driver_id(driver_id)?;
        self.redis
            .clone()
            .del(Self::order_key(driver_id))
            .await
            .map(|_: ()| ())
            .context("Failed to clear driver order")?;

        tracing::debug!(
            driver = driver_id,
            "order cleared, re-enter :ready on next heartbeat"
        );
        Ok(())
    }

    // ── Stale GEO Cleanup ─────────────────────────────────────────────────────

    /// Cleanup dengan batching untuk menghindari O(N) spike di Redis.
    ///
    /// ZRANGE 0 -1 dengan 100k driver adalah satu response ~4MB.
    /// Batch per CLEANUP_BATCH_SIZE menghindari memory spike dan
    /// memberi Redis napas antara batch.
    pub async fn cleanup_stale_drivers(&self, service_type: &str) -> Result<CleanupReport> {
        const CLEANUP_BATCH_SIZE: isize = 500;

        let mut redis = self.redis.clone();
        let geo_key = Self::geo_key(service_type);
        let ready_key = Self::geo_ready_key(service_type);

        let mut report = CleanupReport::default();
        let mut cursor: isize = 0;

        loop {
            let batch: Vec<String> = redis::cmd("ZRANGE")
                .arg(&geo_key)
                .arg(cursor)
                .arg(cursor + CLEANUP_BATCH_SIZE - 1)
                .query_async(&mut redis)
                .await
                .unwrap_or_else(|e| {
                    tracing::error!(
                        error = ?e,
                        service_type,
                        cursor,
                        "ZRANGE failed in cleanup batch"
                    );
                    vec![]
                });

            if batch.is_empty() {
                break;
            }

            let is_last_batch = batch.len() < CLEANUP_BATCH_SIZE as usize;

            // Cek mana yang offline
            let loc_keys: Vec<String> = batch.iter().map(|id| Self::loc_key(id)).collect();
            let loc_data: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
                .arg(&loc_keys)
                .query_async(&mut redis)
                .await
                .unwrap_or_else(|e| {
                    tracing::error!(error = ?e, "MGET loc failed in cleanup batch");
                    vec![None; batch.len()]
                });

            let stale: Vec<&str> = batch
                .iter()
                .zip(loc_data.iter())
                .filter(|(_, data)| data.is_none())
                .map(|(id, _)| id.as_str())
                .collect();

            if !stale.is_empty() {
                let mut pipe = redis::pipe();
                pipe.atomic();
                for id in &stale {
                    pipe.cmd("ZREM").arg(&geo_key).arg(*id);
                    pipe.cmd("ZREM").arg(&ready_key).arg(*id);
                }
                pipe.query_async(&mut redis)
                    .await
                    .map(|_: ()| ())
                    .unwrap_or_else(|e| {
                        tracing::error!(error = ?e, "ZREM stale failed");
                    });

                report.stale_removed += stale.len();
            }

            if is_last_batch {
                break;
            }
            cursor += CLEANUP_BATCH_SIZE;
        }

        if report.stale_removed > 0 {
            tracing::info!(
                count = report.stale_removed,
                service_type,
                "removed stale drivers from geo indexes"
            );
        }

        // Leak detection: driver di :ready yang punya order aktif
        // Dengan fix heartbeat, ini seharusnya mendekati nol.
        // Jika konsisten tinggi → sinyal bug di set_driver_order caller.
        let ready_drivers: Vec<String> = redis::cmd("ZRANGE")
            .arg(&ready_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut redis)
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = ?e, "ZRANGE :ready failed in leak detection");
                vec![]
            });

        if !ready_drivers.is_empty() {
            let mut pipe = redis::pipe();
            for id in &ready_drivers {
                pipe.cmd("EXISTS").arg(Self::order_key(id));
            }
            let order_exists: Vec<i64> = pipe.query_async(&mut redis).await.unwrap_or_else(|e| {
                tracing::error!(error = ?e, "EXISTS check failed in leak detection");
                vec![0; ready_drivers.len()]
            });

            let leaked: Vec<&str> = ready_drivers
                .iter()
                .zip(order_exists.iter())
                .filter(|(_, &exists)| exists == 1)
                .map(|(id, _)| id.as_str())
                .collect();

            report.leak_detected = leaked.len();

            if !leaked.is_empty() {
                tracing::warn!(
                    count = report.leak_detected,
                    service_type,
                    drivers = ?leaked,
                    "LEAK: drivers in :ready with active order — check set_driver_order callers"
                );
            }
        }

        Ok(report)
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    pub async fn count_active_drivers(&self, service_type: &str) -> Result<usize> {
        let mut redis = self.redis.clone();
        let all: Vec<String> = redis::cmd("ZRANGE")
            .arg(Self::geo_key(service_type))
            .arg(0)
            .arg(-1)
            .query_async(&mut redis)
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = ?e, "ZRANGE failed in count_active_drivers");
                vec![]
            });

        if all.is_empty() {
            return Ok(0);
        }

        let loc_keys: Vec<String> = all.iter().map(|id| Self::loc_key(id)).collect();
        let data: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(&loc_keys)
            .query_async(&mut redis)
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = ?e, "MGET failed in count_active_drivers");
                vec![]
            });

        Ok(data.iter().filter(|v| v.is_some()).count())
    }

    /// Hitung driver yang benar-benar ready untuk dispatch (O(1)).
    pub async fn count_ready_drivers(&self, service_type: &str) -> Result<usize> {
        redis::cmd("ZCARD")
            .arg(Self::geo_ready_key(service_type))
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "ZCARD failed in count_ready_drivers");
                anyhow::anyhow!("Failed to count ready drivers: {e}")
            })
    }
}

// ── Cleanup Report ────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct CleanupReport {
    pub stale_removed: usize,
    pub leak_detected: usize,
}

// ── Background Cleanup Task ───────────────────────────────────────────────────

const SERVICE_TYPES: &[&str] = &["motor", "mobil", "food", "send", "nebeng"];

pub fn spawn_cleanup_task(
    svc: std::sync::Arc<LocationService>,
    interval_secs: u64,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    run_cleanup(&svc).await;
                }
                _ = cancel.cancelled() => {
                    tracing::info!("location cleanup task: shutdown signal received");
                    break;
                }
            }
        }
    })
}

async fn run_cleanup(svc: &LocationService) {
    let mut total_stale = 0usize;
    let mut total_leak = 0usize;

    for &st in SERVICE_TYPES {
        match svc.cleanup_stale_drivers(st).await {
            Ok(report) => {
                total_stale += report.stale_removed;
                total_leak += report.leak_detected;
            }
            Err(e) => tracing::error!(service_type = st, error = ?e, "cleanup failed"),
        }
    }

    if total_stale > 0 {
        tracing::info!(total_stale, "stale driver entries removed");
    }

    if total_leak > 0 {
        // Leak metric — push ke Prometheus/Datadog via counter jika tersedia:
        // metrics::gauge!("location.geo_ready_leak", total_leak as f64);
        tracing::warn!(total_leak, "geo:ready leak detected this cycle");
    }
}

// ── Haversine ─────────────────────────────────────────────────────────────────

pub fn haversine_m(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlng / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().asin()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_format_cluster_safe() {
        assert_eq!(LocationService::loc_key("driver123"), "{driver123}:loc");
        assert_eq!(LocationService::order_key("driver123"), "{driver123}:order");
        assert_eq!(LocationService::svc_key("driver123"), "{driver123}:svc");
    }

    #[test]
    fn test_geo_keys() {
        assert_eq!(LocationService::geo_key("motor"), "geo:motor");
        assert_eq!(LocationService::geo_ready_key("motor"), "geo:motor:ready");
        assert_eq!(LocationService::geo_ready_key("food"), "geo:food:ready");
    }

    #[test]
    fn test_validate_coords_ok() {
        assert!(LocationService::validate_coords(-6.2088, 106.8456).is_ok());
        assert!(LocationService::validate_coords(0.0, 0.0).is_ok());
        assert!(LocationService::validate_coords(-90.0, -180.0).is_ok());
        assert!(LocationService::validate_coords(90.0, 180.0).is_ok());
    }

    #[test]
    fn test_validate_coords_err() {
        assert!(LocationService::validate_coords(91.0, 0.0).is_err());
        assert!(LocationService::validate_coords(0.0, 181.0).is_err());
        assert!(LocationService::validate_coords(-90.1, 0.0).is_err());
        assert!(LocationService::validate_coords(0.0, -180.1).is_err());
        assert!(LocationService::validate_coords(f64::NAN, 0.0).is_err());
        assert!(LocationService::validate_coords(0.0, f64::INFINITY).is_err());
    }

    #[test]
    fn test_validate_driver_id() {
        assert!(LocationService::validate_driver_id("").is_err());
        assert!(LocationService::validate_driver_id(&"a".repeat(129)).is_err());
        assert!(LocationService::validate_driver_id(&"a".repeat(128)).is_ok());
        assert!(LocationService::validate_driver_id("driver-abc-123").is_ok());
    }

    #[test]
    fn test_haversine_jakarta_bandung() {
        let d = haversine_m(-6.2088, 106.8456, -6.9175, 107.6191);
        assert!((d - 150_000.0).abs() < 10_000.0, "got {d:.0}m");
    }

    #[test]
    fn test_haversine_same_point() {
        let d = haversine_m(-6.2088, 106.8456, -6.2088, 106.8456);
        assert!(d < 0.001, "same point should be ~0m, got {d}");
    }

    #[test]
    fn test_calculate_fare_minimum_enforced() {
        let fare = LocationService::calculate_fare(100.0, 60, "motor", 1.0);
        assert_eq!(fare, 8000.0);
    }

    #[test]
    fn test_calculate_fare_mobil_10km() {
        // base=5000, 10km*3000=30000, 20min*500=10000 → 45000
        let fare = LocationService::calculate_fare(10_000.0, 1200, "mobil", 1.0);
        assert!((fare - 45_000.0).abs() < 1.0, "got {fare}");
    }

    #[test]
    fn test_calculate_fare_motor() {
        // base=3000, 5km*1500=7500, 10min*300=3000 → 13500
        let fare = LocationService::calculate_fare(5_000.0, 600, "motor", 1.0);
        assert!((fare - 13_500.0).abs() < 1.0, "got {fare}");
    }

    #[test]
    fn test_calculate_fare_surge() {
        let normal = LocationService::calculate_fare(10_000.0, 1200, "motor", 1.0);
        let surge = LocationService::calculate_fare(10_000.0, 1200, "motor", 1.5);
        assert!((surge - normal * 1.5).abs() < 1.0);
    }

    #[test]
    fn test_calculate_fare_unknown_service() {
        let fare = LocationService::calculate_fare(10_000.0, 1200, "unknown", 1.0);
        assert_eq!(fare, 8000.0);
    }

    #[test]
    fn test_constants_sanity() {
        assert!(EXISTS_SAFETY_CHECK_N <= MAX_DRIVERS);
        assert!(CANDIDATE_FETCH >= MAX_DRIVERS);
        assert!(MIN_DRIVERS < MAX_DRIVERS);
        assert!(SEARCH_RADIUS_KM < SEARCH_RADIUS_EXPAND_KM);
        // Buffer harus ada
        assert!(
            CANDIDATE_FETCH > MAX_DRIVERS,
            "no buffer for offline/leak filtering"
        );
    }

    #[test]
    fn test_cleanup_report_default() {
        let r = CleanupReport::default();
        assert_eq!(r.stale_removed, 0);
        assert_eq!(r.leak_detected, 0);
    }
}
