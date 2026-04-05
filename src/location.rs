// ===== LOCATION SERVICE - PRODUCTION v3.0 =====
//
// Key design:
//   geo:{service_type}              → Redis GEO sorted set, semua driver (cleanup & stats)
//   geo:{service_type}:ready        → Redis GEO sorted set, hanya driver available
//   {driver_id}:loc                 → SETEX 30s, proto bytes (source of truth online)
//   {driver_id}:order               → SETEX 4h, order_id jika driver busy
//   {driver_id}:svc                 → SETEX 30s, service_type aktif
//
// Perubahan v3.0 vs v2.1:
//   1. ZSCAN menggantikan ZRANGE+cursor di cleanup (aman saat data berubah)
//   2. CANDIDATE_FETCH naik ke 50 (cukup untuk area padat)
//   3. with_retry wraps semua operasi Redis kritis (max 3 attempts, exp backoff)
//   4. Safety EXISTS check diperluas ke seluruh online_candidates (bukan top-3)
//   5. find_nearby_drivers_all_categories → concurrent query semua service type
//   6. Validasi service_type dengan allowlist
//   7. NearbyDriver struct untuk output yang lebih ekspresif

use anyhow::{Context, Result};
use prost::Message;
use redis::{aio::ConnectionManager, AsyncCommands};
use std::{collections::HashMap, future::Future, sync::Arc};
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

// Fix #2: naik dari 15 → 50. Area padat (Jakarta pusat jam peak)
// bisa punya 200+ driver dalam 5km. CANDIDATE_FETCH 15 terlalu kecil;
// setelah filter offline + safety check bisa habis sebelum dapat MIN_DRIVERS.
const CANDIDATE_FETCH: usize = 50;

// Retry config untuk operasi Redis kritis
const REDIS_MAX_ATTEMPTS: u32 = 3;
const REDIS_RETRY_BASE_MS: u64 = 50; // backoff: 50ms, 100ms, 200ms

/// Semua service type yang valid. Dipakai untuk validasi input
/// dan untuk query all-categories di find_nearby_drivers_all_categories.
pub const SERVICE_TYPES: &[&str] = &["motor", "mobil", "food", "send", "nebeng", "bus", "angkot"];

// ── Structs ───────────────────────────────────────────────────────────────────

/// Driver terdekat dengan metadata service type-nya.
#[derive(Debug, Clone)]
pub struct NearbyDriver {
    pub driver_id: String,
    pub distance_m: f64,
    pub service_type: String,
}

#[derive(Debug, Default)]
pub struct CleanupReport {
    pub stale_removed: usize,
    pub leak_detected: usize,
}

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

    // Fix #6: validasi service_type dengan allowlist.
    // Cegah key injection: "../../etc" atau service_type kosong.
    fn validate_service_type(service_type: &str) -> Result<()> {
        if !SERVICE_TYPES.contains(&service_type) {
            anyhow::bail!(
                "Invalid service_type: '{}'. Allowed: {:?}",
                service_type,
                SERVICE_TYPES
            );
        }
        Ok(())
    }

    // ── Update Location ───────────────────────────────────────────────────────

    /// Atomic pipeline dengan order-aware :ready insertion.
    /// Dibungkus with_retry untuk ketahanan saat Redis blip singkat.
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
        Self::validate_service_type(service_type)?;

        // Capture semua yang dibutuhkan closure sebelum masuk retry
        let driver_id = driver_id.to_string();
        let service_type = service_type.to_string();
        let svc = self.clone();

        with_retry(|| {
            let driver_id = driver_id.clone();
            let service_type = service_type.clone();
            let svc = svc.clone();
            async move {
                svc.update_driver_location_inner(
                    &driver_id,
                    lat,
                    lng,
                    heading,
                    speed,
                    &service_type,
                )
                .await
            }
        })
        .await
    }

    async fn update_driver_location_inner(
        &self,
        driver_id: &str,
        lat: f64,
        lng: f64,
        heading: Option<f32>,
        speed: Option<f32>,
        service_type: &str,
    ) -> Result<()> {
        let mut redis = self.redis.clone();
        let svc_key = Self::svc_key(driver_id);
        let loc_key = Self::loc_key(driver_id);
        let order_key = Self::order_key(driver_id);
        let new_geo_key = Self::geo_key(service_type);
        let new_ready_key = Self::geo_ready_key(service_type);

        // Satu roundtrip: GET svc lama + EXISTS order
        let (old_svc, has_order): (Option<String>, bool) = {
            let mut pipe = redis::pipe();
            pipe.get(&svc_key).exists(&order_key);
            let (svc, ord): (Option<String>, i64) =
                pipe.query_async(&mut redis).await.unwrap_or_else(|e| {
                    tracing::error!(
                        driver = driver_id,
                        error = ?e,
                        "redis GET svc/EXISTS order failed, assuming not busy"
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
            .context("update_driver_location pipeline failed")
    }

    // ── Remove Driver ─────────────────────────────────────────────────────────

    pub async fn remove_driver(&self, driver_id: &str, service_type: &str) -> Result<()> {
        Self::validate_driver_id(driver_id)?;
        Self::validate_service_type(service_type)?;

        let driver_id = driver_id.to_string();
        let service_type = service_type.to_string();
        let svc = self.clone();

        with_retry(|| {
            let driver_id = driver_id.clone();
            let service_type = service_type.clone();
            let svc = svc.clone();
            async move {
                redis::pipe()
                    .atomic()
                    .cmd("ZREM")
                    .arg(Self::geo_key(&service_type))
                    .arg(&driver_id)
                    .cmd("ZREM")
                    .arg(Self::geo_ready_key(&service_type))
                    .arg(&driver_id)
                    .cmd("DEL")
                    .arg(Self::loc_key(&driver_id))
                    .cmd("DEL")
                    .arg(Self::svc_key(&driver_id))
                    .query_async(&mut svc.redis.clone())
                    .await
                    .map(|_: ()| ())
                    .context("remove_driver pipeline failed")
            }
        })
        .await
    }

    // ── Find Nearby Drivers (satu service type) ───────────────────────────────

    /// Timeout 4s — GEOSEARCH besar tidak boleh blok search_driver_loop.
    pub async fn find_nearby_drivers(
        &self,
        lat: f64,
        lng: f64,
        service_type: &str,
    ) -> Result<Vec<(String, f64)>> {
        Self::validate_coords(lat, lng)?;
        Self::validate_service_type(service_type)?;

        tokio::time::timeout(
            std::time::Duration::from_secs(4),
            self.find_nearby_inner(lat, lng, service_type),
        )
        .await
        .map_err(|_| {
            tracing::error!(service_type, lat, lng, "find_nearby_drivers timeout >4s");
            anyhow::anyhow!("find_nearby_drivers timeout")
        })?
    }

    // ── Find Nearby Drivers (semua service type) ──────────────────────────────

    /// Query semua service type secara concurrent, gabungkan hasilnya.
    ///
    /// Return: HashMap<service_type, Vec<(driver_id, distance_m)>>
    ///
    /// Use case utama: HomeScreen rider yang butuh tampilkan semua jenis
    /// driver terdekat sekaligus (RustCar, RustBike, RustSend, dll).
    ///
    /// Semua query berjalan concurrent via tokio::join, bukan sequential.
    /// Total latency ≈ latency query terlambat (bukan sum semua query).
    pub async fn find_nearby_drivers_all_categories(
        &self,
        lat: f64,
        lng: f64,
    ) -> Result<HashMap<String, Vec<NearbyDriver>>> {
        Self::validate_coords(lat, lng)?;

        // Spawn concurrent tasks, satu per service type
        let handles: Vec<_> = SERVICE_TYPES
            .iter()
            .map(|&st| {
                let svc = self.clone();
                let st = st.to_string();
                tokio::spawn(async move {
                    let result = svc.find_nearby_inner(lat, lng, &st).await;
                    (st, result)
                })
            })
            .collect();

        let mut map: HashMap<String, Vec<NearbyDriver>> = HashMap::new();

        for handle in handles {
            match handle.await {
                Ok((service_type, Ok(drivers))) => {
                    let nearby: Vec<NearbyDriver> = drivers
                        .into_iter()
                        .map(|(driver_id, distance_m)| NearbyDriver {
                            driver_id,
                            distance_m,
                            service_type: service_type.clone(),
                        })
                        .collect();
                    map.insert(service_type, nearby);
                }
                Ok((service_type, Err(e))) => {
                    tracing::error!(
                        service_type,
                        error = ?e,
                        "find_nearby_all: query failed for service type"
                    );
                    // Insert empty Vec agar caller tahu key ada tapi kosong
                    map.insert(service_type, vec![]);
                }
                Err(join_err) => {
                    tracing::error!(error = ?join_err, "find_nearby_all: task panicked");
                }
            }
        }

        Ok(map)
    }

    /// Flat list semua driver terdekat dari semua service type, diurutkan by distance.
    /// Lebih praktis untuk use case "tampilkan N driver terdekat apapun jenisnya".
    pub async fn find_nearby_drivers_flat(&self, lat: f64, lng: f64) -> Result<Vec<NearbyDriver>> {
        let map = self.find_nearby_drivers_all_categories(lat, lng).await?;

        let mut all: Vec<NearbyDriver> = map.into_values().flatten().collect();
        all.sort_by(|a, b| {
            a.distance_m
                .partial_cmp(&b.distance_m)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(all)
    }

    // ── Inner Search ──────────────────────────────────────────────────────────

    async fn find_nearby_inner(
        &self,
        lat: f64,
        lng: f64,
        service_type: &str,
    ) -> Result<Vec<(String, f64)>> {
        let mut redis = self.redis.clone();

        let mut candidates =
            Self::geosearch_ready(&mut redis, lat, lng, service_type, SEARCH_RADIUS_KM).await?;

        if candidates.len() < MIN_DRIVERS {
            candidates =
                Self::geosearch_ready(&mut redis, lat, lng, service_type, SEARCH_RADIUS_EXPAND_KM)
                    .await?;
        }

        if candidates.is_empty() {
            return Ok(vec![]);
        }

        // MGET loc — filter driver yang sudah offline (TTL expired)
        let loc_keys: Vec<String> = candidates.iter().map(|(id, _)| Self::loc_key(id)).collect();
        let loc_data: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(&loc_keys)
            .query_async(&mut redis)
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = ?e, "MGET loc failed in find_nearby");
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

        // Fix #4: EXISTS safety check untuk SEMUA online_candidates, bukan hanya top-3.
        // Cost: 1 EXISTS per kandidat dalam 1 pipeline.
        // Dengan CANDIDATE_FETCH=50 dan MGET filter offline, online_candidates
        // biasanya jauh lebih kecil. Trade-off tetap wajar.
        let mut pipe = redis::pipe();
        for &(id, _) in &online_candidates {
            pipe.cmd("EXISTS").arg(Self::order_key(id));
        }

        let order_exists: Vec<i64> = pipe.query_async(&mut redis).await.unwrap_or_else(|e| {
            tracing::error!(error = ?e, "EXISTS order batch failed in safety check");
            vec![0; online_candidates.len()]
        });

        let busy_ids: std::collections::HashSet<&str> = online_candidates
            .iter()
            .zip(order_exists.iter())
            .filter(|(_, &exists)| exists == 1)
            .map(|(&(id, _), _)| id)
            .collect();

        if !busy_ids.is_empty() {
            tracing::warn!(
                count = busy_ids.len(),
                service_type,
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

    async fn geosearch_ready(
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
                // GEOSEARCH WITHDIST → km, konversi ke meter
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
        Self::validate_service_type(service_type)?;

        let drivers = self
            .find_nearby_drivers(pickup_lat, pickup_lng, service_type)
            .await?;

        let Some((_, driver_to_pickup_m)) = drivers.first() else {
            return Ok(None);
        };

        let trip_m = haversine_m(pickup_lat, pickup_lng, dest_lat, dest_lng);
        let total_m = driver_to_pickup_m + trip_m;
        // Asumsi kecepatan rata-rata 28.8 km/jam (8 m/s) — cukup untuk estimasi.
        // Untuk akurasi lebih tinggi, ganti dengan OSRM/Valhalla routing API.
        let duration_sec = (total_m / 8.0) as u64;

        Ok(Some(Self::calculate_fare(
            total_m,
            duration_sec,
            service_type,
            1.0,
        )))
    }

    pub fn calculate_fare(
        distance_m: f64,
        duration_sec: u64,
        service_type: &str,
        surge: f64,
    ) -> f64 {
        let (base, per_km, per_min) = match service_type {
            "mobil" => (5000.0, 3000.0, 500.0),
            "motor" => (3000.0, 1500.0, 300.0),
            "food" => (2000.0, 1000.0, 200.0),
            "send" => (4000.0, 2000.0, 400.0),
            "nebeng" => (2500.0, 1200.0, 250.0),
            _ => (0.0, 0.0, 0.0),
        };

        let fare = base + (distance_m / 1000.0) * per_km + (duration_sec as f64 / 60.0) * per_min;
        (fare * surge).max(8000.0)
    }

    // ── Order Tracking ────────────────────────────────────────────────────────

    /// Atomic: SET order + ZREM dari :ready.
    /// Dibungkus with_retry untuk ketahanan.
    pub async fn set_driver_order(
        &self,
        driver_id: &str,
        order_id: &str,
        service_type: &str,
    ) -> Result<()> {
        Self::validate_driver_id(driver_id)?;
        Self::validate_service_type(service_type)?;
        if order_id.is_empty() {
            anyhow::bail!("order_id cannot be empty");
        }

        let driver_id = driver_id.to_string();
        let order_id = order_id.to_string();
        let service_type = service_type.to_string();
        let svc = self.clone();

        with_retry(|| {
            let driver_id = driver_id.clone();
            let order_id = order_id.clone();
            let service_type = service_type.clone();
            let svc = svc.clone();
            async move {
                redis::pipe()
                    .atomic()
                    .cmd("SET")
                    .arg(Self::order_key(&driver_id))
                    .arg(&order_id)
                    .arg("EX")
                    .arg(ORDER_TTL_SECS)
                    .cmd("ZREM")
                    .arg(Self::geo_ready_key(&service_type))
                    .arg(&driver_id)
                    .query_async(&mut svc.redis.clone())
                    .await
                    .map(|_: ()| ())
                    .context("set_driver_order pipeline failed")?;

                tracing::debug!(
                    driver = %driver_id,
                    order = %order_id,
                    "driver assigned, removed from :ready"
                );
                Ok(())
            }
        })
        .await
    }

    pub async fn get_driver_order(&self, driver_id: &str) -> Result<Option<String>> {
        Self::validate_driver_id(driver_id)?;
        self.redis
            .clone()
            .get(Self::order_key(driver_id))
            .await
            .context("Failed to get driver order")
    }

    /// DEL order key. Driver re-enter :ready via heartbeat berikutnya (~5s).
    pub async fn clear_driver_order(&self, driver_id: &str) -> Result<()> {
        Self::validate_driver_id(driver_id)?;

        let driver_id = driver_id.to_string();
        let svc = self.clone();

        with_retry(|| {
            let driver_id = driver_id.clone();
            let svc = svc.clone();
            async move {
                svc.redis
                    .clone()
                    .del(Self::order_key(&driver_id))
                    .await
                    .map(|_: ()| ())
                    .context("clear_driver_order DEL failed")?;

                tracing::debug!(
                    driver = %driver_id,
                    "order cleared, re-enter :ready on next heartbeat"
                );
                Ok(())
            }
        })
        .await
    }

    // ── Stale GEO Cleanup ─────────────────────────────────────────────────────

    /// Fix #1: ZSCAN menggantikan ZRANGE+manual cursor.
    ///
    /// Masalah ZRANGE+cursor: jika ada INSERT/DELETE selama iterasi,
    /// elemen bisa terlewat atau diproses dua kali karena offset bergeser.
    /// ZSCAN menggunakan cursor internal Redis yang stable selama iterasi.
    ///
    /// Batch COUNT 500 tetap dipertahankan agar Redis punya napas antara batch
    /// dan tidak ada memory spike dari response tunggal yang besar.
    pub async fn cleanup_stale_drivers(&self, service_type: &str) -> Result<CleanupReport> {
        Self::validate_service_type(service_type)?;

        let mut redis = self.redis.clone();
        let geo_key = Self::geo_key(service_type);
        let ready_key = Self::geo_ready_key(service_type);
        let mut report = CleanupReport::default();
        let mut cursor: u64 = 0;

        loop {
            // ZSCAN returns (next_cursor, [member, score, member, score, ...])
            // Tipe flat Vec<String> lebih portable antar versi redis-rs.
            let (next_cursor, flat): (u64, Vec<String>) = redis::cmd("ZSCAN")
                .arg(&geo_key)
                .arg(cursor)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut redis)
                .await
                .unwrap_or_else(|e| {
                    tracing::error!(
                        error = ?e,
                        service_type,
                        cursor,
                        "ZSCAN failed in cleanup"
                    );
                    (0, vec![]) // cursor 0 → loop akan berhenti
                });

            // flat = [member1, score1, member2, score2, ...]
            // Ambil setiap elemen ke-0, 2, 4, ... (member, bukan score)
            let batch: Vec<&str> = flat.iter().step_by(2).map(|s| s.as_str()).collect();

            if !batch.is_empty() {
                // Cek mana yang offline via MGET loc
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
                    .map(|(id, _)| *id)
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
                            tracing::error!(error = ?e, "ZREM stale failed in cleanup");
                        });

                    report.stale_removed += stale.len();
                }
            }

            // cursor 0 → ZSCAN sudah selesai satu putaran penuh
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        if report.stale_removed > 0 {
            tracing::info!(
                count = report.stale_removed,
                service_type,
                "removed stale drivers from geo indexes"
            );
        }

        // Leak detection: driver di :ready yang punya order aktif.
        // Scan :ready terpisah karena bisa berbeda dengan geo global.
        let mut leak_cursor: u64 = 0;
        let mut total_leak = 0usize;

        loop {
            let (next_cursor, flat): (u64, Vec<String>) = redis::cmd("ZSCAN")
                .arg(&ready_key)
                .arg(leak_cursor)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut redis)
                .await
                .unwrap_or_else(|e| {
                    tracing::error!(error = ?e, "ZSCAN :ready failed in leak detection");
                    (0, vec![])
                });

            let ready_batch: Vec<&str> = flat.iter().step_by(2).map(|s| s.as_str()).collect();

            if !ready_batch.is_empty() {
                let mut pipe = redis::pipe();
                for id in &ready_batch {
                    pipe.cmd("EXISTS").arg(Self::order_key(id));
                }
                let order_exists: Vec<i64> =
                    pipe.query_async(&mut redis).await.unwrap_or_else(|e| {
                        tracing::error!(error = ?e, "EXISTS check failed in leak detection");
                        vec![0; ready_batch.len()]
                    });

                let leaked: Vec<&str> = ready_batch
                    .iter()
                    .zip(order_exists.iter())
                    .filter(|(_, &exists)| exists == 1)
                    .map(|(id, _)| *id)
                    .collect();

                if !leaked.is_empty() {
                    tracing::warn!(
                        count = leaked.len(),
                        service_type,
                        drivers = ?leaked,
                        "LEAK: drivers in :ready with active order"
                    );
                    total_leak += leaked.len();
                }
            }

            if next_cursor == 0 {
                break;
            }
            leak_cursor = next_cursor;
        }

        report.leak_detected = total_leak;
        Ok(report)
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    pub async fn count_active_drivers(&self, service_type: &str) -> Result<usize> {
        Self::validate_service_type(service_type)?;

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

    /// Count semua service type sekaligus. Return HashMap<service_type, count>.
    pub async fn count_active_drivers_all(&self) -> Result<HashMap<String, usize>> {
        let handles: Vec<_> = SERVICE_TYPES
            .iter()
            .map(|&st| {
                let svc = self.clone();
                let st = st.to_string();
                tokio::spawn(async move {
                    let count = svc.count_active_drivers(&st).await.unwrap_or(0);
                    (st, count)
                })
            })
            .collect();

        let mut map = HashMap::new();
        for handle in handles {
            if let Ok((st, count)) = handle.await {
                map.insert(st, count);
            }
        }
        Ok(map)
    }

    pub async fn count_ready_drivers(&self, service_type: &str) -> Result<usize> {
        Self::validate_service_type(service_type)?;

        redis::cmd("ZCARD")
            .arg(Self::geo_ready_key(service_type))
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "ZCARD failed in count_ready_drivers");
                anyhow::anyhow!("count_ready_drivers failed: {e}")
            })
    }
}

// ── Retry Helper ──────────────────────────────────────────────────────────────

/// Fix #3: Retry dengan exponential backoff untuk operasi Redis kritis.
///
/// Generik atas semua Future — tidak butuh Box atau 'static.
/// Base delay: 50ms → 100ms → 200ms (total max ~350ms sebelum gagal).
///
/// Hanya retry untuk error transient (network blip, Redis failover).
/// Error logis (bad args, dll) tetap langsung propagate — tapi karena
/// Redis tidak membedakan error jenis ini di level error type, semua
/// attempt 0..MAX-1 di-retry. Validasi input dilakukan sebelum masuk retry.
async fn with_retry<T, F, Fut>(mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut attempt = 0u32;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < REDIS_MAX_ATTEMPTS - 1 => {
                let delay_ms = REDIS_RETRY_BASE_MS * 2u64.pow(attempt);
                tracing::warn!(
                    attempt,
                    delay_ms,
                    error = ?e,
                    "redis op failed, retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

// ── Background Cleanup Task ───────────────────────────────────────────────────

pub fn spawn_cleanup_task(
    svc: Arc<LocationService>,
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
                    tracing::info!("location cleanup task: shutdown received");
                    break;
                }
            }
        }
        tracing::info!("location cleanup task: exited cleanly");
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
    fn test_validate_service_type() {
        assert!(LocationService::validate_service_type("motor").is_ok());
        assert!(LocationService::validate_service_type("mobil").is_ok());
        assert!(LocationService::validate_service_type("food").is_ok());
        assert!(LocationService::validate_service_type("send").is_ok());
        assert!(LocationService::validate_service_type("nebeng").is_ok());
        assert!(LocationService::validate_service_type("unknown").is_err());
        assert!(LocationService::validate_service_type("../../etc").is_err());
        assert!(LocationService::validate_service_type("").is_err());
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
    fn test_calculate_fare_send() {
        // base=4000, 3km*2000=6000, 10min*400=4000 → 14000
        let fare = LocationService::calculate_fare(3_000.0, 600, "send", 1.0);
        assert!((fare - 14_000.0).abs() < 1.0, "got {fare}");
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
        assert!(CANDIDATE_FETCH >= MAX_DRIVERS);
        assert!(MIN_DRIVERS < MAX_DRIVERS);
        assert!(SEARCH_RADIUS_KM < SEARCH_RADIUS_EXPAND_KM);
        assert!(
            CANDIDATE_FETCH > MAX_DRIVERS,
            "no buffer for offline/leak filtering"
        );
        assert!(REDIS_MAX_ATTEMPTS > 0);
        // CANDIDATE_FETCH harus cukup besar untuk area padat
        assert!(
            CANDIDATE_FETCH >= 50,
            "CANDIDATE_FETCH too small for dense areas"
        );
    }

    #[test]
    fn test_cleanup_report_default() {
        let r = CleanupReport::default();
        assert_eq!(r.stale_removed, 0);
        assert_eq!(r.leak_detected, 0);
    }

    #[test]
    fn test_nearby_driver_struct() {
        let d = NearbyDriver {
            driver_id: "drv-001".to_string(),
            distance_m: 1200.0,
            service_type: "motor".to_string(),
        };
        assert_eq!(d.service_type, "motor");
        assert!((d.distance_m - 1200.0).abs() < 0.01);
    }

    #[test]
    fn test_service_types_constant_coverage() {
        // Pastikan semua service type yang ada di calculate_fare ada di SERVICE_TYPES
        for st in &["motor", "mobil", "food", "send", "nebeng"] {
            assert!(
                SERVICE_TYPES.contains(st),
                "service type '{}' missing from SERVICE_TYPES",
                st
            );
        }
    }
}
