// ===== LOCATION SERVICE - FINAL PRODUCTION =====
//
// Key design:
//   geo:{service_type}              → Redis GEO sorted set, semua driver
//   {driver_id}:loc                 → SETEX 30s, proto bytes (source of truth online)
//   {driver_id}:order               → SETEX 4h, order_id jika driver busy
//   {driver_id}:svc                 → SETEX 30s, service_type aktif
//
// Cluster-safe: semua key per-driver pakai hashtag {driver_id} agar
// multi-key ops berada di slot yang sama.

use anyhow::{Context, Result};
use prost::Message;
use redis::{aio::ConnectionManager, AsyncCommands};
use tokio_util::sync::CancellationToken;

use crate::{models::order::DriverLocation, proto::order::DriverLocationMeta};

// ── Constants ─────────────────────────────────────────────────────────────────

const GEO_PREFIX: &str = "geo:";

const META_TTL_SECS: u64 = 30;
const ORDER_TTL_SECS: u64 = 3600 * 4;

const SEARCH_RADIUS_KM: f64 = 5.0;
const SEARCH_RADIUS_EXPAND_KM: f64 = 10.0; // fallback jika hasil < MIN_DRIVERS
const MIN_DRIVERS: usize = 3; // threshold sebelum expand radius
const MAX_DRIVERS: usize = 10;
const CANDIDATE_FETCH: usize = MAX_DRIVERS * 3; // lebih banyak karena akan difilter

// ── LocationService ───────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct LocationService {
    redis: ConnectionManager,
}

impl LocationService {
    pub fn new(redis: ConnectionManager) -> Self {
        Self { redis }
    }

    // ── Key Helpers (Cluster-safe dengan hashtag {driver_id}) ─────────────────

    /// "geo:motor", "geo:mobil" — global per service type
    #[inline]
    fn geo_key(service_type: &str) -> String {
        format!("{}{}", GEO_PREFIX, service_type)
    }

    /// "{driver_id}:loc" — hashtag agar semua key driver satu slot di cluster
    #[inline]
    fn loc_key(driver_id: &str) -> String {
        format!("{{{}}}:loc", driver_id)
    }

    #[inline]
    fn order_key(driver_id: &str) -> String {
        format!("{{{}}}:order", driver_id)
    }

    /// Simpan service_type aktif driver untuk deteksi pergantian tipe
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

    /// Atomic pipeline:
    ///   1. Cek service_type lama → jika beda, ZREM dari geo key lama
    ///   2. GEOADD ke geo key baru
    ///   3. SETEX loc (metadata + TTL heartbeat)
    ///   4. SETEX svc (service_type aktif)
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
        let new_geo_key = Self::geo_key(service_type);

        // Cek apakah driver ganti service_type
        let old_svc: Option<String> = redis.get(&svc_key).await.unwrap_or(None);
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

        // Jika ganti service_type, hapus dari geo key lama
        if switched {
            let old_geo_key = Self::geo_key(old_svc.as_deref().unwrap());
            pipe.cmd("ZREM").arg(&old_geo_key).arg(driver_id);

            tracing::debug!(
                driver = driver_id,
                from = old_svc.as_deref().unwrap_or("?"),
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

        let _: () = pipe
            .query_async(&mut redis)
            .await
            .context("Failed to update driver location")?;

        Ok(())
    }

    // ── Remove Driver ─────────────────────────────────────────────────────────

    pub async fn remove_driver(&self, driver_id: &str, service_type: &str) -> Result<()> {
        Self::validate_driver_id(driver_id)?;

        let _: () = redis::pipe()
            .atomic()
            .cmd("ZREM")
            .arg(Self::geo_key(service_type))
            .arg(driver_id)
            .cmd("DEL")
            .arg(Self::loc_key(driver_id))
            .cmd("DEL")
            .arg(Self::svc_key(driver_id))
            .query_async(&mut self.redis.clone())
            .await
            .context("Failed to remove driver")?;

        Ok(())
    }

    // ── Find Nearby Drivers ───────────────────────────────────────────────────

    /// Alur:
    ///   1. GEOSEARCH 1x, ASC, limit CANDIDATE_FETCH
    ///   2. Jika hasil < MIN_DRIVERS, expand ke SEARCH_RADIUS_EXPAND_KM
    ///   3. MGET batch semua driver:loc → dapat data + filter offline sekaligus
    ///   4. EXISTS batch semua driver:order → filter busy
    ///   5. Collect MAX_DRIVERS pertama
    pub async fn find_nearby_drivers(
        &self,
        lat: f64,
        lng: f64,
        service_type: &str,
    ) -> Result<Vec<(String, f64)>> {
        Self::validate_coords(lat, lng)?;

        let candidates = self
            .geosearch_candidates(lat, lng, service_type, SEARCH_RADIUS_KM)
            .await?;

        // Expand radius jika kandidat terlalu sedikit
        let candidates = if candidates.len() < MIN_DRIVERS {
            tracing::debug!(
                "Only {} candidates at {}km, expanding to {}km",
                candidates.len(),
                SEARCH_RADIUS_KM,
                SEARCH_RADIUS_EXPAND_KM
            );
            self.geosearch_candidates(lat, lng, service_type, SEARCH_RADIUS_EXPAND_KM)
                .await?
        } else {
            candidates
        };

        if candidates.is_empty() {
            return Ok(vec![]);
        }

        self.filter_available(candidates).await
    }

    /// GEOSEARCH helper — reusable untuk radius normal dan expand
    async fn geosearch_candidates(
        &self,
        lat: f64,
        lng: f64,
        service_type: &str,
        radius_km: f64,
    ) -> Result<Vec<(String, f64)>> {
        let geo_key = Self::geo_key(service_type);

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
            .query_async(&mut self.redis.clone())
            .await
            .unwrap_or_default();

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

    /// Filter kandidat: skip offline (loc expired) atau busy (has order)
    ///
    /// Menggunakan MGET untuk loc keys → dapat proto bytes sekaligus,
    /// lebih efisien dari EXISTS karena data sudah ada di response.
    async fn filter_available(&self, candidates: Vec<(String, f64)>) -> Result<Vec<(String, f64)>> {
        let mut redis = self.redis.clone();

        // MGET semua loc keys dalam satu roundtrip
        let loc_keys: Vec<String> = candidates.iter().map(|(id, _)| Self::loc_key(id)).collect();

        let loc_data: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(&loc_keys)
            .query_async(&mut redis)
            .await
            .unwrap_or_else(|_| vec![None; candidates.len()]);

        // EXISTS batch untuk order keys
        let mut pipe = redis::pipe();
        for (id, _) in &candidates {
            pipe.cmd("EXISTS").arg(Self::order_key(id));
        }
        let order_exists: Vec<i64> = pipe
            .query_async(&mut redis)
            .await
            .unwrap_or_else(|_| vec![0; candidates.len()]);

        let mut result = Vec::with_capacity(MAX_DRIVERS);

        for (i, (driver_id, dist_m)) in candidates.into_iter().enumerate() {
            let is_online = loc_data.get(i).and_then(|v| v.as_ref()).is_some();
            let has_order = order_exists.get(i).copied().unwrap_or(0) == 1;

            if is_online && !has_order {
                result.push((driver_id, dist_m));
            }

            if result.len() >= MAX_DRIVERS {
                break;
            }
        }

        Ok(result)
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

    pub async fn set_driver_order(&self, driver_id: &str, order_id: &str) -> Result<()> {
        Self::validate_driver_id(driver_id)?;
        if order_id.is_empty() {
            anyhow::bail!("order_id cannot be empty");
        }
        let _: () = self
            .redis
            .clone()
            .set_ex(Self::order_key(driver_id), order_id, ORDER_TTL_SECS)
            .await
            .context("Failed to set driver order")?;
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

    pub async fn clear_driver_order(&self, driver_id: &str) -> Result<()> {
        Self::validate_driver_id(driver_id)?;
        let _: () = self
            .redis
            .clone()
            .del(Self::order_key(driver_id))
            .await
            .context("Failed to clear driver order")?;
        Ok(())
    }

    // ── Stale GEO Cleanup ─────────────────────────────────────────────────────

    /// Hapus driver dari geo index yang loc key-nya sudah expire.
    /// Race condition acceptable: driver kirim heartbeat <30s → re-insert otomatis.
    pub async fn cleanup_stale_drivers(&self, service_type: &str) -> Result<usize> {
        let mut redis = self.redis.clone();
        let geo_key = Self::geo_key(service_type);

        let all_drivers: Vec<String> = redis::cmd("ZRANGE")
            .arg(&geo_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut redis)
            .await
            .unwrap_or_default();

        if all_drivers.is_empty() {
            return Ok(0);
        }

        let loc_keys: Vec<String> = all_drivers.iter().map(|id| Self::loc_key(id)).collect();
        let exists: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(&loc_keys)
            .query_async(&mut redis)
            .await
            .unwrap_or_else(|_| vec![None; all_drivers.len()]);

        let stale: Vec<&str> = all_drivers
            .iter()
            .zip(exists.iter())
            .filter(|(_, data)| data.is_none())
            .map(|(id, _)| id.as_str())
            .collect();

        if stale.is_empty() {
            return Ok(0);
        }

        let mut pipe = redis::pipe();
        pipe.atomic();
        for id in &stale {
            pipe.cmd("ZREM").arg(&geo_key).arg(*id);
        }
        let _: () = pipe.query_async(&mut redis).await?;

        tracing::info!(
            count = stale.len(),
            service_type,
            "cleaned stale drivers from geo index"
        );

        Ok(stale.len())
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    pub async fn count_active_drivers(&self, service_type: &str) -> Result<usize> {
        let mut redis = self.redis.clone();
        let geo_key = Self::geo_key(service_type);

        let all: Vec<String> = redis::cmd("ZRANGE")
            .arg(&geo_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut redis)
            .await
            .unwrap_or_default();

        if all.is_empty() {
            return Ok(0);
        }

        let loc_keys: Vec<String> = all.iter().map(|id| Self::loc_key(id)).collect();
        let data: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(&loc_keys)
            .query_async(&mut redis)
            .await
            .unwrap_or_default();

        Ok(data.iter().filter(|v| v.is_some()).count())
    }
}

// ── Background Cleanup Task ───────────────────────────────────────────────────

const SERVICE_TYPES: &[&str] = &["motor", "mobil", "food", "send", "nebeng"];

/// Spawn cleanup task dengan graceful shutdown via CancellationToken.
///
/// ```rust
/// let token = CancellationToken::new();
/// let handle = spawn_cleanup_task(svc.clone(), 60, token.clone());
///
/// // Saat shutdown:
/// token.cancel();
/// handle.await.ok();
/// ```
pub fn spawn_cleanup_task(
    svc: std::sync::Arc<LocationService>,
    interval_secs: u64,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

        // Skip missed ticks — jika cleanup lambat, tidak burst catch-up
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
    let mut total = 0usize;

    for &st in SERVICE_TYPES {
        match svc.cleanup_stale_drivers(st).await {
            Ok(n) => total += n,
            Err(e) => tracing::error!(service_type = st, error = ?e, "cleanup failed"),
        }
    }

    if total > 0 {
        tracing::info!(total, "stale driver entries removed");
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
        let loc = LocationService::loc_key("driver123");
        let order = LocationService::order_key("driver123");
        let svc = LocationService::svc_key("driver123");

        assert!(loc.contains("{driver123}"), "loc key missing hashtag");
        assert!(order.contains("{driver123}"), "order key missing hashtag");
        assert!(svc.contains("{driver123}"), "svc key missing hashtag");

        assert_eq!(loc, "{driver123}:loc");
        assert_eq!(order, "{driver123}:order");
        assert_eq!(svc, "{driver123}:svc");
    }

    #[test]
    fn test_geo_key() {
        assert_eq!(LocationService::geo_key("motor"), "geo:motor");
        assert_eq!(LocationService::geo_key("mobil"), "geo:mobil");
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
        assert!(LocationService::validate_coords(f64::NAN, 0.0).is_err());
        assert!(LocationService::validate_coords(0.0, f64::INFINITY).is_err());
    }

    #[test]
    fn test_haversine_jakarta_bandung() {
        let d = haversine_m(-6.2088, 106.8456, -6.9175, 107.6191);
        assert!((d - 150_000.0).abs() < 10_000.0, "got {d:.0}m");
    }

    #[test]
    fn test_calculate_fare_minimum() {
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
    fn test_calculate_fare_surge() {
        let normal = LocationService::calculate_fare(10_000.0, 1200, "motor", 1.0);
        let surge = LocationService::calculate_fare(10_000.0, 1200, "motor", 1.5);
        assert!((surge - normal * 1.5).abs() < 1.0);
    }
}
