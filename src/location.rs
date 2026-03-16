// ===== GEOHASH GRID PARTITIONING - FIXED VERSION =====
// Fix semua bug kritis: precision mismatch, grid migration, KEYS command, pipeline

use std::collections::HashSet;

use anyhow::{Context, Result};
use geohash::{encode, neighbors, Coord, Direction};
use prost::Message;
use redis::{aio::ConnectionManager, AsyncCommands};

use crate::{models::order::DriverLocation, proto::ridehailing::DriverLocationMeta};

// ── Constants ─────────────────────────────────────────────────────────────────

/// FIX: Gunakan precision yang SAMA untuk partition dan search
const GEO_PRECISION: usize = 5; // ~5km × 5km grid

const LOC_DETAIL_PREFIX: &str = "driver:loc:";
const GEO_TTL_PREFIX: &str = "geo:ttl:";
const ORDER_PREFIX: &str = "driver:order:";
const HASH_PREFIX: &str = "driver:hash:";

const META_TTL_SECONDS: i64 = 30;
const ORDER_TTL_SECONDS: u64 = 3600 * 4;
const SEARCH_RADIUS_KM: f64 = 5.0;
const MAX_DRIVERS: usize = 10;

#[derive(Clone)]
pub struct LocationService {
    redis: ConnectionManager,
}

impl LocationService {
    pub fn new(redis: ConnectionManager) -> Self {
        Self { redis }
    }

    // ── Key Helpers ───────────────────────────────────────────────────────────

    fn geo_key(service_type: &str, geohash: &str) -> String {
        format!("geo:{}:{}", service_type, geohash)
    }

    fn geo_ttl_key(service_type: &str, geohash: &str) -> String {
        format!("{}{}:{}", GEO_TTL_PREFIX, service_type, geohash)
    }

    fn detail_key(driver_id: &str) -> String {
        format!("{}{}", LOC_DETAIL_PREFIX, driver_id)
    }

    fn order_key(driver_id: &str) -> String {
        format!("{}{}", ORDER_PREFIX, driver_id)
    }

    fn hash_key(driver_id: &str) -> String {
        format!("{}{}", HASH_PREFIX, driver_id)
    }

    // ── Geohash Utilities ─────────────────────────────────────────────────────

    /// FIX: Satu function, satu precision
    fn get_geohash(lat: f64, lng: f64) -> Result<String> {
        let coord = Coord { x: lng, y: lat };
        encode(coord, GEO_PRECISION).context("Failed to encode geohash")
    }

    /// Get neighboring geohash cells (8 neighbors + center = 9 total)
    /// Get neighboring geohash cells (8 neighbors + center = 9 total)
    fn get_search_grids(lat: f64, lng: f64) -> Result<Vec<String>> {
        let center_hash = Self::get_geohash(lat, lng)?;

        let mut grids = HashSet::with_capacity(9);

        grids.insert(center_hash.clone());

        if let Ok(n) = neighbors(&center_hash) {
            grids.insert(n.n);
            grids.insert(n.ne);
            grids.insert(n.e);
            grids.insert(n.se);
            grids.insert(n.s);
            grids.insert(n.sw);
            grids.insert(n.w);
            grids.insert(n.nw);
        }

        Ok(grids.into_iter().collect())
    }

    // ── Validation ────────────────────────────────────────────────────────────

    fn validate_coordinates(lat: f64, lng: f64) -> Result<()> {
        if !(-90.0..=90.0).contains(&lat) {
            anyhow::bail!("Invalid latitude: {} (must be -90 to 90)", lat);
        }
        if !(-180.0..=180.0).contains(&lng) {
            anyhow::bail!("Invalid longitude: {} (must be -180 to 180)", lng);
        }
        if lat.is_nan() || lng.is_nan() || lat.is_infinite() || lng.is_infinite() {
            anyhow::bail!("Invalid coordinates: lat={}, lng={}", lat, lng);
        }
        Ok(())
    }

    fn validate_driver_id(driver_id: &str) -> Result<()> {
        if driver_id.is_empty() {
            anyhow::bail!("Driver ID cannot be empty");
        }
        if driver_id.len() > 128 {
            anyhow::bail!("Driver ID too long: {} chars", driver_id.len());
        }
        Ok(())
    }

    // ── Update Location ───────────────────────────────────────────────────────

    /// FIX: Handle grid migration (driver pindah dari grid lama ke baru)
    pub async fn update_driver_location(
        &self,
        driver_id: &str,
        lat: f64,
        lng: f64,
        heading: Option<f32>, // FIX: f64 → f32
        speed: Option<f32>,   // FIX: f64 → f32
        service_type: &str,
    ) -> Result<()> {
        Self::validate_coordinates(lat, lng)?;
        Self::validate_driver_id(driver_id)?;

        let mut redis = self.redis.clone();

        // Calculate new geohash
        let new_geohash = Self::get_geohash(lat, lng)?;
        let hash_key = Self::hash_key(driver_id);

        // FIX: Get old geohash untuk detect grid migration
        let old_geohash: Option<String> = redis.get(&hash_key).await.unwrap_or(None);

        let geo_key = Self::geo_key(service_type, &new_geohash);
        let geo_ttl_key = Self::geo_ttl_key(service_type, &new_geohash);
        let detail_key = Self::detail_key(driver_id);

        let ts = chrono::Utc::now().timestamp_millis();

        // Prepare metadata
        let meta = DriverLocationMeta {
            heading: heading.unwrap_or_default() as f64,
            speed: speed.unwrap_or_default() as f64,
            ts,
            lat,
            lng,
        };

        let bytes = meta.encode_to_vec();

        // Build atomic pipeline
        let mut pipe = redis::pipe();
        pipe.atomic();

        // FIX: Jika pindah grid, remove dari grid lama
        if let Some(old) = &old_geohash {
            if old != &new_geohash {
                let old_geo_key = Self::geo_key(service_type, old);
                let old_ttl_key = Self::geo_ttl_key(service_type, old);

                pipe.cmd("ZREM").arg(&old_geo_key).arg(driver_id);
                pipe.cmd("ZREM").arg(&old_ttl_key).arg(driver_id);

                tracing::debug!(
                    "Driver {} migrated from grid {} to {}",
                    driver_id,
                    old,
                    new_geohash
                );
            }
        }

        // Add to new grid
        pipe.cmd("GEOADD")
            .arg(&geo_key)
            .arg(lng)
            .arg(lat)
            .arg(driver_id);

        // Store metadata with TTL
        pipe.cmd("SETEX")
            .arg(&detail_key)
            .arg(META_TTL_SECONDS)
            .arg(&bytes);

        // Track in TTL sorted set
        pipe.cmd("ZADD").arg(&geo_ttl_key).arg(ts).arg(driver_id);

        // Store current geohash
        pipe.cmd("SETEX")
            .arg(&hash_key)
            .arg(META_TTL_SECONDS)
            .arg(&new_geohash);

        let _: () = pipe
            .query_async(&mut redis)
            .await
            .context("Failed to update driver location")?;

        Ok(())
    }

    // ── Remove Driver ─────────────────────────────────────────────────────────

    pub async fn remove_driver(&self, driver_id: &str, service_type: &str) -> Result<()> {
        Self::validate_driver_id(driver_id)?;

        let mut redis = self.redis.clone();
        let hash_key = Self::hash_key(driver_id);

        let geohash: Option<String> = redis
            .get(&hash_key)
            .await
            .context("Failed to get driver geohash")?;

        let geohash = match geohash {
            Some(h) => h,
            None => {
                tracing::warn!("No geohash found for driver {}", driver_id);
                return Ok(());
            }
        };

        let geo_key = Self::geo_key(service_type, &geohash);
        let geo_ttl_key = Self::geo_ttl_key(service_type, &geohash);
        let detail_key = Self::detail_key(driver_id);

        let _: () = redis::pipe()
            .atomic()
            .cmd("ZREM")
            .arg(&geo_key)
            .arg(driver_id)
            .cmd("DEL")
            .arg(&detail_key)
            .cmd("ZREM")
            .arg(&geo_ttl_key)
            .arg(driver_id)
            .cmd("DEL")
            .arg(&hash_key)
            .query_async(&mut redis)
            .await
            .context("Failed to remove driver")?;

        Ok(())
    }

    // ── Find Nearby Drivers ───────────────────────────────────────────────────

    /// FIX: Gunakan pipeline untuk query semua grids sekaligus (5-10x faster)
    pub async fn find_nearby_drivers(
        &self,
        lat: f64,
        lng: f64,
        service_type: &str,
    ) -> Result<Vec<(String, f64)>> {
        Self::validate_coordinates(lat, lng)?;

        let search_grids = Self::get_search_grids(lat, lng)?;
        let mut redis = self.redis.clone();

        let mut pipe = redis::pipe();

        for geohash in &search_grids {
            let geo_key = Self::geo_key(service_type, geohash);

            pipe.cmd("GEOSEARCH")
                .arg(&geo_key)
                .arg("FROMLONLAT")
                .arg(lng)
                .arg(lat)
                .arg("BYRADIUS")
                .arg(SEARCH_RADIUS_KM)
                .arg("km")
                .arg("WITHDIST")
                .arg("ASC");
        }

        // Redis return nested array
        let raw: Vec<Vec<Vec<String>>> = pipe
            .query_async(&mut redis)
            .await
            .unwrap_or_else(|_| vec![vec![]; search_grids.len()]);

        use std::collections::HashMap;

        let mut drivers_map: HashMap<String, f64> = HashMap::new();

        for grid in raw {
            for item in grid {
                if item.len() != 2 {
                    continue;
                }

                let id = item[0].clone();

                if let Ok(dist_km) = item[1].parse::<f64>() {
                    let dist_m = dist_km * 1000.0;

                    drivers_map
                        .entry(id)
                        .and_modify(|d| {
                            if dist_m < *d {
                                *d = dist_m;
                            }
                        })
                        .or_insert(dist_m);
                }
            }
        }

        let mut drivers: Vec<(String, f64)> = drivers_map.into_iter().collect();

        drivers.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        drivers.truncate(MAX_DRIVERS);

        Ok(drivers)
    }

    // ── Get Driver Location ───────────────────────────────────────────────────

    pub async fn get_driver_location(&self, driver_id: &str) -> Result<Option<DriverLocation>> {
        Self::validate_driver_id(driver_id)?;

        let mut redis = self.redis.clone();
        let detail_key = Self::detail_key(driver_id);

        let bytes: Option<Vec<u8>> = redis
            .get(&detail_key)
            .await
            .context("Failed to get driver location")?;

        match bytes {
            Some(b) => {
                let meta = DriverLocationMeta::decode(&b[..])
                    .context("Failed to decode DriverLocationMeta")?;

                Ok(Some(DriverLocation {
                    driver_id: driver_id.to_string(),
                    lat: meta.lat,
                    lng: meta.lng,
                    heading: Some(meta.heading as f32),
                    speed: Some(meta.speed as f32),
                    updated_at: meta.ts,
                }))
            }
            None => Ok(None),
        }
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    pub async fn cleanup_grid(&self, service_type: &str, geohash: &str) -> Result<usize> {
        let mut redis = self.redis.clone();
        let geo_key = Self::geo_key(service_type, geohash);
        let geo_ttl_key = Self::geo_ttl_key(service_type, geohash);

        let cutoff = chrono::Utc::now().timestamp_millis() - (META_TTL_SECONDS * 1000);

        let offline_drivers: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&geo_ttl_key)
            .arg("-inf")
            .arg(cutoff)
            .query_async(&mut redis)
            .await
            .context("Failed to get offline drivers")?;

        if offline_drivers.is_empty() {
            return Ok(0);
        }

        let mut pipe = redis::pipe();
        pipe.atomic();

        for driver_id in &offline_drivers {
            pipe.cmd("ZREM").arg(&geo_key).arg(driver_id);
            pipe.cmd("ZREM").arg(&geo_ttl_key).arg(driver_id);
        }

        let _: () = pipe
            .query_async(&mut redis)
            .await
            .context("Failed to cleanup offline drivers")?;

        if offline_drivers.len() > 0 {
            tracing::info!(
                "Cleaned up {} offline drivers from grid {} ({})",
                offline_drivers.len(),
                geohash,
                service_type
            );
        }

        Ok(offline_drivers.len())
    }

    /// FIX: Gunakan SCAN instead of KEYS (production safe)
    pub async fn get_active_grids(&self, service_type: &str) -> Result<Vec<String>> {
        let mut redis = self.redis.clone();
        let pattern = format!("geo:{}:*", service_type);
        let mut grids = Vec::new();
        let mut cursor = 0u64;

        loop {
            // SCAN dengan COUNT 100 per iteration
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut redis)
                .await
                .context("Failed to scan active grids")?;

            // Extract geohash dari keys
            for key in keys {
                if let Some(hash) = key.strip_prefix(&format!("geo:{}:", service_type)) {
                    grids.push(hash.to_string());
                }
            }

            cursor = new_cursor;

            // Cursor 0 = selesai
            if cursor == 0 {
                break;
            }
        }

        // Deduplicate
        grids.sort();
        grids.dedup();

        Ok(grids)
    }

    // ── Order Tracking ────────────────────────────────────────────────────────

    pub async fn set_driver_order(&self, driver_id: &str, order_id: &str) -> Result<()> {
        Self::validate_driver_id(driver_id)?;
        if order_id.is_empty() {
            anyhow::bail!("Order ID cannot be empty");
        }

        let mut redis = self.redis.clone();
        let order_key = Self::order_key(driver_id);

        let _: () = redis
            .set_ex(&order_key, order_id, ORDER_TTL_SECONDS)
            .await
            .context("Failed to set driver order")?;

        Ok(())
    }

    pub async fn get_driver_order(&self, driver_id: &str) -> Result<Option<String>> {
        Self::validate_driver_id(driver_id)?;

        let mut redis = self.redis.clone();
        let order_key = Self::order_key(driver_id);

        redis
            .get(&order_key)
            .await
            .context("Failed to get driver order")
    }

    pub async fn clear_driver_order(&self, driver_id: &str) -> Result<()> {
        Self::validate_driver_id(driver_id)?;

        let mut redis = self.redis.clone();
        let order_key = Self::order_key(driver_id);

        let _: () = redis
            .del(&order_key)
            .await
            .context("Failed to clear driver order")?;

        Ok(())
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    pub async fn count_active_drivers(&self, service_type: &str) -> Result<usize> {
        let grids = self.get_active_grids(service_type).await?;

        let mut redis = self.redis.clone();

        let mut pipe = redis::pipe();

        for geohash in grids {
            let geo_key = Self::geo_key(service_type, &geohash);
            pipe.cmd("ZCARD").arg(&geo_key);
        }

        let total: Vec<usize> = pipe.query_async(&mut redis).await?;

        Ok(total.into_iter().sum())
    }
}

// ── Background Cleanup Task ───────────────────────────────────────────────────

pub async fn start_cleanup_task(
    location_service: std::sync::Arc<LocationService>,
    interval_secs: u64,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

    loop {
        interval.tick().await;

        for service_type in &["mobil", "motor", "send", "food", "nebeng"] {
            let grids = match location_service.get_active_grids(service_type).await {
                Ok(g) => g,
                Err(e) => {
                    tracing::error!("Failed to get grids for {}: {:?}", service_type, e);
                    continue;
                }
            };

            let mut total_cleaned = 0;
            for geohash in grids {
                match location_service.cleanup_grid(service_type, &geohash).await {
                    Ok(count) => total_cleaned += count,
                    Err(e) => {
                        tracing::error!(
                            "Failed to cleanup grid {} ({}): {:?}",
                            geohash,
                            service_type,
                            e
                        );
                    }
                }
            }

            if total_cleaned > 0 {
                tracing::info!(
                    "Cleaned up {} total offline {} drivers",
                    total_cleaned,
                    service_type
                );
            }
        }
    }
}

// ── Haversine Distance ────────────────────────────────────────────────────────

pub fn haversine_m(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;

    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();

    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlng / 2.0).sin().powi(2);

    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geohash_same_precision() {
        let hash = LocationService::get_geohash(-6.2088, 106.8456).unwrap();
        assert_eq!(hash.len(), GEO_PRECISION);
        println!("Geohash: {}", hash);
    }

    #[test]
    fn test_search_grids() {
        let grids = LocationService::get_search_grids(-6.2088, 106.8456).unwrap();
        assert!(grids.len() <= 9); // Max 9 (center + 8 neighbors)
        assert!(grids.len() >= 1); // Min 1 (center)

        // Semua harus precision yang sama
        for grid in &grids {
            assert_eq!(grid.len(), GEO_PRECISION);
        }

        println!("Search grids: {:?}", grids);
    }

    #[test]
    fn test_haversine() {
        let jakarta = (-6.2088, 106.8456);
        let bandung = (-6.9175, 107.6191);

        let distance = haversine_m(jakarta.0, jakarta.1, bandung.0, bandung.1);

        assert!((distance - 150_000.0).abs() < 10_000.0);
    }
}
