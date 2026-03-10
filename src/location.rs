use anyhow::{Context, Result};
use redis::{aio::ConnectionManager, AsyncCommands};

use crate::models::order::DriverLocation;

const GEO_KEY_MOTOR: &str = "drivers:geo:motor";
const GEO_KEY_MOBIL: &str = "drivers:geo:mobil";
const LOC_DETAIL_PREFIX: &str = "driver:loc:";
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

    fn geo_key(service_type: &str) -> &'static str {
        if service_type == "mobil" {
            GEO_KEY_MOBIL
        } else {
            GEO_KEY_MOTOR
        }
    }

    fn detail_key(driver_id: &str) -> String {
        format!("{}{}", LOC_DETAIL_PREFIX, driver_id)
    }

    fn order_key(driver_id: &str) -> String {
        format!("driver:order:{}", driver_id)
    }

    /// Driver update lokasi — simpan ke Redis GEO + detail JSON
    pub async fn update_driver_location(
        &self,
        driver_id: &str,
        lat: f64,
        lng: f64,
        heading: Option<f64>,
        speed: Option<f64>,
        service_type: &str,
    ) -> Result<()> {
        let mut redis = self.redis.clone();
        let geo_key = Self::geo_key(service_type);

        // Hapus dari geo key LAIN dulu supaya tidak double entry
        let other_key = if service_type == "mobil" {
            GEO_KEY_MOTOR
        } else {
            GEO_KEY_MOBIL
        };
        let _: Result<(), _> = redis.zrem(other_key, driver_id).await?;

        // Baru GEOADD ke key yang benar
        let _: () = redis.geo_add(geo_key, (lng, lat, driver_id)).await?;

        // Detail (heading, speed, timestamp) disimpan sebagai JSON string
        let ts = chrono::Utc::now().timestamp_millis();
        let detail = serde_json::json!({
            "lat":     lat,
            "lng":     lng,
            "heading": heading,
            "speed":   speed,
            "ts":      ts,
        });

        let _: () = redis
            .set_ex(Self::detail_key(driver_id), detail.to_string(), 30u64)
            .await?;

        Ok(())
    }

    /// Hapus driver dari GEO set (saat offline / disconnect)
    pub async fn remove_driver(&self, driver_id: &str, service_type: &str) -> Result<()> {
        let mut redis = self.redis.clone();
        let geo_key = Self::geo_key(service_type);

        let _: Result<(), _> = redis.zrem(geo_key, driver_id).await;
        let _: Result<(), _> = redis.del(Self::detail_key(driver_id)).await;
        Ok(())
    }

    pub async fn find_nearby_drivers(
        &self,
        lat: f64,
        lng: f64,
        service_type: &str,
    ) -> Result<Vec<(String, f64)>> {
        let mut redis = self.redis.clone();
        let geo_key = Self::geo_key(service_type);

        // GEORADIUS dengan WITHCOORD WITHDIST sekaligus — 1 round trip ke Redis
        // format return: Vec<(member, dist, (lng, lat))>
        let members: Vec<(String, f64, (f64, f64))> = redis::cmd("GEORADIUS")
            .arg(&geo_key)
            .arg(lng)
            .arg(lat)
            .arg(SEARCH_RADIUS_KM)
            .arg("km")
            .arg("WITHDIST")
            .arg("WITHCOORD")
            .arg("ASC")
            .arg("COUNT")
            .arg(MAX_DRIVERS)
            .query_async(&mut redis)
            .await
            .unwrap_or_default();

        // HashSet untuk deduplicate driver_id
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();

        for (driver_id, dist_km, _coord) in members {
            if seen.insert(driver_id.clone()) {
                // dist dari Redis dalam km → convert ke meter
                result.push((driver_id, dist_km * 1000.0));
            }
        }

        Ok(result)
    }

    pub async fn get_driver_location(&self, driver_id: &str) -> Result<Option<DriverLocation>> {
        let mut redis = self.redis.clone();
        let raw: Option<String> = redis.get(Self::detail_key(driver_id)).await.unwrap_or(None);

        let location = raw.and_then(|s| {
            let v: serde_json::Value = serde_json::from_str(&s).ok()?;
            Some(DriverLocation {
                driver_id: driver_id.to_string(),
                lat: v["lat"].as_f64()?,
                lng: v["lng"].as_f64()?,
                heading: v["heading"].as_f64().map(|h| h as f32),
                speed: v["speed"].as_f64().map(|s| s as f32),
                updated_at: v["ts"].as_i64().unwrap_or(0),
            })
        });

        Ok(location)
    }

    /// Simpan order aktif driver (untuk recovery jika server restart)
    pub async fn set_driver_order(&self, driver_id: &str, order_id: &str) -> Result<()> {
        let mut redis = self.redis.clone();
        let _: () = redis
            .set_ex(Self::order_key(driver_id), order_id, 3600u64 * 4)
            .await
            .context("failed to set driver order")?;
        Ok(())
    }

    pub async fn get_driver_order(&self, driver_id: &str) -> Result<Option<String>> {
        let mut redis = self.redis.clone();
        Ok(redis.get(Self::order_key(driver_id)).await.unwrap_or(None))
    }

    pub async fn clear_driver_order(&self, driver_id: &str) -> Result<()> {
        let mut redis = self.redis.clone();
        let _: Result<(), _> = redis.del(Self::order_key(driver_id)).await;
        Ok(())
    }
}

/// Haversine distance dalam meter antara dua koordinat
pub fn haversine_m(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let r = 6_371_000.0_f64;
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlng / 2.0).sin().powi(2);
    2.0 * r * a.sqrt().asin()
}
