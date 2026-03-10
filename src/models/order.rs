use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id:              String,
    pub rider_id:        String,
    pub driver_id:       Option<String>,
    pub status:          String,

    pub pickup_lat:      f64,
    pub pickup_lng:      f64,
    pub pickup_address:  String,

    pub dest_lat:        f64,
    pub dest_lng:        f64,
    pub dest_address:    String,

    pub distance_km:     Option<f32>,
    pub fare_estimate:   i32,
    pub fare_final:      Option<i32>,
    pub service_type:    String,   // "motor" | "mobil"

    pub created_at:      String,   // ISO-8601
}

/// Helper: order + jarak dari posisi driver ke pickup
#[derive(Debug, Clone)]
pub struct NearbyOrder {
    pub order:                Order,
    pub distance_to_pickup_m: f32,
    pub eta_to_pickup_min:    f32,
}

/// Lokasi driver (disimpan di Redis sebagai JSON)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverLocation {
    pub driver_id:  String,
    pub lat:        f64,
    pub lng:        f64,
    pub heading:    Option<f32>,
    pub speed:      Option<f32>,
    pub updated_at: i64,   // unix timestamp millis
}
