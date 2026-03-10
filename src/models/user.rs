use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id:         String,
    pub name:       String,
    pub phone:      String,
    pub email:      String,
    pub password:   String,
    pub role:       String,   // "rider" | "driver"
    pub avatar_url: Option<String>,
    pub created_at: String,   // ISO-8601
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverProfile {
    pub user_id:       String,
    pub vehicle_type:  String,   // "motor" | "mobil"
    pub vehicle_plate: String,
    pub vehicle_model: String,
    pub vehicle_color: String,
    pub rating:        f32,
    pub total_trips:   i32,
    pub is_active:     bool,
}

/// Input DTO untuk register — dipakai di service layer,
/// bisa diisi dari gRPC request atau REST JSON.
#[derive(Debug, Clone)]
pub struct RegisterRequest {
    pub name:          String,
    pub phone:         String,
    pub email:         String,
    pub password:      String,
    pub role:          String,
    pub vehicle_type:  Option<String>,
    pub vehicle_plate: Option<String>,
    pub vehicle_model: Option<String>,
    pub vehicle_color: Option<String>,
}
