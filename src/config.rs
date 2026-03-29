use anyhow::{Context, Result};
use dotenvy::dotenv;
use std::env; // Pastikan tambah `dotenvy = "0.15"` di Cargo.toml

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub redis_url: String,
    pub jwt_secret: String,
    pub grpc_addr: String,
    // R2 Configuration
    pub r2_account_id: String,
    pub r2_access_key: String,
    pub r2_secret_key: String,
    pub r2_bucket_name: String,
    pub r2_public_url: String,
    pub database_url_poi: String,
}

#[derive(Clone)]
pub struct WahaConfig {
    pub base_url: String,
    pub session: String,
    pub api_key: String,
}

// Custom Debug implementation supaya tidak sengaja bocorin password di log produksi
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("database_url", &self.database_url)
            .field("redis_url", &self.redis_url)
            .field("jwt_secret", &"***REDACTED***") // Disembunyikan
            .field("grpc_addr", &self.grpc_addr)
            .field("r2_account_id", &self.r2_account_id)
            .field("r2_access_key", &self.r2_access_key)
            .field("r2_secret_key", &"***REDACTED***") // Disembunyikan
            .field("r2_bucket_name", &self.r2_bucket_name)
            .field("r2_public_url", &self.r2_public_url)
            .finish()
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        // Load .env file jika ada
        dotenv().ok();

        let cfg = Config {
            database_url: env::var("DATABASE_URL").unwrap_or_else(|_| "".into()),
            database_url_poi: env::var("DATABASE_URL_POI").unwrap_or_else(|_| "".into()),
            redis_url: env::var("REDIS_URL").unwrap_or_else(|_| "".into()),
            jwt_secret: env::var("JWT_SECRET").unwrap_or_else(|_| "default_secret".into()),
            grpc_addr: env::var("GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".into()),

            // Menggunakan .context() agar jika error, kita tahu variabel mana yang hilang
            r2_account_id: env::var("R2_ACCOUNT_ID").context("R2_ACCOUNT_ID must be set")?,
            r2_access_key: env::var("R2_ACCESS_KEY_ID").context("R2_ACCESS_KEY_ID must be set")?,
            r2_secret_key: env::var("R2_SECRET_ACCESS_KEY")
                .context("R2_SECRET_ACCESS_KEY must be set")?,
            r2_bucket_name: env::var("R2_BUCKET_NAME").unwrap_or_else(|_| "rust-ride".to_string()),
            r2_public_url: env::var("R2_PUBLIC_URL").context("R2_PUBLIC_URL must be set")?,
        };

        // Print debug info (Secret akan disensor sesuai impl Debug di atas)
        println!("Config loaded successfully: {:?}", cfg);

        // Kalau kamu benar-benar ingin lihat isinya pas debugging (HATI-HATI):
        // println!("DEBUG ONLY - Secret Key: {}", cfg.r2_secret_key);

        Ok(cfg)
    }
}
