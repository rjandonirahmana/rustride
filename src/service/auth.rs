use anyhow::{anyhow, bail, Result};
use bcrypt::{hash, verify, DEFAULT_COST};
use prost::Message;
use rand::{rngs::StdRng, Rng, SeedableRng};
use redis::{aio::ConnectionManager, AsyncCommands};
use reqwest::Client as HttpClient;
use serde_json::json;
use std::sync::Arc;
use tracing::info;

use crate::{
    auth::JwtService,
    config::WahaConfig,
    models::user::{RegisterRequest, User},
    proto::auth::PendingUser,
    repository::user::UserRepository,
};

// ─────────────────────────────────────────────────────────────────────────────

pub struct AuthResponse {
    pub token: String,
    pub user: User,
}

pub struct AuthService<R: UserRepository> {
    pub repo: Arc<R>,
    pub jwt: JwtService,
    pub redis: ConnectionManager,
    pub http: HttpClient,
    pub waha: Arc<WahaConfig>,
}

impl<R: UserRepository> AuthService<R> {
    pub fn new(
        repo: Arc<R>,
        jwt: JwtService,
        redis: ConnectionManager,
        http: HttpClient,
        waha: Arc<WahaConfig>,
    ) -> Self {
        Self {
            repo,
            jwt,
            redis,
            http,
            waha,
        }
    }

    // ── REGISTER INIT ─────────────────────────────────────────────────────────

    pub async fn initiate_register(&mut self, req: RegisterRequest) -> Result<()> {
        if req.role != "rider" && req.role != "driver" {
            bail!("Role harus 'rider' atau 'driver'");
        }

        if self.repo.find_by_phone(&req.phone).await?.is_some() {
            bail!("Nomor HP sudah terdaftar");
        }

        let redis_key = format!("reg:{}", req.phone);

        // Rate limit OTP
        if let Ok(Some(_)) = self.redis.get::<_, Option<Vec<u8>>>(&redis_key).await {
            let ttl: i64 = self.redis.ttl(&redis_key).await.unwrap_or(0);
            if ttl > 540 {
                bail!("OTP sudah dikirim. Tunggu {} detik lagi.", ttl - 540);
            }
        }

        let mut rng = StdRng::from_os_rng();
        let otp = format!("{:06}", rng.random_range(100_000..=999_999));

        let pending = PendingUser {
            name: req.name.clone(),
            phone: req.phone.clone(),
            email: req.email.clone(),
            password: req.password.clone(),
            role: req.role.clone(),
            otp: otp.clone(),
            vehicle_type: req.vehicle_type.clone(),
            vehicle_plate: req.vehicle_plate.clone(),
            vehicle_model: req.vehicle_model.clone(),
            vehicle_color: req.vehicle_color.clone(),
        };

        let mut buf = Vec::new();
        pending.encode(&mut buf)?;

        let _: () = self.redis.set_ex(&redis_key, buf, 600u64).await?;

        self.send_wa_otp(&req.phone, &otp).await?;

        info!(phone = %req.phone, "OTP stored & sent");
        Ok(())
    }

    // ── VERIFY REGISTER ───────────────────────────────────────────────────────

    pub async fn verify_register(&mut self, phone: &str, otp_input: &str) -> Result<AuthResponse> {
        let redis_key = format!("reg:{}", phone);

        let bytes: Option<Vec<u8>> = self.redis.get(&redis_key).await?;
        let bytes =
            bytes.ok_or_else(|| anyhow!("Sesi registrasi tidak ditemukan atau sudah expired"))?;

        let pending = PendingUser::decode(bytes.as_slice())
            .map_err(|e| anyhow!("Decode pending user gagal: {e}"))?;

        if !constant_time_eq(&pending.otp, otp_input) {
            bail!("Kode OTP salah");
        }

        self.redis.del::<_, ()>(&redis_key).await?;

        let hashed = hash(&pending.password, DEFAULT_COST)?;

        let req = RegisterRequest {
            name: pending.name,
            phone: pending.phone,
            email: pending.email,
            password: pending.password,
            role: pending.role,
            vehicle_type: pending.vehicle_type,
            vehicle_plate: pending.vehicle_plate,
            vehicle_model: pending.vehicle_model,
            vehicle_color: pending.vehicle_color,
        };

        let user = self.repo.create(&req, &hashed).await?;

        let token = self
            .jwt
            .sign(&user.id, &user.name, &user.role, &user.vehicle_type)?;

        info!(user_id = %user.id, "User registered");

        Ok(AuthResponse { token, user })
    }

    // ── LOGIN ─────────────────────────────────────────────────────────────────

    pub async fn login(&self, phone: &str, password: &str) -> Result<AuthResponse> {
        let user = self
            .repo
            .find_by_phone(phone)
            .await?
            .ok_or_else(|| anyhow!("Nomor HP tidak ditemukan"))?;

        if !verify(password, &user.password)? {
            bail!("Password salah");
        }

        let token = self
            .jwt
            .sign(&user.id, &user.name, &user.role, &user.vehicle_type)?;

        Ok(AuthResponse { token, user })
    }

    // ── WA OTP ────────────────────────────────────────────────────────────────

    async fn send_wa_otp(&self, phone: &str, otp: &str) -> Result<()> {
        let normalized = normalize_phone(phone)?;

        let body = json!({
            "chatId": normalized,
            "text": format!(
                "Kode verifikasi RustRide kamu: *{}*\nBerlaku 10 menit. Jangan bagikan ke siapapun.",
                otp
            ),
            "session": self.waha.session,
        });

        let url = format!("{}/api/sendText", self.waha.base_url);

        let mut req = self.http.post(&url).json(&body);

        if !self.waha.api_key.is_empty() {
            req = req.header("X-Api-Key", &self.waha.api_key);
        }

        let res = req.send().await?;

        if !res.status().is_success() {
            let status = res.status();
            let text = res.text().await.unwrap_or_default();
            bail!("WAHA error {status}: {text}");
        }

        info!("OTP sent to {}", normalized);
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────

fn normalize_phone(phone: &str) -> Result<String> {
    let digits: String = phone.chars().filter(|c| c.is_ascii_digit()).collect();

    let normalized = if digits.starts_with("08") || digits.starts_with("+62") {
        format!("62{}", &digits[1..])
    } else if digits.starts_with("62") {
        digits
    } else {
        return Err(anyhow::anyhow!(
            "Nomor HP harus diawali dengan '08' atau '+62'"
        ));
    };

    Ok(format!("{}@c.us", normalized))
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }

    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}
