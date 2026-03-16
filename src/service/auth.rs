/// AuthService — register & login.
///
/// Dipanggil dari gRPC AuthServiceImpl (unary).
/// Bisa juga dipanggil dari REST handler yang kamu tambah sendiri
/// karena tidak ada dependency ke tonic/gRPC di sini.
use anyhow::{bail, Result};
use bcrypt::{hash, verify, DEFAULT_COST};
use std::sync::Arc;

use crate::{
    auth::JwtService,
    models::user::{RegisterRequest, User},
    repository::user::UserRepository,
};

pub struct AuthResponse {
    pub token: String,
    pub user: User,
}

pub struct AuthService<R: UserRepository> {
    pub repo: Arc<R>,
    pub jwt: JwtService,
}

impl<R: UserRepository> AuthService<R> {
    pub fn new(repo: Arc<R>, jwt: JwtService) -> Self {
        Self { repo, jwt }
    }

    pub async fn register(&self, req: RegisterRequest) -> Result<AuthResponse> {
        // Cek duplikat nomor HP
        if self.repo.find_by_phone(&req.phone).await?.is_some() {
            bail!("Nomor HP sudah terdaftar");
        }

        if req.role != "rider" && req.role != "driver" {
            bail!("Role tidak valid: harus 'rider' atau 'driver'");
        }

        let hashed = hash(&req.password, DEFAULT_COST)?;
        let user = self.repo.create(&req, &hashed).await?;
        let token = self
            .jwt
            .sign(&user.id, &user.name, &user.role, &user.vehicle_type)?;
        Ok(AuthResponse { token, user })
    }

    pub async fn login(&self, phone: &str, password: &str) -> Result<AuthResponse> {
        let user = self
            .repo
            .find_by_phone(phone)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Nomor HP tidak ditemukan"))?;

        if !verify(password, &user.password)? {
            bail!("Password salah");
        }

        let token = self
            .jwt
            .sign(&user.id, &user.name, &user.role, &user.vehicle_type)?;
        Ok(AuthResponse { token, user })
    }
}
