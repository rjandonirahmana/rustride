use redis::aio::ConnectionManager;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::{
    auth::JwtService,
    config::WahaConfig,
    models::user::RegisterRequest,
    proto::auth::{
        auth_service_server::AuthService, AuthResponse, Empty, LoginRequest,
        RegisterRequest as ProtoRegister, VerificationRegister,
    },
    repository::user::UserRepository,
    service::auth::AuthService as AuthSvc,
};

pub struct AuthServiceImpl<R: UserRepository> {
    pub user_repo: Arc<R>,
    pub jwt: JwtService,
    pub redis: ConnectionManager,
    pub waha: Arc<WahaConfig>,
}

#[tonic::async_trait]
impl<R: UserRepository + 'static> AuthService for AuthServiceImpl<R> {
    /// Step 1 register: kirim OTP via WA, return token kosong sebagai ACK.
    /// Client harus lanjut panggil VerifyOtp (atau endpoint terpisah).
    async fn register(&self, req: Request<ProtoRegister>) -> Result<Response<Empty>, Status> {
        let r = req.into_inner();

        if r.name.is_empty() || r.phone.is_empty() || r.password.is_empty() {
            return Err(Status::invalid_argument(
                "name, phone, password wajib diisi",
            ));
        }
        if r.role != "rider" && r.role != "driver" {
            return Err(Status::invalid_argument("role harus 'rider' atau 'driver'"));
        }

        let domain_req = RegisterRequest {
            name: r.name,
            phone: r.phone,
            email: r.email,
            password: r.password,
            role: r.role,
            vehicle_type: nonempty(r.vehicle_type),
            vehicle_plate: nonempty(r.vehicle_plate),
            vehicle_model: nonempty(r.vehicle_model),
            vehicle_color: nonempty(r.vehicle_color),
        };

        // Buat svc per-request dengan clone redis connection
        let mut svc = AuthSvc {
            repo: self.user_repo.clone(),
            jwt: self.jwt.clone(),
            redis: self.redis.clone(),
            http: reqwest::Client::new(),
            waha: WahaConfig {
                base_url: self.waha.base_url.clone(),
                session: self.waha.session.clone(),
                api_key: self.waha.api_key.clone(),
            }
            .into(),
        };

        svc.initiate_register(domain_req)
            .await
            .map(|_| {
                // OTP terkirim — return ACK kosong, token diisi setelah verify
                Response::new(Empty {})
            })
            .map_err(|e| Status::internal(e.to_string()))
    }

    async fn login(&self, req: Request<LoginRequest>) -> Result<Response<AuthResponse>, Status> {
        let r = req.into_inner();

        if r.phone.is_empty() || r.password.is_empty() {
            return Err(Status::invalid_argument("phone dan password wajib diisi"));
        }

        let svc = AuthSvc {
            repo: self.user_repo.clone(),
            jwt: self.jwt.clone(),
            redis: self.redis.clone(),
            http: reqwest::Client::new(),
            waha: WahaConfig {
                base_url: self.waha.base_url.clone(),
                session: self.waha.session.clone(),
                api_key: self.waha.api_key.clone(),
            }
            .into(),
        };

        svc.login(&r.phone, &r.password)
            .await
            .map(|res| {
                Response::new(AuthResponse {
                    token: res.token,
                    user_id: res.user.id,
                    role: res.user.role,
                    name: res.user.name,
                })
            })
            .map_err(|e| Status::unauthenticated(e.to_string()))
    }

    async fn verification_regist(
        &self,
        req: Request<VerificationRegister>,
    ) -> Result<Response<AuthResponse>, Status> {
        let r = req.into_inner();

        if r.phone.is_empty() || r.otp.is_empty() {
            return Err(Status::invalid_argument("phone dan otp wajib diisi"));
        }

        let mut svc = AuthSvc {
            repo: self.user_repo.clone(),
            jwt: self.jwt.clone(),
            redis: self.redis.clone(),
            http: reqwest::Client::new(),
            waha: WahaConfig {
                base_url: self.waha.base_url.clone(),
                session: self.waha.session.clone(),
                api_key: self.waha.api_key.clone(),
            }
            .into(),
        };

        svc.verify_register(&r.phone, &r.otp)
            .await
            .map(|res| {
                Response::new(AuthResponse {
                    token: res.token,
                    user_id: res.user.id,
                    role: res.user.role,
                    name: res.user.name,
                })
            })
            .map_err(|e| Status::unauthenticated(e.to_string()))
    }
}

fn nonempty(s: String) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}
