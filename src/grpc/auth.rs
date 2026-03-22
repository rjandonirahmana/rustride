/// AuthServiceImpl — gRPC unary handler untuk Register & Login.
///
/// Tidak perlu JWT di header; ini endpoint publik.
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::{
    auth::JwtService,
    models::user::RegisterRequest,
    proto::auth::{
        auth_service_server::AuthService, AuthResponse, LoginRequest,
        RegisterRequest as ProtoRegister,
    },
    repository::user::UserRepository,
    service::auth::AuthService as AuthSvc,
};

pub struct AuthServiceImpl<R: UserRepository> {
    pub user_repo: Arc<R>,
    pub jwt: JwtService,
}

#[tonic::async_trait]
impl<R: UserRepository + 'static> AuthService for AuthServiceImpl<R> {
    async fn register(
        &self,
        req: Request<ProtoRegister>,
    ) -> Result<Response<AuthResponse>, Status> {
        let r = req.into_inner();

        // Validasi sederhana
        if r.name.is_empty() || r.phone.is_empty() || r.password.is_empty() {
            return Err(Status::invalid_argument(
                "name, phone, password wajib diisi",
            ));
        }
        if r.role != "rider" && r.role != "driver" {
            return Err(Status::invalid_argument("role harus 'rider' atau 'driver'"));
        }

        let svc = AuthSvc::new(self.user_repo.clone(), self.jwt.clone());

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

        svc.register(domain_req)
            .await
            .map(|res| {
                Response::new(AuthResponse {
                    token: res.token,
                    user_id: res.user.id,
                    role: res.user.role,
                    name: res.user.name,
                })
            })
            .map_err(|e| Status::internal(e.to_string()))
    }

    async fn login(&self, req: Request<LoginRequest>) -> Result<Response<AuthResponse>, Status> {
        let r = req.into_inner();
        if r.phone.is_empty() || r.password.is_empty() {
            return Err(Status::invalid_argument("phone dan password wajib diisi"));
        }

        let svc = AuthSvc::new(self.user_repo.clone(), self.jwt.clone());

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
}

fn nonempty(s: String) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}
