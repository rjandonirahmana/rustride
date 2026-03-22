use crate::{
    auth::JwtService,
    grpc::trip::extract_token,
    proto::order::{
        order_service_server::OrderService as OrderServiceGrpc, ActiveOrderEvent, CancelOrderReq,
        CreateOrderReq, DriverInfo, Empty, EstimateFareReq, FareEstimateEvent, Order,
        OrderCreatedEvent, RateOrderReq,
    },
    repository::{order::OrderRepository, user::UserRepository},
    service::order::OrderService,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct OrderServiceImpl<OR: OrderRepository, UR: UserRepository> {
    pub order_svc: Arc<OrderService<OR, UR>>,
    pub jwt: JwtService,
}

fn verify<OR: OrderRepository, UR: UserRepository>(
    svc: &OrderServiceImpl<OR, UR>,
    meta: &tonic::metadata::MetadataMap,
) -> Result<crate::auth::Claims, Status> {
    let token = extract_token(meta)
        .ok_or_else(|| Status::unauthenticated("Missing Authorization header"))?;
    svc.jwt
        .verify(token)
        .map_err(|e| Status::unauthenticated(format!("Invalid token: {e}")))
}

#[tonic::async_trait]
impl<OR: OrderRepository + 'static, UR: UserRepository + 'static> OrderServiceGrpc
    for OrderServiceImpl<OR, UR>
{
    // ── CreateOrder ───────────────────────────────────────────────────────────
    async fn create_order(
        &self,
        req: Request<CreateOrderReq>,
    ) -> Result<Response<OrderCreatedEvent>, Status> {
        let claims = verify(self, req.metadata())?;
        if claims.role != "rider" {
            return Err(Status::permission_denied("role harus 'rider'"));
        }

        let inner = req.into_inner();
        let order = self
            .order_svc
            .create_order(
                &claims.sub,
                inner.pickup_lat,
                inner.pickup_lng,
                inner.pickup_address,
                inner.dest_lat,
                inner.dest_lng,
                inner.dest_address,
                &inner.service_type,
            )
            .await
            .map_err(|e| Status::internal(format!("CREATE_FAILED: {e}")))?;

        Ok(Response::new(OrderCreatedEvent {
            order_id: order.id,
            fare_estimate: order.fare_estimate,
            service_type: order.service_type,
        }))
    }

    // ── GetActiveOrder ────────────────────────────────────────────────────────
    async fn get_active_order(
        &self,
        req: Request<Empty>,
    ) -> Result<Response<ActiveOrderEvent>, Status> {
        let claims = verify(self, req.metadata())?;

        let maybe_order = match claims.role.as_str() {
            "rider" => self
                .order_svc
                .get_active_order_for_user(&claims.sub)
                .await
                .map_err(|e| Status::internal(e.to_string()))?,
            "driver" => self
                .order_svc
                .get_active_order_for_driver(&claims.sub)
                .await
                .map_err(|e| Status::internal(e.to_string()))?,
            _ => return Err(Status::permission_denied("role tidak valid")),
        };

        let order = match maybe_order {
            None => return Ok(Response::new(ActiveOrderEvent { order: None })),
            Some(o) => o,
        };

        // fetch driver info kalau ada
        let driver_info = if let Some(ref driver_id) = order.driver_id {
            self.order_svc
                .user_repo
                .find_driver_by_id(driver_id)
                .await
                .ok()
                .flatten()
                .map(|(user, profile)| DriverInfo {
                    user_id: driver_id.clone(),
                    name: user.name,
                    phone: user.phone,
                    vehicle_plate: profile.vehicle_plate,
                    vehicle_model: profile.vehicle_model,
                    vehicle_color: profile.vehicle_color,
                    rating: profile.rating,
                })
        } else {
            None
        };

        Ok(Response::new(ActiveOrderEvent {
            order: Some(Order {
                order_id: order.id,
                rider_id: order.rider_id,
                driver_id: order.driver_id.unwrap_or_default(),
                status: order.status,
                pickup_lat: order.pickup_lat,
                pickup_lng: order.pickup_lng,
                pickup_address: order.pickup_address,
                dest_lat: order.dest_lat,
                dest_lng: order.dest_lng,
                dest_address: order.dest_address,
                fare_estimate: order.fare_estimate,
                service_type: order.service_type,
                created_at: order.created_at,
                driver: driver_info,
            }),
        }))
    }

    // ── CancelOrder ───────────────────────────────────────────────────────────
    async fn cancel_order(&self, req: Request<CancelOrderReq>) -> Result<Response<Empty>, Status> {
        let claims = verify(self, req.metadata())?;
        let inner = req.into_inner();
        let reason = (!inner.reason.is_empty()).then_some(inner.reason);

        self.order_svc
            .cancel_order(&claims.sub, &claims.role, &inner.order_id, reason)
            .await
            .map_err(|e| Status::internal(format!("CANCEL_FAILED: {e}")))?;

        Ok(Response::new(Empty {}))
    }

    // ── RateOrder ─────────────────────────────────────────────────────────────
    async fn rate_order(&self, req: Request<RateOrderReq>) -> Result<Response<Empty>, Status> {
        let claims = verify(self, req.metadata())?;
        let inner = req.into_inner();

        self.order_svc
            .submit_rating(
                &claims.sub,
                &claims.role,
                &inner.order_id,
                inner.tip,
                inner.rating as u8,
                &inner.comment,
            )
            .await
            .map_err(|e| Status::internal(format!("RATE_FAILED: {e}")))?;

        Ok(Response::new(Empty {}))
    }

    // ── EstimateFare ──────────────────────────────────────────────────────────
    async fn estimate_fare(
        &self,
        req: Request<EstimateFareReq>,
    ) -> Result<Response<FareEstimateEvent>, Status> {
        let _claims = verify(self, req.metadata())?; // auth tetap diperlukan
        let inner = req.into_inner();

        let fare = self
            .order_svc
            .estimate_fare(
                inner.pickup_lat,
                inner.pickup_lng,
                inner.dest_lat,
                inner.dest_lng,
                &inner.service_type,
            )
            .await
            .map_err(|e| Status::internal(format!("ESTIMATE_FAILED: {e}")))?;

        Ok(Response::new(fare))
    }
}
