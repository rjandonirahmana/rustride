use crate::{
    auth::JwtService,
    connections::Priority,
    context::StreamContext,
    grpc::trip::extract_token,
    models::order::Order,
    proto::{
        order::{
            order_service_server::OrderService as OrderServiceGrpc, ActiveOrderEvent,
            CancelOrderReq, CreateOrderReq, DriverInfo, Empty,
            EstimateFareReq as EstimateFareReqUnary, FareEstimateEvent, Order as protoOrder,
            OrderCreatedEvent, RateOrderReq,
        },
        ridehailing::{
            server_event::Payload as Sp, AcceptOrderReq, ArrivedReq, BrowseOrdersReq,
            CancelTripReq, CompleteTripReq, EstimateFareReq, NearbyOrderItem, NearbyOrdersEvent,
            PongEvent, PresenceEvent, RateReq, RejectOrderReq, StartTripReq, WatchUserReq,
        },
    },
    repository::{
        notification::NotificationRepositoryTrait, order::OrderRepository,
        rideshare::RideshareRepositoryTrait, user::UserRepository,
    },
    service::order::{order_to_proto, OrderService},
    throttle::{THROTTLE_BROWSE_ORDERS, THROTTLE_PING},
};
use anyhow::anyhow;
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
            .map_err(|e| Status::internal(format!("CREATE_FAILED ERRORR: {:?}", e)))?;

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
                    user_id: driver_id.to_string(),
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
            order: Some(protoOrder {
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
        req: Request<EstimateFareReqUnary>,
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

pub async fn handle_ping<OR, UR, RR, NR>(ctx: &StreamContext<OR, UR, RR, NR>)
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    if !ctx.throttle.allow(&ctx.user_id, "ping", THROTTLE_PING) {
        return;
    }
    ctx.send(Sp::Pong(PongEvent {}), Priority::Normal);
}

pub async fn handle_watch_user<OR, UR, RR, NR>(
    req: WatchUserReq,
    ctx: &StreamContext<OR, UR, RR, NR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    let online = match ctx.connections.is_connected(&req.target_user_id).await {
        Ok(on) => on,
        Err(e) => {
            tracing::error!(
                user_id = req.target_user_id,
                "Error checking user presence: {}",
                e
            );
            false
        }
    };

    ctx.send(
        Sp::Presence(PresenceEvent {
            user_id: req.target_user_id,
            online,
            last_seen: String::new(),
        }),
        Priority::Normal,
    );
}

pub async fn handle_browse_orders<OR, UR, RR, NR>(
    b: BrowseOrdersReq,
    ctx: &StreamContext<OR, UR, RR, NR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    if !ctx.require_role("driver") {
        return;
    }
    if !ctx
        .throttle
        .allow(&ctx.user_id, "browse_orders", THROTTLE_BROWSE_ORDERS)
    {
        return;
    }

    // Jika driver sudah punya order aktif, kirim order itu bukan daftar baru
    match ctx
        .order_svc
        .get_active_order_for_driver(&ctx.user_id)
        .await
    {
        Ok(Some(o)) => {
            ctx.send(
                Sp::CurrentOrder(ActiveOrderEvent {
                    order: Some(protoOrder {
                        order_id: o.id,
                        rider_id: o.rider_id,
                        driver_id: ctx.user_id.clone(),
                        status: o.status,
                        pickup_address: o.pickup_address,
                        pickup_lat: o.pickup_lat,
                        pickup_lng: o.pickup_lng,
                        dest_address: o.dest_address,
                        dest_lat: o.dest_lat,
                        dest_lng: o.dest_lng,
                        fare_estimate: o.fare_estimate,
                        service_type: o.service_type,
                        created_at: o.created_at,
                        driver: None,
                    }),
                }),
                Priority::Critical,
            );
        }
        Ok(None) => {
            let radius = (b.radius_km != 0.0).then_some(b.radius_km);
            match ctx
                .order_svc
                .get_nearby_orders(b.lat, b.lng, &b.service_type, radius)
                .await
            {
                Ok(orders) => {
                    let total = orders.len() as u32;
                    let items = orders
                        .iter()
                        .map(|no| NearbyOrderItem {
                            order: Some(order_to_proto(&no.order)),
                            distance_to_pickup_m: no.distance_to_pickup_m,
                            eta_to_pickup_min: no.eta_to_pickup_min,
                        })
                        .collect();
                    ctx.send(
                        Sp::NearbyOrders(NearbyOrdersEvent {
                            orders: items,
                            total,
                        }),
                        Priority::Normal,
                    );
                }
                Err(e) => ctx.send_err("BROWSE_FAILED", &e.to_string()),
            }
        }
        Err(e) => ctx.send_err("BROWSE_FAILED", &e.to_string()),
    }
}

pub async fn handle_accept_order<OR, UR, RR, NR>(
    a: AcceptOrderReq,
    ctx: &StreamContext<OR, UR, RR, NR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    if !ctx.require_role("driver") {
        return;
    }
    if let Err(e) = ctx.order_svc.driver_accept(&ctx.user_id, &a.order_id).await {
        ctx.send_err("ACCEPT_FAILED", &e.to_string());
    }
}

pub async fn handle_reject_order<OR, UR, RR, NR>(
    r: RejectOrderReq,
    ctx: &StreamContext<OR, UR, RR, NR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    // Reject tidak perlu role check ketat — log saja
    tracing::info!(driver_id = %ctx.user_id, order_id = %r.order_id, "Offer rejected");
}

pub async fn handle_arrived<OR, UR, RR, NR>(a: ArrivedReq, ctx: &StreamContext<OR, UR, RR, NR>)
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    if !ctx.require_role("driver") {
        return;
    }
    if let Err(e) = ctx
        .order_svc
        .driver_arrived(&ctx.user_id, &a.order_id)
        .await
    {
        ctx.send_err("ARRIVED_FAILED", &e.to_string());
    }
}

pub async fn handle_start_trip<OR, UR, RR, NR>(s: StartTripReq, ctx: &StreamContext<OR, UR, RR, NR>)
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    if !ctx.require_role("driver") {
        return;
    }
    if let Err(e) = ctx.order_svc.start_trip(&ctx.user_id, &s.order_id).await {
        ctx.send_err("START_FAILED", &e.to_string());
    }
}

pub async fn handle_complete_trip<OR, UR, RR, NR>(
    c: CompleteTripReq,
    ctx: &StreamContext<OR, UR, RR, NR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    if !ctx.require_role("driver") {
        return;
    }
    if let Err(e) = ctx
        .order_svc
        .complete_trip(&ctx.user_id, &c.order_id, c.distance_km)
        .await
    {
        ctx.send_err("COMPLETE_FAILED", &e.to_string());
    }
}

pub async fn handle_cancel_trip<OR, UR, RR, NR>(
    c: CancelTripReq,
    ctx: &StreamContext<OR, UR, RR, NR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    let reason = (!c.reason.is_empty()).then_some(c.reason);
    if let Err(e) = ctx
        .order_svc
        .cancel_order(&ctx.user_id, &ctx.role, &c.order_id, reason)
        .await
    {
        ctx.send_err("CANCEL_FAILED", &e.to_string());
    }
}

pub async fn handle_estimate_fare<OR, UR, RR, NR>(
    req: EstimateFareReq,
    ctx: &StreamContext<OR, UR, RR, NR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    match ctx
        .order_svc
        .estimate_fare(
            req.pickup_lat,
            req.pickup_lng,
            req.dest_lat,
            req.dest_lng,
            &req.service_type,
        )
        .await
    {
        Ok(fare) => ctx.send(Sp::FareEstimate(fare), Priority::Normal),
        Err(e) => ctx.send_err("ESTIMATE_FAILED", &e.to_string()),
    }
}

pub async fn handle_rate<OR, UR, RR, NR>(req: RateReq, ctx: &StreamContext<OR, UR, RR, NR>)
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    if let Err(e) = ctx
        .order_svc
        .submit_rating(
            &ctx.user_id,
            &ctx.role,
            &req.order_id,
            req.tip,
            req.rating as u8,
            &req.comment,
        )
        .await
    {
        ctx.send_err("RATE_FAILED", &e.to_string());
    }
}

// ── Helpers (dipakai webrtc handler juga) ────────────────────────────────────

pub async fn resolve_peer<OR, UR, RR, NR>(
    order_id: &str,
    ctx: &StreamContext<OR, UR, RR, NR>,
) -> Option<(Order, String)>
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    let order = match ctx.order_svc.order_repo.find_by_id(order_id).await {
        Ok(Some(o)) => o,
        Ok(None) => {
            ctx.send_err("ORDER_NOT_FOUND", "Order tidak ditemukan");
            return None;
        }
        Err(e) => {
            ctx.send_err("DB_ERROR", &e.to_string());
            return None;
        }
    };

    let peer_id = match get_peer_id(&ctx.role, &order, &ctx.user_id) {
        Ok(p) => p,
        Err(e) => {
            ctx.send_err("INVALID_ORDER", &e.to_string());
            return None;
        }
    };

    Some((order, peer_id))
}

fn get_peer_id(role: &str, order: &Order, user_id: &str) -> anyhow::Result<String> {
    match role {
        "driver" => {
            if order.driver_id.as_deref() != Some(user_id) {
                return Err(anyhow!("Not the assigned driver for this order"));
            }
            Ok(order.rider_id.clone())
        }
        "rider" => {
            if order.rider_id != user_id {
                return Err(anyhow!("Not the rider of this order"));
            }
            match order.driver_id.clone() {
                Some(d) => Ok(d),
                None => Err(anyhow!("No driver assigned yet")),
            }
        }
        _ => Err(anyhow!("Invalid role")),
    }
}

pub fn is_order_active(status: &str) -> bool {
    !matches!(status, "completed" | "cancelled" | "searching")
}
