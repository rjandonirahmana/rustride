// ===== service.rs =====
// TripServiceImpl + gRPC stream handler.
// Semua logic dispatch sudah dipindah ke handler/*.rs — file ini hanya
// bertanggung jawab untuk: auth, connect, reconnect flow, stream loop, cleanup.

use std::sync::Arc;
use std::{pin::Pin, time::Duration};

use tokio_stream::StreamExt;
use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};

use crate::{
    auth::JwtService,
    connections::{ConnectionManager, Priority},
    context::StreamContext,
    grpc::dispatch,
    location::LocationService,
    proto::{
        order::{ActiveOrderEvent, DriverInfo},
        ridehailing::{
            app_service_server::AppService, server_event::Payload as Sp, ClientEvent,
            ConnectedEvent, OrderStatusEvent, ServerEvent,
        },
    },
    repository::{
        notification::NotificationRepositoryTrait, order::OrderRepository,
        rideshare::RideshareRepositoryTrait, user::UserRepository,
    },
    service::{
        chat::ChatService,
        notification::NotificationService,
        order::{order_to_proto, OrderService},
        rideshare::RideshareService,
    },
    throttle::ThrottleMap,
};

pub type EventStream = Pin<Box<dyn futures::Stream<Item = Result<ServerEvent, Status>> + Send>>;

pub struct TripServiceImpl<OR, UR, RR, NR>
where
    OR: OrderRepository,
    UR: UserRepository,
    RR: RideshareRepositoryTrait,
    NR: NotificationRepositoryTrait,
{
    pub jwt: JwtService,
    pub order_svc: Arc<OrderService<OR, UR>>,
    pub connections: Arc<ConnectionManager>,
    pub user_repo: Arc<UR>,
    pub location: LocationService,
    pub chat_svc: Arc<ChatService>,
    pub rideshare_svc: Arc<RideshareService<RR, NR, UR>>,
    pub notif_svc: Arc<NotificationService<NR>>,
    pub throttle: Arc<ThrottleMap>,
}

fn err_event(code: &str, msg: &str) -> Arc<ServerEvent> {
    Arc::new(ServerEvent {
        payload: Some(Sp::Error(crate::proto::ridehailing::ErrorEvent {
            code: code.into(),
            message: msg.into(),
        })),
    })
}

pub fn extract_token(meta: &MetadataMap) -> Option<&str> {
    meta.get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

#[tonic::async_trait]
impl<OR, UR, RR, NR> AppService for TripServiceImpl<OR, UR, RR, NR>
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    type StreamStream = EventStream;

    async fn stream(
        &self,
        request: Request<Streaming<ClientEvent>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        // ── Auth ──────────────────────────────────────────────────────────────
        let token = extract_token(request.metadata())
            .ok_or_else(|| Status::unauthenticated("Missing Authorization header"))?;

        let claims = self
            .jwt
            .verify(token)
            .map_err(|e| Status::unauthenticated(format!("Invalid or expired token: {e}")))?;

        let user_id = claims.sub.clone();
        let role = claims.role.clone();
        let vehicle_type = claims.vehicle_type.clone();

        // ── Connect + notify ──────────────────────────────────────────────────
        let (event_rx, cancel_token) = self.connections.connect(&user_id, &role).await;

        tracing::info!("user connected {} {} {}", user_id, role, vehicle_type);

        self.connections.send(
            &user_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::Connected(ConnectedEvent {
                    user_id: user_id.clone(),
                    role: role.clone(),
                    username: claims.name.clone(),
                    vehicle_type: vehicle_type.clone(),
                })),
            }),
            Priority::Critical,
        );

        // ── Reconnect flow ────────────────────────────────────────────────────
        if role == "rider" {
            spawn_rider_reconnect(
                user_id.clone(),
                self.order_svc.clone(),
                self.connections.clone(),
                self.user_repo.clone(),
            );
        } else {
            spawn_driver_reconnect(
                user_id.clone(),
                self.order_svc.clone(),
                self.connections.clone(),
                self.user_repo.clone(),
            );
        }

        // ── Stream context (di-clone sekali, di-move ke spawn) ───────────────
        let ctx = StreamContext {
            user_id: user_id.clone(),
            role: role.clone(),
            username: claims.name.clone(),
            vehicle_type: vehicle_type.clone(),
            order_svc: self.order_svc.clone(),
            connections: self.connections.clone(),
            location: self.location.clone(),
            chat_svc: self.chat_svc.clone(),
            rideshare_svc: self.rideshare_svc.clone(),
            notif_svc: self.notif_svc.clone(),
            throttle: self.throttle.clone(),
        };

        let mut in_stream = request.into_inner();

        // ── Stream loop ───────────────────────────────────────────────────────
        tokio::spawn(async move {
            loop {
                tokio::select! {

                    // Normal message processing
                    maybe = in_stream.next() => {
                        match maybe {
                            Some(Ok(event)) => {
                                if let Some(payload) = event.payload {
                                    dispatch(payload, &ctx).await;
                                }
                            }
                            Some(Err(e)) => {
                                // Status error dari gRPC — connection bermasalah
                                tracing::warn!(user_id = %ctx.user_id, error = %e, "stream error");
                                break;
                            }
                            None => {
                                // Client tutup stream secara normal
                                tracing::info!(user_id = %ctx.user_id, "stream closed by client");
                                break;
                            }
                        }
                    }
                    // Graceful shutdown dari server
                    _ = cancel_token.cancelled() => {
                        tracing::info!(user_id = %ctx.user_id, "stream cancelled by server");
                        break;
                    }
                }
            }

            // ── Cleanup on disconnect ─────────────────────────────────────────
            ctx.connections.disconnect(&ctx.user_id, cancel_token).await;

            if ctx.role == "rider" {
                match ctx
                    .order_svc
                    .cancel_active_order_on_disconnect(&ctx.user_id)
                    .await
                {
                    Ok(Some(order_id)) => {
                        tracing::info!(user_id = %ctx.user_id, order_id, "order auto-cancelled on disconnect");
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::error!(user_id = %ctx.user_id, error = %e, "failed to cancel order on disconnect");
                    }
                }
            }

            if ctx.role == "driver" {
                // Hapus dari kedua geo index menggunakan vehicle_type yang benar.
                // Versi sebelumnya hardcode "motor" dan "mobil" — ini menyebabkan
                // driver dengan vehicle_type lain tidak ter-remove dengan benar.
                let _ = ctx
                    .location
                    .remove_driver(&ctx.user_id, &ctx.vehicle_type)
                    .await;
            }
        });

        let out = tokio_stream::wrappers::ReceiverStream::new(event_rx)
            .map(|r| r.map(|arc| (arc).clone()));
        Ok(Response::new(Box::pin(out)))
    }
}

// ── Reconnect helpers ─────────────────────────────────────────────────────────

fn spawn_rider_reconnect<OR, UR>(
    user_id: String,
    order_svc: Arc<OrderService<OR, UR>>,
    connections: Arc<ConnectionManager>,
    user_repo: Arc<UR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
{
    tokio::spawn(async move {
        tracing::info!(rider_id = %user_id, "rider reconnect flow started");

        match tokio::time::timeout(
            std::time::Duration::from_secs(10),
            order_svc.get_active_order_for_user(&user_id),
        )
        .await
        {
            Ok(Ok(Some(o))) => {
                let driver_info = resolve_driver_info(&o.driver_id, &user_repo).await;
                connections.send(
                    &user_id,
                    Arc::new(ServerEvent {
                        payload: Some(Sp::OrderStatus(OrderStatusEvent {
                            order_id: o.id,
                            status: o.status,
                            driver: driver_info,
                            fare_estimate: o.fare_estimate,
                            service_type: o.service_type,
                            rider_id: o.rider_id,
                            rider_name: o.rider_name,
                        })),
                    }),
                    Priority::Critical,
                );
            }
            Ok(Ok(None)) => {
                tracing::info!(rider_id = %user_id, "no active order on reconnect");
            }
            Ok(Err(e)) => {
                tracing::warn!(rider_id = %user_id, error = %e, "failed to fetch active order on reconnect");
                connections.send(
                    &user_id,
                    err_event(
                        "ORDER_FETCH_FAILED",
                        &format!("Could not load active order: {e}"),
                    ),
                    Priority::Normal,
                );
            }
            Err(_) => {
                tracing::error!(rider_id = %user_id, "timeout fetching active order on reconnect");
                connections.send(
                    &user_id,
                    err_event("ORDER_TIMEOUT", "Order fetch timeout - please reconnect"),
                    Priority::Normal,
                );
            }
        }
    });
}

fn spawn_driver_reconnect<OR, UR>(
    user_id: String,
    order_svc: Arc<OrderService<OR, UR>>,
    connections: Arc<ConnectionManager>,
    user_repo: Arc<UR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
{
    tokio::spawn(async move {
        match order_svc.get_active_order_for_driver(&user_id).await {
            Ok(Some(o)) => {
                let mut proto_order = order_to_proto(&o);
                proto_order.driver = resolve_driver_info(&o.driver_id, &user_repo).await;
                connections.send(
                    &user_id,
                    Arc::new(ServerEvent {
                        payload: Some(Sp::CurrentOrder(ActiveOrderEvent {
                            order: Some(proto_order),
                        })),
                    }),
                    Priority::Critical,
                );
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(driver_id = %user_id, error = %e, "failed to fetch active order on reconnect");
            }
        }
    });
}

/// Fetch driver info untuk reconnect event. Gagal = return None, tidak panic.
async fn resolve_driver_info<UR: UserRepository>(
    driver_id: &Option<String>,
    user_repo: &Arc<UR>,
) -> Option<DriverInfo> {
    let driver_id = driver_id.as_ref()?;
    match user_repo.find_driver_by_id(driver_id).await {
        Ok(Some((user, profile))) => Some(DriverInfo {
            user_id: driver_id.clone(),
            name: user.name,
            phone: user.phone,
            vehicle_plate: profile.vehicle_plate,
            vehicle_model: profile.vehicle_model,
            vehicle_color: profile.vehicle_color,
            rating: profile.rating,
        }),
        Ok(None) => {
            tracing::warn!(driver_id, "driver not found for reconnect info");
            None
        }
        Err(e) => {
            tracing::error!(driver_id, error = %e, "failed to load driver info for reconnect");
            None
        }
    }
}
