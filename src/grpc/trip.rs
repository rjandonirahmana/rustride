use anyhow::{anyhow, Result};
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};

use crate::{
    auth::JwtService,
    connections::{ConnectionManager, Priority},
    location::LocationService,
    models::order::Order as ordermodels,
    proto::ridehailing::{
        app_service_server::AppService, client_event::Payload as Cp, server_event::Payload as Sp,
        CallEndedEvent, ClientEvent, ConnectedEvent, CurrentOrder, DriverInfo, ErrorEvent,
        GetUploadUrlRes, HistoryEvent, IceCandidateEvent, IncomingCallAnswer, IncomingCallOffer,
        NearbyOrderItem, NearbyOrdersEvent, NearbyRideshareEvent, NewMessageEvent, Order,
        OrderCreatedEvent, OrderStatusEvent, PongEvent, PresenceEvent, RideshareJoinedEvent,
        RideshareOpenedEvent, SendMediaResponse, ServerEvent,
    },
    repository::{
        notification::NotificationRepositorytrait, order::OrderRepository,
        rideshare::RideshareRepositoryTrait, user::UserRepository,
    },
    service::{
        chat::ChatService as service_chat,
        notification::NotificationService,
        order::{order_to_proto, OrderService},
        rideshare::RideshareService,
    },
};

pub type EventStream = Pin<Box<dyn futures::Stream<Item = Result<ServerEvent, Status>> + Send>>;

pub struct TripServiceImpl<
    OR: OrderRepository,
    UR: UserRepository,
    RR: RideshareRepositoryTrait,
    NR: NotificationRepositorytrait,
> {
    pub jwt: JwtService,
    pub order_svc: Arc<OrderService<OR, UR>>,
    pub connections: Arc<ConnectionManager>,
    pub user_repo: Arc<UR>,
    pub location: LocationService,
    pub chat_svc: Arc<service_chat>,
    pub rideshare_svc: Arc<RideshareService<RR, NR, UR>>,
    pub notif_svc: Arc<NotificationService<NR>>,
}

fn err_event(code: &str, msg: &str) -> Arc<ServerEvent> {
    Arc::new(ServerEvent {
        payload: Some(Sp::Error(ErrorEvent {
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
    NR: NotificationRepositorytrait + 'static,
{
    type StreamStream = EventStream;

    async fn stream(
        &self,
        request: Request<Streaming<ClientEvent>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        // ── 1. Auth ───────────────────────────────────────────────────────────
        let token = extract_token(request.metadata())
            .ok_or_else(|| Status::unauthenticated("Missing Authorization header"))?;

        let claims = self
            .jwt
            .verify(token)
            .map_err(|e| Status::unauthenticated(format!("Invalid or expired token: {}", e)))?;

        let user_id = claims.sub.clone();
        let role = claims.role.clone();
        let vehicle_type = claims.vehicle_type.clone();

        // ── 2. Register channel server→client ─────────────────────────────────
        // FIX: connect() sekarang return (Receiver, CancellationToken)
        let (event_rx, cancel_token) = self.connections.connect(&user_id, &role).await;

        if role == "driver" {
            let _ = self.user_repo.set_driver_active(&user_id, true).await;
        } else if role == "rider" {
        }

        tracing::info!("userid {} connect {} {}", &user_id, &role, &vehicle_type);

        // FIX: semua send() pakai Arc::new(event) + Priority
        self.connections.send(
            &user_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::Connected(ConnectedEvent {
                    user_id: user_id.clone(),
                    role: role.clone(),
                    username: claims.name.clone(),
                })),
            }),
            Priority::Critical,
        );

        let uid = user_id.clone();

        if role == "rider" {
            let order_svc2 = self.order_svc.clone();
            let connections2 = self.connections.clone();
            let user_repo2 = self.user_repo.clone();
            let uid2 = user_id.clone();

            tokio::spawn(async move {
                tracing::info!("Rider reconnect flow started for: {}", uid2);

                match tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    order_svc2.get_active_order_for_user(&uid2),
                )
                .await
                {
                    Ok(Ok(Some(o))) => {
                        let driver_info = if let Some(ref driver_id) = o.driver_id {
                            match user_repo2.find_driver_by_id(driver_id).await {
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
                                    tracing::warn!(
                                        driver_id = %driver_id,
                                        "Driver not found in database"
                                    );
                                    None
                                }
                                Err(e) => {
                                    tracing::error!(
                                        driver_id = %driver_id,
                                        error = %e,
                                        "Failed to load driver info"
                                    );
                                    None
                                }
                            }
                        } else {
                            None
                        };

                        connections2.send(
                            &uid2,
                            Arc::new(ServerEvent {
                                payload: Some(Sp::OrderStatus(OrderStatusEvent {
                                    order_id: o.id,
                                    status: o.status,
                                    driver: driver_info,
                                    fare_estimate: o.fare_estimate,
                                    service_type: o.service_type,
                                })),
                            }),
                            Priority::Critical,
                        );
                    }
                    Ok(Ok(None)) => {
                        tracing::info!(rider_id = %uid2, "No active order");
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(rider_id = %uid2, error = %e, "Failed to fetch active order");
                        connections2.send(
                            &uid2,
                            err_event(
                                "ORDER_FETCH_FAILED",
                                &format!("Could not load active order: {}", e),
                            ),
                            Priority::Normal,
                        );
                    }
                    Err(_) => {
                        tracing::error!(rider_id = %uid2, "Timeout fetching active order (>10s)");
                        connections2.send(
                            &uid2,
                            err_event("ORDER_TIMEOUT", "Order fetch timeout - please reconnect"),
                            Priority::Normal,
                        );
                    }
                }
            });
        } else {
            let order_svc = self.order_svc.clone();
            let connections2 = self.connections.clone();
            let uid2 = user_id.clone();

            tokio::spawn(async move {
                match order_svc.get_active_order_for_driver(&uid2).await {
                    Ok(Some(o)) => {
                        connections2.send(
                            &uid2,
                            Arc::new(ServerEvent {
                                payload: Some(Sp::CurrentOrder(CurrentOrder {
                                    order: Some(Order {
                                        id: o.id,
                                        rider_id: o.rider_id,
                                        driver_id: uid2.to_string(),
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
                                        rider_name: o.rider_name,
                                    }),
                                })),
                            }),
                            Priority::Critical,
                        );
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(user_id = %uid2, "Gagal fetch active order on connect: {}", e);
                    }
                }
            });
        }

        // ── 3. Spawn reader task — client → server ────────────────────────────
        let order_svc = self.order_svc.clone();
        let connections = self.connections.clone();
        let user_repo = self.user_repo.clone();
        let location = self.location.clone();
        let role_r = role.clone();
        let mut in_stream = request.into_inner();
        let name = claims.name.clone();
        let chat_svc = self.chat_svc.clone();
        let notif_svc = self.notif_svc.clone();
        let rideshare_svc = self.rideshare_svc.clone();

        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(event) => {
                        dispatch(
                            event,
                            &uid,
                            &role_r,
                            &name,
                            &vehicle_type,
                            &order_svc,
                            &connections,
                            &location,
                            &chat_svc,
                            &rideshare_svc,
                            &notif_svc,
                        )
                        .await;
                    }
                    Err(e) => {
                        tracing::warn!(user_id = %uid, "Stream error: {}", e);
                        break;
                    }
                }
            }

            tracing::info!(user_id = %uid, "gRPC stream closed");
            // FIX: disconnect() sekarang butuh cancel_token
            connections.disconnect(&uid, cancel_token).await;

            if role_r == "rider" {
                match order_svc.cancel_active_order_on_disconnect(&uid).await {
                    Ok(Some(order_id)) => {
                        tracing::info!(user_id = %uid, order_id = %order_id, "Order auto-cancelled");
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::error!(user_id = %uid, "Gagal cancel order saat disconnect: {}", e);
                    }
                }
            }

            if role_r == "driver" {
                let _ = user_repo.set_driver_active(&uid, false).await;
                let _ = location.remove_driver(&uid, "motor").await;
                let _ = location.remove_driver(&uid, "mobil").await;
            }
        });

        let out = tokio_stream::wrappers::ReceiverStream::new(event_rx);
        Ok(Response::new(Box::pin(out)))
    }
}

// ── Event dispatcher ──────────────────────────────────────────────────────────

async fn dispatch<OR, UR, RR, NR>(
    event: ClientEvent,
    user_id: &str,
    role: &str,
    username: &str,
    vehicle_type: &str,
    order_svc: &Arc<OrderService<OR, UR>>,
    connections: &Arc<ConnectionManager>,
    location: &LocationService,
    chat_svc: &Arc<service_chat>,
    rideshare_svc: &Arc<RideshareService<RR, NR, UR>>,
    notif_svc: &Arc<NotificationService<NR>>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositorytrait + 'static,
{
    let payload = match event.payload {
        Some(p) => p,
        None => return,
    };

    match payload {
        Cp::GetMessageReq(_) => match chat_svc.get_conversations(user_id).await {
            Ok(data) => {
                connections.send(
                    user_id,
                    Arc::new(ServerEvent {
                        payload: Some(Sp::ConversationItems(
                            crate::proto::ridehailing::ConversationItems { items: data },
                        )),
                    }),
                    Priority::Normal,
                );
            }
            Err(e) => {
                connections.send(
                    user_id,
                    err_event("FIND_MESSAGE_ERROR", &e.to_string()),
                    Priority::Normal,
                );
            }
        },

        // ── Rider: watch driver aktif di sekitar ──────────────────────────────
        Cp::WatchDrivers(req) => {
            if role != "rider" {
                connections.send(
                    user_id,
                    err_event("FORBIDDEN", "Hanya rider"),
                    Priority::Normal,
                );
                return;
            }

            match location
                .find_nearby_drivers(req.lat, req.lng, &req.service_type)
                .await
            {
                Ok(drivers) => {
                    let mut items = vec![];
                    for (driver_id, dist_m) in drivers.iter() {
                        if let Ok(Some(loc)) = location.get_driver_location(driver_id).await {
                            items.push(crate::proto::ridehailing::NearbyDriverItem {
                                driver_id: driver_id.clone(),
                                lat: loc.lat,
                                lng: loc.lng,
                                heading: loc.heading.unwrap_or(0.0),
                                distance_m: *dist_m as f32,
                            });
                        }
                    }
                    connections.send(
                        user_id,
                        Arc::new(ServerEvent {
                            payload: Some(Sp::NearbyDrivers(
                                crate::proto::ridehailing::NearbyDriversEvent { drivers: items },
                            )),
                        }),
                        Priority::Normal,
                    );
                }
                Err(e) => {
                    connections.send(
                        user_id,
                        err_event("FIND_FAILED", &e.to_string()),
                        Priority::Normal,
                    );
                }
            }
        }

        // ── Rider: buat order ─────────────────────────────────────────────────
        Cp::CreateOrder(c) => {
            if role != "rider" {
                connections.send(
                    user_id,
                    err_event("FORBIDDEN", "Hanya rider"),
                    Priority::Normal,
                );
                return;
            }
            match order_svc
                .create_order(
                    user_id,
                    c.pickup_lat,
                    c.pickup_lng,
                    c.pickup_address,
                    c.dest_lat,
                    c.dest_lng,
                    c.dest_address,
                    &c.service_type,
                )
                .await
            {
                Ok(order) => connections.send(
                    user_id,
                    Arc::new(ServerEvent {
                        payload: Some(Sp::OrderCreated(OrderCreatedEvent {
                            order_id: order.id,
                            fare_estimate: order.fare_estimate,
                            service_type: order.service_type,
                        })),
                    }),
                    Priority::Critical,
                ),
                Err(e) => connections.send(
                    user_id,
                    err_event("CREATE_FAILED", &e.to_string()),
                    Priority::Normal,
                ),
            }
        }

        // ── Driver: update lokasi ─────────────────────────────────────────────
        Cp::Location(loc) => {
            if role != "driver" {
                connections.send(
                    user_id,
                    err_event("FORBIDDEN", "Hanya driver"),
                    Priority::Normal,
                );
                return;
            }

            if let Err(e) = location
                .update_driver_location(
                    user_id,
                    loc.lat,
                    loc.lng,
                    (loc.heading != 0.0).then_some(loc.heading as f32),
                    (loc.speed != 0.0).then_some(loc.speed as f32),
                    vehicle_type,
                )
                .await
            {
                connections.send(
                    user_id,
                    err_event("LOC_UPDATE_FAILED", &e.to_string()),
                    Priority::Normal,
                );
                return;
            }

            match order_svc.get_active_order_for_driver(user_id).await {
                Ok(Some(order)) => {
                    connections.send(
                        &order.rider_id,
                        Arc::new(ServerEvent {
                            payload: Some(Sp::DriverLoc(
                                crate::proto::ridehailing::DriverLocationEvent {
                                    lat: loc.lat,
                                    lng: loc.lng,
                                    heading: loc.heading,
                                    order_id: order.id,
                                },
                            )),
                        }),
                        // Location update ke rider saat trip aktif = Normal priority
                        Priority::Normal,
                    );
                }
                Err(e) => {
                    connections.send(
                        user_id,
                        err_event("LOC_UPDATE_FAILED", &e.to_string()),
                        Priority::Normal,
                    );
                }
                Ok(None) => {}
            }
        }

        // ── Driver: browse order tersedia ─────────────────────────────────────
        Cp::BrowseOrders(b) => {
            if role != "driver" {
                connections.send(
                    user_id,
                    err_event("FORBIDDEN", "Hanya driver"),
                    Priority::Normal,
                );
                return;
            }

            match order_svc.get_active_order_for_driver(user_id).await {
                Ok(Some(o)) => {
                    connections.send(
                        user_id,
                        Arc::new(ServerEvent {
                            payload: Some(Sp::CurrentOrder(CurrentOrder {
                                order: Some(Order {
                                    id: o.id,
                                    rider_id: o.rider_id,
                                    driver_id: user_id.to_string(),
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
                                    rider_name: o.rider_name,
                                }),
                            })),
                        }),
                        Priority::Critical,
                    );
                }
                Ok(None) => {
                    let radius = (b.radius_km != 0.0).then_some(b.radius_km);
                    match order_svc
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
                            connections.send(
                                user_id,
                                Arc::new(ServerEvent {
                                    payload: Some(Sp::NearbyOrders(NearbyOrdersEvent {
                                        orders: items,
                                        total,
                                    })),
                                }),
                                Priority::Normal,
                            );
                        }
                        Err(e) => {
                            connections.send(
                                user_id,
                                err_event("BROWSE_FAILED", &e.to_string()),
                                Priority::Normal,
                            );
                        }
                    }
                }
                Err(e) => {
                    connections.send(
                        user_id,
                        err_event("BROWSE_FAILED", &e.to_string()),
                        Priority::Normal,
                    );
                }
            }
        }

        // ── Driver: accept order ──────────────────────────────────────────────
        Cp::AcceptOrder(a) => {
            if role != "driver" {
                connections.send(
                    user_id,
                    err_event("FORBIDDEN", "Hanya driver"),
                    Priority::Normal,
                );
                return;
            }
            if let Err(e) = order_svc.driver_accept(user_id, &a.order_id).await {
                connections.send(
                    user_id,
                    err_event("ACCEPT_FAILED", &e.to_string()),
                    Priority::Normal,
                );
            }
        }

        // ── Driver: reject offer ──────────────────────────────────────────────
        Cp::RejectOrder(r) => {
            tracing::info!(driver_id = %user_id, order_id = %r.order_id, "Offer ditolak");
        }

        // ── Driver: tiba di pickup ────────────────────────────────────────────
        Cp::Arrived(a) => {
            if role != "driver" {
                return;
            }
            if let Err(e) = order_svc.driver_arrived(user_id, &a.order_id).await {
                connections.send(
                    user_id,
                    err_event("ARRIVED_FAILED", &e.to_string()),
                    Priority::Normal,
                );
            }
        }

        // ── Driver: mulai trip ────────────────────────────────────────────────
        Cp::StartTrip(s) => {
            if role != "driver" {
                return;
            }
            if let Err(e) = order_svc.start_trip(user_id, &s.order_id).await {
                connections.send(
                    user_id,
                    err_event("START_FAILED", &e.to_string()),
                    Priority::Normal,
                );
            }
        }

        // ── Driver: selesai trip ──────────────────────────────────────────────
        Cp::CompleteTrip(c) => {
            if role != "driver" {
                return;
            }
            if let Err(e) = order_svc
                .complete_trip(user_id, &c.order_id, c.distance_km)
                .await
            {
                connections.send(
                    user_id,
                    err_event("COMPLETE_FAILED", &e.to_string()),
                    Priority::Normal,
                );
            }
        }

        // ── Cancel (rider atau driver) ────────────────────────────────────────
        Cp::CancelTrip(c) => {
            let reason = (!c.reason.is_empty()).then_some(c.reason);
            if let Err(e) = order_svc
                .cancel_order(user_id, role, &c.order_id, reason)
                .await
            {
                connections.send(
                    user_id,
                    err_event("CANCEL_FAILED", &e.to_string()),
                    Priority::Normal,
                );
            }
        }

        // ── Ping/Pong ─────────────────────────────────────────────────────────
        Cp::Ping(_) => {
            connections.send(
                user_id,
                Arc::new(ServerEvent {
                    payload: Some(Sp::Pong(PongEvent {})),
                }),
                Priority::Normal,
            );
        }

        // ── Send message ──────────────────────────────────────────────────────
        Cp::SendMessage(m) => {
            if m.content.trim().is_empty() {
                connections.send(
                    user_id,
                    err_event("EMPTY_CONTENT", "Pesan tidak boleh kosong"),
                    Priority::Normal,
                );
                return;
            }

            let order = match order_svc.order_repo.find_by_id(&m.order_id).await {
                Ok(Some(o)) => o,
                Ok(None) => {
                    connections.send(
                        user_id,
                        err_event("ORDER_NOT_FOUND", "Order tidak ditemukan"),
                        Priority::Normal,
                    );
                    return;
                }
                Err(e) => {
                    connections.send(
                        user_id,
                        err_event("DB_ERROR", &e.to_string()),
                        Priority::Normal,
                    );
                    return;
                }
            };

            let peer_id = match get_peer_id(role, &order, user_id) {
                Ok(p) => p,
                Err(e) => {
                    connections.send(
                        user_id,
                        err_event("INVALID_ORDER", &e.to_string()),
                        Priority::Normal,
                    );
                    return;
                }
            };

            if m.recipient_id != peer_id {
                tracing::warn!(
                    user_id = %user_id,
                    recipient = %m.recipient_id,
                    expected = %peer_id,
                    "Recipient mismatch"
                );
                connections.send(
                    user_id,
                    err_event("INVALID_RECIPIENT", "Tidak bisa chat dengan person ini"),
                    Priority::Normal,
                );
                return;
            }

            if !is_order_active(&order.status) {
                connections.send(
                    user_id,
                    err_event(
                        "INACTIVE_ORDER",
                        "Chat hanya tersedia saat order berlangsung",
                    ),
                    Priority::Normal,
                );
                return;
            }

            if let Err(e) = chat_svc
                .send_text(user_id, &peer_id, &m.content, username, &order.id)
                .await
            {
                tracing::error!(error = %e, "send_text failed");
                connections.send(
                    user_id,
                    err_event("SEND_FAILED", &e.to_string()),
                    Priority::Normal,
                );
            }
        }

        // ── Get upload URL ────────────────────────────────────────────────────
        Cp::GetUploadUrl(req) => match chat_svc.get_upload_url(user_id, &req.mime).await {
            Ok(d) => {
                connections.send(
                    user_id,
                    Arc::new(ServerEvent {
                        payload: Some(Sp::UploadUrlRes(GetUploadUrlRes {
                            upload_url: d.0,
                            public_url: d.1,
                        })),
                    }),
                    Priority::Normal,
                );
            }
            Err(e) => {
                connections.send(
                    user_id,
                    err_event("GET_UPLOAD_URL_ERROR", &e.to_string()),
                    Priority::Normal,
                );
            }
        },

        // ── Kirim media ───────────────────────────────────────────────────────
        Cp::SendMedia(m) => {
            if m.media_url.is_empty() {
                connections.send(
                    user_id,
                    err_event("MISSING_URL", "media_url tidak boleh kosong"),
                    Priority::Normal,
                );
                return;
            }

            if !m
                .media_url
                .starts_with("http://vmi3152926.contaboserver.net/rustride/")
            {
                connections.send(
                    user_id,
                    err_event("INVALID_URL", "URL tidak valid"),
                    Priority::Normal,
                );
                return;
            }

            if !m.media_mime.starts_with("image/")
                && !m.media_mime.starts_with("video/")
                && !m.media_mime.starts_with("audio/")
            {
                connections.send(
                    user_id,
                    err_event("INVALID_MIME", "Tipe file tidak didukung"),
                    Priority::Normal,
                );
                return;
            }

            let order = match order_svc.order_repo.find_by_id(&m.order_id).await {
                Ok(Some(o)) => o,
                Ok(None) => {
                    connections.send(
                        user_id,
                        err_event("ORDER_NOT_FOUND", "Order tidak ditemukan"),
                        Priority::Normal,
                    );
                    return;
                }
                Err(e) => {
                    connections.send(
                        user_id,
                        err_event("DB_ERROR", &e.to_string()),
                        Priority::Normal,
                    );
                    return;
                }
            };

            let peer_id = match get_peer_id(role, &order, user_id) {
                Ok(p) => p,
                Err(e) => {
                    connections.send(
                        user_id,
                        err_event("INVALID_ORDER", &e.to_string()),
                        Priority::Normal,
                    );
                    return;
                }
            };

            if m.recipient_id != peer_id {
                tracing::warn!(
                    user_id = %user_id,
                    recipient = %m.recipient_id,
                    expected = %peer_id,
                    "Recipient mismatch in SendMedia"
                );
                connections.send(
                    user_id,
                    err_event("INVALID_RECIPIENT", "Tidak bisa chat dengan person ini"),
                    Priority::Normal,
                );
                return;
            }

            if !is_order_active(&order.status) {
                connections.send(
                    user_id,
                    err_event(
                        "INACTIVE_ORDER",
                        "Chat hanya tersedia saat order berlangsung",
                    ),
                    Priority::Normal,
                );
                return;
            }

            match chat_svc
                .send_media(
                    user_id,
                    &peer_id,
                    &m.media_url,
                    &m.media_mime,
                    m.media_size,
                    username,
                    &m.order_id,
                )
                .await
            {
                Err(e) => {
                    tracing::error!(error = %e, "send_media failed");
                    connections.send(
                        user_id,
                        err_event("SEND_FAILED", &e.to_string()),
                        Priority::Normal,
                    );
                }
                Ok(d) => {
                    connections.send(
                        user_id,
                        Arc::new(ServerEvent {
                            payload: Some(Sp::SendMediaResponse(SendMediaResponse {
                                client_msg_id: m.client_msg_id,
                                msg_id: d.id,
                            })),
                        }),
                        Priority::Normal,
                    );
                }
            }
        }

        // ── Mark read ─────────────────────────────────────────────────────────
        Cp::MarkRead(r) => {
            if let Err(e) = chat_svc.mark_read(user_id, &r.from_user_id).await {
                tracing::error!(error = %e, "mark_read failed");
            }
        }

        // ── Watch user presence ───────────────────────────────────────────────
        Cp::WatchUser(w) => {
            let online = connections.is_connected(&w.target_user_id).await;
            connections.send(
                user_id,
                Arc::new(ServerEvent {
                    payload: Some(Sp::Presence(PresenceEvent {
                        user_id: w.target_user_id,
                        online,
                        last_seen: String::new(),
                    })),
                }),
                Priority::Normal,
            );
        }

        // ── Get history ───────────────────────────────────────────────────────
        Cp::GetHistory(h) => {
            let before = (!h.before.is_empty()).then_some(h.before.as_str());
            match chat_svc
                .get_history(user_id, &h.peer_id, h.limit, before)
                .await
            {
                Ok(msgs) => {
                    let has_more = msgs.len() as i32 == h.limit;
                    let items = msgs
                        .iter()
                        .map(|m| NewMessageEvent {
                            msg_id: m.id.clone(),
                            sender_id: m.sender_id.clone(),
                            sender_name: m.sender_name.clone().unwrap(),
                            recipient_id: m.recipient_id.clone(),
                            content: m.content.clone(),
                            msg_type: m.msg_type.clone(),
                            sent_at: m.sent_at.clone(),
                            media_url: m.media_url.clone().unwrap_or_default(),
                            media_mime: m.media_mime.clone().unwrap_or_default(),
                            media_size: m.media_size.unwrap_or(0),
                            media_duration: m.media_duration.unwrap_or(0),
                            media_thumb: m.media_thumb.clone().unwrap_or_default(),
                            sender_avatar: m.sender_avatar.clone().unwrap_or_default(),
                            status_order: m.status_order.clone().unwrap_or_default(),
                        })
                        .collect();
                    connections.send(
                        user_id,
                        Arc::new(ServerEvent {
                            payload: Some(Sp::History(HistoryEvent {
                                messages: items,
                                has_more,
                            })),
                        }),
                        Priority::Normal,
                    );
                }
                Err(e) => connections.send(
                    user_id,
                    err_event("HISTORY_FAILED", &e.to_string()),
                    Priority::Normal,
                ),
            }
        }

        // ── Get active order ──────────────────────────────────────────────────
        Cp::GetActiveOrder(_) => match order_svc.get_active_order_for_user(user_id).await {
            Ok(Some(order)) => {
                connections.send(
                    user_id,
                    Arc::new(ServerEvent {
                        payload: Some(Sp::OrderCreated(
                            crate::proto::ridehailing::OrderCreatedEvent {
                                order_id: order.id,
                                fare_estimate: order.fare_estimate,
                                service_type: order.service_type,
                            },
                        )),
                    }),
                    Priority::Critical,
                );
            }
            Ok(None) => {}
            Err(e) => connections.send(
                user_id,
                err_event("ACTIVE_ORDER_FAILED", &e.to_string()),
                Priority::Normal,
            ),
        },

        // ── Estimate fare ─────────────────────────────────────────────────────
        Cp::EstimateFare(req) => {
            match order_svc
                .estimate_fare(
                    req.pickup_lat,
                    req.pickup_lng,
                    req.dest_lat,
                    req.dest_lng,
                    &req.service_type,
                )
                .await
            {
                Ok(fare) => {
                    connections.send(
                        user_id,
                        Arc::new(ServerEvent {
                            payload: Some(Sp::FareEstimate(fare)),
                        }),
                        Priority::Normal,
                    );
                }
                Err(e) => connections.send(
                    user_id,
                    err_event("ESTIMATE_FAILED", &e.to_string()),
                    Priority::Normal,
                ),
            }
        }

        // ── Rate ──────────────────────────────────────────────────────────────
        Cp::Rate(req) => {
            if let Err(e) = order_svc
                .submit_rating(
                    user_id,
                    role,
                    &req.order_id,
                    req.tip,
                    req.rating as u8,
                    &req.comment,
                )
                .await
            {
                connections.send(
                    user_id,
                    err_event("RATE_FAILED", &e.to_string()),
                    Priority::Normal,
                );
            }
        }

        // ── Driver: buka trip nebeng ──────────────────────────────────────────
        Cp::OpenRideshare(req) => {
            if role != "driver" {
                connections.send(
                    user_id,
                    err_event("FORBIDDEN", "Hanya driver"),
                    Priority::Normal,
                );
                return;
            }

            let order = match order_svc.order_repo.find_active_for_driver(user_id).await {
                Ok(Some(o)) => o,
                Ok(None) => {
                    connections.send(
                        user_id,
                        err_event("NO_ORDER", "Tidak ada order aktif"),
                        Priority::Normal,
                    );
                    return;
                }
                Err(e) => {
                    connections.send(
                        user_id,
                        err_event("ORDER_FAILED", &e.to_string()),
                        Priority::Normal,
                    );
                    return;
                }
            };

            let deadline = (req.join_deadline_s > 0).then_some(req.join_deadline_s as i64);
            let max = if req.max_passengers > 0 {
                req.max_passengers
            } else {
                3
            };

            match rideshare_svc
                .open_trip(
                    user_id,
                    &req.order_id,
                    order.pickup_lat,
                    order.pickup_lng,
                    order.dest_lat,
                    order.dest_lng,
                    &order.service_type,
                    max,
                    deadline,
                )
                .await
            {
                Ok(trip_id) => connections.send(
                    user_id,
                    Arc::new(ServerEvent {
                        payload: Some(Sp::RideshareOpened(RideshareOpenedEvent {
                            trip_id,
                            order_id: req.order_id,
                            max_passengers: max,
                        })),
                    }),
                    Priority::Critical,
                ),
                Err(e) => connections.send(
                    user_id,
                    err_event("OPEN_FAILED", &e.to_string()),
                    Priority::Normal,
                ),
            }
        }

        // ── Rider: cari trip nebeng ───────────────────────────────────────────
        Cp::BrowseRideshare(req) => {
            if role != "rider" {
                connections.send(
                    user_id,
                    err_event("FORBIDDEN", "Hanya rider"),
                    Priority::Normal,
                );
                return;
            }

            let radius = if req.radius_km > 0.0 {
                req.radius_km as f64
            } else {
                5.0
            };

            match rideshare_svc
                .browse_trips(
                    req.pickup_lat,
                    req.pickup_lng,
                    req.dest_lat,
                    req.dest_lng,
                    &req.service_type,
                    radius,
                )
                .await
            {
                Ok(trips) => connections.send(
                    user_id,
                    Arc::new(ServerEvent {
                        payload: Some(Sp::NearbyRideshare(NearbyRideshareEvent { trips })),
                    }),
                    Priority::Normal,
                ),
                Err(e) => connections.send(
                    user_id,
                    err_event("BROWSE_RS_FAILED", &e.to_string()),
                    Priority::Normal,
                ),
            }
        }

        // ── Rider: request nebeng ─────────────────────────────────────────────
        Cp::JoinRideshare(req) => {
            if role != "rider" {
                connections.send(
                    user_id,
                    err_event("FORBIDDEN", "Hanya rider"),
                    Priority::Normal,
                );
                return;
            }

            let avatar = order_svc
                .user_repo
                .find_by_id(user_id)
                .await
                .ok()
                .flatten()
                .and_then(|u| u.avatar_url)
                .unwrap_or_default();

            match rideshare_svc
                .join_trip(
                    user_id,
                    username,
                    &avatar,
                    &req.trip_id,
                    req.pickup_lat,
                    req.pickup_lng,
                    &req.pickup_address,
                    req.dest_lat,
                    req.dest_lng,
                    &req.dest_address,
                )
                .await
            {
                Ok((passenger_id, fare)) => connections.send(
                    user_id,
                    Arc::new(ServerEvent {
                        payload: Some(Sp::RideshareJoined(RideshareJoinedEvent {
                            passenger_id,
                            trip_id: req.trip_id,
                            fare_estimate: fare,
                            status: "pending".to_string(),
                        })),
                    }),
                    Priority::Critical,
                ),
                Err(e) => connections.send(
                    user_id,
                    err_event("JOIN_FAILED", &e.to_string()),
                    Priority::Normal,
                ),
            }
        }

        // ── Driver: terima penumpang ──────────────────────────────────────────
        Cp::AcceptPassenger(req) => {
            if role != "driver" {
                connections.send(
                    user_id,
                    err_event("FORBIDDEN", "Hanya driver"),
                    Priority::Normal,
                );
                return;
            }
            if let Err(e) = rideshare_svc
                .accept_passenger(user_id, &req.trip_id, &req.passenger_id)
                .await
            {
                connections.send(
                    user_id,
                    err_event("ACCEPT_PASSENGER_FAILED", &e.to_string()),
                    Priority::Normal,
                );
            }
        }

        // ── Driver: tolak penumpang ───────────────────────────────────────────
        Cp::RejectPassenger(req) => {
            if role != "driver" {
                connections.send(
                    user_id,
                    err_event("FORBIDDEN", "Hanya driver"),
                    Priority::Normal,
                );
                return;
            }
            let reason = (!req.reason.is_empty()).then_some(req.reason.as_str());
            if let Err(e) = rideshare_svc
                .reject_passenger(user_id, &req.trip_id, &req.passenger_id, reason)
                .await
            {
                connections.send(
                    user_id,
                    err_event("REJECT_PASSENGER_FAILED", &e.to_string()),
                    Priority::Normal,
                );
            }
        }

        // ── Driver: jemput penumpang ──────────────────────────────────────────
        Cp::PickupPassenger(req) => {
            if role != "driver" {
                connections.send(
                    user_id,
                    err_event("FORBIDDEN", "Hanya driver"),
                    Priority::Normal,
                );
                return;
            }
            if let Err(e) = rideshare_svc
                .pickup_passenger(user_id, &req.trip_id, &req.passenger_id)
                .await
            {
                connections.send(
                    user_id,
                    err_event("PICKUP_PASSENGER_FAILED", &e.to_string()),
                    Priority::Normal,
                );
            }
        }

        // ── Driver: antar penumpang ke tujuan ─────────────────────────────────
        Cp::DropPassenger(req) => {
            if role != "driver" {
                connections.send(
                    user_id,
                    err_event("FORBIDDEN", "Hanya driver"),
                    Priority::Normal,
                );
                return;
            }
            let dist = if req.distance_km > 0.0 {
                req.distance_km as f64
            } else {
                1.0
            };
            if let Err(e) = rideshare_svc
                .drop_passenger(user_id, &req.trip_id, &req.passenger_id, dist)
                .await
            {
                connections.send(
                    user_id,
                    err_event("DROP_PASSENGER_FAILED", &e.to_string()),
                    Priority::Normal,
                );
            }
        }

        // ── Rider: cancel nebeng ──────────────────────────────────────────────
        Cp::CancelRideshare(req) => {
            let reason = (!req.reason.is_empty()).then_some(req.reason.as_str());
            if let Err(e) = rideshare_svc
                .cancel_passenger(user_id, &req.trip_id, &req.passenger_id, reason)
                .await
            {
                connections.send(
                    user_id,
                    err_event("CANCEL_RS_FAILED", &e.to_string()),
                    Priority::Normal,
                );
            }
        }

        // ── Ambil notifikasi ──────────────────────────────────────────────────
        Cp::GetNotifications(req) => {
            let limit = if req.limit > 0 { req.limit } else { 20 };
            let before = (req.before > 0).then_some(req.before);

            match notif_svc
                .list(user_id, limit, before, req.unread_only)
                .await
            {
                Ok(event) => connections.send(
                    user_id,
                    Arc::new(ServerEvent {
                        payload: Some(Sp::Notifications(event)),
                    }),
                    Priority::Normal,
                ),
                Err(e) => connections.send(
                    user_id,
                    err_event("NOTIF_FAILED", &e.to_string()),
                    Priority::Normal,
                ),
            }
        }

        // ── Tandai notif sudah dibaca ─────────────────────────────────────────
        Cp::MarkNotifRead(req) => {
            if let Err(e) = notif_svc.mark_read(req.notif_id, user_id).await {
                tracing::error!(error = %e, "mark_notif_read failed");
            }
        }

        // ── Tandai semua notif sudah dibaca ──────────────────────────────────
        Cp::MarkAllNotifRead(_) => {
            if let Err(e) = notif_svc.mark_all_read(user_id).await {
                tracing::error!(error = %e, "mark_all_notif_read failed");
            }
        }

        // ── WebRTC: call offer ────────────────────────────────────────────────
        Cp::CallOffer(c) => {
            let order = match order_svc.order_repo.find_by_id(&c.order_id).await {
                Ok(Some(o)) => o,
                Ok(None) => {
                    connections.send(
                        user_id,
                        err_event("ORDER_NOT_FOUND", "Order tidak ditemukan"),
                        Priority::Normal,
                    );
                    return;
                }
                Err(e) => {
                    connections.send(
                        user_id,
                        err_event("DB_ERROR", &e.to_string()),
                        Priority::Normal,
                    );
                    return;
                }
            };

            let peer_id = match get_peer_id(role, &order, user_id) {
                Ok(p) => p,
                Err(e) => {
                    connections.send(
                        user_id,
                        err_event("INVALID_ORDER", &e.to_string()),
                        Priority::Normal,
                    );
                    return;
                }
            };

            if c.target_user_id != peer_id {
                tracing::warn!(
                    caller = %user_id,
                    target = %c.target_user_id,
                    expected = %peer_id,
                    "Call target mismatch"
                );
                connections.send(
                    user_id,
                    err_event("INVALID_TARGET", "Tidak bisa call person ini"),
                    Priority::Normal,
                );
                return;
            }

            if !is_order_active(&order.status) {
                connections.send(
                    user_id,
                    err_event(
                        "INACTIVE_ORDER",
                        "Call hanya tersedia saat order berlangsung",
                    ),
                    Priority::Normal,
                );
                return;
            }

            tracing::info!(caller = %user_id, callee = %peer_id, order_id = %c.order_id, "Call offer initiated");

            connections.send(
                &peer_id,
                Arc::new(ServerEvent {
                    payload: Some(Sp::IncomingCallOffer(IncomingCallOffer {
                        caller_id: user_id.to_string(),
                        sdp: c.sdp,
                    })),
                }),
                Priority::Critical,
            );
        }

        // ── WebRTC: call answer ───────────────────────────────────────────────
        Cp::CallAnswer(a) => {
            connections.send(
                &a.caller_id,
                Arc::new(ServerEvent {
                    payload: Some(Sp::IncomingCallAnswer(IncomingCallAnswer {
                        callee_id: user_id.to_string(),
                        sdp: a.sdp,
                    })),
                }),
                Priority::Critical,
            );
        }

        // ── WebRTC: ICE candidate ─────────────────────────────────────────────
        Cp::IceCandidate(i) => {
            connections.send(
                &i.peer_id,
                Arc::new(ServerEvent {
                    payload: Some(Sp::IceCandidateEvent(IceCandidateEvent {
                        peer_id: user_id.to_string(),
                        candidate: i.candidate,
                    })),
                }),
                Priority::Critical,
            );
        }

        // ── WebRTC: end call ──────────────────────────────────────────────────
        Cp::EndCall(e) => {
            connections.send(
                &e.peer_id,
                Arc::new(ServerEvent {
                    payload: Some(Sp::CallEnded(CallEndedEvent {
                        peer_id: user_id.to_string(),
                    })),
                }),
                Priority::Critical,
            );
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn get_peer_id(role: &str, order: &ordermodels, user_id: &str) -> Result<String> {
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

fn is_order_active(status: &str) -> bool {
    !matches!(status, "completed" | "cancelled" | "searching")
}
