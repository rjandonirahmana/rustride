// ===== handler/rideshare.rs =====
use crate::{
    connections::Priority,
    proto::ridehailing::{
        server_event::Payload as Sp, AcceptPassengerReq, BrowseRideshareReq, CancelRideshareReq,
        DropPassengerReq, JoinRideshareReq, NearbyRideshareEvent, OpenRideshareReq,
        PickupPassengerReq, RejectPassengerReq, RideshareJoinedEvent, RideshareOpenedEvent,
    },
    repository::{
        notification::NotificationRepositoryTrait, order::OrderRepository,
        rideshare::RideshareRepositoryTrait, user::UserRepository,
    },
};

use super::super::context::StreamContext;

pub async fn handle_open_rideshare<OR, UR, RR, NR>(
    req: OpenRideshareReq,
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

    let order = match ctx
        .order_svc
        .order_repo
        .find_active_for_driver(&ctx.user_id)
        .await
    {
        Ok(Some(o)) => o,
        Ok(None) => return ctx.send_err("NO_ORDER", "Tidak ada order aktif"),
        Err(e) => return ctx.send_err("ORDER_FAILED", &e.to_string()),
    };

    let deadline = (req.join_deadline_s > 0).then_some(req.join_deadline_s as i64);
    let max = if req.max_passengers > 0 {
        req.max_passengers
    } else {
        3
    };

    match ctx
        .rideshare_svc
        .open_trip(
            &ctx.user_id,
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
        Ok(trip_id) => ctx.send(
            Sp::RideshareOpened(RideshareOpenedEvent {
                trip_id,
                order_id: req.order_id,
                max_passengers: max,
            }),
            Priority::Critical,
        ),
        Err(e) => ctx.send_err("OPEN_FAILED", &e.to_string()),
    }
}

pub async fn handle_browse_rideshare<OR, UR, RR, NR>(
    req: BrowseRideshareReq,
    ctx: &StreamContext<OR, UR, RR, NR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    if !ctx.require_role("rider") {
        return;
    }

    let radius = if req.radius_km > 0.0 {
        req.radius_km as f64
    } else {
        5.0
    };

    match ctx
        .rideshare_svc
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
        Ok(trips) => ctx.send(
            Sp::NearbyRideshare(NearbyRideshareEvent { trips }),
            Priority::Normal,
        ),
        Err(e) => ctx.send_err("BROWSE_RS_FAILED", &e.to_string()),
    }
}

pub async fn handle_join_rideshare<OR, UR, RR, NR>(
    req: JoinRideshareReq,
    ctx: &StreamContext<OR, UR, RR, NR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    if !ctx.require_role("rider") {
        return;
    }

    let avatar = ctx
        .order_svc
        .user_repo
        .find_by_id(&ctx.user_id)
        .await
        .ok()
        .flatten()
        .and_then(|u| u.avatar_url)
        .unwrap_or_default();

    match ctx
        .rideshare_svc
        .join_trip(
            &ctx.user_id,
            &ctx.username,
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
        Ok((passenger_id, fare)) => ctx.send(
            Sp::RideshareJoined(RideshareJoinedEvent {
                passenger_id,
                trip_id: req.trip_id,
                fare_estimate: fare,
                status: "pending".into(),
            }),
            Priority::Critical,
        ),
        Err(e) => ctx.send_err("JOIN_FAILED", &e.to_string()),
    }
}

pub async fn handle_accept_passenger<OR, UR, RR, NR>(
    req: AcceptPassengerReq,
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
        .rideshare_svc
        .accept_passenger(&ctx.user_id, &req.trip_id, &req.passenger_id)
        .await
    {
        ctx.send_err("ACCEPT_PASSENGER_FAILED", &e.to_string());
    }
}

pub async fn handle_reject_passenger<OR, UR, RR, NR>(
    req: RejectPassengerReq,
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
    let reason = (!req.reason.is_empty()).then_some(req.reason.as_str());
    if let Err(e) = ctx
        .rideshare_svc
        .reject_passenger(&ctx.user_id, &req.trip_id, &req.passenger_id, reason)
        .await
    {
        ctx.send_err("REJECT_PASSENGER_FAILED", &e.to_string());
    }
}

pub async fn handle_pickup_passenger<OR, UR, RR, NR>(
    req: PickupPassengerReq,
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
        .rideshare_svc
        .pickup_passenger(&ctx.user_id, &req.trip_id, &req.passenger_id)
        .await
    {
        ctx.send_err("PICKUP_PASSENGER_FAILED", &e.to_string());
    }
}

pub async fn handle_drop_passenger<OR, UR, RR, NR>(
    req: DropPassengerReq,
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
    let dist = if req.distance_km > 0.0 {
        req.distance_km as f64
    } else {
        1.0
    };
    if let Err(e) = ctx
        .rideshare_svc
        .drop_passenger(&ctx.user_id, &req.trip_id, &req.passenger_id, dist)
        .await
    {
        ctx.send_err("DROP_PASSENGER_FAILED", &e.to_string());
    }
}

pub async fn handle_cancel_rideshare<OR, UR, RR, NR>(
    req: CancelRideshareReq,
    ctx: &StreamContext<OR, UR, RR, NR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    let reason = (!req.reason.is_empty()).then_some(req.reason.as_str());
    if let Err(e) = ctx
        .rideshare_svc
        .cancel_passenger(&ctx.user_id, &req.trip_id, &req.passenger_id, reason)
        .await
    {
        ctx.send_err("CANCEL_RS_FAILED", &e.to_string());
    }
}
