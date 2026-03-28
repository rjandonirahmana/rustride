// ===== handler/webrtc.rs =====
use std::sync::Arc;

use crate::{
    connections::Priority,
    proto::ridehailing::{
        server_event::Payload as Sp, CallAnswer, CallEndedEvent, CallOffer, EndCallReq,
        IceCandidate, IceCandidateEvent, IncomingCallAnswer, IncomingCallOffer, ServerEvent,
    },
    repository::{
        notification::NotificationRepositoryTrait, order::OrderRepository,
        rideshare::RideshareRepositoryTrait, user::UserRepository,
    },
};

use super::{
    super::context::StreamContext,
    order::{is_order_active, resolve_peer},
};

pub async fn handle_call_offer<OR, UR, RR, NR>(c: CallOffer, ctx: &StreamContext<OR, UR, RR, NR>)
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    let (order, peer_id) = match resolve_peer(&c.order_id, ctx).await {
        Some(v) => v,
        None => return,
    };

    if c.target_user_id != peer_id {
        ctx.send_err("INVALID_TARGET", "Tidak bisa call person ini");
        return;
    }

    if !is_order_active(&order.status) {
        ctx.send_err(
            "INACTIVE_ORDER",
            "Call hanya tersedia saat order berlangsung",
        );
        return;
    }

    tracing::info!(
        caller = %ctx.user_id,
        callee = %peer_id,
        order_id = %c.order_id,
        "WebRTC call offer"
    );

    ctx.connections.send(
        &peer_id,
        Arc::new(ServerEvent {
            payload: Some(Sp::IncomingCall(IncomingCallOffer {
                caller_id: ctx.user_id.clone(),
                sdp: c.sdp,
            })),
        }),
        Priority::Critical,
    );
}

pub async fn handle_call_answer<OR, UR, RR, NR>(a: CallAnswer, ctx: &StreamContext<OR, UR, RR, NR>)
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    ctx.connections.send(
        &a.caller_id,
        Arc::new(ServerEvent {
            payload: Some(Sp::CallAnswer(IncomingCallAnswer {
                callee_id: ctx.user_id.clone(),
                sdp: a.sdp,
            })),
        }),
        Priority::Critical,
    );
}

pub async fn handle_ice_candidate<OR, UR, RR, NR>(
    i: IceCandidate,
    ctx: &StreamContext<OR, UR, RR, NR>,
) where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    ctx.connections.send(
        &i.peer_id,
        Arc::new(ServerEvent {
            payload: Some(Sp::IceCandidate(IceCandidateEvent {
                peer_id: ctx.user_id.clone(),
                candidate: i.candidate,
            })),
        }),
        Priority::Critical,
    );
}

pub async fn handle_end_call<OR, UR, RR, NR>(e: EndCallReq, ctx: &StreamContext<OR, UR, RR, NR>)
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    ctx.connections.send(
        &e.peer_id,
        Arc::new(ServerEvent {
            payload: Some(Sp::CallEnded(CallEndedEvent {
                peer_id: ctx.user_id.clone(),
            })),
        }),
        Priority::Critical,
    );
}
