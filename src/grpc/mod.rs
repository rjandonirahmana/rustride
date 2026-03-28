pub mod auth;
pub mod driver;
pub mod location;
pub mod message;
pub mod notification;
pub mod order;
pub mod rideshare;
pub mod trip;
pub mod webrtc;

use crate::{
    proto::ridehailing::client_event::Payload as Cp,
    repository::{
        notification::NotificationRepositoryTrait, order::OrderRepository,
        rideshare::RideshareRepositoryTrait, user::UserRepository,
    },
};

use super::context::StreamContext;

/// Entry point dispatch — routing ke domain handler masing-masing.
pub async fn dispatch<OR, UR, RR, NR>(payload: Cp, ctx: &StreamContext<OR, UR, RR, NR>)
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    match payload {
        Cp::Location(loc) => location::handle_location_update(loc, ctx).await,
        Cp::WatchDrivers(req) => location::handle_watch_drivers(req, ctx).await,

        Cp::BrowseOrders(b) => order::handle_browse_orders(b, ctx).await,
        Cp::AcceptOrder(a) => order::handle_accept_order(a, ctx).await,
        Cp::RejectOrder(r) => order::handle_reject_order(r, ctx).await,
        Cp::Arrived(a) => order::handle_arrived(a, ctx).await,
        Cp::StartTrip(s) => order::handle_start_trip(s, ctx).await,
        Cp::CompleteTrip(c) => order::handle_complete_trip(c, ctx).await,
        Cp::CancelTrip(c) => order::handle_cancel_trip(c, ctx).await,
        Cp::EstimateFare(req) => order::handle_estimate_fare(req, ctx).await,
        Cp::Rate(req) => order::handle_rate(req, ctx).await,
        Cp::Ping(_) => order::handle_ping(ctx).await,
        Cp::WatchUser(w) => order::handle_watch_user(w, ctx).await,

        Cp::OpenRideshare(req) => rideshare::handle_open_rideshare(req, ctx).await,
        Cp::BrowseRideshare(req) => rideshare::handle_browse_rideshare(req, ctx).await,
        Cp::JoinRideshare(req) => rideshare::handle_join_rideshare(req, ctx).await,
        Cp::AcceptPassenger(req) => rideshare::handle_accept_passenger(req, ctx).await,
        Cp::RejectPassenger(req) => rideshare::handle_reject_passenger(req, ctx).await,
        Cp::PickupPassenger(req) => rideshare::handle_pickup_passenger(req, ctx).await,
        Cp::DropPassenger(req) => rideshare::handle_drop_passenger(req, ctx).await,
        Cp::CancelRideshare(req) => rideshare::handle_cancel_rideshare(req, ctx).await,

        Cp::CallOffer(c) => webrtc::handle_call_offer(c, ctx).await,
        Cp::CallAnswer(a) => webrtc::handle_call_answer(a, ctx).await,
        Cp::IceCandidate(i) => webrtc::handle_ice_candidate(i, ctx).await,
        Cp::EndCall(e) => webrtc::handle_end_call(e, ctx).await,
    }
}
