// ===== handler/location.rs =====
use std::sync::Arc;

use crate::{
    connections::Priority,
    proto::ridehailing::{
        server_event::Payload as Sp, DriverLocationEvent, LocationUpdate, NearbyDriverItem,
        NearbyDriversEvent, ServerEvent, WatchDriversReq,
    },
    repository::{
        notification::NotificationRepositoryTrait, order::OrderRepository,
        rideshare::RideshareRepositoryTrait, user::UserRepository,
    },
};

use super::super::{
    context::StreamContext,
    throttle::{THROTTLE_LOCATION, THROTTLE_WATCH_DRIVERS},
};

pub async fn handle_location_update<OR, UR, RR, NR>(
    loc: LocationUpdate,
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

    // Throttle: heartbeat tidak perlu lebih dari 1x/detik
    if !ctx
        .throttle
        .allow(&ctx.user_id, "location", THROTTLE_LOCATION)
    {
        return; // Silently drop — client tidak perlu tahu
    }

    if let Err(e) = ctx
        .location
        .update_driver_location(
            &ctx.user_id,
            loc.lat,
            loc.lng,
            (loc.heading != 0.0).then_some(loc.heading as f32),
            (loc.speed != 0.0).then_some(loc.speed as f32),
            &ctx.vehicle_type,
        )
        .await
    {
        tracing::error!(driver = %ctx.user_id, error = %e, "update_driver_location GAGAL");
        ctx.send_err("LOC_UPDATE_FAILED", &e.to_string());
        return;
    }

    tracing::info!(
        driver = %ctx.user_id,
        vehicle_type = %ctx.vehicle_type,
        "Driver berhasil masuk geo:ready di Redis"
    );

    // Forward lokasi ke rider jika sedang dalam order aktif
    match ctx
        .order_svc
        .get_active_order_for_driver(&ctx.user_id)
        .await
    {
        Ok(Some(order)) => {
            tracing::info!("kirim driver loc ke {:?}", &order.rider_id);
            ctx.connections.send(
                &order.rider_id,
                Arc::new(ServerEvent {
                    payload: Some(Sp::DriverLoc(DriverLocationEvent {
                        lat: loc.lat,
                        lng: loc.lng,
                        heading: loc.heading,
                        order_id: order.id,
                    })),
                }),
                Priority::Normal,
            );
        }
        Ok(None) => {}
        Err(e) => {
            // Lokasi sudah tersimpan, error ini hanya gagal forward ke rider
            tracing::warn!(
                driver = %ctx.user_id,
                error = %e,
                "Failed to fetch active order for location forward"
            );
        }
    }
}

pub async fn handle_watch_drivers<OR, UR, RR, NR>(
    req: WatchDriversReq,
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

    if !ctx
        .throttle
        .allow(&ctx.user_id, "watch_drivers", THROTTLE_WATCH_DRIVERS)
    {
        return;
    }

    match ctx
        .location
        .find_nearby_drivers(req.lat, req.lng, &req.service_type)
        .await
    {
        Ok(drivers) => {
            let mut items = Vec::with_capacity(drivers.len());
            for (driver_id, dist_m) in &drivers {
                // get_driver_location gagal untuk satu driver tidak boleh
                // membatalkan seluruh response — skip saja yang error
                match ctx.location.get_driver_location(driver_id).await {
                    Ok(Some(loc)) => items.push(NearbyDriverItem {
                        driver_id: driver_id.clone(),
                        lat: loc.lat,
                        lng: loc.lng,
                        heading: loc.heading.unwrap_or(0.0),
                        distance_m: *dist_m as f32,
                    }),
                    Ok(None) => {} // Driver baru offline — skip
                    Err(e) => {
                        tracing::warn!(driver_id, error = %e, "Failed to get driver location detail");
                    }
                }
            }
            ctx.send(
                Sp::NearbyDrivers(NearbyDriversEvent { drivers: items }),
                Priority::Critical,
            );
        }
        Err(e) => ctx.send_err("FIND_FAILED", &e.to_string()),
    }
}
