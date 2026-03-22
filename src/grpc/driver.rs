use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::{
    auth::JwtService,
    grpc::trip::extract_token,
    proto::driver::{
        driver_service_server::DriverService as DriverServiceGrpc,
        DriverOrderDetail as ProtoDetail, DriverOrderItem as ProtoItem, GetOrderDetailReq,
        GetOrderHistoryReq, GetTodayOrdersReq, GetTodaySummaryReq, OrderHistoryRes, TodayOrdersRes,
        TodaySummaryRes,
    },
    repository::driver::DriverRepository,
    service::driver::DriverService,
};

// ── gRPC handler ──────────────────────────────────────────────────────────────

pub struct DriverServiceImpl<DR: DriverRepository> {
    pub driver_svc: Arc<DriverService<DR>>,
    pub jwt: JwtService,
}

// ── Auth helper ───────────────────────────────────────────────────────────────

fn verify_driver<DR: DriverRepository>(
    svc: &DriverServiceImpl<DR>,
    meta: &tonic::metadata::MetadataMap,
) -> Result<crate::auth::Claims, Status> {
    let token = extract_token(meta)
        .ok_or_else(|| Status::unauthenticated("Missing Authorization header"))?;
    let claims = svc
        .jwt
        .verify(token)
        .map_err(|e| Status::unauthenticated(format!("Invalid token: {e}")))?;
    if claims.role != "driver" {
        return Err(Status::permission_denied("role harus 'driver'"));
    }
    Ok(claims)
}

// ── Proto converters ──────────────────────────────────────────────────────────

fn item_to_proto(i: &crate::repository::driver::DriverOrderItem) -> ProtoItem {
    ProtoItem {
        order_id: i.order_id.clone(),
        status: i.status.clone(),
        rider_name: i.rider_name.clone(),
        pickup_address: i.pickup_address.clone(),
        dest_address: i.dest_address.clone(),
        distance_km: i.distance_km,
        duration_min: i.duration_min,
        fare: i.fare,
        driver_earning: i.driver_earning,
        tip: i.tip,
        service_type: i.service_type.clone(),
        started_at: i.started_at.clone(),
        completed_at: i.completed_at.clone(),
        cancel_reason: i.cancel_reason.clone(),
        rating: i.rating,
        rating_comment: i.rating_comment.clone(),
    }
}

fn summary_to_proto(s: &crate::repository::driver::DailySummary) -> TodaySummaryRes {
    TodaySummaryRes {
        total_orders: s.total_orders,
        cancelled_orders: s.cancelled_orders,
        gross_earnings: s.gross_earnings,
        net_earnings: s.net_earnings,
        tips: s.tips,
        online_minutes: s.online_minutes as f32,
        distance_km: s.distance_km,
        avg_rating: s.avg_rating,
        peak_hour: s.peak_hour.clone(),
    }
}

// ── Implementasi trait ────────────────────────────────────────────────────────

#[tonic::async_trait]
impl<DR: DriverRepository + 'static> DriverServiceGrpc for DriverServiceImpl<DR> {
    // ── GetTodayOrders ────────────────────────────────────────────────────────
    async fn get_today_orders(
        &self,
        req: Request<GetTodayOrdersReq>,
    ) -> Result<Response<TodayOrdersRes>, Status> {
        let claims = verify_driver(self, req.metadata())?;

        let result = self
            .driver_svc
            .get_today_orders(&claims.sub)
            .await
            .map_err(|e| Status::internal(format!("TODAY_ORDERS_FAILED: {e}")))?;

        Ok(Response::new(TodayOrdersRes {
            orders: result.orders.iter().map(item_to_proto).collect(),
            summary: Some(summary_to_proto(&result.summary)),
        }))
    }

    // ── GetTodaySummary ───────────────────────────────────────────────────────
    async fn get_today_summary(
        &self,
        req: Request<GetTodaySummaryReq>,
    ) -> Result<Response<TodaySummaryRes>, Status> {
        let claims = verify_driver(self, req.metadata())?;

        let summary = self
            .driver_svc
            .get_today_summary(&claims.sub)
            .await
            .map_err(|e| Status::internal(format!("TODAY_SUMMARY_FAILED: {e}")))?;

        Ok(Response::new(summary_to_proto(&summary)))
    }

    // ── GetOrderHistory ───────────────────────────────────────────────────────
    async fn get_order_history(
        &self,
        req: Request<GetOrderHistoryReq>,
    ) -> Result<Response<OrderHistoryRes>, Status> {
        let claims = verify_driver(self, req.metadata())?;
        let inner = req.into_inner();

        let page = if inner.page > 0 { inner.page as u32 } else { 1 };
        let limit = if inner.limit > 0 {
            inner.limit as u32
        } else {
            20
        };

        let result = self
            .driver_svc
            .get_order_history(
                &claims.sub,
                (!inner.date_from.is_empty()).then_some(inner.date_from),
                (!inner.date_to.is_empty()).then_some(inner.date_to),
                (!inner.status.is_empty()).then_some(inner.status),
                page,
                limit,
            )
            .await
            .map_err(|e| Status::internal(format!("HISTORY_FAILED: {e}")))?;

        Ok(Response::new(OrderHistoryRes {
            orders: result.orders.iter().map(item_to_proto).collect(),
            has_more: result.has_more,
            total: result.total as i32,
        }))
    }

    // ── GetOrderDetail ────────────────────────────────────────────────────────
    async fn get_order_detail(
        &self,
        req: Request<GetOrderDetailReq>,
    ) -> Result<Response<ProtoDetail>, Status> {
        let claims = verify_driver(self, req.metadata())?;
        let inner = req.into_inner();

        if inner.order_id.is_empty() {
            return Err(Status::invalid_argument("order_id tidak boleh kosong"));
        }

        let detail = self
            .driver_svc
            .get_order_detail(&claims.sub, &inner.order_id)
            .await
            .map_err(|e| {
                if e.to_string().contains("tidak ditemukan") {
                    Status::not_found(e.to_string())
                } else {
                    Status::internal(format!("DETAIL_FAILED: {e}"))
                }
            })?;

        Ok(Response::new(ProtoDetail {
            order: Some(item_to_proto(&detail.item)),
            pickup_lat: detail.pickup_lat,
            pickup_lng: detail.pickup_lng,
            dest_lat: detail.dest_lat,
            dest_lng: detail.dest_lng,
            rider_phone: detail.rider_phone,
            rider_avatar: detail.rider_avatar,
            rider_rating: detail.rider_rating,
            route: vec![], // route points opsional, isi kalau ada tabel terpisah
        }))
    }
}
