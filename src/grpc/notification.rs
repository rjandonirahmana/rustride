use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::{
    auth::JwtService,
    grpc::trip::extract_token,
    proto::notification::{
        notification_service_server::NotificationService as NotificationServiceGrpc,
        GetNotificationsReq, MarkNotifReadReq, NotificationsEvent,
    },
    repository::notification::NotificationRepositorytrait,
    service::notification::NotificationService,
};

// google.protobuf.Empty → pakai () di tonic
type Empty = ();

pub struct NotificationServiceImpl<NR: NotificationRepositorytrait> {
    pub notif_svc: Arc<NotificationService<NR>>,
    pub jwt: JwtService,
}

fn verify(jwt: &JwtService, meta: &tonic::metadata::MetadataMap) -> Result<String, Status> {
    let token = extract_token(meta)
        .ok_or_else(|| Status::unauthenticated("Missing Authorization header"))?;
    let claims = jwt
        .verify(token)
        .map_err(|e| Status::unauthenticated(format!("Invalid token: {e}")))?;
    Ok(claims.sub)
}

#[tonic::async_trait]
impl<NR: NotificationRepositorytrait + 'static> NotificationServiceGrpc
    for NotificationServiceImpl<NR>
{
    // ── GetNotifications ──────────────────────────────────────────────────────
    async fn get_notifications(
        &self,
        req: Request<GetNotificationsReq>,
    ) -> Result<Response<NotificationsEvent>, Status> {
        let user_id = verify(&self.jwt, req.metadata())?;
        let r = req.into_inner();

        let limit = if r.limit > 0 { r.limit } else { 20 };
        let before = (r.before > 0).then_some(r.before);

        let event = self
            .notif_svc
            .list(&user_id, limit, before, r.unread_only)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(event))
    }

    // ── MarkNotifRead ─────────────────────────────────────────────────────────
    async fn mark_notif_read(
        &self,
        req: Request<MarkNotifReadReq>,
    ) -> Result<Response<Empty>, Status> {
        let user_id = verify(&self.jwt, req.metadata())?;
        let r = req.into_inner();

        self.notif_svc
            .mark_read(r.notif_id, &user_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(()))
    }

    // ── MarkAllNotifRead ──────────────────────────────────────────────────────
    async fn mark_all_notif_read(&self, req: Request<Empty>) -> Result<Response<Empty>, Status> {
        let user_id = verify(&self.jwt, req.metadata())?;

        self.notif_svc
            .mark_all_read(&user_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(()))
    }
}
