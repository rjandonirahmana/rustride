use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::{
    auth::JwtService,
    connections::ConnectionManager,
    grpc::trip::extract_token,
    proto::message::{
        message_service_server::MessageService, ConversationItems, Empty, GetHistoryReq,
        GetUploadUrlReq, GetUploadUrlRes, HistoryEvent, MarkReadReq, MessageAckEvent,
        NewMessageEvent, SendMediaReq, SendMediaResponse, SendMessageReq,
    },
    repository::order::OrderRepository,
    service::chat::ChatService,
};

pub struct MessageServiceImpl<OR: OrderRepository> {
    pub chat_svc: Arc<ChatService>,
    pub jwt: JwtService,
    pub connections: Arc<ConnectionManager>,
    pub order_repo: Arc<OR>,
}

fn unauth(msg: &str) -> Status {
    Status::unauthenticated(msg)
}

fn internal(msg: &str) -> Status {
    Status::internal(msg)
}

fn extract_user(
    jwt: &JwtService,
    req_meta: &tonic::metadata::MetadataMap,
) -> Result<(String, String), Status> {
    let token = extract_token(req_meta).ok_or_else(|| unauth("Missing Authorization header"))?;
    let claims = jwt
        .verify(token)
        .map_err(|e| unauth(&format!("Invalid token: {e}")))?;
    Ok((claims.sub, claims.role))
}

#[tonic::async_trait]
impl<OR: OrderRepository + 'static> MessageService for MessageServiceImpl<OR> {
    async fn send_message(
        &self,
        req: Request<SendMessageReq>,
    ) -> Result<Response<MessageAckEvent>, Status> {
        let (user_id, _role) = extract_user(&self.jwt, req.metadata())?;
        let m = req.into_inner();

        if m.content.trim().is_empty() {
            return Err(Status::invalid_argument("Pesan tidak boleh kosong"));
        }

        let order = self
            .order_repo
            .find_by_id(&m.order_id)
            .await
            .map_err(|e| internal(&e.to_string()))?
            .ok_or_else(|| Status::not_found("Order tidak ditemukan"))?;

        // validasi peer
        let peer_id = get_peer_id_from_order(&user_id, &order)
            .map_err(|e| Status::permission_denied(&e.to_string()))?;

        if m.recipient_id != peer_id {
            return Err(Status::permission_denied("Recipient mismatch"));
        }

        if !is_order_active(&order.status) {
            return Err(Status::failed_precondition("Order tidak aktif"));
        }

        // ambil username dari order / fallback ke user_id
        let username = order.rider_name.clone(); // atau fetch dari user_repo kalau perlu

        let msg = self
            .chat_svc
            .send_text(&user_id, &peer_id, &m.content, &username, &m.order_id)
            .await
            .map_err(|e| internal(&e.to_string()))?;

        Ok(Response::new(MessageAckEvent {
            msg_id: msg.id,
            status: "sent".into(),
            sent_at: msg.sent_at.to_rfc3339(),
        }))
    }

    async fn send_media(
        &self,
        req: Request<SendMediaReq>,
    ) -> Result<Response<SendMediaResponse>, Status> {
        let (user_id, _role) = extract_user(&self.jwt, req.metadata())?;
        let m = req.into_inner();

        if m.media_url.is_empty() {
            return Err(Status::invalid_argument("media_url kosong"));
        }

        if !m.media_url.contains("r2.dev") {
            return Err(Status::invalid_argument("URL tidak valid"));
        }

        if !m.media_mime.starts_with("image/")
            && !m.media_mime.starts_with("video/")
            && !m.media_mime.starts_with("audio/")
        {
            return Err(Status::invalid_argument("Tipe file tidak didukung"));
        }

        let order = self
            .order_repo
            .find_by_id(&m.order_id)
            .await
            .map_err(|e| internal(&e.to_string()))?
            .ok_or_else(|| Status::not_found("Order tidak ditemukan"))?;

        let peer_id = get_peer_id_from_order(&user_id, &order)
            .map_err(|e| Status::permission_denied(&e.to_string()))?;

        if m.recipient_id != peer_id {
            return Err(Status::permission_denied("Recipient mismatch"));
        }

        if !is_order_active(&order.status) {
            return Err(Status::failed_precondition("Order tidak aktif"));
        }

        let username = order.rider_name.clone();

        let result = self
            .chat_svc
            .send_media(
                &user_id,
                &peer_id,
                &m.media_url,
                &m.media_mime,
                m.media_size,
                &username,
                &m.order_id,
            )
            .await
            .map_err(|e| internal(&e.to_string()))?;

        Ok(Response::new(SendMediaResponse {
            client_msg_id: m.client_msg_id,
            msg_id: result.id,
        }))
    }

    async fn mark_read(&self, req: Request<MarkReadReq>) -> Result<Response<Empty>, Status> {
        let (user_id, _) = extract_user(&self.jwt, req.metadata())?;
        let r = req.into_inner();

        self.chat_svc
            .mark_read(&user_id, &r.from_user_id)
            .await
            .map_err(|e| internal(&e.to_string()))?;

        Ok(Response::new(Empty {}))
    }

    async fn get_history(
        &self,
        req: Request<GetHistoryReq>,
    ) -> Result<Response<HistoryEvent>, Status> {
        let (user_id, _) = extract_user(&self.jwt, req.metadata())?;
        let h = req.into_inner();

        let before = (!h.before.is_empty()).then_some(h.before.as_str());
        let msgs = self
            .chat_svc
            .get_history(&user_id, &h.peer_id, h.limit, before)
            .await
            .map_err(|e| internal(&e.to_string()))?;

        let has_more = msgs.len() as i32 == h.limit;
        let items = msgs
            .into_iter()
            .map(|m| NewMessageEvent {
                msg_id: m.id,
                sender_id: m.sender_id,
                sender_name: m.sender_name.unwrap_or_default(),
                recipient_id: m.recipient_id,
                content: m.content,
                msg_type: m.msg_type,
                sent_at: m.sent_at.to_rfc3339(),
                media_url: m.media_url.unwrap_or_default(),
                media_mime: m.media_mime.unwrap_or_default(),
                media_size: m.media_size.unwrap_or(0),
                media_duration: m.media_duration.unwrap_or(0),
                media_thumb: m.media_thumb.unwrap_or_default(),
                sender_avatar: m.sender_avatar.unwrap_or_default(),
                status_order: m.status_order.unwrap_or_default(),
            })
            .collect();

        Ok(Response::new(HistoryEvent {
            messages: items,
            has_more,
        }))
    }

    async fn get_conversations(
        &self,
        req: Request<Empty>,
    ) -> Result<Response<ConversationItems>, Status> {
        let (user_id, _) = extract_user(&self.jwt, req.metadata())?;

        let items = self
            .chat_svc
            .get_conversations(&user_id)
            .await
            .map_err(|e| internal(&e.to_string()))?;

        Ok(Response::new(ConversationItems { items }))
    }

    async fn get_upload_url(
        &self,
        req: Request<GetUploadUrlReq>,
    ) -> Result<Response<GetUploadUrlRes>, Status> {
        let (user_id, _) = extract_user(&self.jwt, req.metadata())?;
        let r = req.into_inner();

        let (upload_url, public_url) = self
            .chat_svc
            .get_upload_url(&user_id, &r.mime)
            .await
            .map_err(|e| internal(&e.to_string()))?;

        Ok(Response::new(GetUploadUrlRes {
            upload_url,
            public_url,
        }))
    }
}

// helper lokal — sama logikanya dengan trip_service tapi tanpa username context
fn get_peer_id_from_order(
    user_id: &str,
    order: &crate::models::order::Order,
) -> anyhow::Result<String> {
    if order.rider_id == user_id {
        order
            .driver_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Driver belum assigned"))
    } else if order.driver_id.as_deref() == Some(user_id) {
        Ok(order.rider_id.clone())
    } else {
        Err(anyhow::anyhow!("User bukan rider/driver dari order ini"))
    }
}

fn is_order_active(status: &str) -> bool {
    !matches!(status, "completed" | "cancelled" | "searching")
}
