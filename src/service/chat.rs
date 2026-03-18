use anyhow::Result;
use aws_sdk_s3::presigning::PresigningConfig;
use prost::bytes;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

use crate::{
    connections::{ConnectionManager, Priority},
    models::message::Message,
    proto::ridehailing::{
        server_event::Payload as Sp, ConversationItem, MessageAckEvent, NewMessageEvent,
        ReadReceiptEvent, ServerEvent,
    },
    repository::message::MessageRepository,
};

pub struct ChatService {
    pub msg_repo: Arc<MessageRepository>,
    pub connections: Arc<ConnectionManager>,
    pub s3_client: Arc<aws_sdk_s3::Client>,
}

impl ChatService {
    /// Kirim pesan teks
    pub async fn send_text(
        &self,
        sender_id: &str,
        recipient_id: &str,
        content: &str,
        sender: &str,
        order_id: &str,
    ) -> Result<()> {
        if sender_id == recipient_id {
            anyhow::bail!("Tidak bisa mengirim pesan ke diri sendiri");
        }

        let mut msg = Message {
            id: Uuid::new_v4().to_string(),
            sender_id: sender_id.to_string(),
            recipient_id: recipient_id.to_string(),
            content: content.to_string(),
            msg_type: "text".to_string(),
            media_url: None,
            media_mime: None,
            media_size: None,
            media_duration: None,
            media_thumb: None,
            sent_at: chrono::Utc::now()
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string(),
            delivered_at: None,
            read_at: None,
            sender_avatar: None,
            sender_name: None,
            order_id: order_id.to_string(),
            status_order: None,
        };

        self.msg_repo.save(&mut msg).await?;
        self.push_message(&msg, sender).await;
        Ok(())
    }

    /// Kirim pesan media
    pub async fn send_media(
        &self,
        sender_id: &str,
        recipient_id: &str,
        media_url: &str,
        media_mime: &str,
        media_size: i64,
        sender: &str,
        order_id: &str,
    ) -> Result<Message> {
        if sender_id == recipient_id {
            anyhow::bail!("Tidak bisa kirim ke diri sendiri");
        }

        // 🔐 VALIDASI URL (WAJIB)
        if !media_url.starts_with("http://vmi3152926.contaboserver.net/rustride/") {
            anyhow::bail!("Invalid media URL");
        }

        // ✅ tentukan type dari mime
        let msg_type = mime_to_type(media_mime);

        let mut msg = Message {
            id: uuid::Uuid::new_v4().to_string(),
            sender_id: sender_id.to_string(),
            recipient_id: recipient_id.to_string(),
            content: "".to_string(),
            msg_type: msg_type.to_string(),
            media_url: Some(media_url.to_string()),
            media_mime: Some(media_mime.to_string()),
            media_size: Some(media_size),
            media_duration: None,
            media_thumb: None,
            sent_at: chrono::Utc::now()
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string(),
            delivered_at: None,
            read_at: None,
            sender_avatar: None,
            sender_name: None,
            order_id: order_id.to_string(),
            status_order: None,
        };

        self.msg_repo.save(&mut msg).await?;

        // 🔥 push ke recipient
        self.push_message(&msg, sender).await;

        Ok(msg)
    }

    pub async fn get_upload_url(
        &self,
        user_id: &str,
        mime: &str,
    ) -> anyhow::Result<(String, String)> {
        let ext = match mime {
            "image/jpeg" => "jpg",
            "image/png" => "png",
            "image/webp" => "webp",
            "video/mp4" => "mp4",
            _ => "bin",
        };

        let file_id = Uuid::new_v4().to_string();
        let key = format!("chat/{}/{}.{}", user_id, file_id, ext);

        let presigned = self
            .s3_client
            .put_object()
            .bucket("rustride")
            .key(&key)
            .content_type(mime)
            .presigned(PresigningConfig::expires_in(Duration::from_secs(300))?)
            .await?;

        let upload_url = presigned.uri().to_string();

        let public_url = format!("http://vmi3152926.contaboserver.net/rustride/{}", key);

        Ok((upload_url, public_url))
    }

    pub async fn get_conversations(&self, user_id: &str) -> Result<Vec<ConversationItem>> {
        self.msg_repo.get_conversations(user_id).await
    }

    /// Mark pesan dari sender sebagai sudah dibaca
    pub async fn mark_read(&self, reader_id: &str, sender_id: &str) -> Result<()> {
        self.msg_repo.mark_read(reader_id, sender_id).await?;

        // Notify sender bahwa pesannya sudah dibaca
        let read_at = chrono::Utc::now()
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string();
        self.connections.send(
            sender_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::ReadReceipt(ReadReceiptEvent {
                    from_user_id: reader_id.to_string(),
                    read_at,
                })),
            }),
            Priority::Normal,
        );
        Ok(())
    }

    /// Ambil history pesan antara dua user
    pub async fn get_history(
        &self,
        user_id: &str,
        peer_id: &str,
        limit: i32,
        before: Option<&str>,
    ) -> Result<Vec<Message>> {
        let limit = if limit <= 0 || limit > 100 { 50 } else { limit };
        let msgs = self
            .msg_repo
            .get_history(user_id, peer_id, limit, before)
            .await?;

        Ok(msgs)
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    async fn push_message(&self, msg: &Message, sender_name: &str) {
        let event = ServerEvent {
            payload: Some(Sp::NewMessage(NewMessageEvent {
                msg_id: msg.id.clone(),
                sender_id: msg.sender_id.clone(),
                sender_name: sender_name.to_string(),
                recipient_id: msg.recipient_id.clone(),
                content: msg.content.clone(),
                msg_type: msg.msg_type.clone(),
                sent_at: msg.sent_at.clone(),
                media_url: msg.media_url.clone().unwrap_or_default(),
                media_mime: msg.media_mime.clone().unwrap_or_default(),
                media_size: msg.media_size.unwrap_or(0),
                media_duration: msg.media_duration.unwrap_or(0),
                media_thumb: msg.media_thumb.clone().unwrap_or_default(),
                sender_avatar: msg.sender_avatar.clone().unwrap_or_default(),
                status_order: msg.status_order.clone().unwrap_or_default(),
            })),
        };

        // Push ke recipient
        self.connections.send(
            &msg.recipient_id,
            Arc::new(event.clone()),
            Priority::Critical,
        );

        // Ack ke sender
        self.connections.send(
            &msg.sender_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::MessageAck(MessageAckEvent {
                    msg_id: msg.id.clone(),
                    status: "sent".to_string(),
                    sent_at: msg.sent_at.clone(),
                })),
            }),
            Priority::Normal,
        );

        // Mark delivered kalau recipient online
        if self.connections.is_connected(&msg.recipient_id).await {
            let _ = self.msg_repo.mark_delivered(&msg.id).await;
        }
    }
}

fn detect_mime(bytes: &[u8]) -> Option<&'static str> {
    infer::get(bytes).map(|kind| kind.mime_type())
}

fn mime_to_type(mime: &str) -> &'static str {
    if mime.starts_with("image/") {
        "image"
    } else if mime.starts_with("video/") {
        "video"
    } else if mime.starts_with("audio/") {
        "audio"
    } else {
        "file"
    }
}
