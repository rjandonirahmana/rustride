use anyhow::Result;
use aws_sdk_s3::presigning::PresigningConfig;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    connections::{ConnectionManager, Priority},
    models::message::Message,
    proto::{
        message::{ConversationItem, NewMessageEvent},
        ridehailing::{server_event::Payload as Sp, ReadReceiptEvent, ServerEvent},
    },
    repository::message::MessageRepository,
    utils::ulid::{mime_to_type, new_ulid},
};

#[derive(Clone)]
pub struct ChatService {
    pub msg_repo: Arc<MessageRepository>,
    pub connections: Arc<ConnectionManager>,
    pub s3_client: Arc<aws_sdk_s3::Client>,
    pub r2_public_url: String,
    pub r2_bucket: String,
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
    ) -> Result<Message> {
        if sender_id == recipient_id {
            anyhow::bail!("Tidak bisa mengirim pesan ke diri sendiri");
        }

        let mut msg = Message {
            id: new_ulid(),
            sender_id: sender_id.to_string(),
            recipient_id: recipient_id.to_string(),
            content: content.to_string(),
            msg_type: "text".to_string(),
            media_url: None,
            media_mime: None,
            media_size: None,
            media_duration: None,
            media_thumb: None,
            sent_at: chrono::Utc::now(),
            delivered_at: None,
            read_at: None,
            sender_avatar: None,
            sender_name: Some(sender.to_string()),
            order_id: order_id.to_string(),
            status_order: None,
        };

        self.msg_repo.save(&mut msg).await?;
        self.push_message(&msg, sender).await;
        Ok(msg)
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

        if !media_url.contains(".r2.cloudflarestorage.com") && !media_url.contains("r2.dev") {
            anyhow::bail!("Invalid media URL");
        }

        let msg_type = mime_to_type(media_mime);

        let mut msg = Message {
            id: new_ulid(),
            sender_id: sender_id.to_string(),
            recipient_id: recipient_id.to_string(),
            content: String::new(),
            msg_type: msg_type.to_string(),
            media_url: Some(media_url.to_string()),
            media_mime: Some(media_mime.to_string()),
            media_size: Some(media_size),
            media_duration: None,
            media_thumb: None,
            sent_at: chrono::Utc::now(),
            delivered_at: None,
            read_at: None,
            sender_avatar: None,
            sender_name: Some(sender.to_string()),
            order_id: order_id.to_string(),
            status_order: None,
        };

        self.msg_repo.save(&mut msg).await?;
        self.push_message(&msg, sender).await;
        Ok(msg)
    }

    pub async fn get_upload_url(&self, user_id: &str, mime: &str) -> Result<(String, String)> {
        let ext = match mime {
            "image/jpeg" => "jpg",
            "image/png" => "png",
            "image/webp" => "webp",
            "video/mp4" => "mp4",
            _ => "bin",
        };

        let file_id = new_ulid();
        let key = format!("chat/{}/{}.{}", user_id, file_id, ext);

        let presigned = self
            .s3_client
            .put_object()
            .bucket(&self.r2_bucket)
            .key(&key)
            .content_type(mime)
            .presigned(PresigningConfig::expires_in(Duration::from_secs(300))?)
            .await?;

        let upload_url = presigned.uri().to_string();
        let public_url = format!("{}/{}", self.r2_public_url, key);

        Ok((upload_url, public_url))
    }

    pub async fn get_conversations(&self, user_id: &str) -> Result<Vec<ConversationItem>> {
        self.msg_repo.get_conversations(user_id).await
    }

    /// Mark pesan dari sender sebagai sudah dibaca, lalu notify sender
    pub async fn mark_read(&self, reader_id: &str, sender_id: &str) -> Result<()> {
        self.msg_repo.mark_read(reader_id, sender_id).await?;

        let read_at = chrono::Utc::now().to_rfc3339();

        // Kirim ReadReceipt ke sender pesan agar UI-nya update centang biru
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
        let limit = limit.clamp(1, 100);
        self.msg_repo
            .get_history(user_id, peer_id, limit, before)
            .await
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    async fn push_message(&self, msg: &Message, sender_name: &str) {
        let event = Arc::new(ServerEvent {
            payload: Some(Sp::NewMessage(NewMessageEvent {
                msg_id: msg.id.clone(),
                sender_id: msg.sender_id.clone(),
                sender_name: sender_name.to_string(),
                recipient_id: msg.recipient_id.clone(),
                content: msg.content.clone(),
                msg_type: msg.msg_type.clone(),
                sent_at: msg.sent_at.to_rfc3339(),
                media_url: msg.media_url.clone().unwrap_or_default(),
                media_mime: msg.media_mime.clone().unwrap_or_default(),
                media_size: msg.media_size.unwrap_or(0),
                media_duration: msg.media_duration.unwrap_or(0),
                media_thumb: msg.media_thumb.clone().unwrap_or_default(),
                sender_avatar: msg.sender_avatar.clone().unwrap_or_default(),
                status_order: msg.status_order.clone().unwrap_or_default(),
            })),
        });

        // ── Push ke recipient ─────────────────────────────────────────────────
        //
        // BUG LAMA 1: send() dipanggil SEBELUM is_connected check
        //   → pesan di-push ke koneksi yang belum tentu ada, error diabaikan
        //
        // BUG LAMA 2: match Ok(_) { ... } menangkap Ok(false) juga
        //   → mark_delivered dipanggil meski recipient sedang offline
        //
        // FIX: cek is_connected dulu → kalau Ok(true) baru send + mark_delivered
        match self.connections.is_connected(&msg.recipient_id).await {
            Ok(true) => {
                self.connections
                    .send(&msg.recipient_id, Arc::clone(&event), Priority::Normal);

                // Mark delivered hanya setelah event berhasil dikirim ke koneksi aktif
                if let Err(e) = self.msg_repo.mark_delivered(&msg.id).await {
                    tracing::warn!(
                        msg_id = %msg.id,
                        error  = %e,
                        "mark_delivered failed"
                    );
                }
            }
            Ok(false) => {
                // Recipient offline — pesan tersimpan di DB,
                // akan di-deliver saat reconnect via getCurrentOrder / stream reconnect
                tracing::debug!(
                    msg_id       = %msg.id,
                    recipient_id = %msg.recipient_id,
                    "Recipient offline, message queued"
                );
            }
            Err(e) => {
                tracing::warn!(
                    msg_id       = %msg.id,
                    recipient_id = %msg.recipient_id,
                    error        = %e,
                    "is_connected check failed"
                );
            }
        }
    }
}
