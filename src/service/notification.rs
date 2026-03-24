use std::sync::Arc;

use crate::{
    connections::{ConnectionManager, Priority},
    proto::notification::{NewNotificationEvent, NotificationItem, NotificationsEvent},
    proto::ridehailing::{server_event::Payload as Sp, ServerEvent},
    repository::notification::NotificationRepositoryTrait,
};

pub struct NotificationService<NR: NotificationRepositoryTrait> {
    pub notif_repo: Arc<NR>,
    pub connections: Arc<ConnectionManager>,
}

impl<NR: NotificationRepositoryTrait + 'static> NotificationService<NR> {
    /// Push notif realtime ke user (dan simpan ke DB sekaligus)
    pub async fn push(
        &self,
        user_id: &str,
        notif_type: &str,
        title: &str,
        body: &str,
        ref_order_id: Option<&str>,
        ref_user_id: Option<&str>,
        meta: Option<&str>,
    ) -> anyhow::Result<()> {
        let notif_id = self
            .notif_repo
            .create(
                user_id,
                notif_type,
                title,
                body,
                ref_order_id,
                ref_user_id,
                meta,
            )
            .await?;

        // Jika user online, push realtime
        self.connections.send(
            user_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::NewNotification(NewNotificationEvent {
                    notification: Some(NotificationItem {
                        id: notif_id,
                        r#type: notif_type.to_string(),
                        title: title.to_string(),
                        body: body.to_string(),
                        ref_order_id: ref_order_id.unwrap_or("").to_string(),
                        ref_user_id: ref_user_id.unwrap_or("").to_string(),
                        meta: meta.unwrap_or("{}").to_string(),
                        is_read: false,
                        created_at: chrono::Utc::now().to_rfc3339(),
                    }),
                })),
            }),
            Priority::Normal,
        );

        Ok(())
    }

    /// Ambil daftar notif user (untuk GetNotifications request)
    pub async fn list(
        &self,
        user_id: &str,
        limit: i32,
        before_id: Option<i64>,
        unread_only: bool,
    ) -> anyhow::Result<NotificationsEvent> {
        let limit = limit.clamp(1, 50);
        let items = self
            .notif_repo
            .list(user_id, limit, before_id, unread_only)
            .await?;

        let has_more = items.len() as i32 == limit;

        Ok(NotificationsEvent {
            items: items
                .into_iter()
                .map(|n| NotificationItem {
                    id: n.id,
                    r#type: n.notif_type,
                    title: n.title,
                    body: n.body,
                    ref_order_id: n.ref_order_id.unwrap_or_default(),
                    ref_user_id: n.ref_user_id.unwrap_or_default(),
                    meta: n.meta.unwrap_or_else(|| "{}".into()),
                    is_read: n.is_read,
                    created_at: n.created_at,
                })
                .collect(),
            has_more,
        })
    }

    pub async fn mark_read(&self, notif_id: i64, user_id: &str) -> anyhow::Result<()> {
        self.notif_repo.mark_read(notif_id, user_id).await
    }

    pub async fn mark_all_read(&self, user_id: &str) -> anyhow::Result<()> {
        self.notif_repo.mark_all_read(user_id).await
    }
}
