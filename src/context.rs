// ===== context.rs =====
// StreamContext: semua dependency yang dibutuhkan handler, di-bundle sekali.
// Menghindari clone parade di setiap spawn.

use std::sync::Arc;

use crate::{
    connections::{ConnectionManager, Priority},
    location::LocationService,
    proto::ridehailing::{server_event::Payload as Sp, ErrorEvent, ServerEvent},
    repository::{
        notification::NotificationRepositoryTrait, order::OrderRepository,
        rideshare::RideshareRepositoryTrait, user::UserRepository,
    },
    service::{
        chat::ChatService, notification::NotificationService, order::OrderService,
        rideshare::RideshareService,
    },
};

use super::throttle::ThrottleMap;

pub struct StreamContext<OR, UR, RR, NR>
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    pub user_id: String,
    pub role: String,
    pub username: String,
    pub vehicle_type: String,

    pub order_svc: Arc<OrderService<OR, UR>>,
    pub connections: Arc<ConnectionManager>,
    pub location: LocationService,
    pub chat_svc: Arc<ChatService>,
    pub rideshare_svc: Arc<RideshareService<RR, NR, UR>>,
    pub notif_svc: Arc<NotificationService<NR>>,
    pub throttle: Arc<ThrottleMap>,
}

impl<OR, UR, RR, NR> StreamContext<OR, UR, RR, NR>
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
    RR: RideshareRepositoryTrait + 'static,
    NR: NotificationRepositoryTrait + 'static,
{
    /// Kirim error event ke user ini. Shorthand agar handler tidak verbose.
    pub fn send_err(&self, code: &str, msg: &str) {
        self.connections.send(
            &self.user_id,
            Arc::new(ServerEvent {
                payload: Some(Sp::Error(ErrorEvent {
                    code: code.into(),
                    message: msg.into(),
                })),
            }),
            Priority::Normal,
        );
    }

    /// Kirim arbitrary ServerEvent ke user ini.
    pub fn send(&self, payload: Sp, priority: Priority) {
        self.connections.send(
            &self.user_id,
            Arc::new(ServerEvent {
                payload: Some(payload),
            }),
            priority,
        );
    }

    /// Guard: return false + kirim FORBIDDEN jika role tidak cocok.
    pub fn require_role(&self, required: &str) -> bool {
        if self.role != required {
            self.send_err("FORBIDDEN", &format!("Hanya {required}"));
            return false;
        }
        true
    }
}
