use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub sender_id: String,
    pub recipient_id: String,
    pub order_id: String,
    pub content: String,
    pub msg_type: String,
    pub media_url: Option<String>,
    pub media_mime: Option<String>,
    pub media_size: Option<i64>,
    pub media_duration: Option<i32>,
    pub media_thumb: Option<String>,
    pub sent_at: DateTime<Utc>,              // ← langsung DateTime
    pub delivered_at: Option<DateTime<Utc>>, // ← Option
    pub read_at: Option<DateTime<Utc>>,      // ← Option
    pub sender_name: Option<String>,
    pub sender_avatar: Option<String>,
    pub status_order: Option<String>,
}
