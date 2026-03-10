use crate::proto::ridehailing::ServerEvent;
use anyhow::Result;
use futures::StreamExt;
use prost::Message;
use redis::AsyncCommands;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

type Tx = UnboundedSender<Result<ServerEvent, tonic::Status>>;

pub struct ConnectionManager {
    channels: RwLock<HashMap<String, Tx>>,
    drivers: RwLock<HashSet<String>>,
    riders: RwLock<HashSet<String>>,
    redis: redis::aio::ConnectionManager, // ← tambah ini
    redis_client: redis::Client,
}

impl ConnectionManager {
    pub fn new(redis: redis::aio::ConnectionManager, redis_client: redis::Client) -> Arc<Self> {
        Arc::new(Self {
            channels: RwLock::default(),
            drivers: RwLock::default(),
            riders: RwLock::default(),
            redis: redis,
            redis_client,
        })
    }

    pub async fn disconnect(&self, user_id: &str) {
        self.channels.write().unwrap().remove(user_id);
        self.drivers.write().unwrap().remove(user_id);
        self.riders.write().unwrap().remove(user_id);

        let mut redis = self.redis.clone();
        let _: Result<(), _> = redis.del(format!("online:rider:{}", user_id)).await;
        let _: Result<(), _> = redis.del(format!("online:driver:{}", user_id)).await;
    }

    pub async fn connect(
        self: &Arc<Self>,
        user_id: &str,
    ) -> UnboundedReceiver<Result<ServerEvent, tonic::Status>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.channels
            .write()
            .unwrap()
            .insert(user_id.to_string(), tx.clone());

        let uid = user_id.to_string();
        let redis_client = self.redis_client.clone();
        tokio::spawn(async move {
            let Ok(mut pubsub) = redis_client.get_async_pubsub().await else {
                return;
            };
            let Ok(_) = pubsub.subscribe(format!("user:{}", uid)).await else {
                return;
            };

            let mut stream = pubsub.on_message();
            while let Some(msg) = stream.next().await {
                let bytes: Vec<u8> = msg.get_payload().unwrap_or_default();
                if let Ok(event) = ServerEvent::decode(bytes.as_slice()) {
                    if tx.send(Ok(event)).is_err() {
                        break; // tx closed = user disconnect, loop berhenti otomatis
                    }
                }
            }
            // Task selesai, pubsub subscription otomatis di-drop
        });

        rx
    }

    pub async fn register_driver(&self, driver_id: &str) {
        self.drivers.write().unwrap().insert(driver_id.to_string());

        // Set di Redis dengan TTL supaya auto-expire kalau instance mati
        let mut redis = self.redis.clone();
        let _: Result<(), _> = redis
            .set_ex(format!("online:driver:{}", driver_id), "1", 60u64)
            .await;
    }

    pub async fn register_rider(&self, rider_id: &str) {
        self.riders.write().unwrap().insert(rider_id.to_string());

        // Set di Redis dengan TTL supaya auto-expire kalau instance mati
        let mut redis = self.redis.clone();
        let _: Result<(), _> = redis
            .set_ex(format!("online:rider:{}", rider_id), "1", 60u64)
            .await;
    }

    pub fn send_to_drivers(&self, driver_ids: &[String], event: ServerEvent) {
        for driver_id in driver_ids {
            self.send(driver_id, event.clone()); // ← pakai self.send yang sudah handle Redis
        }
    }

    // Send — coba local dulu, kalau tidak ada publish ke Redis
    pub fn send(&self, user_id: &str, event: ServerEvent) {
        if let Some(tx) = self.channels.read().unwrap().get(user_id) {
            let _ = tx.send(Ok(event));
        } else {
            // User tidak ada di instance ini → publish ke Redis
            let mut redis = self.redis.clone();
            let channel = format!("user:{}", user_id);
            let bytes = Self::encode_event(&event); // serialize ke bytes
            tokio::spawn(async move {
                let _: Result<(), _> = redis.publish(channel, bytes).await;
            });
        }
    }

    fn encode_event(event: &ServerEvent) -> Vec<u8> {
        use prost::Message;
        event.encode_to_vec()
    }

    pub async fn is_connected(&self, user_id: &str) -> bool {
        if self.channels.read().unwrap().contains_key(user_id) {
            return true;
        }
        let mut redis = self.redis.clone();
        let as_rider = redis
            .exists::<_, bool>(format!("online:rider:{}", user_id))
            .await
            .unwrap_or(false);
        if as_rider {
            return true;
        }
        redis
            .exists::<_, bool>(format!("online:driver:{}", user_id))
            .await
            .unwrap_or(false)
    }

    pub fn online_drivers(&self) -> Vec<String> {
        let channels = self.channels.read().unwrap();
        self.drivers
            .read()
            .unwrap()
            .iter()
            .filter(|id| channels.contains_key(*id))
            .cloned()
            .collect()
    }

    pub fn online_users(&self) -> Vec<String> {
        self.channels.read().unwrap().keys().cloned().collect()
    }
}
