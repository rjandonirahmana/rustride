use crate::proto::ridehailing::ServerEvent;
use anyhow::Result;
use dashmap::{DashMap, DashSet};
use futures::StreamExt;
use prost::Message;
use redis::AsyncCommands;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::{self, Receiver, Sender};

const CHANNEL_BUFFER: usize = 64;

type Tx = Sender<Result<ServerEvent, tonic::Status>>;

pub struct ConnectionManager {
    channels: DashMap<Arc<str>, Tx>,
    drivers: DashSet<Arc<str>>,
    riders: DashSet<Arc<str>>,
    redis: redis::aio::ConnectionManager,
    redis_tx: Sender<(String, Vec<u8>)>,
}

impl ConnectionManager {
    pub fn new(redis: redis::aio::ConnectionManager, redis_client: redis::Client) -> Arc<Self> {
        let (redis_tx, mut redis_rx) = mpsc::channel::<(String, Vec<u8>)>(512);
        let mut pub_redis = redis.clone();

        // 1 publish worker — 1 Redis connection untuk semua publish
        tokio::spawn(async move {
            while let Some((channel, bytes)) = redis_rx.recv().await {
                if let Err(e) = pub_redis.publish::<_, _, ()>(&channel, bytes).await {
                    tracing::warn!("Redis publish error: {}", e);
                }
            }
        });

        let mgr = Arc::new(Self {
            channels: DashMap::with_capacity_and_shard_amount(1024, 16),
            drivers: DashSet::new(),
            riders: DashSet::new(),
            redis,
            redis_tx,
        });

        // 1 shared subscriber untuk semua user — bukan per-user
        // Sebelumnya: 100k user = 100k Redis connection (OOM)
        // Sekarang:   1 Redis connection, routing di aplikasi
        Self::spawn_shared_subscriber(mgr.clone(), redis_client);

        mgr
    }

    // ── Shared Redis Subscriber ───────────────────────────────────────────────
    //
    // Pattern subscribe ke "evt:*"
    // Channel format: "evt:{user_id}"
    // Auto-reconnect kalau Redis putus

    fn spawn_shared_subscriber(mgr: Arc<Self>, redis_client: redis::Client) {
        tokio::spawn(async move {
            loop {
                match redis_client.get_async_pubsub().await {
                    Ok(mut pubsub) => {
                        if let Err(e) = pubsub.psubscribe("evt:*").await {
                            tracing::error!("psubscribe failed: {}", e);
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            continue;
                        }

                        tracing::info!("Redis shared subscriber ready");
                        let mut stream = pubsub.on_message();

                        while let Some(msg) = stream.next().await {
                            let channel: String = msg.get_channel_name().to_string();
                            let user_id = match channel.strip_prefix("evt:") {
                                Some(id) if !id.is_empty() => id,
                                _ => continue,
                            };

                            let bytes: Vec<u8> = msg.get_payload().unwrap_or_default();
                            match ServerEvent::decode(bytes.as_slice()) {
                                Ok(event) => {
                                    if let Some(tx) = mgr.channels.get(user_id) {
                                        if tx.try_send(Ok(event)).is_err() {
                                            drop(tx);
                                            mgr.channels.remove(user_id);
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(user_id, "Decode error: {}", e);
                                }
                            }
                        }

                        tracing::warn!("Redis subscriber disconnected, reconnecting...");
                    }
                    Err(e) => {
                        tracing::error!("Redis pubsub connect failed: {}", e);
                    }
                }

                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    // ── Connect / Disconnect ──────────────────────────────────────────────────

    pub async fn connect(&self, user_id: &str) -> Receiver<Result<ServerEvent, tonic::Status>> {
        let (tx, rx) = mpsc::channel(CHANNEL_BUFFER);
        let key: Arc<str> = user_id.into();
        self.channels.insert(key, tx);
        rx
    }

    pub async fn disconnect(&self, user_id: &str) {
        self.channels.remove(user_id);
        self.drivers.remove(user_id);
        self.riders.remove(user_id);

        let mut redis = self.redis.clone();
        let _: Result<(), _> = redis.del(format!("online:rider:{}", user_id)).await;
        let _: Result<(), _> = redis.del(format!("online:driver:{}", user_id)).await;
    }

    // ── Register ──────────────────────────────────────────────────────────────

    pub async fn register_driver(&self, driver_id: &str) {
        self.drivers.insert(driver_id.into());
        let mut redis = self.redis.clone();
        let _: Result<(), _> = redis
            .set_ex(format!("online:driver:{}", driver_id), "1", 300u64)
            .await;
    }

    pub async fn register_rider(&self, rider_id: &str) {
        self.riders.insert(rider_id.into());
        let mut redis = self.redis.clone();
        let _: Result<(), _> = redis
            .set_ex(format!("online:rider:{}", rider_id), "1", 300u64)
            .await;
    }

    pub async fn refresh_ttl(&self, user_id: &str, role: &str) {
        let mut redis = self.redis.clone();
        let key = format!("online:{}:{}", role, user_id);
        if let Err(e) = redis.expire::<_, ()>(key, 300).await {
            tracing::warn!(user_id, "refresh_ttl failed: {}", e);
        }
    }

    // ── Send ──────────────────────────────────────────────────────────────────

    pub fn send(&self, user_id: &str, event: ServerEvent) {
        if let Some(tx) = self.channels.get(user_id) {
            match tx.try_send(Ok(event.clone())) {
                Ok(_) => return,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(user_id, "Channel full, dropping connection");
                    drop(tx);
                    self.channels.remove(user_id);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    drop(tx);
                    self.channels.remove(user_id);
                }
            }
        }

        let bytes = event.encode_to_vec();
        let channel = format!("evt:{}", user_id);
        if self.redis_tx.try_send((channel, bytes)).is_err() {
            tracing::warn!(user_id, "Redis queue full, event dropped");
        }
    }

    pub fn send_to_drivers(&self, driver_ids: &[String], event: ServerEvent) {
        let mut remote_bytes: Option<Vec<u8>> = None;

        for driver_id in driver_ids {
            if let Some(tx) = self.channels.get(driver_id.as_str()) {
                if tx.try_send(Ok(event.clone())).is_err() {
                    drop(tx);
                    self.channels.remove(driver_id.as_str());
                } else {
                    continue;
                }
            }

            let bytes = remote_bytes.get_or_insert_with(|| event.encode_to_vec());
            let _ = self
                .redis_tx
                .try_send((format!("evt:{}", driver_id), bytes.clone()));
        }
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    pub async fn is_connected(&self, user_id: &str) -> bool {
        if self.channels.contains_key(user_id) {
            return true;
        }
        let mut redis = self.redis.clone();
        if redis
            .exists::<_, bool>(format!("online:rider:{}", user_id))
            .await
            .unwrap_or(false)
        {
            return true;
        }
        redis
            .exists::<_, bool>(format!("online:driver:{}", user_id))
            .await
            .unwrap_or(false)
    }

    pub fn online_drivers(&self) -> Vec<String> {
        self.drivers
            .iter()
            .filter(|id| self.channels.contains_key(id.as_ref()))
            .map(|id| id.to_string())
            .collect()
    }

    pub fn online_users(&self) -> Vec<String> {
        self.channels.iter().map(|e| e.key().to_string()).collect()
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    pub fn spawn_cleanup(self: &Arc<Self>) {
        let mgr = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                let before = mgr.channels.len();
                mgr.channels.retain(|_, tx| !tx.is_closed());
                let after = mgr.channels.len();
                if before != after {
                    tracing::info!("Cleanup: removed {} stale connections", before - after);
                }
            }
        });
    }
}
