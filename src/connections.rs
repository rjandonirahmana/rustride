use crate::proto::ridehailing::ServerEvent;
use anyhow::Result;
use dashmap::{DashMap, DashSet};
use futures::StreamExt;
use prost::Message;
use redis::AsyncCommands;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;

type Tx = UnboundedSender<Result<ServerEvent, tonic::Status>>;

pub struct ConnectionManager {
    channels: DashMap<String, Tx>,
    /// Cancellation token per user — untuk matikan Redis pubsub task saat disconnect
    cancel_tokens: DashMap<String, CancellationToken>,
    drivers: DashSet<String>,
    riders: DashSet<String>,
    redis: redis::aio::ConnectionManager,
    redis_client: redis::Client,
    redis_tx: UnboundedSender<(String, Vec<u8>)>,
}

impl ConnectionManager {
    pub fn new(redis: redis::aio::ConnectionManager, redis_client: redis::Client) -> Arc<Self> {
        let (redis_tx, mut redis_rx) = mpsc::unbounded_channel::<(String, Vec<u8>)>();
        let mut pub_redis = redis.clone();

        // Single background task — semua Redis publish lewat sini
        tokio::spawn(async move {
            while let Some((channel, bytes)) = redis_rx.recv().await {
                if let Err(e) = pub_redis.publish::<_, _, ()>(channel, bytes).await {
                    tracing::warn!("Redis publish error: {}", e);
                }
            }
        });

        Arc::new(Self {
            channels: DashMap::new(),
            cancel_tokens: DashMap::new(),
            drivers: DashSet::new(),
            riders: DashSet::new(),
            redis,
            redis_client,
            redis_tx,
        })
    }

    // ── Connect / Disconnect ──────────────────────────────────────────────────

    pub async fn connect(
        self: &Arc<Self>,
        user_id: &str,
    ) -> UnboundedReceiver<Result<ServerEvent, tonic::Status>> {
        // Batalkan sesi lama kalau ada (reconnect)
        if let Some((_, old_token)) = self.cancel_tokens.remove(user_id) {
            old_token.cancel();
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let token = CancellationToken::new();

        self.channels.insert(user_id.to_string(), tx.clone());
        self.cancel_tokens
            .insert(user_id.to_string(), token.clone());

        let uid = user_id.to_string();
        let redis_client = self.redis_client.clone();

        tokio::spawn(async move {
            // ── Setup Redis Pub/Sub ──────────────────────────────────────────
            let pubsub = match redis_client.get_async_pubsub().await {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(user_id = %uid, "Redis pubsub connect failed: {}", e);
                    return;
                }
            };
            let mut pubsub = pubsub;
            if let Err(e) = pubsub.subscribe(format!("user:{}", uid)).await {
                tracing::error!(user_id = %uid, "Redis subscribe failed: {}", e);
                return;
            }

            let mut stream = pubsub.on_message();

            loop {
                tokio::select! {
                    // Terima pesan dari Redis
                    Some(msg) = stream.next() => {
                        let bytes: Vec<u8> = msg.get_payload().unwrap_or_default();
                        match ServerEvent::decode(bytes.as_slice()) {
                            Ok(event) => {
                                if tx.send(Ok(event)).is_err() {
                                    tracing::debug!(user_id = %uid, "gRPC rx dropped, closing pubsub");
                                    break;
                                }
                            }
                            Err(e) => {
                                tracing::warn!(user_id = %uid, "Decode error: {}", e);
                            }
                        }
                    }
                    // Disconnect eksplisit dipanggil
                    _ = token.cancelled() => {
                        tracing::debug!(user_id = %uid, "Pubsub task cancelled");
                        break;
                    }
                }
            }
        });

        rx
    }

    pub async fn disconnect(&self, user_id: &str) {
        // Cancel Redis pubsub task dulu
        if let Some((_, token)) = self.cancel_tokens.remove(user_id) {
            token.cancel();
        }

        self.channels.remove(user_id);
        self.drivers.remove(user_id);
        self.riders.remove(user_id);

        let mut redis = self.redis.clone();
        let _: Result<(), _> = redis.del(format!("online:rider:{}", user_id)).await;
        let _: Result<(), _> = redis.del(format!("online:driver:{}", user_id)).await;
    }

    // ── Register ──────────────────────────────────────────────────────────────

    pub async fn register_driver(&self, driver_id: &str) {
        self.drivers.insert(driver_id.to_string());
        let mut redis = self.redis.clone();
        let _: Result<(), _> = redis
            .set_ex(format!("online:driver:{}", driver_id), "1", 60u64)
            .await;
    }

    pub async fn register_rider(&self, rider_id: &str) {
        self.riders.insert(rider_id.to_string());
        let mut redis = self.redis.clone();
        let _: Result<(), _> = redis
            .set_ex(format!("online:rider:{}", rider_id), "1", 60u64)
            .await;
    }

    pub async fn refresh_ttl(&self, user_id: &str, role: &str) {
        let mut redis = self.redis.clone();
        let key = format!("online:{}:{}", role, user_id);
        if let Err(e) = redis.expire::<_, ()>(key, 60).await {
            tracing::warn!(user_id, "refresh_ttl failed: {}", e);
        }
    }

    // ── Send ──────────────────────────────────────────────────────────────────

    /// Kirim ke satu user. Local-first, fallback Redis Pub/Sub.
    pub fn send(&self, user_id: &str, event: ServerEvent) {
        if let Some(tx) = self.channels.get(user_id) {
            if tx.send(Ok(event)).is_err() {
                // rx sudah drop → bersihkan
                drop(tx);
                self.channels.remove(user_id);
                if let Some((_, token)) = self.cancel_tokens.remove(user_id) {
                    token.cancel();
                }
            }
        } else {
            let bytes = event.encode_to_vec();
            if self
                .redis_tx
                .send((format!("user:{}", user_id), bytes))
                .is_err()
            {
                tracing::error!(user_id, "Redis publish channel closed");
            }
        }
    }

    /// Kirim ke banyak driver. Encode bytes sekali untuk semua remote.
    pub fn send_to_drivers(&self, driver_ids: &[String], event: ServerEvent) {
        let mut remote_bytes: Option<Vec<u8>> = None;

        for driver_id in driver_ids {
            if let Some(tx) = self.channels.get(driver_id) {
                if tx.send(Ok(event.clone())).is_err() {
                    drop(tx);
                    self.channels.remove(driver_id);
                    if let Some((_, token)) = self.cancel_tokens.remove(driver_id.as_str()) {
                        token.cancel();
                    }
                }
            } else {
                let bytes = remote_bytes.get_or_insert_with(|| event.encode_to_vec());
                if self
                    .redis_tx
                    .send((format!("user:{}", driver_id), bytes.clone()))
                    .is_err()
                {
                    tracing::error!("Redis publish channel closed");
                    break;
                }
            }
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
            .filter(|id| self.channels.contains_key(id.as_str()))
            .map(|id| id.clone())
            .collect()
    }

    pub fn online_users(&self) -> Vec<String> {
        self.channels.iter().map(|e| e.key().clone()).collect()
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    /// Sweep channel + token mati tiap 30 detik.
    /// Panggil sekali saat startup: `connections.spawn_cleanup()`
    pub fn spawn_cleanup(self: &Arc<Self>) {
        let mgr = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                let before = mgr.channels.len();
                mgr.channels.retain(|user_id, tx| {
                    if tx.is_closed() {
                        // Juga cancel token-nya
                        if let Some((_, token)) = mgr.cancel_tokens.remove(user_id) {
                            token.cancel();
                        }
                        false
                    } else {
                        true
                    }
                });
                let after = mgr.channels.len();
                if before != after {
                    tracing::info!("Cleanup: removed {} stale connections", before - after);
                }
            }
        });
    }
}
