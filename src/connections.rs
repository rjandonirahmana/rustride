// Tambahkan ke Cargo.toml:
//
// flume   = "0.11"
// ahash   = "0.8"
// futures = "0.3"

use crate::proto::ridehailing::ServerEvent;
use crate::proto::ridehailing::{server_event::Payload as Sp, ErrorEvent};
use ahash::AHasher;
use anyhow::Result;
use dashmap::{DashMap, DashSet};
use futures::{stream, StreamExt};
use prost::Message;
use redis::AsyncCommands;
use std::{
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Semaphore,
};
use tokio_util::sync::CancellationToken;

// ── Constants ─────────────────────────────────────────────────────────────────

const CHANNEL_BUFFER: usize = 128;
const PUB_WORKER_COUNT: usize = 4;
const PUB_CHANNEL_BUFFER_CRITICAL: usize = 1024;
const PUB_CHANNEL_BUFFER_NORMAL: usize = 4096;
const DECODE_WORKER_COUNT: usize = 4;
const DECODE_CHANNEL_BUFFER: usize = 2048;
const SPAWN_CONCURRENCY_LIMIT: usize = 256;
const PRESENCE_TTL_SECS: u64 = 300;
const REDIS_PUBLISH_RETRIES: usize = 3;
const CLEANUP_INTERVAL_SECS: u64 = 30;
const PRESENCE_BATCH_INTERVAL_SECS: u64 = 55;
const PRESENCE_BATCH_SIZE: usize = 1000;

// FIX: Batas concurrency untuk fanout di send_to_drivers.
// 32 = cukup untuk saturate Redis pipeline tanpa spawn terlalu banyak task.
const FANOUT_CONCURRENCY: usize = 32;

// ── Types ─────────────────────────────────────────────────────────────────────

type Tx = Sender<Result<ServerEvent, tonic::Status>>;
type ArcEvent = Arc<ServerEvent>;
type ArcBytes = Arc<[u8]>;

// ── Priority ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
pub enum Priority {
    Critical, // order, driver_matched, payment, WebRTC signaling
    Normal,   // location_update, chat, browse results
}

// ── Jitter counter ────────────────────────────────────────────────────────────
//
// FIX: Ganti SystemTime::now() (syscall tiap retry) dengan atomic counter.
// Cukup untuk mencegah thundering herd — tidak perlu kriptografi.
static JITTER_COUNTER: AtomicU64 = AtomicU64::new(0);

fn rand_jitter() -> u64 {
    JITTER_COUNTER.fetch_add(1, Ordering::Relaxed) % 10
}

// ── Sharded publish workers ───────────────────────────────────────────────────

struct ShardedWorkers {
    senders: Vec<flume::Sender<(String, ArcBytes)>>,
}

impl ShardedWorkers {
    fn new(redis: Arc<redis::aio::ConnectionManager>, n: usize, buf: usize) -> Self {
        let mut senders = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = flume::bounded::<(String, ArcBytes)>(buf);
            senders.push(tx);
            // FIX: redis.clone() — clone Arc, bukan buka koneksi baru.
            // as_ref().clone() akan clone ConnectionManager di dalamnya (koneksi baru).
            // Arc::clone hanya increment refcount = O(1), tidak alokasi.
            let mut conn = (*redis).clone();
            tokio::spawn(async move {
                while let Ok((channel, bytes)) = rx.recv_async().await {
                    publish_with_retry(&mut conn, &channel, &bytes).await;
                }
            });
        }
        Self { senders }
    }

    async fn send_critical(&self, user_id: &str, channel: String, bytes: ArcBytes) {
        let idx = shard_index(user_id, self.senders.len());
        if let Err(e) = self.senders[idx].send_async((channel, bytes)).await {
            tracing::error!(user_id, "Critical shard closed (worker dead): {}", e);
        }
    }

    fn send_normal(
        &self,
        user_id: &str,
        channel: String,
        bytes: ArcBytes,
        dropped: &Arc<AtomicU64>,
    ) {
        let idx = shard_index(user_id, self.senders.len());
        if self.senders[idx].try_send((channel, bytes)).is_err() {
            // FIX: increment metric, bukan hanya log
            dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(user_id, shard = idx, "Normal pub shard full, dropping");
        }
    }
}

// ── PubWorkers ────────────────────────────────────────────────────────────────

struct PubWorkers {
    critical: ShardedWorkers,
    normal: ShardedWorkers,
    dropped_events: Arc<AtomicU64>, // shared dengan ConnectionManager
}

impl PubWorkers {
    fn new(redis: Arc<redis::aio::ConnectionManager>, dropped: Arc<AtomicU64>) -> Arc<Self> {
        Arc::new(Self {
            critical: ShardedWorkers::new(
                redis.clone(),
                PUB_WORKER_COUNT,
                PUB_CHANNEL_BUFFER_CRITICAL,
            ),
            normal: ShardedWorkers::new(redis, PUB_WORKER_COUNT, PUB_CHANNEL_BUFFER_NORMAL),
            dropped_events: dropped,
        })
    }

    async fn publish(&self, user_id: &str, bytes: ArcBytes, priority: Priority) {
        let channel = format!("evt:{}", user_id);
        match priority {
            Priority::Critical => self.critical.send_critical(user_id, channel, bytes).await,
            Priority::Normal => {
                self.normal
                    .send_normal(user_id, channel, bytes, &self.dropped_events)
            }
        }
    }
}

// ── ConnectionManager ─────────────────────────────────────────────────────────

pub struct ConnectionManager {
    channels: DashMap<Arc<str>, Tx>,
    drivers: DashSet<Arc<str>>,
    riders: DashSet<Arc<str>>,
    redis: Arc<redis::aio::ConnectionManager>,
    pub_workers: Arc<PubWorkers>,
    spawn_sem: Arc<Semaphore>,
    decode_tx: flume::Sender<(String, ArcBytes)>,

    pub dropped_events: Arc<AtomicU64>,
    pub critical_queued: Arc<AtomicU64>,
}

impl ConnectionManager {
    pub fn new(redis: redis::aio::ConnectionManager, redis_client: redis::Client) -> Arc<Self> {
        let redis = Arc::new(redis);

        let dropped_events = Arc::new(AtomicU64::new(0));
        let pub_workers = PubWorkers::new(redis.clone(), Arc::clone(&dropped_events));

        let (decode_tx, decode_rx) = flume::bounded::<(String, ArcBytes)>(DECODE_CHANNEL_BUFFER);

        let mgr = Arc::new(Self {
            channels: DashMap::with_capacity_and_shard_amount(1024, 16),
            drivers: DashSet::new(),
            riders: DashSet::new(),
            redis,
            pub_workers,
            spawn_sem: Arc::new(Semaphore::new(SPAWN_CONCURRENCY_LIMIT)),
            decode_tx,
            dropped_events,
            critical_queued: Arc::new(AtomicU64::new(0)),
        });

        for _ in 0..DECODE_WORKER_COUNT {
            let mgr_w = mgr.clone();
            let rx_clone = decode_rx.clone();
            tokio::spawn(async move {
                while let Ok((user_id, bytes)) = rx_clone.recv_async().await {
                    match ServerEvent::decode(bytes.as_ref()) {
                        Ok(event) => {
                            if let Some(tx) = mgr_w.channels.get(&*user_id) {
                                if tx.try_send(Ok(event)).is_err() {
                                    // FIX: increment dropped juga di decode pool
                                    mgr_w.dropped_events.fetch_add(1, Ordering::Relaxed);
                                    drop(tx);
                                    mgr_w.channels.remove(&*user_id);
                                }
                            }
                        }
                        Err(e) => tracing::warn!(user_id, "Decode error: {}", e),
                    }
                }
            });
        }

        Self::spawn_shared_subscriber(mgr.clone(), redis_client);
        Self::spawn_global_presence_refresher(mgr.clone());

        mgr
    }

    // ── Redis Subscriber ──────────────────────────────────────────────────────

    fn spawn_shared_subscriber(mgr: Arc<Self>, redis_client: redis::Client) {
        let decode_tx = mgr.decode_tx.clone();
        let dropped = Arc::clone(&mgr.dropped_events);

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
                            let channel = msg.get_channel_name().to_string();
                            let user_id = match channel.strip_prefix("evt:") {
                                Some(id) if !id.is_empty() => id.to_string(),
                                _ => continue,
                            };
                            let raw: Vec<u8> = match msg.get_payload() {
                                Ok(b) => b,
                                Err(e) => {
                                    tracing::warn!("Invalid redis payload: {}", e);
                                    continue;
                                }
                            };
                            let bytes: ArcBytes = raw.into();

                            if decode_tx.try_send((user_id, bytes)).is_err() {
                                // FIX: increment dropped di subscriber juga
                                dropped.fetch_add(1, Ordering::Relaxed);
                                tracing::warn!("Decode pool full, dropping incoming message");
                            }
                        }
                        tracing::warn!("Redis subscriber disconnected, reconnecting...");
                    }
                    Err(e) => tracing::error!("Redis pubsub connect failed: {}", e),
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    // ── Global Presence Refresher ─────────────────────────────────────────────

    fn spawn_global_presence_refresher(mgr: Arc<Self>) {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(PRESENCE_BATCH_INTERVAL_SECS));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tick.tick().await;

                // FIX: (*mgr.redis).clone() — clone Arc isi, bukan buka koneksi baru
                let mut conn = (*mgr.redis).clone();
                let mut pipe = redis::pipe();
                let mut count = 0usize;
                let mut total = 0usize;

                for entry in mgr.channels.iter() {
                    let uid = entry.key();
                    let role = if mgr.drivers.contains(uid.as_ref()) {
                        "driver"
                    } else if mgr.riders.contains(uid.as_ref()) {
                        "rider"
                    } else {
                        continue;
                    };

                    pipe.set_ex(format!("online:{}", uid), role, PRESENCE_TTL_SECS);
                    count += 1;
                    total += 1;

                    if count >= PRESENCE_BATCH_SIZE {
                        if let Err(e) = pipe
                            .query_async::<redis::aio::ConnectionManager, ()>(&mut conn)
                            .await
                        {
                            tracing::warn!("Presence batch flush failed: {}", e);
                        }
                        pipe = redis::pipe();
                        count = 0;
                    }
                }

                if count > 0 {
                    if let Err(e) = pipe
                        .query_async::<redis::aio::ConnectionManager, ()>(&mut conn)
                        .await
                    {
                        tracing::warn!("Presence final flush failed: {}", e);
                    }
                }

                if total > 0 {
                    tracing::debug!("Presence refreshed for {} users", total);
                }
            }
        });
    }

    // ── Connect / Disconnect ──────────────────────────────────────────────────

    pub async fn connect(
        &self,
        user_id: &str,
        role: &str,
    ) -> (
        Receiver<Result<ServerEvent, tonic::Status>>,
        CancellationToken,
    ) {
        let (tx, rx) = mpsc::channel(CHANNEL_BUFFER);
        let key: Arc<str> = user_id.into();
        self.channels.insert(key.clone(), tx);

        // Jika user reconnect dengan role berbeda, bersihkan set lama dulu
        match role {
            "driver" => {
                self.riders.remove(key.as_ref());
                self.drivers.insert(key);
            }
            "rider" => {
                self.drivers.remove(key.as_ref());
                self.riders.insert(key);
            }
            _ => tracing::warn!(user_id, role, "Unknown role in connect()"),
        }

        // NX: set only if not exists — cegah ghost overwrite saat race reconnect
        let mut redis = (*self.redis).clone();
        let _: Result<(), _> = redis
            .set_options(
                format!("online:{}", user_id),
                role,
                redis::SetOptions::default()
                    .with_expiration(redis::SetExpiry::EX(PRESENCE_TTL_SECS as usize))
                    .conditional_set(redis::ExistenceCheck::NX),
            )
            .await;

        (rx, CancellationToken::new())
    }

    pub async fn disconnect(&self, user_id: &str, cancel: CancellationToken) {
        cancel.cancel();

        self.channels.remove(user_id);
        self.drivers.remove(user_id);
        self.riders.remove(user_id);

        let mut redis = (*self.redis).clone();
        let _: Result<(), _> = redis.del(format!("online:{}", user_id)).await;
    }

    // ── Register ──────────────────────────────────────────────────────────────
    //
    // FIX: register_* hanya update set lokal — tidak menyentuh Redis.
    // Alasan:
    //   1. connect() sudah set Redis presence dengan NX.
    //   2. presence_refresher akan refresh TTL setiap 55 detik.
    //   3. set_ex di sini akan overwrite key NX yang baru saja di-set di connect(),
    //      yang bisa menyebabkan race condition di multi-node.
    //
    // Jika ada skenario register dipanggil tanpa connect() sebelumnya
    // (misal: pre-register sebelum WebSocket tersambung), tambahkan Redis call
    // di sini secara eksplisit dengan komentar yang jelas.

    // ── Send ──────────────────────────────────────────────────────────────────

    pub fn send(&self, user_id: &str, event: ArcEvent, priority: Priority) {
        // Local fast-path: tidak encode sama sekali kalau user ada di node ini
        if let Some(tx) = self.channels.get(user_id) {
            match tx.try_send(Ok((*event).clone())) {
                Ok(_) => return,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(user_id, "Channel full, forcing disconnect");
                    // FIX: hapus try_send error — channel sudah penuh, hampir pasti gagal.
                    // Langsung drop dan increment metric, client akan reconnect.
                    drop(tx);
                    self.channels.remove(user_id);
                    self.dropped_events.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    drop(tx);
                    self.channels.remove(user_id);
                    // Fallthrough ke Redis: user mungkin sudah reconnect ke node lain
                }
            }
        }

        // Remote path: encode hanya di sini
        let bytes: ArcBytes = event.encode_to_vec().into();
        let workers = self.pub_workers.clone();
        let uid = user_id.to_string();

        match priority {
            Priority::Normal => {
                workers.normal.send_normal(
                    &uid,
                    format!("evt:{}", uid),
                    bytes,
                    &self.dropped_events,
                );
            }
            Priority::Critical => {
                self.critical_queued.fetch_add(1, Ordering::Relaxed);
                let sem = self.spawn_sem.clone();
                let cq = Arc::clone(&self.critical_queued);
                tokio::spawn(async move {
                    let Ok(_permit) = sem.acquire_owned().await else {
                        return;
                    };
                    workers
                        .critical
                        .send_critical(&uid, format!("evt:{}", uid), bytes)
                        .await;
                    cq.fetch_sub(1, Ordering::Relaxed);
                });
            }
        }
    }

    // ── Send to Drivers ───────────────────────────────────────────────────────
    //
    // FIX: remote fanout sekarang concurrent dengan for_each_concurrent(FANOUT_CONCURRENCY).
    // Sebelumnya: sequential await per driver = total_latency = N × redis_latency.
    // Sekarang:   32 publish jalan paralel = total_latency ≈ ceil(N/32) × redis_latency.
    // Contoh: 1000 driver, 1ms/publish → sebelum: 1000ms, sekarang: ~32ms.

    pub fn send_to_drivers(&self, driver_ids: Vec<String>, event: ArcEvent, priority: Priority) {
        let mut remote_ids: Vec<String> = Vec::new();

        for driver_id in &driver_ids {
            if let Some(tx) = self.channels.get(driver_id.as_str()) {
                if tx.try_send(Ok((*event).clone())).is_ok() {
                    continue;
                }
                drop(tx);
                self.channels.remove(driver_id.as_str());
            }
            remote_ids.push(driver_id.clone());
        }

        if remote_ids.is_empty() {
            return;
        }

        let bytes: ArcBytes = event.encode_to_vec().into();
        let workers = self.pub_workers.clone();
        let sem = self.spawn_sem.clone();

        tokio::spawn(async move {
            let Ok(_permit) = sem.acquire_owned().await else {
                return;
            };

            // FIX: concurrent fanout — FANOUT_CONCURRENCY publish jalan bersamaan
            stream::iter(remote_ids)
                .for_each_concurrent(FANOUT_CONCURRENCY, |driver_id| {
                    let w = workers.clone();
                    let b = Arc::clone(&bytes);
                    async move {
                        w.publish(&driver_id, b, priority).await;
                    }
                })
                .await;
        });
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    pub async fn is_connected(&self, user_id: &str) -> bool {
        if self.channels.contains_key(user_id) {
            return true;
        }
        let mut redis = (*self.redis).clone();
        redis
            .exists::<_, bool>(format!("online:{}", user_id))
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

    // ── Metrics ───────────────────────────────────────────────────────────────

    /// Total event yang di-drop: channel penuh, queue overflow, atau decode pool penuh
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    /// Total event critical yang sedang di-queue ke Redis (inflight)
    pub fn critical_queued(&self) -> u64 {
        self.critical_queued.load(Ordering::Relaxed)
    }

    /// (total_connections, connections_with_low_buffer)
    pub fn queue_depths(&self) -> (usize, usize) {
        let low = self
            .channels
            .iter()
            .filter(|e| e.value().capacity() < CHANNEL_BUFFER / 4)
            .count();
        (self.channels.len(), low)
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    pub fn spawn_cleanup(self: &Arc<Self>) {
        let mgr = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(CLEANUP_INTERVAL_SECS));
            loop {
                tick.tick().await;

                let stale: Vec<Arc<str>> = mgr
                    .channels
                    .iter()
                    .filter(|e| e.value().is_closed())
                    .map(|e| e.key().clone())
                    .collect();

                if stale.is_empty() {
                    continue;
                }

                let mut conn = (*mgr.redis).clone();
                let keys: Vec<String> = stale.iter().map(|uid| format!("online:{}", uid)).collect();
                if let Err(e) = conn.del::<_, ()>(keys).await {
                    tracing::warn!("Cleanup Redis del failed: {}", e);
                }

                for uid in &stale {
                    mgr.channels.remove(uid.as_ref());
                    mgr.drivers.remove(uid.as_ref());
                    mgr.riders.remove(uid.as_ref());
                }

                tracing::info!("Cleanup: removed {} stale connections", stale.len());
            }
        });
    }
}

// ── Standalone helpers ────────────────────────────────────────────────────────

fn shard_index(user_id: &str, n: usize) -> usize {
    let mut hasher = AHasher::default();
    user_id.hash(&mut hasher);
    (hasher.finish() as usize) % n
}

async fn publish_with_retry(conn: &mut redis::aio::ConnectionManager, channel: &str, bytes: &[u8]) {
    for attempt in 0..REDIS_PUBLISH_RETRIES {
        match conn.publish::<_, _, ()>(channel, bytes).await {
            Ok(_) => return,
            Err(e) if attempt + 1 == REDIS_PUBLISH_RETRIES => {
                tracing::error!(
                    channel,
                    "Redis publish failed after {} retries: {}",
                    REDIS_PUBLISH_RETRIES,
                    e
                );
            }
            Err(e) => {
                let base_ms = 20u64 * (1 << attempt);
                let jitter = rand_jitter();
                tracing::warn!(channel, attempt, "Redis publish retry: {}", e);
                tokio::time::sleep(Duration::from_millis(base_ms + jitter)).await;
            }
        }
    }
}
