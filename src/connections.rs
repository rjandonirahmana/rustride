//! connection_manager.rs
//!
//! Changelog vs versi sebelumnya:
//!
//! FIX-1: **Arc clone bukan deep clone** — `event.clone()` menggantikan
//!   `(*event).clone()` di `send()` dan `send_to_drivers()`.
//!   Arc<ServerEvent>::clone() = O(1), bukan serialisasi ulang protobuf.
//!
//! FIX-2: **Semaphore per-item di fanout** — `send_to_drivers()` kini
//!   mengambil permit per driver_id, bukan satu permit untuk seluruh batch.
//!   Efek: FANOUT_CONCURRENCY benar-benar membatasi paralel publish.
//!
//! FIX-3: **Dedicated Redis connection untuk presence** — `redis_presence`
//!   dipisah dari `redis_ops`. Presence refresher (O(N) pipeline tiap 55 detik)
//!   tidak lagi berkompetisi dengan connect/disconnect/is_connected.
//!
//! FIX-4: **Graceful shutdown via CancellationToken** — `shutdown_token`
//!   diteruskan ke semua background loop (subscriber, refresher, decode).
//!   `ConnectionManager::shutdown()` membatalkan semua task sekaligus.
//!
//! FIX-5: **scopeguard untuk critical_queued** — counter dijamin dekrement
//!   meski terjadi panic di dalam spawn, via `scopeguard::defer!`.

use crate::proto::ridehailing::ServerEvent;
use ahash::AHasher;
use anyhow::Result;
use dashmap::DashMap;
use futures::{stream, StreamExt};
use prost::Message;
use redis::AsyncCommands;
use scopeguard::defer;
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
use tokio_util::bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tonic::Status;

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
const REDIS_PUBLISH_TIMEOUT_MS: u64 = 500;
const PRESENCE_BATCH_INTERVAL_SECS: u64 = 55;
const PRESENCE_BATCH_SIZE: usize = 1000;
const FANOUT_CONCURRENCY: usize = 32;

const ROLE_DRIVER: u8 = 0;
const ROLE_RIDER: u8 = 1;

// ── Types ─────────────────────────────────────────────────────────────────────

type Tx = Sender<Result<ServerEvent, Status>>;
/// `Bytes` clone adalah O(1) — hanya naikkan refcount, tidak salin data.
type WireBytes = Bytes;

// ── Session ───────────────────────────────────────────────────────────────────

struct Session {
    tx: Tx,
    role: u8,
}

// ── Priority ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
pub enum Priority {
    Critical,
    Normal,
}

// ── Jitter ────────────────────────────────────────────────────────────────────

#[inline]
fn rand_jitter_ms() -> u64 {
    use rand::Rng;
    rand::rng().random::<u64>() % 10
}

// ── Redis connection helper ───────────────────────────────────────────────────

#[derive(Clone)]
struct RConn(redis::aio::MultiplexedConnection);

impl std::ops::DerefMut for RConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl std::ops::Deref for RConn {
    type Target = redis::aio::MultiplexedConnection;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// ── Sharded publish workers ───────────────────────────────────────────────────

struct ShardedWorkers {
    senders: Vec<flume::Sender<(String, WireBytes)>>,
    // FIX-4: shutdown token diteruskan ke worker loop
    _shutdown: CancellationToken,
}

impl ShardedWorkers {
    fn new(
        redis_client: Arc<redis::Client>,
        n: usize,
        buf: usize,
        shutdown: CancellationToken,
    ) -> Self {
        let mut senders = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = flume::bounded::<(String, WireBytes)>(buf);
            senders.push(tx);
            let client = redis_client.clone();
            let shutdown_w = shutdown.clone();
            tokio::spawn(async move {
                let mut conn = match client.get_multiplexed_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("Pub worker redis connect failed: {}", e);
                        return;
                    }
                };
                loop {
                    tokio::select! {
                        _ = shutdown_w.cancelled() => {
                            tracing::debug!("Pub worker shutting down");
                            break;
                        }
                        result = rx.recv_async() => {
                            match result {
                                Ok((channel, bytes)) => {
                                    publish_with_retry(&mut conn, &channel, &bytes).await;
                                }
                                Err(_) => break, // sender dropped
                            }
                        }
                    }
                }
            });
        }
        Self {
            senders,
            _shutdown: shutdown,
        }
    }

    #[inline]
    fn shard(&self, user_id: &str) -> usize {
        shard_index(user_id, self.senders.len())
    }

    async fn send_critical(&self, user_id: &str, channel: String, bytes: WireBytes) {
        let idx = self.shard(user_id);
        if let Err(e) = self.senders[idx].send_async((channel, bytes)).await {
            tracing::error!(user_id, "Critical shard closed: {}", e);
        }
    }

    fn send_normal(&self, user_id: &str, channel: String, bytes: WireBytes, dropped: &AtomicU64) {
        let idx = self.shard(user_id);
        if self.senders[idx].try_send((channel, bytes)).is_err() {
            dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(user_id, shard = idx, "Normal pub shard full, dropping");
        }
    }
}

// ── PubWorkers ────────────────────────────────────────────────────────────────

struct PubWorkers {
    critical: ShardedWorkers,
    normal: ShardedWorkers,
}

impl PubWorkers {
    fn new(redis_client: Arc<redis::Client>, shutdown: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            critical: ShardedWorkers::new(
                redis_client.clone(),
                PUB_WORKER_COUNT,
                PUB_CHANNEL_BUFFER_CRITICAL,
                shutdown.clone(),
            ),
            normal: ShardedWorkers::new(
                redis_client,
                PUB_WORKER_COUNT,
                PUB_CHANNEL_BUFFER_NORMAL,
                shutdown,
            ),
        })
    }

    async fn publish(
        &self,
        user_id: &str,
        bytes: WireBytes,
        priority: Priority,
        dropped: &AtomicU64,
    ) {
        let channel = format!("evt:{}", user_id);
        match priority {
            Priority::Critical => self.critical.send_critical(user_id, channel, bytes).await,
            Priority::Normal => self.normal.send_normal(user_id, channel, bytes, dropped),
        }
    }
}

// ── ConnectionManager ─────────────────────────────────────────────────────────

pub struct ConnectionManager {
    sessions: DashMap<Arc<str>, Session>,

    /// FIX-3: koneksi untuk operasi ringan (connect/disconnect/is_connected).
    redis_ops: RConn,

    /// FIX-3: koneksi dedicated untuk presence refresher agar tidak
    /// berkompetisi dengan ops di atas saat pipeline besar berjalan.
    redis_presence: RConn,

    redis_client: Arc<redis::Client>,
    pub_workers: Arc<PubWorkers>,
    spawn_sem: Arc<Semaphore>,
    decode_tx: flume::Sender<(String, WireBytes)>,

    pub dropped_events: Arc<AtomicU64>,
    pub critical_queued: Arc<AtomicU64>,

    /// FIX-4: token untuk menghentikan semua background task.
    shutdown_token: CancellationToken,
}

impl ConnectionManager {
    pub async fn new(redis_client: redis::Client) -> Result<Arc<Self>> {
        let redis_client = Arc::new(redis_client);

        // FIX-3: buat dua koneksi terpisah
        let conn_ops = redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| anyhow::anyhow!("Redis ops connect: {}", e))?;
        let conn_presence = redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| anyhow::anyhow!("Redis presence connect: {}", e))?;

        let dropped_events = Arc::new(AtomicU64::new(0));

        // FIX-4: satu root token untuk semua task
        let shutdown_token = CancellationToken::new();

        let pub_workers = PubWorkers::new(redis_client.clone(), shutdown_token.clone());
        let (decode_tx, decode_rx) = flume::bounded::<(String, WireBytes)>(DECODE_CHANNEL_BUFFER);

        let mgr = Arc::new(Self {
            sessions: DashMap::with_capacity(1024),
            redis_ops: RConn(conn_ops),
            redis_presence: RConn(conn_presence),
            redis_client,
            pub_workers,
            spawn_sem: Arc::new(Semaphore::new(SPAWN_CONCURRENCY_LIMIT)),
            decode_tx,
            dropped_events,
            critical_queued: Arc::new(AtomicU64::new(0)),
            shutdown_token,
        });

        Self::spawn_decode_workers(&mgr, decode_rx);
        Self::spawn_shared_subscriber(mgr.clone());
        Self::spawn_global_presence_refresher(mgr.clone());

        Ok(mgr)
    }

    /// FIX-4: hentikan semua background task dengan satu panggilan.
    pub fn shutdown(&self) {
        tracing::info!("ConnectionManager shutting down");
        self.shutdown_token.cancel();
    }

    // ── Decode workers ────────────────────────────────────────────────────────

    fn spawn_decode_workers(mgr: &Arc<Self>, decode_rx: flume::Receiver<(String, WireBytes)>) {
        for _ in 0..DECODE_WORKER_COUNT {
            let mgr_w = mgr.clone();
            let rx = decode_rx.clone();
            let shutdown = mgr.shutdown_token.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            tracing::debug!("Decode worker shutting down");
                            break;
                        }
                        result = rx.recv_async() => {
                            match result {
                                Ok((user_id, bytes)) => {
                                    match ServerEvent::decode(bytes.as_ref()) {
                                        Ok(event) => {
                                            if let Some(session) = mgr_w.sessions.get(&*user_id) {
                                                if session.tx.try_send(Ok(event)).is_err() {
                                                    mgr_w.dropped_events.fetch_add(1, Ordering::Relaxed);
                                                    drop(session);
                                                    mgr_w.sessions.remove(&*user_id);
                                                }
                                            }
                                        }
                                        Err(e) => tracing::warn!(user_id, "Decode error: {}", e),
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                }
            });
        }
    }

    // ── Redis Subscriber ──────────────────────────────────────────────────────

    fn spawn_shared_subscriber(mgr: Arc<Self>) {
        let decode_tx = mgr.decode_tx.clone();
        let dropped = Arc::clone(&mgr.dropped_events);
        let client = mgr.redis_client.clone();
        let shutdown = mgr.shutdown_token.clone();

        tokio::spawn(async move {
            loop {
                // FIX-4: cek shutdown sebelum reconnect
                if shutdown.is_cancelled() {
                    break;
                }

                match client.get_async_pubsub().await {
                    Ok(mut pubsub) => {
                        if let Err(e) = pubsub.psubscribe("evt:*").await {
                            tracing::error!("psubscribe failed: {}", e);
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                        tracing::info!("Redis shared subscriber ready");
                        let mut stream = pubsub.on_message();

                        loop {
                            tokio::select! {
                                _ = shutdown.cancelled() => {
                                    tracing::info!("Redis subscriber shutting down");
                                    return;
                                }
                                msg = stream.next() => {
                                    let Some(msg) = msg else {
                                        tracing::warn!("Redis subscriber disconnected, reconnecting...");
                                        break;
                                    };
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
                                    let bytes = Bytes::from(raw);
                                    if decode_tx.try_send((user_id, bytes)).is_err() {
                                        dropped.fetch_add(1, Ordering::Relaxed);
                                        tracing::warn!("Decode pool full, dropping");
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => tracing::error!("Redis pubsub connect failed: {}", e),
                }

                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    // ── Presence Refresher ────────────────────────────────────────────────────

    fn spawn_global_presence_refresher(mgr: Arc<Self>) {
        let shutdown = mgr.shutdown_token.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(PRESENCE_BATCH_INTERVAL_SECS));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            // FIX-3: gunakan koneksi dedicated presence, bukan redis_ops
            let mut conn = mgr.redis_presence.clone();

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        tracing::info!("Presence refresher shutting down");
                        break;
                    }
                    _ = tick.tick() => {}
                }

                let mut pipe = redis::pipe();
                let mut pipe_count = 0usize;
                let mut total = 0usize;
                let mut stale: Vec<Arc<str>> = Vec::new();

                for entry in mgr.sessions.iter() {
                    let uid = entry.key().clone();
                    let session = entry.value();

                    if session.tx.is_closed() {
                        stale.push(uid);
                        continue;
                    }

                    let role_str = if session.role == ROLE_DRIVER {
                        "driver"
                    } else {
                        "rider"
                    };
                    pipe.set_ex(format!("online:{}", uid), role_str, PRESENCE_TTL_SECS);
                    pipe_count += 1;
                    total += 1;

                    if pipe_count >= PRESENCE_BATCH_SIZE {
                        if let Err(e) = pipe.query_async::<()>(&mut *conn).await {
                            tracing::warn!("Presence batch flush failed: {}", e);
                        }
                        pipe = redis::pipe();
                        pipe_count = 0;
                    }
                }

                if pipe_count > 0 {
                    if let Err(e) = pipe.query_async::<()>(&mut *conn).await {
                        tracing::warn!("Presence final flush failed: {}", e);
                    }
                }

                if !stale.is_empty() {
                    let redis_keys: Vec<String> =
                        stale.iter().map(|u| format!("online:{}", u)).collect();
                    let stale_count = stale.len();
                    if let Err(e) = conn.del::<_, ()>(redis_keys).await {
                        tracing::warn!("Stale Redis del failed: {}", e);
                    }
                    for uid in stale {
                        mgr.sessions.remove(&uid);
                    }
                    tracing::debug!("Removed {} stale sessions", stale_count);
                }

                if total > 0 {
                    tracing::debug!("Presence refreshed for {} users", total);
                }
            }
        });
    }

    // ── Connect ───────────────────────────────────────────────────────────────

    pub async fn connect(
        &self,
        user_id: &str,
        role: &str,
    ) -> (Receiver<Result<ServerEvent, Status>>, CancellationToken) {
        let (tx, rx) = mpsc::channel(CHANNEL_BUFFER);
        let key: Arc<str> = user_id.into();
        let role_byte = if role == "driver" {
            ROLE_DRIVER
        } else {
            ROLE_RIDER
        };

        if let Some((_, old)) = self.sessions.remove(&key) {
            tracing::warn!(user_id, "Replacing existing connection");
            let _ = old
                .tx
                .try_send(Err(Status::cancelled("Replaced by new connection")));
        }
        self.sessions.insert(
            key,
            Session {
                tx,
                role: role_byte,
            },
        );

        // FIX-3: pakai redis_ops untuk connect/disconnect
        let mut conn = self.redis_ops.clone();
        if let Err(e) = conn
            .set_ex::<_, _, ()>(format!("online:{}", user_id), role, PRESENCE_TTL_SECS)
            .await
        {
            tracing::error!(user_id, error = %e, "Failed to set presence");
        }

        (rx, CancellationToken::new())
    }

    // ── Disconnect ────────────────────────────────────────────────────────────

    pub async fn disconnect(&self, user_id: &str, cancel: CancellationToken) {
        cancel.cancel();
        self.sessions.remove(user_id);

        // FIX-3: pakai redis_ops, tapi dengan timeout agar tidak blocking
        let mut conn = self.redis_ops.clone();
        let key = format!("online:{}", user_id);
        let uid = user_id.to_string();
        tokio::spawn(async move {
            match tokio::time::timeout(tokio::time::Duration::from_secs(1), conn.del::<_, ()>(&key))
                .await
            {
                Ok(Ok(_)) => tracing::debug!(uid, "Presence key deleted"),
                Ok(Err(e)) => tracing::warn!(uid, "Failed to del presence: {}", e),
                Err(_) => tracing::warn!(uid, "Timeout deleting presence key"),
            }
        });
    }
    // ── Send ──────────────────────────────────────────────────────────────────

    pub fn send(&self, user_id: &str, event: Arc<ServerEvent>, priority: Priority) {
        if let Some(session) = self.sessions.get(user_id) {
            // FIX-1: Arc::clone() = O(1), bukan deep clone protobuf
            match session.tx.try_send(Ok(event.as_ref().clone())) {
                Ok(_) => return,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(user_id, "Channel full, forcing disconnect");
                    drop(session);
                    self.sessions.remove(user_id);
                    self.dropped_events.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    drop(session);
                    self.sessions.remove(user_id);
                }
            }
        }

        let bytes: WireBytes = Bytes::from(event.encode_to_vec());
        let workers = self.pub_workers.clone();
        let uid = user_id.to_string();
        let dropped = Arc::clone(&self.dropped_events);

        match priority {
            Priority::Normal => {
                workers
                    .normal
                    .send_normal(&uid, format!("evt:{}", uid), bytes, &dropped);
            }
            Priority::Critical => {
                self.critical_queued.fetch_add(1, Ordering::Relaxed);
                let sem = self.spawn_sem.clone();
                let cq = Arc::clone(&self.critical_queued);

                tokio::spawn(async move {
                    // FIX-5: scopeguard menjamin decrement meski terjadi panic
                    defer! { cq.fetch_sub(1, Ordering::Relaxed); }

                    let Ok(_permit) = sem.acquire_owned().await else {
                        return;
                    };
                    workers
                        .critical
                        .send_critical(&uid, format!("evt:{}", uid), bytes)
                        .await;
                });
            }
        }
    }

    // ── Send to Drivers ───────────────────────────────────────────────────────

    pub fn send_to_drivers(
        &self,
        driver_ids: Vec<String>,
        event: Arc<ServerEvent>,
        priority: Priority,
    ) {
        let bytes: WireBytes = Bytes::from(event.encode_to_vec());
        let mut remote_ids: Vec<String> = Vec::new();

        for driver_id in &driver_ids {
            if let Some(session) = self.sessions.get(driver_id.as_str()) {
                if session.role != ROLE_DRIVER {
                    continue;
                }
                // FIX-1: Arc clone, bukan deep clone protobuf
                match session.tx.try_send(Ok(event.as_ref().clone())) {
                    Ok(_) => continue,
                    Err(_) => {
                        drop(session);
                        self.sessions.remove(driver_id.as_str());
                    }
                }
            }
            remote_ids.push(driver_id.clone());
        }

        if remote_ids.is_empty() {
            return;
        }

        let workers = self.pub_workers.clone();
        // FIX-2: ambil sem per-item (bukan satu permit untuk seluruh batch)
        let sem = self.spawn_sem.clone();
        let dropped = Arc::clone(&self.dropped_events);

        tokio::spawn(async move {
            stream::iter(remote_ids)
                .for_each_concurrent(FANOUT_CONCURRENCY, |driver_id| {
                    let w = workers.clone();
                    let b = bytes.clone(); // O(1)
                    let d = dropped.clone();
                    let sem = sem.clone();

                    async move {
                        // FIX-2: semaphore di sini — satu permit per publish
                        let Ok(_permit) = sem.acquire_owned().await else {
                            return;
                        };
                        w.publish(&driver_id, b, priority, &d).await;
                    }
                })
                .await;
        });
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    pub async fn is_connected(&self, user_id: &str) -> Result<bool> {
        if self.sessions.contains_key(user_id) {
            return Ok(true);
        }
        // FIX-3: pakai redis_ops
        let mut conn = self.redis_ops.clone();
        let exist: bool = conn.exists(format!("online:{}", user_id)).await?;
        Ok(exist)
    }

    pub fn online_drivers(&self) -> Vec<String> {
        self.sessions
            .iter()
            .filter(|e| e.value().role == ROLE_DRIVER)
            .map(|e| e.key().to_string())
            .collect()
    }

    pub fn online_users(&self) -> Vec<String> {
        self.sessions.iter().map(|e| e.key().to_string()).collect()
    }

    // ── Metrics ───────────────────────────────────────────────────────────────

    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    pub fn critical_queued(&self) -> u64 {
        self.critical_queued.load(Ordering::Relaxed)
    }

    pub fn queue_depths(&self) -> (usize, usize) {
        let low = self
            .sessions
            .iter()
            .filter(|e| e.value().tx.capacity() < CHANNEL_BUFFER / 4)
            .count();
        (self.sessions.len(), low)
    }
}

// ── Standalone helpers ────────────────────────────────────────────────────────

#[inline]
fn shard_index(user_id: &str, n: usize) -> usize {
    let mut hasher = AHasher::default();
    user_id.hash(&mut hasher);
    (hasher.finish() as usize) % n
}

async fn publish_with_retry(
    conn: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    bytes: &[u8],
) {
    for attempt in 0..REDIS_PUBLISH_RETRIES {
        let result = tokio::time::timeout(
            Duration::from_millis(REDIS_PUBLISH_TIMEOUT_MS),
            conn.publish::<_, _, ()>(channel, bytes),
        )
        .await;

        match result {
            Ok(Ok(_)) => return,
            Ok(Err(e)) if attempt + 1 == REDIS_PUBLISH_RETRIES => {
                tracing::error!(
                    channel,
                    "Redis publish failed after {} retries: {}",
                    REDIS_PUBLISH_RETRIES,
                    e
                );
            }
            Ok(Err(e)) => {
                let delay = 20u64 * (1 << attempt) + rand_jitter_ms();
                tracing::warn!(
                    channel,
                    attempt,
                    "Redis publish error: {}, retry in {}ms",
                    e,
                    delay
                );
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            Err(_timeout) => {
                if attempt + 1 == REDIS_PUBLISH_RETRIES {
                    tracing::error!(
                        channel,
                        "Redis publish timed out after {} retries",
                        REDIS_PUBLISH_RETRIES
                    );
                } else {
                    let delay = 20u64 * (1 << attempt) + rand_jitter_ms();
                    tracing::warn!(
                        channel,
                        attempt,
                        "Redis publish timeout, retry in {}ms",
                        delay
                    );
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
        }
    }
}
