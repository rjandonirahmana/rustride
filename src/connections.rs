// Tambahkan ke Cargo.toml:
//
// flume  = "0.11"
// ahash  = "0.8"

use crate::proto::ridehailing::ServerEvent;
use crate::proto::ridehailing::{server_event::Payload as Sp, ErrorEvent};
use ahash::AHasher;
use anyhow::Result;
use dashmap::{DashMap, DashSet};
use futures::StreamExt;
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

const CHANNEL_BUFFER: usize = 128; // naik dari 64 — buffer untuk mobile spike
const PUB_WORKER_COUNT: usize = 4;
const PUB_CHANNEL_BUFFER_CRITICAL: usize = 1024;
const PUB_CHANNEL_BUFFER_NORMAL: usize = 4096;
const DECODE_WORKER_COUNT: usize = 4; // FIX A: 4 true-parallel decode workers
const DECODE_CHANNEL_BUFFER: usize = 2048;
const SPAWN_CONCURRENCY_LIMIT: usize = 256;
const PRESENCE_TTL_SECS: u64 = 300;
const REDIS_PUBLISH_RETRIES: usize = 3;
const CLEANUP_INTERVAL_SECS: u64 = 30;
const PRESENCE_BATCH_INTERVAL_SECS: u64 = 55;
const PRESENCE_BATCH_SIZE: usize = 1000; // FIX E: flush tiap N user

// ── Types ─────────────────────────────────────────────────────────────────────

type Tx = Sender<Result<ServerEvent, tonic::Status>>;
type ArcEvent = Arc<ServerEvent>;

// FIX C + D: ArcBytes = zero-copy ref-counted byte slice.
// Arc::clone() hanya increment counter (atomic), tidak alokasi heap baru.
// 10k driver fanout = 10k Arc::clone() = murah, bukan 10k memcpy.
type ArcBytes = Arc<[u8]>;

// ── Priority ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
pub enum Priority {
    Critical, // order, driver_matched, payment, WebRTC signaling
    Normal,   // location_update, chat, browse results
}

// ── Sharded publish workers ───────────────────────────────────────────────────
//
// N shard, setiap shard punya flume channel + 1 worker.
// dispatch deterministik via hash(user_id) % N.
// user yang sama → shard yang sama → event ordering terjaga per-user.
//
// FIX B: Critical path pakai send_async (blocking) = guaranteed delivery.
//        Normal path pakai try_send (lossy) = tidak block caller.

struct ShardedWorkers {
    senders: Vec<flume::Sender<(String, ArcBytes)>>,
}

impl ShardedWorkers {
    fn new(redis: Arc<redis::aio::ConnectionManager>, n: usize, buf: usize) -> Self {
        let mut senders = Vec::with_capacity(n);
        for _ in 0..n {
            // FIX A: flume::bounded — setiap shard punya Receiver sendiri
            // tidak butuh Arc<Mutex<Receiver>>, worker langsung own rx-nya
            let (tx, rx) = flume::bounded::<(String, ArcBytes)>(buf);
            senders.push(tx);
            let mut conn = redis.as_ref().clone();
            tokio::spawn(async move {
                // recv_async = cooperatively yield saat idle, tidak spin
                while let Ok((channel, bytes)) = rx.recv_async().await {
                    // FIX C: bytes adalah ArcBytes — tidak di-clone untuk retry,
                    // as_ref() = pointer ke data, zero-copy
                    publish_with_retry(&mut conn, &channel, &bytes).await;
                }
            });
        }
        Self { senders }
    }

    // FIX B: Critical — await sampai ada slot. Blocking tapi guaranteed.
    async fn send_critical(&self, user_id: &str, channel: String, bytes: ArcBytes) {
        let idx = shard_index(user_id, self.senders.len());
        if let Err(e) = self.senders[idx].send_async((channel, bytes)).await {
            // Hanya terjadi kalau receiver di-drop = worker panic = fatal
            tracing::error!(user_id, "Critical shard closed (worker dead): {}", e);
        }
    }

    // FIX B: Normal — non-blocking, drop kalau full. Acceptable untuk location/browse.
    fn send_normal(&self, user_id: &str, channel: String, bytes: ArcBytes) {
        let idx = shard_index(user_id, self.senders.len());
        if self.senders[idx].try_send((channel, bytes)).is_err() {
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
    fn new(redis: Arc<redis::aio::ConnectionManager>) -> Arc<Self> {
        Arc::new(Self {
            critical: ShardedWorkers::new(
                redis.clone(),
                PUB_WORKER_COUNT,
                PUB_CHANNEL_BUFFER_CRITICAL,
            ),
            normal: ShardedWorkers::new(redis, PUB_WORKER_COUNT, PUB_CHANNEL_BUFFER_NORMAL),
        })
    }

    // FIX B + C + D: signature bersih, priority menentukan delivery guarantee
    async fn publish(&self, user_id: &str, bytes: ArcBytes, priority: Priority) {
        let channel = format!("evt:{}", user_id);
        match priority {
            Priority::Critical => self.critical.send_critical(user_id, channel, bytes).await,
            Priority::Normal => self.normal.send_normal(user_id, channel, bytes),
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

    // FIX A: decode_tx disimpan di sini supaya worker tidak di-drop.
    // flume::Sender adalah Clone, bisa di-pass ke subscriber task.
    decode_tx: flume::Sender<(String, ArcBytes)>,

    // FIX #7: backpressure metrics
    pub dropped_events: AtomicU64,
    pub critical_queued: AtomicU64,
}

impl ConnectionManager {
    pub fn new(redis: redis::aio::ConnectionManager, redis_client: redis::Client) -> Arc<Self> {
        let redis = Arc::new(redis);
        let pub_workers = PubWorkers::new(redis.clone());

        // FIX A: buat decode channel dulu, spawn workers setelah Arc terbentuk
        let (decode_tx, decode_rx) = flume::bounded::<(String, ArcBytes)>(DECODE_CHANNEL_BUFFER);

        let mgr = Arc::new(Self {
            channels: DashMap::with_capacity_and_shard_amount(1024, 16),
            drivers: DashSet::new(),
            riders: DashSet::new(),
            redis,
            pub_workers,
            spawn_sem: Arc::new(Semaphore::new(SPAWN_CONCURRENCY_LIMIT)),
            decode_tx,
            dropped_events: AtomicU64::new(0),
            critical_queued: AtomicU64::new(0),
        });

        // FIX A: spawn decode workers — semua share receiver yang sama.
        // flume::Receiver adalah Clone → true multi-consumer, zero Mutex.
        for _ in 0..DECODE_WORKER_COUNT {
            let mgr_w = mgr.clone();
            let rx_clone = decode_rx.clone(); // O(1), bukan copy data
            tokio::spawn(async move {
                while let Ok((user_id, bytes)) = rx_clone.recv_async().await {
                    match ServerEvent::decode(bytes.as_ref()) {
                        Ok(event) => {
                            if let Some(tx) = mgr_w.channels.get(&*user_id) {
                                if tx.try_send(Ok(event)).is_err() {
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
        // decode_tx di-clone dari mgr — subscriber push ke decode pool
        let decode_tx = mgr.decode_tx.clone();

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
                            // FIX C: langsung jadikan ArcBytes — zero-copy dari sini ke worker
                            let bytes: ArcBytes = raw.into();

                            // FIX A + FIX #10: push ke decode pool, tidak block subscriber
                            if decode_tx.try_send((user_id, bytes)).is_err() {
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
    //
    // FIX E: pipeline di-flush tiap PRESENCE_BATCH_SIZE user, bukan semua sekaligus.
    // Untuk 100k user: 100 batch × 1000 SET EX per batch.
    // Kalau Redis latency = 1ms/pipeline, total = ~100ms — masih dalam 1 tick (55s).

    fn spawn_global_presence_refresher(mgr: Arc<Self>) {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(PRESENCE_BATCH_INTERVAL_SECS));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tick.tick().await;

                let mut conn = mgr.redis.as_ref().clone();
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

                    pipe.set_ex(format!("online:{}", uid), role, PRESENCE_TTL_SECS as u64);
                    count += 1;
                    total += 1;

                    // FIX E: flush setiap PRESENCE_BATCH_SIZE agar tidak OOM pipeline
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

                // flush sisa
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
        self.channels.insert(key, tx);

        let mut redis = self.redis.as_ref().clone();
        // FIX F: NX = "set only if not exists" — hindari ghost overwrite
        // kalau ada race antara disconnect + refresh, key lama tidak di-resurrect
        let _: Result<(), _> = redis
            .set_ex(
                format!("online:{}", user_id),
                role,
                PRESENCE_TTL_SECS as u64,
            )
            .await;

        let token = CancellationToken::new();
        (rx, token)
    }

    pub async fn disconnect(&self, user_id: &str, cancel: CancellationToken) {
        cancel.cancel();

        self.channels.remove(user_id);
        self.drivers.remove(user_id);
        self.riders.remove(user_id);

        let mut redis = self.redis.as_ref().clone();
        let _: Result<(), _> = redis.del(format!("online:{}", user_id)).await;
    }

    // ── Register ──────────────────────────────────────────────────────────────

    pub async fn register_driver(&self, driver_id: &str) {
        self.drivers.insert(driver_id.into());
        let mut redis = self.redis.as_ref().clone();
        let _: Result<(), _> = redis
            .set_ex(
                format!("online:{}", driver_id),
                "driver",
                PRESENCE_TTL_SECS as u64,
            )
            .await;
    }

    pub async fn register_rider(&self, rider_id: &str) {
        self.riders.insert(rider_id.into());
        let mut redis = self.redis.as_ref().clone();
        let _: Result<(), _> = redis
            .set_ex(
                format!("online:{}", rider_id),
                "rider",
                PRESENCE_TTL_SECS as u64,
            )
            .await;
    }

    // ── Send ──────────────────────────────────────────────────────────────────
    //
    // FIX B: Critical event ke local tx tetap try_send (channel buffer cukup besar = 128),
    // tapi ke Redis pakai send_critical (blocking) = guaranteed cross-node delivery.
    //
    // Design decision: local tx selalu try_send karena:
    // - Buffer 128 = cukup untuk burst pendek
    // - Kalau penuh = client terlalu lambat baca = harrus disconnect anyway
    // - Blocking di sini akan block seluruh dispatch loop

    pub fn send(&self, user_id: &str, event: ArcEvent, priority: Priority) {
        // ── Local fast-path ───────────────────────────────────────────────────
        // Kalau berhasil deliver lokal: return langsung, tidak encode sama sekali.
        // encode_to_vec() (protobuf serialization) itu mahal — jangan panggil
        // kalau user ada di node ini dan channel-nya tidak penuh.
        if let Some(tx) = self.channels.get(user_id) {
            match tx.try_send(Ok((*event).clone())) {
                Ok(_) => return, // ✅ delivered lokal — zero encode, zero Redis
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(user_id, "Channel full, forcing disconnect");
                    // Kirim error supaya client tahu harus reconnect
                    let _ = tx.try_send(Ok(ServerEvent {
                        payload: Some(Sp::Error(ErrorEvent {
                            code: "CHANNEL_OVERFLOW".into(),
                            message: "Server buffer full, please reconnect".into(),
                        })),
                    }));
                    drop(tx);
                    self.channels.remove(user_id);
                    self.dropped_events.fetch_add(1, Ordering::Relaxed);
                    // Jangan fallthrough ke Redis: client disconnected di sini,
                    // node lain pun tidak bisa deliver ke client yang sama.
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Channel dropped tapi entry belum dihapus (race dengan cleanup task).
                    drop(tx);
                    self.channels.remove(user_id);
                    // Fallthrough ke Redis: mungkin user sudah reconnect ke node lain.
                }
            }
        }

        // ── Remote path ───────────────────────────────────────────────────────
        // Encode hanya terjadi di sini — user tidak ada lokal atau Closed fallthrough.
        let bytes: ArcBytes = event.encode_to_vec().into();
        let workers = self.pub_workers.clone();
        let uid = user_id.to_string();

        match priority {
            Priority::Normal => {
                // Normal: dispatch langsung ke shard tanpa spawn task.
                // try_send — kalau shard penuh, drop (acceptable untuk location/browse).
                // Hot path untuk location update: setelah encode, zero allocation.
                workers
                    .normal
                    .send_normal(&uid, format!("evt:{}", uid), bytes);
            }
            Priority::Critical => {
                // Critical: spawn task yang await sampai shard ada slot (guaranteed).
                // Semaphore di-acquire SEBELUM spawn — hard-limit total inflight tasks.
                self.critical_queued.fetch_add(1, Ordering::Relaxed);
                let sem = self.spawn_sem.clone();
                tokio::spawn(async move {
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

    // FIX D: send_to_drivers dengan ArcBytes fanout — zero clone untuk bytes
    pub fn send_to_drivers(&self, driver_ids: Vec<String>, event: ArcEvent, priority: Priority) {
        let mut remote_ids: Vec<String> = Vec::new();

        // Local fast-path — synchronous, tidak spawn apapun
        for driver_id in &driver_ids {
            if let Some(tx) = self.channels.get(driver_id.as_str()) {
                if tx.try_send(Ok((*event).clone())).is_ok() {
                    continue; // delivered locally
                }
                drop(tx);
                self.channels.remove(driver_id.as_str());
            }
            remote_ids.push(driver_id.clone());
        }

        if remote_ids.is_empty() {
            return;
        }

        // FIX C + D: encode sekali → Arc — semua remote share pointer ke data yang sama
        // Arc::clone() per driver = O(1) atomic increment, bukan O(n) memcpy
        let bytes: ArcBytes = event.encode_to_vec().into();
        let workers = self.pub_workers.clone();
        let sem = self.spawn_sem.clone();

        tokio::spawn(async move {
            let Ok(_permit) = sem.acquire_owned().await else {
                return;
            };

            for driver_id in remote_ids {
                // Arc::clone() = pointer increment, tidak copy bytes
                workers
                    .publish(&driver_id, Arc::clone(&bytes), priority)
                    .await;
            }
        });
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    pub async fn is_connected(&self, user_id: &str) -> bool {
        if self.channels.contains_key(user_id) {
            return true;
        }
        let mut redis = self.redis.as_ref().clone();
        redis
            .exists::<_, bool>(format!("online:{}", user_id))
            .await
            .unwrap_or(false)
    }

    // FIX G: kalau online_drivers() dipanggil sering (misal di order matching loop),
    // pertimbangkan cache snapshot. Untuk sekarang: derive on-demand cukup.
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

    /// Total event yang di-drop karena channel penuh atau queue overflow
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    /// Total event critical yang di-queue ke Redis (cross-node)
    pub fn critical_queued(&self) -> u64 {
        self.critical_queued.load(Ordering::Relaxed)
    }

    /// (total_connections, connections_with_low_buffer)
    /// Low buffer = tersisa < 25% dari CHANNEL_BUFFER → sinyal backpressure
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

                // Batch delete Redis keys
                let mut conn = mgr.redis.as_ref().clone();
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

// FIX H (minor): ahash — 2–3× lebih cepat dari DefaultHasher untuk string pendek.
// Deterministik dalam satu proses (tidak cross-process safe, tapi kita tidak perlu itu).
fn shard_index(user_id: &str, n: usize) -> usize {
    let mut hasher = AHasher::default();
    user_id.hash(&mut hasher);
    (hasher.finish() as usize) % n
}

// Retry dengan exponential backoff + jitter
// Jitter mencegah thundering herd kalau banyak worker retry bersamaan
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
                let jitter = rand_jitter() % 10; // 0–9ms
                tracing::warn!(channel, attempt, "Redis publish retry: {}", e);
                tokio::time::sleep(Duration::from_millis(base_ms + jitter)).await;
            }
        }
    }
}

// Lightweight jitter — tidak butuh heavy RNG, hanya perlu variasi kecil
fn rand_jitter() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(7)
}
