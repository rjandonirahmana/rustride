// ===== throttle.rs =====
// Pragmatic per-user per-event throttle menggunakan DashMap.
//
// Tidak butuh background cleanup — entry lama otomatis tertimpa saat
// user aktif kembali. Memory overhead: ~100 bytes per (user, event) pair,
// acceptable untuk ribuan concurrent users.
//
// Untuk production scale (100k+), ganti dengan token bucket di Redis.

use dashmap::DashMap;
use std::time::{Duration, Instant};

pub struct ThrottleMap {
    // Key: (user_id, event_key) → last_allowed_at
    map: DashMap<(String, &'static str), Instant>,
}

impl Default for ThrottleMap {
    fn default() -> Self {
        Self {
            map: DashMap::with_capacity(10_000),
        }
    }
}

impl ThrottleMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return true jika event boleh diproses (belum terlalu frequent).
    /// min_interval: minimum jeda antara dua event yang sama dari user yang sama.
    pub fn allow(&self, user_id: &str, event_key: &'static str, min_interval: Duration) -> bool {
        let key = (user_id.to_string(), event_key);
        let now = Instant::now();

        match self.map.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                if now.duration_since(*e.get()) >= min_interval {
                    *e.get_mut() = now;
                    true
                } else {
                    false
                }
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                e.insert(now);
                true
            }
        }
    }
}

// ── Per-event throttle intervals ─────────────────────────────────────────────

/// Heartbeat lokasi — tidak perlu lebih sering dari 1x/detik.
/// Client biasanya kirim tiap 2-3 detik, ini safety net.
pub const THROTTLE_LOCATION: Duration = Duration::from_millis(800);

/// WatchDrivers — polling sekitar, cukup 2x/detik.
pub const THROTTLE_WATCH_DRIVERS: Duration = Duration::from_millis(500);

/// Ping — anti-spam, 1x/5 detik sudah lebih dari cukup.
pub const THROTTLE_PING: Duration = Duration::from_secs(5);

/// BrowseOrders — DB query, jangan terlalu frequent.
pub const THROTTLE_BROWSE_ORDERS: Duration = Duration::from_millis(500);
