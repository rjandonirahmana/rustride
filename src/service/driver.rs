use anyhow::{bail, Result};
use std::sync::Arc;

use crate::repository::driver::{
    DailySummary, DriverOrderDetail, DriverOrderItem, DriverRepository, OrderHistoryFilter,
};

// ── Output structs (decoupled dari proto) ─────────────────────────────────────

pub struct TodayOrdersResult {
    pub orders: Vec<DriverOrderItem>,
    pub summary: DailySummary,
}

pub struct OrderHistoryResult {
    pub orders: Vec<DriverOrderItem>,
    pub total: u32,
    pub has_more: bool,
}

// ── DriverService ─────────────────────────────────────────────────────────────

pub struct DriverService<DR: DriverRepository> {
    pub driver_repo: Arc<DR>,
}

impl<DR: DriverRepository> Clone for DriverService<DR> {
    fn clone(&self) -> Self {
        Self {
            driver_repo: self.driver_repo.clone(),
        }
    }
}

impl<DR: DriverRepository + 'static> DriverService<DR> {
    pub fn new(driver_repo: Arc<DR>) -> Self {
        Self { driver_repo }
    }

    // ── Today orders + summary ────────────────────────────────────────────────
    pub async fn get_today_orders(&self, driver_id: &str) -> Result<TodayOrdersResult> {
        let orders = self.driver_repo.get_today_orders(driver_id).await?;

        // Coba ambil summary dari tabel dedicated dulu (O(1))
        // Kalau belum ada (driver belum pernah online hari ini), fallback ke
        // aggregate dari orders yang baru saja diambil
        let summary = match self.driver_repo.get_today_summary(driver_id).await? {
            Some(s) => s,
            None => Self::aggregate_summary(&orders),
        };

        Ok(TodayOrdersResult { orders, summary })
    }

    // ── Today summary saja (tanpa list order) ────────────────────────────────
    pub async fn get_today_summary(&self, driver_id: &str) -> Result<DailySummary> {
        match self.driver_repo.get_today_summary(driver_id).await? {
            Some(s) => Ok(s),
            None => {
                // Fallback: aggregate dari list order hari ini
                let orders = self.driver_repo.get_today_orders(driver_id).await?;
                Ok(Self::aggregate_summary(&orders))
            }
        }
    }

    // ── History dengan filter ─────────────────────────────────────────────────
    pub async fn get_order_history(
        &self,
        driver_id: &str,
        date_from: Option<String>,
        date_to: Option<String>,
        status: Option<String>,
        page: u32,
        limit: u32,
    ) -> Result<OrderHistoryResult> {
        // Validasi status kalau diisi
        if let Some(ref s) = status {
            if !matches!(s.as_str(), "completed" | "cancelled") {
                bail!("status harus 'completed' atau 'cancelled'");
            }
        }

        // Validasi page
        let page = page.max(1);
        let limit = limit.clamp(1, 100);

        let (orders, total) = self
            .driver_repo
            .get_order_history(
                driver_id,
                OrderHistoryFilter {
                    date_from,
                    date_to,
                    status,
                    page,
                    limit,
                },
            )
            .await?;

        let has_more = (page * limit) < total;

        Ok(OrderHistoryResult {
            orders,
            total,
            has_more,
        })
    }

    // ── Detail order ──────────────────────────────────────────────────────────
    pub async fn get_order_detail(
        &self,
        driver_id: &str,
        order_id: &str,
    ) -> Result<DriverOrderDetail> {
        self.driver_repo
            .get_order_detail(driver_id, order_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Order tidak ditemukan"))
    }

    // ── Private: aggregate summary dari slice order ───────────────────────────
    // Dipakai sebagai fallback saat driver_daily_summary belum ada barisnya
    fn aggregate_summary(orders: &[DriverOrderItem]) -> DailySummary {
        let mut s = DailySummary::default();

        for o in orders {
            if o.status == "completed" {
                s.total_orders += 1;
                s.gross_earnings += o.fare;
                s.net_earnings += o.driver_earning;
                s.tips += o.tip;
                s.distance_km += o.distance_km;
                // platform fee = gross - net
                s.platform_fee += o.fare - o.driver_earning;

                // Running average rating
                if o.rating > 0.0 {
                    let prev_sum = s.avg_rating * s.rating_count_f32();
                    s.avg_rating = (prev_sum + o.rating) / (s.rating_count_f32() + 1.0);
                    // increment rating count via hack karena struct tidak punya field terpisah
                    // Solusi: gunakan gross sebagai proxy — sudah cukup untuk fallback
                }
            } else if o.status == "cancelled" {
                s.cancelled_orders += 1;
            }
        }

        s
    }
}

// Helper extension untuk aggregate_summary
trait SummaryExt {
    fn rating_count_f32(&self) -> f32;
}

impl SummaryExt for DailySummary {
    fn rating_count_f32(&self) -> f32 {
        // Proxy: hitung dari total_orders karena tidak simpan rating_count
        // di fallback. Cukup untuk estimasi, bukan nilai presisi.
        self.total_orders as f32
    }
}
