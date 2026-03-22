use anyhow::{bail, Result};
use std::sync::Arc;

use crate::repository::driver::{
    DailySummary, DriverOrderDetail, DriverOrderItem, DriverRepository, EarningsResult,
    OrderHistoryFilter,
};

// ── Output structs ────────────────────────────────────────────────────────────

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

        let summary = match self.driver_repo.get_today_summary(driver_id).await? {
            Some(s) => s,
            None => Self::aggregate_summary(&orders),
        };

        Ok(TodayOrdersResult { orders, summary })
    }

    // ── Today summary saja ────────────────────────────────────────────────────
    pub async fn get_today_summary(&self, driver_id: &str) -> Result<DailySummary> {
        match self.driver_repo.get_today_summary(driver_id).await? {
            Some(s) => Ok(s),
            None => {
                let orders = self.driver_repo.get_today_orders(driver_id).await?;
                Ok(Self::aggregate_summary(&orders))
            }
        }
    }

    // ── Earnings multi-period ─────────────────────────────────────────────────
    pub async fn get_earnings(
        &self,
        driver_id: &str,
        date_from: &str,
        date_to: &str,
    ) -> Result<EarningsResult> {
        self.driver_repo
            .get_earnings(driver_id, date_from, date_to)
            .await
    }

    // ── History dengan filter ─────────────────────────────────────────────────
    pub async fn get_order_history(
        &self,
        driver_id: &str,
        date_from: Option<String>,
        date_to: Option<String>,
        status: Option<String>,
        service_type: Option<String>,
        page: u32,
        limit: u32,
    ) -> Result<OrderHistoryResult> {
        if let Some(ref s) = status {
            if !matches!(s.as_str(), "completed" | "cancelled") {
                bail!("status harus 'completed' atau 'cancelled'");
            }
        }

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
                    service_type,
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

    // ── Fallback aggregate dari slice order ───────────────────────────────────
    // Dipakai saat driver_daily_summary belum ada (driver baru online pertama kali)
    fn aggregate_summary(orders: &[DriverOrderItem]) -> DailySummary {
        let mut s = DailySummary::default();

        // Untuk running average yang benar, track sum dan count secara terpisah
        let mut rating_sum: f32 = 0.0;
        let mut rating_count: u32 = 0;

        for o in orders {
            match o.status.as_str() {
                "completed" => {
                    s.total_orders += 1;
                    s.gross_earnings += o.fare;
                    s.net_earnings += o.driver_earning;
                    s.tips += o.tip;
                    s.distance_km += o.distance_km;
                    s.platform_fee += o.fare - o.driver_earning;

                    if o.rating > 0.0 {
                        rating_sum += o.rating;
                        rating_count += 1;
                    }
                }
                "cancelled" => {
                    s.cancelled_orders += 1;
                }
                _ => {}
            }
        }

        if rating_count > 0 {
            s.avg_rating = rating_sum / rating_count as f32;
        }

        // online_minutes tidak bisa di-aggregate dari orders — tetap 0 di fallback,
        // akan diisi dari driver_daily_summary ketika sudah tersedia.

        s
    }
}
