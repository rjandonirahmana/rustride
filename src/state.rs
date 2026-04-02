use crate::{
    connections::ConnectionManager,
    location::LocationService,
    repository::{order::OrderRepository, user::UserRepository},
    service::order::OrderService,
};
use anyhow::Result;
/// AppState — Arc-wrapped, di-share ke semua gRPC handler.
use std::sync::Arc;

pub struct AppState<OR: OrderRepository, UR: UserRepository> {
    pub connections: Arc<ConnectionManager>,
    pub location: LocationService,
    pub order_svc: Arc<OrderService<OR, UR>>,
}

impl<OR, UR> AppState<OR, UR>
where
    OR: OrderRepository + 'static,
    UR: UserRepository + 'static,
{
    // async karena ConnectionManager::new sekarang async + Result
    pub async fn new(
        user_repo: Arc<UR>,
        order_repo: Arc<OR>,
        location: LocationService,
        redis_client: redis::Client,
    ) -> Result<Arc<Self>> {
        // parameter redis: redis::aio::ConnectionManager dihapus —
        // ConnectionManager::new kini buat koneksinya sendiri dari Client
        let connections = ConnectionManager::new(redis_client).await?;

        let order_svc = Arc::new(OrderService::new(
            order_repo.clone(),
            user_repo.clone(),
            location.clone(),
            connections.clone(),
        ));

        Ok(Arc::new(Self {
            connections,
            location,
            order_svc,
        }))
    }
}
