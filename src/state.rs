/// AppState — Arc-wrapped, di-share ke semua gRPC handler.
use std::sync::Arc;

use crate::{
    connections::ConnectionManager,
    location::LocationService,
    repository::{order::OrderRepository, user::UserRepository},
    service::order::OrderService,
};

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
    pub fn new(
        user_repo: Arc<UR>,
        order_repo: Arc<OR>,
        location: LocationService,
        redis: redis::aio::ConnectionManager,
        redis_client: redis::Client,
    ) -> Arc<Self> {
        let connections = ConnectionManager::new(redis, redis_client);
        let order_svc = Arc::new(OrderService::new(
            order_repo.clone(),
            user_repo.clone(),
            location.clone(),
            connections.clone(),
        ));
        Arc::new(Self {
            connections,
            location,
            order_svc,
        })
    }
}
