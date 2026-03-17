use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod auth;
mod config;
mod connections;
mod grpc;
mod location;
mod models;
mod proto;
mod repository;
mod service;
mod state;

use auth::JwtService;
use grpc::{auth::AuthServiceImpl, trip::TripServiceImpl};
use proto::ridehailing::{
    app_service_server::AppServiceServer, auth_service_server::AuthServiceServer,
};
use repository::{order::MySqlOrderRepository, user::MySqlUserRepository};

use crate::{
    repository::{message::MessageRepository, notification, rideshare},
    state::AppState,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Logging ───────────────────────────────────────────────────────────────
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ridehailing=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    dotenvy::dotenv().ok();

    // ── Config ────────────────────────────────────────────────────────────────
    let cfg = config::Config::from_env()?;
    tracing::info!("Config loaded");

    // ── Database (mysql_async Pool) ───────────────────────────────────────────
    let pool = mysql_async::Pool::new(cfg.database_url.as_str());

    let _conn = pool.get_conn().await?;
    tracing::info!("MySQL connected");

    // ── Redis ─────────────────────────────────────────────────────────────────
    let redis_client = redis::Client::open(cfg.redis_url.as_str())?;
    let redis_conn = redis::aio::ConnectionManager::new(redis_client.clone()).await?;
    tracing::info!("Redis connected");

    let region_provider = RegionProviderChain::first_try(Some(Region::new("us-east-1")));

    let config = aws_config::defaults(BehaviorVersion::v2026_01_12())
        .region("us-east-1")
        .endpoint_url("http://77.237.242.1:9000") // RustFS
        .region(region_provider)
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "rustridecompany",
            "rustridecompany123",
            None,
            None,
            "loaded",
        ))
        .load()
        .await;

    let s3 = aws_sdk_s3::Client::new(&config);

    print!("{:?}", s3.get_bucket_acl());

    // ── Repositories & services ───────────────────────────────────────────────
    let jwt = JwtService::new(&cfg.jwt_secret);
    let user_repo = Arc::new(MySqlUserRepository::new(pool.clone()));
    let message_repo = Arc::new(MessageRepository::new(pool.clone()));
    let order_repo = Arc::new(MySqlOrderRepository::new(pool.clone()));
    let location = location::LocationService::new(redis_conn.clone());
    let notifrepo = Arc::new(notification::NotificationRepository::new(pool.clone()));
    let ridesharerepo = Arc::new(rideshare::RideshareRepository::new(pool.clone()));

    let state: Arc<AppState<MySqlOrderRepository, MySqlUserRepository>> = AppState::new(
        user_repo.clone(),
        order_repo,
        location,
        redis_conn,
        redis_client,
    );

    let service_chat = service::chat::ChatService {
        msg_repo: message_repo.clone(),
        connections: state.connections.clone(),
        s3_client: Arc::new(s3),
    };

    // ── gRPC server ───────────────────────────────────────────────────────────
    let addr: std::net::SocketAddr = cfg.grpc_addr.parse()?;
    tracing::info!("gRPC listening on {}", addr);

    tonic::transport::Server::builder()
        // AuthService — publik, tidak butuh token
        .add_service(AuthServiceServer::new(AuthServiceImpl {
            user_repo: user_repo.clone(),
            jwt: jwt.clone(),
        }))
        // TripService — bidi stream, JWT di metadata
        .add_service(AppServiceServer::new(TripServiceImpl {
            jwt: jwt,
            order_svc: state.order_svc.clone(),
            connections: state.connections.clone(),
            user_repo: user_repo.clone(),
            location: state.location.clone(),
            chat_svc: Arc::new(service_chat),
            notif_svc: Arc::new(service::notification::NotificationService {
                notif_repo: notifrepo.clone(),
                connections: state.connections.clone(),
            }),
            rideshare_svc: Arc::new(service::rideshare::RideshareService {
                rideshare_repo: ridesharerepo,
                notif_repo: notifrepo,
                user_repo: user_repo,
                connections: state.connections.clone(),
            }),
        }))
        .serve(addr)
        .await?;

    Ok(())
}
