// ── perubahan dari versi mysql_async ─────────────────────────────────────────
//
//  SEBELUM                         SESUDAH
//  mysql_async::Pool               deadpool_postgres::Pool  (via db_pool::create_pool)
//  MySqlOrderRepository            PgOrderRepository
//  MySqlUserRepository             PgUserRepository
//  MySqlDriverRepository           PgDriverRepository
//  pool.get_conn().await?          pool.get().await?  (dihandle di repo layer)
//
// Semua tipe repository struct sudah ganti nama — kalau ada code lain yang
// import MySql* tinggal ganti ke Pg*.

use aws_config::BehaviorVersion;
use deadpool_postgres::{Config, Runtime};
use std::sync::Arc;
use tokio_postgres::NoTls;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod auth;
mod config;
mod connections;
mod context;
mod grpc;
mod location;
mod models;
mod proto;
mod repository;
mod service;
mod state;
mod throttle;
mod utils;

use auth::JwtService;
use grpc::{auth::AuthServiceImpl, trip::TripServiceImpl};
use proto::auth::auth_service_server::AuthServiceServer;
use proto::ridehailing::app_service_server::AppServiceServer;
// Ganti MySql* → Pg* di semua import
use repository::{order::PgOrderRepository, user::PgUserRepository};

use crate::{
    config::WahaConfig,
    grpc::{
        driver::DriverServiceImpl, message::MessageServiceImpl,
        notification::NotificationServiceImpl, order::OrderServiceImpl,
    },
    proto::{
        driver::driver_service_server, message::message_service_server::MessageServiceServer,
        notification::notification_service_server::NotificationServiceServer,
        order::order_service_server::OrderServiceServer, poi::poi_service_server::PoiServiceServer,
    },
    repository::{
        driver::PgDriverRepository, message::MessageRepository, notification, poi::PoiServiceImpl,
        rideshare,
    },
    service::driver::DriverService,
    state::AppState,
    throttle::ThrottleMap,
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

    let mut cfgpostgres = Config::new();
    cfgpostgres.url = Some(cfg.database_url);

    let pool = cfgpostgres.create_pool(Some(Runtime::Tokio1), NoTls)?;

    // POI database — bisa pakai DB yang sama atau berbeda

    let mut cfgpostgres_poi = Config::new();
    cfgpostgres_poi.url = Some(cfg.database_url_poi);
    let poi_pool = cfgpostgres_poi.create_pool(Some(Runtime::Tokio1), NoTls)?;

    let _conn_poi = poi_pool.clone().get().await?;
    tracing::info!("PostgreSQL POI connected");

    // ── Redis ─────────────────────────────────────────────────────────────────
    let redis_client = redis::Client::open(cfg.redis_url.as_str())?;
    let redis_conn = redis::aio::ConnectionManager::new_with_config(
        redis_client.clone(),
        redis::aio::ConnectionManagerConfig::new()
            .set_response_timeout(Some(std::time::Duration::from_secs(5)))
            .set_connection_timeout(Some(std::time::Duration::from_secs(5)))
            .set_number_of_retries(3),
    )
    .await?;
    tracing::info!("Redis connected");

    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut redis_conn.clone())
        .await?;
    tracing::info!("Redis FLUSHDB on startup — stale location/order data cleared");

    // ── R2 / S3 ───────────────────────────────────────────────────────────────
    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(format!(
            "https://{}.r2.cloudflarestorage.com",
            cfg.r2_account_id
        ))
        .region(aws_sdk_s3::config::Region::new("auto"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            cfg.r2_access_key.clone(),
            cfg.r2_secret_key.clone(),
            None,
            None,
            "static",
        ))
        .load()
        .await;

    let s3 = aws_sdk_s3::Client::new(&aws_config);
    match s3.list_buckets().send().await {
        Ok(output) => tracing::info!("R2 connected, buckets: {:?}", output.buckets()),
        Err(e) => tracing::warn!("R2 connection check failed: {:?}", e),
    }

    // ── Repositories & services ───────────────────────────────────────────────
    let jwt = JwtService::new(&cfg.jwt_secret);

    // Ganti MySql* → Pg*
    let user_repo = Arc::new(PgUserRepository::new(pool.clone()));
    let driver_repo = Arc::new(PgDriverRepository::new(pool.clone()));
    let message_repo = Arc::new(MessageRepository::new(pool.clone()));
    let order_repo = Arc::new(PgOrderRepository::new(pool.clone()));
    let location = location::LocationService::new(redis_conn.clone());
    let notifrepo = Arc::new(notification::NotificationRepository::new(pool.clone()));
    let ridesharerepo = Arc::new(rideshare::RideshareRepository::new(pool.clone()));

    let state: Arc<AppState<PgOrderRepository, PgUserRepository>> = AppState::new(
        user_repo.clone(),
        order_repo.clone(),
        location,
        redis_conn.clone(),
        redis_client.clone(),
    );

    let service_chat = service::chat::ChatService {
        msg_repo: message_repo.clone(),
        connections: state.connections.clone(),
        s3_client: Arc::new(s3),
        r2_public_url: cfg.r2_public_url,
        r2_bucket: cfg.r2_bucket_name,
    };

    let driver_service = Arc::new(DriverService::new(driver_repo));

    let waha = Arc::new(WahaConfig {
        base_url: std::env::var("WAHA_BASE_URL")
            .unwrap_or_else(|_| "http://localhost:3000".to_string()),
        session: std::env::var("WAHA_SESSION").unwrap_or_else(|_| "default".to_string()),
        api_key: std::env::var("WAHA_API_KEY").unwrap_or_default(),
    });

    // ── gRPC server ───────────────────────────────────────────────────────────
    let addr: std::net::SocketAddr = cfg.grpc_addr.parse()?;
    tracing::info!("gRPC listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(AuthServiceServer::new(AuthServiceImpl {
            user_repo: user_repo.clone(),
            jwt: jwt.clone(),
            waha,
            redis: redis_conn.clone(),
        }))
        .add_service(MessageServiceServer::new(MessageServiceImpl {
            chat_svc: Arc::new(service_chat.clone()),
            connections: state.connections.clone(),
            jwt: jwt.clone(),
            order_repo: order_repo.clone(),
        }))
        .add_service(driver_service_server::DriverServiceServer::new(
            DriverServiceImpl {
                driver_svc: driver_service,
                jwt: jwt.clone(),
            },
        ))
        .add_service(NotificationServiceServer::new(NotificationServiceImpl {
            notif_svc: Arc::new(service::notification::NotificationService {
                notif_repo: notifrepo.clone(),
                connections: state.connections.clone(),
            }),
            jwt: jwt.clone(),
        }))
        .add_service(PoiServiceServer::new(PoiServiceImpl {
            pool: Arc::new(poi_pool),
        }))
        .add_service(OrderServiceServer::new(OrderServiceImpl {
            order_svc: state.order_svc.clone(),
            jwt: jwt.clone(),
        }))
        .add_service(AppServiceServer::new(TripServiceImpl {
            jwt: jwt.clone(),
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
            throttle: Arc::new(ThrottleMap::new()),
        }))
        .serve(addr)
        .await?;

    Ok(())
}
