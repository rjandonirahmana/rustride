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
mod utils;

use auth::JwtService;
use grpc::{auth::AuthServiceImpl, trip::TripServiceImpl};
use proto::auth::auth_service_server::AuthServiceServer;
use proto::ridehailing::app_service_server::AppServiceServer;
use repository::{order::MySqlOrderRepository, user::MySqlUserRepository};

use crate::{
    config::WahaConfig,
    grpc::{
        driver::DriverServiceImpl, message::MessageServiceImpl,
        notification::NotificationServiceImpl, order::OrderServiceImpl,
    },
    proto::{
        driver::driver_service_server, message::message_service_server::MessageServiceServer,
        notification::notification_service_server::NotificationServiceServer,
        order::order_service_server::OrderServiceServer,
    },
    repository::{driver, message::MessageRepository, notification, rideshare},
    service::driver::DriverService,
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

    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(format!(
            "https://{}.r2.cloudflarestorage.com",
            cfg.r2_account_id
        ))
        .region(aws_sdk_s3::config::Region::new("auto")) // HARUS "auto"
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            cfg.r2_access_key,
            cfg.r2_secret_key,
            None,
            None,
            "static",
        ))
        .load()
        .await;

    let s3 = aws_sdk_s3::Client::new(&config);
    let request = s3.list_buckets();

    // 2. KIRIM request ke Cloudflare (ini yang menentukan konek atau tidak)
    let response = request.send().await;

    // 3. Cek hasilnya
    match response {
        Ok(output) => {
            println!("Koneksi BERHASIL!");
            println!("Daftar bucket: {:?}", output.buckets());
        }
        Err(e) => {
            println!("Koneksi GAGAL!");
            println!("Error detail: {:?}", e);
        }
    }

    // ── Repositories & services ───────────────────────────────────────────────
    let jwt = JwtService::new(&cfg.jwt_secret);
    let user_repo = Arc::new(MySqlUserRepository::new(pool.clone()));
    let driver_repo = Arc::new(driver::MySqlDriverRepository::new(pool.clone()));
    let message_repo = Arc::new(MessageRepository::new(pool.clone()));
    let order_repo = Arc::new(MySqlOrderRepository::new(pool.clone()));
    let location = location::LocationService::new(redis_conn.clone());
    let notifrepo = Arc::new(notification::NotificationRepository::new(pool.clone()));
    let ridesharerepo = Arc::new(rideshare::RideshareRepository::new(pool.clone()));

    let state: Arc<AppState<MySqlOrderRepository, MySqlUserRepository>> = AppState::new(
        user_repo.clone(),
        order_repo.clone(),
        location,
        redis_conn,
        redis_client.clone(),
    );

    // create a 'static str for r2_public_url by leaking a cloned String so it can live for the program lifetime
    let r2_public_url_static: &'static str = Box::leak(cfg.r2_public_url.clone().into_boxed_str());
    let service_chat = service::chat::ChatService {
        msg_repo: message_repo.clone(),
        connections: state.connections.clone(),
        s3_client: Arc::new(s3),
        r2_public_url: r2_public_url_static,
        r2_bucket: Box::leak(cfg.r2_bucket_name.clone().into_boxed_str()),
    };

    let driver_service = Arc::new(DriverService::new(driver_repo));

    let waha = Arc::new(WahaConfig {
        base_url: std::env::var("WAHA_BASE_URL")
            .unwrap_or_else(|_| "http://localhost:3000".to_string()),
        session: std::env::var("WAHA_SESSION").unwrap_or_else(|_| "default".to_string()),
        api_key: std::env::var("WAHA_API_KEY").unwrap_or_default(), // kosong = tidak pakai auth
    });
    // ── gRPC server ───────────────────────────────────────────────────────────
    let addr: std::net::SocketAddr = cfg.grpc_addr.parse()?;
    tracing::info!("gRPC listening on {}", addr);

    tonic::transport::Server::builder()
        // AuthService — publik, tidak butuh token
        .add_service(AuthServiceServer::new(AuthServiceImpl {
            user_repo: user_repo.clone(),
            jwt: jwt.clone(),
            waha: waha,
            redis: redis_client.get_multiplexed_async_connection().await?,
        }))
        .add_service(MessageServiceServer::new(MessageServiceImpl {
            chat_svc: Arc::new(service_chat.clone()),
            connections: state.connections.clone(),
            jwt: jwt.clone(),
            order_repo: order_repo,
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
        .add_service(OrderServiceServer::new(OrderServiceImpl {
            order_svc: state.order_svc.clone(),
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
