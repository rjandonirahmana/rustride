use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub redis_url: String,
    pub jwt_secret: String,

    pub grpc_addr: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: env::var("DATABASE_URL")
                .unwrap_or("mysql://root:@localhost:3306/ridehailing".into()),
            redis_url: env::var("REDIS_URL").unwrap_or("redis://127.0.0.1:6379".into()),
            jwt_secret: env::var("JWT_SECRET")
                .ok()
                .unwrap_or_else(|| "default_secret".into()),
            grpc_addr: env::var("GRPC_ADDR").unwrap_or("0.0.0.0:50051".into()),
        })
    }
}
