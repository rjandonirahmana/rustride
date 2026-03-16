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
            database_url: env::var("DATABASE_URL").unwrap_or_else(|_| {
                "mysql://root:LawakKocak1001!@77.237.242.1:3306/ridehailing".into()
            }),
            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://:awokawok1001!@77.237.242.1:6379".into()),
            jwt_secret: env::var("JWT_SECRET").unwrap_or_else(|_| "default_secret".into()),
            grpc_addr: env::var("GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".into()),
        })
    }
}
