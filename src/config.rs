use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub redis_url: String,
    pub jwt_secret: String,
    pub grpc_addr: String,
    // Field R2 baru
    pub r2_account_id: String,
    pub r2_access_key: String,
    pub r2_secret_key: String,
    pub r2_bucket_name: String,
    pub r2_public_url: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: env::var("DATABASE_URL").unwrap_or_else(|_| "".into()),
            redis_url: env::var("REDIS_URL").unwrap_or_else(|_| "".into()),
            jwt_secret: env::var("JWT_SECRET").unwrap_or_else(|_| "default_secret".into()),
            grpc_addr: env::var("GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".into()),
            // Masukkan variabel ke struct
            r2_account_id: env::var("R2_ACCOUNT_ID").expect("R2_ACCOUNT_ID must be set"),
            r2_access_key: env::var("R2_ACCESS_KEY_ID").expect("R2_ACCESS_KEY_ID must be set"),
            r2_secret_key: env::var("R2_SECRET_ACCESS_KEY")
                .expect("R2_SECRET_ACCESS_KEY must be set"),
            r2_bucket_name: env::var("R2_BUCKET_NAME").unwrap_or_else(|_| "rust-ride".to_string()),
            r2_public_url: env::var("R2_PUBLIC_URL").expect("R2_PUBLIC_URL must be set"),
        })
    }
}

// impl Config {
//     pub fn from_env() -> anyhow::Result<Self> {
//         use std::env;

//         let account_id = env::var("R2_ACCOUNT_ID").expect("R2_ACCOUNT_ID must be set");
//         let access_key = env::var("R2_ACCESS_KEY_ID").expect("R2_ACCESS_KEY_ID must be set");
//         let secret_key =
//             env::var("R2_SECRET_ACCESS_KEY").expect("R2_SECRET_ACCESS_KEY must be set");
//         let bucket_name = env::var("R2_BUCKET_NAME").unwrap_or_else(|_| "rustride".to_string());
//         let public_base_url = env::var("R2_PUBLIC_URL").expect("R2_PUBLIC_URL must be set");

//         // Gunakan variabel ini di s3_client config Anda

//         Ok(Self {
//             database_url: env::var("DATABASE_URL").unwrap_or_else(|_| {
//                 "mysql://root:LawakKocak1001!@77.237.242.1:3306/ridehailing".into()
//             }),
//             redis_url: env::var("REDIS_URL")
//                 .unwrap_or_else(|_| "redis://:awokawok1001!@77.237.242.1:6379".into()),
//             jwt_secret: env::var("JWT_SECRET").unwrap_or_else(|_| "default_secret".into()),
//             grpc_addr: env::var("GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".into()),
//         })
//     }
// }
