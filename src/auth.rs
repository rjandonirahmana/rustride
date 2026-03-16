use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,  // user_id
    pub role: String, // "rider" | "driver"
    pub name: String,
    pub exp: usize, // Expiration time as UNIX timestamp
    pub vehicle_type: String,
}

#[derive(Clone)]
pub struct JwtService {
    secret: String,
}

impl JwtService {
    pub fn new(secret: &str) -> Self {
        Self {
            secret: secret.to_string(),
        }
    }

    pub fn sign(
        &self,
        user_id: &str,
        name: &str,
        role: &str,
        vehicle_type: &str,
    ) -> anyhow::Result<String> {
        // PERBAIKAN: Gunakan TimeDelta atau Duration dengan cara yang benar

        let claims = Claims {
            sub: user_id.to_string(),
            role: role.to_string(),
            name: name.to_string(),
            exp: (chrono::Utc::now() + chrono::Duration::days(100)).timestamp() as usize,
            vehicle_type: vehicle_type.to_string(),
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .map_err(Into::into)
    }

    pub fn verify(&self, token: &str) -> anyhow::Result<Claims> {
        // Buat validation manual daripada pakai default
        let mut validation = Validation::new(Algorithm::HS256);

        // Matikan pengecekan yang tidak ada di struct Claims kamu
        validation.validate_exp = true; // Tetap cek kadaluarsa
                                        // validation.required_spec_claims.clear(); // Opsional: hapus jika masih error

        decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &validation,
        )
        .map(|d| d.claims)
        .map_err(|e| {
            // Log error di sini untuk melihat alasan spesifiknya
            eprintln!("JWT Verify Error: {:?}", e);
            e.into()
        })
    }
}
