use anyhow::{Context, Result};
use async_trait::async_trait;
use deadpool_postgres::Pool;

use super::db::{col_opt_str, exec_drop, exec_first, exec_rows, f32_col, get_conn};
use crate::{
    models::user::{DriverProfile, RegisterRequest, User},
    utils::ulid::{bin_to_ulid, id_to_vec, new_ulid, ulid_to_vec},
};

// ── Cara bind BYTEA yang benar di tokio-postgres ──────────────────────────────
//
// Vec<u8> impl ToSql → pass sebagai &vec (single ref, BUKAN &vec.as_slice())
//
//   let id_b: Vec<u8> = ulid_to_vec(id)?;
//   exec_rows(&pool, "WHERE id = $1", &[&id_b])   // ✅ &Vec<u8>
//
// ── Cara bind ENUM yang benar ─────────────────────────────────────────────────
//
// Custom ENUM (user_role, vehicle_type) tidak bisa di-bind dari &str langsung.
// Solusi: cast di SQL → $N::text::nama_enum
//
//   "INSERT INTO users ... VALUES ($1, $6::text::user_role)"
//                                      ↑
//                               PostgreSQL cast &str → text → user_role ENUM

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn create(&self, req: &RegisterRequest, hashed: &str) -> Result<User>;
    async fn find_by_id(&self, id: &str) -> Result<Option<User>>;
    async fn find_by_phone(&self, phone: &str) -> Result<Option<User>>;
    async fn find_driver_by_id(&self, id: &str) -> Result<Option<(User, DriverProfile)>>;
    async fn find_drivers_by_ids(&self, ids: &[String]) -> Result<Vec<(User, DriverProfile)>>;
}

#[derive(Clone)]
pub struct PgUserRepository {
    pool: Pool,
}

impl PgUserRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    // Ambil BYTEA langsung — decode ke ULID via bin_to_ulid di row_to_user.
    // Tidak pakai encode(id,'hex') di SQL.
    fn user_cols() -> &'static str {
        r#"u.id, u.name, u.phone,
           COALESCE(u.email, '') AS email,
           u.password,
           u.role::TEXT AS role,
           u.avatar_url,
           to_char(u.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at_fmt,
           COALESCE(dp.vehicle_type::TEXT, '') AS vehicle_type"#
    }

    fn row_to_user(row: &tokio_postgres::Row) -> Result<User> {
        let id_bytes: Vec<u8> = row.try_get("id")?;
        Ok(User {
            id: bin_to_ulid(id_bytes)?,
            name: row.try_get("name").context("name")?,
            phone: row.try_get("phone").context("phone")?,
            email: row.try_get("email").context("email")?,
            password: row.try_get("password").context("password")?,
            role: row.try_get("role").context("role")?,
            avatar_url: col_opt_str(row, "avatar_url")?,
            created_at: row.try_get("created_at_fmt").context("created_at_fmt")?,
            vehicle_type: row.try_get::<_, String>("vehicle_type").unwrap_or_default(),
        })
    }

    fn row_to_driver_profile(row: &tokio_postgres::Row) -> Result<DriverProfile> {
        // user_id diambil dari kolom id (users.id) sebagai BYTEA → ULID
        let id_bytes: Vec<u8> = row.try_get("id").context("id")?;
        let rating: f64 = row.try_get("rating").context("rating")?;
        let is_active: bool = row.try_get("is_active").context("is_active")?;
        Ok(DriverProfile {
            user_id: bin_to_ulid(id_bytes)?,
            vehicle_type: row.try_get("vehicle_type").context("vehicle_type")?,
            vehicle_plate: row.try_get("vehicle_plate").context("vehicle_plate")?,
            vehicle_model: row.try_get("vehicle_model").context("vehicle_model")?,
            vehicle_color: row.try_get("vehicle_color").context("vehicle_color")?,
            rating: rating as f32,
            total_trips: row.try_get("total_trips").context("total_trips")?,
            is_active,
        })
    }
}

#[async_trait]
impl UserRepository for PgUserRepository {
    async fn create(&self, req: &RegisterRequest, hashed: &str) -> Result<User> {
        let id = new_ulid();
        let id_b = ulid_to_vec(&id)?; // Vec<u8>

        let email_opt: Option<&str> = if req.email.is_empty() {
            None
        } else {
            Some(req.email.as_str())
        };

        // &id_b → &Vec<u8> impl ToSql ✅
        // $6::text::user_role → bind &str ke ENUM lewat double-cast ✅
        exec_drop(
            &self.pool,
            "INSERT INTO users (id, name, phone, email, password, role)
             VALUES ($1, $2, $3, $4, $5, $6::text::user_role)",
            &[
                &id_b,
                &req.name.as_str(),
                &req.phone.as_str(),
                &email_opt,
                &hashed,
                &req.role.as_str(),
            ],
        )
        .await?;

        if req.role == "driver" {
            let vtype = req.vehicle_type.as_deref().unwrap_or("motor");
            let plate = req.vehicle_plate.as_deref().unwrap_or("");
            let model = req.vehicle_model.as_deref().unwrap_or("");
            let color = req.vehicle_color.as_deref().unwrap_or("");
            exec_drop(
                &self.pool,
                "INSERT INTO driver_profiles
                 (user_id, vehicle_type, vehicle_plate, vehicle_model, vehicle_color)
                 VALUES ($1, $2::text::vehicle_type, $3, $4, $5)",
                &[&id_b, &vtype, &plate, &model, &color],
            )
            .await?;
        }

        self.find_by_id(&id)
            .await?
            .context("user not found after create")
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<User>> {
        let id_b = id_to_vec(id)?;
        let q = format!(
            "SELECT {} FROM users u
             LEFT JOIN driver_profiles dp ON u.id = dp.user_id
             WHERE u.id = $1",
            Self::user_cols()
        );
        exec_first(&self.pool, &q, &[&id_b])
            .await?
            .map(|r| Self::row_to_user(&r))
            .transpose()
    }

    async fn find_by_phone(&self, phone: &str) -> Result<Option<User>> {
        let q = format!(
            "SELECT {} FROM users u
             LEFT JOIN driver_profiles dp ON u.id = dp.user_id
             WHERE u.phone = $1",
            Self::user_cols()
        );
        exec_first(&self.pool, &q, &[&phone])
            .await?
            .map(|r| Self::row_to_user(&r))
            .transpose()
    }

    async fn find_driver_by_id(&self, id: &str) -> Result<Option<(User, DriverProfile)>> {
        let id_b = id_to_vec(id)?;
        let q = format!(
            r#"SELECT {cols},
                      dp.vehicle_plate, dp.vehicle_model,
                      dp.vehicle_color, dp.rating::FLOAT8 AS rating, dp.total_trips, dp.is_active
               FROM users u
               INNER JOIN driver_profiles dp ON u.id = dp.user_id
               WHERE u.id = $1 AND u.role = 'driver'::user_role"#,
            cols = Self::user_cols()
        );
        exec_first(&self.pool, &q, &[&id_b])
            .await?
            .map(|r| Ok((Self::row_to_user(&r)?, Self::row_to_driver_profile(&r)?)))
            .transpose()
    }

    async fn find_drivers_by_ids(&self, ids: &[String]) -> Result<Vec<(User, DriverProfile)>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        // Vec<Vec<u8>> — bind sebagai &Vec<Vec<u8>>, PostgreSQL terima sebagai bytea[]
        let id_bytes: Vec<Vec<u8>> = ids.iter().map(|id| id_to_vec(id)).collect::<Result<_>>()?;

        let q = format!(
            r#"SELECT {cols},
                      dp.vehicle_plate, dp.vehicle_model,
                      dp.vehicle_color, dp.rating::FLOAT8 AS rating, dp.total_trips, dp.is_active
               FROM users u
               INNER JOIN driver_profiles dp ON u.id = dp.user_id
               WHERE u.id = ANY($1) AND dp.is_active = true"#,
            cols = Self::user_cols()
        );

        let conn = get_conn(&self.pool).await?;
        let rows = conn
            .query(&q, &[&id_bytes])
            .await
            .context("find_drivers_by_ids failed")?;

        rows.iter()
            .map(|r| Ok((Self::row_to_user(r)?, Self::row_to_driver_profile(r)?)))
            .collect()
    }
}
