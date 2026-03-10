use anyhow::{Context, Ok, Result};
use async_trait::async_trait;
use mysql_async::{from_value, prelude::*, Pool, Row, Value};
use uuid::Uuid;

use super::db::{col, col_opt_str, exec_drop, exec_rows};
use crate::models::user::{DriverProfile, RegisterRequest, User};

// ── Trait (bisa di-mock atau diganti implementasi lain) ───────────────────────

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn create(&self, req: &RegisterRequest, hashed: &str) -> Result<User>;
    async fn find_by_id(&self, id: &str) -> Result<Option<User>>;
    async fn find_by_phone(&self, phone: &str) -> Result<Option<User>>;
    async fn find_driver_by_id(&self, id: &str) -> Result<Option<(User, DriverProfile)>>;
    async fn find_drivers_by_ids(&self, ids: &[String]) -> Result<Vec<(User, DriverProfile)>>;
    async fn set_driver_active(&self, driver_id: &str, active: bool) -> Result<()>;
}

// ── MySQL implementasi ────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct MySqlUserRepository {
    pool: Pool,
}

impl MySqlUserRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    /// Kolom SELECT standar untuk tabel users (alias u).
    fn user_cols() -> &'static str {
        r#"u.id, u.name, u.phone, u.email, u.password, u.role, u.avatar_url,
           DATE_FORMAT(CONVERT_TZ(u.created_at,'+00:00','+00:00'),'%Y-%m-%dT%H:%i:%sZ') AS created_at_fmt"#
    }

    fn row_to_user(row: &Row) -> Result<User> {
        Ok(User {
            id: from_value(col(row, "id")?),
            name: from_value(col(row, "name")?),
            phone: from_value(col(row, "phone")?),
            email: from_value(col(row, "email")?),
            password: from_value(col(row, "password")?),
            role: from_value(col(row, "role")?),
            avatar_url: col_opt_str(row, "avatar_url"),
            created_at: from_value(col(row, "created_at_fmt")?),
        })
    }

    fn row_to_driver_profile(row: &Row) -> Result<DriverProfile> {
        Ok(DriverProfile {
            user_id: from_value(col(row, "dp_user_id")?),
            vehicle_type: from_value(col(row, "vehicle_type")?),
            vehicle_plate: from_value(col(row, "vehicle_plate")?),
            vehicle_model: from_value(col(row, "vehicle_model")?),
            vehicle_color: from_value(col(row, "vehicle_color")?),
            rating: {
                let v: f64 = from_value(col(row, "rating")?);
                v as f32
            },
            total_trips: from_value(col(row, "total_trips")?),
            is_active: {
                let v: i8 = from_value(col(row, "is_active")?);
                v == 1
            },
        })
    }
}

#[async_trait]
impl UserRepository for MySqlUserRepository {
    async fn create(&self, req: &RegisterRequest, hashed: &str) -> Result<User> {
        let id = Uuid::new_v4().to_string();

        exec_drop(
            &self.pool,
            "INSERT INTO users (id, name, phone, email, password, role) VALUES (?, ?, ?, ?, ?, ?)",
            (
                &id,
                req.name.as_str(),
                req.phone.as_str(),
                req.email.as_str(),
                hashed,
                req.role.as_str(),
            ),
        )
        .await?;

        if req.role == "driver" {
            let vtype = req.vehicle_type.as_deref().unwrap_or("motor");
            let plate = req.vehicle_plate.as_deref().unwrap_or("");
            let model = req.vehicle_model.as_deref().unwrap_or("");
            let color = req.vehicle_color.as_deref().unwrap_or("");
            exec_drop(
                &self.pool,
                "INSERT INTO driver_profiles (user_id, vehicle_type, vehicle_plate, vehicle_model, vehicle_color) VALUES (?, ?, ?, ?, ?)",
                (&id, vtype, plate, model, color),
            )
            .await?;
        }

        self.find_by_id(&id)
            .await?
            .context("user not found after create")
    }

    async fn find_by_id(&self, id: &str) -> Result<Option<User>> {
        let q = format!("SELECT {} FROM users u WHERE u.id = ?", Self::user_cols());
        let rows = exec_rows(&self.pool, &q, (id,)).await?;
        rows.into_iter()
            .next()
            .map(|r| Self::row_to_user(&r))
            .transpose()
    }

    async fn find_by_phone(&self, phone: &str) -> Result<Option<User>> {
        let q = format!(
            "SELECT {} FROM users u WHERE u.phone = ?",
            Self::user_cols()
        );
        let rows = exec_rows(&self.pool, &q, (phone,)).await?;
        rows.into_iter()
            .next()
            .map(|r| Self::row_to_user(&r))
            .transpose()
    }

    async fn find_driver_by_id(&self, id: &str) -> Result<Option<(User, DriverProfile)>> {
        let q = format!(
            r#"SELECT {}, dp.user_id AS dp_user_id,
               dp.vehicle_type, dp.vehicle_plate, dp.vehicle_model,
               dp.vehicle_color, dp.rating, dp.total_trips, dp.is_active
               FROM users u
               JOIN driver_profiles dp ON dp.user_id = u.id
               WHERE u.id = ?"#,
            Self::user_cols()
        );
        let rows = exec_rows(&self.pool, &q, (id,)).await?;
        rows.into_iter()
            .next()
            .map(|r| {
                let user = Self::row_to_user(&r)?;
                let profile = Self::row_to_driver_profile(&r)?;
                Ok((user, profile))
            })
            .transpose()
    }

    async fn find_drivers_by_ids(&self, ids: &[String]) -> Result<Vec<(User, DriverProfile)>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let ph = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let q = format!(
            r#"SELECT {}, dp.user_id AS dp_user_id,
               dp.vehicle_type, dp.vehicle_plate, dp.vehicle_model,
               dp.vehicle_color, dp.rating, dp.total_trips, dp.is_active
               FROM users u
               JOIN driver_profiles dp ON dp.user_id = u.id
               WHERE u.id IN ({}) AND dp.is_active = 1"#,
            Self::user_cols(),
            ph
        );
        let params: Vec<Value> = ids.iter().map(|id| Value::from(id.as_str())).collect();
        let mut conn = self.pool.get_conn().await.context("db connection failed")?;
        let rows: Vec<Row> = conn
            .exec(&q, mysql_async::Params::Positional(params))
            .await
            .context("find_drivers_by_ids failed")?;
        rows.into_iter()
            .map(|r| {
                let user = Self::row_to_user(&r)?;
                let profile = Self::row_to_driver_profile(&r)?;
                Ok((user, profile))
            })
            .collect()
    }

    async fn set_driver_active(&self, driver_id: &str, active: bool) -> Result<()> {
        exec_drop(
            &self.pool,
            "UPDATE driver_profiles SET is_active = ? WHERE user_id = ?",
            (active as i8, driver_id),
        )
        .await
    }
}
