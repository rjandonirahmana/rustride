// src/repository/poi.rs

use deadpool_postgres::Pool;
use std::sync::Arc;
use tokio_postgres::Row;
use tonic::{Request, Response, Status};

use crate::proto::poi::{
    poi_service_server::PoiService, NearbyPoiRequest, NearbyPoiResponse, PoiDetailRequest,
    PoiDetailResponse, PoiItem,
};

pub struct PoiServiceImpl {
    pub pool: Arc<Pool>,
}

#[tonic::async_trait]
impl PoiService for PoiServiceImpl {
    async fn search_nearby(
        &self,
        req: Request<NearbyPoiRequest>,
    ) -> Result<Response<NearbyPoiResponse>, Status> {
        let r = req.into_inner();

        if r.lat == 0.0 || r.lng == 0.0 {
            return Err(Status::invalid_argument("lat dan lng wajib diisi"));
        }

        let radius_km = if r.radius_km <= 0.0 { 2.0 } else { r.radius_km };
        let limit = if r.limit <= 0 { 20 } else { r.limit.min(100) };

        // deadpool: pool.get() → Object (auto-return saat drop)
        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Status::internal(format!("DB connection error: {e}")))?;

        let pois = if r.subcategory.is_empty() {
            search_nearby_query(&conn, r.lat, r.lng, radius_km, &r.category, None, limit).await?
        } else {
            search_nearby_query(
                &conn,
                r.lat,
                r.lng,
                radius_km,
                &r.category,
                Some(&r.subcategory),
                limit,
            )
            .await?
        };

        Ok(Response::new(NearbyPoiResponse { pois }))
    }

    async fn get_poi_detail(
        &self,
        req: Request<PoiDetailRequest>,
    ) -> Result<Response<PoiDetailResponse>, Status> {
        let r = req.into_inner();

        if r.id.is_empty() {
            return Err(Status::invalid_argument("id wajib diisi"));
        }

        let id: i64 =
            r.id.parse()
                .map_err(|_| Status::invalid_argument("id tidak valid"))?;

        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| Status::internal(format!("DB connection error: {e}")))?;

        let poi = get_poi_by_id(&conn, id).await?;
        Ok(Response::new(PoiDetailResponse { poi: Some(poi) }))
    }
}

// ── Query helpers ─────────────────────────────────────────────────────────────

async fn search_nearby_query(
    conn: &deadpool_postgres::Object,
    lat: f64,
    lng: f64,
    radius_km: f32,
    category: &str,
    subcategory: Option<&str>,
    limit: i32,
) -> Result<Vec<PoiItem>, Status> {
    let radius_m = (radius_km * 1000.0) as f64;
    let limit_i64 = limit as i64;

    // tokio-postgres: tidak ada named params (:lat) — pakai $1..$N
    // tokio-postgres: tidak ada HAVING pada ekspresi non-aggregate di level yang sama
    // — pindahkan filter jarak ke WHERE dengan subquery atau ulangi ekspresi
    // Di sini kita ulangi ekspresi haversine di WHERE supaya tetap satu query sederhana.

    let rows: Vec<Row> = if let Some(sub) = subcategory {
        conn.query(
            r#"SELECT
                   id, name, COALESCE(name_en, '') AS name_en,
                   lat, lng,
                   COALESCE(category, '')      AS category,
                   COALESCE(subcategory, '')   AS subcategory,
                   COALESCE(address, '')       AS address,
                   COALESCE(city, '')          AS city,
                   COALESCE(phone, '')         AS phone,
                   COALESCE(website, '')       AS website,
                   COALESCE(opening_hours, '') AS opening_hours,
                   (6371000 * acos(
                       GREATEST(-1.0, LEAST(1.0,
                           cos(radians($1)) * cos(radians(lat)) *
                           cos(radians(lng) - radians($2)) +
                           sin(radians($1)) * sin(radians(lat))
                       ))
                   )) AS distance_m
               FROM osm_places
               WHERE category    = $3
                 AND subcategory = $4
                 AND (6371000 * acos(
                         GREATEST(-1.0, LEAST(1.0,
                             cos(radians($1)) * cos(radians(lat)) *
                             cos(radians(lng) - radians($2)) +
                             sin(radians($1)) * sin(radians(lat))
                         ))
                     )) < $5
               ORDER BY distance_m
               LIMIT $6"#,
            &[&lat, &lng, &category, &sub, &radius_m, &limit_i64],
        )
        .await
        .map_err(|e| Status::internal(format!("Query error: {e}")))?
    } else {
        conn.query(
            r#"SELECT
                   id, name, COALESCE(name_en, '') AS name_en,
                   lat, lng,
                   COALESCE(category, '')      AS category,
                   COALESCE(subcategory, '')   AS subcategory,
                   COALESCE(address, '')       AS address,
                   COALESCE(city, '')          AS city,
                   COALESCE(phone, '')         AS phone,
                   COALESCE(website, '')       AS website,
                   COALESCE(opening_hours, '') AS opening_hours,
                   (6371000 * acos(
                       GREATEST(-1.0, LEAST(1.0,
                           cos(radians($1)) * cos(radians(lat)) *
                           cos(radians(lng) - radians($2)) +
                           sin(radians($1)) * sin(radians(lat))
                       ))
                   )) AS distance_m
               FROM osm_places
               WHERE category = $3
                 AND (6371000 * acos(
                         GREATEST(-1.0, LEAST(1.0,
                             cos(radians($1)) * cos(radians(lat)) *
                             cos(radians(lng) - radians($2)) +
                             sin(radians($1)) * sin(radians(lat))
                         ))
                     )) < $4
               ORDER BY distance_m
               LIMIT $5"#,
            &[&lat, &lng, &category, &radius_m, &limit_i64],
        )
        .await
        .map_err(|e| Status::internal(format!("Query error: {e}")))?
    };

    Ok(rows.iter().map(row_to_poi).collect())
}

async fn get_poi_by_id(conn: &deadpool_postgres::Object, id: i64) -> Result<PoiItem, Status> {
    conn.query_opt(
        r#"SELECT
               id, name, COALESCE(name_en, '') AS name_en,
               lat, lng,
               COALESCE(category, '')      AS category,
               COALESCE(subcategory, '')   AS subcategory,
               COALESCE(address, '')       AS address,
               COALESCE(city, '')          AS city,
               COALESCE(phone, '')         AS phone,
               COALESCE(website, '')       AS website,
               COALESCE(opening_hours, '') AS opening_hours,
               0.0::FLOAT8                AS distance_m
           FROM osm_places
           WHERE id = $1"#,
        &[&id],
    )
    .await
    .map_err(|e| Status::internal(format!("Query error: {e}")))?
    .map(|r| row_to_poi(&r))
    .ok_or_else(|| Status::not_found(format!("POI {id} tidak ditemukan")))
}

// ── Row mapper ────────────────────────────────────────────────────────────────
//
// Sebelumnya: row.get(0), row.get(1) — index positional mysql_async
// Sesudah:    row.try_get("col_name") — by name tokio-postgres (lebih aman)

fn row_to_poi(row: &Row) -> PoiItem {
    let id: i64 = row.try_get("id").unwrap_or(0);
    PoiItem {
        id: id.to_string(),
        name: row.try_get("name").unwrap_or_default(),
        name_en: row.try_get("name_en").unwrap_or_default(),
        lat: row.try_get("lat").unwrap_or(0.0),
        lng: row.try_get("lng").unwrap_or(0.0),
        category: row.try_get("category").unwrap_or_default(),
        subcategory: row.try_get("subcategory").unwrap_or_default(),
        address: row.try_get("address").unwrap_or_default(),
        city: row.try_get("city").unwrap_or_default(),
        phone: row.try_get("phone").unwrap_or_default(),
        website: row.try_get("website").unwrap_or_default(),
        opening_hours: row.try_get("opening_hours").unwrap_or_default(),
        distance_m: row.try_get::<_, f64>("distance_m").unwrap_or(0.0) as f32,
    }
}
