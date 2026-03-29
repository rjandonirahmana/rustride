// src/handler/poi.rs

use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::proto::poi::{
    poi_service_server::PoiService, NearbyPoiRequest, NearbyPoiResponse, PoiDetailRequest,
    PoiDetailResponse, PoiItem,
};

pub struct PoiServiceImpl {
    pub pool: Arc<mysql_async::Pool>,
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

        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| Status::internal(format!("DB connection error: {e}")))?;

        // Build query — filter subcategory opsional
        let pois = if r.subcategory.is_empty() {
            search_nearby_query(&mut conn, r.lat, r.lng, radius_km, &r.category, None, limit)
                .await?
        } else {
            search_nearby_query(
                &mut conn,
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

        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| Status::internal(format!("DB connection error: {e}")))?;

        let id: i64 =
            r.id.parse()
                .map_err(|_| Status::invalid_argument("id tidak valid"))?;

        let poi = get_poi_by_id(&mut conn, id).await?;

        Ok(Response::new(PoiDetailResponse { poi: Some(poi) }))
    }
}

// ── Query helpers ─────────────────────────────────────────────────────────────

async fn search_nearby_query(
    conn: &mut mysql_async::Conn,
    lat: f64,
    lng: f64,
    radius_km: f32,
    category: &str,
    subcategory: Option<&str>,
    limit: i32,
) -> Result<Vec<PoiItem>, Status> {
    use mysql_async::prelude::*;

    // Haversine formula — hitung jarak dalam meter
    let base_query = r#"
        SELECT
            id, name, COALESCE(name_en, '') as name_en,
            lat, lng,
            COALESCE(category, '') as category,
            COALESCE(subcategory, '') as subcategory,
            COALESCE(address, '') as address,
            COALESCE(city, '') as city,
            COALESCE(phone, '') as phone,
            COALESCE(website, '') as website,
            COALESCE(opening_hours, '') as opening_hours,
            (6371000 * acos(
                GREATEST(-1.0, LEAST(1.0,
                    cos(radians(:lat)) * cos(radians(lat)) *
                    cos(radians(lng) - radians(:lng)) +
                    sin(radians(:lat)) * sin(radians(lat))
                ))
            )) AS distance_m
        FROM osm_places
        WHERE category = :category
        {{subcategory_filter}}
        HAVING distance_m < :radius_m
        ORDER BY distance_m
        LIMIT :limit
    "#;

    let rows: Vec<PoiItem> = if let Some(sub) = subcategory {
        let query = base_query.replace("{{subcategory_filter}}", "AND subcategory = :subcategory");
        conn.exec_map(
            query,
            mysql_async::params! {
                "lat" => lat,
                "lng" => lng,
                "category" => category,
                "subcategory" => sub,
                "radius_m" => (radius_km * 1000.0) as f64,
                "limit" => limit,
            },
            row_to_poi,
        )
        .await
        .map_err(|e| Status::internal(format!("Query error: {e}")))?
    } else {
        let query = base_query.replace("{{subcategory_filter}}", "");
        conn.exec_map(
            query,
            mysql_async::params! {
                "lat" => lat,
                "lng" => lng,
                "category" => category,
                "radius_m" => (radius_km * 1000.0) as f64,
                "limit" => limit,
            },
            row_to_poi,
        )
        .await
        .map_err(|e| Status::internal(format!("Query error: {e}")))?
    };

    Ok(rows)
}

async fn get_poi_by_id(conn: &mut mysql_async::Conn, id: i64) -> Result<PoiItem, Status> {
    use mysql_async::prelude::*;

    let row: Option<PoiItem> = conn
        .exec_first(
            r#"
            SELECT
                id, name, COALESCE(name_en, '') as name_en,
                lat, lng,
                COALESCE(category, '') as category,
                COALESCE(subcategory, '') as subcategory,
                COALESCE(address, '') as address,
                COALESCE(city, '') as city,
                COALESCE(phone, '') as phone,
                COALESCE(website, '') as website,
                COALESCE(opening_hours, '') as opening_hours,
                0.0 AS distance_m
            FROM osm_places
            WHERE id = :id
            "#,
            mysql_async::params! { "id" => id },
        )
        .await
        .map_err(|e| Status::internal(format!("Query error: {e}")))?
        .map(row_to_poi);

    row.ok_or_else(|| Status::not_found(format!("POI {id} tidak ditemukan")))
}

// ── Row mapper ────────────────────────────────────────────────────────────────

fn row_to_poi(row: mysql_async::Row) -> PoiItem {
    use mysql_async::prelude::FromRow;

    let id: i64 = row.get(0).unwrap_or(0);
    PoiItem {
        id: id.to_string(),
        name: row.get(1).unwrap_or_default(),
        name_en: row.get(2).unwrap_or_default(),
        lat: row.get(3).unwrap_or(0.0),
        lng: row.get(4).unwrap_or(0.0),
        category: row.get(5).unwrap_or_default(),
        subcategory: row.get(6).unwrap_or_default(),
        address: row.get(7).unwrap_or_default(),
        city: row.get(8).unwrap_or_default(),
        phone: row.get(9).unwrap_or_default(),
        website: row.get(10).unwrap_or_default(),
        opening_hours: row.get(11).unwrap_or_default(),
        distance_m: row.get::<f64, _>(12).unwrap_or(0.0) as f32,
    }
}
