use std::time::Duration;

/// Thin wrapper di atas deadpool_postgres::Pool.
/// Semua repository clone Pool-nya — Pool sudah Arc di dalamnya.
use anyhow::{Context, Result};
use deadpool_postgres::Pool;
use tokio_postgres::Row;

// ── Pool helper ───────────────────────────────────────────────────────────────

/// Ambil koneksi dari pool.
pub async fn get_conn(pool: &Pool) -> Result<deadpool_postgres::Object> {
    match tokio::time::timeout(Duration::from_secs(5), pool.get()).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(e)) => Err(e).context("Failed to get connection from pool"),
        Err(e) => anyhow::bail!("Timeout getting connection from pool {:?}", e),
    }
}
// ── Row accessor helpers ──────────────────────────────────────────────────────
// tokio-postgres: row.get::<_, T>(name) — panic kalau nama kolom salah.
// Helpers di bawah wrap dengan try_get agar error bisa di-propagate.

/// Ambil kolom T (error kalau kolom tidak ada atau tipe salah).
pub fn col<T: for<'a> tokio_postgres::types::FromSql<'a>>(row: &Row, name: &str) -> Result<T> {
    row.try_get::<_, T>(name)
        .with_context(|| format!("Column '{}' not found or wrong type", name))
}

/// Ambil kolom String opsional (NULL → None).
pub fn col_opt_str(row: &Row, name: &str) -> Result<Option<String>> {
    row.try_get::<_, Option<String>>(name)
        .with_context(|| format!("Column '{}' not found or wrong type", name))
}

/// Ambil kolom f64 opsional.
pub fn col_opt_f64(row: &Row, name: &str) -> Option<f64> {
    row.try_get::<_, Option<f64>>(name).ok().flatten()
}

/// Ambil kolom f32 opsional.
/// tokio-postgres tidak punya native f32 dari NUMERIC/FLOAT4 — baca f64 lalu cast.
pub fn col_opt_f32(row: &Row, name: &str) -> Option<f32> {
    // Coba f64 dulu (NUMERIC, FLOAT8), fallback ke f32 native (FLOAT4)
    if let Ok(Some(v)) = row.try_get::<_, Option<f64>>(name) {
        return Some(v as f32);
    }
    row.try_get::<_, Option<f32>>(name).ok().flatten()
}

/// Ambil kolom i32 opsional.
pub fn col_opt_i32(row: &Row, name: &str) -> Option<i32> {
    row.try_get::<_, Option<i32>>(name).ok().flatten()
}

/// Ambil kolom i64 opsional.
pub fn col_opt_i64(row: &Row, name: &str) -> Option<i64> {
    row.try_get::<_, Option<i64>>(name).ok().flatten()
}

/// Baca kolom NUMERIC(x,y) / FLOAT8 / FLOAT4 sebagai f32.
/// Berbeda dari col_opt_f32 — ini error kalau NULL bukan return None.
pub fn f32_col(row: &Row, name: &str) -> Result<f32> {
    // tokio-postgres: NUMERIC → rust_decimal, FLOAT8 → f64, FLOAT4 → f32
    // Coba f64 dulu (FLOAT8 / NUMERIC cast), lalu f32
    if let Ok(v) = row.try_get::<_, f64>(name) {
        return Ok(v as f32);
    }
    row.try_get::<_, f32>(name)
        .with_context(|| format!("f32_col '{}' failed", name))
}

// ── Query helpers ─────────────────────────────────────────────────────────────

pub async fn exec_drop(
    pool: &Pool,
    query: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<()> {
    let conn = get_conn(pool).await?;

    // Debug tipe setiap parameter
    for (i, param) in params.iter().enumerate() {
        tracing::debug!(
            "Param {} type: {:?}",
            i + 1,
            std::any::type_name_of_val(param)
        );
    }

    conn.execute(query, params).await?;
    Ok(())
}

/// Jalankan query, kembalikan semua rows.
pub async fn exec_rows(
    pool: &Pool,
    query: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Vec<Row>> {
    let conn = get_conn(pool).await?;
    match conn.query(query, params).await {
        Ok(rows) => Ok(rows),
        Err(e) => {
            tracing::error!(
                error = %e,
                query = %query,
                params_count = params.len(),
                "Database query failed"
            );
            Err(e).context(format!("internal error")) // ← lebih baik pakai context
        }
    }
}
pub async fn exec_first(
    pool: &Pool,
    query: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Option<Row>> {
    let rows = exec_rows(pool, query, params).await?;
    Ok(rows.into_iter().next())
}
