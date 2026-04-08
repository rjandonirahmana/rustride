use std::time::Duration;

use anyhow::{Context, Result};
use deadpool_postgres::Pool;
use tokio_postgres::Row;

// ── Pool helper ───────────────────────────────────────────────────────────────

pub async fn get_conn(pool: &Pool) -> Result<deadpool_postgres::Object> {
    match tokio::time::timeout(Duration::from_secs(5), pool.get()).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(e)) => Err(e).context("Failed to get connection from pool"),
        // BUG LAMA: Err(e) => anyhow::bail!("... {:?}", e)
        //   — tokio::time::Elapsed tidak implement Debug dengan info berguna
        //   — lebih bersih tanpa binding variabel yang tidak dipakai
        Err(_) => anyhow::bail!("Timeout getting connection from pool"),
    }
}

// ── Row accessor helpers ──────────────────────────────────────────────────────

pub fn col_opt_str(row: &Row, name: &str) -> Result<Option<String>> {
    row.try_get::<_, Option<String>>(name)
        .with_context(|| format!("Column '{}' not found or wrong type", name))
}

pub fn col_opt_f64(row: &Row, name: &str) -> Option<f64> {
    row.try_get::<_, Option<f64>>(name).ok().flatten()
}

pub fn col_opt_f32(row: &Row, name: &str) -> Option<f32> {
    if let Ok(Some(v)) = row.try_get::<_, Option<f64>>(name) {
        return Some(v as f32);
    }
    row.try_get::<_, Option<f32>>(name).ok().flatten()
}

pub fn col_opt_i32(row: &Row, name: &str) -> Option<i32> {
    row.try_get::<_, Option<i32>>(name).ok().flatten()
}

pub fn col_opt_i64(row: &Row, name: &str) -> Option<i64> {
    row.try_get::<_, Option<i64>>(name).ok().flatten()
}

pub fn f32_col(row: &Row, name: &str) -> Result<f32> {
    if let Ok(v) = row.try_get::<_, f64>(name) {
        return Ok(v as f32);
    }
    row.try_get::<_, f32>(name)
        .with_context(|| format!("f32_col '{}' failed", name))
}

pub fn i32_col(row: &Row, name: &str) -> Result<i32> {
    if let Ok(v) = row.try_get::<_, i64>(name) {
        return Ok(v as i32);
    }
    if let Ok(v) = row.try_get::<_, f64>(name) {
        return Ok(v as i32);
    }
    row.try_get::<_, i32>(name)
        .with_context(|| format!("i32_col '{}' failed", name))
}

// ── Query helpers ─────────────────────────────────────────────────────────────

/// Jalankan query yang tidak mengembalikan rows (INSERT tanpa RETURNING / UPDATE / DELETE).
pub async fn exec_drop(
    pool: &Pool,
    query: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<()> {
    // BUG LAMA: ada loop debug type_name_of_val di setiap exec_drop — sangat noisy di prod
    // FIX: dihapus; gunakan tracing::debug! di caller kalau memang perlu
    let conn = get_conn(pool).await?;
    conn.execute(query, params)
        .await
        .with_context(|| "exec_drop failed")?;
    Ok(())
}

/// Jalankan query, kembalikan semua rows.
pub async fn exec_rows(
    pool: &Pool,
    query: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Vec<Row>> {
    let conn = get_conn(pool).await?;
    conn.query(query, params).await.map_err(|e| {
        tracing::error!(
            error = %e,
            query,
            params_count = params.len(),
            "exec_rows failed"
        );
        // BUG LAMA: Err(e).context(format!("internal error"))
        //   — pesan "internal error" tidak informatif, format! tidak perlu
        // FIX: wrap error asli dengan anyhow
        anyhow::anyhow!("exec_rows failed: {e}")
    })
}

/// Jalankan query, kembalikan row pertama atau `None` kalau kosong.
pub async fn exec_first(
    pool: &Pool,
    query: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Option<Row>> {
    let rows = exec_rows(pool, query, params).await?;
    Ok(rows.into_iter().next())
}

/// Jalankan query, kembalikan tepat satu row — error kalau tidak ada.
/// Dipakai untuk INSERT ... RETURNING atau lookup yang dijamin ada.
pub async fn exec_one(
    pool: &Pool,
    query: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Row> {
    exec_first(pool, query, params)
        .await?
        .ok_or_else(|| anyhow::anyhow!("exec_one: query returned no rows"))
}
