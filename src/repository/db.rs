/// Thin wrapper di atas mysql_async::Pool.
/// Semua repository clone Pool-nya — Pool sudah Arc di dalamnya.
use anyhow::{Context, Result};
use mysql_async::{prelude::*, Params, Row, Value};

pub use mysql_async::{from_value, Pool};

/// Ambil kolom sebagai Value (error kalau kolom tidak ada / NULL).
pub fn col(row: &Row, name: &str) -> Result<Value> {
    let idx = row
        .columns_ref()
        .iter()
        .position(|c| c.name_str() == name)
        .ok_or_else(|| anyhow::anyhow!("Column '{}' not found", name))?;
    row.get::<Value, usize>(idx)
        .ok_or_else(|| anyhow::anyhow!("Column '{}' is NULL", name))
}

/// Ambil kolom string opsional (NULL → None).
pub fn col_opt_str(row: &Row, name: &str) -> Option<String> {
    let idx = row
        .columns_ref()
        .iter()
        .position(|c| c.name_str() == name)?;
    match row.get::<Value, usize>(idx)? {
        Value::NULL => None,
        v => Some(from_value::<String>(v)),
    }
}

/// Ambil kolom f32 opsional.
pub fn col_opt_f32(row: &Row, name: &str) -> Option<f32> {
    let idx = row
        .columns_ref()
        .iter()
        .position(|c| c.name_str() == name)?;
    match row.get::<Value, usize>(idx)? {
        Value::NULL => None,
        v => Some(from_value::<f64>(v) as f32),
    }
}

/// Ambil kolom f32 opsional.
pub fn col_opt_f64(row: &Row, name: &str) -> Option<f64> {
    let idx = row
        .columns_ref()
        .iter()
        .position(|c| c.name_str() == name)?;
    match row.get::<Value, usize>(idx)? {
        Value::NULL => None,
        v => Some(from_value::<f64>(v) as f64),
    }
}

/// Ambil kolom i32 opsional.
pub fn col_opt_i32(row: &Row, name: &str) -> Option<i32> {
    let idx = row
        .columns_ref()
        .iter()
        .position(|c| c.name_str() == name)?;
    match row.get::<Value, usize>(idx)? {
        Value::NULL => None,
        v => Some(from_value::<i32>(v)),
    }
}

pub async fn exec_drop(
    pool: &Pool,
    query: &str,
    params: impl Into<mysql_async::Params> + Send,
) -> Result<()> {
    let mut conn = pool.get_conn().await.context("db connection failed")?;
    conn.exec_drop(query, params.into()).await?;
    Ok(())
}

/// Helper: jalankan query, kembalikan rows.
pub async fn exec_rows(
    pool: &Pool,
    query: &str,
    params: impl Into<mysql_async::Params> + Send,
) -> Result<Vec<Row>> {
    let mut conn = pool.get_conn().await.context("db connection failed")?;
    conn.exec(query, params.into())
        .await
        .context("exec rows failed")
}

pub fn col_opt<T: FromValue>(row: &Row, name: &str) -> Option<T> {
    row.get(name).unwrap_or(None)
}

pub async fn exec_first<P: Into<Params> + Send>(
    pool: &Pool,
    query: &str,
    params: P,
) -> Result<Option<Row>> {
    let mut conn = pool.get_conn().await?;
    let row: Option<Row> = conn.exec_first(query, params).await?;
    Ok(row)
}

pub fn col_opt_i64(row: &Row, name: &str) -> Option<i64> {
    row.get::<Option<i64>, _>(name).unwrap_or(None)
}
