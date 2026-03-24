use anyhow::Result;
use mysql_async::Value;
use ulid::Ulid;
// ── ULID helpers ──────────────────────────────────────────────────────────────

/// Generate ULID baru sebagai display string (26 char Crockford base32).
/// Dipakai di service layer — model.id tetap String.
/// Konversi ke [u8; 16] terjadi di repo layer saat INSERT/WHERE.
pub fn new_ulid() -> String {
    Ulid::new().to_string()
}

/// Konversi ULID string → [u8; 16] untuk parameter DB (BINARY(16)).
/// Dipakai di repo layer sebelum exec_drop / exec_rows.
pub fn ulid_to_bytes(s: &str) -> Result<[u8; 16]> {
    s.parse::<Ulid>()
        .map(|u| u.to_bytes())
        .map_err(|e| anyhow::anyhow!("Invalid ULID '{}': {}", s, e))
}

/// Konversi bytes dari DB (BINARY(16)) → ULID display string.
/// Dipakai di repo layer saat row mapper.
pub fn bytes_to_ulid(b: Vec<u8>) -> Result<String> {
    let arr: [u8; 16] = b
        .try_into()
        .map_err(|_| anyhow::anyhow!("Expected 16 bytes for ULID"))?;
    Ok(Ulid::from_bytes(arr).to_string())
}

/// Konversi Vec<u8> (16 bytes dari BINARY(16)) → ULID display string
pub fn bin_to_ulid(raw: Vec<u8>) -> Result<String> {
    let arr: [u8; 16] = raw
        .try_into()
        .map_err(|_| anyhow::anyhow!("Expected 16 bytes for ULID"))?;
    Ok(Ulid::from_bytes(arr).to_string())
}

/// Sama tapi untuk kolom nullable — Value::NULL → None
pub fn bin_to_ulid_opt(val: Value) -> Result<Option<String>> {
    match val {
        Value::NULL => Ok(None),
        Value::Bytes(b) => Ok(Some(bin_to_ulid(b)?)),
        other => anyhow::bail!("Unexpected value type for nullable BINARY(16): {:?}", other),
    }
}

// ── Mime helpers ──────────────────────────────────────────────────────────────

pub fn mime_to_type(mime: &str) -> &'static str {
    if mime.starts_with("image/") {
        "image"
    } else if mime.starts_with("video/") {
        "video"
    } else if mime.starts_with("audio/") {
        "audio"
    } else {
        "file"
    }
}
