use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use tokio_postgres::Row;

use super::db::{col_opt_i32, col_opt_i64, col_opt_str, exec_drop, exec_first, exec_rows};
use crate::utils::ulid::{bin_to_ulid, id_to_vec, ulid_to_vec};
use crate::{models::message::Message, proto::message::ConversationItem};

pub struct MessageRepository {
    pool: Pool,
}

impl MessageRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    const MSG_COLS: &'static str = r#"
        m.id,
        m.sender_id,
        m.recipient_id,
        m.content,
        m.msg_type,
        m.order_id,
        m.media_url,
        m.media_mime,
        m.media_size,
        m.media_duration,
        m.media_thumb,
        m.sent_at,
        m.delivered_at,
        m.read_at,
        u.name       AS sender_name,
        u.avatar_url AS sender_avatar,
        COALESCE(o.status, '') AS status_order
    "#;

    fn row_to_message(row: &Row) -> Result<Message> {
        let id_bytes: Vec<u8> = row.try_get("id")?;
        let sender_bytes: Vec<u8> = row.try_get("sender_id")?;
        let recipient_bytes: Vec<u8> = row.try_get("recipient_id")?;
        let order_bytes: Vec<u8> = row.try_get("order_id")?;

        Ok(Message {
            id: bin_to_ulid(id_bytes)?,
            sender_id: bin_to_ulid(sender_bytes)?,
            recipient_id: bin_to_ulid(recipient_bytes)?,
            order_id: bin_to_ulid(order_bytes)?,
            content: row.try_get("content")?,
            msg_type: row.try_get("msg_type")?,
            media_url: col_opt_str(row, "media_url")?,
            media_mime: col_opt_str(row, "media_mime")?,
            media_size: col_opt_i64(row, "media_size"),
            media_duration: col_opt_i32(row, "media_duration"),
            media_thumb: col_opt_str(row, "media_thumb")?,
            sent_at: row.try_get("sent_at")?,
            delivered_at: row.try_get("delivered_at")?,
            read_at: row.try_get("read_at")?,
            sender_name: col_opt_str(row, "sender_name")?,
            sender_avatar: col_opt_str(row, "sender_avatar")?,
            status_order: col_opt_str(row, "status_order")?,
        })
    }

    // ── INSERT ────────────────────────────────────────────────────────────────
    pub async fn save(&self, msg: &mut Message) -> Result<()> {
        let id_b = ulid_to_vec(&msg.id)?;
        let sender_b = id_to_vec(&msg.sender_id)?;
        let recipient_b = id_to_vec(&msg.recipient_id)?;
        let order_b = id_to_vec(&msg.order_id)?;

        exec_drop(
            &self.pool,
            r#"
            INSERT INTO messages
                (id, sender_id, recipient_id, order_id, content, msg_type,
                 media_url, media_mime, media_size, media_duration, media_thumb,
                 sent_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            "#,
            &[
                &id_b,
                &sender_b,
                &recipient_b,
                &order_b,
                &msg.content.as_str(),
                &msg.msg_type.as_str(),
                &msg.media_url.as_deref(),
                &msg.media_mime.as_deref(),
                &msg.media_size,
                &msg.media_duration,
                &msg.media_thumb.as_deref(),
                &msg.sent_at,
            ],
        )
        .await
    }

    // ── Daftar percakapan ─────────────────────────────────────────────────────
    //
    // Tujuan: menampilkan daftar siapa saja yang pernah chat dengan user ($1),
    // satu baris per peer, beserta pesan terakhir dan jumlah unread.
    //
    // Alur query:
    //  1. CTE `raw`    — kumpulkan semua pesan yang melibatkan $1, hitung peer_id
    //                    (lawan bicara) dari tiap pesan.
    //  2. CTE `latest` — dari `raw`, ambil 1 pesan terbaru per peer_id menggunakan
    //                    DISTINCT ON. DISTINCT ON harus ada di subquery terpisah
    //                    karena peer_id adalah computed column — tidak bisa dirujuk
    //                    langsung di SELECT yang sama.
    //  3. CTE `unread` — hitung pesan masuk yang belum dibaca, digroup per peer
    //                    (sender_id = peer yang mengirim ke $1).
    //  4. SELECT akhir — join ke users, orders, dan unread untuk melengkapi data.
    pub async fn get_conversations(&self, user_id: &str) -> Result<Vec<ConversationItem>> {
        let user_b = id_to_vec(user_id)?;

        let q = r#"
            WITH raw AS (
                -- Tentukan siapa peer untuk setiap pesan yang melibatkan $1
                SELECT
                    CASE
                        WHEN m.sender_id = $1 THEN m.recipient_id
                        ELSE m.sender_id
                    END          AS peer_id,
                    m.id         AS msg_id,
                    m.content    AS last_message,
                    m.msg_type   AS last_msg_type,
                    m.sent_at    AS last_sent_at,
                    m.order_id
                FROM messages m
                WHERE m.sender_id = $1
                   OR m.recipient_id = $1
            ),
            latest AS (
                -- Satu baris per peer, ambil pesan paling baru
                -- ORDER BY harus: peer_id dulu (untuk DISTINCT ON), lalu sent_at DESC
                -- agar baris yang "dipilih" adalah yang terbaru
                SELECT DISTINCT ON (peer_id)
                    peer_id,
                    last_message,
                    last_msg_type,
                    last_sent_at,
                    order_id
                FROM raw
                ORDER BY peer_id, last_sent_at DESC
            ),
            unread AS (
                -- Hitung pesan yang belum dibaca dari tiap peer ke $1
                SELECT
                    sender_id        AS peer_id,
                    COUNT(*)::BIGINT AS unread_count
                FROM messages
                WHERE recipient_id = $1
                  AND read_at IS NULL
                GROUP BY sender_id
            )
            SELECT
                l.peer_id,
                COALESCE(u.name,          '')  AS peer_name,
                COALESCE(u.avatar_url,    '')  AS peer_avatar,
                COALESCE(l.last_message,  '')  AS last_message,
                COALESCE(l.last_msg_type, '')  AS last_msg_type,
                l.last_sent_at,
                l.order_id,
                COALESCE(o.status,        '')  AS order_status,
                COALESCE(un.unread_count,  0)  AS unread_count
            FROM latest l
            LEFT JOIN users  u  ON u.id  = l.peer_id
            LEFT JOIN orders o  ON o.id  = l.order_id
            LEFT JOIN unread un ON un.peer_id = l.peer_id
            ORDER BY l.last_sent_at DESC
        "#;

        let rows = exec_rows(&self.pool, q, &[&user_b]).await?;

        rows.iter()
            .map(|r| -> Result<ConversationItem> {
                let peer_bytes: Vec<u8> = r.try_get("peer_id")?;
                let order_bytes: Vec<u8> = r.try_get("order_id")?;
                let last_sent: DateTime<Utc> = r.try_get("last_sent_at")?;

                let unread_raw: i64 = r.try_get("unread_count")?;
                let unread_count = u32::try_from(unread_raw).unwrap_or(u32::MAX);

                Ok(ConversationItem {
                    peer_id: bin_to_ulid(peer_bytes)?,
                    peer_name: r.try_get("peer_name")?,
                    peer_avatar: r.try_get("peer_avatar")?,
                    last_message: r.try_get("last_message")?,
                    last_message_type: r.try_get("last_msg_type")?,
                    // last_sent_at = waktu pesan terakhir dikirim di percakapan ini
                    last_seen_at: last_sent.to_rfc3339(),
                    unread_count,
                    order_id: bin_to_ulid(order_bytes)?,
                    order_status: r.try_get("order_status")?,
                })
            })
            .collect()
    }

    // ── Riwayat pesan antara dua user (cursor-based pagination) ───────────────
    //
    // Tujuan: mengambil pesan antara user_id dan peer_id, diurutkan kronologis
    // dari yang lama ke baru, dengan pagination mundur (load older messages).
    //
    // Mekanisme cursor:
    //  - Client kirim `before_id` = msg_id pesan tertua yang sudah ditampilkan.
    //  - Query cari pesan yang lebih lama dari cursor tersebut.
    //  - Tie-break dengan `m.id` karena ULID monotonic — aman untuk byte compare.
    //
    // Deteksi has_more:
    //  - Repo ambil `limit + 1` baris.
    //  - Jika dapat lebih dari `limit`, berarti masih ada pesan lebih lama.
    //  - Baris extra dibuang sebelum dikembalikan.
    //
    // Return urutan:
    //  - Query ambil DESC (terbaru dulu, efisien untuk LIMIT).
    //  - Setelah collect, di-reverse → kronologis ASC untuk tampilan chat.
    pub async fn get_history(
        &self,
        user_id: &str,
        peer_id: &str,
        limit: i64,
        before_id: Option<&str>,
    ) -> Result<(Vec<Message>, bool)> {
        let user_b = id_to_vec(user_id)?;
        let peer_b = id_to_vec(peer_id)?;
        let fetch = limit + 1; // 1 extra untuk deteksi has_more

        let rows = if let Some(before) = before_id {
            let before_b = id_to_vec(before)?;

            // Lookup cursor: butuh sent_at DAN id untuk tie-break
            // (dua pesan bisa punya sent_at identik)
            let cursor_row = exec_first(
                &self.pool,
                "SELECT sent_at, id FROM messages WHERE id = $1",
                &[&before_b],
            )
            .await?
            .ok_or_else(|| anyhow!("cursor message not found: {}", before))?;

            let cursor_time: DateTime<Utc> = cursor_row.try_get("sent_at")?;
            let cursor_id: Vec<u8> = cursor_row.try_get("id")?;

            // Kondisi "lebih lama dari cursor":
            //   - sent_at lebih kecil, ATAU
            //   - sent_at sama persis tapi id lebih kecil (ULID tie-break)
            // Seluruh filter percakapan dibungkus kurung agar AND berlaku
            // untuk kedua sisi OR — tanpa kurung, AND hanya menempel ke
            // cabang OR terakhir (operator precedence SQL).
            let q = format!(
                r#"
                SELECT {cols}
                FROM   messages m
                JOIN   users u
                    ON u.id = m.sender_id
                LEFT JOIN orders o
                    ON o.id = m.order_id
                WHERE (
                        (m.sender_id = $1 AND m.recipient_id = $2)
                     OR (m.sender_id = $2 AND m.recipient_id = $1)
                )
                  AND (
                        m.sent_at < $3
                     OR (m.sent_at = $3 AND m.id < $4)
                )
                ORDER BY m.sent_at DESC, m.id DESC
                LIMIT $5
                "#,
                cols = Self::MSG_COLS
            );

            exec_rows(
                &self.pool,
                &q,
                &[&user_b, &peer_b, &cursor_time, &cursor_id, &fetch],
            )
            .await?
        } else {
            // Tanpa cursor — ambil N pesan terbaru dari percakapan ini
            let q = format!(
                r#"
                SELECT {cols}
                FROM   messages m
                JOIN   users u
                    ON u.id = m.sender_id
                LEFT JOIN orders o
                    ON o.id = m.order_id
                WHERE (
                        (m.sender_id = $1 AND m.recipient_id = $2)
                     OR (m.sender_id = $2 AND m.recipient_id = $1)
                )
                ORDER BY m.sent_at DESC, m.id DESC
                LIMIT $3
                "#,
                cols = Self::MSG_COLS
            );

            exec_rows(&self.pool, &q, &[&user_b, &peer_b, &fetch]).await?
        };

        // Jika dapat lebih dari limit, masih ada pesan lebih lama
        let has_more = rows.len() > limit as usize;

        // Buang baris extra sebelum diproses
        let rows = if has_more {
            &rows[..limit as usize]
        } else {
            &rows[..]
        };

        // Query ambil DESC → reverse agar tampil kronologis ASC di chat
        let mut msgs = rows
            .iter()
            .map(Self::row_to_message)
            .collect::<Result<Vec<_>>>()?;

        msgs.reverse();
        Ok((msgs, has_more))
    }

    pub async fn mark_read(&self, reader_id: &str, sender_id: &str) -> Result<()> {
        let reader_b = id_to_vec(reader_id)?;
        let sender_b = id_to_vec(sender_id)?;

        exec_drop(
            &self.pool,
            r#"
            UPDATE messages
            SET    read_at = NOW()
            WHERE  recipient_id = $1
              AND  sender_id    = $2
              AND  read_at IS NULL
            "#,
            &[&reader_b, &sender_b],
        )
        .await
    }

    pub async fn mark_delivered(&self, msg_id: &str) -> Result<()> {
        let msg_b = id_to_vec(msg_id)?;

        exec_drop(
            &self.pool,
            r#"
            UPDATE messages
            SET    delivered_at = NOW()
            WHERE  id           = $1
              AND  delivered_at IS NULL
            "#,
            &[&msg_b],
        )
        .await
    }
}
