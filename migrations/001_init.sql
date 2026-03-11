-- migrations/001_init.sql  (dari soal, sudah lengkap)
CREATE DATABASE IF NOT EXISTS ridehailing
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ridehailing;

CREATE TABLE IF NOT EXISTS users (
    id          VARCHAR(36)  NOT NULL,
    name        VARCHAR(150) NOT NULL,
    phone       VARCHAR(20)  NOT NULL UNIQUE,
    email       VARCHAR(255) NOT NULL UNIQUE,
    password    VARCHAR(255) NOT NULL,
    role        ENUM('rider','driver') NOT NULL,
    avatar_url  VARCHAR(500) NULL,
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    INDEX idx_phone (phone),
    INDEX idx_role  (role)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS driver_profiles (
    user_id         VARCHAR(36)  NOT NULL,
    vehicle_type    ENUM('motor','mobil') NOT NULL DEFAULT 'motor',
    vehicle_plate   VARCHAR(20)  NOT NULL,
    vehicle_model   VARCHAR(100) NOT NULL,
    vehicle_color   VARCHAR(50)  NOT NULL,
    rating          DECIMAL(3,2) NOT NULL DEFAULT 5.00,
    total_trips     INT          NOT NULL DEFAULT 0,
    is_active       TINYINT(1)   NOT NULL DEFAULT 0,
    created_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id),
    CONSTRAINT fk_dp_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS orders (
    id              VARCHAR(36)   NOT NULL,
    rider_id        VARCHAR(36)   NOT NULL,
    driver_id       VARCHAR(36)   NULL,
    status          VARCHAR(20)   NOT NULL DEFAULT 'searching',
    pickup_lat      DECIMAL(10,8) NOT NULL,
    pickup_lng      DECIMAL(11,8) NOT NULL,
    pickup_address  VARCHAR(500)  NOT NULL,
    dest_lat        DECIMAL(10,8) NOT NULL,
    dest_lng        DECIMAL(11,8) NOT NULL,
    dest_address    VARCHAR(500)  NOT NULL,
    distance_km     DECIMAL(8,2)  NULL,
    fare_estimate   INT           NOT NULL,
    fare_final      INT           NULL,
    service_type    ENUM('motor','mobil') NOT NULL DEFAULT 'motor',
    created_at      DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    driver_found_at DATETIME(3)   NULL,
    accepted_at     DATETIME(3)   NULL,
    arrived_at      DATETIME(3)   NULL,
    started_at      DATETIME(3)   NULL,
    completed_at    DATETIME(3)   NULL,
    cancelled_at    DATETIME(3)   NULL,
    cancel_reason   VARCHAR(255)  NULL,
    rider_rating    TINYINT       NULL,
    driver_rating   TINYINT       NULL,
    PRIMARY KEY (id),
    INDEX idx_rider_id  (rider_id),
    INDEX idx_driver_id (driver_id),
    INDEX idx_status    (status),
    INDEX idx_created   (created_at),
    CONSTRAINT fk_order_rider  FOREIGN KEY (rider_id)  REFERENCES users(id),
    CONSTRAINT fk_order_driver FOREIGN KEY (driver_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Demo seed (password = "password123")
INSERT IGNORE INTO users (id, name, phone, email, password, role) VALUES
(
    'rider-demo-0001-0001-000000000001',
    'Budi Rider', '08111111111', 'budi@example.com',
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj4oLbcGFpK2',
    'rider'
),
(
    'driver-demo-0001-0001-00000000001',
    'Andi Driver', '08222222222', 'andi@example.com',
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj4oLbcGFpK2',
    'driver'
);

INSERT IGNORE INTO driver_profiles
    (user_id, vehicle_type, vehicle_plate, vehicle_model, vehicle_color, is_active)
VALUES
    ('driver-demo-0001-0001-00000000001', 'motor', 'B 1234 ABC', 'Honda Beat', 'Hitam', 1);



CREATE TABLE IF NOT EXISTS messages (
    id            VARCHAR(100)   NOT NULL,
    sender_id     VARCHAR(36)   NOT NULL,
    recipient_id  VARCHAR(36)   NOT NULL,
    order_id      VARCHAR(36)   NOT NULL,
    content       TEXT           NOT NULL,
    msg_type      VARCHAR(20)    NOT NULL DEFAULT 'text',
    media_url     VARCHAR(1000)  NULL,
    media_mime    VARCHAR(100)   NULL,
    media_size    BIGINT         NULL,
    media_duration INT           NULL,
    media_thumb   VARCHAR(1000)  NULL,
    sent_at       DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    delivered_at  DATETIME(3)    NULL,
    read_at       DATETIME(3)    NULL,
    PRIMARY KEY (id),
    INDEX idx_conversation     (sender_id, recipient_id, sent_at),
    INDEX idx_recipient_unread (recipient_id, read_at),
    INDEX idx_sent_at          (sent_at),
    CONSTRAINT fk_messages_sender
        FOREIGN KEY (sender_id)    REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_messages_recipient
        FOREIGN KEY (recipient_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_messages_order
        FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE OR REPLACE VIEW conversations AS
    SELECT
        LEAST(sender_id, recipient_id)    AS user_a,
        GREATEST(sender_id, recipient_id) AS user_b,
        MAX(sent_at)                      AS last_message_at,
        COUNT(*)                          AS message_count
    FROM messages
    GROUP BY user_a, user_b;


CREATE TABLE IF NOT EXISTS ratings (
    id          VARCHAR(36)  NOT NULL,
    order_id    VARCHAR(36)  NOT NULL,
    poster_id   VARCHAR(36)  NOT NULL, -- Siapa yang memberi rating
    target_id   VARCHAR(36)  NOT NULL, -- Siapa yang menerima rating
    score       TINYINT      NOT NULL CHECK (score BETWEEN 1 AND 5),
    comment     TEXT         NULL,
    tip_amount  INT          NOT NULL DEFAULT 0, -- Tip dalam satuan rupiah
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    INDEX idx_order  (order_id),
    INDEX idx_target (target_id),
    CONSTRAINT fk_rating_order  FOREIGN KEY (order_id)  REFERENCES orders(id) ON DELETE CASCADE,
    CONSTRAINT fk_rating_poster FOREIGN KEY (poster_id) REFERENCES users(id),
    CONSTRAINT fk_rating_target FOREIGN KEY (target_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



-- ============================================================
-- Migration 002: Notifications + Rideshare (Nebeng)
-- ============================================================

USE ridehailing;

-- ============================================================
-- NOTIFICATIONS
-- ============================================================
-- Type reference:
--   order_completed   → order selesai
--   order_cancelled   → order dibatalkan
--   driver_accepted   → driver accept order
--   driver_arrived    → driver tiba di pickup
--   trip_started      → perjalanan dimulai
--   wallet_topup      → saldo wallet masuk
--   promo_available   → promo baru tersedia
--   system            → pengumuman sistem
--   nebeng_request    → ada rider minta nebeng
--   nebeng_accepted   → request nebeng diterima driver
--   nebeng_rejected   → request nebeng ditolak driver
-- ============================================================

CREATE TABLE IF NOT EXISTS notifications (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_id         VARCHAR(36)     NOT NULL,           -- penerima notifikasi
    type            VARCHAR(50)     NOT NULL,           -- lihat komentar di atas
    title           VARCHAR(255)    NOT NULL,
    body            TEXT            NOT NULL,
    -- referensi opsional ke entitas terkait
    ref_order_id    VARCHAR(36)     NULL,
    ref_user_id     VARCHAR(36)     NULL,               -- misal: driver/rider terkait
    -- metadata tambahan (JSON) untuk deep link / payload extra
    meta            JSON            NULL,
    is_read         TINYINT(1)      NOT NULL DEFAULT 0,
    read_at         DATETIME(3)     NULL,
    created_at      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (id),
    INDEX idx_notif_user        (user_id, created_at DESC),
    INDEX idx_notif_unread      (user_id, is_read),
    INDEX idx_notif_ref_order   (ref_order_id),

    CONSTRAINT fk_notif_user
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_notif_ref_order
        FOREIGN KEY (ref_order_id) REFERENCES orders(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
-- RIDESHARE / NEBENG
-- ============================================================
--
-- Arsitektur:
--
--   orders (existing)
--   └── satu "host order" milik driver (status normal)
--
--   rideshare_trips
--   └── metadata trip bersama: max kapasitas, rute driver, status
--
--   rideshare_passengers
--   └── setiap rider yang nebeng punya row di sini
--       masing-masing dengan pickup/dest sendiri + fare sendiri
--
-- Flow:
--   1. Driver buat order normal → create_rideshare_trip(order_id, max_passengers)
--   2. Rider search → find_rideshare_trips(dest_lat, dest_lng, service_type)
--      (server cek apakah rute driver melewati/mendekati tujuan rider)
--   3. Rider request join → rideshare_passengers row dengan status=pending
--   4. Driver terima/tolak → status=accepted/rejected
--   5. Trip jalan → semua passenger ikut
--   6. Selesai → masing-masing rider dapat order_completed notification
-- ============================================================

CREATE TABLE IF NOT EXISTS rideshare_trips (
    id                  VARCHAR(36)     NOT NULL,
    host_order_id       VARCHAR(36)     NOT NULL UNIQUE,   -- order milik driver (anchor)
    driver_id           VARCHAR(36)     NOT NULL,
    service_type        ENUM('motor','mobil') NOT NULL DEFAULT 'mobil',

    -- Rute driver (untuk matching searah)
    route_start_lat     DECIMAL(10,8)   NOT NULL,
    route_start_lng     DECIMAL(11,8)   NOT NULL,
    route_end_lat       DECIMAL(10,8)   NOT NULL,
    route_end_lng       DECIMAL(11,8)   NOT NULL,
    route_polyline      TEXT            NULL,   -- encoded polyline opsional, untuk matching presisi

    -- Kapasitas
    max_passengers      TINYINT         NOT NULL DEFAULT 3,  -- tidak termasuk driver
    current_passengers  TINYINT         NOT NULL DEFAULT 0,

    -- Status trip bersama
    -- open       → masih bisa menerima penumpang
    -- full       → kapasitas penuh
    -- in_progress → sedang berjalan
    -- completed  → selesai
    -- cancelled  → dibatalkan
    status              VARCHAR(20)     NOT NULL DEFAULT 'open',

    -- Batas waktu rider bisa join (misal: 5 menit sebelum driver berangkat)
    join_deadline       DATETIME(3)     NULL,

    created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    started_at          DATETIME(3)     NULL,
    completed_at        DATETIME(3)     NULL,

    PRIMARY KEY (id),
    INDEX idx_rst_driver        (driver_id),
    INDEX idx_rst_status        (status),
    INDEX idx_rst_service       (service_type, status),
    -- Spatial-ish index untuk filter rough bounding box
    INDEX idx_rst_route_start   (route_start_lat, route_start_lng),
    INDEX idx_rst_route_end     (route_end_lat, route_end_lng),

    CONSTRAINT fk_rst_host_order
        FOREIGN KEY (host_order_id) REFERENCES orders(id) ON DELETE CASCADE,
    CONSTRAINT fk_rst_driver
        FOREIGN KEY (driver_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS rideshare_passengers (
    id                  VARCHAR(36)     NOT NULL,
    trip_id             VARCHAR(36)     NOT NULL,          -- → rideshare_trips.id
    rider_id            VARCHAR(36)     NOT NULL,

    -- Pickup & dest spesifik rider ini (bisa beda dengan driver)
    pickup_lat          DECIMAL(10,8)   NOT NULL,
    pickup_lng          DECIMAL(11,8)   NOT NULL,
    pickup_address      VARCHAR(500)    NOT NULL,
    dest_lat            DECIMAL(10,8)   NOT NULL,
    dest_lng            DECIMAL(11,8)   NOT NULL,
    dest_address        VARCHAR(500)    NOT NULL,

    -- Fare rider ini (dihitung sendiri, bukan split)
    fare_estimate       INT             NOT NULL DEFAULT 0,
    fare_final          INT             NULL,
    distance_km         DECIMAL(8,2)    NULL,

    -- Status per passenger:
    -- pending    → menunggu konfirmasi driver
    -- accepted   → driver terima
    -- rejected   → driver tolak
    -- picked_up  → sudah dijemput
    -- dropped_off → sudah diantar
    -- cancelled  → rider cancel sendiri
    status              VARCHAR(20)     NOT NULL DEFAULT 'pending',

    -- Timestamps per rider
    requested_at        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    accepted_at         DATETIME(3)     NULL,
    picked_up_at        DATETIME(3)     NULL,
    dropped_off_at      DATETIME(3)     NULL,
    cancelled_at        DATETIME(3)     NULL,
    cancel_reason       VARCHAR(255)    NULL,

    -- Rating dari rider ke driver (post-trip)
    rating              TINYINT         NULL CHECK (rating BETWEEN 1 AND 5),
    rating_comment      TEXT            NULL,
    tip_amount          INT             NOT NULL DEFAULT 0,

    PRIMARY KEY (id),
    UNIQUE KEY uq_trip_rider (trip_id, rider_id),     -- satu rider max 1x per trip
    INDEX idx_rsp_trip      (trip_id),
    INDEX idx_rsp_rider     (rider_id),
    INDEX idx_rsp_status    (status),

    CONSTRAINT fk_rsp_trip
        FOREIGN KEY (trip_id) REFERENCES rideshare_trips(id) ON DELETE CASCADE,
    CONSTRAINT fk_rsp_rider
        FOREIGN KEY (rider_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
-- TRIGGER: auto-update current_passengers di rideshare_trips
-- ============================================================

DROP TRIGGER IF EXISTS trg_rsp_accepted;
DELIMITER $$
CREATE TRIGGER trg_rsp_accepted
    AFTER UPDATE ON rideshare_passengers
    FOR EACH ROW
BEGIN
    -- Saat passenger di-accept, increment counter
    IF NEW.status = 'accepted' AND OLD.status != 'accepted' THEN
        UPDATE rideshare_trips
        SET current_passengers = current_passengers + 1,
            -- Auto-set full kalau sudah maks
            status = IF(current_passengers + 1 >= max_passengers, 'full', status)
        WHERE id = NEW.trip_id;
    END IF;

    -- Saat passenger cancel / reject setelah accepted, decrement
    IF OLD.status = 'accepted' AND NEW.status IN ('cancelled', 'rejected') THEN
        UPDATE rideshare_trips
        SET current_passengers = GREATEST(current_passengers - 1, 0),
            status = IF(status = 'full', 'open', status)
        WHERE id = NEW.trip_id;
    END IF;
END$$
DELIMITER ;


-- ============================================================
-- VIEW: rideshare_trips_open
-- Dipakai server untuk query "trip yang bisa diikuti rider"
-- Filter kasar di SQL, filter presisi (arah rute) di Rust
-- ============================================================

CREATE OR REPLACE VIEW rideshare_trips_open AS
    SELECT
        rt.id,
        rt.host_order_id,
        rt.driver_id,
        rt.service_type,
        rt.route_start_lat,
        rt.route_start_lng,
        rt.route_end_lat,
        rt.route_end_lng,
        rt.route_polyline,
        rt.max_passengers,
        rt.current_passengers,
        rt.max_passengers - rt.current_passengers   AS seats_available,
        rt.join_deadline,
        rt.created_at,
        u.name          AS driver_name,
        u.avatar_url    AS driver_avatar,
        dp.vehicle_plate,
        dp.vehicle_model,
        dp.vehicle_color,
        dp.rating       AS driver_rating
    FROM rideshare_trips rt
    JOIN users          u  ON u.id  = rt.driver_id
    JOIN driver_profiles dp ON dp.user_id = rt.driver_id
    WHERE rt.status IN ('open', 'full')   -- full tetap tampil (bisa berubah)
      AND (rt.join_deadline IS NULL OR rt.join_deadline > NOW());


-- ============================================================
-- Tambah kolom wallet ke users (dibutuhkan flow nebeng + notif topup)
-- Pakai IF NOT EXISTS-style workaround untuk MySQL
-- ============================================================

SET @col_exists = (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = 'ridehailing'
      AND TABLE_NAME   = 'users'
      AND COLUMN_NAME  = 'wallet_balance'
);

SET @sql = IF(
    @col_exists = 0,
    'ALTER TABLE users ADD COLUMN wallet_balance INT NOT NULL DEFAULT 0',
    'SELECT "wallet_balance already exists" AS info'
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;