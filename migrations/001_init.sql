-- ============================================================
-- RustRide — Clean Schema (BINARY(16) dari awal)
-- Gabungan migration 001 + 002 + 003 + 004
--
-- ID generation:
--   Rust: let bytes = Ulid::new().to_bytes(); // [u8; 16]
--   Read: Ulid::from_bytes(raw.try_into()?).to_string()
--   Cargo.toml: ulid = "1"
-- ============================================================

DROP DATABASE IF EXISTS ridehailing;
CREATE DATABASE ridehailing
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ridehailing;

-- ============================================================
-- USERS
-- ============================================================

CREATE TABLE users (
    id              BINARY(16)   NOT NULL,
    name            VARCHAR(150) NOT NULL,
    phone           VARCHAR(20)  NOT NULL UNIQUE,
    email           VARCHAR(255) NOT NULL UNIQUE,
    password        VARCHAR(255) NOT NULL,
    role            ENUM('rider','driver') NOT NULL,
    avatar_url      VARCHAR(500) NULL,
    wallet_balance  INT          NOT NULL DEFAULT 0,
    created_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    INDEX idx_phone (phone),
    INDEX idx_role  (role)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- DRIVER_PROFILES
-- ============================================================

CREATE TABLE driver_profiles (
    user_id         BINARY(16)   NOT NULL,
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
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- ORDERS
-- ============================================================

CREATE TABLE orders (
    id              BINARY(16)    NOT NULL,
    rider_id        BINARY(16)    NOT NULL,
    driver_id       BINARY(16)    NULL,
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
    INDEX idx_rider_status  (rider_id,  status),
    INDEX idx_driver_status (driver_id, status),
    INDEX idx_rider_created (rider_id,  created_at DESC),
    INDEX idx_status        (status),
    INDEX idx_search_geo    (status, service_type, pickup_lat, pickup_lng),
    INDEX idx_id_status     (id, status),
    CONSTRAINT fk_order_rider  FOREIGN KEY (rider_id)  REFERENCES users(id),
    CONSTRAINT fk_order_driver FOREIGN KEY (driver_id) REFERENCES users(id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- MESSAGES
-- id: BINARY(16) — di-generate Rust (Ulid::new())
-- ============================================================

CREATE TABLE messages (
    id              BINARY(16)    NOT NULL,
    sender_id       BINARY(16)    NOT NULL,
    recipient_id    BINARY(16)    NOT NULL,
    order_id        BINARY(16)    NOT NULL,
    content         TEXT          NOT NULL,
    msg_type        VARCHAR(20)   NOT NULL DEFAULT 'text',
    media_url       VARCHAR(1000) NULL,
    media_mime      VARCHAR(100)  NULL,
    media_size      BIGINT        NULL,
    media_duration  INT           NULL,
    media_thumb     VARCHAR(1000) NULL,
    sent_at         DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    delivered_at    DATETIME(3)   NULL,
    read_at         DATETIME(3)   NULL,
    PRIMARY KEY (id),
    INDEX idx_conversation     (sender_id, recipient_id, sent_at),
    INDEX idx_recipient_unread (recipient_id, read_at),
    INDEX idx_sent_at          (sent_at),
    CONSTRAINT fk_messages_sender
        FOREIGN KEY (sender_id)    REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_messages_recipient
        FOREIGN KEY (recipient_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_messages_order
        FOREIGN KEY (order_id)     REFERENCES orders(id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- RATINGS
-- ============================================================

CREATE TABLE ratings (
    id          BINARY(16)   NOT NULL,
    order_id    BINARY(16)   NOT NULL,
    poster_id   BINARY(16)   NOT NULL,
    target_id   BINARY(16)   NOT NULL,
    score       TINYINT      NOT NULL CHECK (score BETWEEN 1 AND 5),
    comment     TEXT         NULL,
    tip_amount  INT          NOT NULL DEFAULT 0,
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    INDEX idx_order  (order_id),
    INDEX idx_target (target_id),
    CONSTRAINT fk_rating_order  FOREIGN KEY (order_id)  REFERENCES orders(id) ON DELETE CASCADE,
    CONSTRAINT fk_rating_poster FOREIGN KEY (poster_id) REFERENCES users(id),
    CONSTRAINT fk_rating_target FOREIGN KEY (target_id) REFERENCES users(id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- NOTIFICATIONS
-- id: BIGINT AUTO_INCREMENT (tidak perlu ULID)
-- ============================================================

CREATE TABLE notifications (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_id         BINARY(16)      NOT NULL,
    type            VARCHAR(50)     NOT NULL,
    title           VARCHAR(255)    NOT NULL,
    body            TEXT            NOT NULL,
    ref_order_id    BINARY(16)      NULL,
    ref_user_id     BINARY(16)      NULL,
    meta            JSON            NULL,
    is_read         TINYINT(1)      NOT NULL DEFAULT 0,
    read_at         DATETIME(3)     NULL,
    created_at      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    INDEX idx_notif_user      (user_id, created_at DESC),
    INDEX idx_notif_unread    (user_id, is_read),
    INDEX idx_notif_ref_order (ref_order_id),
    CONSTRAINT fk_notif_user
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_notif_ref_order
        FOREIGN KEY (ref_order_id) REFERENCES orders(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- RIDESHARE_TRIPS
-- ============================================================

CREATE TABLE rideshare_trips (
    id                  BINARY(16)    NOT NULL,
    host_order_id       BINARY(16)    NOT NULL UNIQUE,
    driver_id           BINARY(16)    NOT NULL,
    service_type        ENUM('motor','mobil') NOT NULL DEFAULT 'mobil',
    route_start_lat     DECIMAL(10,8) NOT NULL,
    route_start_lng     DECIMAL(11,8) NOT NULL,
    route_end_lat       DECIMAL(10,8) NOT NULL,
    route_end_lng       DECIMAL(11,8) NOT NULL,
    route_polyline      TEXT          NULL,
    max_passengers      TINYINT       NOT NULL DEFAULT 3,
    current_passengers  TINYINT       NOT NULL DEFAULT 0,
    status              VARCHAR(20)   NOT NULL DEFAULT 'open',
    join_deadline       DATETIME(3)   NULL,
    created_at          DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    started_at          DATETIME(3)   NULL,
    completed_at        DATETIME(3)   NULL,
    PRIMARY KEY (id),
    INDEX idx_rst_driver      (driver_id),
    INDEX idx_rst_status      (status),
    INDEX idx_rst_service     (service_type, status),
    INDEX idx_rst_route_start (route_start_lat, route_start_lng),
    INDEX idx_rst_route_end   (route_end_lat, route_end_lng),
    CONSTRAINT fk_rst_host_order
        FOREIGN KEY (host_order_id) REFERENCES orders(id) ON DELETE CASCADE,
    CONSTRAINT fk_rst_driver
        FOREIGN KEY (driver_id) REFERENCES users(id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- RIDESHARE_PASSENGERS
-- ============================================================

CREATE TABLE rideshare_passengers (
    id              BINARY(16)    NOT NULL,
    trip_id         BINARY(16)    NOT NULL,
    rider_id        BINARY(16)    NOT NULL,
    pickup_lat      DECIMAL(10,8) NOT NULL,
    pickup_lng      DECIMAL(11,8) NOT NULL,
    pickup_address  VARCHAR(500)  NOT NULL,
    dest_lat        DECIMAL(10,8) NOT NULL,
    dest_lng        DECIMAL(11,8) NOT NULL,
    dest_address    VARCHAR(500)  NOT NULL,
    fare_estimate   INT           NOT NULL DEFAULT 0,
    fare_final      INT           NULL,
    distance_km     DECIMAL(8,2)  NULL,
    status          VARCHAR(20)   NOT NULL DEFAULT 'pending',
    requested_at    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    accepted_at     DATETIME(3)   NULL,
    picked_up_at    DATETIME(3)   NULL,
    dropped_off_at  DATETIME(3)   NULL,
    cancelled_at    DATETIME(3)   NULL,
    cancel_reason   VARCHAR(255)  NULL,
    rating          TINYINT       NULL CHECK (rating BETWEEN 1 AND 5),
    rating_comment  TEXT          NULL,
    tip_amount      INT           NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY uq_trip_rider (trip_id, rider_id),
    INDEX idx_rsp_trip   (trip_id),
    INDEX idx_rsp_rider  (rider_id),
    INDEX idx_rsp_status (status),
    CONSTRAINT fk_rsp_trip
        FOREIGN KEY (trip_id)  REFERENCES rideshare_trips(id) ON DELETE CASCADE,
    CONSTRAINT fk_rsp_rider
        FOREIGN KEY (rider_id) REFERENCES users(id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- DRIVER_DAILY_SUMMARY
-- id: BIGINT AUTO_INCREMENT (tidak perlu ULID)
-- ============================================================

CREATE TABLE driver_daily_summary (
    id                  BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT,
    driver_id           BINARY(16)        NOT NULL,
    summary_date        DATE              NOT NULL,
    total_orders        SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    cancelled_orders    SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    gross_earnings      INT UNSIGNED      NOT NULL DEFAULT 0,
    platform_fee        INT UNSIGNED      NOT NULL DEFAULT 0,
    net_earnings        INT UNSIGNED      NOT NULL DEFAULT 0,
    tips                INT UNSIGNED      NOT NULL DEFAULT 0,
    online_minutes      SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    distance_km         DECIMAL(8,2)      NOT NULL DEFAULT 0.00,
    rating_sum          DECIMAL(8,2)      NOT NULL DEFAULT 0.00,
    rating_count        SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    avg_rating          DECIMAL(3,2)      NOT NULL DEFAULT 0.00,
    peak_hour           VARCHAR(15)       NULL,
    updated_at          DATETIME(3)       NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                            ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uq_driver_date (driver_id, summary_date),
    INDEX idx_dds_date   (summary_date),
    INDEX idx_dds_driver (driver_id),
    CONSTRAINT fk_dds_driver
        FOREIGN KEY (driver_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- VIEWS
-- ============================================================

CREATE OR REPLACE VIEW conversations AS
    SELECT
        LEAST(sender_id, recipient_id)    AS user_a,
        GREATEST(sender_id, recipient_id) AS user_b,
        MAX(sent_at)                      AS last_message_at,
        COUNT(*)                          AS message_count
    FROM messages
    GROUP BY user_a, user_b;

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
        rt.max_passengers - rt.current_passengers AS seats_available,
        rt.join_deadline,
        rt.created_at,
        u.name          AS driver_name,
        u.avatar_url    AS driver_avatar,
        dp.vehicle_plate,
        dp.vehicle_model,
        dp.vehicle_color,
        dp.rating       AS driver_rating
    FROM rideshare_trips rt
    JOIN users           u  ON u.id       = rt.driver_id
    JOIN driver_profiles dp ON dp.user_id = rt.driver_id
    WHERE rt.status IN ('open', 'full')
      AND (rt.join_deadline IS NULL OR rt.join_deadline > NOW());

CREATE OR REPLACE VIEW driver_today_summary AS
    SELECT
        dds.*,
        u.name          AS driver_name,
        dp.vehicle_type,
        dp.vehicle_plate
    FROM driver_daily_summary dds
    JOIN users           u  ON u.id       = dds.driver_id
    JOIN driver_profiles dp ON dp.user_id = dds.driver_id
    WHERE dds.summary_date = CURDATE();

-- ============================================================
-- TRIGGERS
-- ============================================================

DROP TRIGGER IF EXISTS trg_rsp_accepted;
DELIMITER $$
CREATE TRIGGER trg_rsp_accepted
    AFTER UPDATE ON rideshare_passengers
    FOR EACH ROW
BEGIN
    IF NEW.status = 'accepted' AND OLD.status != 'accepted' THEN
        UPDATE rideshare_trips
        SET current_passengers = current_passengers + 1,
            status = IF(current_passengers + 1 >= max_passengers, 'full', status)
        WHERE id = NEW.trip_id;
    END IF;

    IF OLD.status = 'accepted' AND NEW.status IN ('cancelled', 'rejected') THEN
        UPDATE rideshare_trips
        SET current_passengers = GREATEST(current_passengers - 1, 0),
            status = IF(status = 'full', 'open', status)
        WHERE id = NEW.trip_id;
    END IF;
END$$

DROP TRIGGER IF EXISTS trg_order_completed_summary;
CREATE TRIGGER trg_order_completed_summary
    AFTER UPDATE ON orders
    FOR EACH ROW
BEGIN
    DECLARE v_date DATE;

    IF NEW.status = 'completed' AND OLD.status != 'completed'
       AND NEW.driver_id IS NOT NULL THEN

        SET v_date = DATE(NEW.completed_at);

        SET @gross    = COALESCE(NEW.fare_final, NEW.fare_estimate);
        SET @platform = FLOOR(@gross * 0.20);
        SET @net      = @gross - @platform;
        SET @dist     = COALESCE(NEW.distance_km, 0);

        INSERT INTO driver_daily_summary
            (driver_id, summary_date,
             total_orders, gross_earnings, platform_fee, net_earnings, distance_km)
        VALUES
            (NEW.driver_id, v_date,
             1, @gross, @platform, @net, @dist)
        ON DUPLICATE KEY UPDATE
            total_orders   = total_orders + 1,
            gross_earnings = gross_earnings + @gross,
            platform_fee   = platform_fee  + @platform,
            net_earnings   = net_earnings  + @net,
            distance_km    = distance_km   + @dist;
    END IF;
END$$

DROP TRIGGER IF EXISTS trg_order_cancelled_summary;
CREATE TRIGGER trg_order_cancelled_summary
    AFTER UPDATE ON orders
    FOR EACH ROW
BEGIN
    IF NEW.status = 'cancelled' AND OLD.status != 'cancelled'
       AND NEW.driver_id IS NOT NULL THEN

        INSERT INTO driver_daily_summary
            (driver_id, summary_date, cancelled_orders)
        VALUES
            (NEW.driver_id, DATE(NEW.cancelled_at), 1)
        ON DUPLICATE KEY UPDATE
            cancelled_orders = cancelled_orders + 1;
    END IF;
END$$

DROP TRIGGER IF EXISTS trg_rating_driver_summary;
CREATE TRIGGER trg_rating_driver_summary
    AFTER INSERT ON ratings
    FOR EACH ROW
BEGIN
    DECLARE v_driver_role VARCHAR(10);
    DECLARE v_date DATE;

    SELECT role INTO v_driver_role FROM users WHERE id = NEW.target_id;

    IF v_driver_role = 'driver' THEN
        SELECT DATE(completed_at) INTO v_date
        FROM orders WHERE id = NEW.order_id;

        INSERT INTO driver_daily_summary
            (driver_id, summary_date,
             rating_sum, rating_count, avg_rating, tips)
        VALUES
            (NEW.target_id, v_date,
             NEW.score, 1, NEW.score, NEW.tip_amount)
        ON DUPLICATE KEY UPDATE
            rating_sum   = rating_sum + NEW.score,
            rating_count = rating_count + 1,
            avg_rating   = (rating_sum + NEW.score) / (rating_count + 1),
            tips         = tips + NEW.tip_amount;
    END IF;
END$$

DELIMITER ;

-- ============================================================
-- STORED PROCEDURE
-- ============================================================

DROP PROCEDURE IF EXISTS sp_update_online_minutes;
DELIMITER $$
CREATE PROCEDURE sp_update_online_minutes(
    IN p_driver_id  BINARY(16),
    IN p_minutes    SMALLINT UNSIGNED
)
BEGIN
    DECLARE v_date DATE DEFAULT CURDATE();

    INSERT INTO driver_daily_summary
        (driver_id, summary_date, online_minutes)
    VALUES
        (p_driver_id, v_date, p_minutes)
    ON DUPLICATE KEY UPDATE
        online_minutes = online_minutes + p_minutes;

    UPDATE driver_daily_summary dds
    JOIN (
        SELECT
            driver_id,
            DATE(completed_at)                          AS ord_date,
            CONCAT(
                LPAD(HOUR(completed_at), 2, '0'), ':00-',
                LPAD(HOUR(completed_at) + 1, 2, '0'), ':00'
            )                                           AS hour_slot,
            COUNT(*)                                    AS cnt
        FROM orders
        WHERE driver_id          = p_driver_id
          AND DATE(completed_at) = v_date
          AND status             = 'completed'
        GROUP BY hour_slot
        ORDER BY cnt DESC
        LIMIT 1
    ) AS ph ON ph.driver_id = dds.driver_id
           AND ph.ord_date  = dds.summary_date
    SET dds.peak_hour = ph.hour_slot
    WHERE dds.driver_id    = p_driver_id
      AND dds.summary_date = v_date
      AND dds.total_orders >= 2;
END$$

DELIMITER ;

-- ============================================================
-- SEED DATA
--
-- UUID-as-binary dipakai sebagai placeholder dev seed.
-- Produksi: semua ID generate dari Rust (Ulid::new().to_bytes()).
--   rider  → aaaaaaaa-0001-0001-0001-000000000001
--   driver → bbbbbbbb-0001-0001-0001-000000000001
-- ============================================================

INSERT IGNORE INTO users
    (id, name, phone, email, password, role)
VALUES
(
    UNHEX(REPLACE('aaaaaaaa-0001-0001-0001-000000000001', '-', '')),
    'Budi Rider', '08111111111', 'budi@example.com',
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj4oLbcGFpK2',
    'rider'
),
(
    UNHEX(REPLACE('bbbbbbbb-0001-0001-0001-000000000001', '-', '')),
    'Andi Driver', '08222222222', 'andi@example.com',
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj4oLbcGFpK2',
    'driver'
);

INSERT IGNORE INTO driver_profiles
    (user_id, vehicle_type, vehicle_plate, vehicle_model, vehicle_color, is_active)
VALUES
(
    UNHEX(REPLACE('bbbbbbbb-0001-0001-0001-000000000001', '-', '')),
    'motor', 'B 1234 ABC', 'Honda Beat', 'Hitam', 1
);