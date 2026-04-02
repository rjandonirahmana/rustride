 cat skema_database.sql 
--
-- PostgreSQL database dump
--

\restrict MoQ4zvVytnP06ljtWEmaBc4epKU3fQXvbEE0O5ZHfinWUdreB439TgjttNKf2b1

-- Dumped from database version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)
-- Dumped by pg_dump version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: user_role; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.user_role AS ENUM (
    'rider',
    'driver'
);


ALTER TYPE public.user_role OWNER TO postgres;

--
-- Name: vehicle_type; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.vehicle_type AS ENUM (
    'motor',
    'mobil'
);


ALTER TYPE public.vehicle_type OWNER TO postgres;

--
-- Name: fn_order_cancelled_summary(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.fn_order_cancelled_summary() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.status = 'cancelled' AND OLD.status <> 'cancelled'
       AND NEW.driver_id IS NOT NULL THEN

        INSERT INTO driver_daily_summary
            (driver_id, summary_date, cancelled_orders)
        VALUES
            (NEW.driver_id, NEW.cancelled_at::DATE, 1)
        ON CONFLICT (driver_id, summary_date) DO UPDATE
        SET cancelled_orders = driver_daily_summary.cancelled_orders + 1;
    END IF;

    RETURN NEW;
END;
$$;


ALTER FUNCTION public.fn_order_cancelled_summary() OWNER TO postgres;

--
-- Name: fn_order_completed_summary(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.fn_order_completed_summary() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_date      DATE;
    v_gross     INTEGER;
    v_platform  INTEGER;
    v_net       INTEGER;
    v_dist      NUMERIC(8,2);
BEGIN
    IF NEW.status = 'completed' AND OLD.status <> 'completed'
       AND NEW.driver_id IS NOT NULL THEN

        v_date     := NEW.completed_at::DATE;
        v_gross    := COALESCE(NEW.fare_final, NEW.fare_estimate);
        v_platform := FLOOR(v_gross * 0.20);
        v_net      := v_gross - v_platform;
        v_dist     := COALESCE(NEW.distance_km, 0);

        INSERT INTO driver_daily_summary
            (driver_id, summary_date,
             total_orders, gross_earnings, platform_fee, net_earnings, distance_km)
        VALUES
            (NEW.driver_id, v_date,
             1, v_gross, v_platform, v_net, v_dist)
        ON CONFLICT (driver_id, summary_date) DO UPDATE
        SET
            total_orders   = driver_daily_summary.total_orders   + 1,
            gross_earnings = driver_daily_summary.gross_earnings + v_gross,
            platform_fee   = driver_daily_summary.platform_fee   + v_platform,
            net_earnings   = driver_daily_summary.net_earnings   + v_net,
            distance_km    = driver_daily_summary.distance_km    + v_dist;
    END IF;

    RETURN NEW;
END;
$$;


ALTER FUNCTION public.fn_order_completed_summary() OWNER TO postgres;

--
-- Name: fn_rating_driver_summary(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.fn_rating_driver_summary() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_driver_role   user_role;
    v_date          DATE;
BEGIN
    SELECT role INTO v_driver_role FROM users WHERE id = NEW.target_id;

    IF v_driver_role = 'driver' THEN
        SELECT completed_at::DATE INTO v_date
        FROM orders WHERE id = NEW.order_id;

        INSERT INTO driver_daily_summary
            (driver_id, summary_date,
             rating_sum, rating_count, avg_rating, tips)
        VALUES
            (NEW.target_id, v_date,
             NEW.score, 1, NEW.score, NEW.tip_amount)
        ON CONFLICT (driver_id, summary_date) DO UPDATE
        SET
            rating_sum   = driver_daily_summary.rating_sum   + NEW.score,
            rating_count = driver_daily_summary.rating_count + 1,
            avg_rating   = (driver_daily_summary.rating_sum + NEW.score)
                           / (driver_daily_summary.rating_count + 1),
            tips         = driver_daily_summary.tips + NEW.tip_amount;
    END IF;

    RETURN NEW;
END;
$$;


ALTER FUNCTION public.fn_rating_driver_summary() OWNER TO postgres;

--
-- Name: fn_rsp_accepted(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.fn_rsp_accepted() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.status = 'accepted' AND OLD.status <> 'accepted' THEN
        UPDATE rideshare_trips
        SET
            current_passengers = current_passengers + 1,
            status = CASE
                WHEN current_passengers + 1 >= max_passengers THEN 'full'
                ELSE status
            END
        WHERE id = NEW.trip_id;
    END IF;

    IF OLD.status = 'accepted' AND NEW.status IN ('cancelled', 'rejected') THEN
        UPDATE rideshare_trips
        SET
            current_passengers = GREATEST(current_passengers - 1, 0),
            status = CASE
                WHEN status = 'full' THEN 'open'
                ELSE status
            END
        WHERE id = NEW.trip_id;
    END IF;

    RETURN NEW;
END;
$$;


ALTER FUNCTION public.fn_rsp_accepted() OWNER TO postgres;

--
-- Name: fn_set_updated_at(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.fn_set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.fn_set_updated_at() OWNER TO postgres;

--
-- Name: sp_update_online_minutes(bytea, smallint); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.sp_update_online_minutes(p_driver_id bytea, p_minutes smallint) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_date  DATE := CURRENT_DATE;
    v_slot  TEXT;
BEGIN
    INSERT INTO driver_daily_summary
        (driver_id, summary_date, online_minutes)
    VALUES
        (p_driver_id, v_date, p_minutes)
    ON CONFLICT (driver_id, summary_date) DO UPDATE
    SET online_minutes = driver_daily_summary.online_minutes + p_minutes;

    -- Hitung peak_hour: slot jam dengan order terbanyak hari ini
    SELECT
        LPAD(EXTRACT(HOUR FROM completed_at)::TEXT, 2, '0') || ':00-' ||
        LPAD((EXTRACT(HOUR FROM completed_at) + 1)::TEXT,   2, '0') || ':00'
    INTO v_slot
    FROM orders
    WHERE driver_id          = p_driver_id
      AND completed_at::DATE = v_date
      AND status             = 'completed'
    GROUP BY EXTRACT(HOUR FROM completed_at)
    ORDER BY COUNT(*) DESC
    LIMIT 1;

    IF v_slot IS NOT NULL THEN
        UPDATE driver_daily_summary
        SET peak_hour = v_slot
        WHERE driver_id    = p_driver_id
          AND summary_date = v_date
          AND total_orders >= 2;
    END IF;
END;
$$;


ALTER FUNCTION public.sp_update_online_minutes(p_driver_id bytea, p_minutes smallint) OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: messages; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.messages (
    id bytea NOT NULL,
    sender_id bytea NOT NULL,
    recipient_id bytea NOT NULL,
    order_id bytea NOT NULL,
    content text NOT NULL,
    msg_type character varying(20) DEFAULT 'text'::character varying NOT NULL,
    media_url character varying(1000),
    media_mime character varying(100),
    media_size bigint,
    media_duration integer,
    media_thumb character varying(1000),
    sent_at timestamp with time zone DEFAULT now() NOT NULL,
    delivered_at timestamp with time zone,
    read_at timestamp with time zone
);


ALTER TABLE public.messages OWNER TO postgres;

--
-- Name: conversations; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.conversations AS
 SELECT LEAST(sender_id, recipient_id) AS user_a,
    GREATEST(sender_id, recipient_id) AS user_b,
    max(sent_at) AS last_message_at,
    count(*) AS message_count
   FROM public.messages
  GROUP BY LEAST(sender_id, recipient_id), GREATEST(sender_id, recipient_id);


ALTER VIEW public.conversations OWNER TO postgres;

--
-- Name: driver_daily_summary; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.driver_daily_summary (
    id bigint NOT NULL,
    driver_id bytea NOT NULL,
    summary_date date NOT NULL,
    total_orders smallint DEFAULT 0 NOT NULL,
    cancelled_orders smallint DEFAULT 0 NOT NULL,
    gross_earnings integer DEFAULT 0 NOT NULL,
    platform_fee integer DEFAULT 0 NOT NULL,
    net_earnings integer DEFAULT 0 NOT NULL,
    tips integer DEFAULT 0 NOT NULL,
    online_minutes smallint DEFAULT 0 NOT NULL,
    distance_km numeric(8,2) DEFAULT 0.00 NOT NULL,
    rating_sum numeric(8,2) DEFAULT 0.00 NOT NULL,
    rating_count smallint DEFAULT 0 NOT NULL,
    avg_rating numeric(3,2) DEFAULT 0.00 NOT NULL,
    peak_hour character varying(15),
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.driver_daily_summary OWNER TO postgres;

--
-- Name: driver_daily_summary_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.driver_daily_summary ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.driver_daily_summary_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: driver_profiles; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.driver_profiles (
    user_id bytea NOT NULL,
    vehicle_type public.vehicle_type DEFAULT 'motor'::public.vehicle_type NOT NULL,
    vehicle_plate character varying(20) NOT NULL,
    vehicle_model character varying(100) NOT NULL,
    vehicle_color character varying(50) NOT NULL,
    rating numeric(3,2) DEFAULT 5.00 NOT NULL,
    total_trips integer DEFAULT 0 NOT NULL,
    is_active boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.driver_profiles OWNER TO postgres;

--
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    id bytea NOT NULL,
    name character varying(150) NOT NULL,
    phone character varying(20) NOT NULL,
    email character varying(255),
    password character varying(255) NOT NULL,
    role public.user_role NOT NULL,
    avatar_url character varying(500),
    wallet_balance integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.users OWNER TO postgres;

--
-- Name: driver_today_summary; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.driver_today_summary AS
 SELECT dds.id,
    dds.driver_id,
    dds.summary_date,
    dds.total_orders,
    dds.cancelled_orders,
    dds.gross_earnings,
    dds.platform_fee,
    dds.net_earnings,
    dds.tips,
    dds.online_minutes,
    dds.distance_km,
    dds.rating_sum,
    dds.rating_count,
    dds.avg_rating,
    dds.peak_hour,
    dds.updated_at,
    u.name AS driver_name,
    dp.vehicle_type,
    dp.vehicle_plate
   FROM ((public.driver_daily_summary dds
     JOIN public.users u ON ((u.id = dds.driver_id)))
     JOIN public.driver_profiles dp ON ((dp.user_id = dds.driver_id)))
  WHERE (dds.summary_date = CURRENT_DATE);


ALTER VIEW public.driver_today_summary OWNER TO postgres;

--
-- Name: notifications; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.notifications (
    id bigint NOT NULL,
    user_id bytea NOT NULL,
    type character varying(50) NOT NULL,
    title character varying(255) NOT NULL,
    body text NOT NULL,
    ref_order_id bytea,
    ref_user_id bytea,
    meta jsonb,
    is_read boolean DEFAULT false NOT NULL,
    read_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.notifications OWNER TO postgres;

--
-- Name: notifications_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.notifications ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.notifications_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: orders; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.orders (
    id bytea NOT NULL,
    rider_id bytea NOT NULL,
    driver_id bytea,
    status character varying(20) DEFAULT 'searching'::character varying NOT NULL,
    pickup_lat numeric(10,8) NOT NULL,
    pickup_lng numeric(11,8) NOT NULL,
    pickup_address character varying(500) NOT NULL,
    dest_lat numeric(10,8) NOT NULL,
    dest_lng numeric(11,8) NOT NULL,
    dest_address character varying(500) NOT NULL,
    distance_km numeric(8,2),
    fare_estimate integer NOT NULL,
    fare_final integer,
    service_type public.vehicle_type DEFAULT 'motor'::public.vehicle_type NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    driver_found_at timestamp with time zone,
    accepted_at timestamp with time zone,
    arrived_at timestamp with time zone,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    cancelled_at timestamp with time zone,
    cancel_reason character varying(255),
    rider_rating smallint,
    driver_rating smallint
);


ALTER TABLE public.orders OWNER TO postgres;

--
-- Name: ratings; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ratings (
    id bytea NOT NULL,
    order_id bytea NOT NULL,
    poster_id bytea NOT NULL,
    target_id bytea NOT NULL,
    score smallint NOT NULL,
    comment text,
    tip_amount integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chk_ratings_score CHECK (((score >= 1) AND (score <= 5)))
);


ALTER TABLE public.ratings OWNER TO postgres;

--
-- Name: rideshare_passengers; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.rideshare_passengers (
    id bytea NOT NULL,
    trip_id bytea NOT NULL,
    rider_id bytea NOT NULL,
    pickup_lat numeric(10,8) NOT NULL,
    pickup_lng numeric(11,8) NOT NULL,
    pickup_address character varying(500) NOT NULL,
    dest_lat numeric(10,8) NOT NULL,
    dest_lng numeric(11,8) NOT NULL,
    dest_address character varying(500) NOT NULL,
    fare_estimate integer DEFAULT 0 NOT NULL,
    fare_final integer,
    distance_km numeric(8,2),
    status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    accepted_at timestamp with time zone,
    picked_up_at timestamp with time zone,
    dropped_off_at timestamp with time zone,
    cancelled_at timestamp with time zone,
    cancel_reason character varying(255),
    rating smallint,
    rating_comment text,
    tip_amount integer DEFAULT 0 NOT NULL,
    CONSTRAINT chk_rsp_rating CHECK (((rating >= 1) AND (rating <= 5)))
);


ALTER TABLE public.rideshare_passengers OWNER TO postgres;

--
-- Name: rideshare_trips; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.rideshare_trips (
    id bytea NOT NULL,
    host_order_id bytea NOT NULL,
    driver_id bytea NOT NULL,
    service_type public.vehicle_type DEFAULT 'mobil'::public.vehicle_type NOT NULL,
    route_start_lat numeric(10,8) NOT NULL,
    route_start_lng numeric(11,8) NOT NULL,
    route_end_lat numeric(10,8) NOT NULL,
    route_end_lng numeric(11,8) NOT NULL,
    route_polyline text,
    max_passengers smallint DEFAULT 3 NOT NULL,
    current_passengers smallint DEFAULT 0 NOT NULL,
    status character varying(20) DEFAULT 'open'::character varying NOT NULL,
    join_deadline timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone
);


ALTER TABLE public.rideshare_trips OWNER TO postgres;

--
-- Name: rideshare_trips_open; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.rideshare_trips_open AS
 SELECT rt.id,
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
    (rt.max_passengers - rt.current_passengers) AS seats_available,
    rt.join_deadline,
    rt.created_at,
    u.name AS driver_name,
    u.avatar_url AS driver_avatar,
    dp.vehicle_plate,
    dp.vehicle_model,
    dp.vehicle_color,
    dp.rating AS driver_rating
   FROM ((public.rideshare_trips rt
     JOIN public.users u ON ((u.id = rt.driver_id)))
     JOIN public.driver_profiles dp ON ((dp.user_id = rt.driver_id)))
  WHERE (((rt.status)::text = ANY ((ARRAY['open'::character varying, 'full'::character varying])::text[])) AND ((rt.join_deadline IS NULL) OR (rt.join_deadline > now())));


ALTER VIEW public.rideshare_trips_open OWNER TO postgres;

--
-- Name: driver_daily_summary driver_daily_summary_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.driver_daily_summary
    ADD CONSTRAINT driver_daily_summary_pkey PRIMARY KEY (id);


--
-- Name: driver_profiles driver_profiles_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.driver_profiles
    ADD CONSTRAINT driver_profiles_pkey PRIMARY KEY (user_id);


--
-- Name: messages messages_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_pkey PRIMARY KEY (id);


--
-- Name: notifications notifications_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.notifications
    ADD CONSTRAINT notifications_pkey PRIMARY KEY (id);


--
-- Name: orders orders_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_pkey PRIMARY KEY (id);


--
-- Name: ratings ratings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ratings
    ADD CONSTRAINT ratings_pkey PRIMARY KEY (id);


--
-- Name: rideshare_passengers rideshare_passengers_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.rideshare_passengers
    ADD CONSTRAINT rideshare_passengers_pkey PRIMARY KEY (id);


--
-- Name: rideshare_trips rideshare_trips_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.rideshare_trips
    ADD CONSTRAINT rideshare_trips_pkey PRIMARY KEY (id);


--
-- Name: driver_daily_summary uq_dds_driver_date; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.driver_daily_summary
    ADD CONSTRAINT uq_dds_driver_date UNIQUE (driver_id, summary_date);


--
-- Name: rideshare_passengers uq_rsp_trip_rider; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.rideshare_passengers
    ADD CONSTRAINT uq_rsp_trip_rider UNIQUE (trip_id, rider_id);


--
-- Name: rideshare_trips uq_rst_host_order; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.rideshare_trips
    ADD CONSTRAINT uq_rst_host_order UNIQUE (host_order_id);


--
-- Name: users uq_users_email; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT uq_users_email UNIQUE (email);


--
-- Name: users uq_users_phone; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT uq_users_phone UNIQUE (phone);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: idx_dds_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_dds_date ON public.driver_daily_summary USING btree (summary_date);


--
-- Name: idx_dds_driver; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_dds_driver ON public.driver_daily_summary USING btree (driver_id);


--
-- Name: idx_messages_conversation; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_messages_conversation ON public.messages USING btree (sender_id, recipient_id, sent_at);


--
-- Name: idx_messages_recipient_unread; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_messages_recipient_unread ON public.messages USING btree (recipient_id, read_at) WHERE (read_at IS NULL);


--
-- Name: idx_messages_sent_at; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_messages_sent_at ON public.messages USING btree (sent_at);


--
-- Name: idx_notif_meta; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_notif_meta ON public.notifications USING gin (meta) WHERE (meta IS NOT NULL);


--
-- Name: idx_notif_ref_order; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_notif_ref_order ON public.notifications USING btree (ref_order_id) WHERE (ref_order_id IS NOT NULL);


--
-- Name: idx_notif_unread; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_notif_unread ON public.notifications USING btree (user_id, is_read) WHERE (is_read = false);


--
-- Name: idx_notif_user; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_notif_user ON public.notifications USING btree (user_id, created_at DESC);


--
-- Name: idx_orders_driver_status; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_orders_driver_status ON public.orders USING btree (driver_id, status);


--
-- Name: idx_orders_id_status; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_orders_id_status ON public.orders USING btree (id, status);


--
-- Name: idx_orders_rider_created; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_orders_rider_created ON public.orders USING btree (rider_id, created_at DESC);


--
-- Name: idx_orders_rider_status; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_orders_rider_status ON public.orders USING btree (rider_id, status);


--
-- Name: idx_orders_search_geo; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_orders_search_geo ON public.orders USING btree (status, service_type, pickup_lat, pickup_lng) WHERE ((status)::text = ANY ((ARRAY['searching'::character varying, 'found'::character varying])::text[]));


--
-- Name: idx_orders_status; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_orders_status ON public.orders USING btree (status);


--
-- Name: idx_ratings_order; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ratings_order ON public.ratings USING btree (order_id);


--
-- Name: idx_ratings_target; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ratings_target ON public.ratings USING btree (target_id);


--
-- Name: idx_rsp_rider; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_rsp_rider ON public.rideshare_passengers USING btree (rider_id);


--
-- Name: idx_rsp_status; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_rsp_status ON public.rideshare_passengers USING btree (status);


--
-- Name: idx_rsp_trip; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_rsp_trip ON public.rideshare_passengers USING btree (trip_id);


--
-- Name: idx_rst_driver; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_rst_driver ON public.rideshare_trips USING btree (driver_id);


--
-- Name: idx_rst_route_end; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_rst_route_end ON public.rideshare_trips USING btree (route_end_lat, route_end_lng);


--
-- Name: idx_rst_route_start; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_rst_route_start ON public.rideshare_trips USING btree (route_start_lat, route_start_lng);


--
-- Name: idx_rst_service; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_rst_service ON public.rideshare_trips USING btree (service_type, status);


--
-- Name: idx_rst_status; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_rst_status ON public.rideshare_trips USING btree (status);


--
-- Name: idx_users_email_unique; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_users_email_unique ON public.users USING btree (email) WHERE (email IS NOT NULL);


--
-- Name: idx_users_phone; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_users_phone ON public.users USING btree (phone);


--
-- Name: idx_users_role; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_users_role ON public.users USING btree (role);


--
-- Name: driver_daily_summary trg_dds_updated_at; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_dds_updated_at BEFORE UPDATE ON public.driver_daily_summary FOR EACH ROW EXECUTE FUNCTION public.fn_set_updated_at();


--
-- Name: orders trg_order_cancelled_summary; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_order_cancelled_summary AFTER UPDATE ON public.orders FOR EACH ROW EXECUTE FUNCTION public.fn_order_cancelled_summary();


--
-- Name: orders trg_order_completed_summary; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_order_completed_summary AFTER UPDATE ON public.orders FOR EACH ROW EXECUTE FUNCTION public.fn_order_completed_summary();


--
-- Name: ratings trg_rating_driver_summary; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_rating_driver_summary AFTER INSERT ON public.ratings FOR EACH ROW EXECUTE FUNCTION public.fn_rating_driver_summary();


--
-- Name: rideshare_passengers trg_rsp_accepted; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_rsp_accepted AFTER UPDATE ON public.rideshare_passengers FOR EACH ROW EXECUTE FUNCTION public.fn_rsp_accepted();


--
-- Name: driver_daily_summary fk_dds_driver; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.driver_daily_summary
    ADD CONSTRAINT fk_dds_driver FOREIGN KEY (driver_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: driver_profiles fk_dp_user; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.driver_profiles
    ADD CONSTRAINT fk_dp_user FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: messages fk_messages_order; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT fk_messages_order FOREIGN KEY (order_id) REFERENCES public.orders(id) ON DELETE CASCADE;


--
-- Name: messages fk_messages_recipient; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT fk_messages_recipient FOREIGN KEY (recipient_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: messages fk_messages_sender; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT fk_messages_sender FOREIGN KEY (sender_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: notifications fk_notif_ref_order; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.notifications
    ADD CONSTRAINT fk_notif_ref_order FOREIGN KEY (ref_order_id) REFERENCES public.orders(id) ON DELETE SET NULL;


--
-- Name: notifications fk_notif_user; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.notifications
    ADD CONSTRAINT fk_notif_user FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: orders fk_order_driver; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT fk_order_driver FOREIGN KEY (driver_id) REFERENCES public.users(id);


--
-- Name: orders fk_order_rider; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT fk_order_rider FOREIGN KEY (rider_id) REFERENCES public.users(id);


--
-- Name: ratings fk_rating_order; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ratings
    ADD CONSTRAINT fk_rating_order FOREIGN KEY (order_id) REFERENCES public.orders(id) ON DELETE CASCADE;


--
-- Name: ratings fk_rating_poster; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ratings
    ADD CONSTRAINT fk_rating_poster FOREIGN KEY (poster_id) REFERENCES public.users(id);


--
-- Name: ratings fk_rating_target; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ratings
    ADD CONSTRAINT fk_rating_target FOREIGN KEY (target_id) REFERENCES public.users(id);


--
-- Name: rideshare_passengers fk_rsp_rider; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.rideshare_passengers
    ADD CONSTRAINT fk_rsp_rider FOREIGN KEY (rider_id) REFERENCES public.users(id);


--
-- Name: rideshare_passengers fk_rsp_trip; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.rideshare_passengers
    ADD CONSTRAINT fk_rsp_trip FOREIGN KEY (trip_id) REFERENCES public.rideshare_trips(id) ON DELETE CASCADE;


--
-- Name: rideshare_trips fk_rst_driver; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.rideshare_trips
    ADD CONSTRAINT fk_rst_driver FOREIGN KEY (driver_id) REFERENCES public.users(id);


--
-- Name: rideshare_trips fk_rst_host_order; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.rideshare_trips
    ADD CONSTRAINT fk_rst_host_order FOREIGN KEY (host_order_id) REFERENCES public.orders(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--
