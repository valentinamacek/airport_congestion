-- ============================================================
--  Airport Analytics — initial schema
--  Runs automatically on first `docker compose up`
-- ============================================================

-- Static airport metadata (seeded below)
CREATE TABLE IF NOT EXISTS airports (
    icao_code   VARCHAR(4)        PRIMARY KEY,
    name        VARCHAR(120)      NOT NULL,
    city        VARCHAR(80),
    country     VARCHAR(60),
    latitude    DOUBLE PRECISION  NOT NULL,
    longitude   DOUBLE PRECISION  NOT NULL,
    radius_km   DOUBLE PRECISION  DEFAULT 50.0
);

-- Per-airport aggregated metrics written by Flink (one row per window)
CREATE TABLE IF NOT EXISTS airport_metrics (
    id               SERIAL         PRIMARY KEY,
    airport_icao     VARCHAR(4)     NOT NULL REFERENCES airports(icao_code),
    window_start     TIMESTAMPTZ    NOT NULL,
    window_end       TIMESTAMPTZ    NOT NULL,
    inbound_count    INT            DEFAULT 0,
    outbound_count   INT            DEFAULT 0,
    arrivals         INT            DEFAULT 0,
    departures       INT            DEFAULT 0,
    holding_count    INT            DEFAULT 0,
    runway_load      INT            DEFAULT 0,
    congestion_score DOUBLE PRECISION DEFAULT 0.0,
    created_at       TIMESTAMPTZ    DEFAULT NOW(),
    UNIQUE (airport_icao, window_start)
);

-- Congestion time-series for the "over time" Grafana panel
CREATE TABLE IF NOT EXISTS congestion_history (
    id               SERIAL         PRIMARY KEY,
    airport_icao     VARCHAR(4)     NOT NULL REFERENCES airports(icao_code),
    ts               TIMESTAMPTZ    NOT NULL,
    congestion_score DOUBLE PRECISION NOT NULL,
    inbound_count    INT            DEFAULT 0,
    holding_count    INT            DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_congestion_ts
    ON congestion_history (airport_icao, ts DESC);

-- Live inbound aircraft detail (upserted each cycle)
CREATE TABLE IF NOT EXISTS inbound_aircraft (
    icao24        VARCHAR(10)       NOT NULL,
    airport_icao  VARCHAR(4)        NOT NULL REFERENCES airports(icao_code),
    callsign      VARCHAR(20),
    latitude      DOUBLE PRECISION,
    longitude     DOUBLE PRECISION,
    altitude_m    DOUBLE PRECISION,
    velocity_ms   DOUBLE PRECISION,
    heading       DOUBLE PRECISION,
    on_ground     BOOLEAN           DEFAULT FALSE,
    manufacturer  VARCHAR(80),
    model         VARCHAR(80),
    registration  VARCHAR(20),
    distance_km   DOUBLE PRECISION,
    last_seen     TIMESTAMPTZ       NOT NULL,
    PRIMARY KEY (icao24, airport_icao)
);

-- Aircraft currently in holding patterns
CREATE TABLE IF NOT EXISTS holding_aircraft (
    icao24               VARCHAR(10)  NOT NULL,
    airport_icao         VARCHAR(4)   NOT NULL REFERENCES airports(icao_code),
    callsign             VARCHAR(20),
    altitude_m           DOUBLE PRECISION,
    holding_since        TIMESTAMPTZ  NOT NULL,
    holding_duration_min DOUBLE PRECISION DEFAULT 0.0,
    last_seen            TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (icao24, airport_icao)
);

-- ============================================================
--  Seed: Alpine / Central Europe airports
-- ============================================================
INSERT INTO airports (icao_code, name, city, country, latitude, longitude) VALUES
  ('LOWW', 'Vienna International Airport',         'Vienna',      'Austria',     48.1103,  16.5697),
  ('LOWS', 'Salzburg Airport',                     'Salzburg',    'Austria',     47.7933,  13.0043),
  ('LOWG', 'Graz Airport',                         'Graz',        'Austria',     46.9911,  15.4396),
  ('LOWI', 'Innsbruck Airport',                    'Innsbruck',   'Austria',     47.2602,  11.3440),
  ('EDDF', 'Frankfurt Airport',                    'Frankfurt',   'Germany',     50.0264,   8.5431),
  ('EDDM', 'Munich Airport',                       'Munich',      'Germany',     48.3537,  11.7750),
  ('EDDS', 'Stuttgart Airport',                    'Stuttgart',   'Germany',     48.6899,   9.2220),
  ('EDDL', 'Düsseldorf Airport',                   'Düsseldorf',  'Germany',     51.2895,   6.7668),
  ('LSZH', 'Zurich Airport',                       'Zurich',      'Switzerland', 47.4647,   8.5492),
  ('LSGG', 'Geneva Airport',                       'Geneva',      'Switzerland', 46.2380,   6.1089),
  ('LIMC', 'Milan Malpensa Airport',               'Milan',       'Italy',       45.6306,   8.7281),
  ('LIME', 'Milan Bergamo Airport',                'Bergamo',     'Italy',       45.6739,   9.7042),
  ('LIML', 'Milan Linate Airport',                 'Milan',       'Italy',       45.4453,   9.2767),
  ('LIPZ', 'Venice Marco Polo Airport',            'Venice',      'Italy',       45.5053,  12.3519),
  ('LJLJ', 'Ljubljana Jože Pučnik Airport',        'Ljubljana',   'Slovenia',    46.2237,  14.4576),
  ('LKPR', 'Václav Havel Prague Airport',          'Prague',      'Czechia',     50.1008,  14.2600),
  ('EPWA', 'Warsaw Chopin Airport',                'Warsaw',      'Poland',      52.1657,  20.9671),
  ('LHBP', 'Budapest Ferenc Liszt Airport',        'Budapest',    'Hungary',     47.4298,  19.2610)
ON CONFLICT (icao_code) DO NOTHING;
