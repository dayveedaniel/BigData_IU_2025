START TRANSACTION;

DROP TABLE IF EXISTS accidents       CASCADE;
DROP TABLE IF EXISTS road_features   CASCADE;
DROP TABLE IF EXISTS twilight        CASCADE;
DROP TABLE IF EXISTS weather         CASCADE;
DROP TABLE IF EXISTS locations       CASCADE;
DROP TABLE IF EXISTS bitcoin_heist   CASCADE;

-- ── Dimension tables ─────────────────────────────────────────────
CREATE TABLE locations (
    id            BIGSERIAL PRIMARY KEY,
    start_lat     NUMERIC,
    start_lng     NUMERIC,
    end_lat       NUMERIC,
    end_lng       NUMERIC,
    street        TEXT,
    city          TEXT,
    county        TEXT,
    state         CHAR(2),
    zipcode       VARCHAR(10),
    timezone      VARCHAR(32)
);

CREATE TABLE weather (
    id                 BIGSERIAL PRIMARY KEY,
    airport_code       CHAR(4),
    weather_timestamp  TIMESTAMPTZ,
    temperature_f      NUMERIC,
    wind_chill_f       NUMERIC,
    humidity_pct       NUMERIC,
    pressure_in        NUMERIC,
    visibility_mi      NUMERIC,
    wind_direction     VARCHAR(10),
    wind_speed_mph     NUMERIC,
    precipitation_in   NUMERIC,
    weather_condition  VARCHAR(64)
);

CREATE TABLE twilight (
    id                     SMALLSERIAL PRIMARY KEY,
    sunrise_sunset         VARCHAR(5),
    civil_twilight         VARCHAR(5),
    nautical_twilight      VARCHAR(5),
    astronomical_twilight  VARCHAR(5)
);

CREATE TABLE road_features (
    id               SMALLSERIAL PRIMARY KEY,
    amenity          BOOLEAN,
    bump             BOOLEAN,
    crossing         BOOLEAN,
    give_way         BOOLEAN,
    junction         BOOLEAN,
    no_exit          BOOLEAN,
    railway          BOOLEAN,
    roundabout       BOOLEAN,
    station          BOOLEAN,
    stop             BOOLEAN,
    traffic_calming  BOOLEAN,
    traffic_signal   BOOLEAN,

    UNIQUE (amenity, bump, crossing, give_way, junction,
            no_exit, railway, roundabout, station, stop,
            traffic_calming, traffic_signal)
);

CREATE TABLE accidents (
    id             VARCHAR(20) PRIMARY KEY,      -- needed for ON CONFLICT
    source         VARCHAR(32),
    severity       SMALLINT CHECK (severity BETWEEN 1 AND 4),
    start_time     TIMESTAMPTZ,
    end_time       TIMESTAMPTZ,
    distance_mi    NUMERIC,
    description    TEXT,

    location_id    BIGINT      REFERENCES locations(id),
    weather_id     BIGINT      REFERENCES weather(id),
    twilight_id    SMALLINT    REFERENCES twilight(id),
    road_feat_id   SMALLINT    REFERENCES road_features(id)
);
COMMIT;