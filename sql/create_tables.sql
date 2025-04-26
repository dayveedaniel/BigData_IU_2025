START TRANSACTION;

CREATE EXTENSION IF NOT EXISTS postgis;

DROP TABLE IF EXISTS accidents       CASCADE;
DROP TABLE IF EXISTS road_features   CASCADE;
DROP TABLE IF EXISTS twilight        CASCADE;
DROP TABLE IF EXISTS weather         CASCADE;
DROP TABLE IF EXISTS locations       CASCADE;

CREATE TABLE locations (
    id            BIGSERIAL PRIMARY KEY,
    start_lat     NUMERIC(9,6),
    start_lng     NUMERIC(9,6),
    end_lat       NUMERIC(9,6),
    end_lng       NUMERIC(9,6),

    start_point   GEOMETRY(Point, 4326),
    end_point     GEOMETRY(Point, 4326),

    street        TEXT,
    city          TEXT,
    county        TEXT,
    state         CHAR(2),
    zipcode       VARCHAR(10),
    timezone      VARCHAR(32)
);

-- indexes
CREATE INDEX IF NOT EXISTS ix_loc_state_city
    ON locations (state, city);
CREATE INDEX IF NOT EXISTS ix_loc_start_point_gix
    ON locations USING GIST (start_point);

CREATE TABLE weather (
    id                 BIGSERIAL PRIMARY KEY,
    airport_code       CHAR(4),
    weather_timestamp  TIMESTAMPTZ,
    temperature_f      NUMERIC(4,1),
    wind_chill_f       NUMERIC(4,1),
    humidity_pct       NUMERIC(5,2),
    pressure_in        NUMERIC(5,2),
    visibility_mi      NUMERIC(4,1),
    wind_direction     VARCHAR(3),
    wind_speed_mph     NUMERIC(4,1),
    precipitation_in   NUMERIC(4,2),
    weather_condition  VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS ix_weather_time
    ON weather (weather_timestamp);

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
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_road_features_combo
    ON road_features (amenity, bump, crossing, give_way, junction,
                      no_exit, railway, roundabout, station, stop,
                      traffic_calming, traffic_signal);

CREATE TABLE accidents (
    id             VARCHAR(20) PRIMARY KEY,           
    source         VARCHAR(32),
    severity       SMALLINT CHECK (severity BETWEEN 1 AND 4),
    start_time     TIMESTAMPTZ,
    end_time       TIMESTAMPTZ,
    distance_mi    NUMERIC(6,3),
    description    TEXT,

    location_id    BIGINT      REFERENCES locations(id),
    weather_id     BIGINT      REFERENCES weather(id),
    twilight_id    SMALLINT    REFERENCES twilight(id),
    road_feat_id   SMALLINT    REFERENCES road_features(id)
);

CREATE INDEX IF NOT EXISTS ix_acc_start_time
    ON accidents (start_time);

CREATE INDEX IF NOT EXISTS ix_acc_sev_time_loc
    ON accidents (severity, start_time DESC)
    INCLUDE (location_id);

COMMIT;
