-- Optimized version of create_staging.sql

DROP TABLE IF EXISTS staging_raw;

CREATE UNLOGGED TABLE staging_raw (
    id                   text,
    source               text,
    severity             int,
    start_time           text,
    end_time             text,
    start_lat            numeric,
    start_lng            numeric,
    end_lat              numeric,
    end_lng              numeric,
    distance_mi          numeric,
    description          text,
    street               text,
    city                 text,
    county               text,
    state                text,
    zipcode              text,
    country              text,
    timezone             text,
    airport_code         text,
    weather_timestamp    text,
    temperature_f        numeric,
    wind_chill_f         numeric,
    humidity_pct         numeric,
    pressure_in          numeric,
    visibility_mi        numeric,
    wind_direction       text,
    wind_speed_mph       numeric,
    precipitation_in     numeric,
    weather_condition    text,
    amenity              bool,
    bump                 bool,
    crossing             bool,
    give_way             bool,
    junction             bool,
    no_exit              bool,
    railway              bool,
    roundabout           bool,
    station              bool,
    stop                 bool,
    traffic_calming      bool,
    traffic_signal       bool,
    turning_loop         bool,
    sunrise_sunset       text,
    civil_twilight       text,
    nautical_twilight    text,
    astronomical_twilight text
);

-- Create indexes on the staging table to speed up the JOINs that will follow
-- Note: These indexes take time to build but save much more time during the JOINs
CREATE INDEX IF NOT EXISTS idx_staging_raw_location ON staging_raw 
    (start_lat, start_lng, end_lat, end_lng, street, city, county, state, zipcode, timezone);
    
CREATE INDEX IF NOT EXISTS idx_staging_raw_weather ON staging_raw 
    (airport_code, weather_timestamp);
    
CREATE INDEX IF NOT EXISTS idx_staging_raw_twilight ON staging_raw 
    (sunrise_sunset, civil_twilight, nautical_twilight, astronomical_twilight);
    
CREATE INDEX IF NOT EXISTS idx_staging_raw_road ON staging_raw 
    (amenity, bump, crossing, give_way, junction, no_exit, railway, 
     roundabout, station, stop, traffic_calming, traffic_signal);