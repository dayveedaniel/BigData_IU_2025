START TRANSACTION;

-- ─────────────────────────────────────────────────────────────
-- staging table
-- ─────────────────────────────────────────────────────────────
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

\copy staging_raw FROM 'us_accidents.csv' CSV HEADER NULL '';

-- 1. Locations table
INSERT INTO locations
        (start_lat, start_lng, end_lat, end_lng,
         start_point,                    end_point,
         street, city, county, state, zipcode, timezone)
SELECT  DISTINCT
        start_lat, start_lng, end_lat, end_lng,
        ST_SetSRID(ST_MakePoint(start_lng, start_lat),4326),
        ST_SetSRID(ST_MakePoint(end_lng,   end_lat)  ,4326),
        street, city, county, state, zipcode, timezone
FROM    staging_raw
ON CONFLICT DO NOTHING;

-- 2. Weather table
INSERT INTO weather
        (airport_code, weather_timestamp,
         temperature_f, wind_chill_f, humidity_pct,
         pressure_in, visibility_mi, wind_direction,
         wind_speed_mph, precipitation_in, weather_condition)
SELECT DISTINCT
       airport_code,
       to_timestamp(weather_timestamp, 'YYYY-MM-DD HH24:MI:SS'),
       temperature_f, wind_chill_f, humidity_pct,
       pressure_in, visibility_mi, wind_direction,
       wind_speed_mph, precipitation_in, weather_condition
FROM   staging_raw
ON CONFLICT DO NOTHING;

-- 3. Twilight table
INSERT INTO twilight
        (sunrise_sunset, civil_twilight,
         nautical_twilight, astronomical_twilight)
SELECT DISTINCT
       sunrise_sunset, civil_twilight,
       nautical_twilight, astronomical_twilight
FROM   staging_raw
ON CONFLICT DO NOTHING;

-- 4. Road-features table
INSERT INTO road_features
        (amenity, bump, crossing, give_way, junction, no_exit,
         railway, roundabout, station, stop,
         traffic_calming, traffic_signal)
SELECT DISTINCT
       amenity, bump, crossing, give_way, junction, no_exit,
       railway, roundabout, station, stop,
       traffic_calming, traffic_signal
FROM   staging_raw
ON CONFLICT DO NOTHING;

-- 5. Accidents table
INSERT INTO accidents
        (id, source, severity, start_time, end_time,
         distance_mi, description,
         location_id, weather_id, twilight_id, road_feat_id)
SELECT  s.id,
        s.source,
        s.severity,
        to_timestamp(s.start_time, 'YYYY-MM-DD HH24:MI:SS'),
        to_timestamp(s.end_time,   'YYYY-MM-DD HH24:MI:SS'),
        s.distance_mi,
        s.description,

        l.id        AS location_id,
        w.id        AS weather_id,
        t.id        AS twilight_id,
        rf.id       AS road_feat_id
FROM    staging_raw s

JOIN    locations      l  USING (start_lat, start_lng, end_lat, end_lng,
                                 street, city, county, state, zipcode, timezone)

JOIN    weather        w  ON   w.airport_code      = s.airport_code
                         AND w.weather_timestamp = to_timestamp(s.weather_timestamp,'YYYY-MM-DD HH24:MI:SS')

JOIN    twilight       t  USING (sunrise_sunset, civil_twilight,
                                 nautical_twilight, astronomical_twilight)

JOIN    road_features  rf ON  rf.amenity          = s.amenity
                         AND rf.bump             = s.bump
                         AND rf.crossing         = s.crossing
                         AND rf.give_way         = s.give_way
                         AND rf.junction         = s.junction
                         AND rf.no_exit          = s.no_exit
                         AND rf.railway          = s.railway
                         AND rf.roundabout       = s.roundabout
                         AND rf.station          = s.station
                         AND rf.stop             = s.stop
                         AND rf.traffic_calming  = s.traffic_calming
                         AND rf.traffic_signal   = s.traffic_signal

ON CONFLICT (id) DO NOTHING;

DROP TABLE staging_raw;

COMMIT;
