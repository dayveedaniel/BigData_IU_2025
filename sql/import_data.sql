-- START TRANSACTION;

-- -- 1. Locations table
-- INSERT INTO locations (start_lat, start_lng, end_lat, end_lng, street, city, county, state, zipcode, timezone)
-- SELECT DISTINCT 
--     start_lat, 
--     start_lng, 
--     end_lat, 
--     end_lng, 
--     street, 
--     city, 
--     county, 
--     state, 
--     zipcode, 
--     timezone
-- FROM staging_raw
-- ON CONFLICT DO NOTHING;

-- -- 2. Weather table
-- INSERT INTO weather (airport_code, weather_timestamp, temperature_f, wind_chill_f, humidity_pct, 
--                      pressure_in, visibility_mi, wind_direction, wind_speed_mph, precipitation_in, weather_condition)
-- SELECT DISTINCT 
--     airport_code, 
--     to_timestamp(weather_timestamp, 'YYYY-MM-DD HH24:MI:SS'), 
--     temperature_f, 
--     wind_chill_f, 
--     humidity_pct, 
--     pressure_in, 
--     visibility_mi, 
--     wind_direction, 
--     wind_speed_mph, 
--     precipitation_in, 
--     weather_condition
-- FROM staging_raw
-- ON CONFLICT DO NOTHING;

-- -- 3. Twilight table
-- INSERT INTO twilight (sunrise_sunset, civil_twilight, nautical_twilight, astronomical_twilight)
-- SELECT DISTINCT 
--     sunrise_sunset, 
--     civil_twilight, 
--     nautical_twilight, 
--     astronomical_twilight
-- FROM staging_raw
-- ON CONFLICT DO NOTHING;

-- -- 4. Road-features table
-- INSERT INTO road_features (amenity, bump, crossing, give_way, junction, no_exit, 
--                            railway, roundabout, station, stop, traffic_calming, traffic_signal)
-- SELECT DISTINCT 
--     amenity, 
--     bump, 
--     crossing, 
--     give_way, 
--     junction, 
--     no_exit, 
--     railway, 
--     roundabout, 
--     station, 
--     stop, 
--     traffic_calming, 
--     traffic_signal
-- FROM staging_raw
-- ON CONFLICT DO NOTHING;
-- -- 5. Accidents table
-- INSERT INTO accidents (id, source, severity, start_time, end_time, distance_mi, description, location_id, weather_id, twilight_id, road_feat_id)
-- SELECT 
--     s.id, 
--     s.source, 
--     s.severity, 
--     to_timestamp(s.start_time, 'YYYY-MM-DD HH24:MI:SS'), 
--     to_timestamp(s.end_time, 'YYYY-MM-DD HH24:MI:SS'), 
--     s.distance_mi, 
--     s.description,
--     l.id AS location_id,
--     w.id AS weather_id,
--     t.id AS twilight_id,
--     rf.id AS road_feat_id
-- FROM staging_raw s
-- LEFT JOIN locations l ON l.start_lat = s.start_lat 
--                       AND l.start_lng = s.start_lng 
--                       AND (l.end_lat IS NOT DISTINCT FROM s.end_lat)
--                       AND (l.end_lng IS NOT DISTINCT FROM s.end_lng)
--                       AND (l.street IS NOT DISTINCT FROM s.street)
--                       AND (l.city IS NOT DISTINCT FROM s.city)
--                       AND (l.county IS NOT DISTINCT FROM s.county)
--                       AND (l.state IS NOT DISTINCT FROM s.state)
--                       AND (l.zipcode IS NOT DISTINCT FROM s.zipcode)
--                       AND (l.timezone IS NOT DISTINCT FROM s.timezone)
-- LEFT JOIN weather w ON w.airport_code = s.airport_code 
--                     AND (w.weather_timestamp IS NOT DISTINCT FROM to_timestamp(s.weather_timestamp,'YYYY-MM-DD HH24:MI:SS'))
-- LEFT JOIN twilight t ON (t.sunrise_sunset IS NOT DISTINCT FROM s.sunrise_sunset)
--                      AND (t.civil_twilight IS NOT DISTINCT FROM s.civil_twilight)
--                      AND (t.nautical_twilight IS NOT DISTINCT FROM s.nautical_twilight)
--                      AND (t.astronomical_twilight IS NOT DISTINCT FROM s.astronomical_twilight)
-- LEFT JOIN road_features rf ON (rf.amenity IS NOT DISTINCT FROM s.amenity)
--                           AND (rf.bump IS NOT DISTINCT FROM s.bump)
--                           AND (rf.crossing IS NOT DISTINCT FROM s.crossing)
--                           AND (rf.give_way IS NOT DISTINCT FROM s.give_way)
--                           AND (rf.junction IS NOT DISTINCT FROM s.junction)
--                           AND (rf.no_exit IS NOT DISTINCT FROM s.no_exit)
--                           AND (rf.railway IS NOT DISTINCT FROM s.railway)
--                           AND (rf.roundabout IS NOT DISTINCT FROM s.roundabout)
--                           AND (rf.station IS NOT DISTINCT FROM s.station)
--                           AND (rf.stop IS NOT DISTINCT FROM s.stop)
--                           AND (rf.traffic_calming IS NOT DISTINCT FROM s.traffic_calming)
--                           AND (rf.traffic_signal IS NOT DISTINCT FROM s.traffic_signal)
-- ON CONFLICT (id) DO NOTHING;

-- -- Dropping staging table and creating indexes
-- DROP TABLE staging_raw CASCADE;
-- CREATE INDEX IF NOT EXISTS ix_loc_state_city ON locations (state, city);
-- CREATE INDEX IF NOT EXISTS ix_weather_time ON weather (weather_timestamp);
-- CREATE INDEX IF NOT EXISTS ix_acc_start_time ON accidents (start_time);
-- CREATE INDEX IF NOT EXISTS ix_acc_sev_time_loc ON accidents (severity, start_time DESC) INCLUDE (location_id);

-- COMMIT;


START TRANSACTION;

-- 1. Locations table
INSERT INTO locations
(start_lat, start_lng, end_lat, end_lng,
-- start_point,                    end_point,
street, city, county, state, zipcode, timezone)
SELECT  DISTINCT
start_lat, start_lng, end_lat, end_lng,
-- ST_SetSRID(ST_MakePoint(start_lng, start_lat),4326),
-- ST_SetSRID(ST_MakePoint(end_lng,   end_lat)  ,4326),
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

DROP TABLE staging_raw CASCADE;

CREATE INDEX IF NOT EXISTS ix_loc_state_city
ON locations (state, city);

-- weather
CREATE INDEX IF NOT EXISTS ix_weather_time
ON weather (weather_timestamp);

-- accidents
CREATE INDEX IF NOT EXISTS ix_acc_start_time
ON accidents (start_time);

CREATE INDEX IF NOT EXISTS ix_acc_sev_time_loc
ON accidents (severity, start_time DESC)
INCLUDE (location_id);

COMMIT;