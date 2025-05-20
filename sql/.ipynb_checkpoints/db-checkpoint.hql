DROP DATABASE team5_projectdb CASCADE;
CREATE DATABASE team5_projectdb LOCATION 'project/hive/warehouse';

USE team5_projectdb;


-- Keep dynamic‑partition & bucketing switches
SET hive.exec.dynamic.partition       = true;
SET hive.exec.dynamic.partition.mode  = nonstrict;
SET hive.enforce.bucketing            = true;
SET avro.output.codec                 = bzip2;


CREATE EXTERNAL TABLE locations 
STORED AS AVRO LOCATION 'project/warehouse/avro/locations'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/locations.avsc');

CREATE EXTERNAL TABLE weather 
STORED AS AVRO LOCATION 'project/warehouse/avro/weather'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/weather.avsc');

CREATE EXTERNAL TABLE twilight 
STORED AS AVRO LOCATION 'project/warehouse/avro/twilight'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/twilight.avsc');

CREATE EXTERNAL TABLE road_features 
STORED AS AVRO LOCATION 'project/warehouse/avro/road_features'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/road_features.avsc');

CREATE EXTERNAL TABLE accidents 
STORED AS AVRO LOCATION 'project/warehouse/avro/accidents'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/accidents.avsc');

DESCRIBE accidents;
SELECT * FROM accidents LIMIT 10;


CREATE EXTERNAL TABLE weather_buck (
    id                BIGINT,
    airport_code      CHAR(4),
    weather_timestamp TIMESTAMP,
    temperature_f     DECIMAL,
    wind_chill_f      DECIMAL,
    humidity_pct      DECIMAL,
    pressure_in       DECIMAL,
    visibility_mi     DECIMAL,
    wind_direction    STRING,
    wind_speed_mph    DECIMAL,
    precipitation_in  DECIMAL,
    weather_condition STRING
)
CLUSTERED BY (weather_condition)      
INTO 144 BUCKETS                      
STORED AS AVRO
LOCATION 'project/warehouse/avro/weather_buck';


-- ─────────────────────────────────────────────────────────────
-- 2. Locations (table)  ➜  locations_part_avro
--    Partitioned by state, bucketed by city
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE locations_part (
    id         BIGINT,
    start_lat  DECIMAL,
    start_lng  DECIMAL,
    end_lat    DECIMAL,
    end_lng    DECIMAL,
    street     STRING,
    city       STRING,
    county     STRING,
    zipcode    STRING,
    timezone   STRING
    
)
PARTITIONED BY (state CHAR(2))
CLUSTERED BY (city) INTO 16 BUCKETS
STORED AS AVRO
LOCATION 'project/warehouse/avro/locations_part';

-- ─────────────────────────────────────────────────────────────
-- 3. Accidents (table)  ➜  accidents_part
--    Partitioned by severity & year, bucketed by month
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE accidents_part (
    id            STRING,
    source        STRING,
    distance_mi   DECIMAL(10,2),
    description   STRING,
    location_id   BIGINT,
    weather_id    BIGINT,
    twilight_id   SMALLINT,
    road_feat_id  SMALLINT,
    start_time    TIMESTAMP,
    end_time      TIMESTAMP,
    month         INT            -- bucket key stays in row schema
)
PARTITIONED BY (severity SMALLINT, year INT)
CLUSTERED BY (month) INTO 12 BUCKETS
STORED AS AVRO
LOCATION 'project/warehouse/avro/accidents_part';

-- ─────────────────────────────────────────────────────────────
-- 6. Data‑conversion queries
--    (read from existing Parquet tables, write to Avro tables)
-- ─────────────────────────────────────────────────────────────

-- Weather
INSERT OVERWRITE TABLE weather_buck
SELECT
    id, airport_code, weather_timestamp,
    temperature_f, wind_chill_f, humidity_pct, pressure_in,
    visibility_mi, wind_direction, wind_speed_mph, precipitation_in,
    weather_condition           
FROM weather;                      


-- Locations
INSERT OVERWRITE TABLE locations_part PARTITION (state)
SELECT
    id, start_lat, start_lng, end_lat, end_lng,
    street, county, zipcode, timezone, city,
    state
FROM locations;

-- Accidents
INSERT OVERWRITE TABLE accidents_part
PARTITION (severity, year)
SELECT
    id,
    source,
    distance_mi,
    description,
    location_id,
    weather_id,
    twilight_id,
    road_feat_id,

    -- timestamps converted from epoch‑millis
    from_unixtime(CAST(start_time / 1000 AS BIGINT)) AS start_time,
    from_unixtime(CAST(end_time   / 1000 AS BIGINT)) AS end_time,

    month(from_unixtime(CAST(start_time / 1000 AS BIGINT))) AS month,
    severity,
    year(from_unixtime(CAST(start_time / 1000 AS BIGINT)))  AS year
FROM accidents;

-- Delete tables partitioned or bucketed
DROP TABLE IF EXISTS weather;
DROP TABLE IF EXISTS locations;
DROP TABLE IF EXISTS accidents;

-- ─────────────────────────────────────────────────────────────
-- 7. Smoke test
-- ─────────────────────────────────────────────────────────────
SELECT COUNT(*)
FROM accidents_part
WHERE severity = 2 AND year = 2021;