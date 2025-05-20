USE team5_projectdb;

DROP TABLE IF EXISTS q5_results;
CREATE EXTERNAL TABLE q5_results (
  weather_condition STRING,
  accident_count    BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team5/project/hive/output/q5';

INSERT OVERWRITE TABLE q5_results
SELECT
  w.weather_condition,
  COUNT(*) AS accident_count
FROM accidents_part a
JOIN weather_buck w
  ON a.weather_id = w.id
GROUP BY w.weather_condition
ORDER BY accident_count DESC
LIMIT 10;

SELECT * FROM q5_results;