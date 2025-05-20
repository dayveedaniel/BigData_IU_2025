USE team5_projectdb;

DROP TABLE IF EXISTS q4_results;
CREATE EXTERNAL TABLE q4_results (
  city           STRING,
  state          STRING,
  accident_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team5/project/hive/output/q4';

INSERT OVERWRITE TABLE q4_results
SELECT
  l.city,
  l.state,
  COUNT(*) AS accident_count
FROM accidents_part a
JOIN locations_part l
  ON a.location_id = l.id
GROUP BY l.city, l.state
ORDER BY accident_count DESC
LIMIT 10;

SELECT * FROM q4_results;