USE team5_projectdb;

DROP TABLE IF EXISTS q1_results;
CREATE EXTERNAL TABLE q1_results (
  state_code     STRING,
  accident_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team5/project/hive/output/q1';

INSERT OVERWRITE TABLE q1_results
SELECT
  l.state           AS state_code,
  COUNT(*)          AS accident_count
FROM accidents_part a
JOIN locations_part l
  ON a.location_id = l.id
GROUP BY l.state
ORDER BY accident_count DESC;

SELECT * FROM q1_results;