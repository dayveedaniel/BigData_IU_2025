USE team5_projectdb;

-- 1. results table
DROP TABLE IF EXISTS q7_results;
CREATE EXTERNAL TABLE q7_results (
  severity          SMALLINT,
  avg_distance_mi   DECIMAL(10,2),
  total_accidents   BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team5/project/hive/output/q7';

-- 2. populate
INSERT OVERWRITE TABLE q7_results
SELECT
  severity,
  ROUND(AVG(CAST(distance_mi AS DOUBLE)), 2) AS avg_distance_mi,
  COUNT(*)                                   AS total_accidents
FROM accidents_part
GROUP BY severity
ORDER BY severity;

SELECT * FROM q7_results;