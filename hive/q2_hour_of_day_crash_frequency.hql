USE team5_projectdb;

DROP TABLE IF EXISTS q2_results;
CREATE EXTERNAL TABLE q2_results (
  hour_of_day   INT,
  accident_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team5/project/hive/output/q2';

INSERT OVERWRITE TABLE q2_results
SELECT
  HOUR(start_time) AS hour_of_day,
  COUNT(*)         AS accident_count
FROM accidents_part
GROUP BY HOUR(start_time)
ORDER BY hour_of_day;

SELECT * FROM q2_results;