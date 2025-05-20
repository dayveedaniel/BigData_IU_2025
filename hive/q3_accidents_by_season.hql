USE team5_projectdb;

DROP TABLE IF EXISTS q3_results;
CREATE EXTERNAL TABLE q3_results (
  year            INT,
  season          STRING,
  accident_count  BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team5/project/hive/output/q3';

INSERT OVERWRITE TABLE q3_results
SELECT
  year,
  CASE
    WHEN month IN (12,1,2) THEN 'Winter'
    WHEN month IN (3,4,5)  THEN 'Spring'
    WHEN month IN (6,7,8)  THEN 'Summer'
    ELSE                       'Fall'
  END AS season,
  COUNT(*) AS accident_count
FROM accidents_part
GROUP BY
  year,
  CASE
    WHEN month IN (12,1,2) THEN 'Winter'
    WHEN month IN (3,4,5)  THEN 'Spring'
    WHEN month IN (6,7,8)  THEN 'Summer'
    ELSE                       'Fall'
  END
ORDER BY
  year,
  CASE season
    WHEN 'Winter' THEN 1
    WHEN 'Spring' THEN 2
    WHEN 'Summer' THEN 3
    ELSE               4
  END;
  
SELECT * FROM q3_results;