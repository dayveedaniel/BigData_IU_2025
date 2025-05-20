USE team5_projectdb;

-- results table
DROP TABLE IF EXISTS q6_results;
CREATE EXTERNAL TABLE q6_results (
    year               INT,
    intersection_type  STRING,   -- e.g.  "signal_roundabout_crosswalk"
    total_accidents    BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team5/project/hive/output/q6';

-- overwrite instead of append
INSERT OVERWRITE TABLE q6_results
SELECT
    a.year                                                        AS year,               -- comes from partition column
    CONCAT_WS('_',
        IF(rf.traffic_signal , 'signal'     , 'no_signal'),
        IF(rf.roundabout     , 'roundabout' , 'no_roundabout'),
        IF(rf.crossing       , 'crosswalk'  , 'no_crosswalk')
    )                                                             AS intersection_type,
    COUNT(*)                                                      AS total_accidents
FROM accidents_part  a
JOIN road_features   rf ON a.road_feat_id = rf.id
WHERE rf.traffic_signal IS NOT NULL      -- all three flags must be TRUE/FALSE (never NULL)
  AND rf.roundabout     IS NOT NULL
  AND rf.crossing       IS NOT NULL
GROUP BY
    a.year,
    rf.traffic_signal,
    rf.roundabout,
    rf.crossing
ORDER BY
    year,
    intersection_type;
    
SELECT * FROM q6_results;