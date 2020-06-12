SELECT 
  A.timestamp,
  IFNULL( B.requests_per_seconds, 0 ) AS requests_per_seconds,
  IFNULL(B.instances_per_seconds, 0 ) AS instances_per_seconds
FROM (
  SELECT DATETIME_ADD( DATETIME "2020-06-10 10:53:01" , INTERVAL unit SECOND) AS timestamp
  FROM (
    SELECT sec * 10 as unit
    FROM 
        UNNEST(
          GENERATE_ARRAY( 
            0, 
            CAST( DATETIME_DIFF( COALESCE( NULL, CURRENT_DATETIME() ), DATETIME "2020-06-10 10:53:01",SECOND ) AS INT64),
            10
          )
        ) 
    WITH OFFSET AS sec
    )
) AS A
LEFT JOIN (
  SELECT 
    DATETIME_ADD(
      DATETIME "2020-06-10 10:53:01",
      INTERVAL CAST( 
        FLOOR( 
          DATETIME_DIFF( 
            DATETIME(`timestamp`), 
            DATETIME "2020-06-10 10:53:01", 
            SECOND 
          ) / 10
        ) AS INT64
      ) * 10 SECOND
    ) AS timestamp,
    COUNT(labels.instanceid) AS requests_per_seconds,
    COUNT(distinct labels.instanceid) AS instances_per_seconds

  FROM  `ggg-cloud-run-demo.cloud_run_revision.run_googleapis_com_requests` 
  WHERE `timestamp` between TIMESTAMP "2020-06-10 10:53:01" AND COALESCE(null, CURRENT_TIMESTAMP() )
  AND resource.labels.service_name = "helloworld"
  GROUP BY timestamp
) AS B
ON A.timestamp = B.timestamp
WHERE A.timestamp BETWEEN  DATETIME "2020-06-10 10:53:01" AND DATETIME "2020-06-10 10:55:00"
ORDER BY A.timestamp
;