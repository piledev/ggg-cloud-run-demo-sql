SELECT 
  A.timestamp,
  IFNULL( B.requests_per_seconds, 0 ) AS requests_per_seconds,
  IFNULL(B.instances_per_seconds, 0 ) AS instances_per_seconds
FROM (
  SELECT DATETIME_ADD( DATETIME @begin_at , INTERVAL unit SECOND) AS timestamp
  FROM (
    SELECT sec * @interval_seconds as unit
    FROM UNNEST(
      GENERATE_ARRAY(
        0, 
        CAST( DATETIME_DIFF( COALESCE( DATETIME @end_at, CURRENT_DATETIME() ), DATETIME @begin_at, SECOND ) AS INT64),
        @interval_seconds 
      )
    ) 
    WITH OFFSET AS sec
    )
) AS A
LEFT JOIN (
  SELECT 
    DATETIME_ADD(
      DATETIME @begin_at,
      INTERVAL CAST( 
        FLOOR( 
          DATETIME_DIFF( 
            DATETIME(`timestamp`), 
            DATETIME @begin_at, 
            SECOND 
          ) / @interval_seconds
        ) AS INT64
      ) * @interval_seconds SECOND
    ) AS timestamp,
    COUNT(labels.instanceid) AS requests_per_seconds,
    COUNT(distinct labels.instanceid) AS instances_per_seconds

  FROM  `ggg-cloud-run-demo.cloud_run_revision.run_googleapis_com_requests` 
  WHERE `timestamp` between TIMESTAMP @begin_at AND COALESCE( @end_at, CURRENT_TIMESTAMP() )
  AND resource.labels.service_name = @service_name
  GROUP BY timestamp
) AS B
ON A.timestamp = B.timestamp 
WHERE A.timestamp BETWEEN  DATETIME @begin_at AND COALESCE( @end_at, CURRENT_TIMESTAMP() )
ORDER BY A.timestamp
;
