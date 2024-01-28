
WITH
  base AS (
  SELECT
    t.trainNumber,
    t.departureDate,
    t.trainType,
    t.trainCategory,
    t.version,
    tt.* EXCEPT(trainready,
      causes)
  FROM
    datasci.train_data t,
    UNNEST(t.timetablerows) tt
  WHERE
    TRUE
    AND trainstopping
    AND stationShortCode IN ( 'HKI',
      'TPE')
    AND CONCAT(type,'-',stationShortCode) <> 'DEPARTURE-TPE'
    AND actualtime IS NOT NULL
    -- and version = 276605740047
    ),
  versioning AS (
  SELECT
    * EXCEPT(departuredate,
      actualtime,
      scheduledTime),
    CAST(actualtime AS timestamp) AS actual_departure_time,
    CAST(scheduledTime AS timestamp) AS scheduled_departure_time,
    CAST(departuredate AS timestamp) AS departuredate,
    LEAD(actualtime,1) OVER (PARTITION BY version ORDER BY actualtime) AS actual_arrival,
    LEAD(scheduledTime,1) OVER (PARTITION BY version ORDER BY scheduledTime) AS scheduled_arrival,
    TIMESTAMP_DIFF(CAST(LEAD(actualtime,1) OVER (PARTITION BY version ORDER BY CAST(actualtime AS timestamp)) AS timestamp), CAST(LEAD(scheduledTime,1) OVER (PARTITION BY version ORDER BY CAST(scheduledTime AS timestamp)) AS timestamp),minute) AS delayed_arrival,
    TIMESTAMP_DIFF(CAST(LEAD(actualtime,1) OVER (PARTITION BY version ORDER BY CAST(actualtime AS timestamp)) AS timestamp),CAST(actualtime AS timestamp), minute) actual_journey_minutes,
    TIMESTAMP_DIFF(CAST(LEAD(scheduledTime,1) OVER (PARTITION BY version ORDER BY CAST(actualtime AS timestamp)) AS timestamp),CAST(scheduledTime AS timestamp), minute) scheduled_journey_minutes,
    FORMAT_DATE('%A',DATE(CAST(actualtime AS timestamp))) weekday,
    -- time(cast(extract(hour from cast(lead(actualtime,1) over (partition by version order by actualtime) as timestamp)) as int),cast(extract(minute from cast(lead(actualtime,1) over (partition by version order by actualtime) as timestamp)) as int)) as arrival_hour,
    CAST(FORMAT_TIMESTAMP('%H%M',CAST(LEAD(actualtime,1) OVER (PARTITION BY version ORDER BY actualtime) AS timestamp)) AS int) arrival_hour,
    CAST(FORMAT_TIMESTAMP('%H%M',CAST(LEAD(scheduledTime,1) OVER (PARTITION BY version ORDER BY scheduledTime) AS timestamp)) AS int) scheduled_hour
  FROM
    base ),
  exclude_outliers AS (
  SELECT
    delayed_arrival,
    COUNT(1)
  FROM
    versioning
  GROUP BY
    1
  ORDER BY
    1 DESC
  LIMIT
    3 --top 3 outliers.
    )
SELECT
  v.*,
IF
  (arrival_hour > 1600
    OR v.delayed_arrival >0,TRUE,FALSE) AS is_delayed,
  arrival_hour >0 AS is_train_delayed
FROM
  versioning v
LEFT JOIN
  exclude_outliers eo
ON
  v. delayed_arrival = eo.delayed_arrival
WHERE
  stationUICCode <> 160
  AND eo.delayed_arrival IS NULL
  -- and version = 287483952865