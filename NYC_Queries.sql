/*.
Answer the following questions: from the tables on bigquery
*/

-- Question 1
--What season has the highest number of pickup rides (Winter, Summer, Autumn and Spring)
-- GREEN
SELECT season, count(*)
FROM `logical-craft-384210.alt.green_tripdata`
GROUP BY season
ORDER BY 2 desc
LIMIT 1;
-- for green service the season is winter

-- YELLOW
SELECT season, count(*)
FROM `logical-craft-384210.alt.yellow_tripdata`
GROUP BY season
ORDER BY 2 desc
LIMIT 1;
-- for yellow service the season is spring

--FHV
SELECT season, count(*)
FROM `logical-craft-384210.alt.fhv_tripdata`
GROUP BY season
ORDER BY 2 desc
LIMIT 1;
-- for for-hire-vehicle the season is winter

--Question 2
-- What period of the day has the highest pickup number
--GREEN
SELECT r.period_of_day
FROM(
  SELECT
    pickup_time,
    CASE
      WHEN EXTRACT(HOUR FROM pickup_time) >= 0 AND EXTRACT(HOUR FROM pickup_time) < 6 THEN 'Night'
      WHEN EXTRACT(HOUR FROM pickup_time) >= 6 AND EXTRACT(HOUR FROM pickup_time) < 12 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM pickup_time) >= 12 AND EXTRACT(HOUR FROM pickup_time) < 18 THEN 'Afternoon'
      ELSE 'Evening'
    END AS period_of_day
  FROM `logical-craft-384210.alt.green_tripdata`
) AS r
GROUP BY 1
LIMIT 1;
-- The period of the day that has the highest pickup number in Green services is Afternoon

--YELLOW
SELECT r.period_of_day
FROM(
  SELECT
    pickup_time,
    CASE
      WHEN EXTRACT(HOUR FROM pickup_time) >= 0 AND EXTRACT(HOUR FROM pickup_time) < 6 THEN 'Night'
      WHEN EXTRACT(HOUR FROM pickup_time) >= 6 AND EXTRACT(HOUR FROM pickup_time) < 12 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM pickup_time) >= 12 AND EXTRACT(HOUR FROM pickup_time) < 18 THEN 'Afternoon'
      ELSE 'Evening'
    END AS period_of_day
  FROM `logical-craft-384210.alt.yellow_tripdata`
) AS r
GROUP BY 1
LIMIT 1;
-- The period of the day that has the highest pickup number in Yellow services is Night

--FHV
SELECT r.period_of_day
FROM(
  SELECT
    pickup_time,
    CASE
      WHEN EXTRACT(HOUR FROM TIMESTAMP(pickup_time)) >= 0 AND EXTRACT(HOUR FROM TIMESTAMP(pickup_time)) < 6 THEN 'Night'
      WHEN EXTRACT(HOUR FROM TIMESTAMP(pickup_time)) >= 6 AND EXTRACT(HOUR FROM TIMESTAMP(pickup_time)) < 12 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM TIMESTAMP(pickup_time)) >= 12 AND EXTRACT(HOUR FROM TIMESTAMP(pickup_time)) < 18 THEN 'Afternoon'
      ELSE 'Evening'
    END AS period_of_day
  FROM `logical-craft-384210.alt.fhv_tripdata`
) AS r
GROUP BY 1
LIMIT 1;
-- The period of the day that has the highest pickup number in fhv services is Morning

--Question 3
-- What day of the week (Monday- Sunday) has the highest pickup number
--GREEN
SELECT 
  r.num_day_of_week,
  CASE
    WHEN num_day_of_week = 1 then "Sunday"
    WHEN num_day_of_week = 2 then "Monday"
    WHEN num_day_of_week = 3 then "Tuesday"
    WHEN num_day_of_week = 4 then "Wednesday"
    WHEN num_day_of_week = 5 then "Thursday"
    WHEN num_day_of_week = 6 then "Friday"
    ELSE "Saturday"
  END AS day_of_week,
FROM (
  SELECT
    pickup_time,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP(pickup_time)) AS num_day_of_week
  FROM `logical-craft-384210.alt.green_tripdata`
  LIMIT 1000
) AS r
GROUP BY 1
LIMIT 1;
-- The day of the week (Monday- Sunday) that has the highest pickup number in green services is Friday

--YELLOW
SELECT 
  r.num_day_of_week,
  CASE
    WHEN num_day_of_week = 1 then "Sunday"
    WHEN num_day_of_week = 2 then "Monday"
    WHEN num_day_of_week = 3 then "Tuesday"
    WHEN num_day_of_week = 4 then "Wednesday"
    WHEN num_day_of_week = 5 then "Thursday"
    WHEN num_day_of_week = 6 then "Friday"
    ELSE "Saturday"
  END AS day_of_week,
FROM (
  SELECT
    pickup_time,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP(pickup_time)) AS num_day_of_week
  FROM `logical-craft-384210.alt.fhv_tripdata`
  LIMIT 1000
) AS r
GROUP BY 1
LIMIT 1;
-- The day of the week (Monday- Sunday) that has the highest pickup number in yellow services is Saturday

--FHV
SELECT 
  r.num_day_of_week,
  CASE
    WHEN num_day_of_week = 1 then "Sunday"
    WHEN num_day_of_week = 2 then "Monday"
    WHEN num_day_of_week = 3 then "Tuesday"
    WHEN num_day_of_week = 4 then "Wednesday"
    WHEN num_day_of_week = 5 then "Thursday"
    WHEN num_day_of_week = 6 then "Friday"
    ELSE "Saturday"
  END AS day_of_week,
FROM (
  SELECT
    pickup_time,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP(pickup_time)) AS num_day_of_week
  FROM `logical-craft-384210.alt.fhv_tripdata`
  LIMIT 1000
) AS r
GROUP BY 1
LIMIT 1;
-- The day of the week (Monday- Sunday) that has the highest pickup number in fhv services is Saturday

-- Question 4
-- What zone has the highest total amount of paid.
-- GREEN
-- SELECT zone, sum(total_amount)
-- FROM `logical-craft-384210.alt.green_tripdata`
-- GROUP BY 1;
