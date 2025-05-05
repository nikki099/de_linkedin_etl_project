{{ config(materialized='view') }}


WITH base AS (
  SELECT * FROM  {{ ref('mart_total_jobs_daily') }}
),
weekly_jobs AS (SELECT
JOB_CATEGORY,
DATE_TRUNC('WEEK', date) AS week_start,
SUM(total_jobs) AS total_jobs_this_week
FROM base
GROUP BY JOB_CATEGORY, week_start),
wow_jobs AS (
SELECT
*,
LAG(total_jobs_this_week) OVER (PARTITION BY JOB_CATEGORY ORDER BY week_start) AS total_jobs_last_week,
total_jobs_this_week -  LAG(total_jobs_this_week) OVER (PARTITION BY JOB_CATEGORY ORDER BY week_start) AS wow_diff
FROM weekly_jobs)
SELECT
*,
wow_diff/NULLIF(total_jobs_last_week, 0) as wow_diff_pct
FROM wow_jobs
ORDER BY JOB_CATEGORY, week_start
