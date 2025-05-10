{{ config(materialized='table') }}


-- complete week calc
-- WITH base AS (
--   SELECT * FROM  {{ ref('mart_total_jobs_daily') }}
-- ),
-- weekly_jobs AS (SELECT
-- JOB_CATEGORY,
-- DATE_TRUNC('WEEK', date) AS week_start,
-- SUM(total_jobs) AS total_jobs_this_week
-- FROM base
-- GROUP BY JOB_CATEGORY, week_start),
-- wow_jobs AS (
-- SELECT
-- *,
-- LAG(total_jobs_this_week) OVER (PARTITION BY JOB_CATEGORY ORDER BY week_start) AS total_jobs_last_week,
-- total_jobs_this_week -  LAG(total_jobs_this_week) OVER (PARTITION BY JOB_CATEGORY ORDER BY week_start) AS wow_diff
-- FROM weekly_jobs)
-- SELECT
-- *,
-- wow_diff/NULLIF(total_jobs_last_week, 0) as wow_diff_pct
-- FROM wow_jobs
-- ORDER BY JOB_CATEGORY, week_start


-- WOW DYNAMIC CALC
WITH base AS (
    SELECT *,
        DAYOFWEEK(date) as dow  -- week starting on Sunday
    FROM {{ ref('mart_total_jobs_daily') }}
),
params AS (
    SELECT
        CURRENT_DATE() AS report_date,  -- get this week date
        DAYOFWEEK(CURRENT_DATE()) as report_dow
),
filtered AS (
    SELECT
        b.JOB_CATEGORY,
        DATE_TRUNC('WEEK', b.date) AS week_start,
        b.dow,
        b.total_jobs,
        p.report_date,
        p.report_dow
    FROM base b
    CROSS JOIN params p
    WHERE b.dow <= p.report_dow
      AND b.date <= p.report_date
),
agg_weekly AS(
SELECT JOB_CATEGORY,
week_start,
SUM(total_jobs) as jobs_sum
FROM filtered
GROUP BY JOB_CATEGORY, week_start),
final AS(
SELECT
JOB_CATEGORY,
week_start,
jobS_sum,
LAG(jobS_sum, 1) OVER (PARTITION BY JOB_CATEGORY ORDER BY week_start) AS lastweek_jobs_sum
FROM agg_weekly)
SELECT
JOB_CATEGORY,
week_start,
jobs_sum AS this_week_jobs,
lastweek_jobs_sum AS last_week_jobs,
jobs_sum - lastweek_jobs_sum  AS wow_diff,
(jobs_sum - lastweek_jobs_sum)/NULLIF(lastweek_jobs_sum, 0) as wow_diff_pct
FROM final
ORDER BY JOB_CATEGORY, week_start DESC
