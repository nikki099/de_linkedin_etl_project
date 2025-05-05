{{ config(materialized='view') }}
-- total jobs to date by state

SELECT
JOB_CATEGORY,
STATE,
COUNT(DISTINCT ID) AS total_jobs
-- FROM {{ ref('int_linkedin_data') }}
FROM {{ source('linkedin_mart', 'INT_LINKEDIN_DATA') }}
GROUP BY
  JOB_CATEGORY,
  STATE
ORDER BY
  JOB_CATEGORY,
  STATE
