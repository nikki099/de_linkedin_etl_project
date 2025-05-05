{{ config(materialized='view') }}
-- total jobs to date by city

SELECT
JOB_CATEGORY,
CITY,
COUNT(DISTINCT ID) AS total_jobs
-- FROM {{ ref('int_linkedin_data') }}
FROM {{ source('linkedin_mart', 'INT_LINKEDIN_DATA') }}
GROUP BY
  JOB_CATEGORY,
  CITY
ORDER BY
  JOB_CATEGORY,
  CITY,
  total_jobs DESC
