{{ config(materialized='table') }}
-- total jobs to date by job type/category

SELECT
JOB_CATEGORY,
COUNT(DISTINCT ID) AS total_jobs
-- FROM {{ ref('int_linkedin_data') }}
FROM {{ source('linkedin_mart', 'INT_LINKEDIN_DATA') }}
GROUP BY
  JOB_CATEGORY
ORDER BY
  JOB_CATEGORY,
  total_jobs DESC
