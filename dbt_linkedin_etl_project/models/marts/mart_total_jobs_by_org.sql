{{ config(materialized='view') }}
-- total jobs to date by organizations

SELECT
ORGANIZATION,
JOB_CATEGORY,
COUNT(DISTINCT ID) AS total_jobs
-- FROM {{ ref('int_linkedin_data') }}
FROM {{ source('linkedin_mart', 'INT_LINKEDIN_DATA') }}
GROUP BY
  ORGANIZATION,
  JOB_CATEGORY
ORDER BY
  ORGANIZATION,
  JOB_CATEGORY,
  total_jobs DESC
