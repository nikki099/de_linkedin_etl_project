{{ config(materialized='table') }}


SELECT
CAST(JOB_DATE AS DATE) AS date,
DATE_PART('Month', JOB_DATE) AS month,
DATE_PART('Year', JOB_DATE) AS Year,
JOB_CATEGORY,
COUNT(DISTINCT ID) AS total_jobs
-- FROM {{ ref('int_linkedin_data') }}
FROM {{ source('linkedin_mart', 'INT_LINKEDIN_DATA') }}
GROUP BY
  JOB_CATEGORY,
  JOB_DATE,
  DATE_PART('Month', JOB_DATE),
  DATE_PART('Year', JOB_DATE)
