{{ config(materialized='table') }}


SELECT DISTINCT
    ID, TITLE, JOB_CATEGORY, URL
FROM {{ source('linkedin_base', 'LINKEDIN_JOB_API_CLEANED_DATA') }}
ORDER BY JOB_CATEGORY, ID, TITLE
