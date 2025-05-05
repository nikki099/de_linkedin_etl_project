{{ config(materialized='table') }}

-- fix some wrong city names
WITH CITY_BASE AS (
  SELECT DISTINCT CITY, STATE
  FROM {{ source('linkedin_base', 'LINKEDIN_JOB_API_CLEANED_DATA') }}
)
SELECT
  CITY,
  CASE
    WHEN lower(CITY) = 'new south wales' THEN 'Sydney'
    WHEN lower(CITY) = 'capital australia' THEN 'Canberra'
    WHEN lower(CITY) = 'barzbin' THEN 'Brisbane'
    WHEN lower(CITY) = 'queensland' THEN 'Brisbane'
    WHEN lower(CITY) = 'south australia' THEN 'Adelaide'
    WHEN lower(CITY) = 'australian capital territory' THEN 'Canberra'
    WHEN lower(CITY) = 'victoria' THEN 'Melbourne'
    WHEN lower(CITY) = 'northern territory' THEN 'Darwin'
    WHEN lower(CITY) = 'west australia' THEN 'Perth'
    WHEN lower(CITY) LIKE '%tasmania%' THEN 'Hobart'
    WHEN lower(CITY) = 'no regrets' THEN 'Unknown'
    WHEN lower(CITY) = 'other side of the moon' THEN 'Unknown'
    WHEN lower(CITY) LIKE '%sydney%' THEN 'Sydney'
    WHEN lower(CITY) LIKE '%melbourne%' THEN 'Melbourne'
    WHEN lower(CITY) LIKE '%brisbane%' THEN 'Brisbane'
    WHEN lower(CITY) LIKE '%canberra%' THEN 'Canberra'
    WHEN lower(CITY) LIKE '%perth%' THEN 'Perth'
    WHEN lower(CITY) LIKE '%adelaide%' THEN 'Adelaide'
    WHEN lower(CITY) LIKE '%darwin%' THEN 'Darwin'
    WHEN lower(CITY) LIKE '%hobart%' THEN 'Hobart'
    ELSE CITY
  END AS CITY_STANDARDIZED,
  STATE
FROM CITY_BASE
