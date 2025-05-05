-- fix some wrong state names

{{ config(materialized='table') }}

WITH STATE_BASE AS (
SELECT CITY_STANDARDIZED, STATE
FROM {{ ref('base_city') }}
)
SELECT
    DISTINCT CITY_STANDARDIZED,
    CASE
        WHEN lower(STATE) IN ('of', 'on', 'australia', 'na') AND CITY_STANDARDIZED = 'Perth' THEN 'WA'
        WHEN lower(STATE) IN ('of', 'on', 'australia', 'na') AND CITY_STANDARDIZED = 'Brisbane' THEN 'QLD'
        WHEN lower(STATE) IN ('of', 'on', 'australia', 'na') AND CITY_STANDARDIZED = 'Sydney' THEN 'NSW'
        WHEN lower(STATE) IN ('of', 'on', 'australia', 'na') AND CITY_STANDARDIZED = 'Melbourne' THEN 'VIC'
        WHEN lower(STATE) IN ('of', 'on', 'australia', 'na') AND CITY_STANDARDIZED = 'Canberra' THEN 'ACT'
        WHEN lower(STATE) IN ('of', 'on', 'australia', 'na') AND CITY_STANDARDIZED = 'Adelaide' THEN 'SA'
        WHEN lower(STATE) IN ('of', 'on', 'australia', 'na') AND CITY_STANDARDIZED = 'Hobart' THEN 'TAS'
        WHEN lower(STATE) IN ('of', 'on', 'australia', 'na') AND CITY_STANDARDIZED = 'Darwin' THEN 'NT'
        WHEN lower(STATE) IN ('of', 'on', 'australia', 'na') AND CITY_STANDARDIZED = 'Woden' THEN 'ACT'
        WHEN lower(STATE) = 'na' THEN 'UNKNOWN'
        ELSE STATE
    END AS STATE_STANDARDIZED
FROM STATE_BASE
