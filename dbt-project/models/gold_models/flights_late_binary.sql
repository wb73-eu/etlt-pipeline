{{ config(materialized='table') }}

WITH silver_data AS (
    SELECT * FROM {{ source('sch_cleaned_data', 'silver_table') }}
),

flights_late_binary AS (
    SELECT 
        *,
        CASE 
            WHEN dep_delay > 0 THEN 1 
            ELSE 0 
        END AS is_late
    FROM silver_data
)

SELECT * FROM flights_late_binary