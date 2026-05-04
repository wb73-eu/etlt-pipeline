{{ config(materialized='table') }}

WITH silver_data AS (
    SELECT * FROM {{ source('sch_cleaned_data', 'silver_table') }}
),

top5_busiest_airports AS (
    SELECT Airport
    FROM {{ ref('top_airports') }}
    ORDER BY Flights_From DESC
    LIMIT 5
)

SELECT * FROM silver_data
WHERE origin_airport_id IN (SELECT Airport FROM top5_busiest_airports)