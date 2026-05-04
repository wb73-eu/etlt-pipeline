{{ config(materialized='table') }}

WITH top5_airports_flights AS (
    SELECT * FROM {{ ref('top5_airports_flights') }}
),

airport_data AS (
    SELECT * FROM {{ source('sch_cleaned_data', 'airports_table') }}
),

delay_average AS (
    SELECT
        origin_airport_id,
        op_unique_carrier,
        AVG(dep_delay) AS avg_dep_delay
    FROM top5_airports_flights
    GROUP BY origin_airport_id, op_unique_carrier
),

airline_ranking AS (
    SELECT
        origin_airport_id,
        op_unique_carrier,
        avg_dep_delay,
        ROW_NUMBER() OVER (PARTITION BY origin_airport_id ORDER BY avg_dep_delay) AS airline_rank
    FROM delay_average
)

SELECT
    origin_airport_id AS Airport,
    a.city || ', ' || a.state AS Location,
    op_unique_carrier AS Airline,
    avg_dep_delay AS Average_Departure_Delay
FROM airline_ranking
JOIN airport_data a ON a.id = origin_airport_id
WHERE airline_rank = 1