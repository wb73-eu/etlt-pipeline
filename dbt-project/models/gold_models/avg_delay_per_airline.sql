{{ config(materialized='table') }}

WITH top5_airports_flights AS (
    SELECT * FROM {{ ref('top5_airports_flights') }}
)

SELECT op_unique_carrier AS Airline_Code, AVG(dep_delay) AS Average_Departure_Delay
FROM top5_airports_flights
GROUP BY op_unique_carrier
ORDER BY Average_Departure_Delay DESC