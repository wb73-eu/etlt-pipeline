{{ config(materialized='table') }}

WITH silver_data AS (
    SELECT * FROM {{ source('sch_cleaned_data', 'silver_table') }}
),

airport_data AS (
    SELECT * FROM {{ source('sch_cleaned_data', 'airports_table') }}
),

flights_origin AS (
    SELECT origin_airport_id as Airport, COUNT(*) as Flight_From
    FROM silver_data
    GROUP BY origin_airport_id
),

flights_destination AS (
    SELECT dest_airport_id as Airport, COUNT(*) as Flight_To
    FROM silver_data
    GROUP BY dest_airport_id
),

flights_per_airport AS (
    SELECT COALESCE(flights_origin.Airport, flights_destination.Airport) AS Airport, 
    a.city || ', ' || a.state AS Location, 
    COALESCE(Flight_From, 0) AS Flights_From, 
    COALESCE(Flight_To, 0) AS Flights_To
    FROM flights_origin
    FULL OUTER JOIN flights_destination ON flights_origin.Airport = flights_destination.Airport
    JOIN airport_data a ON COALESCE(flights_origin.Airport, flights_destination.Airport) = a.id
    ORDER BY Flight_From DESC, Flight_To DESC
)

SELECT * FROM flights_per_airport
