

  create or replace view `logistics-500519`.`logistics_mart`.`stg_weather_consensus`
  OPTIONS()
  as 

WITH raw_weather AS (
    SELECT * FROM `logistics-500519`.`logistics_raw`.`weather_api_raw`
    WHERE captured_at IS NOT NULL
),

hourly_grouped AS (
    SELECT
        TRIM(hub_city) AS hub_city,
        TIMESTAMP_TRUNC(CAST(captured_at AS TIMESTAMP), HOUR) AS weather_captured_at,
        ROUND(AVG(CAST(temperature_2m AS FLOAT64)), 1) AS temperature_celsius,
        ROUND(AVG(CAST(precipitation AS FLOAT64)), 1) AS precipitation_mm,
        COUNT(1) AS api_response_count,
        -- Robust aggregation using code frequencies for consensus
        ARRAY_AGG(weather_code ORDER BY weather_code LIMIT 1)[OFFSET(0)] AS weather_code
    FROM raw_weather
    GROUP BY 1, 2
)

SELECT * FROM hourly_grouped;

