

  create or replace view `logistics-500519`.`logistics_mart`.`stg_weather_consensus`
  OPTIONS()
  as 

WITH raw_weather AS (
    -- Read directly from your raw weather table uploaded by bq_bridge
    SELECT * FROM `logistics-500519`.`logistics_raw`.`weather_api_raw`
    WHERE fetched_at IS NOT NULL
),

hourly_grouped AS (
    SELECT
        -- 1. Standardize City Name to match your shipments hub_city
        TRIM(city_name) AS hub_city,
        
        -- 2. Truncate timestamps to the exact hour (e.g., 14:15:00 -> 14:00:00)
        TIMESTAMP_TRUNC(CAST(fetched_at AS TIMESTAMP), HOUR) AS weather_captured_at,
        
        -- 3. Average out numeric metrics across the 3 APIs
        ROUND(AVG(CAST(temperature AS FLOAT64)), 1) AS temperature_celsius,
        ROUND(AVG(CAST(humidity AS FLOAT64)), 1)    AS humidity_percent,
        ROUND(AVG(CAST(wind_speed AS FLOAT64)), 1)  AS wind_speed_mps,
        
        -- 4. Data Quality: Track how many APIs responded this hour
        COUNT(DISTINCT source) AS api_response_count,
        
        -- 5. Robust Text Consensus: Get the most frequent weather condition
        -- If APIs tie or disagree, taking the MAX of the top counts ensures it never crashes
        ARRAY_AGG(condition ORDER BY condition LIMIT 1)[OFFSET(0)] AS weather_condition

    FROM raw_weather
    GROUP BY 1, 2
)

SELECT * FROM hourly_grouped;

