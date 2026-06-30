WITH raw_weather AS (
    SELECT
        hub_city,
        captured_at,
        -- Extracting values from the JSON structure depending on ingestion format
        temperature_2m,
        precipitation,
        weather_code
    FROM `logistics-500519`.`logistics_raw`.`weather_api_raw`
)

SELECT
    UPPER(hub_city) AS hub_city,
    CAST(captured_at AS TIMESTAMP) AS weather_captured_at,
    CAST(temperature_2m AS FLOAT64) AS temperature_celsius,
    CAST(precipitation AS FLOAT64) AS precipitation_mm,
    weather_code
FROM raw_weather