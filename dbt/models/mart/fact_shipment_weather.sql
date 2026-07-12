WITH shipments AS (
    SELECT * FROM {{ ref('stg_shipments') }}
),

hubs AS (
    SELECT * FROM {{ ref('dim_hubs') }}
),

weather AS (
    SELECT * FROM {{ ref('stg_weather_consensus') }}
)

SELECT
    s.shipment_id,
    s.tracking_code,
    s.shipment_status,
    s.destination_city,
    h.hub_name,
    h.hub_city,
    w.temperature_celsius,
    w.precipitation_mm,
    w.weather_code,
    s.order_placed_at,
    s.last_updated_at
FROM shipments s
LEFT JOIN hubs h 
    ON s.hub_id = h.hub_id 
    AND s.order_placed_at >= h.valid_from 
    AND (s.order_placed_at < h.valid_to OR h.valid_to IS NULL)
LEFT JOIN weather w 
    ON UPPER(h.hub_city) = UPPER(w.hub_city)
    AND TIMESTAMP_TRUNC(s.order_placed_at, HOUR) = w.weather_captured_at