WITH shipments AS (
    SELECT * FROM {{ ref('stg_shipments') }}
),

hubs AS (
    SELECT * FROM {{ ref('dim_hubs') }}
),

weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
)

SELECT
    s.shipment_id,
    s.tracking_code,
    s.shipment_status,
    s.destination_city,
    h.hub_name,
    h.hub_city,
    
    -- Metrics
    w.temperature_celsius,
    w.precipitation_mm,
    w.weather_code,
    
    s.order_placed_at,
    s.last_updated_at
FROM shipments s
LEFT JOIN hubs h ON s.hub_id = h.hub_id
LEFT JOIN weather w 
    ON h.hub_city = w.hub_city 
    -- Matches weather to the day the order was placed
    AND DATE(s.order_placed_at) = DATE(w.weather_captured_at)