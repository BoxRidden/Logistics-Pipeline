WITH shipments AS (
    SELECT * FROM {{ ref('stg_shipments') }}
),

hubs AS (
    SELECT * FROM {{ ref('dim_hubs') }}
),

-- Get only the single most recent weather record per city to avoid duplication
latest_weather AS (
    SELECT * FROM {{ ref('stg_weather_consensus') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY UPPER(hub_city) ORDER BY weather_captured_at DESC) = 1
)

SELECT
    s.shipment_id,
    s.tracking_code,
    s.shipment_status,
    s.order_type,
    s.product_category,
    s.revenue,
    s.item_quantity,
    s.destination_city,
    h.hub_name AS store_name,
    h.hub_id AS store_id,
    w.temperature_celsius,
    w.precipitation_mm,
    w.weather_code AS weather_condition,
    s.order_placed_at AS order_placed_at_utc,
    s.last_updated_at AS last_updated_at_utc
FROM shipments s
LEFT JOIN hubs h 
    ON s.hub_id = h.hub_id 
    AND s.order_placed_at >= h.valid_from 
    AND (s.order_placed_at < h.valid_to OR h.valid_to IS NULL)
LEFT JOIN latest_weather w 
    ON UPPER(h.hub_city) = UPPER(w.hub_city)