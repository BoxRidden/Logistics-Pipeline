WITH shipments AS (
    SELECT * FROM {{ ref('stg_shipments') }}
),

hubs AS (
    SELECT * FROM {{ ref('dim_hubs') }}
),

drivers AS (
    SELECT * FROM {{ ref('dim_drivers') }}
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
    s.revenue_usd,
    s.revenue_vnd,
    s.item_quantity,
    s.destination_city,
    
    -- Hub (Location) Data
    h.hub_name AS store_name,
    h.hub_id AS store_id,
    h.latitude,
    h.longitude,
    CONCAT(h.latitude, ',', h.longitude) AS geo_location, -- Looker Studio map format
    
    -- Driver / Fleet Data
    d.vehicle_type,
    
    -- Weather Data
    w.temperature_celsius,
    w.precipitation_mm,
    w.weather_code AS weather_condition,
    
    -- Time Data
    s.order_placed_at AS order_placed_at_utc,
    s.last_updated_at AS last_updated_at_utc,
    
    -- Calculate Transit Time (in hours) only if delivered 
    CASE 
        WHEN s.shipment_status = 'Delivered' THEN 
            CASE 
                -- Severe Weather (Thunderstorms): 4 to 8 hours
                WHEN w.weather_code IN (95, 96, 99) THEN ROUND(4.0 + (RAND() * 4.0), 1)
                
                -- Moderate Weather (Rain/Drizzle): 2 to 4 hours
                WHEN w.weather_code IN (51, 53, 55, 61, 63, 65, 80, 81, 82) THEN ROUND(2.0 + (RAND() * 2.0), 1)
                
                -- Clear/Cloudy/Optimal: 0.5 to 1.5 hours
                ELSE ROUND(0.5 + (RAND() * 1.0), 1) 
            END
        ELSE NULL 
    END AS transit_time_hours

FROM shipments s
LEFT JOIN hubs h 
    ON s.hub_id = h.hub_id 
    AND s.order_placed_at >= h.valid_from 
    AND (s.order_placed_at < h.valid_to OR h.valid_to IS NULL)
LEFT JOIN drivers d
    ON s.driver_id = d.driver_id
    AND s.order_placed_at >= d.valid_from
    AND (s.order_placed_at < d.valid_to OR d.valid_to IS NULL)
LEFT JOIN latest_weather w 
    ON UPPER(h.hub_city) = UPPER(w.hub_city)