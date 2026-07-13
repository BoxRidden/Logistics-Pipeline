
  
    

    create or replace table `logistics-500519`.`logistics_mart`.`fact_shipment_weather`
      
    
    

    
    OPTIONS()
    as (
      WITH shipments AS (
    SELECT * FROM `logistics-500519`.`logistics_mart`.`stg_shipments`
),

hubs AS (
    SELECT * FROM `logistics-500519`.`logistics_mart`.`dim_hubs`
),

weather AS (
    SELECT * FROM `logistics-500519`.`logistics_mart`.`stg_weather_consensus`
)

SELECT
    s.shipment_id,
    s.tracking_code,
    s.shipment_status,                 -- Reverted to staging alias
    s.order_type,
    s.product_category,
    s.revenue,
    s.item_quantity,
    s.destination_city,                -- Reverted to staging alias
    h.hub_name AS store_name,
    h.hub_id AS store_id,
    w.temperature_celsius,
    w.precipitation_mm,
    w.weather_code AS weather_condition,
    s.order_placed_at AS order_placed_at_utc, -- Reverted to staging alias
    s.last_updated_at AS last_updated_at_utc  -- Reverted to staging alias
FROM shipments s
LEFT JOIN hubs h 
    ON s.hub_id = h.hub_id 
    AND s.order_placed_at >= h.valid_from 
    AND (s.order_placed_at < h.valid_to OR h.valid_to IS NULL)
LEFT JOIN weather w 
    ON UPPER(h.hub_city) = UPPER(w.hub_city)
    AND TIMESTAMP_TRUNC(s.order_placed_at, HOUR) = w.weather_captured_at
    );
  