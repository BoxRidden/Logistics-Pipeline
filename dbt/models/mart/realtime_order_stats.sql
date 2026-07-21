{{ config(materialized='table') }}

SELECT 
    s.hub_id AS store_id, 
    h.hub_name AS store_name,
    s.shipment_status, 
    s.order_type, 
    TIMESTAMP_TRUNC(s.order_placed_at, HOUR) AS order_hour, 
    COUNT(s.shipment_id) AS order_count, 
    SUM(s.revenue_usd) AS total_revenue_usd, 
    SUM(s.revenue_vnd) AS total_revenue_vnd,
    SUM(s.item_quantity) AS total_items_sold
FROM 
    {{ ref('stg_shipments') }} s
LEFT JOIN 
    {{ ref('dim_hubs') }} h ON s.hub_id = h.hub_id
GROUP BY 
    1, 2, 3, 4, 5