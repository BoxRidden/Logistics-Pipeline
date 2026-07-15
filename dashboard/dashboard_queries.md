DROP VIEW IF EXISTS `<your-project-name>.logistics_mart.realtime_order_stats`;

CREATE MATERIALIZED VIEW `<your-project-name>.logistics_mart.realtime_order_stats` AS
SELECT 
    hub_id AS store_id, 
    status AS shipment_status, 
    order_type, 
    TIMESTAMP_TRUNC(created_at, HOUR) AS order_hour, 
    COUNT(shipment_id) AS order_count, 
    SUM(revenue) AS total_revenue, 
    SUM(item_quantity) AS total_items_sold
FROM 
    `<your-project-name>.logistics_raw.shipments` 
GROUP BY 
    1, 2, 3, 4;