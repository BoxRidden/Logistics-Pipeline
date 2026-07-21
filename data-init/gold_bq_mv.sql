-- Materialized view for near-real-time dashboard scorecards.
-- Looker Studio will query this view for operational charts,
-- benefiting from automatic incremental background refreshes by BigQuery.

CREATE MATERIALIZED VIEW `logistics-500519.logistics_mart.realtime_order_stats` AS
SELECT
    hub_id AS store_id,
    status AS shipment_status,
    order_type,
    -- Truncate to hour to allow BI tools to group time-series efficiently 
    TIMESTAMP_TRUNC(created_at, HOUR) AS order_hour,
    COUNT(shipment_id) AS order_count,
    SUM(revenue) AS total_revenue,
    SUM(item_quantity) AS total_items_sold
FROM 
    `logistics-500519.logistics_raw.shipments`
GROUP BY 
    1, 2, 3, 4;