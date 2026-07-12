-- ============================================================================
-- FILE: gold_bq_mv_realtime_order_stats.sql
-- DESCRIPTION: Materialized view for near-real-time dashboard scorecards.
-- REVIEWER NOTE: Materialized views compute aggregations incrementally in 
-- BigQuery's background infrastructure. This prevents Looker Studio from 
-- executing full table scans on every dashboard refresh.
-- ============================================================================

CREATE MATERIALIZED VIEW `logistics-500519.logistics_mart.realtime_order_stats` AS
SELECT
    hub_id,
    status AS shipment_status,
    order_type,
    -- Time truncation allows Looker to filter by hour efficiently
    TIMESTAMP_TRUNC(created_at, HOUR) AS order_hour,
    COUNT(shipment_id) AS order_count,
    SUM(revenue) AS total_revenue,
    SUM(item_quantity) AS total_items_sold
FROM 
    `logistics-500519.logistics_raw.shipments`
GROUP BY 
    1, 2, 3, 4;