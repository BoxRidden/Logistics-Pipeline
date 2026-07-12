-- dbt/models/mart/dim_drivers.sql
-- Dimension table holding driver details and vehicle types
SELECT
    driver_id,
    name AS driver_name,
    vehicle_type,
    valid_from,
    valid_to,
    is_current
FROM `logistics-500519`.`logistics_raw`.`drivers`
WHERE is_current = TRUE