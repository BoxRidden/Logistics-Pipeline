-- dbt/models/mart/dim_drivers.sql
-- Dimension table holding driver details and vehicle types

SELECT
    driver_id,
    name AS driver_name,
    vehicle_type,
    created_at AS employment_start_date
FROM {{ ref('drivers') }}