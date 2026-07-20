
  
    

    create or replace table `logistics-500519`.`logistics_mart`.`dim_drivers`
      
    
    

    
    OPTIONS()
    as (
      -- dbt/models/mart/dim_drivers.sql
-- Dimension table holding driver details and vehicle types
SELECT
    driver_id,
    name AS driver_name,
    vehicle_type
FROM `logistics-500519`.`logistics_raw`.`drivers`
QUALIFY ROW_NUMBER() OVER(PARTITION BY driver_id ORDER BY valid_from DESC) = 1
    );
  