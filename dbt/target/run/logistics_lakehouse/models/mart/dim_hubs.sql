
  
    

    create or replace table `logistics-500519`.`logistics_mart`.`dim_hubs`
      
    
    

    
    OPTIONS()
    as (
      SELECT
    hub_id,
    name AS hub_name,
    UPPER(city) AS hub_city,
    lat AS latitude,
    lon AS longitude,
    valid_from,
    valid_to,
    is_current
FROM `logistics-500519`.`logistics_raw`.`hubs`
WHERE is_current = TRUE
    );
  