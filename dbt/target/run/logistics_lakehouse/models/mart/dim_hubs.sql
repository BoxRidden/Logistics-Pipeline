
  
    

    create or replace table `logistics-500519`.`logistics_mart`.`dim_hubs`
      
    
    

    
    OPTIONS()
    as (
      SELECT
    hub_id,
    name AS hub_name,
    UPPER(city) AS hub_city,
    lat AS latitude,
    lon AS longitude
FROM `logistics-500519`.`logistics_mart`.`hubs`
    );
  