WITH raw_shipments AS (
    SELECT
        shipment_id,
        tracking_code,
        hub_id,
        driver_id,
        customer_city,
        status,
        --Grab the new columns from the raw BigQuery table
        revenue,
        item_quantity,
        product_category,
        order_type,
        created_at,
        updated_at
    FROM `logistics-500519`.`logistics_raw`.`shipments`
)

SELECT
    shipment_id,
    tracking_code,
    hub_id,
    driver_id,
    
    --Pass the new columns through to the fact model 
    revenue,
    item_quantity,
    product_category,
    order_type,
    
    --Preserve your existing transformations and aliases
    UPPER(customer_city) AS destination_city,
    COALESCE(status, 'Unknown') AS shipment_status,
    created_at AS order_placed_at,
    updated_at AS last_updated_at
FROM raw_shipments