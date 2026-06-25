WITH raw_shipments AS (
    SELECT
        shipment_id,
        tracking_code,
        hub_id,
        driver_id,
        customer_city,
        status,
        created_at,
        updated_at
    FROM `logistics-500519`.`logistics_mart`.`shipments`
)

SELECT
    shipment_id,
    tracking_code,
    hub_id,
    driver_id,
    UPPER(customer_city) AS destination_city,
    COALESCE(status, 'Unknown') AS shipment_status,
    created_at AS order_placed_at,
    updated_at AS last_updated_at
FROM raw_shipments