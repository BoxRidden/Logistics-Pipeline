SELECT
    hub_id,
    name AS hub_name,
    UPPER(city) AS hub_city,
    lat AS latitude,
    lon AS longitude
FROM {{ ref('hubs') }}
