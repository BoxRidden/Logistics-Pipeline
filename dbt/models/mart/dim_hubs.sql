SELECT
    hub_id,
    name AS hub_name,
    UPPER(city) AS hub_city,
    lat AS latitude,
    lon AS longitude
FROM {{ source('postgres_source', 'hubs') }}
QUALIFY ROW_NUMBER() OVER(PARTITION BY hub_id ORDER BY valid_from DESC) = 1