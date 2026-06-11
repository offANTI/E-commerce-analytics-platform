{{ config(materialized='view', schema='silver') }}

SELECT
    id,
    raw_data->>'title'       AS product_title,
    raw_data->>'brand'       AS brand,
    raw_data->>'category'    AS category,
    (raw_data->>'price')::NUMERIC(10,2) AS price,
    (raw_data->>'stock')::INT           AS stock,
    loaded_at
FROM {{ source('bronze', 'dummy_products') }}
WHERE raw_data IS NOT NULL
  AND raw_data->>'price' IS NOT NULL
