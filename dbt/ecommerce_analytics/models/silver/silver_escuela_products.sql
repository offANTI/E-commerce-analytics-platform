{{ config(materialized='view', schema='silver') }}

SELECT
    (item->>'id')::INT AS product_id,
    (item->>'title')::VARCHAR(255) AS title,
    CASE
        WHEN (item->>'price')::NUMERIC BETWEEN 1 AND 10000
        THEN (item->>'price')::NUMERIC(12,2)
        ELSE NULL::NUMERIC(12,2)
    END AS price,
    CASE
        WHEN (item->>'price')::NUMERIC BETWEEN 1 AND 10000
        THEN TRUE
        ELSE FALSE
    END AS is_valid_price,
    0.00::NUMERIC(5,2) AS discount_percentage,
    NULL::NUMERIC(3,2) AS rating,
    NULL::VARCHAR(100) AS brand,
    (item->'category'->>'id')::INT AS category_id
FROM {{ source('bronze', 'escuela_products') }},
LATERAL jsonb_array_elements(raw_data) AS item