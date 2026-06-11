{{ config(
    materialized='view',
    schema='silver'
) }}

SELECT
    order_id,
    user_id,
    product_id,
    store_name,
    quantity,
    unit_price,
    total_amount,
    order_date::DATE AS order_date,
    created_at
FROM {{ source('bronze', 'orders') }}
WHERE total_amount > 0
  AND quantity > 0
  AND order_date IS NOT NULL
