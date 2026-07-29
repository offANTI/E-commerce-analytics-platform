{{ config(
    materialized='incremental',
    unique_key='order_id',
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

{% if is_incremental() %}
  AND created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
