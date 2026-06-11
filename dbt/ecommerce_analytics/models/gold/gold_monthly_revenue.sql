{{ config(materialized='table', schema='gold') }}

SELECT
    DATE_TRUNC('month', order_date)::DATE  AS order_month,
    store_name,
    SUM(total_amount)                       AS total_revenue,
    COUNT(order_id)                         AS orders_count,
    AVG(total_amount)                       AS avg_order_value
FROM {{ ref('silver_orders') }}
GROUP BY DATE_TRUNC('month', order_date), store_name
ORDER BY order_month
