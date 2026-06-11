{{ config(materialized='table', schema='gold') }}

SELECT
    user_id,
    COUNT(order_id)                             AS total_orders,
    SUM(total_amount)                           AS lifetime_value,
    AVG(total_amount)                           AS avg_order_value,
    MIN(order_date)                             AS first_order_date,
    MAX(order_date)                             AS last_order_date,
    MAX(order_date) - MIN(order_date)           AS customer_lifespan_days,
    RANK() OVER (ORDER BY SUM(total_amount) DESC) AS ltv_rank
FROM {{ ref('silver_orders') }}
GROUP BY user_id
