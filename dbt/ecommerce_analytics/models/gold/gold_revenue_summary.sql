{{ config(materialized='table', schema='gold') }}

SELECT
    store_name,
    order_date,
    COUNT(order_id)            AS total_orders,
    SUM(total_amount)          AS total_revenue,
    AVG(total_amount)          AS avg_order_value,
    SUM(quantity)              AS total_items_sold
FROM {{ ref('silver_orders') }}
GROUP BY store_name, order_date
