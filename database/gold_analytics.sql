CREATE SCHEMA IF NOT EXISTS gold;


CREATE OR REPLACE VIEW gold.all_store_products_ranked AS
WITH unified_products AS (
    SELECT
        product_id,
        'DummyJSON'::VARCHAR(50) AS store_name,
        title,
        price,
        brand,
        category_name AS category
    FROM silver.dummy_products

    UNION ALL

    SELECT
        p.product_id,
        'Escuela'::VARCHAR(50) AS store_name,
        p.title,
        p.price,
        p.brand,
        c.category_name AS category
    FROM silver.escuela_products p
    JOIN silver.escuela_categories c
        ON p.category_id = c.category_id
)
SELECT
    product_id,
    store_name,
    title,
    price,
    brand,
    category,
    DENSE_RANK() OVER (
        PARTITION BY category
        ORDER BY price DESC
    ) AS price_rank_in_category
FROM unified_products;


CREATE OR REPLACE VIEW gold.daily_revenue AS
SELECT
    DATE(order_date) AS order_day,
    store_name,
    COUNT(*) AS orders_count,
    SUM(quantity) AS items_sold,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
FROM bronze.orders
GROUP BY DATE(order_date), store_name
ORDER BY order_day, store_name;


CREATE OR REPLACE VIEW gold.revenue_by_store AS
SELECT
    store_name,
    COUNT(*) AS orders_count,
    SUM(quantity) AS items_sold,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
FROM bronze.orders
GROUP BY store_name
ORDER BY total_revenue DESC;


CREATE OR REPLACE VIEW gold.top_products_by_revenue AS
SELECT
    store_name,
    product_id,
    COUNT(*) AS orders_count,
    SUM(quantity) AS items_sold,
    SUM(total_amount) AS total_revenue,
    RANK() OVER (
        PARTITION BY store_name
        ORDER BY SUM(total_amount) DESC
    ) AS revenue_rank_in_store
FROM bronze.orders
GROUP BY store_name, product_id;


CREATE OR REPLACE VIEW gold.customer_ltv AS
SELECT
    user_id,
    COUNT(*) AS orders_count,
    SUM(total_amount) AS lifetime_value,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date
FROM bronze.orders
GROUP BY user_id
ORDER BY lifetime_value DESC;


CREATE OR REPLACE VIEW gold.repeat_customers AS
SELECT
    COUNT(*) FILTER (WHERE orders_count > 1) AS repeat_customers,
    COUNT(*) AS total_customers,
    ROUND(
        COUNT(*) FILTER (WHERE orders_count > 1)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS repeat_customer_rate_pct
FROM (
    SELECT
        user_id,
        COUNT(*) AS orders_count
    FROM bronze.orders
    GROUP BY user_id
) customer_orders;