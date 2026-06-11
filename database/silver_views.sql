CREATE SCHEMA IF NOT EXISTS silver;
DROP VIEW IF EXISTS silver.escuela_products CASCADE;
DROP VIEW IF EXISTS silver.dummy_products CASCADE;
DROP VIEW IF EXISTS silver.escuela_categories CASCADE;
DROP VIEW IF EXISTS silver.dummy_categories CASCADE;
DROP VIEW IF EXISTS silver.escuela_users CASCADE;
-- ==========================================
-- ESCUELA PRODUCTS
-- ==========================================

CREATE OR REPLACE VIEW silver.escuela_products AS
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
FROM bronze.escuela_products,
LATERAL jsonb_array_elements(raw_data) AS item;


-- ==========================================
-- DUMMY PRODUCTS
-- ==========================================

CREATE OR REPLACE VIEW silver.dummy_products AS
SELECT
    (item->>'id')::INT AS product_id,
    (item->>'title')::VARCHAR(255) AS title,
    (item->>'price')::NUMERIC(10,2) AS price,
    (item->>'discountPercentage')::NUMERIC(5,2) AS discount_percentage,
    (item->>'rating')::NUMERIC(3,2) AS rating,
    (item->>'brand')::VARCHAR(100) AS brand,
    (item->>'category')::VARCHAR(100) AS category_name
FROM bronze.dummy_products,
LATERAL jsonb_array_elements(raw_data->'products') AS item;


-- ==========================================
-- ESCUELA CATEGORIES
-- ==========================================

CREATE OR REPLACE VIEW silver.escuela_categories AS
SELECT
    (item->>'id')::INT AS category_id,
    (item->>'name')::VARCHAR(255) AS category_name,
    (item->>'image')::TEXT AS image_url
FROM bronze.escuela_categories,
LATERAL jsonb_array_elements(raw_data) AS item;


-- ==========================================
-- DUMMY CATEGORIES
-- ==========================================

CREATE OR REPLACE VIEW silver.dummy_categories AS
SELECT
    (item->>'slug')::VARCHAR(255) AS category_slug,
    (item->>'name')::VARCHAR(255) AS category_name
FROM bronze.dummy_categories,
LATERAL jsonb_array_elements(raw_data) AS item;


-- ==========================================
-- ESCUELA USERS
-- ==========================================

CREATE OR REPLACE VIEW silver.escuela_users AS
SELECT
    (item->>'id')::INT AS user_id,
    (item->>'email')::VARCHAR(255) AS email,
    (item->>'name')::VARCHAR(255) AS name,
    (item->>'role')::VARCHAR(50) AS role,
    (item->>'avatar')::TEXT AS avatar_url
FROM bronze.escuela_users,
LATERAL jsonb_array_elements(raw_data) AS item;