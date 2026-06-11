CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    store_name VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.dummy_products (
    id SERIAL PRIMARY KEY,
    raw_data JSONB,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.dummy_categories (
    id SERIAL PRIMARY KEY,
    raw_data JSONB,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.escuela_products (
    id SERIAL PRIMARY KEY,
    raw_data JSONB,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.escuela_categories (
    id SERIAL PRIMARY KEY,
    raw_data JSONB,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.escuela_users (
    id SERIAL PRIMARY KEY,
    raw_data JSONB,
    loaded_at TIMESTAMP DEFAULT NOW()
);