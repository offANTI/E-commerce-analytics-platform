CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id SERIAL PRIMARY KEY,
    user_id INT,
    product_id INT,
    quantity INT DEFAULT 1,
    store_name VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);
