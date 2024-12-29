CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    store_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    transaction_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics (
    store_id INT NOT NULL,
    total_sales INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
