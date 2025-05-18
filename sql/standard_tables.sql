-- Drop existing tables if they exist
DROP TABLE IF EXISTS FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.CUSTOMER_FEATURES;
DROP TABLE IF EXISTS FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.CUSTOMER_TRANSACTIONS;

-- Create customer transactions table
CREATE TABLE FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.CUSTOMER_TRANSACTIONS (
    customer_id VARCHAR,
    transaction_date DATE,
    amount FLOAT,
    product_category VARCHAR,
    payment_method VARCHAR,
    region VARCHAR
);

-- Insert sample data
INSERT INTO FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.CUSTOMER_TRANSACTIONS
VALUES
    ('C001', '2025-05-16', 100.50, 'Electronics', 'Credit Card', 'North'),
    ('C002', '2025-05-16', 200.75, 'Clothing', 'Debit Card', 'South'),
    ('C003', '2025-05-16', 150.25, 'Food', 'Cash', 'East'),
    ('C001', '2025-05-15', 75.00, 'Electronics', 'Credit Card', 'North'),
    ('C002', '2025-05-15', 125.50, 'Clothing', 'Debit Card', 'South'),
    ('C003', '2025-05-15', 80.00, 'Food', 'Cash', 'East');

-- Create customer features table
CREATE TABLE FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.CUSTOMER_FEATURES (
    customer_id VARCHAR,
    feature_set_id VARCHAR,
    total_amount_7d FLOAT,
    avg_amount_7d FLOAT,
    transaction_count_7d INTEGER,
    unique_categories_7d INTEGER,
    unique_payment_methods_7d INTEGER,
    total_amount_30d FLOAT,
    avg_amount_30d FLOAT,
    transaction_count_30d INTEGER,
    unique_categories_30d INTEGER,
    unique_payment_methods_30d INTEGER,
    total_amount_90d FLOAT,
    avg_amount_90d FLOAT,
    transaction_count_90d INTEGER,
    unique_categories_90d INTEGER,
    unique_payment_methods_90d INTEGER,
    last_update_timestamp TIMESTAMP,
    PRIMARY KEY (customer_id, feature_set_id)
);

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.CUSTOMER_FEATURES TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.CUSTOMER_TRANSACTIONS TO ROLE ACCOUNTADMIN; 