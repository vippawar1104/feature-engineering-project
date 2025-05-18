-- Use the SNOWFLAKE database
USE DATABASE SNOWFLAKE;

-- Create our schema
CREATE SCHEMA IF NOT EXISTS FEATURE_ENGINEERING;

-- Create tables in our schema
CREATE OR REPLACE TABLE SNOWFLAKE.FEATURE_ENGINEERING.CUSTOMER_TRANSACTIONS (
    transaction_id STRING,
    customer_id STRING,
    transaction_date TIMESTAMP_NTZ,
    amount FLOAT,
    product_category STRING,
    payment_method STRING,
    region STRING
);

CREATE OR REPLACE TABLE SNOWFLAKE.FEATURE_ENGINEERING.CUSTOMER_FEATURES (
    customer_id STRING,
    feature_set_id STRING,
    avg_transaction_amount_7d FLOAT,
    max_transaction_amount_30d FLOAT,
    transaction_count_30d INTEGER,
    favorite_category STRING,
    payment_preference STRING,
    region STRING,
    last_update_timestamp TIMESTAMP_NTZ
);

-- Insert sample data
INSERT INTO SNOWFLAKE.FEATURE_ENGINEERING.CUSTOMER_TRANSACTIONS VALUES
    ('T001', 'C001', CURRENT_DATE(), 100.50, 'Electronics', 'Credit Card', 'North'),
    ('T002', 'C002', CURRENT_DATE(), 200.75, 'Clothing', 'Debit Card', 'South'),
    ('T003', 'C003', CURRENT_DATE(), 150.25, 'Food', 'Cash', 'East');

-- Verify data
SELECT * FROM SNOWFLAKE.FEATURE_ENGINEERING.CUSTOMER_TRANSACTIONS;