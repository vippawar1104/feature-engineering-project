import snowflake.connector
import pandas as pd
import json

def get_snowflake_config():
    with open("config/snowflake.json") as f:
        return json.load(f)

def extract_raw_data(days=30):
    """Extract raw transaction data from Snowflake"""
    config = get_snowflake_config()
    conn = snowflake.connector.connect(**config)
    
    query = f"""
    SELECT 
        customer_id,
        transaction_date,
        amount,
        product_category,
        payment_method,
        region
    FROM FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.CUSTOMER_TRANSACTIONS
    WHERE transaction_date >= DATEADD(day, -{days}, CURRENT_DATE())
    """
    
    df = pd.read_sql(query, conn)
    print("Columns in the data:", df.columns.tolist())
    print("First few rows:", df.head())
    conn.close()
    return df