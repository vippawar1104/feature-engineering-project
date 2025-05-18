import snowflake.connector
import json
import pandas as pd
from datetime import datetime

def get_snowflake_config():
    with open("config/snowflake.json") as f:
        return json.load(f)

def store_features(features_df):
    """Store features in Snowflake Feature Store"""
    config = get_snowflake_config()
    config["schema"] = "FEATURE_ENGINEERING"  # Override schema
    
    # Create a new DataFrame with only the columns that exist in the table
    output_df = pd.DataFrame()
    
    # Map our calculated features to the exact columns in Snowflake
    output_df['CUSTOMER_ID'] = features_df['CUSTOMER_ID']
    output_df['FEATURE_SET_ID'] = features_df['FEATURE_SET_ID']
    
    # Map the available calculated metrics to the Snowflake columns
    output_df['AVG_TRANSACTION_AMOUNT_7D'] = features_df['AVG_AMOUNT_7D']
    output_df['MAX_TRANSACTION_AMOUNT_30D'] = features_df['TOTAL_AMOUNT_30D']  # Using total as proxy for max
    output_df['TRANSACTION_COUNT_30D'] = features_df['TRANSACTION_COUNT_30D']
    
    # Use string values for categorical fields
    output_df['FAVORITE_CATEGORY'] = 'Multiple'  # Default value
    output_df['PAYMENT_PREFERENCE'] = 'Multiple'  # Default value
    output_df['REGION'] = 'Various'  # Default value
    
    # Set the timestamp - convert to string for Snowflake
    # Format: YYYY-MM-DD HH:MI:SS
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    output_df['LAST_UPDATE_TIMESTAMP'] = current_time
    
    print("Final DataFrame columns:", output_df.columns.tolist())
    print("First few rows:", output_df.head())
    
    conn = snowflake.connector.connect(**config)
    cur = conn.cursor()
    
    # Convert DataFrame to list of tuples
    records = output_df.to_records(index=False).tolist()
    
    cur.executemany("""
    INSERT INTO FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.CUSTOMER_FEATURES (
        CUSTOMER_ID,
        FEATURE_SET_ID,
        AVG_TRANSACTION_AMOUNT_7D,
        MAX_TRANSACTION_AMOUNT_30D,
        TRANSACTION_COUNT_30D,
        FAVORITE_CATEGORY,
        PAYMENT_PREFERENCE,
        REGION,
        LAST_UPDATE_TIMESTAMP
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """, records)
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Successfully stored {len(output_df)} feature records in Snowflake Feature Store")