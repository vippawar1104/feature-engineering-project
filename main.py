from feature_pipeline.extract import extract_raw_data
from feature_pipeline.transform import calculate_all_features
from feature_pipeline.load.snowflake_fs import store_features
from datetime import datetime
import os
import snowflake.connector
import json

def get_snowflake_config():
    """Get Snowflake configuration from config file"""
    config_path = os.path.join("config", "snowflake.json")
    with open(config_path) as f:
        return json.load(f)

def create_feature_metrics_table():
    """Create the feature usage metrics table if it doesn't exist"""
    config = get_snowflake_config()
    conn = snowflake.connector.connect(**config)
    cur = conn.cursor()
    
    try:
        # Create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.FEATURE_USAGE_METRICS (
            FEATURE_NAME VARCHAR(255),
            MODEL_NAME VARCHAR(255),
            USAGE_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            LAST_USED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            USAGE_CONTEXT VARCHAR(255),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cur.execute(create_table_query)
        
        # Create the view
        create_view_query = """
        CREATE OR REPLACE VIEW FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.FEATURE_USAGE_STATS AS
        SELECT 
            FEATURE_NAME,
            COUNT(*) AS usage_count,
            MAX(LAST_USED) AS last_used,
            COUNT(DISTINCT MODEL_NAME) AS models_used_in
        FROM 
            FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.FEATURE_USAGE_METRICS
        GROUP BY 
            FEATURE_NAME
        ORDER BY 
            usage_count DESC
        """
        cur.execute(create_view_query)
        
        conn.commit()
        print("Successfully created feature metrics table and view")
        
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def log_feature_usage(feature_names, model_name, usage_context=None):
    """Log feature usage in Snowflake"""
    config = get_snowflake_config()
    conn = snowflake.connector.connect(**config)
    cur = conn.cursor()
    
    try:
        insert_query = """
        INSERT INTO FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.FEATURE_USAGE_METRICS 
        (FEATURE_NAME, MODEL_NAME, USAGE_CONTEXT)
        VALUES (%s, %s, %s)
        """
        
        for feature_name in feature_names:
            cur.execute(insert_query, (feature_name, model_name, usage_context))
        
        conn.commit()
        print(f"Successfully logged usage of {len(feature_names)} features for model {model_name}")
        
    except Exception as e:
        print(f"Error logging feature usage: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def get_feature_usage_stats():
    """Get feature usage statistics"""
    config = get_snowflake_config()
    conn = snowflake.connector.connect(**config)
    cur = conn.cursor()
    
    try:
        query = """
        SELECT 
            FEATURE_NAME,
            usage_count,
            last_used,
            models_used_in
        FROM 
            FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.FEATURE_USAGE_STATS
        ORDER BY 
            usage_count DESC
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        print("\nFeature Usage Statistics:")
        for row in results:
            print(f"\nFeature: {row[0]}")
            print(f"Usage Count: {row[1]}")
            print(f"Last Used: {row[2]}")
            print(f"Models Used In: {row[3]}")
        
    except Exception as e:
        print(f"Error getting feature usage stats: {e}")
    finally:
        cur.close()
        conn.close()

def run_pipeline():
    print("🚀 Starting feature engineering pipeline...")
    
    # 1. Extract
    print("🔍 Extracting raw data from Snowflake...")
    raw_data = extract_raw_data(days=90)
    
    # 2. Transform
    print("🔄 Calculating features...")
    final_features = calculate_all_features(raw_data)
    
    # Add metadata with uppercase column names for Snowflake
    final_features['FEATURE_SET_ID'] = 'customer_features_v1'
    final_features['LAST_UPDATE_TIMESTAMP'] = datetime.now()
    
    # 3. Load
    print("💾 Storing features in Snowflake Feature Store...")
    store_features(final_features)
    
    print("✅ Pipeline completed successfully!")

def main():
    # Create the feature metrics table
    print("Creating feature metrics table...")
    create_feature_metrics_table()
    
    # Log some example feature usage
    print("\nLogging example feature usage...")
    features = [
        'avg_transaction_amount_7d',
        'max_transaction_amount_30d',
        'transaction_count_30d'
    ]
    log_feature_usage(features, 'customer_churn_model', 'model_training')
    
    # Get and display feature usage statistics
    print("\nRetrieving feature usage statistics...")
    get_feature_usage_stats()

if __name__ == "__main__":
    run_pipeline()