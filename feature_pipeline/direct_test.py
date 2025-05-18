import snowflake.connector
import json
from datetime import datetime
import os

def get_snowflake_config():
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up one level to the project root
    project_root = os.path.dirname(script_dir)
    # Construct the path to the config file
    config_path = os.path.join(project_root, "config", "snowflake.json")
    with open(config_path) as f:
        return json.load(f)

def run_test():
    """Test direct connection and insert to Snowflake"""
    print("Testing direct Snowflake connection...")
    
    config = get_snowflake_config()
    conn = snowflake.connector.connect(**config)
    cur = conn.cursor()
    
    try:
        # Check if table exists
        print("Checking if CUSTOMER_FEATURES table exists...")
        cur.execute("SHOW TABLES LIKE 'CUSTOMER_FEATURES' IN FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING")
        tables = cur.fetchall()
        if not tables:
            print("Table CUSTOMER_FEATURES does not exist!")
            return
        
        # Get column names from the table
        print("Getting column names from CUSTOMER_FEATURES table...")
        cur.execute("DESCRIBE TABLE FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.CUSTOMER_FEATURES")
        columns = cur.fetchall()
        print("Columns in the table:")
        for col in columns:
            print(f"  - {col[0]} ({col[1]})")
        
        # Print sample insert statement
        column_names = [col[0] for col in columns]
        print(f"\nColumn names for INSERT: {', '.join(column_names)}")
    
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run_test() 