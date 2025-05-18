import snowflake.connector
import pandas as pd
import json
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib
from sqlalchemy import create_engine
from urllib.parse import quote_plus

def get_snowflake_config():
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up one level to the project root
    project_root = os.path.dirname(script_dir)
    # Construct the path to the config file
    config_path = os.path.join(project_root, "config", "snowflake.json")
    with open(config_path) as f:
        return json.load(f)

def retrieve_features():
    """Retrieve features from Snowflake Feature Store"""
    config = get_snowflake_config()
    
    # Create SQLAlchemy engine
    conn_str = f"snowflake://{config['user']}:{quote_plus(config['password'])}@{config['account']}/{config['database']}/{config['schema']}"
    engine = create_engine(conn_str)
    
    # First, let's check the table structure
    describe_query = """
    DESCRIBE TABLE FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.CUSTOMER_FEATURES
    """
    
    print("Checking table structure...")
    df_structure = pd.read_sql(describe_query, engine)
    print("\nTable Structure:")
    print(df_structure)
    
    # Now use the correct column names from the table
    query = """
    SELECT 
        CUSTOMER_ID,
        FEATURE_SET_ID,
        AVG_TRANSACTION_AMOUNT_7D,
        MAX_TRANSACTION_AMOUNT_30D,
        TRANSACTION_COUNT_30D,
        FAVORITE_CATEGORY,
        PAYMENT_PREFERENCE,
        REGION,
        LAST_UPDATE_TIMESTAMP
    FROM FEATURE_ENGINEERING_DB.FEATURE_ENGINEERING.CUSTOMER_FEATURES
    WHERE LAST_UPDATE_TIMESTAMP >= DATEADD(day, -7, CURRENT_DATE())
    """
    
    df = pd.read_sql(query, engine)
    print("\nColumns returned from Snowflake:", df.columns.tolist())
    print("First few rows:", df.head())
    print("DataFrame shape:", df.shape)
    
    # For demonstration purposes, we'll create a synthetic target variable
    # In a real scenario, this would come from your business data
    if 'transaction_count_30d' in df.columns:
        df['target'] = (df['transaction_count_30d'] > df['transaction_count_30d'].median()).astype(int)
    else:
        print("transaction_count_30d column not found! Columns are:", df.columns.tolist())
        df['target'] = 0  # fallback to dummy target
    
    return df

def prepare_features(df):
    """Prepare features for model training"""
    # Drop columns not needed for training
    columns_to_drop = ['customer_id', 'feature_set_id', 'last_update_timestamp']
    X = df.drop(columns_to_drop + ['target'], axis=1)
    y = df['target']
    
    # Handle categorical variables
    categorical_columns = ['favorite_category', 'payment_preference', 'region']
    X = pd.get_dummies(X, columns=categorical_columns, drop_first=True)
    
    print("\nFeatures after encoding:")
    print("Feature names:", X.columns.tolist())
    print("Feature matrix shape:", X.shape)
    
    return X, y

def train_model():
    """Train a model using features from the Feature Store"""
    print("🔍 Retrieving features from Snowflake Feature Store...")
    df = retrieve_features()
    
    print("🔄 Preparing features for training...")
    X, y = prepare_features(df)
    
    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    print("🤖 Training Random Forest model...")
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Evaluate the model
    y_pred = model.predict(X_test)
    print("\nModel Performance:")
    print(classification_report(y_test, y_pred))
    
    # Save the model
    print("💾 Saving model...")
    model_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(model_dir, 'customer_churn_model.joblib')
    joblib.dump(model, model_path)
    
    # Save feature names for future reference
    feature_names = X.columns.tolist()
    feature_names_path = os.path.join(model_dir, 'feature_names.json')
    with open(feature_names_path, 'w') as f:
        json.dump(feature_names, f)
    
    print("✅ Model training completed!")

if __name__ == "__main__":
    train_model() 