from datetime import datetime, timedelta
import pandas as pd

def calculate_time_window_features(df, days):
    """Calculate features for a specific time window"""
    cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days)
    
    # Make sure column names match what Snowflake returns (uppercase)
    window_df = df[df['TRANSACTION_DATE'] >= cutoff_date]
    
    features = window_df.groupby('CUSTOMER_ID').agg({
        'AMOUNT': ['sum', 'mean', 'count'],
        'PRODUCT_CATEGORY': lambda x: x.nunique(),
        'PAYMENT_METHOD': lambda x: x.nunique()
    }).reset_index()
    
    # Flatten the multi-level column names
    features.columns = ['_'.join(col).strip('_') for col in features.columns.values]
    
    # Rename columns to match table schema - use uppercase for Snowflake
    column_mapping = {
        'CUSTOMER_ID': 'CUSTOMER_ID',
        'AMOUNT_sum': f'TOTAL_AMOUNT_{days}D',
        'AMOUNT_mean': f'AVG_AMOUNT_{days}D',
        'AMOUNT_count': f'TRANSACTION_COUNT_{days}D',
        'PRODUCT_CATEGORY_<lambda_0>': f'UNIQUE_CATEGORIES_{days}D',
        'PAYMENT_METHOD_<lambda_0>': f'UNIQUE_PAYMENT_METHODS_{days}D'
    }
    
    features = features.rename(columns=column_mapping)
    return features

def calculate_all_features(df):
    """Calculate features for different time windows"""
    features_7d = calculate_time_window_features(df, 7)
    features_30d = calculate_time_window_features(df, 30)
    features_90d = calculate_time_window_features(df, 90)
    
    # Merge all features
    all_features = features_7d.merge(features_30d, on='CUSTOMER_ID', how='left')
    all_features = all_features.merge(features_90d, on='CUSTOMER_ID', how='left')
    
    # Ensure all expected columns exist
    return all_features