import pandas as pd
import os
from weather_service import fetch_nyc_weather
from modeling import prepare_cyclical_features, train_surge_model

# Paths to processed data (created by retrieval.py)
PROCESSED_DIR = './processed_data/'

# Features and target (must match retrieval.py output)
FEATURES = ['SupplyElasticity', 'Lag_DER_t-15', 'Lag_DER_t-30', 
            'DemandVelocity_t', 'Lag_DemandVelocity_t-15']
TARGET = 'Target_DER_t+15'

if __name__ == "__main__":
    # 1. Load Pre-processed Taxi Data (run retrieval.py first!)
    print("=" * 60)
    print("Loading processed taxi data...")
    print("=" * 60)
    
    train_path = os.path.join(PROCESSED_DIR, 'train_data.parquet')
    test_path = os.path.join(PROCESSED_DIR, 'test_data.parquet')
    
    if not os.path.exists(train_path):
        print("ERROR: Processed data not found!")
        print("Please run 'python retrieval.py' first to process the taxi data.")
        exit(1)
    
    df_train = pd.read_parquet(train_path)
    df_test = pd.read_parquet(test_path)
    print(f"Train: {df_train.shape}, Test: {df_test.shape}")

    # 2. Get Weather Data (dates match your 2025 taxi data)
    print("\nFetching Weather Data...")
    df_weather = fetch_nyc_weather("2025-01-01", "2025-03-31")
    print(f"Weather data: {df_weather.shape}")

    # 3. Merge Weather with Taxi Data
    print("\nMerging datasets...")
    df_train = pd.merge(df_train, df_weather, on='Time_Bin', how='left')
    df_test = pd.merge(df_test, df_weather, on='Time_Bin', how='left')

    # 4. Feature Engineering (Cyclical Time Features)
    print("Adding cyclical time features...")
    df_train = prepare_cyclical_features(df_train)
    df_test = prepare_cyclical_features(df_test)
    
    # Update features list to include weather and cyclical features
    all_features = FEATURES + ['temp', 'precip', 'hour_sin', 'hour_cos']
    
    # Handle any NaN values from weather merge
    df_train = df_train.dropna(subset=all_features + [TARGET])
    df_test = df_test.dropna(subset=all_features + [TARGET])

    # 5. Prepare X and y
    X_train = df_train[all_features]
    y_train = df_train[TARGET]
    X_test = df_test[all_features]
    y_test = df_test[TARGET]
    
    print(f"\nFinal shapes:")
    print(f"  X_train: {X_train.shape}, y_train: {y_train.shape}")
    print(f"  X_test: {X_test.shape}, y_test: {y_test.shape}")

    # 6. Train Model
    print("\n" + "=" * 60)
    print("Training XGBoost Model...")
    print("=" * 60)
    model = train_surge_model(X_train, y_train, X_test, y_test)
    
    print("\n✓ Pipeline complete!")