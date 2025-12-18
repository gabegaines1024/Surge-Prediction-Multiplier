import pandas as pd
from retrieval import run_dask_aggregation
from weather_service import fetch_nyc_weather
from modeling import prepare_cyclical_features, train_surge_model
from sklearn.model_selection import train_test_split

if __name__ == "__main__":
    # 1. Aggregate Taxi Data (Dask)
    print("Aggregating Taxi Data...")
    df_taxi = run_dask_aggregation() 

    # 2. Get Weather Data
    print("Fetching Weather...")
    df_weather = fetch_nyc_weather("2024-01-01", "2024-01-31")

    # 3. Merge
    df_final = pd.merge(df_taxi, df_weather, on='Time_Bin', how='left')

    # 4. Feature Engineering (Lags, Cyclical Time)
    df_final = prepare_cyclical_features(df_final)

    # 5. Split and Train
    X_train, X_test, y_train, y_test = train_test_split(df_final, test_size=0.2, random_state=42)
    model, mae = train_surge_model(X_train, y_train, X_test, y_test)

    print(f"Model Training Complete. MAE: {mae:.4f}")
    print(f"Model: {model.get_params()}")