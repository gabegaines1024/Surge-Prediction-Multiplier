import xgboost as xgb
import numpy as np
from sklearn.metrics import mean_absolute_error

def prepare_cyclical_features(df):
    # Convert Hour to Sine/Cosine so 23:00 is close to 00:00
    df['hour'] = df['Time_Bin'].dt.hour
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    return df

def train_surge_model(X_train, y_train, X_test, y_test):
    # Initialize XGBoost Regressor
    model = xgb.XGBRegressor(
        n_estimators=100,
        learning_rate=0.1,
        max_depth=6,
        objective='reg:squarederror',
        random_state=42
    )
    
    print(f"Training on {len(X_train):,} samples...")
    model.fit(X_train, y_train)
    
    # Predict and Evaluate
    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    
    print(f"✓ Model Training Complete!")
    print(f"  MAE: {mae:.4f}")
    print(f"  Features: {list(X_train.columns)}")
    
    return model