"""
Script phân tích dữ liệu du lịch với weather extended features
Và training LightGBM model để dự đoán traffic
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# ========================================================================
# 1. LOAD DATA
# ========================================================================
print("\n" + "="*70)
print("STEP 1: LOAD NEW DATASET")
print("="*70)

# Base paths (script-relative) so the script works when run from any CWD
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / 'data'
NORMALIZED_DIR = DATA_DIR / 'normalized'
MODELS_DIR = BASE_DIR.parent / 'models'
PREDICTIONS_DIR = DATA_DIR / 'predictions'

df = pd.read_csv(NORMALIZED_DIR / 'merged_tourism_data_extended.csv')
df['date_parsed'] = pd.to_datetime(df['date_parsed'])

print("Shape: {}".format(df.shape))
print("Date range: {} → {}".format(df['date_parsed'].min(), df['date_parsed'].max()))
print("Destinations: {}".format(df['destination'].nunique()))
print("Provinces: {}".format(df['province'].nunique()))

# ========================================================================
# 2. BASIC STATISTICS
# ========================================================================
print("\n" + "="*70)
print("STEP 2: BASIC STATISTICS")
print("="*70)

# Weather stats
print("\nWEATHER EXTENDED FEATURES:")
weather_cols = ['temp_mean', 'temp_min', 'temp_max', 'temp_amplitude', 'temp_std',
                'rainfall_total', 'rainfall_max_daily', 'rainfall_days']
for col in weather_cols:
    if col in df.columns:
        print("   {}: mean={:.2f}, min={:.2f}, max={:.2f}".format(col, df[col].mean(), df[col].min(), df[col].max()))

# Traffic stats
print("\nTRAFFIC STATISTICS:")
print("   Mean traffic: {:.2f}".format(df['traffic'].mean()))
print("   Median traffic: {:.2f}".format(df['traffic'].median()))
print("   Max traffic: {:.0f}".format(df['traffic'].max()))
print("   Zero traffic %: {:.1f}%".format((df['traffic']==0).mean()*100))

# ========================================================================
# 3. FEATURE ENGINEERING
# ========================================================================
print("\n" + "="*70)
print("STEP 3: FEATURE ENGINEERING")
print("="*70)

# Sort by destination and date
df = df.sort_values(['destination', 'date_parsed'])

# Lag features
print("Creating lag features...")
for lag in [1, 2, 3, 6, 12]:
    df['traffic_lag_{}m'.format(lag)] = df.groupby('destination')['traffic'].shift(lag)
    print("   traffic_lag_{}m done".format(lag))

# Rolling features
print("\nCreating rolling features...")
for window in [3, 6, 12]:
    df['traffic_rolling_mean_{}m'.format(window)] = df.groupby('destination')['traffic'].transform(
        lambda x: x.shift(1).rolling(window, min_periods=1).mean()
    )
    df['traffic_rolling_std_{}m'.format(window)] = df.groupby('destination')['traffic'].transform(
        lambda x: x.shift(1).rolling(window, min_periods=1).std()
    )
    print("   {}m rolling done".format(window))

# YoY change
print("\nCreating YoY features...")
df['traffic_yoy_change'] = df.groupby('destination')['traffic'].transform(
    lambda x: x.pct_change(12)
)
df['traffic_yoy_change'] = df['traffic_yoy_change'].replace([np.inf, -np.inf], np.nan)

# Seasonal encoding
print("Creating seasonal encoding...")
df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)

# Is peak month
df['is_peak_month'] = df.apply(
    lambda row: str(row['month']) in str(row.get('Peak_Months_List', '')) if pd.notna(row.get('Peak_Months_List')) else False,
    axis=1
).astype(int)

# Weather comfort score (inverse of extreme temps)
df['weather_comfort'] = 100 - abs(df['temp_mean'] - 25)  # 25°C is ideal

# Rain intensity
df['rainfall_intensity'] = np.where(
    df['rainfall_days'] > 0,
    df['rainfall_total'] / df['rainfall_days'],
    0
)

print("\nFinal features: {} columns".format(df.shape[1]))
print("Rows with complete lag features: {}".format(df['traffic_lag_12m'].notna().sum()))

# ========================================================================
# 4. CORRELATION ANALYSIS
# ========================================================================
print("\n" + "="*70)
print("STEP 4: CORRELATION ANALYSIS")
print("="*70)

# Numeric columns for correlation
numeric_cols = ['traffic', 'temp_mean', 'temp_min', 'temp_max', 'temp_amplitude', 'temp_std',
                'rainfall_total', 'rainfall_days', 'weather_comfort', 'rainfall_intensity',
                'traffic_lag_1m', 'traffic_lag_3m', 'traffic_rolling_mean_3m']

# Filter to rows with data
df_corr = df[numeric_cols].dropna()
print("Rows for correlation: {}".format(len(df_corr)))

# Correlation with traffic
corr_with_traffic = df_corr.corr()['traffic'].sort_values(ascending=False)
print("\nCorrelation with traffic:")
for col, corr in corr_with_traffic.items():
    if col != 'traffic':
        print("   {}: {:.3f}".format(col, corr))

# ========================================================================
# 5. PREPARE DATA FOR MODELING
# ========================================================================
print("\n" + "="*70)
print("STEP 5: PREPARE DATA FOR MODELING")
print("="*70)

# Feature columns
feature_cols = [
    # Lag features
    'traffic_lag_1m', 'traffic_lag_2m', 'traffic_lag_3m', 'traffic_lag_6m', 'traffic_lag_12m',
    # Rolling features
    'traffic_rolling_mean_3m', 'traffic_rolling_mean_6m', 'traffic_rolling_mean_12m',
    'traffic_rolling_std_3m', 'traffic_rolling_std_6m',
    # YoY
    'traffic_yoy_change',
    # Time features
    'month', 'quarter', 'year',
    'month_sin', 'month_cos',
    # Weather features
    'temp_mean', 'temp_min', 'temp_max', 'temp_amplitude', 'temp_std',
    'rainfall_total', 'rainfall_max_daily', 'rainfall_days',
    'weather_comfort', 'rainfall_intensity',
    # Geo features
    'latitude', 'longitude',
    'distance_to_hanoi_km', 'distance_to_hcm_km',
    # Seasonal patterns
    'seasonal_amplitude', 'is_peak_month', 'Num_Strong_Months',
    # Destination stats
    'dest_mean_traffic', 'dest_max_traffic', 'dest_std_traffic',
    # Economic
    'population_thousand', 'density', 'grdp',
    # Social
    'youtube_views', 'youtube_likes'
]

# Filter to existing columns
feature_cols = [c for c in feature_cols if c in df.columns]
print("Using {} features".format(len(feature_cols)))

# Target
target_col = 'traffic'

# Filter rows with complete features
df_model = df.dropna(subset=['traffic_lag_12m'])  # Need 12 months history
print("Rows for modeling: {}".format(len(df_model)))

# Time-based split (last 6 months for test)
split_date = df_model['date_parsed'].max() - pd.DateOffset(months=6)
train_df = df_model[df_model['date_parsed'] <= split_date]
test_df = df_model[df_model['date_parsed'] > split_date]

print("Train: {} rows ({} to {})".format(len(train_df), train_df['date_parsed'].min(), train_df['date_parsed'].max()))
print("Test: {} rows ({} to {})".format(len(test_df), test_df['date_parsed'].min(), test_df['date_parsed'].max()))

# ========================================================================
# 6. TRAIN LIGHTGBM MODEL
# ========================================================================
print("\n" + "="*70)
print("STEP 6: TRAINING LIGHTGBM MODEL")
print("="*70)

try:
    import lightgbm as lgb
    
    # Prepare data
    X_train = train_df[feature_cols].copy()
    y_train = train_df[target_col].copy()
    X_test = test_df[feature_cols].copy()
    y_test = test_df[target_col].copy()
    
    # Convert object columns to numeric
    for col in X_train.columns:
        if X_train[col].dtype == 'object':
            X_train[col] = X_train[col].astype(str).str.replace(',', '').str.replace('.', '', regex=False)
            X_train[col] = pd.to_numeric(X_train[col], errors='coerce')
            X_test[col] = X_test[col].astype(str).str.replace(',', '').str.replace('.', '', regex=False)
            X_test[col] = pd.to_numeric(X_test[col], errors='coerce')
    
    # Fill NaN
    X_train = X_train.fillna(0)
    X_test = X_test.fillna(0)
    
    print("X_train shape: {}".format(X_train.shape))
    print("X_test shape: {}".format(X_test.shape))
    
    # Create LightGBM datasets
    train_data = lgb.Dataset(X_train, label=y_train)
    test_data = lgb.Dataset(X_test, label=y_test, reference=train_data)
    
    # Parameters
    params = {
        'objective': 'regression',
        'metric': ['rmse', 'mae'],
        'boosting_type': 'gbdt',
        'num_leaves': 63,
        'learning_rate': 0.05,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'verbose': 0,
        'seed': 42
    }
    
    # Train
    print("\nTraining...")
    model = lgb.train(
        params,
        train_data,
        num_boost_round=500,
        valid_sets=[train_data, test_data],
        valid_names=['train', 'test'],
        callbacks=[
            lgb.early_stopping(stopping_rounds=50),
            lgb.log_evaluation(100)
        ]
    )
    
    # Predictions
    y_pred = model.predict(X_test)
    
    # Metrics
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
    
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print("\nMODEL PERFORMANCE:")
    print("   RMSE: {:.2f}".format(rmse))
    print("   MAE: {:.2f}".format(mae))
    print("   R2: {:.4f}".format(r2))
    
    # Feature importance
    print("\nTOP 20 IMPORTANT FEATURES:")
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance()
    }).sort_values('importance', ascending=False)
    
    for i, row in importance.head(20).iterrows():
        print("   {}: {}".format(row['feature'], row['importance']))
    
    # Save model
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODELS_DIR / 'traffic_prediction_extended.lgb'
    model.save_model(str(model_path))
    print("\nModel saved to {}".format(model_path))
    
except ImportError:
    print("LightGBM not installed. Install with: pip install lightgbm")

# ========================================================================
# 7. PREDICT NEXT MONTH
# ========================================================================
print("\n" + "="*70)
print("STEP 7: PREDICT NEXT MONTH")
print("="*70)

try:
    # Get latest data for each destination
    latest_date = df_model['date_parsed'].max()
    latest_data = df_model[df_model['date_parsed'] == latest_date].copy()
    
    print("Latest date: {}".format(latest_date))
    print("Destinations with data: {}".format(len(latest_data)))
    
    # Prepare features
    X_predict = latest_data[feature_cols].fillna(0)
    
    # Predict
    predictions = model.predict(X_predict)
    
    # Create results
    results = latest_data[['destination', 'province', 'region', 'traffic']].copy()
    results['predicted_traffic'] = predictions
    results['predicted_change'] = (predictions - results['traffic']) / (results['traffic'] + 1) * 100
    
    # Top rising destinations
    print("\nTOP 30 HIGHEST PREDICTED DESTINATIONS:")
    top_predicted = results.nlargest(30, 'predicted_traffic')
    for i, row in top_predicted.iterrows():
        print("   {} ({}): {:.0f} (current: {:.0f})".format(row['destination'], row['province'], row['predicted_traffic'], row['traffic']))
    
    # Top trending (biggest increase)
    print("\nTOP 20 FASTEST GROWING DESTINATIONS:")
    top_trending = results[results['traffic'] > 10].nlargest(20, 'predicted_change')
    for i, row in top_trending.iterrows():
        print("   {}: +{:.1f}% ({:.0f} to {:.0f})".format(row['destination'], row['predicted_change'], row['traffic'], row['predicted_traffic']))
    
    # Group by province
    print("\nTOP 10 HOTTEST PROVINCES:")
    province_traffic = results.groupby('province')['predicted_traffic'].sum().sort_values(ascending=False)
    for province, traffic in province_traffic.head(10).items():
        print("   {}: {:.0f}".format(province, traffic))
    
    # Save predictions
    PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
    pred_path = PREDICTIONS_DIR / 'traffic_predictions_extended.csv'
    results.to_csv(pred_path, index=False)
    print("\nPredictions saved to {}".format(pred_path))
    
except Exception as e:
    print("Error in prediction: {}".format(e))

print("\n" + "="*70)
print("ANALYSIS COMPLETE!")
print("="*70)
