"""
Script phân tích dữ liệu du lịch - FILTERED VERSION
Loại bỏ các địa điểm có traffic cực thấp để kiểm tra R² thực tế
"""

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ========================================================================
# 1. LOAD DATA & ANALYZE TRAFFIC DISTRIBUTION
# ========================================================================
print("=" * 70)
print("STEP 1: LOAD DATA & ANALYZE TRAFFIC DISTRIBUTION")
print("=" * 70)

df = pd.read_csv('../data/normalized/merged_tourism_data_extended.csv', low_memory=False)
df['date_parsed'] = pd.to_datetime(df['date_parsed'])

print("Original data shape: {}".format(df.shape))
print("Total destinations: {}".format(df['destination'].nunique()))

# Analyze traffic distribution
print("\n--- TRAFFIC DISTRIBUTION ANALYSIS ---")
print("Overall traffic statistics:")
print("   Mean: {:.2f}".format(df['traffic'].mean()))
print("   Median: {:.2f}".format(df['traffic'].median()))
print("   Max: {:.0f}".format(df['traffic'].max()))
print("   Zero traffic rows: {:.1f}%".format((df['traffic'] == 0).mean() * 100))
print("   Traffic <= 1 rows: {:.1f}%".format((df['traffic'] <= 1).mean() * 100))
print("   Traffic <= 5 rows: {:.1f}%".format((df['traffic'] <= 5).mean() * 100))

# ========================================================================
# 2. ANALYZE DESTINATIONS BY AVERAGE TRAFFIC
# ========================================================================
print("\n" + "=" * 70)
print("STEP 2: ANALYZE DESTINATIONS BY AVERAGE TRAFFIC")
print("=" * 70)

# Calculate average traffic per destination
dest_avg_traffic = df.groupby('destination')['traffic'].mean().reset_index()
dest_avg_traffic.columns = ['destination', 'avg_traffic']

print("\nDestination average traffic distribution:")
print("   Destinations with avg_traffic = 0: {}".format((dest_avg_traffic['avg_traffic'] == 0).sum()))
print("   Destinations with avg_traffic <= 1: {}".format((dest_avg_traffic['avg_traffic'] <= 1).sum()))
print("   Destinations with avg_traffic <= 5: {}".format((dest_avg_traffic['avg_traffic'] <= 5).sum()))
print("   Destinations with avg_traffic <= 10: {}".format((dest_avg_traffic['avg_traffic'] <= 10).sum()))
print("   Destinations with avg_traffic > 10: {}".format((dest_avg_traffic['avg_traffic'] > 10).sum()))
print("   Total destinations: {}".format(len(dest_avg_traffic)))

# Percentiles
print("\nPercentiles of destination average traffic:")
for p in [10, 25, 50, 75, 90, 95]:
    val = dest_avg_traffic['avg_traffic'].quantile(p / 100)
    print("   {}th percentile: {:.2f}".format(p, val))

# ========================================================================
# 3. FILTER LOW-TRAFFIC DESTINATIONS
# ========================================================================
print("\n" + "=" * 70)
print("STEP 3: FILTER LOW-TRAFFIC DESTINATIONS")
print("=" * 70)

# Use 80th percentile as threshold (removes ~80% of lowest-traffic destinations)
threshold = dest_avg_traffic['avg_traffic'].quantile(0.8)
print("Filter threshold (80th percentile): avg_traffic > {:.2f}".format(threshold))

# Get high-traffic destinations
high_traffic_dests = dest_avg_traffic[dest_avg_traffic['avg_traffic'] > threshold]['destination'].tolist()
print("Destinations kept: {} / {} ({:.1f}%)".format(
    len(high_traffic_dests), 
    len(dest_avg_traffic),
    len(high_traffic_dests) / len(dest_avg_traffic) * 100
))

# Filter dataframe
df_filtered = df[df['destination'].isin(high_traffic_dests)].copy()
print("Filtered data shape: {}".format(df_filtered.shape))
print("Rows removed: {:.1f}%".format((1 - len(df_filtered) / len(df)) * 100))

# New traffic stats
print("\nFiltered traffic statistics:")
print("   Mean: {:.2f}".format(df_filtered['traffic'].mean()))
print("   Median: {:.2f}".format(df_filtered['traffic'].median()))
print("   Max: {:.0f}".format(df_filtered['traffic'].max()))
print("   Zero traffic rows: {:.1f}%".format((df_filtered['traffic'] == 0).mean() * 100))

# ========================================================================
# 4. FEATURE ENGINEERING (on filtered data)
# ========================================================================
print("\n" + "=" * 70)
print("STEP 4: FEATURE ENGINEERING")
print("=" * 70)

# Sort by destination and date
df_filtered = df_filtered.sort_values(['destination', 'date_parsed'])

# Lag features
print("Creating lag features...")
for lag in [1, 2, 3, 6, 12]:
    df_filtered['traffic_lag_{}m'.format(lag)] = df_filtered.groupby('destination')['traffic'].shift(lag)

# Rolling features
print("Creating rolling features...")
for window in [3, 6, 12]:
    df_filtered['traffic_rolling_mean_{}m'.format(window)] = df_filtered.groupby('destination')['traffic'].transform(
        lambda x: x.shift(1).rolling(window, min_periods=1).mean()
    )
    df_filtered['traffic_rolling_std_{}m'.format(window)] = df_filtered.groupby('destination')['traffic'].transform(
        lambda x: x.shift(1).rolling(window, min_periods=1).std()
    )

# YoY change
print("Creating YoY features...")
df_filtered['traffic_yoy_change'] = df_filtered.groupby('destination')['traffic'].transform(
    lambda x: x.pct_change(12)
)
df_filtered['traffic_yoy_change'] = df_filtered['traffic_yoy_change'].replace([np.inf, -np.inf], np.nan)

# Seasonal encoding
df_filtered['month_sin'] = np.sin(2 * np.pi * df_filtered['month'] / 12)
df_filtered['month_cos'] = np.cos(2 * np.pi * df_filtered['month'] / 12)

# Weather comfort score
df_filtered['weather_comfort'] = 100 - abs(df_filtered['temp_mean'] - 25)

# Rain intensity
df_filtered['rainfall_intensity'] = np.where(
    df_filtered['rainfall_days'] > 0,
    df_filtered['rainfall_total'] / df_filtered['rainfall_days'],
    0
)

print("Final features: {} columns".format(df_filtered.shape[1]))

# ========================================================================
# 5. PREPARE DATA FOR MODELING
# ========================================================================
print("\n" + "=" * 70)
print("STEP 5: PREPARE DATA FOR MODELING")
print("=" * 70)

# Feature columns
feature_cols = [
    'traffic_lag_1m', 'traffic_lag_2m', 'traffic_lag_3m', 'traffic_lag_6m', 'traffic_lag_12m',
    'traffic_rolling_mean_3m', 'traffic_rolling_mean_6m', 'traffic_rolling_mean_12m',
    'traffic_rolling_std_3m', 'traffic_rolling_std_6m',
    'traffic_yoy_change',
    'month', 'quarter', 'year',
    'month_sin', 'month_cos',
    'temp_mean', 'temp_min', 'temp_max', 'temp_amplitude', 'temp_std',
    'rainfall_total', 'rainfall_max_daily', 'rainfall_days',
    'weather_comfort', 'rainfall_intensity',
    'latitude', 'longitude',
    'distance_to_hanoi_km', 'distance_to_hcm_km',
    'dest_mean_traffic', 'dest_max_traffic', 'dest_std_traffic',
    'population_thousand', 'density', 'grdp',
    'youtube_views', 'youtube_likes'
]

# Filter to existing columns
feature_cols = [c for c in feature_cols if c in df_filtered.columns]
print("Using {} features".format(len(feature_cols)))

# Filter rows with complete lag features
df_model = df_filtered.dropna(subset=['traffic_lag_12m'])
print("Rows for modeling: {}".format(len(df_model)))

# Time-based split
split_date = df_model['date_parsed'].max() - pd.DateOffset(months=6)
train_df = df_model[df_model['date_parsed'] <= split_date]
test_df = df_model[df_model['date_parsed'] > split_date]

print("Train: {} rows".format(len(train_df)))
print("Test: {} rows".format(len(test_df)))

# ========================================================================
# 6. TRAIN LIGHTGBM MODEL
# ========================================================================
print("\n" + "=" * 70)
print("STEP 6: TRAIN LIGHTGBM MODEL")
print("=" * 70)

try:
    import lightgbm as lgb
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
    
    # Prepare data
    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df['traffic']
    X_test = test_df[feature_cols].fillna(0)
    y_test = test_df['traffic']
    
    # Convert object columns to numeric
    for col in X_train.columns:
        if X_train[col].dtype == 'object':
            X_train[col] = pd.to_numeric(X_train[col].astype(str).str.replace(',', '').str.replace('.', '', regex=False), errors='coerce')
            X_test[col] = pd.to_numeric(X_test[col].astype(str).str.replace(',', '').str.replace('.', '', regex=False), errors='coerce')
    
    X_train = X_train.fillna(0)
    X_test = X_test.fillna(0)
    
    print("X_train shape: {}".format(X_train.shape))
    print("X_test shape: {}".format(X_test.shape))
    
    # Create datasets
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
        'verbose': -1,
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
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print("\n" + "=" * 70)
    print("MODEL PERFORMANCE (FILTERED DATA)")
    print("=" * 70)
    print("   RMSE: {:.2f}".format(rmse))
    print("   MAE: {:.2f}".format(mae))
    print("   R2: {:.4f} ({:.2f}%)".format(r2, r2 * 100))
    
    # Feature importance
    print("\nTOP 15 IMPORTANT FEATURES:")
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance()
    }).sort_values('importance', ascending=False)
    
    for idx, row in importance.head(15).iterrows():
        print("   {}: {}".format(row['feature'], row['importance']))
    
    # Save model
    model.save_model('traffic_prediction_filtered.lgb')
    print("\nModel saved to analysis_filtered/traffic_prediction_filtered.lgb")
    
    # ========================================================================
    # 7. COMPARE WITH ORIGINAL (ALL DATA)
    # ========================================================================
    print("\n" + "=" * 70)
    print("STEP 7: COMPARISON ANALYSIS")
    print("=" * 70)
    
    # Analyze prediction errors
    test_df_result = test_df[['destination', 'province', 'traffic']].copy()
    test_df_result['predicted'] = y_pred
    test_df_result['error'] = abs(test_df_result['traffic'] - test_df_result['predicted'])
    test_df_result['error_pct'] = test_df_result['error'] / (test_df_result['traffic'] + 1) * 100
    
    print("\nPrediction error analysis:")
    print("   Mean absolute error: {:.2f}".format(test_df_result['error'].mean()))
    print("   Median absolute error: {:.2f}".format(test_df_result['error'].median()))
    print("   Mean error %: {:.2f}%".format(test_df_result['error_pct'].mean()))
    
    # Error by traffic level
    print("\nError by traffic level:")
    for lower, upper in [(0, 10), (10, 50), (50, 100), (100, 500), (500, 10000)]:
        subset = test_df_result[(test_df_result['traffic'] >= lower) & (test_df_result['traffic'] < upper)]
        if len(subset) > 0:
            print("   Traffic {}-{}: MAE={:.2f}, count={}".format(lower, upper, subset['error'].mean(), len(subset)))
    
    # Save results
    test_df_result.to_csv('prediction_results_filtered.csv', index=False)
    print("\nResults saved to analysis_filtered/prediction_results_filtered.csv")

except ImportError:
    print("LightGBM not installed. Install with: pip install lightgbm")

print("\n" + "=" * 70)
print("ANALYSIS COMPLETE!")
print("=" * 70)
