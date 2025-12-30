"""
Script để tính toán các features thời tiết mở rộng từ dữ liệu hàng ngày
- temp_min: Nhiệt độ thấp nhất trong tháng
- temp_max: Nhiệt độ cao nhất trong tháng  
- temp_mean: Nhiệt độ trung bình (đã có sẵn)
- temp_amplitude: Biên độ nhiệt (max - min)
- temp_std: Độ lệch chuẩn nhiệt độ
- rainfall_max: Lượng mưa cao nhất 1 ngày
- rainfall_days: Số ngày có mưa
"""

import pandas as pd
import numpy as np
from pathlib import Path

def create_weather_extended_features():
    print("📂 Loading daily weather data...")
    
    # Load dữ liệu hàng ngày
    daily_df = pd.read_csv('../data/vietnam_weather_by_province_2011_2025.csv')
    daily_df['date'] = pd.to_datetime(daily_df['date'])
    
    print(f"   Shape: {daily_df.shape}")
    print(f"   Date range: {daily_df['date'].min()} to {daily_df['date'].max()}")
    print(f"   Provinces: {daily_df['province'].nunique()}")
    
    # Tạo cột year-month
    daily_df['year'] = daily_df['date'].dt.year
    daily_df['month'] = daily_df['date'].dt.month
    daily_df['year_month'] = daily_df['date'].dt.to_period('M')
    
    print("\n🔧 Calculating monthly statistics...")
    
    # Aggregate theo tỉnh và tháng
    monthly_stats = daily_df.groupby(['province', 'year', 'month']).agg({
        'temp_avg': ['mean', 'min', 'max', 'std'],
        'rainfall': ['sum', 'max', 'mean', lambda x: (x > 0).sum()],  # sum, max daily, mean, rainy days
        'latitude': 'first',
        'longitude': 'first'
    }).reset_index()
    
    # Flatten column names
    monthly_stats.columns = [
        'province', 'year', 'month',
        'temp_mean', 'temp_min', 'temp_max', 'temp_std',
        'rainfall_total', 'rainfall_max_daily', 'rainfall_mean_daily', 'rainfall_days',
        'latitude', 'longitude'
    ]
    
    # Tính biên độ nhiệt
    monthly_stats['temp_amplitude'] = monthly_stats['temp_max'] - monthly_stats['temp_min']
    
    # Tạo date column (ngày đầu tháng)
    monthly_stats['date'] = pd.to_datetime(
        monthly_stats['year'].astype(str) + '-' + 
        monthly_stats['month'].astype(str).str.zfill(2) + '-01'
    )
    
    # Sắp xếp lại cột
    columns_order = [
        'province', 'date', 'year', 'month',
        'temp_mean', 'temp_min', 'temp_max', 'temp_amplitude', 'temp_std',
        'rainfall_total', 'rainfall_max_daily', 'rainfall_mean_daily', 'rainfall_days',
        'latitude', 'longitude'
    ]
    monthly_stats = monthly_stats[columns_order]
    
    print(f"\n✅ Monthly stats shape: {monthly_stats.shape}")
    print(f"\n📊 Sample data:")
    print(monthly_stats.head(10))
    
    print(f"\n📊 Statistics summary:")
    print(monthly_stats[['temp_mean', 'temp_min', 'temp_max', 'temp_amplitude', 'temp_std', 
                         'rainfall_total', 'rainfall_days']].describe())
    
    # Save to file
    output_path = '../data/normalized/vietnam_weather_monthly_extended.csv'
    monthly_stats.to_csv(output_path, index=False)
    print(f"\n💾 Saved to {output_path}")
    
    return monthly_stats

if __name__ == "__main__":
    monthly_stats = create_weather_extended_features()
