"""
Script tổng hợp dữ liệu du lịch với thời tiết mở rộng
Merge tất cả nguồn dữ liệu thành một dataset hoàn chỉnh cho modeling
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def parse_vietnamese_date(date_str):
    """Chuyển 'thg 1 2011' → datetime"""
    months_vi = {
        'thg 1': 1, 'thg 2': 2, 'thg 3': 3, 'thg 4': 4,
        'thg 5': 5, 'thg 6': 6, 'thg 7': 7, 'thg 8': 8,
        'thg 9': 9, 'thg 10': 10, 'thg 11': 11, 'thg 12': 12
    }
    parts = date_str.rsplit(' ', 1)
    month_str, year = parts[0], int(parts[1])
    month = months_vi.get(month_str, 1)
    return pd.Timestamp(year=year, month=month, day=1)

def merge_tourism_data():
    print("="*70)
    print("📂 BƯỚC 1: LOAD TẤT CẢ DỮ LIỆU")
    print("="*70)
    
    # 1. Traffic data
    traffic_df = pd.read_csv('../data/normalized/vietnam_destinations_normalized.csv')
    traffic_df['date_parsed'] = traffic_df['date'].apply(parse_vietnamese_date)
    print(f"✅ Traffic: {traffic_df.shape}")
    
    # 2. Mapping
    mapping_df = pd.read_csv('../data/normalized/keyword_mapping_normalized.csv')
    print(f"✅ Mapping: {mapping_df.shape}")
    
    # 3. Weather extended
    weather_df = pd.read_csv('../data/normalized/vietnam_weather_monthly_extended.csv')
    weather_df['date'] = pd.to_datetime(weather_df['date'])
    print(f"✅ Weather Extended: {weather_df.shape}")
    
    # 4. Seasonal patterns
    seasonal_df = pd.read_csv('../data/normalized/vietnam_seasonal_destinations_strong.csv')
    print(f"✅ Seasonal: {seasonal_df.shape}")
    
    # 5. Destination statistics
    stats_df = pd.read_csv('../data/normalized/destinations_statistics.csv')
    print(f"✅ Statistics: {stats_df.shape}")
    
    # 6. Infrastructure data
    regions_df = pd.read_csv('../data/normalized/vietnam_regions_with_distances.csv')
    print(f"✅ Regions: {regions_df.shape}")
    
    # 7. Accommodation, restaurants, etc.
    try:
        accommodation_df = pd.read_csv('../data/normalized/vietnam_accommodation.csv')
        restaurant_df = pd.read_csv('../data/normalized/vietnam_restaurants.csv')
        entertainment_df = pd.read_csv('../data/normalized/vietnam_entertainment.csv')
        healthcare_df = pd.read_csv('../data/normalized/vietnam_healthcare.csv')
        shops_df = pd.read_csv('../data/normalized/vietnam_shops.csv')
        print(f"✅ Loaded infrastructure data")
    except:
        print("⚠️ Some infrastructure files not found")
        
    # 8. YouTube data
    youtube_df = pd.read_csv('../data/normalized/vietnam_youtube_province_aggregates.csv')
    print(f"✅ YouTube: {youtube_df.shape}")
    
    # 9. GRDP data
    try:
        grdp_df = pd.read_csv('../data/normalized/vietnam_grdp_by_province.csv')
        print(f"✅ GRDP: {grdp_df.shape}")
    except:
        grdp_df = None
        print("⚠️ GRDP file not found")
    
    # 10. Population data
    try:
        pop_df = pd.read_csv('../data/normalized/vietnam_area_population.csv')
        print(f"✅ Population: {pop_df.shape}")
    except:
        pop_df = None
        
    print("\n" + "="*70)
    print("📂 BƯỚC 2: CHUYỂN TRAFFIC SANG LONG FORMAT")
    print("="*70)
    
    # Get destination columns
    destination_cols = [col for col in traffic_df.columns if col not in ['date', 'date_parsed']]
    print(f"   Destinations: {len(destination_cols)}")
    
    # Melt
    traffic_long = traffic_df.melt(
        id_vars=['date_parsed'],
        value_vars=destination_cols,
        var_name='destination',
        value_name='traffic'
    )
    print(f"   Long format: {traffic_long.shape}")
    
    print("\n" + "="*70)
    print("📂 BƯỚC 3: GHÉP MAPPING (destination → province)")
    print("="*70)
    
    # Merge with mapping
    merged = traffic_long.merge(
        mapping_df[['normalized_name', 'province_normalized']],
        left_on='destination',
        right_on='normalized_name',
        how='left'
    )
    merged = merged.rename(columns={'province_normalized': 'province'})
    merged = merged.drop(columns=['normalized_name'], errors='ignore')
    
    # Fill missing provinces
    missing_pct = merged['province'].isna().mean() * 100
    print(f"   Missing province: {missing_pct:.2f}%")
    
    print("\n" + "="*70)
    print("📂 BƯỚC 4: GHÉP WEATHER EXTENDED")
    print("="*70)
    
    # Merge weather
    merged = merged.merge(
        weather_df,
        left_on=['province', 'date_parsed'],
        right_on=['province', 'date'],
        how='left'
    )
    merged = merged.drop(columns=['date', 'year', 'month'], errors='ignore')
    
    weather_cols = ['temp_mean', 'temp_min', 'temp_max', 'temp_amplitude', 'temp_std',
                    'rainfall_total', 'rainfall_max_daily', 'rainfall_days']
    for col in weather_cols:
        if col in merged.columns:
            print(f"   {col}: {merged[col].notna().mean()*100:.1f}% coverage")
    
    print("\n" + "="*70)
    print("📂 BƯỚC 5: GHÉP SEASONAL PATTERNS")
    print("="*70)
    
    # Rename seasonal columns if needed
    seasonal_df = seasonal_df.rename(columns={
        'Amplitude (Median Peak/Trough)': 'seasonal_amplitude',
        'CV (std/median of months)': 'seasonal_cv',
        'Strong_Months (>=1.2x)': 'strong_months'
    })
    
    # Merge seasonal
    seasonal_cols = ['Destination', 'seasonal_amplitude', 'Peak_Months', 'Primary_Peak_Month', 
                     'Peak_Months_List', 'Num_Strong_Months', 'seasonal_cv']
    seasonal_subset = seasonal_df[[c for c in seasonal_cols if c in seasonal_df.columns]]
    
    merged = merged.merge(
        seasonal_subset,
        left_on='destination',
        right_on='Destination',
        how='left'
    )
    merged = merged.drop(columns=['Destination'], errors='ignore')
    merged['has_strong_seasonality'] = merged['seasonal_amplitude'].notna()
    print(f"   Has strong seasonality: {merged['has_strong_seasonality'].mean()*100:.1f}%")
    
    print("\n" + "="*70)
    print("📂 BƯỚC 6: THÊM TIME FEATURES")
    print("="*70)
    
    merged['year'] = merged['date_parsed'].dt.year
    merged['month'] = merged['date_parsed'].dt.month
    merged['quarter'] = merged['date_parsed'].dt.quarter
    print(f"   Years: {merged['year'].min()} - {merged['year'].max()}")
    
    print("\n" + "="*70)
    print("📂 BƯỚC 7: GHÉP REGIONS & DISTANCES")
    print("="*70)
    
    # Standardize province names in regions
    if 'province' in regions_df.columns:
        merged = merged.merge(
            regions_df[['province', 'region', 'distance_to_hanoi_km', 'distance_to_hcm_km']],
            on='province',
            how='left'
        )
        print(f"   Region coverage: {merged['region'].notna().mean()*100:.1f}%")
    
    print("\n" + "="*70)
    print("📂 BƯỚC 8: GHÉP INFRASTRUCTURE")
    print("="*70)
    
    try:
        # Count by province
        infra_counts = {}
        
        if 'accommodation_df' in dir():
            infra_counts['accommodation_count'] = accommodation_df.groupby('province_normalized').size()
        if 'restaurant_df' in dir():
            infra_counts['restaurant_count'] = restaurant_df.groupby('province_normalized').size()
        if 'entertainment_df' in dir():
            infra_counts['entertainment_count'] = entertainment_df.groupby('province_normalized').size()
        if 'healthcare_df' in dir():
            infra_counts['healthcare_count'] = healthcare_df.groupby('province_normalized').size()
        if 'shops_df' in dir():
            infra_counts['shop_count'] = shops_df.groupby('province_normalized').size()
            
        infra_df = pd.DataFrame(infra_counts).reset_index()
        infra_df = infra_df.rename(columns={'index': 'province'})
        
        merged = merged.merge(infra_df, on='province', how='left')
        print(f"   Infrastructure columns added: {list(infra_counts.keys())}")
    except Exception as e:
        print(f"   ⚠️ Error adding infrastructure: {e}")
    
    print("\n" + "="*70)
    print("📂 BƯỚC 9: GHÉP YOUTUBE & ECONOMICS")
    print("="*70)
    
    # YouTube
    youtube_cols = ['province_normalized', 'views', 'likes', 'comments']
    youtube_subset = youtube_df[[c for c in youtube_cols if c in youtube_df.columns]]
    youtube_subset = youtube_subset.rename(columns={
        'province_normalized': 'province',
        'views': 'youtube_views',
        'likes': 'youtube_likes',
        'comments': 'youtube_comments'
    })
    merged = merged.merge(youtube_subset, on='province', how='left')
    print(f"   YouTube coverage: {merged['youtube_views'].notna().mean()*100:.1f}%")
    
    # GRDP (by year)
    if grdp_df is not None:
        # Rename columns to standard format
        grdp_df = grdp_df.rename(columns={
            'Năm': 'year',
            'province_normalized': 'province',
            'Tổng GRDP\n\xa0(tỷ đồng)': 'grdp'
        })
        grdp_df['year'] = grdp_df['year'].astype(int)
        grdp_subset = grdp_df[['province', 'year', 'grdp']].copy()
        merged = merged.merge(grdp_subset, on=['province', 'year'], how='left')
        print(f"   GRDP coverage: {merged['grdp'].notna().mean()*100:.1f}%")
    
    # Population
    if pop_df is not None:
        pop_df = pop_df.rename(columns={
            'province_normalized': 'province',
            'Năm': 'pop_year',
            'Diện tích (Km2)': 'area_km2',
            'Dân số trung bình (nghìn)': 'population_thousand',
            'Mật độ dân số (người/km2)': 'density'
        })
        pop_cols = ['province', 'area_km2', 'population_thousand', 'density']
        # Get latest year per province
        pop_latest = pop_df.sort_values('pop_year', ascending=False).groupby('province').first().reset_index()
        pop_subset = pop_latest[[c for c in pop_cols if c in pop_latest.columns]]
        merged = merged.merge(pop_subset, on='province', how='left')
        print(f"   Population coverage: {merged['population_thousand'].notna().mean()*100:.1f}%")
    
    print("\n" + "="*70)
    print("📂 BƯỚC 10: GHÉP DESTINATION STATISTICS")
    print("="*70)
    
    # Rename stats columns
    stats_df = stats_df.rename(columns={
        'Destination': 'destination',
        'Mean': 'dest_mean_traffic',
        'Median': 'dest_median_traffic',
        'Max': 'dest_max_traffic',
        'Min': 'dest_min_traffic',
        'Std Dev': 'dest_std_traffic',
        'Coverage %': 'dest_coverage_pct'
    })
    
    # Stats
    stats_cols = ['destination', 'dest_mean_traffic', 'dest_median_traffic', 'dest_max_traffic', 
                  'dest_std_traffic', 'dest_coverage_pct']
    stats_subset = stats_df[[c for c in stats_cols if c in stats_df.columns]]
    merged = merged.merge(stats_subset, on='destination', how='left')
    print(f"   Stats coverage: {merged['dest_mean_traffic'].notna().mean()*100:.1f}%")
    
    print("\n" + "="*70)
    print("📂 FINAL CLEANUP & SAVE")
    print("="*70)
    
    # Sort
    merged = merged.sort_values(['destination', 'date_parsed'])
    
    # Final shape
    print(f"\n📊 FINAL DATASET:")
    print(f"   Shape: {merged.shape}")
    print(f"   Columns: {merged.columns.tolist()}")
    print(f"   Memory: {merged.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Data quality
    print(f"\n📊 DATA QUALITY:")
    null_pct = merged.isnull().mean().sort_values(ascending=False)
    for col, pct in null_pct.head(10).items():
        if pct > 0:
            print(f"   {col}: {pct*100:.1f}% missing")
    
    # Save
    output_path = '../data/normalized/merged_tourism_data_extended.csv'
    merged.to_csv(output_path, index=False)
    print(f"\n💾 Saved to {output_path}")
    
    return merged

if __name__ == "__main__":
    merged = merge_tourism_data()
