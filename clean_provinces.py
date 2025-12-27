import pandas as pd
import re

# Read mapping.csv to get valid provinces
mapping = pd.read_csv('/workspaces/pokemon/data/vietnam_province_name_mapping.csv')
valid_provinces = set(mapping['old'].unique())

# Create a mapping for "Thành phố" variations to standardized names
city_mappings = {
    'Thành phố Hà Nội': 'TP. Hà Nội',
    'Thành phố Hồ Chí Minh': 'TP. Hồ Chí Minh',
    'Thành phố Hải Phòng': 'TP. Hải Phòng',
    'Thành phố Đà Nẵng': 'TP. Đà Nẵng',
    'Thành phố Cần Thơ': 'TP. Cần Thơ',
    'Thành phố Huế': 'TP. Huế',
}

# Also add variations with dash instead of dash in province names
dash_replacements = {
    'Bà Rịa - Vũng Tàu': 'Bà Rịa – Vũng Tàu',
    'Bà Rịa -Vũng Tàu': 'Bà Rịa – Vũng Tàu',
    'Bà Rịa-Vũng Tàu': 'Bà Rịa – Vũng Tàu',
    'Bà Rịa – Vũng Tàu': 'Bà Rịa – Vũng Tàu',
}

print("Valid provinces từ mapping.csv:")
print(f"Total valid provinces: {len(valid_provinces)}")

# List of files to clean
files = [
    'vietnam_accommodation.csv',
    'vietnam_entertainment.csv', 
    'vietnam_healthcare.csv',
    'vietnam_restaurants.csv',
    'vietnam_shops.csv'
]

results = {}

for filename in files:
    filepath = f'/workspaces/pokemon/data/{filename}'
    
    # Determine the separator
    try:
        df = pd.read_csv(filepath, sep=';')
    except:
        df = pd.read_csv(filepath)
    
    print(f"\n{'='*60}")
    print(f"File: {filename}")
    print(f"Total rows before: {len(df)}")
    
    # Check which column is province
    if 'province' in df.columns:
        province_col = 'province'
    else:
        print(f"⚠️  Cannot find province column")
        continue
    
    # First, standardize city names with "Thành phố" 
    for city_variant, standard_name in city_mappings.items():
        df[province_col] = df[province_col].str.replace(city_variant, standard_name, regex=False)
    
    # Standardize dash variations
    for dash_variant, standard_name in dash_replacements.items():
        df[province_col] = df[province_col].str.replace(dash_variant, standard_name, regex=False)
    
    # Also add standardized cities to valid provinces
    valid_provinces_with_cities = valid_provinces.copy()
    valid_provinces_with_cities.update(city_mappings.values())
    
    # Filter rows where province is in valid_provinces
    df_clean = df[df[province_col].isin(valid_provinces_with_cities)].copy()
    
    # Find removed rows for report
    removed_df = df[~df[province_col].isin(valid_provinces_with_cities)]
    
    print(f"Total rows after: {len(df_clean)}")
    print(f"Rows removed: {len(removed_df)}")
    
    if len(removed_df) > 0:
        print(f"\nRemaining invalid provinces (if any):")
        invalid_counts = removed_df[province_col].value_counts().head(10)
        print(invalid_counts.to_string())
    
    # Save cleaned file
    if ';' in open(filepath).readline():
        df_clean.to_csv(filepath, sep=';', index=False)
    else:
        df_clean.to_csv(filepath, index=False)
    
    results[filename] = {
        'before': len(df),
        'after': len(df_clean),
        'removed': len(removed_df)
    }

print(f"\n{'='*60}")
print("SUMMARY:")
print(f"{'='*60}")
for filename, stats in results.items():
    print(f"{filename}:")
    print(f"  Before: {stats['before']:,} → After: {stats['after']:,} (Removed: {stats['removed']:,})")

print(f"\n✅ Tất cả file đã được làm sạch!")
