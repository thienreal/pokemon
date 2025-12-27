import pandas as pd

# Read mapping
mapping = pd.read_csv('/workspaces/pokemon/data/vietnam_province_name_mapping.csv')
province_mapping = dict(zip(mapping['old'], mapping['new']))

print("Province Mapping:")
for old, new in list(province_mapping.items())[:10]:
    print(f"  {old} → {new}")
print(f"  ... ({len(province_mapping)} mappings)")

# List of files to update
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
    
    # Read the file (with semicolon separator)
    df = pd.read_csv(filepath, sep=';')
    
    print(f"\n{'='*60}")
    print(f"File: {filename}")
    print(f"Total rows: {len(df)}")
    
    # Get the province column
    province_col = 'province'
    
    # Count changes
    unmapped = set()
    mapped_provinces = []
    
    # Apply mapping
    for prov in df[province_col]:
        if prov in province_mapping:
            mapped_provinces.append(province_mapping[prov])
        else:
            unmapped.add(prov)
            mapped_provinces.append(prov)
    
    changes = sum(1 for old, new in zip(df[province_col], mapped_provinces) if old != new)
    df[province_col] = mapped_provinces
    
    print(f"Provinces mapped: {changes}")
    
    if unmapped:
        print(f"Unmapped provinces ({len(unmapped)}):")
        for prov in sorted(unmapped):
            count = len(df[df[province_col] == prov])
            print(f"  {prov}: {count}")
    
    # Save the file
    df.to_csv(filepath, sep=';', index=False)
    
    results[filename] = {
        'total': len(df),
        'changed': changes,
        'unmapped': len(unmapped)
    }

print(f"\n{'='*60}")
print("SUMMARY:")
print(f"{'='*60}")
for filename, stats in results.items():
    print(f"{filename}: {stats['total']} rows, {stats['changed']} changed, {stats['unmapped']} unmapped")

print(f"\n✅ Ánh xạ tỉnh/thành hoàn tất!")
