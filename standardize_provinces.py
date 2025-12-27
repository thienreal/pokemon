import pandas as pd
import os

data_dir = "/workspaces/pokemon/data"

# Định nghĩa các quy tắc chuẩn hóa tên tỉnh
standardization_rules = {
    'TP.Hồ Chí Minh': 'TP. Hồ Chí Minh',
    'TP. Cần Thơ': 'Cần Thơ',
    'TP. Hải Phòng': 'Hải Phòng',
    'TP. Đà Nẵng': 'Đà Nẵng',
    'TP. Hà Nội': 'Hà Nội',
    'TP. Huế': 'Thừa Thiên Huế',
    'Cần Thơ': 'Cần Thơ',
    'Hải Phòng': 'Hải Phòng',
    'Đà Nẵng': 'Đà Nẵng',
    'Hoà Bình': 'Hòa Bình',
    'Thanh Hoá': 'Thanh Hóa',
}

files_to_standardize = [
    "youtube_province_videos.csv",
    "cacKhuVucVietNam_with_distances.csv"
]

for file_name in files_to_standardize:
    file_path = os.path.join(data_dir, file_name)
    print(f"Standardizing: {file_name}")
    
    df = pd.read_csv(file_path)
    
    # Apply standardization
    for old_name, new_name in standardization_rules.items():
        df['province'] = df['province'].replace(old_name, new_name)
    
    # Save
    df.to_csv(file_path, index=False)
    
    print(f"✓ {file_name} standardized")
    print(f"  Unique provinces: {df['province'].nunique()}")
    print(f"  Provinces: {sorted(df['province'].unique())}")
    print()

print("✓ All standardization completed!")
