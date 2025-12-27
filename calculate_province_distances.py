#!/usr/bin/env python3
"""
Script tính khoảng cách từ 63 tỉnh thành Việt Nam đến trung tâm TP.HCM và Hà Nội.
Chọn thành phố nào gần hơn và lưu khoảng cách đó.
"""

import pandas as pd
import requests
import time
from math import radians, sin, cos, sqrt, atan2

# Tọa độ trung tâm các thành phố chính
CITY_CENTERS = {
    'Hà Nội': (21.0285, 105.8542),  # Hồ Hoàn Kiếm
    'TP.Hồ Chí Minh': (10.8231, 106.6297)  # Khu vực Bến Thành
}

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Tính khoảng cách đường chim bay (km) giữa 2 điểm sử dụng công thức Haversine
    
    Args:
        lat1, lon1: Tọa độ điểm 1 (latitude, longitude)
        lat2, lon2: Tọa độ điểm 2 (latitude, longitude)
    
    Returns:
        float: Khoảng cách tính bằng km
    """
    R = 6371  # Bán kính Trái Đất (km)
    
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    return R * c

def geocode_province(province_name):
    """
    Lấy tọa độ của tỉnh/thành phố từ Nominatim API (OpenStreetMap)
    
    Args:
        province_name: Tên tỉnh/thành phố
    
    Returns:
        tuple: (latitude, longitude) hoặc (None, None) nếu không tìm thấy
    """
    query = f"{province_name}, Vietnam"
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        'q': query,
        'format': 'json',
        'limit': 1,
        'addressdetails': 1
    }
    headers = {
        'User-Agent': 'Vietnam-Province-Distance-Calculator/1.0'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data:
            lat = float(data[0]['lat'])
            lon = float(data[0]['lon'])
            print(f"  ✓ {province_name}: ({lat:.4f}, {lon:.4f})")
            return lat, lon
        else:
            print(f"  ✗ {province_name}: Không tìm thấy")
            return None, None
            
    except Exception as e:
        print(f"  ✗ {province_name}: Lỗi - {e}")
        return None, None

def calculate_province_distances(input_csv='cacKhuVuc/cacKhuVucVietNam.csv', 
                                 output_csv='cacKhuVuc/cacKhuVucVietNam_with_distances.csv'):
    """
    Đọc file CSV chứa danh sách tỉnh thành, tính khoảng cách và lưu kết quả
    
    Args:
        input_csv: Đường dẫn file CSV đầu vào
        output_csv: Đường dẫn file CSV đầu ra
    """
    # Đọc file CSV
    print(f"Đang đọc file: {input_csv}")
    df = pd.read_csv(input_csv, header=None, names=['stt', 'province', 'region'])
    
    print(f"Tìm thấy {len(df)} tỉnh thành\n")
    
    # Thêm các cột mới
    df['latitude'] = None
    df['longitude'] = None
    df['distance_to_hanoi_km'] = None
    df['distance_to_hcm_km'] = None
    df['nearest_city'] = None
    df['nearest_distance_km'] = None
    
    # Tọa độ của HCM và Hà Nội
    hanoi_lat, hanoi_lon = CITY_CENTERS['Hà Nội']
    hcm_lat, hcm_lon = CITY_CENTERS['TP.Hồ Chí Minh']
    
    print("Bắt đầu geocoding và tính khoảng cách...\n")
    
    for idx, row in df.iterrows():
        province = row['province']
        print(f"[{idx+1}/{len(df)}] {province}")
        
        # Lấy tọa độ
        lat, lon = geocode_province(province)
        
        if lat and lon:
            df.at[idx, 'latitude'] = lat
            df.at[idx, 'longitude'] = lon
            
            # Tính khoảng cách đến Hà Nội
            dist_hanoi = haversine_distance(lat, lon, hanoi_lat, hanoi_lon)
            df.at[idx, 'distance_to_hanoi_km'] = round(dist_hanoi, 1)
            
            # Tính khoảng cách đến TP.HCM
            dist_hcm = haversine_distance(lat, lon, hcm_lat, hcm_lon)
            df.at[idx, 'distance_to_hcm_km'] = round(dist_hcm, 1)
            
            # Chọn thành phố gần nhất
            if dist_hanoi < dist_hcm:
                df.at[idx, 'nearest_city'] = 'Hà Nội'
                df.at[idx, 'nearest_distance_km'] = round(dist_hanoi, 1)
            else:
                df.at[idx, 'nearest_city'] = 'TP.HCM'
                df.at[idx, 'nearest_distance_km'] = round(dist_hcm, 1)
            
            print(f"    → Hà Nội: {dist_hanoi:.1f} km | TP.HCM: {dist_hcm:.1f} km | Gần nhất: {df.at[idx, 'nearest_city']} ({df.at[idx, 'nearest_distance_km']} km)\n")
        
        # Nghỉ 1 giây giữa các request (tuân thủ rate limit của Nominatim)
        time.sleep(1)
        
        # Lưu checkpoint mỗi 10 tỉnh
        if (idx + 1) % 10 == 0:
            df.to_csv(output_csv, index=False)
            print(f"  💾 Checkpoint saved at row {idx+1}\n")
    
    # Lưu kết quả cuối cùng
    df.to_csv(output_csv, index=False)
    print(f"\n✅ Hoàn thành! Kết quả đã lưu vào: {output_csv}")
    
    # In thống kê
    print("\n" + "="*60)
    print("THỐNG KÊ")
    print("="*60)
    print(f"Tổng số tỉnh thành: {len(df)}")
    print(f"Geocoded thành công: {df['latitude'].notna().sum()}")
    print(f"Không tìm thấy: {df['latitude'].isna().sum()}")
    
    if df['nearest_city'].notna().any():
        print(f"\nGần Hà Nội hơn: {(df['nearest_city'] == 'Hà Nội').sum()} tỉnh")
        print(f"Gần TP.HCM hơn: {(df['nearest_city'] == 'TP.HCM').sum()} tỉnh")
        
        print(f"\nKhoảng cách trung bình đến thành phố gần nhất: {df['nearest_distance_km'].mean():.1f} km")
        print(f"Khoảng cách gần nhất: {df['nearest_distance_km'].min():.1f} km")
        print(f"Khoảng cách xa nhất: {df['nearest_distance_km'].max():.1f} km")
        
        # Tỉnh xa nhất
        farthest = df.loc[df['nearest_distance_km'].idxmax()]
        print(f"\nTỉnh xa nhất: {farthest['province']} - {farthest['nearest_distance_km']:.1f} km đến {farthest['nearest_city']}")
        
        # Top 5 tỉnh gần Hà Nội nhất
        df_valid = df[df['nearest_distance_km'].notna()].copy()
        df_valid['nearest_distance_km'] = pd.to_numeric(df_valid['nearest_distance_km'])
        
        hanoi_provinces = df_valid[df_valid['nearest_city'] == 'Hà Nội'].nsmallest(5, 'nearest_distance_km')
        if not hanoi_provinces.empty:
            print(f"\nTop 5 tỉnh gần Hà Nội nhất:")
            for _, p in hanoi_provinces.iterrows():
                print(f"  - {p['province']}: {p['nearest_distance_km']} km")
        
        # Top 5 tỉnh gần TP.HCM nhất
        hcm_provinces = df_valid[df_valid['nearest_city'] == 'TP.HCM'].nsmallest(5, 'nearest_distance_km')
        if not hcm_provinces.empty:
            print(f"\nTop 5 tỉnh gần TP.HCM nhất:")
            for _, p in hcm_provinces.iterrows():
                print(f"  - {p['province']}: {p['nearest_distance_km']} km")
    
    return df

if __name__ == '__main__':
    print("="*60)
    print("TÍNH KHOẢNG CÁCH 63 TỈNH THÀNH VIỆT NAM")
    print("Đến trung tâm Hà Nội và TP.HCM")
    print("="*60 + "\n")
    
    df = calculate_province_distances()
    
    print("\n✅ Script hoàn tất!")
