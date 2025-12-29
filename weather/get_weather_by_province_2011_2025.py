import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import time
import sys

# Tạo session với headers giống browser để tránh bị chặn
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://open-meteo.com/',
})

# Đọc danh sách 34 tỉnh/thành đã chuẩn hóa
script_dir = Path(__file__).resolve().parent.parent  # /workspaces/pokemon
provinces_file = script_dir / 'data' / 'vietnam_regions_with_distances.csv'

print("=== LẤY DỮ LIỆU THỜI TIẾT 34 TỈNH/THÀNH VIỆT NAM (2011-2025) ===\n", flush=True)
print(f"📁 Đọc file: {provinces_file}", flush=True)

df_provinces = pd.read_csv(provinces_file)
print(f"✅ Đã đọc {len(df_provinces)} tỉnh/thành từ file\n", flush=True)

# Các năm cần lấy (chia nhỏ theo năm vì API không cho phép lấy quá nhiều năm cùng lúc)
years = list(range(2011, 2026))  # 2011-2025
print(f"📅 Khoảng thời gian: 2011-2025 (15 năm)", flush=True)
print(f"⚡ Chiến lược: Lấy từng năm một để tránh timeout\n", flush=True)

# Hàm lấy dữ liệu thời tiết cho một tỉnh trong một năm
def get_weather_data(province_name, lat, lon, year):
    """
    Lấy dữ liệu thời tiết cho một tỉnh trong một năm cụ thể
    """
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_mean,precipitation_sum",
        "timezone": "Asia/Bangkok"
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                weather_df = pd.DataFrame({
                    "date": data["daily"]["time"],
                    "temp_avg": data["daily"]["temperature_2m_mean"],
                    "rainfall": data["daily"]["precipitation_sum"],
                    "province": province_name,
                    "latitude": lat,
                    "longitude": lon
                })
                return weather_df
            elif response.status_code == 429:
                wait_time = 5 * (attempt + 1)
                print(f" ⏳ Rate limit, chờ {wait_time}s...", end='', flush=True)
                time.sleep(wait_time)
            else:
                if attempt == max_retries - 1:
                    print(f" Lỗi {response.status_code}", end='', flush=True)
                return pd.DataFrame()
        except Exception as e:
            if attempt == max_retries - 1:
                print(f" Timeout", end='', flush=True)
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return pd.DataFrame()
    
    return pd.DataFrame()

# Lấy dữ liệu cho từng tỉnh, từng năm
all_weather_data = []
success_count = 0
failed_count = 0
total_requests = len(df_provinces) * len(years)

print(f"🔄 BẮT ĐẦU LẤY DỮ LIỆU:")
print(f"   Tổng: {len(df_provinces)} tỉnh × {len(years)} năm = {total_requests} requests\n")
print("=" * 80, flush=True)

request_num = 0
for year in years:
    print(f"\n📅 NĂM {year}:", flush=True)
    year_data = []
    
    for index, row in df_provinces.iterrows():
        request_num += 1
        province_name = row['province']
        lat = row['latitude']
        lon = row['longitude']
        
        print(f"  [{request_num}/{total_requests}] {province_name} ({year})...", end=' ', flush=True)
        
        weather_df = get_weather_data(province_name, lat, lon, year)
        
        if not weather_df.empty:
            year_data.append(weather_df)
            success_count += 1
            print(f"✅ {len(weather_df)} ngày", flush=True)
        else:
            failed_count += 1
            print(f"❌", flush=True)
        
        # Chờ 2s giữa các request
        if request_num < total_requests:
            time.sleep(2)
    
    # Gộp dữ liệu của năm vào tổng
    if year_data:
        all_weather_data.extend(year_data)
        print(f"  ✅ Năm {year}: {len(year_data)}/{len(df_provinces)} tỉnh thành công", flush=True)

# Tổng hợp kết quả
print("\n" + "=" * 80, flush=True)
print("📊 KẾT QUẢ:", flush=True)
print(f"   ✅ Thành công: {success_count}/{total_requests} requests", flush=True)
print(f"   ❌ Thất bại: {failed_count}/{total_requests} requests", flush=True)
print(f"   📈 Tỷ lệ thành công: {success_count/total_requests*100:.1f}%", flush=True)

# Gộp và lưu file
if all_weather_data:
    print(f"\n🔄 Đang gộp dữ liệu...", flush=True)
    final_df = pd.concat(all_weather_data, ignore_index=True)
    
    # Thống kê
    print(f"\n📈 THỐNG KÊ DỮ LIỆU:", flush=True)
    print(f"   - Tổng số dòng: {len(final_df):,}", flush=True)
    print(f"   - Số tỉnh: {final_df['province'].nunique()}", flush=True)
    print(f"   - Khoảng thời gian: {final_df['date'].min()} đến {final_df['date'].max()}", flush=True)
    print(f"   - Nhiệt độ TB: {final_df['temp_avg'].mean():.2f}°C", flush=True)
    print(f"   - Lượng mưa TB: {final_df['rainfall'].mean():.2f} mm/ngày", flush=True)
    
    # Lưu file
    output_file = script_dir / "data" / "vietnam_weather_by_province_2011_2025.csv"
    print(f"\n💾 Đang lưu file...", flush=True)
    final_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    
    print(f"\n✅ ĐÃ LƯU FILE:", flush=True)
    print(f"   📁 {output_file}", flush=True)
    print(f"   📊 Kích thước: {output_file.stat().st_size / 1024 / 1024:.2f} MB", flush=True)
    
    print("\n✅ HOÀN TẤT!", flush=True)
    print("\nCột trong file:", flush=True)
    print("   - date: Ngày (YYYY-MM-DD)", flush=True)
    print("   - temp_avg: Nhiệt độ trung bình (°C)", flush=True)
    print("   - rainfall: Lượng mưa (mm)", flush=True)
    print("   - province: Tên tỉnh/thành", flush=True)
    print("   - latitude: Vĩ độ", flush=True)
    print("   - longitude: Kinh độ", flush=True)
    
    print("\n📋 PREVIEW 10 DÒNG ĐẦU:", flush=True)
    print(final_df.head(10).to_string(index=False), flush=True)
else:
    print("\n❌ KHÔNG LẤY ĐƯỢC DỮ LIỆU NÀO!", flush=True)

print("\n" + "=" * 80, flush=True)
