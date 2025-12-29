import requests
import pandas as pd
from pathlib import Path
import time

# Tạo session
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://open-meteo.com/',
})

# Đọc danh sách 34 tỉnh/thành
script_dir = Path(__file__).resolve().parent.parent
provinces_file = script_dir / 'data' / 'vietnam_regions_with_distances.csv'

print("=== LẤY DỮ LIỆU THỜI TIẾT 2025-07-01 ĐẾN 2025-12-29 ===\n", flush=True)
print(f"📁 Đọc file: {provinces_file}", flush=True)

df_provinces = pd.read_csv(provinces_file)
print(f"✅ Đã đọc {len(df_provinces)} tỉnh/thành từ file\n", flush=True)

# Hàm lấy dữ liệu
def get_weather_data(province_name, lat, lon):
    """Lấy dữ liệu thời tiết từ 2025-07-01 đến 2025-12-29"""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": "2025-07-01",
        "end_date": "2025-12-29",
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

# Lấy dữ liệu
print(f"🔄 BẮT ĐẦU LẤY DỮ LIỆU (2025-07-01 ĐẾN 2025-12-29):\n", flush=True)

all_weather_data = []
success_count = 0

for index, row in df_provinces.iterrows():
    province_name = row['province']
    lat = row['latitude']
    lon = row['longitude']
    
    print(f"[{index+1}/34] {province_name}...", end=' ', flush=True)
    
    weather_df = get_weather_data(province_name, lat, lon)
    
    if not weather_df.empty:
        all_weather_data.append(weather_df)
        success_count += 1
        print(f"✅ {len(weather_df)} ngày", flush=True)
    else:
        print(f"❌", flush=True)
    
    # Delay 0.5s
    if index < len(df_provinces) - 1:
        time.sleep(0.5)

print("\n" + "=" * 80, flush=True)
print(f"📊 KẾT QUẢ: ✅ {success_count}/34 tỉnh", flush=True)

# Gộp và lưu
if all_weather_data:
    print(f"\n🔄 Đang gộp dữ liệu...", flush=True)
    new_data_df = pd.concat(all_weather_data, ignore_index=True)
    
    # Đọc file cũ từ normalized folder
    normalized_file = script_dir / "data" / "normalized" / "vietnam_weather_2011_2025.csv"
    print(f"📖 Đang đọc file cũ...", flush=True)
    existing_df = pd.read_csv(normalized_file)
    
    print(f"📝 Đang gộp dữ liệu (cũ + mới)...", flush=True)
    final_df = pd.concat([existing_df, new_data_df], ignore_index=True)
    
    # Lưu file
    print(f"💾 Đang lưu file...", flush=True)
    final_df.to_csv(normalized_file, index=False, encoding="utf-8-sig")
    
    # Thống kê
    print(f"\n✅ ĐÃ LƯU FILE:", flush=True)
    print(f"   📁 {normalized_file}", flush=True)
    print(f"   📊 Tổng dòng: {len(final_df):,}", flush=True)
    print(f"   📊 Kích thước: {normalized_file.stat().st_size / 1024 / 1024:.2f} MB", flush=True)
    print(f"   📅 Khoảng thời gian: {final_df['date'].min()} đến {final_df['date'].max()}", flush=True)
    
    print("\n✅ HOÀN TẤT!", flush=True)
else:
    print("\n❌ KHÔNG LẤY ĐƯỢC DỮ LIỆU NÀO!", flush=True)

print("=" * 80, flush=True)
