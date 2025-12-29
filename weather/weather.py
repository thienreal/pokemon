import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import time
import random

# Tạo session với headers giống browser để tránh bị chặn
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://open-meteo.com/',
})

# Đường dẫn tương đối từ script
script_dir = Path(__file__).resolve().parent
file_path = script_dir / 'destinations_location.csv'

df_locations = pd.read_csv(file_path)  # Hoặc pd.read_excel nếu là .xlsx

# Kiểm tra cột (nếu cần in ra console)
print("Các địa điểm trong file:")
print(df_locations[['name', 'province', 'latitude', 'longitude']])

# Ngày lấy dữ liệu (năm 2018)
start_date = "2018-01-01"
end_date = "2018-12-31"

# Lấy dữ liệu theo tỉnh thay vì theo từng địa điểm để tránh rate limit
print("\n=== CHIẾN LƯỢC: Lấy dữ liệu theo TỈNH thay vì từng địa điểm ===")
print("Điều này giảm số request từ 975 xuống ~63 tỉnh\n")

# Lấy danh sách tỉnh duy nhất với tọa độ đại diện (trung bình)
provinces = df_locations.groupby('province').agg({
    'latitude': 'mean',
    'longitude': 'mean'
}).reset_index()
print(f"Số tỉnh cần lấy dữ liệu: {len(provinces)}\n")

# Hàm lấy dữ liệu thời tiết cho một địa điểm
def get_weather_data(lat, lon, location_name, province_name):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_mean,precipitation_sum",
        "timezone": "Asia/Bangkok"
    }
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                weather_df = pd.DataFrame({
                    "date": data["daily"]["time"],
                    "temp_avg": data["daily"]["temperature_2m_mean"],
                    "rainfall": data["daily"]["precipitation_sum"]
                })
                weather_df["name"] = location_name
                weather_df["province"] = province_name
                weather_df["province_latitude"] = lat
                weather_df["province_longitude"] = lon
                return weather_df
            elif response.status_code == 429:
                wait_time = 30 * (2 ** attempt)
                print(f"⏳ Rate limit - chờ {wait_time}s rồi thử lại ({attempt+1}/{max_retries})...")
                time.sleep(wait_time)
            else:
                print(f"❌ Lỗi {response.status_code} cho {location_name}")
                return pd.DataFrame()
        except requests.exceptions.RequestException as e:
            print(f"❌ Lỗi kết nối: {e}")
            if attempt < max_retries - 1:
                time.sleep(10)
                continue
            return pd.DataFrame()
    
    print(f"❌ Không thể lấy dữ liệu cho {location_name} sau {max_retries} lần thử")
    return pd.DataFrame()

# Bước 1: Lấy dữ liệu thời tiết cho từng tỉnh
province_weather = {}
import time
for index, row in provinces.iterrows():
    province_name = row['province']
    print(f"Đang lấy thời tiết {index+1}/{len(provinces)}: {province_name}...")
    
    weather = get_weather_data(row['latitude'], row['longitude'], province_name, province_name)
    if not weather.empty:
        province_weather[province_name] = weather
        print(f"  ✅ Thành công - {len(weather)} ngày dữ liệu")
    else:
        print(f"  ❌ Không lấy được dữ liệu cho {province_name}")
    
    # Random delay 15-25 giây để tránh pattern detection
    delay = random.uniform(15, 25)
    print(f"  💤 Chờ {delay:.1f}s trước tỉnh tiếp theo...\n")
    time.sleep(delay)

print(f"\n=== Đã lấy thời tiết cho {len(province_weather)}/{len(provinces)} tỉnh ===\n")

# Bước 2: Gán dữ liệu tỉnh cho tất cả địa điểm trong tỉnh đó
all_weather_data = []
for index, loc in df_locations.iterrows():
    province_name = loc['province']
    if province_name in province_weather:
        # Lấy dữ liệu thời tiết của tỉnh
        weather_df = province_weather[province_name].copy()
        # Gán thông tin địa điểm cụ thể
        weather_df['name'] = loc['name']
        weather_df['location_latitude'] = loc['latitude']
        weather_df['location_longitude'] = loc['longitude']
        all_weather_data.append(weather_df)

print(f"Đã gán thời tiết cho {len(all_weather_data)}/{len(df_locations)} địa điểm")

# Gộp và lưu file
if all_weather_data:
    final_df = pd.concat(all_weather_data, ignore_index=True)
    output_file = "thoi_tiet_danlam_thangcanh_2018.csv"
    final_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\nHoàn tất! Đã lưu dữ liệu thời tiết cho {len(df_locations)} địa điểm vào file: {output_file}")
    print("Cột trong file: date, temp_avg, rainfall, name, province, latitude, longitude")
    print(final_df.head())  # Xem mẫu 5 dòng đầu
else:
    print("Không lấy được dữ liệu nào.")