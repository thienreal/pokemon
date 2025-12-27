````markdown
# YouTube hashtag fetcher

Fetch YouTube videos by hashtag and tag each row with a detected Vietnam province based on title/description text.

## Setup
```bash
pip install -r requirements.txt
```

## Run single hashtag
```bash
export YOUTUBE_API_KEY="<your_api_key>"
python hastag/youtube_hashtags.py --hashtag travel --max-results 100 --output youtube_travel.csv
```

- `--hashtag`: do not include the leading `#`.
- `--max-results`: how many videos to pull (quota dependent).
- `--output`: optional CSV path; defaults to `youtube_<hashtag>.csv`.
- Province detection uses `cacKhuVuc/cacKhuVucVietNam.csv` via `province_lookup.py`; it matches names/aliases in video title and description. Add more aliases in `province_lookup.py` if needed.

## Run per-province hashtag counts (all 63 tỉnh/thành)
```bash
export YOUTUBE_API_KEY="<your_api_key>"
python hastag/youtube_province_hashtags.py --max-results-per-province 50 --days 7 --output youtube_province_counts.csv --details-output youtube_province_videos.csv
```
- Mặc định tìm video trong **7 ngày gần đây**, ưu tiên nội dung Việt Nam (regionCode=VN, relevanceLanguage=vi).
- Query: `#<hashtag> <tên_tỉnh> vietnam` (ví dụ: `#hanoi Hà Nội vietnam`) để lọc chính xác hơn.
- `--days`: số ngày gần đây muốn lấy (mặc định 7). Ví dụ `--days 14` để lấy 2 tuần.
- `--max-results-per-province`: số video tối đa lấy cho mỗi tỉnh (giới hạn quota). Nếu chỉ cần đếm nhanh, giữ nhỏ (20-50).
- `--output`: CSV tổng hợp số lượng video mỗi tỉnh.
- `--details-output` (tùy chọn): CSV chi tiết với title, views, likes, comments...

````
