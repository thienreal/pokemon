#!/usr/bin/env python3
"""Vietnam Tourism Destinations - Monthly Google Trends
======================================================
Phân tích lượng tìm kiếm theo THÁNG cho từng điểm du lịch (975 điểm).

Input:
  - ../tourism_destinations_province_only_fixed.csv (cột name, address)

Output:
  - dest_trends_raw/: cache theo nhóm (CSV theo tuần/ngày từ Google Trends)
  - destination_monthly_trends.csv: long-format (destination, year_month, interest)
  - destination_summary_stats.csv: avg, peak, min, seasonality

Lưu ý:
  - Google Trends trả dữ liệu theo tuần (12m) hoặc theo tháng (5y). Script sẽ tự quy đổi ra tháng.
  - Batching: tối đa 5 keywords mỗi request. Dùng delay và retry khi 429.
"""

import os
import sys
import time
import random
import math
import logging
import re
from dataclasses import dataclass
from typing import List, Dict

import pandas as pd
from pytrends.request import TrendReq

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

DEFAULT_SOURCE = '../tourism_destinations_province_only_fixed.csv'
DEFAULT_TIMEFRAME = 'today 12-m'  # monthly sẽ aggregate từ weekly
RAW_DIR = 'dest_trends_raw'

@dataclass
class DestStats:
    destination: str
    avg_interest: float
    peak_interest: int
    peak_month: str
    min_interest: int
    min_month: str
    seasonality: float


class DestinationMonthlyTrends:
    def __init__(self, source_csv: str = DEFAULT_SOURCE, timeframe: str = DEFAULT_TIMEFRAME):
        """timeframe có thể là preset (today 12-m, today 5-y) hoặc 'YYYY-MM-DD YYYY-MM-DD' cho >5 năm"""
        self.source_csv = source_csv
        self.timeframe = timeframe
        self.pytrends = TrendReq(hl='vi', tz=420, timeout=(10, 25))
        os.makedirs(RAW_DIR, exist_ok=True)
        self.destinations: List[str] = []
        self.weekly_cache: Dict[str, pd.DataFrame] = {}

    def sanitize_keyword(self, kw: str) -> str:
        k = kw.strip()
        # Loại phần ngoặc để tránh query quá dài
        k = re.sub(r'\([^\)]*\)', '', k).strip()
        # Loại ký tự lạ ở cuối
        k = re.sub(r'[\s,.;]+$', '', k)
        # Rút gọn khoảng trắng
        k = re.sub(r'\s+', ' ', k)
        # Nếu quá dài, cắt ngắn về 60 ký tự
        if len(k) > 60:
            k = k[:60].strip()
        return k

    def load_destinations(self, keywords_file: str | None = None, delimiter: str = ',') -> List[str]:
        """Load danh sách từ khóa đích. Nếu có keywords_file, dùng cột normalized_name."""
        if keywords_file:
            if not os.path.exists(keywords_file):
                logging.error(f"File {keywords_file} không tồn tại")
                sys.exit(1)
            # keywords_file luôn dùng delimiter ',' (output của normalizer)
            dfk = pd.read_csv(keywords_file)
            col = None
            for c in ('normalized_name', 'verified_keyword', 'keyword'):
                if c in dfk.columns:
                    col = c
                    break
            if not col:
                logging.error("keywords_file cần có cột normalized_name")
                sys.exit(1)
            names = dfk[col].dropna().astype(str).str.strip().tolist()
        else:
            if not os.path.exists(self.source_csv):
                logging.error(f"File {self.source_csv} không tồn tại")
                sys.exit(1)
            df = pd.read_csv(self.source_csv, delimiter=delimiter)
            if 'name' not in df.columns:
                logging.error("CSV cần có cột 'name'")
                sys.exit(1)
            names = df['name'].dropna().astype(str).str.strip().tolist()
        # Loại bỏ tên quá ngắn
        names = [self.sanitize_keyword(n) for n in names if len(n) > 2]
        # Loại bỏ trùng sau sanitize
        self.destinations = list(dict.fromkeys(names))
        logging.info(f"Loaded {len(self.destinations)} destinations")
        return self.destinations

    def _group_list(self, items: List[str], n: int) -> List[List[str]]:
        return [items[i:i+n] for i in range(0, len(items), n)]

    def fetch_group(self, group: List[str], idx: int, max_retries: int = 3, base_sleep: int = 5, resume: bool = True) -> pd.DataFrame:
        path = os.path.join(RAW_DIR, f"dest_group_{idx:03d}.csv")
        if resume and os.path.exists(path):
            try:
                df = pd.read_csv(path, parse_dates=['date'])
                logging.info(f"[Group {idx}] RESUME: {path}")
                return df
            except Exception as e:
                logging.warning(f"[Group {idx}] Resume failed: {e}")
        logging.info(f"[Group {idx}] FETCH {len(group)} items -> {', '.join(group[:5])}")
        attempt = 0
        while attempt <= max_retries:
            attempt += 1
            try:
                self.pytrends.build_payload(group, cat=0, timeframe=self.timeframe, geo='VN', gprop='')
                df = self.pytrends.interest_over_time()
                if df.empty:
                    logging.warning(f"  ⚠️ Empty (attempt {attempt})")
                    time.sleep(2)
                    continue
                if 'isPartial' in df.columns:
                    df = df.drop(columns=['isPartial'])
                df = df.reset_index()
                df.to_csv(path, index=False, encoding='utf-8-sig')
                logging.info(f"  💾 Saved {path} ({len(df)} rows)")
                return df
            except Exception as e:
                msg = str(e)
                if '429' in msg:
                    sleep_time = base_sleep * (2 ** (attempt - 1)) + random.randint(0, 3)
                    logging.warning(f"  ⏳ 429, retry {attempt}/{max_retries}, sleep {sleep_time}s")
                    time.sleep(sleep_time)
                else:
                    logging.error(f"  ❌ Error: {e}")
                    if attempt < max_retries:
                        time.sleep(base_sleep)
                    else:
                        break
        logging.warning(f"  ⚠️ Group {idx} failed. Falling back to per-keyword fetch...")
        # Fallback: fetch each keyword separately and merge on date
        per_frames = []
        for kw in group:
            try:
                self.pytrends.build_payload([kw], cat=0, timeframe=self.timeframe, geo='VN', gprop='')
                sub = self.pytrends.interest_over_time()
                if sub.empty:
                    logging.warning(f"    ▪️ Empty for '{kw}', skipping")
                    continue
                if 'isPartial' in sub.columns:
                    sub = sub.drop(columns=['isPartial'])
                sub = sub.reset_index()[['date', kw]]
                per_frames.append(sub)
                time.sleep(1)
            except Exception as e:
                logging.warning(f"    ▪️ Error keyword '{kw}': {e}")
                time.sleep(1)
        if not per_frames:
            logging.error(f"  ❌ Failed group {idx} with no fallback data, creating zero-data frame")
            # Tạo DataFrame với tất cả keywords trong group = 0
            # Dùng date range từ timeframe
            try:
                # Thử lấy date range từ một keyword bất kỳ thành công
                test_kw = 'Hà Nội'  # keyword phổ biến để lấy date range
                self.pytrends.build_payload([test_kw], cat=0, timeframe=self.timeframe, geo='VN', gprop='')
                ref = self.pytrends.interest_over_time()
                if not ref.empty:
                    dates = ref.reset_index()['date']
                    zero_df = pd.DataFrame({'date': dates})
                    for kw in group:
                        zero_df[kw] = 0
                    zero_df.to_csv(path, index=False, encoding='utf-8-sig')
                    logging.info(f"  💾 Saved zero-data group {idx} ({len(zero_df)} rows)")
                    return zero_df
            except Exception:
                pass
            return pd.DataFrame()
        # Merge all per-keyword frames on date
        merged = per_frames[0]
        for f in per_frames[1:]:
            merged = merged.merge(f, on='date', how='outer')
        # Fill NaN với 0 cho các keyword không có data
        merged = merged.fillna(0)
        merged.to_csv(path, index=False, encoding='utf-8-sig')
        logging.info(f"  💾 Saved fallback group {idx} ({len(merged)} rows)")
        return merged

    def collect_all(self, batch_size: int = 5, group_delay: int = 4, max_retries: int = 3, resume: bool = True):
        groups = self._group_list(self.destinations, batch_size)
        for i, g in enumerate(groups, start=1):
            df = self.fetch_group(g, i, max_retries=max_retries, resume=resume)
            if not df.empty:
                for col in df.columns:
                    if col == 'date':
                        continue
                    # Lưu cache weekly cho từng destination trong group
                    self.weekly_cache[col] = df[['date', col]].rename(columns={col: 'interest'})
            if i < len(groups):
                logging.info(f"  ⏲️ Wait {group_delay}s before next group...")
                time.sleep(group_delay)
        logging.info(f"✅ Collected weekly data for {len(self.weekly_cache)} destinations")

    def to_monthly(self) -> pd.DataFrame:
        """Chuyển dữ liệu weekly/daily thành monthly bằng cách lấy trung bình theo tháng."""
        all_frames = []
        for dest, df in self.weekly_cache.items():
            d = df.copy()
            d['date'] = pd.to_datetime(d['date'])
            # Fill NaN với 0 cho các destination không có data
            d['interest'] = d['interest'].fillna(0)
            # Resample theo tháng: mean interest
            d = d.set_index('date').resample('M').mean().reset_index()
            d['destination'] = dest
            d['year_month'] = d['date'].dt.to_period('M').astype(str)
            all_frames.append(d[['destination', 'date', 'year_month', 'interest']])
        if not all_frames:
            logging.warning("No data to convert to monthly")
            return pd.DataFrame()
        monthly = pd.concat(all_frames, ignore_index=True)
        # Fill NaN cuối cùng với 0
        monthly['interest'] = monthly['interest'].fillna(0)
        monthly = monthly.sort_values(['destination', 'date']).reset_index(drop=True)
        monthly.to_csv('destination_monthly_trends.csv', index=False, encoding='utf-8-sig')
        logging.info("💾 Saved destination_monthly_trends.csv")
        return monthly

    def summary_stats(self, monthly_df: pd.DataFrame) -> pd.DataFrame:
        stats: List[DestStats] = []
        for dest in monthly_df['destination'].unique():
            sub = monthly_df[monthly_df['destination'] == dest].copy()
            # Fill NaN với 0
            sub['interest'] = sub['interest'].fillna(0)
            if sub.empty:
                continue
            avg = sub['interest'].mean()
            peak = int(sub['interest'].max())
            row_peak = sub.loc[sub['interest'].idxmax()]
            minv = int(sub['interest'].min())
            row_min = sub.loc[sub['interest'].idxmin()]
            season = peak / avg if avg > 0 else 0.0
            stats.append(DestStats(
                destination=dest,
                avg_interest=round(avg, 2),
                peak_interest=peak,
                peak_month=row_peak['year_month'],
                min_interest=minv,
                min_month=row_min['year_month'],
                seasonality=round(season, 3)
            ))
        df = pd.DataFrame([s.__dict__ for s in stats]).sort_values('avg_interest', ascending=False)
        df.to_csv('destination_summary_stats.csv', index=False, encoding='utf-8-sig')
        logging.info("💾 Saved destination_summary_stats.csv")
        return df

    def print_preview(self, stats_df: pd.DataFrame, n: int = 15):
        print("\n" + "="*90)
        print(f"TOP {n} ĐIỂM DU LỊCH ĐƯỢC TÌM KIẾM THEO THÁNG (Avg Interest)")
        print("="*90)
        print(f"{'#':<4} {'Điểm du lịch':<50} {'Avg':<8} {'Peak':<8} {'Peak Month':<12} {'Season':<8}")
        print("-"*90)
        for i, row in stats_df.head(n).reset_index(drop=True).iterrows():
            name = row['destination'][:48]
            print(f"{i+1:<4} {name:<50} {row['avg_interest']:<8.2f} {row['peak_interest']:<8} {row['peak_month']:<12} {row['seasonality']:<8.3f}")
        print("="*90)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Monthly Google Trends for Vietnam tourism destinations')
    parser.add_argument('--source-csv', default=DEFAULT_SOURCE, help='CSV containing name,address')
    parser.add_argument('--timeframe', default=None, help='Preset timeframe (today 12-m, today 5-y, etc.)')
    parser.add_argument('--start-date', help='Custom start date YYYY-MM-DD for >5 năm')
    parser.add_argument('--end-date', help='Custom end date YYYY-MM-DD')
    parser.add_argument('--batch-size', type=int, default=5, help='Keywords per request (max 5)')
    parser.add_argument('--group-delay', type=int, default=4, help='Seconds delay between requests')
    parser.add_argument('--max-retries', type=int, default=3, help='Max retries for 429 or transient errors')
    parser.add_argument('--no-resume', action='store_true', help='Ignore cached raw data and refetch')
    parser.add_argument('--keywords-file', help='CSV chứa cột verified_keyword để dùng từ khóa tối ưu')
    parser.add_argument('--delimiter', default=',', help='Delimiter for input/keywords CSV (default ,; use ; for semicolon)')
    args = parser.parse_args()

    # Xác định timeframe cuối cùng
    if args.start_date and args.end_date:
        timeframe = f"{args.start_date} {args.end_date}"
    elif args.timeframe:
        timeframe = args.timeframe
    else:
        timeframe = DEFAULT_TIMEFRAME

    analyzer = DestinationMonthlyTrends(source_csv=args.source_csv, timeframe=timeframe)
    analyzer.load_destinations(keywords_file=args.keywords_file, delimiter=args.delimiter)
    analyzer.collect_all(batch_size=args.batch_size, group_delay=args.group_delay, max_retries=args.max_retries, resume=not args.no_resume)

    if not analyzer.weekly_cache:
        logging.error('No data collected')
        sys.exit(1)

    monthly = analyzer.to_monthly()
    stats = analyzer.summary_stats(monthly)
    analyzer.print_preview(stats, n=20)

    print("\n✅ Done!")
    print(f"   - Timeframe dùng: {timeframe}")
    print("   - Raw weekly: dest_trends_raw/*.csv")
    print("   - Monthly trends: destination_monthly_trends.csv")
    print("   - Summary: destination_summary_stats.csv")


if __name__ == '__main__':
    main()
