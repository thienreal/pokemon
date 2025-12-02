#!/usr/bin/env python3
"""Fetch Google Trends Data for Vietnam Tourism Destinations
===========================================================
Lấy dữ liệu Google Trends và lưu vào dest_trends_raw/

Input:
  - keyword_mapping.csv (cột normalized_name)

Output:
  - dest_trends_raw/dest_group_*.csv: Raw data từng group
"""

import os
import sys
import time
import random
import logging
import re
from typing import List, Dict

import pandas as pd
from pytrends.request import TrendReq

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

DEFAULT_SOURCE = '../tourism_destinations_province_only_fixed.csv'
DEFAULT_TIMEFRAME = 'today 12-m'
RAW_DIR = 'dest_trends_raw'


class TrendsDataFetcher:
    def __init__(self, source_csv: str = DEFAULT_SOURCE, timeframe: str = DEFAULT_TIMEFRAME, 
                 anchor_keyword: str = "Rau má"):
        """timeframe có thể là preset (today 12-m, today 5-y) hoặc 'YYYY-MM-DD YYYY-MM-DD'"""
        self.source_csv = source_csv
        self.timeframe = timeframe
        self.anchor_keyword = anchor_keyword
        # Tạo session với timeout dài hơn (không dùng retries vì urllib3 incompatible)
        self.pytrends = TrendReq(hl='vi', tz=420, timeout=(10, 30))
        os.makedirs(RAW_DIR, exist_ok=True)
        self.destinations: List[str] = []
        self.anchor_values: Dict[str, float] = {}  # Cache anchor values by date
        self.request_count = 0  # Đếm số requests đã gửi

    def sanitize_keyword(self, kw: str) -> str:
        """Sanitize nhẹ nhàng hơn để giữ lại tên tỉnh/thành phố phân biệt duplicates"""
        k = kw.strip()
        # Loại ký tự lạ ở cuối
        k = re.sub(r'[\s,.;]+$', '', k)
        # Rút gọn khoảng trắng
        k = re.sub(r'\s+', ' ', k)
        # Chỉ cắt ngắn nếu QUÁ dài (>100 ký tự)
        if len(k) > 100:
            k = k[:100].strip()
        return k

    def load_destinations(self, keywords_file: str | None = None, delimiter: str = ',') -> List[str]:
        """Load danh sách từ khóa. Nếu có keywords_file, dùng cột normalized_name."""
        if keywords_file:
            if not os.path.exists(keywords_file):
                logging.error(f"File {keywords_file} không tồn tại")
                sys.exit(1)
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
        
        names = [self.sanitize_keyword(n) for n in names if len(n) > 2]
        self.destinations = list(dict.fromkeys(names))
        logging.info(f"Loaded {len(self.destinations)} destinations")
        return self.destinations

    def _group_list(self, items: List[str], n: int) -> List[List[str]]:
        return [items[i:i+n] for i in range(0, len(items), n)]

    def fetch_group(self, group: List[str], idx: int, retry_delay: float = 6.0, resume: bool = True, 
                    use_anchor: bool = True) -> pd.DataFrame:
        """Fetch một group với anchor normalization: max 3 lần, mỗi lần retry_delay giây"""
        path = os.path.join(RAW_DIR, f"dest_group_{idx:03d}.csv")
        
        # Thêm anchor vào group nếu chưa có
        if use_anchor and self.anchor_keyword not in group:
            group = [self.anchor_keyword] + group
        
        if resume and os.path.exists(path):
            try:
                df = pd.read_csv(path, parse_dates=['date'])
                logging.info(f"[Group {idx}] RESUME: {path}")
                return df
            except Exception as e:
                logging.warning(f"[Group {idx}] Resume failed: {e}")
        
        logging.info(f"[Group {idx}] FETCH {len(group)} items -> {', '.join(group[:3])}...")
        
        # Tạo session mới mỗi 10 requests để reset cookies
        if self.request_count > 0 and self.request_count % 10 == 0:
            logging.info(f"  🔄 Recreating session (request #{self.request_count})...")
            self.pytrends = TrendReq(hl='vi', tz=420, timeout=(10, 30))
            time.sleep(random.uniform(3, 5))
        
        max_retries = 3
        attempt = 0
        
        while attempt < max_retries:
            attempt += 1
            try:
                # Random delay trước khi fetch để tránh pattern detection
                if attempt > 1:
                    jitter = random.uniform(1.0, 3.0)
                    wait_time = retry_delay + jitter
                    logging.info(f"  ⏳ Waiting {wait_time:.1f}s before retry {attempt}/{max_retries}...")
                    time.sleep(wait_time)
                
                # Thêm delay nhỏ trước mỗi request
                time.sleep(random.uniform(1, 2))
                
                self.pytrends.build_payload(group, cat=0, timeframe=self.timeframe, geo='VN', gprop='')
                self.request_count += 1
                df = self.pytrends.interest_over_time()
                
                if df.empty:
                    logging.warning(f"  ⚠️ Empty (attempt {attempt})")
                    time.sleep(3)
                    continue
                
                if 'isPartial' in df.columns:
                    df = df.drop(columns=['isPartial'])
                
                df = df.reset_index()
                
                # Lưu anchor values để normalize sau
                if use_anchor and self.anchor_keyword in df.columns:
                    for _, row in df.iterrows():
                        date_key = row['date'].strftime('%Y-%m-%d')
                        self.anchor_values[date_key] = row[self.anchor_keyword]
                
                df.to_csv(path, index=False, encoding='utf-8-sig')
                logging.info(f"  ✅ Saved {path} ({len(df)} rows, total requests: {self.request_count})")
                return df
                
            except Exception as e:
                msg = str(e)
                if '429' in msg or 'Too Many Requests' in msg:
                    backoff = retry_delay * (2 ** (attempt - 1))  # Exponential backoff
                    logging.warning(f"  🚫 429 Too Many Requests (attempt {attempt}/{max_retries})")
                    
                    if attempt >= max_retries:
                        logging.error(f"  ❌ Max retries reached. Recreating session and waiting longer...")
                        # Tạo session mới và chờ lâu hơn
                        self.pytrends = TrendReq(hl='vi', tz=420, timeout=(10, 30))
                        extra_wait = random.uniform(30, 45)
                        logging.warning(f"  ⏳ Cooling down: waiting {extra_wait:.1f}s...")
                        time.sleep(extra_wait)
                        break
                    
                    logging.warning(f"  ⏳ Exponential backoff: waiting {backoff:.1f}s...")
                    time.sleep(backoff)
                    
                    # Tạo session mới sau mỗi lần gặp 429
                    logging.info(f"  🔄 Recreating session after 429...")
                    self.pytrends = TrendReq(hl='vi', tz=420, timeout=(10, 30))
                    continue
                else:
                    logging.error(f"  ❌ Error: {e}")
                    break
        
        # Không fallback - trả về None để skip group này
        logging.error(f"  ❌ Group {idx} failed after {max_retries} attempts - SKIPPING")
        return None

    def fetch_all(self, batch_size: int = 5, group_delay: float = 4.0, retry_delay: float = 6.0, 
                  resume: bool = True, use_anchor: bool = True, start_group: int = 0, end_group: int = None):
        """Fetch tất cả groups với anchor normalization
        
        Note: Anchor keyword không cần phải có trong danh sách destinations.
        Nó chỉ cần là một từ khóa phổ biến để Google Trends có thể lấy được data.
        """
        all_groups = self._group_list(self.destinations, batch_size)
        
        # Giảm batch size xuống 4 để chừa chỗ cho anchor
        if use_anchor:
            batch_size = min(batch_size, 4)
            all_groups = self._group_list(self.destinations, batch_size)
            logging.info(f"Using anchor: '{self.anchor_keyword}' (batch size reduced to {batch_size})")
            logging.info(f"Note: Anchor không cần phải có trong destinations list")
        
        # Filter groups theo start/end
        groups = all_groups[start_group:end_group]
        group_offset = start_group
        
        logging.info(f"📦 Processing {len(self.destinations)} destinations")
        logging.info(f"   Groups {start_group+1} to {(end_group or len(all_groups))} (total: {len(groups)} groups)")
        
        success_count = 0
        failed_groups = []
        
        # Thêm initial delay để đảm bảo không bị rate limit từ đầu
        if start_group == 0:
            initial_wait = random.uniform(3, 5)
            logging.info(f"Initial cooldown: waiting {initial_wait:.1f}s before starting...")
            time.sleep(initial_wait)
        
        for i, group in enumerate(groups, 1):
            actual_group_num = group_offset + i
            df = self.fetch_group(group, actual_group_num, retry_delay=retry_delay, resume=resume, use_anchor=use_anchor)
            
            if df is not None and not df.empty:
                success_count += 1
            else:
                keywords_in_group = [k for k in group if k != self.anchor_keyword]
                failed_groups.append({
                    'group_num': actual_group_num,
                    'keywords': keywords_in_group
                })
            
            if i < len(groups):
                # Random jitter để tránh pattern detection
                jitter = random.uniform(0, min(2.0, group_delay * 0.5))
                total_wait = group_delay + jitter
                logging.info(f"  ⏲️ Wait {total_wait:.1f}s...")
                time.sleep(total_wait)
        
        # Summary
        total_groups = len(groups)
        logging.info(f"\n{'='*60}")
        logging.info(f"✅ Fetching complete: {success_count}/{total_groups} groups successful")
        
        if use_anchor:
            logging.info(f"   Collected {len(self.anchor_values)} anchor values for normalization")
        
        if failed_groups:
            logging.warning(f"\n⚠️  {len(failed_groups)} groups FAILED - need to retry:")
            for fg in failed_groups:
                keywords_str = ', '.join(fg['keywords'][:3])
                if len(fg['keywords']) > 3:
                    keywords_str += f" ... (+{len(fg['keywords'])-3} more)"
                logging.warning(f"   - Group {fg['group_num']}: {keywords_str}")
            
            # Lưu danh sách failed groups
            failed_file = os.path.join(RAW_DIR, "failed_groups.txt")
            with open(failed_file, 'w', encoding='utf-8') as f:
                f.write(f"Failed groups: {len(failed_groups)}/{total_groups}\n")
                f.write(f"Date: {pd.Timestamp.now()}\n\n")
                for fg in failed_groups:
                    f.write(f"Group {fg['group_num']}:\n")
                    for kw in fg['keywords']:
                        f.write(f"  - {kw}\n")
                    f.write("\n")
            logging.info(f"\n💾 Failed groups saved to: {failed_file}")
            logging.info(f"   → Re-run with --start-group and --end-group to retry specific groups")
        
        logging.info(f"{'='*60}")
        return success_count


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Fetch Google Trends data for destinations')
    parser.add_argument('--source-csv', default=DEFAULT_SOURCE, help='CSV source')
    parser.add_argument('--timeframe', default=None, help='Preset timeframe')
    parser.add_argument('--start-date', help='Custom start YYYY-MM-DD')
    parser.add_argument('--end-date', help='Custom end YYYY-MM-DD')
    parser.add_argument('--batch-size', type=int, default=5, help='Keywords per request (will be 4 with anchor)')
    parser.add_argument('--group-delay', type=float, default=10.0, help='Seconds between groups (default: 10, khuyên dùng >= 10 để tránh 429)')
    parser.add_argument('--retry-delay', type=float, default=15.0, help='Initial delay khi retry sau 429 (default: 15, sẽ tăng exponential)')
    parser.add_argument('--anchor', default='Rau má', help='Anchor keyword for normalization (bất kỳ từ khóa phổ biến nào)')
    parser.add_argument('--no-anchor', action='store_true', help='Disable anchor normalization')
    parser.add_argument('--no-resume', action='store_true', help='Force refetch')
    parser.add_argument('--keywords-file', help='CSV with normalized_name')
    parser.add_argument('--delimiter', default=',', help='CSV delimiter')
    parser.add_argument('--start-group', type=int, help='Start from group number (1-based)')
    parser.add_argument('--end-group', type=int, help='End at group number (inclusive)')
    args = parser.parse_args()

    if args.start_date and args.end_date:
        timeframe = f"{args.start_date} {args.end_date}"
    elif args.timeframe:
        timeframe = args.timeframe
    else:
        timeframe = DEFAULT_TIMEFRAME

    fetcher = TrendsDataFetcher(source_csv=args.source_csv, timeframe=timeframe, 
                                anchor_keyword=args.anchor)
    fetcher.load_destinations(keywords_file=args.keywords_file, delimiter=args.delimiter)
    
    # Convert to 0-based index
    start_idx = (args.start_group - 1) if args.start_group else 0
    end_idx = args.end_group if args.end_group else None
    
    fetcher.fetch_all(batch_size=args.batch_size, group_delay=args.group_delay, 
                      retry_delay=args.retry_delay, resume=not args.no_resume,
                      use_anchor=not args.no_anchor, start_group=start_idx, end_group=end_idx)

    print(f"\n✅ Fetch complete! Timeframe: {timeframe}")
    print(f"   - Anchor: {args.anchor if not args.no_anchor else 'None (disabled)'}")
    print(f"   - Raw data: {RAW_DIR}/*.csv")
    print(f"\nNext: Run analyze_trends_data.py --normalize to normalize with anchor")


if __name__ == '__main__':
    main()
