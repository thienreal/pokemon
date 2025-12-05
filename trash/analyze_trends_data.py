#!/usr/bin/env python3
"""Analyze Google Trends Data
============================
Phân tích dữ liệu đã fetch từ dest_trends_raw/

Input:
  - dest_trends_raw/dest_group_*.csv

Output:
  - destination_monthly_trends.csv: long-format (destination, date, year_month, interest)
  - destination_summary_stats.csv: avg, peak, min, seasonality
"""

import os
import sys
import logging
from dataclasses import dataclass
from typing import List, Dict
from glob import glob

import pandas as pd

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

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


class TrendsDataAnalyzer:
    def __init__(self, raw_dir: str = RAW_DIR, keywords_file: str = None, 
                 anchor_keyword: str = "Rau má"):
        self.raw_dir = raw_dir
        self.keywords_file = keywords_file
        self.anchor_keyword = anchor_keyword
        self.weekly_cache: Dict[str, pd.DataFrame] = {}
        self.duplicate_keywords: Dict[str, List[str]] = {}
        self.anchor_data: pd.DataFrame = None

    def check_duplicates_from_mapping(self):
        """Kiểm tra duplicate keywords từ keyword_mapping.csv"""
        if not self.keywords_file or not os.path.exists(self.keywords_file):
            return
        
        import re
        
        def sanitize_keyword(kw):
            k = kw.strip()
            k = re.sub(r'\([^\)]*\)', '', k).strip()
            k = re.sub(r'[\s,.;]+$', '', k)
            k = re.sub(r'\s+', ' ', k)
            if len(k) > 60:
                k = k[:60].strip()
            return k
        
        try:
            km = pd.read_csv(self.keywords_file)
            originals = km['original_name'].tolist()
            normalized = km['normalized_name'].dropna().astype(str).str.strip().tolist()
            
            # Áp dụng sanitize như trong fetch
            sanitized = [sanitize_keyword(n) for n in normalized if len(n) > 2]
            
            # Tìm duplicates
            seen = {}
            for i, (orig, san) in enumerate(zip(originals, sanitized)):
                if san in seen:
                    seen[san].append(orig)
                else:
                    seen[san] = [orig]
            
            self.duplicate_keywords = {k: v for k, v in seen.items() if len(v) > 1}
            
            if self.duplicate_keywords:
                total_lost = sum(len(v) - 1 for v in self.duplicate_keywords.values())
                logging.warning(f"\n⚠️  Found {len(self.duplicate_keywords)} duplicate keywords after sanitization")
                logging.warning(f"   Total destinations lost due to merge: {total_lost}")
                logging.warning(f"   Original keywords: {len(originals)}, After dedup: {len(originals) - total_lost}\n")
                
                for kw, origs in sorted(self.duplicate_keywords.items(), key=lambda x: -len(x[1]))[:20]:
                    logging.warning(f"   '{kw}' ← merged from {len(origs)} sources:")
                    for orig in origs[:5]:
                        logging.warning(f"      • {orig}")
                    if len(origs) > 5:
                        logging.warning(f"      ... and {len(origs) - 5} more")
                
                if len(self.duplicate_keywords) > 20:
                    logging.warning(f"   ... and {len(self.duplicate_keywords) - 20} more duplicate groups\n")
                else:
                    logging.warning("")
        except Exception as e:
            logging.warning(f"Cannot check duplicates: {e}")

    def load_raw_data(self):
        """Load tất cả raw CSV từ dest_trends_raw/"""
        pattern = os.path.join(self.raw_dir, 'dest_group_*.csv')
        files = sorted(glob(pattern))
        
        if not files:
            logging.error(f"No raw data files found in {self.raw_dir}")
            sys.exit(1)
        
        # Kiểm tra duplicates từ keyword mapping trước
        self.check_duplicates_from_mapping()
        
        logging.info(f"Loading {len(files)} raw data files...")
        
        for fpath in files:
            try:
                df = pd.read_csv(fpath, parse_dates=['date'])
                if df.empty:
                    continue
                
                for col in df.columns:
                    if col == 'date':
                        continue
                    self.weekly_cache[col] = df[['date', col]].rename(columns={col: 'interest'})
            except Exception as e:
                logging.warning(f"Error loading {fpath}: {e}")
        
        logging.info(f"✅ Loaded data for {len(self.weekly_cache)} destinations")
        
        # Extract anchor data if exists
        if self.anchor_keyword in self.weekly_cache:
            self.anchor_data = self.weekly_cache[self.anchor_keyword].copy()
            logging.info(f"✓ Found anchor data: '{self.anchor_keyword}'")

    def normalize_with_anchor(self, normalize: bool = True):
        """Normalize tất cả destinations với anchor"""
        if not normalize or self.anchor_data is None:
            logging.info("Skipping anchor normalization")
            return
        
        if self.anchor_keyword not in self.weekly_cache:
            logging.warning(f"⚠️  Anchor '{self.anchor_keyword}' not found in data")
            return
        
        logging.info(f"Normalizing {len(self.weekly_cache)} destinations with anchor '{self.anchor_keyword}'...")
        
        # Merge anchor data vào từng destination
        normalized_count = 0
        for dest, df in self.weekly_cache.items():
            if dest == self.anchor_keyword:
                continue
            
            # Merge với anchor
            merged = df.merge(self.anchor_data, on='date', how='left', suffixes=('', '_anchor'))
            
            # Normalize: (interest / anchor_interest) * 100
            merged['interest_normalized'] = (merged['interest'] / merged['interest_anchor'] * 100).fillna(0)
            
            # Thay thế interest bằng normalized value
            self.weekly_cache[dest]['interest'] = merged['interest_normalized']
            normalized_count += 1
        
        logging.info(f"✅ Normalized {normalized_count} destinations with anchor")

    def to_monthly(self, normalize: bool = False) -> pd.DataFrame:
        """Convert weekly/daily data to monthly averages"""
        all_frames = []
        
        for dest, df in self.weekly_cache.items():
            d = df.copy()
            d['date'] = pd.to_datetime(d['date'])
            d['interest'] = d['interest'].fillna(0)
            
            # Resample to monthly (use 'ME' for month end instead of deprecated 'M')
            d = d.set_index('date').resample('ME').mean().reset_index()
            d['destination'] = dest
            d['year_month'] = d['date'].dt.to_period('M').astype(str)
            all_frames.append(d[['destination', 'date', 'year_month', 'interest']])
        
        if not all_frames:
            logging.warning("No data to aggregate")
            return pd.DataFrame()
        
        monthly = pd.concat(all_frames, ignore_index=True)
        monthly['interest'] = monthly['interest'].fillna(0)
        monthly = monthly.sort_values(['destination', 'date']).reset_index(drop=True)
        
        monthly.to_csv('destination_monthly_trends.csv', index=False, encoding='utf-8-sig')
        logging.info("💾 Saved destination_monthly_trends.csv")
        return monthly

    def calculate_stats(self, monthly_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate summary statistics"""
        stats: List[DestStats] = []
        
        for dest in monthly_df['destination'].unique():
            sub = monthly_df[monthly_df['destination'] == dest].copy()
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

    def print_summary(self, stats_df: pd.DataFrame, n: int = 20):
        """Print top destinations"""
        print("\n" + "="*95)
        print(f"TOP {n} ĐIỂM DU LỊCH ĐƯỢC TÌM KIẾM NHIỀU NHẤT")
        print("="*95)
        print(f"{'#':<4} {'Điểm du lịch':<50} {'Avg':<8} {'Peak':<8} {'Peak Month':<12} {'Season':<8}")
        print("-"*95)
        
        for i, row in stats_df.head(n).reset_index(drop=True).iterrows():
            name = row['destination'][:48]
            print(f"{i+1:<4} {name:<50} {row['avg_interest']:<8.2f} {row['peak_interest']:<8} "
                  f"{row['peak_month']:<12} {row['seasonality']:<8.3f}")
        
        print("="*95)
        
        # High seasonality
        print(f"\nTOP 10 ĐIỂM CÓ BIẾN ĐỘNG MÙA VỤ MẠNH (Seasonality > 2)")
        print("-"*95)
        seasonal = stats_df[stats_df['seasonality'] > 2].head(10)
        for _, row in seasonal.iterrows():
            print(f"  {row['destination'][:45]:<45} Season: {row['seasonality']:.2f} "
                  f"(Peak: {row['peak_month']}, Min: {row['min_month']})")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Analyze Google Trends data')
    parser.add_argument('--keywords-file', default='keyword_mapping.csv', help='Keyword mapping file to check duplicates')
    parser.add_argument('--anchor', default='Rau má', help='Anchor keyword for normalization (bất kỳ từ khóa phổ biến nào)')
    parser.add_argument('--normalize', action='store_true', help='Normalize interest values with anchor')
    args = parser.parse_args()
    
    analyzer = TrendsDataAnalyzer(keywords_file=args.keywords_file, anchor_keyword=args.anchor)
    analyzer.load_raw_data()
    
    # Normalize if requested
    if args.normalize:
        analyzer.normalize_with_anchor(normalize=True)
    
    if not analyzer.weekly_cache:
        logging.error("No data loaded")
        sys.exit(1)
    
    monthly = analyzer.to_monthly(normalize=args.normalize)
    stats = analyzer.calculate_stats(monthly)
    analyzer.print_summary(stats, n=20)
    
    print("\n✅ Analysis complete!")
    if args.normalize:
        print(f"   - Normalized with anchor: {args.anchor}")
    print("   - Monthly trends: destination_monthly_trends.csv")
    print("   - Summary stats: destination_summary_stats.csv")


if __name__ == '__main__':
    main()
