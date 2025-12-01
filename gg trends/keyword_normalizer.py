#!/usr/bin/env python3
import re
import os
import sys
import argparse
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

PREFIX_PATTERNS = [
    r'^Khu\s+di\s+tích\s+lịch\s+sử\s+và\s+Danh\s+thắng\s+',
    r'^Di\s+tích\s+lịch\s+sử\s+quốc\s+gia\s+đặc\s+biệt\s+',
    r'^Khu\s+di\s+tích\s+lịch\s+sử\s+-\s+VVăn\s+hóa\s+',
    r'^Khu\s+du\s+lịch\s+và\s+vườn\s+Quốc\s+gia\s+',
    r'^Điểm\s+du\s+lịch\s+Di\s+tích\s+Lịch\s+sử\s+',
    r'^Di\s+tích\s+lịch\s+sử\s+-\s+văn\s+hóa\s+',
    r'^Du\s+lịch\s+Suối\s+khoáng\s+nóng\s+',
    r'^Khu\s+bảo\s+tồn\s+thiên\s+nhiên\s+',
    r'^Khu\s+du\s+lịch\s+sinh\s+thái\s+',
    r'^Khu\s+du\s+lich\s+sinh\s+thái\s+',
    r'^Khu\s+vui\s+chơi\s+giải\s+trí\s+',
    r'^Làng\s+nghề\s+truyền\s+thống\s+',
    r'^Quần\s+thể\s+khu\s+di\s+tích\s+',
    r'^Cụm\s+di\s+tích\s+lịch\s+sử\s+',
    r'^Trung\s+tâm\s+thương\s+mại\s+',
    r'^Trung\s+tâm\s+văn\s+hóa\s+',
    r'^Di\s+tích\s+lịch\s+sử\s+',
    r'^Khu\s+nghỉ\s+dưỡng\s+',
    r'^Khu\s+trung\s+tâm\s+',
    r'^Vườn\s+Quốc\s+gia\s+',
    r'^Khu\s+sinh\s+thái\s+',
    r'^Khu\s+lưu\s+niệm\s+',
    r'^Điểm\s+du\s+lịch\s+',
    r'^Khu\s+du\s+lịch\s+',
    r'^Khu\s+di\s+tích\s+',
    r'^Cụm\s+di\s+tích\s+',
    r'^Trung\s+tâm\s+',
    r'^Quán\s+Café\s+',
    r'^Quần\s+thể\s+',
    r'^Di\s+tích\s+',
]

def normalize_name(name: str) -> tuple[str, str | None]:
    s = name.strip()
    removed = None
    for pat in PREFIX_PATTERNS:
        m = re.match(pat, s, flags=re.IGNORECASE)
        if m:
            removed = m.group(0)
            s = re.sub(pat, '', s, flags=re.IGNORECASE).strip()
            break
    # Thu gọn dấu nối dài kiểu " - "
    s = re.sub(r'\s*-\s*', ' - ', s)
    # Loại bỏ phần trong ngoặc để rút gọn: ( ... )
    s = re.sub(r'\([^\)]*\)', '', s).strip()
    # Loại dấu phẩy/dấu chấm ở cuối
    s = re.sub(r'[\s,.;]+$', '', s)
    # Rút gọn khoảng trắng thừa
    s = re.sub(r'\s+', ' ', s).strip()
    # Giữ nguyên tiếng Việt có dấu, chỉ loại khoảng trắng thừa
    return s, removed

def main():
    ap = argparse.ArgumentParser(description='Normalize destination names and output mapping')
    ap.add_argument('--input', default='../tourism.csv', help='Source CSV with name,province')
    ap.add_argument('--delimiter', default=';', help='CSV delimiter (default ;)')
    ap.add_argument('--output', default='keyword_mapping.csv', help='Output mapping CSV')
    args = ap.parse_args()

    if not os.path.exists(args.input):
        logging.error(f'Input not found: {args.input}')
        sys.exit(1)

    df = pd.read_csv(args.input, delimiter=args.delimiter)
    if 'name' not in df.columns:
        logging.error("CSV must contain 'name' column")
        sys.exit(1)
    
    # Check if province column exists
    has_province = 'province' in df.columns

    # First pass: normalize all names
    rows = []
    for idx, row in df.iterrows():
        name_clean = str(row['name']).strip()
        if not name_clean:
            continue
        norm, removed = normalize_name(name_clean)
        province = str(row['province']).strip() if has_province and pd.notna(row.get('province')) else ''
        rows.append({
            'row_index': idx + 1,
            'original_name': name_clean,
            'normalized_name': norm,
            'province': province,
            'removed_prefix': removed or ''
        })
    
    # Second pass: detect duplicates and append province to normalized_name
    from collections import defaultdict
    norm_count = defaultdict(list)
    for r in rows:
        norm_count[r['normalized_name']].append(r)
    
    # Track duplicates before dedup
    original_count = len(rows)
    duplicates_resolved = 0
    exact_duplicates_removed = 0
    
    final_rows = []
    seen_norm_province = set()
    
    for norm, entries in norm_count.items():
        if len(entries) > 1:
            duplicates_resolved += 1
            # Append province to normalized_name for all duplicates
            for entry in entries:
                if entry['province']:
                    entry['normalized_name'] = f"{norm} {entry['province']}"
                
                # Check if this normalized_name + province combo already exists
                key = (entry['normalized_name'], entry['province'])
                if key not in seen_norm_province:
                    seen_norm_province.add(key)
                    final_rows.append(entry)
                    logging.info(f"  Kept: '{entry['original_name']}' → '{entry['normalized_name']}'")
                else:
                    exact_duplicates_removed += 1
                    logging.info(f"  Removed duplicate: '{entry['original_name']}' (same as existing)")
        else:
            # No duplicates, keep as is
            entry = entries[0]
            key = (entry['normalized_name'], entry['province'])
            if key not in seen_norm_province:
                seen_norm_province.add(key)
                final_rows.append(entry)
    
    if duplicates_resolved > 0:
        logging.warning(f"⚠️  Found {duplicates_resolved} duplicate keyword groups")
        logging.warning(f"   Removed {exact_duplicates_removed} exact duplicates (same name + province)")
        logging.warning(f"   Final count: {len(final_rows)} unique destinations (from {original_count})")
    
    rows = final_rows
    
    out = pd.DataFrame(rows)
    out.to_csv(args.output, index=False, encoding='utf-8-sig')
    logging.info(f'💾 Saved mapping -> {args.output} ({len(out)} rows)')

if __name__ == '__main__':
    main()
