#!/usr/bin/env python3
"""Vietnam Tourism Destination Scraper (Rút gọn - Province Only)
-------------------------------------------------
Chức năng: Cào DUY NHẤT hai trường `name`, `address` (chỉ là TỈNH) từ
        https://csdl.vietnamtourism.gov.vn/dest/ qua phân trang `?page=N`.

Phạm vi tối giản:
 - Không lấy id, URL chi tiết hay số trang trong CSV.
- Chỉ xuất đúng hai cột: name,address (UTF-8 BOM để mở Excel) trong đó address = tên tỉnh cuối cùng trích từ địa chỉ gốc.
 - Dừng khi gặp trang trống hoặc đạt giới hạn --max-pages.

CLI:
    --output FILE       (mặc định tourism_destinations_names_addresses.csv)
    --max-pages N       (mặc định 65)
    --start-page N      (mặc định 1)
    --sleep SEC         (delay lịch sự giữa trang, mặc định 0.8)
    --user-agent STR    (tùy chỉnh UA)

Ghi chú: Trang render server-side nên dùng requests + BeautifulSoup đủ.
"""

from __future__ import annotations
import time
import csv
import argparse
import logging
import sys
from dataclasses import dataclass
from typing import List

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)

BASE_URL = "https://csdl.vietnamtourism.gov.vn/dest/"


@dataclass
class DestinationSimple:
    name: str
    address: str  # province only


class DestinationScraper:
    def __init__(self, max_pages: int = 65, start_page: int = 1, sleep: float = 0.8, user_agent: str = 'Mozilla/5.0'):
        self.max_pages = max_pages
        self.start_page = start_page
        self.sleep = sleep
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})

    def fetch_html(self, page: int) -> str:
        url = BASE_URL
        if page > 1:
            url = f"{BASE_URL}?page={page}"
        logging.debug(f"Fetch page {page} {url}")
        resp = self.session.get(url, timeout=25)
        resp.raise_for_status()
        return resp.text

    def extract_province(self, raw_address: str) -> str:
        """Tách tên tỉnh/thành phố từ chuỗi địa chỉ thô.
        Chiến lược:
          - Loại bỏ tiền tố 'Địa chỉ:' nếu còn.
          - Split theo dấu phẩy, lấy phần tử cuối cùng không rỗng.
          - Chuẩn hóa khoảng trắng, strip.
          - Trường hợp đặc biệt: nếu kết quả quá ngắn (<2 ký tự) trả về chuỗi gốc đã strip.
        """
        if not raw_address:
            return ''
        addr = raw_address.strip()
        if addr.lower().startswith('địa chỉ:'):
            addr = addr[len('Địa chỉ:'):].strip()
        parts = [p.strip() for p in addr.split(',') if p.strip()]
        if not parts:
            return ''
        province = parts[-1]
        # Một số phần có thể là mô tả dài thay vì tỉnh: giữ nguyên.
        return province

    def parse_page(self, html: str) -> List[DestinationSimple]:
        soup = BeautifulSoup(html, 'html.parser')
        out: List[DestinationSimple] = []
        for h4 in soup.find_all('h4'):
            a = h4.find('a')
            if not a:
                continue
            name = a.get_text(strip=True)
            raw_address = ''
            for sib in h4.next_siblings:
                if getattr(sib, 'name', None) == 'h4':
                    break
                if hasattr(sib, 'get_text') and 'map-marker' in str(sib):
                    txt = sib.get_text(' ', strip=True)
                    raw_address = txt.replace('\xa0', ' ').strip()
                    break
            if name:
                province = self.extract_province(raw_address)
                out.append(DestinationSimple(name=name, address=province))
        return out

    def scrape(self) -> List[DestinationSimple]:
        all_items: List[DestinationSimple] = []
        for page in range(self.start_page, self.max_pages + 1):
            try:
                html = self.fetch_html(page)
            except Exception as e:
                logging.warning(f"Page {page} fetch error: {e}")
                logging.info("Stopping due to fetch error.")
                break
            page_items = self.parse_page(html)
            if not page_items:
                logging.info(f"No items found on page {page}; stopping.")
                break
            logging.info(f"Page {page}: {len(page_items)} records")
            all_items.extend(page_items)
            time.sleep(self.sleep)
        logging.info(f"Total collected (name+address): {len(all_items)}")
        return all_items


def write_csv(items: List[DestinationSimple], path: str, delimiter: str=','):
    """Ghi CSV hai cột name,address (address = province).
    Xử lý:
      - Giữ nguyên province (không phẩy nên không cần quote).
      - name nếu chứa dấu phẩy hoặc dấu nháy sẽ được csv.writer tự động bao nháy kép chuẩn CSV.
      - Loại bỏ escape '\,' lỗi trước đây (do QUOTE_NONE + escapechar).
    """
    with open(path, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.writer(f, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        w.writerow(['name', 'address'])
        for d in items:
            name = d.name.replace('\r', ' ').replace('\n', ' ').strip()
            province = ' '.join(d.address.split())
            w.writerow([name, province])
    logging.info(f"Saved CSV (name,province, minimal quoting) -> {path}")


def main():
    parser = argparse.ArgumentParser(description='Scrape Vietnam tourism destinations (only name & address)')
    parser.add_argument('--output', default='tourism_destinations_names_addresses.csv', help='Output CSV path')
    parser.add_argument('--max-pages', type=int, default=65, help='Max pages to scan')
    parser.add_argument('--start-page', type=int, default=1, help='Start page number')
    parser.add_argument('--sleep', type=float, default=0.8, help='Sleep seconds between pages')
    parser.add_argument('--user-agent', default='Mozilla/5.0', help='Custom User-Agent header')
    parser.add_argument('--delimiter', default=',', help="Delimiter output: ',' hoặc '\t' nếu cần tab")
    args = parser.parse_args()

    scraper = DestinationScraper(
        max_pages=args.max_pages,
        start_page=args.start_page,
        sleep=args.sleep,
        user_agent=args.user_agent
    )
    items = scraper.scrape()
    delim = args.delimiter
    if delim in ('\\t', 'tab'):
        delim = '\t'
    if len(delim) != 1:
        logging.error(f"Delimiter '{delim}' không hợp lệ (phải 1 ký tự). Dùng '\t' hoặc ','.")
        sys.exit(1)
    write_csv(items, args.output, delimiter=delim)
    logging.info('Done.')


if __name__ == '__main__':
    main()
