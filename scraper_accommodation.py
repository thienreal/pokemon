#!/usr/bin/env python3
"""Vietnam Tourism Accommodation Database Scraper
-------------------------------------------------
Chức năng: Cào dữ liệu cơ sở lưu trú từ https://csdl.vietnamtourism.gov.vn/cslt/
        bao gồm: tên, địa chỉ, loại hình, xếp hạng, liên hệ (nếu có)

Phạm vi:
 - Lấy tên, địa chỉ từ danh sách lưu trú
 - Tự động follow paginated links (?page=N)
 - Dừng khi gặp trang trống hoặc đạt giới hạn --max-pages
 - Xuất ra CSV với UTF-8 BOM

CLI:
    --output FILE       (mặc định accommodation.csv)
    --max-pages N       (mặc định không giới hạn, dừng khi hết dữ liệu)
    --start-page N      (mặc định 1)
    --sleep SEC         (delay giữa trang, mặc định 0.7)
    --user-agent STR    (tùy chỉnh User-Agent)

Ghi chú: Trang render server-side nên dùng requests + BeautifulSoup đủ.
"""

from __future__ import annotations
import time
import csv
import argparse
import logging
import sys
from dataclasses import dataclass
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)

BASE_URL = "https://csdl.vietnamtourism.gov.vn/index.php/cslt"


@dataclass
class Accommodation:
    """Dữ liệu cơ sở lưu trú"""
    name: str
    address: str


class AccommodationScraper:
    def __init__(
        self,
        max_pages: Optional[int] = None,
        start_page: int = 1,
        sleep: float = 0.7,
        user_agent: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    ):
        self.max_pages = max_pages
        self.start_page = start_page
        self.sleep = sleep
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
        self.empty_count = 0

    def fetch_html(self, page: int, max_retries: int = 3) -> str:
        """Tải HTML của một trang với retry logic"""
        url = BASE_URL
        if page > 1:
            url = f"{BASE_URL}?page={page}"
        
        logging.debug(f"Fetch page {page}: {url}")
        
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # exponential backoff
                    logging.warning(f"Lỗi trang {page}: {e} - Thử lại sau {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logging.warning(f"Lỗi trang {page} sau {max_retries} lần: {e}")
                    return ""
        
        return ""

    def extract_address(self, raw_text: str) -> str:
        """Tách địa chỉ từ chuỗi HTML
        
        Chuỗi thường có dạng: "Địa chỉ: <địa chỉ thực tế>"
        """
        if not raw_text:
            return ""
        
        # Loại bỏ tiền tố "Địa chỉ:" nếu có
        text = raw_text.strip()
        if text.lower().startswith("địa chỉ:"):
            text = text[len("Địa chỉ:"):].strip()
        
        # Loại bỏ thông tin thừa phía sau (nếu có)
        # Ví dụ: "... Thành phố Hà Nội   Thông tin Cơ sở..."
        # Tìm các chuỗi thường xuất hiện sau địa chỉ
        cleanup_phrases = [
            "Thông tin Cơ sở lưu trú tự đăng ký",
            "Thông tin Cơ sở",
            "tự đăng ký"
        ]
        
        for phrase in cleanup_phrases:
            if phrase in text:
                text = text.split(phrase)[0].strip()
        
        # Normalize khoảng trắng
        text = ' '.join(text.split())
        
        return text

    def parse_page(self, html: str) -> List[Accommodation]:
        """Phân tích HTML để lấy danh sách cơ sở lưu trú"""
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        accommodations: List[Accommodation] = []
        
        # Tìm tất cả các h4 chứa tên cơ sở lưu trú
        for h4 in soup.find_all('h4'):
            a_tag = h4.find('a')
            if not a_tag:
                continue
            
            name = a_tag.get_text(strip=True)
            if not name:
                continue
            
            # Tìm địa chỉ trong các elements kế tiếp
            address = ""
            for sibling in h4.next_siblings:
                # Dừng khi gặp h4 tiếp theo
                if getattr(sibling, 'name', None) == 'h4':
                    break
                
                # Tìm span chứa biểu tượng địa chỉ (map-marker)
                if hasattr(sibling, 'get_text'):
                    sibling_str = str(sibling)
                    if 'map-marker' in sibling_str or 'fa-map-marker' in sibling_str:
                        raw_address = sibling.get_text(' ', strip=True)
                        address = self.extract_address(raw_address)
                        break
            
            if name and address:
                accommodations.append(Accommodation(name=name, address=address))
        
        return accommodations

    def scrape(self, output_file: str = "accommodation.csv") -> List[Accommodation]:
        """Cào tất cả dữ liệu lưu trú"""
        all_accommodations: List[Accommodation] = []
        page = self.start_page
        empty_count = 0

        logging.info("=== Bắt đầu cào dữ liệu cơ sở lưu trú ===")

        while True:
            # Kiểm tra giới hạn trang
            if self.max_pages and page - self.start_page >= self.max_pages:
                logging.info(f"Đạt giới hạn {self.max_pages} trang")
                break

            # Tải và phân tích trang
            html = self.fetch_html(page)
            accommodations = self.parse_page(html)

            if not accommodations:
                empty_count += 1
                logging.info(f"Trang {page}: trống (lần {empty_count})")
                
                # Dừng khi gặp 2 trang trống liên tiếp
                if empty_count >= 2:
                    logging.info("Gặp 2 trang trống liên tiếp, dừng cào")
                    break
            else:
                empty_count = 0
                logging.info(f"Trang {page}: {len(accommodations)} mục")
                all_accommodations.extend(accommodations)

                # Checkpoint mỗi 10 trang
                if page % 10 == 0 or page == self.start_page:
                    self._save_csv(all_accommodations, output_file)

            page += 1
            time.sleep(self.sleep)

        # Lưu kết quả cuối cùng
        self._save_csv(all_accommodations, output_file)
        logging.info(f"=== Hoàn thành ===")
        logging.info(f"Tổng cộng: {len(all_accommodations)} cơ sở lưu trú")

        return all_accommodations

    def _save_csv(self, accommodations: List[Accommodation], filepath: str):
        """Ghi dữ liệu ra CSV với UTF-8 BOM (để mở được trên Excel)"""
        try:
            with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow(['name', 'address'])
                
                for acc in accommodations:
                    writer.writerow([acc.name, acc.address])
            
            logging.info(f"Checkpoint: lưu {len(accommodations)} mục vào {filepath}")
        except IOError as e:
            logging.error(f"Lỗi ghi file: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Cào dữ liệu cơ sở lưu trú từ csdl.vietnamtourism.gov.vn"
    )
    parser.add_argument(
        '--output', '-o',
        default='accommodation.csv',
        help='Tệp output CSV (mặc định: accommodation.csv)'
    )
    parser.add_argument(
        '--max-pages', '-m',
        type=int,
        default=None,
        help='Số trang tối đa (mặc định: không giới hạn, dừng khi hết dữ liệu)'
    )
    parser.add_argument(
        '--start-page', '-s',
        type=int,
        default=1,
        help='Trang bắt đầu (mặc định: 1)'
    )
    parser.add_argument(
        '--sleep',
        type=float,
        default=0.7,
        help='Delay giữa các trang (giây, mặc định: 0.7)'
    )
    parser.add_argument(
        '--user-agent',
        default='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        help='Custom User-Agent'
    )

    args = parser.parse_args()

    scraper = AccommodationScraper(
        max_pages=args.max_pages,
        start_page=args.start_page,
        sleep=args.sleep,
        user_agent=args.user_agent
    )

    try:
        accommodations = scraper.scrape(output_file=args.output)
        logging.info(f"Hoàn thành! Dữ liệu đã được lưu vào {args.output}")
    except KeyboardInterrupt:
        logging.info("Người dùng hủy bỏ")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Lỗi: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
