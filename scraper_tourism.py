import time, csv, logging
import requests
from bs4 import BeautifulSoup

# Cấu hình logging để in thông tin tiến trình
logging.basicConfig(level=logging.INFO, format="%(message)s")

BASE_URL = "https://csdl.vietnamtourism.gov.vn/dest/"
session = requests.Session()
session.headers["User-Agent"] = "Mozilla/5.0"

# Tải HTML của một trang
def get_html(page):
    url = BASE_URL if page == 1 else BASE_URL + "?page=" + str(page)
    try:
        resp = session.get(url, timeout=25)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logging.warning("Lỗi tải trang " + str(page) + ": " + str(e))
        return ""

# Rút tỉnh/thành từ địa chỉ
def extract_province(addr):
    if not addr: return ""
    s = addr.strip()
    if s.lower().startswith("địa chỉ:"): s = s[8:].strip()
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return parts[-1] if parts else ""

# Phân tích HTML để lấy tên và địa chỉ
def parse(html):
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for h4 in soup.find_all("h4"):
        a = h4.find("a")
        if not a: continue
        name = a.get_text(strip=True)
        raw = ""
        for sib in h4.next_siblings:
            if getattr(sib, "name", None) == "h4": break
            if hasattr(sib, "get_text") and "map-marker" in str(sib):
                raw = sib.get_text(" ", strip=True).replace("\xa0", " ").strip()
                break
        out.append((name, extract_province(raw)))
    return out

# Ghi dữ liệu ra CSV
def save_csv(rows, path, mode="w"):
    # dùng dấu ; làm delimiter để tránh thêm dấu "
    with open(path, mode, newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        if mode == "w":
            w.writerow(["name", "province"])
        w.writerows(rows)
    logging.info("Checkpoint: đã lưu " + str(len(rows)) + " mục vào " + path)

# Cào dữ liệu, tự dừng khi hết trang
def scrape(sleep_sec=0.7, checkpoint_file="tourism.csv"):
    all_items = []
    page = 1
    empty_count = 0

    while True:
        html = get_html(page)
        items = parse(html)

        if not items:
            empty_count += 1
            logging.info("Trang " + str(page) + " trống")
            if empty_count >= 2:  # gặp 2 trang trống liên tiếp thì dừng
                break
        else:
            empty_count = 0
            logging.info("Trang " + str(page) + ": " + str(len(items)) + " mục")
            all_items.extend(items)

            if page % 10 == 0:
                save_csv(all_items, checkpoint_file, mode="w")

        page += 1
        time.sleep(sleep_sec)

    save_csv(all_items, checkpoint_file, mode="w")
    logging.info("Tổng cộng: " + str(len(all_items)) + " mục")
    return all_items

# Hàm main để chạy chương trình
def main():
    items = scrape(sleep_sec=0.7)
    logging.info("Done.")

if __name__ == "__main__":
    main()
