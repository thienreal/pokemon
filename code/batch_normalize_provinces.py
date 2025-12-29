import pandas as pd
import unicodedata
import re
from pathlib import Path

WS = Path(__file__).resolve().parent.parent
DATA = WS / "data"
OUT_DIR = DATA / "normalized"
OUT_DIR.mkdir(exist_ok=True)

# --- Load canonical mapping (34 provinces) ---
mapping_34 = pd.read_csv(DATA / "vietnam_province_name_mapping.csv")
old_to_new = dict(zip(mapping_34["old"], mapping_34["new"]))
canonical_34 = sorted(set(mapping_34["new"].unique()))


def normalize(text: str) -> str:
    """Lowercase, strip accents, unify dashes, keep alnum/space/hyphen."""
    s = str(text).strip().lower()
    s = s.replace("đ", "d").replace("Đ", "d")
    s = s.replace("–", "-").replace("—", "-")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9\s\-]", "", s)
    return s.strip()


# Build alias map
alias_map = {}
for canonical in canonical_34:
    norm = normalize(canonical)
    if norm:
        alias_map[norm] = canonical
for old, new in old_to_new.items():
    norm = normalize(old)
    if norm and norm not in alias_map:
        alias_map[norm] = new
alias_map.update(
    {
        "dac lak": "Đắk Lắk",
        "dac nong": "Lâm Đồng",
        "ba ria - vung tau": "TP. Hồ Chí Minh",
        "ba ria vung tau": "TP. Hồ Chí Minh",
        # Major cities often appear without the "TP." prefix
        normalize("Hà Nội"): "TP. Hà Nội",
        normalize("Hải Phòng"): "TP. Hải Phòng",
        normalize("Huế"): "TP. Huế",
        normalize("Đà Nẵng"): "TP. Đà Nẵng",
        normalize("Cần Thơ"): "TP. Cần Thơ",
        normalize("Thừa Thiên Huế"): "TP. Huế",
        # Abbreviated city names
        "tp ho chi minh": "TP. Hồ Chí Minh",
        # Regional names (population data) → pick largest city in region
        normalize("Đồng bằng sông Hồng"): "TP. Hà Nội",
        normalize("Trung du và miền núi phía Bắc"): "Cao Bằng",
        normalize("Bắc Trung Bộ và Duyên hải miền Trung"): "Thanh Hóa",
        normalize("Tây Nguyên"): "Đắk Lắk",
        normalize("Đông Nam Bộ"): "TP. Hồ Chí Minh",
        normalize("Đồng bằng sông Cửu Long"): "TP. Cần Thơ",
        # Additional edge cases
        normalize("Thừa Thiên - Huế"): "TP. Huế",
    }
)

# Weather CSVs were saved with mangled encodings; map those raw strings directly.
CORRUPTED_WEATHER = {
    "B¯c Ninh": "Bắc Ninh",
    "Qu£ng Ninh": "Quảng Ninh",
    "Lâm \x10Óng": "Lâm Đồng",
    "Thái Nguyên": "Thái Nguyên",
    "H°ng Yên": "Hưng Yên",
    "\x10iÇn Biên": "Điện Biên",
    "Qu£ng TrË": "Quảng Trị",
    "Gia Lai": "Gia Lai",
    "Cao B±ng": "Cao Bằng",
    "An Giang": "An Giang",
    "\x10¯k L¯k": "Đắk Lắk",
    "TP. \x10à Nµng": "TP. Đà Nẵng",
    "V)nh Long": "Vĩnh Long",
    "Tây Ninh": "Tây Ninh",
    "Qu£ng Ngãi": "Quảng Ngãi",
    "Thanh Hóa": "Thanh Hóa",
    "TP. C§n Th¡": "TP. Cần Thơ",
    "Lào Cai": "Lào Cai",
    "Phú ThÍ": "Phú Thọ",
    "\x10Óng Nai": "Đồng Nai",
    "\x10Óng Tháp": "Đồng Tháp",
    "Cà Mau": "Cà Mau",
    "Hà T)nh": "Hà Tĩnh",
    "L¡ng S¡n": "Lạng Sơn",
    "NghÇ An": "Nghệ An",
    "Ninh B́nh": "Ninh Bình",
    "Qu£ng Ngăi": "Quảng Ngãi",
    "S¡n La": "Sơn La",
    "TP. H£i Pḥng": "TP. Hải Phòng",
    "TP. H£i Phòng": "TP. Hải Phòng",
    "TP. HÓ Chí Minh": "TP. Hồ Chí Minh",
}

# Curated district/city to province map (minimal set for robustness)
DISTRICT_MAP = {
    # Alternative/abbreviated forms of major cities
    "ho chi minh": "TP. Hồ Chí Minh",
    "thanh pho ho chi minh": "TP. Hồ Chí Minh",
    "tpho chi minh": "TP. Hồ Chí Minh",
    "thua thien hue": "TP. Huế",
    "thua thien - hue": "TP. Huế",
    # An Giang
    "chau doc": "An Giang",
    "thanh pho chau doc": "An Giang",
    "tri ton": "An Giang",
    "tinh bien": "An Giang",
    "thoai son": "An Giang",
    "long xuyen": "An Giang",
    "an phu": "An Giang",
    # Đồng Tháp
    "sa dec": "Đồng Tháp",
    "thanh pho sa dec": "Đồng Tháp",
    "tam nong": "Đồng Tháp",
    "thap muoi": "Đồng Tháp",
    "huyen thap muoi": "Đồng Tháp",
    "cao lanh": "Đồng Tháp",
    # Gia Lai
    "kon ka kinh": "Gia Lai",
    # Đắk Lắk / Đắk Nông variants
    "dak lak": "Đắk Lắk",
    "ak lak": "Đắk Lắk",
    "ac lak": "Đắk Lắk",
    "dac lak": "Đắk Lắk",
    "dak nong": "Lâm Đồng",
    "ak nong": "Lâm Đồng",
    "ac nong": "Lâm Đồng",
    "dac nong": "Lâm Đồng",
}


PREFIXES = [
    "vuon quoc gia",
    "thanh pho",
    "tp",
    "thi xa",
    "quan",
    "huyen",
    "thi tran",
    "di tich",
]


def normalize_province(raw_name: str) -> str:
    if pd.isna(raw_name) or not str(raw_name).strip():
        return ""
    raw = str(raw_name).strip()
    if raw in CORRUPTED_WEATHER:
        return CORRUPTED_WEATHER[raw]
    # Try to repair mojibake strings that are UTF-8 read as latin1.
    try:
        repaired = raw.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        if repaired and repaired != raw:
            norm_repaired = normalize(repaired)
            if norm_repaired in alias_map:
                return alias_map[norm_repaired]
            if norm_repaired in DISTRICT_MAP:
                return DISTRICT_MAP[norm_repaired]
    except Exception:
        pass
    norm = normalize(raw)
    if norm in alias_map:
        return alias_map[norm]
    if norm in DISTRICT_MAP:
        return DISTRICT_MAP[norm]
    for prefix in PREFIXES:
        if norm.startswith(prefix + " ") or norm.startswith(prefix + "-"):
            rest = norm[len(prefix) :].strip().replace("-", " ")
            if rest in alias_map:
                return alias_map[rest]
            if rest in DISTRICT_MAP:
                return DISTRICT_MAP[rest]
    return ""


FILE_CONFIGS = [
    # semicolon-separated name/province pairs
    {"name": "vietnam_accommodation.csv", "sep": ";", "encoding": "utf-8-sig", "province_col": "province"},
    {"name": "vietnam_entertainment.csv", "sep": ";", "encoding": "utf-8-sig", "province_col": "province"},
    {"name": "vietnam_healthcare.csv", "sep": ";", "encoding": "utf-8-sig", "province_col": "province"},
    {"name": "vietnam_restaurants.csv", "sep": ";", "encoding": "utf-8-sig", "province_col": "province"},
    {"name": "vietnam_shops.csv", "sep": ";", "encoding": "utf-8-sig", "province_col": "province"},
    # comma-separated
    {"name": "vietnam_festivals.csv", "sep": ",", "encoding": "utf-8-sig", "province_col": "province"},
    {"name": "vietnam_youtube_province_aggregates.csv", "sep": ",", "encoding": "utf-8-sig", "province_col": "province"},
    {"name": "vietnam_youtube_province_videos.csv", "sep": ",", "encoding": "utf-8-sig", "province_col": "province"},
    # Additional files with province columns
    {"name": "keyword_mapping.csv", "sep": ",", "encoding": "utf-8-sig", "province_col": "province"},
    {"name": "vietnam_regions_with_distances.csv", "sep": ",", "encoding": "utf-8-sig", "province_col": "province"},
    {"name": "vietnam_population.csv", "sep": ",", "encoding": "utf-8-sig", "province_col": "Tỉnh, thành phố"},
    {"name": "vietnam_grdp_by_province.csv", "sep": ",", "encoding": "utf-8-sig", "province_col": "Tên tỉnh, thành phố"},
    {"name": "vietnam_area_population.csv", "sep": ",", "encoding": "utf-8-sig", "province_col": "Địa phương"},
]

# Weather files
for year in range(2018, 2026):
    FILE_CONFIGS.append(
        {
            "name": f"vietnam_weather_tourism_{year}.csv",
            "sep": ",",
            # Weather CSVs carry a stray 0xff byte up front, so read with latin1 to avoid decode errors.
            "encoding": "latin1",
            "province_col": "province",
        }
    )


# Helper to process one file

def process_file(conf):
    path = DATA / conf["name"]
    if not path.exists():
        print(f"SKIP (missing): {path.name}")
        return None
    df = pd.read_csv(path, sep=conf["sep"], encoding=conf["encoding"], dtype=str)
    prov_col = conf["province_col"]
    if prov_col not in df.columns:
        print(f"SKIP (no province col): {path.name}")
        return None
    df["province_normalized"] = df[prov_col].apply(normalize_province)
    # stats
    total = len(df)
    mapped = (df["province_normalized"] != "").sum()
    unmapped = total - mapped
    unique = df.loc[df["province_normalized"] != "", "province_normalized"].nunique()
    out_path = OUT_DIR / path.name
    df.to_csv(out_path, index=False, sep=conf["sep"], encoding="utf-8-sig")
    print(
        f"{path.name}: total={total}, mapped={mapped} ({mapped/total*100:.1f}%), "
        f"unmapped={unmapped}, unique={unique}, out={out_path.name}"
    )
    return {
        "file": path.name,
        "total": total,
        "mapped": mapped,
        "unmapped": unmapped,
        "unique": unique,
        "out": str(out_path.relative_to(DATA)),
    }


def main():
    summaries = []
    for conf in FILE_CONFIGS:
        res = process_file(conf)
        if res:
            summaries.append(res)
    # Save summary
    if summaries:
        summary_df = pd.DataFrame(summaries)
        summary_df.to_csv(OUT_DIR / "_summary_normalization.csv", index=False)
        print("\nSummary written to", OUT_DIR / "_summary_normalization.csv")


if __name__ == "__main__":
    main()
