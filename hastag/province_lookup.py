from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# Dataset lives at repo_root/cacKhuVuc/cacKhuVucVietNam.csv
DATASET_PATH = Path(__file__).resolve().parent.parent / "cacKhuVuc" / "cacKhuVucVietNam.csv"


def _strip_accents(text: str) -> str:
    """Normalize Vietnamese text by removing accents and lowercasing."""
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.lower()
    return re.sub(r"[^a-z0-9\s]", " ", text)


class ProvinceMatcher:
    def __init__(
        self,
        dataset_path: Path = DATASET_PATH,
        extra_aliases: Optional[Dict[str, Iterable[str]]] = None,
    ) -> None:
        self.dataset_path = dataset_path
        self.provinces: List[Tuple[str, str]] = []  # (province, region)
        self.alias_to_province: Dict[str, Tuple[str, str]] = {}
        self._load(extra_aliases or {})

    def _load(self, extra_aliases: Dict[str, Iterable[str]]) -> None:
        with self.dataset_path.open(encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 3:
                    continue
                _, province, region = row[0].strip(), row[1].strip(), row[2].strip()
                self.provinces.append((province, region))

        default_aliases: Dict[str, List[str]] = {
            "Hà Nội": ["ha noi", "hanoi", "hn"],
            "TP.Hồ Chí Minh": [
                "ho chi minh",
                "tp ho chi minh",
                "tp hcm",
                "hcm",
                "sai gon",
                "saigon",
                "sg",
            ],
            "Đà Nẵng": ["da nang", "danang", "dn"],
            "Thừa Thiên Huế": ["hue", "thua thien hue"],
            "Bà Rịa - Vũng Tàu": ["ba ria", "vung tau", "ba ria vung tau", "brvt"],
        }
        for key, aliases in extra_aliases.items():
            default_aliases.setdefault(key, []).extend(aliases)

        for province, region in self.provinces:
            normalized = _strip_accents(province)
            self.alias_to_province[normalized.strip()] = (province, region)
            for alias in default_aliases.get(province, []):
                alias_norm = _strip_accents(alias).strip()
                if alias_norm:
                    self.alias_to_province[alias_norm] = (province, region)

        # Precompile regex patterns for faster lookups
        self._patterns: List[Tuple[re.Pattern[str], Tuple[str, str]]] = []
        for alias_norm, dest in self.alias_to_province.items():
            pattern = re.compile(rf"\b{re.escape(alias_norm)}\b", flags=re.IGNORECASE)
            self._patterns.append((pattern, dest))

    def detect(self, text: str) -> Optional[Tuple[str, str]]:
        if not text:
            return None
        normalized_text = _strip_accents(text)
        for pattern, province in self._patterns:
            if pattern.search(normalized_text):
                return province
        return None


_default_matcher: Optional[ProvinceMatcher] = None


def get_default_matcher() -> ProvinceMatcher:
    global _default_matcher
    if _default_matcher is None:
        _default_matcher = ProvinceMatcher()
    return _default_matcher


def detect_province(text: str) -> Optional[Tuple[str, str]]:
    """Convenience wrapper returning (province, region) or None."""
    return get_default_matcher().detect(text)
