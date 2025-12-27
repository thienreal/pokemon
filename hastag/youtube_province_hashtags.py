from __future__ import annotations

import argparse
import csv
import os
import sys
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Paths
DATASET_PATH = Path(__file__).resolve().parent.parent / "cacKhuVuc" / "cacKhuVucVietNam.csv"

SEARCH_PAGE_SIZE = 50


def _strip_accents(text: str) -> str:
    """Normalize Vietnamese text by removing accents, handling Đ/đ specially."""
    # Handle Đ/đ before NFD normalization
    text = text.replace("Đ", "D").replace("đ", "d")
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.lower()
    return "".join(ch for ch in text if ch.isalnum() or ch.isspace()).strip()


def load_provinces(path: Path = DATASET_PATH) -> List[Dict[str, str]]:
    provinces: List[Dict[str, str]] = []
    with path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue
            provinces.append({"id": row[0].strip(), "name": row[1].strip(), "region": row[2].strip()})
    return provinces


def search_hashtag(service, hashtag: str, province_name: str, max_results: int, days: int = 7) -> List[str]:
    """Search for videos about a province within last N days."""
    ids: List[str] = []
    page_token: Optional[str] = None
    
    # Calculate date for publishedAfter
    published_after = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    
    # Search by province name (more results) instead of just hashtag
    query = f"{province_name}"
    
    while len(ids) < max_results:
        try:
            resp = (
                service.search()
                .list(
                    q=query,
                    type="video",
                    part="id",
                    maxResults=min(SEARCH_PAGE_SIZE, max_results - len(ids)),
                    pageToken=page_token,
                    order="date",  # Sort by upload date (newest first)
                    publishedAfter=published_after,
                )
                .execute()
            )
        except HttpError as exc:
            sys.stderr.write(f"[error] search failed for '{query}': {exc}\n")
            break

        items = resp.get("items", [])
        if not items:
            break
            
        for item in items:
            vid = item.get("id", {}).get("videoId")
            if vid:
                ids.append(vid)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.2)
    return ids[:max_results]


def fetch_video_details(service, video_ids: List[str]) -> List[Dict]:
    """Fetch full video details (snippet + statistics) for given video IDs."""
    videos: List[Dict] = []
    # Batch in chunks of 50 (API limit)
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        try:
            resp = (
                service.videos()
                .list(id=",".join(batch), part="snippet,statistics")
                .execute()
            )
            videos.extend(resp.get("items", []))
        except HttpError as exc:
            sys.stderr.write(f"[error] videos fetch failed: {exc}\n")
        time.sleep(0.2)
    return videos


def main() -> None:
    parser = argparse.ArgumentParser(description="Count YouTube videos by province hashtag")
    parser.add_argument("--api-key", default=os.getenv("YOUTUBE_API_KEY"), help="YouTube Data API key")
    parser.add_argument("--max-results-per-province", type=int, default=50, help="Max videos to fetch per province hashtag")
    parser.add_argument("--days", type=int, default=7, help="Filter videos published within last N days (default 7)")
    parser.add_argument("--output", default="youtube_province_counts.csv", help="Output CSV for counts")
    parser.add_argument("--details-output", default=None, help="Optional CSV with all video IDs (province, video_id)")
    args = parser.parse_args()

    if not args.api_key:
        sys.stderr.write("Missing API key. Provide --api-key or set YOUTUBE_API_KEY.\n")
        sys.exit(1)

    provinces = load_provinces()
    service = build("youtube", "v3", developerKey=args.api_key)

    count_rows: List[Dict[str, str]] = []
    detail_rows: List[Dict[str, str]] = []

    for p in provinces:
        name = p["name"]
        region = p["region"]
        hashtag = _strip_accents(name).replace(" ", "")  # e.g., "ha noi" -> "hanoi", "Đắk Nông" -> "daknong"
        video_ids = search_hashtag(service, hashtag, name, args.max_results_per_province, args.days)
        sys.stderr.write(f"[info] {name}: found {len(video_ids)} videos\n")
        count_rows.append(
            {
                "province": name,
                "region": region,
                "hashtag_query": f"#{hashtag}",
                "video_count": len(video_ids),
            }
        )
        if args.details_output and video_ids:
            # Fetch full video details including views, likes, comments
            videos = fetch_video_details(service, video_ids)
            for v in videos:
                snippet = v.get("snippet", {})
                stats = v.get("statistics", {})
                detail_rows.append(
                    {
                        "province": name,
                        "region": region,
                        "hashtag_query": f"#{hashtag}",
                        "video_id": v.get("id"),
                        "title": snippet.get("title", ""),
                        "channel_title": snippet.get("channelTitle", ""),
                        "published_at": snippet.get("publishedAt", ""),
                        "views": stats.get("viewCount", "0"),
                        "likes": stats.get("likeCount", "0"),
                        "comments": stats.get("commentCount", "0"),
                        "url": f"https://www.youtube.com/watch?v={v.get('id')}",
                    }
                )
        # Gentle pacing for quota friendliness
        time.sleep(0.1)

    # Write counts
    with open(args.output, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["province", "region", "hashtag_query", "video_count"])
        writer.writeheader()
        writer.writerows(count_rows)

    # Write details if requested
    if args.details_output:
        with open(args.details_output, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "province",
                    "region",
                    "hashtag_query",
                    "video_id",
                    "title",
                    "channel_title",
                    "published_at",
                    "views",
                    "likes",
                    "comments",
                    "url",
                ],
            )
            writer.writeheader()
            writer.writerows(detail_rows)

    print(f"Saved counts to {args.output}")
    if args.details_output:
        print(f"Saved video list to {args.details_output}")


if __name__ == "__main__":
    main()
