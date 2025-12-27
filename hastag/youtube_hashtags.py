from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from province_lookup import detect_province, get_default_matcher

SEARCH_PAGE_SIZE = 50
VIDEO_PAGE_SIZE = 50


def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def fetch_video_ids(service, hashtag: str, max_results: int) -> List[str]:
    ids: List[str] = []
    page_token: Optional[str] = None
    while len(ids) < max_results:
        try:
            resp = (
                service.search()
                .list(
                    q=f"#{hashtag}",
                    type="video",
                    part="id",
                    maxResults=min(SEARCH_PAGE_SIZE, max_results - len(ids)),
                    pageToken=page_token,
                    order="date",
                )
                .execute()
            )
        except HttpError as exc:
            sys.stderr.write(f"[error] search failed: {exc}\n")
            break

        for item in resp.get("items", []):
            vid = item.get("id", {}).get("videoId")
            if vid:
                ids.append(vid)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.2)
    return ids[:max_results]


def fetch_video_details(service, video_ids: List[str]) -> List[Dict]:
    videos: List[Dict] = []
    for batch in chunked(video_ids, VIDEO_PAGE_SIZE):
        try:
            resp = (
                service.videos()
                .list(id=",".join(batch), part="snippet,statistics")
                .execute()
            )
        except HttpError as exc:
            sys.stderr.write(f"[error] videos lookup failed: {exc}\n")
            continue
        videos.extend(resp.get("items", []))
        time.sleep(0.2)
    return videos


def annotate_with_province(videos: List[Dict]) -> List[Dict]:
    matcher = get_default_matcher()
    annotated: List[Dict] = []
    for v in videos:
        snippet = v.get("snippet", {})
        stats = v.get("statistics", {})
        text_for_match = " ".join(
            [snippet.get("title", ""), snippet.get("description", "")]
        )
        province = matcher.detect(text_for_match)
        province_name, region = (province if province else (None, None))

        annotated.append(
            {
                "video_id": v.get("id"),
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "channel_title": snippet.get("channelTitle"),
                "published_at": snippet.get("publishedAt"),
                "views": stats.get("viewCount"),
                "likes": stats.get("likeCount"),
                "comments": stats.get("commentCount"),
                "province": province_name,
                "region": region,
            }
        )
    return annotated


def write_csv(rows: List[Dict], hashtag: str, output: str) -> None:
    fieldnames = [
        "platform",
        "hashtag",
        "video_id",
        "title",
        "description",
        "channel_title",
        "published_at",
        "views",
        "likes",
        "comments",
        "province",
        "region",
        "url",
        "collected_at_utc",
    ]
    collected_at = datetime.now(timezone.utc).isoformat()
    with open(output, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    **row,
                    "platform": "youtube",
                    "hashtag": hashtag,
                    "url": f"https://www.youtube.com/watch?v={row.get('video_id')}",
                    "collected_at_utc": collected_at,
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch YouTube videos by hashtag")
    parser.add_argument("--hashtag", required=True, help="Hashtag without #, e.g. travel")
    parser.add_argument(
        "--api-key",
        default=os.getenv("YOUTUBE_API_KEY"),
        help="YouTube Data API key (or set YOUTUBE_API_KEY)",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=100,
        help="Max videos to fetch (quota dependent)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV path (default youtube_<hashtag>.csv)",
    )
    args = parser.parse_args()

    if not args.api_key:
        sys.stderr.write("Missing API key. Provide --api-key or env YOUTUBE_API_KEY.\n")
        sys.exit(1)

    output_path = args.output or f"youtube_{args.hashtag}.csv"

    service = build("youtube", "v3", developerKey=args.api_key)
    video_ids = fetch_video_ids(service, args.hashtag, args.max_results)
    if not video_ids:
        sys.stderr.write("No videos found for this hashtag.\n")
        sys.exit(0)

    videos = fetch_video_details(service, video_ids)
    annotated = annotate_with_province(videos)
    write_csv(annotated, args.hashtag, output_path)
    print(f"Saved {len(annotated)} rows to {output_path}")


if __name__ == "__main__":
    main()
