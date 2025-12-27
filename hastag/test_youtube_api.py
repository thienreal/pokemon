#!/usr/bin/env python3
"""Quick test to verify YouTube API is working."""

import os
import sys
from googleapiclient.discovery import build

API_KEY = os.getenv("YOUTUBE_API_KEY")

if not API_KEY:
    print("ERROR: YOUTUBE_API_KEY not set")
    sys.exit(1)

print(f"Testing YouTube API with key: {API_KEY[:10]}...")

try:
    service = build("youtube", "v3", developerKey=API_KEY)
    
    # Simple search for "Hanoi"
    print("\nTest 1: Search for 'Hanoi'")
    resp = service.search().list(
        q="Hanoi",
        type="video",
        part="id,snippet",
        maxResults=5,
    ).execute()
    
    items = resp.get("items", [])
    print(f"Found {len(items)} videos")
    for item in items:
        vid = item["id"]["videoId"]
        title = item["snippet"]["title"]
        print(f"  - {vid}: {title[:60]}...")
    
    # Test with publishedAfter
    from datetime import datetime, timedelta, timezone
    published_after = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    
    print(f"\nTest 2: Search for 'Hanoi' in last 30 days")
    resp = service.search().list(
        q="Hanoi",
        type="video",
        part="id,snippet",
        maxResults=5,
        publishedAfter=published_after,
    ).execute()
    
    items = resp.get("items", [])
    print(f"Found {len(items)} videos")
    for item in items:
        vid = item["id"]["videoId"]
        title = item["snippet"]["title"]
        published = item["snippet"]["publishedAt"]
        print(f"  - {vid}: {title[:50]}... ({published})")
    
    print("\n✓ API is working!")
    
except Exception as exc:
    print(f"\n✗ ERROR: {exc}")
    sys.exit(1)
