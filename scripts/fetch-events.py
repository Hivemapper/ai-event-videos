#!/usr/bin/env python3
"""Fetch AI event data with GNSS and IMU from the Bee Maps API, plus download videos."""

import json
import os
import urllib.request
import urllib.error
import urllib.parse

API_KEY = "NjhkYjIzZmQ1YjY5YmQ1MDY5NTJlZGU4OmIzYmRkMzc1LTU4MzMtNDMxNC04NjNlLTVhOGUyM2U4YTYwZg=="
BASE_URL = "https://beemaps.com/api/developer/aievents"
OUT_DIR = os.path.join(os.getcwd(), "data")

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

EVENT_IDS = [
    "698cad30682a8cf295d9dd68",
    "698cffbd92e4b9ac6459a134",
    "698cc4ba6233e22bb3f36ec7",
    "698e0242e87005775bf7d9d3",
    "698e024092e4b9ac64d3a329",
]

os.makedirs(OUT_DIR, exist_ok=True)

for event_id in EVENT_IDS:
    print(f"Fetching event {event_id} ...")
    params = urllib.parse.urlencode({
        "includeGnssData": "true",
        "includeImuData": "true",
        "apiKey": API_KEY,
    })
    url = f"{BASE_URL}/{event_id}?{params}"
    req = urllib.request.Request(url, headers=HEADERS)

    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  ERROR {e.code}: {body}")
        continue

    # Save JSON
    json_path = os.path.join(OUT_DIR, f"{event_id}.json")
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  → saved to {json_path}")

    # Download video
    video_url = data.get("videoUrl")
    if video_url:
        video_path = os.path.join(OUT_DIR, f"{event_id}.mp4")
        print(f"  Downloading video ...")
        try:
            video_req = urllib.request.Request(video_url, headers={
                "User-Agent": HEADERS["User-Agent"],
            })
            with urllib.request.urlopen(video_req) as vresp:
                with open(video_path, "wb") as vf:
                    while True:
                        chunk = vresp.read(1024 * 64)
                        if not chunk:
                            break
                        vf.write(chunk)
            size_mb = os.path.getsize(video_path) / (1024 * 1024)
            print(f"  → saved to {video_path} ({size_mb:.1f} MB)")
        except urllib.error.HTTPError as e:
            print(f"  Video download ERROR {e.code}: {e.read().decode('utf-8', errors='replace')}")
    else:
        print("  No videoUrl found in response")

print(f"Done. Fetched {len(EVENT_IDS)} events.")
