#!/usr/bin/env python3
"""Estimate bitrate for Period 1 events using HEAD (file size) + ffprobe (duration).

Categorizes events as low (~1.5 Mbps), medium (~3 Mbps), or high (~5 Mbps)
based on calculated bitrate. Reports total counts and harsh braking breakdown.
"""

import json
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Period 1: Jan 1 – Sep 15, 2025
START = "2025-01-01T00:00:00.000Z"
END = "2025-09-15T00:00:00.000Z"


def load_api_key():
    key = os.environ.get("BEEMAPS_API_KEY")
    if key:
        return key
    env_local = PROJECT_ROOT / ".env.local"
    if env_local.exists():
        for line in env_local.read_text().splitlines():
            if line.startswith("BEEMAPS_API_KEY="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("BEEMAPS_API_KEY not found")


def api_request(method, url, **kwargs):
    kwargs.setdefault("timeout", 30)
    wait = 10
    while True:
        resp = requests.request(method, url, **kwargs)
        if resp.status_code != 403:
            return resp
        print(f"  403 rate-limited — waiting {wait}s...", file=sys.stderr, flush=True)
        time.sleep(wait)
        wait = min(wait * 2, 120)


def get_file_size(video_url: str) -> int | None:
    """HEAD request to get Content-Length."""
    try:
        resp = requests.head(video_url, timeout=15, allow_redirects=True)
        if resp.status_code == 200:
            cl = resp.headers.get("Content-Length")
            if cl:
                return int(cl)
        # Some CDNs don't support HEAD, try GET with range
        resp = requests.get(
            video_url, headers={"Range": "bytes=0-0"}, timeout=15, allow_redirects=True
        )
        cr = resp.headers.get("Content-Range", "")  # e.g. "bytes 0-0/12345678"
        if "/" in cr:
            total = cr.split("/")[-1]
            if total.isdigit():
                return int(total)
    except Exception as e:
        print(f"  HEAD failed: {e}", file=sys.stderr)
    return None


def get_duration(video_url: str) -> float | None:
    """Use ffprobe to get video duration without downloading the full file."""
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                video_url,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            info = json.loads(result.stdout)
            dur = info.get("format", {}).get("duration")
            if dur:
                return float(dur)
    except Exception as e:
        print(f"  ffprobe failed: {e}", file=sys.stderr)
    return None


def estimate_bitrate_mbps(file_size_bytes: int, duration_secs: float) -> float:
    """Calculate bitrate in Mbps."""
    return (file_size_bytes * 8) / duration_secs / 1_000_000


def main():
    api_key = load_api_key()
    auth = f"Basic {api_key}"

    # Counters
    total_events = 0
    total_checked = 0
    skipped = 0

    # Bitrate buckets
    high_bitrate = []       # > 4 Mbps (the ~5 Mbps tier)
    medium_bitrate = []     # 2-4 Mbps (the ~3 Mbps tier)
    low_bitrate = []        # < 2 Mbps (the ~1.5 Mbps tier)

    high_harsh_braking = 0
    total_harsh_braking = 0

    # Page through all Period 1 events
    from datetime import datetime, timedelta, timezone
    s = datetime.fromisoformat("2025-01-01").replace(tzinfo=timezone.utc)
    e = datetime.fromisoformat("2025-09-15").replace(tzinfo=timezone.utc)
    chunk_days = 31

    while s < e:
        chunk_end = min(s + timedelta(days=chunk_days), e)
        offset = 0
        while True:
            body = {
                "startDate": s.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "endDate": chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "limit": 500,
                "offset": offset,
            }
            resp = api_request(
                "POST",
                "https://beemaps.com/api/developer/aievents/search",
                headers={"Content-Type": "application/json", "Authorization": auth},
                json=body,
            )
            resp.raise_for_status()
            events = resp.json().get("events", [])
            if not events:
                break

            for ev in events:
                total_events += 1
                event_type = ev.get("type", "UNKNOWN")
                video_url = ev.get("videoUrl", "")

                if event_type == "HARSH_BRAKING":
                    total_harsh_braking += 1

                if not video_url:
                    skipped += 1
                    continue

                file_size = get_file_size(video_url)
                if file_size is None:
                    skipped += 1
                    continue

                duration = get_duration(video_url)
                if duration is None or duration < 1:
                    skipped += 1
                    continue

                total_checked += 1
                mbps = estimate_bitrate_mbps(file_size, duration)

                entry = {
                    "id": ev.get("id"),
                    "type": event_type,
                    "mbps": round(mbps, 2),
                    "size_mb": round(file_size / 1_000_000, 1),
                    "duration": round(duration, 1),
                }

                if mbps > 4.0:
                    high_bitrate.append(entry)
                    if event_type == "HARSH_BRAKING":
                        high_harsh_braking += 1
                elif mbps > 2.0:
                    medium_bitrate.append(entry)
                else:
                    low_bitrate.append(entry)

                # Progress
                if total_checked % 50 == 0:
                    print(
                        f"  Checked {total_checked} / {total_events} events | "
                        f"High: {len(high_bitrate)}, Med: {len(medium_bitrate)}, Low: {len(low_bitrate)}",
                        file=sys.stderr,
                        flush=True,
                    )

            offset += len(events)
            if len(events) < 500:
                break
        s = chunk_end

    # Results
    print("\n" + "=" * 60)
    print(f"PERIOD 1 BITRATE ANALYSIS")
    print(f"=" * 60)
    print(f"Total events:        {total_events:,}")
    print(f"Successfully probed: {total_checked:,}")
    print(f"Skipped (no data):   {skipped:,}")
    print()
    print(f"HIGH bitrate (>4 Mbps, ~5 Mbps tier):  {len(high_bitrate):,}")
    print(f"  of which HARSH_BRAKING:               {high_harsh_braking:,}")
    print(f"MEDIUM bitrate (2-4 Mbps, ~3 Mbps tier): {len(medium_bitrate):,}")
    print(f"LOW bitrate (<2 Mbps, ~1.5 Mbps tier):  {len(low_bitrate):,}")
    print()
    print(f"Total HARSH_BRAKING in Period 1:        {total_harsh_braking:,}")

    # Save detailed results
    out_path = PROJECT_ROOT / "data" / "period1-bitrate.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(
            {
                "total_events": total_events,
                "total_checked": total_checked,
                "skipped": skipped,
                "high_bitrate_count": len(high_bitrate),
                "high_harsh_braking": high_harsh_braking,
                "medium_bitrate_count": len(medium_bitrate),
                "low_bitrate_count": len(low_bitrate),
                "total_harsh_braking": total_harsh_braking,
                "high_bitrate_events": high_bitrate,
            },
            f,
            indent=2,
        )
    print(f"\nDetailed results saved to {out_path}")


if __name__ == "__main__":
    main()
