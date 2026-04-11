#!/usr/bin/env python3
"""Count events by type across date periods, output CSV."""

import csv
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent

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


def fetch_counts(api_key, start_str, end_str):
    auth = f"Basic {api_key}"
    counts = Counter()
    s = datetime.fromisoformat(start_str).replace(tzinfo=timezone.utc)
    e = datetime.fromisoformat(end_str).replace(tzinfo=timezone.utc)
    chunk_days = 31
    total_fetched = 0

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
                counts[ev.get("type", "UNKNOWN")] += 1
            offset += len(events)
            total_fetched += len(events)
            print(f"  {total_fetched} events so far ({s.strftime('%Y-%m-%d')} chunk, offset {offset})...", file=sys.stderr, end="\r", flush=True)
            if len(events) < 500:
                break
        s = chunk_end

    print(f"  {total_fetched} events total" + " " * 40, file=sys.stderr, flush=True)
    return counts


PERIODS = [
    ("Period 1: Jan 1 – Sep 15, 2025", "2025-01-01", "2025-09-15"),
    ("Period 2: Sep 15, 2025 – Jan 15, 2026", "2025-09-15", "2026-01-15"),
    ("Period 3: Jan 15 – Feb 10, 2026", "2026-01-15", "2026-02-10"),
    ("Period 4: Feb 11 – Mar 15, 2026", "2026-02-11", "2026-03-15"),
    ("Period 5: Mar 15, 2026+", "2026-03-15", "2026-04-06"),
]

EVENT_TYPES = [
    "HARSH_BRAKING",
    "AGGRESSIVE_ACCELERATION",
    "HIGH_SPEED",
    "HIGH_G_FORCE",
    "SWERVING",
]


def main():
    api_key = load_api_key()
    all_counts = {}

    for label, start, end in PERIODS:
        print(f"\nFetching {label} ({start} to {end})...", file=sys.stderr, flush=True)
        counts = fetch_counts(api_key, start, end)
        all_counts[label] = counts

    # Write CSV
    out_path = PROJECT_ROOT / "data" / "events-by-period.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        header = ["Event Type"] + [p[0] for p in PERIODS]
        writer.writerow(header)

        for etype in EVENT_TYPES:
            row = [etype] + [all_counts[p[0]].get(etype, 0) for p in PERIODS]
            writer.writerow(row)

        # Total row
        totals = ["TOTAL"] + [sum(all_counts[p[0]].values()) for p in PERIODS]
        writer.writerow(totals)

    print(f"\nCSV written to {out_path}", file=sys.stderr, flush=True)

    # Also print a nice table
    print()
    col_width = 12
    header_labels = ["Event Type"] + [f"P{i+1}" for i in range(len(PERIODS))]
    print(f"{'Event Type':<30}" + "".join(f"{h:>{col_width}}" for h in header_labels[1:]))
    print("-" * (30 + col_width * len(PERIODS)))
    for etype in EVENT_TYPES:
        vals = [all_counts[p[0]].get(etype, 0) for p in PERIODS]
        print(f"{etype:<30}" + "".join(f"{v:>{col_width},}" for v in vals))
    totals_vals = [sum(all_counts[p[0]].values()) for p in PERIODS]
    print("-" * (30 + col_width * len(PERIODS)))
    print(f"{'TOTAL':<30}" + "".join(f"{v:>{col_width},}" for v in totals_vals))


if __name__ == "__main__":
    main()
