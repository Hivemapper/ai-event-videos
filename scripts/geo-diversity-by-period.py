#!/usr/bin/env python3
"""
Geo diversity table — event counts by country for each period.

Usage:
    python3 scripts/geo-diversity-by-period.py
"""

import argparse
import csv
import json
import os
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "labels.db"

DEFAULT_PERIODS = [
    ("Period 3: Jan 15 – Feb 10, 2026", "2026-01-15", "2026-02-10"),
    ("Period 4: Feb 11 – Mar 15, 2026", "2026-02-11", "2026-03-15"),
    ("Period 5: Mar 15 – Apr 17, 2026", "2026-03-15", "2026-04-17"),
    ("Period 6: Apr 17, 2026+ (fw ≥7.0.12)", "2026-04-17", "2099-01-01"),
]

# Cache geocoded coordinates to avoid re-querying
GEO_CACHE_PATH = PROJECT_ROOT / "data" / "geo-cache.json"

BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
CYAN = "\033[36m"
RESET = "\033[0m"


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


def load_mapbox_token():
    for var in ("NEXT_PUBLIC_MAPBOX_TOKEN", "MAPBOX_TOKEN"):
        val = os.environ.get(var)
        if val:
            return val
    env_local = PROJECT_ROOT / ".env.local"
    if env_local.exists():
        for line in env_local.read_text().splitlines():
            if line.startswith("NEXT_PUBLIC_MAPBOX_TOKEN="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("MAPBOX_TOKEN not found")


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


def fetch_events_from_db(conn, start_str, end_str):
    """Fetch signal events from triage_results DB for a date range."""
    start_iso = f"{start_str}T00:00:00.000Z"
    end_iso = f"{end_str}T00:00:00.000Z"
    rows = conn.execute(
        """SELECT lat, lon, event_type FROM triage_results
           WHERE triage_result = 'signal'
             AND event_timestamp >= ? AND event_timestamp < ?
             AND lat IS NOT NULL AND lon IS NOT NULL""",
        (start_iso, end_iso),
    ).fetchall()
    print(f"  {len(rows)} signal events", file=sys.stderr, flush=True)
    return [(r[0], r[1], r[2] or "UNKNOWN") for r in rows]


def load_geo_cache():
    if GEO_CACHE_PATH.exists():
        return json.loads(GEO_CACHE_PATH.read_text())
    return {}


def save_geo_cache(cache):
    GEO_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    GEO_CACHE_PATH.write_text(json.dumps(cache))


def coord_key(lat, lon):
    """Round to ~1km grid to batch nearby coords into same country."""
    return f"{round(lat, 2)},{round(lon, 2)}"


def reverse_geocode_country(lat, lon, token):
    """Get country name from Mapbox reverse geocoding."""
    url = (
        f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
        f"?types=country&limit=1&access_token={token}"
    )
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 429:
            time.sleep(2)
            resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        features = resp.json().get("features", [])
        if features:
            return features[0].get("place_name", "Unknown")
    except Exception:
        pass
    return "Unknown"


def geocode_countries(events_by_period, token):
    """Resolve all unique coord grid cells to country names."""
    cache = load_geo_cache()
    all_keys = set()

    for events in events_by_period.values():
        for lat, lon, _ in events:
            all_keys.add(coord_key(lat, lon))

    to_resolve = [k for k in all_keys if k not in cache]
    print(f"\n  {len(all_keys)} unique grid cells, {len(to_resolve)} need geocoding", file=sys.stderr)

    for i, key in enumerate(to_resolve):
        lat, lon = [float(x) for x in key.split(",")]
        country = reverse_geocode_country(lat, lon, token)
        cache[key] = country
        if (i + 1) % 50 == 0 or i + 1 == len(to_resolve):
            print(f"  Geocoded {i + 1}/{len(to_resolve)}...", file=sys.stderr, end="\r", flush=True)
            save_geo_cache(cache)
        # Rate limit: Mapbox allows 600 req/min for geocoding
        time.sleep(0.12)

    if to_resolve:
        save_geo_cache(cache)
        print(f"  Geocoded {len(to_resolve)} locations" + " " * 20, file=sys.stderr, flush=True)

    return cache


def main():
    parser = argparse.ArgumentParser(description="Geo diversity by period")
    parser.add_argument("--test", action="store_true", help="Test with 1 day of data")
    args = parser.parse_args()

    if args.test:
        PERIODS = [("Test: Apr 7, 2026", "2026-04-07", "2026-04-08")]
    else:
        PERIODS = DEFAULT_PERIODS

    token = load_mapbox_token()
    conn = sqlite3.connect(str(DB_PATH))

    events_by_period = {}
    for label, start, end in PERIODS:
        print(f"\n{CYAN}Fetching {label} ({start} to {end})...{RESET}", file=sys.stderr)
        events_by_period[label] = fetch_events_from_db(conn, start, end)

    conn.close()

    # Geocode all coordinates
    geo_cache = geocode_countries(events_by_period, token)

    # Count by country per period
    country_counts = {}  # {period_label: Counter({country: count})}
    for label, events in events_by_period.items():
        counts = Counter()
        for lat, lon, _ in events:
            country = geo_cache.get(coord_key(lat, lon), "Unknown")
            counts[country] += 1
        country_counts[label] = counts

    # Get all countries, sorted by total across all periods
    all_countries = Counter()
    for counts in country_counts.values():
        all_countries.update(counts)
    sorted_countries = [c for c, _ in all_countries.most_common()]

    # Write CSV
    out_path = PROJECT_ROOT / "data" / "geo-diversity-by-period.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    period_labels = [p[0] for p in PERIODS]
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Country"] + period_labels + ["Total"])
        for country in sorted_countries:
            row = [country]
            total = 0
            for label in period_labels:
                val = country_counts[label].get(country, 0)
                row.append(val)
                total += val
            row.append(total)
            writer.writerow(row)

        # Total row
        totals = ["TOTAL"]
        grand_total = 0
        for label in period_labels:
            t = sum(country_counts[label].values())
            totals.append(t)
            grand_total += t
        totals.append(grand_total)
        writer.writerow(totals)

    print(f"\n{GREEN}CSV written to {out_path}{RESET}", file=sys.stderr)

    # Print table
    col_width = max(14, max(len(p[0]) for p in PERIODS) + 2)
    print(f"\n{'Country':<25}", end="")
    for label in period_labels:
        short = label.split(":")[0]
        print(f"{short:>{col_width}}", end="")
    print(f"{'Total':>{col_width}}")
    print("-" * (25 + col_width * (len(PERIODS) + 1)))

    for country in sorted_countries:
        print(f"{country:<25}", end="")
        total = 0
        for label in period_labels:
            val = country_counts[label].get(country, 0)
            total += val
            print(f"{val:>{col_width},}", end="")
        print(f"{total:>{col_width},}")

    print("-" * (25 + col_width * (len(PERIODS) + 1)))
    print(f"{'TOTAL':<25}", end="")
    grand = 0
    for label in period_labels:
        t = sum(country_counts[label].values())
        grand += t
        print(f"{t:>{col_width},}", end="")
    print(f"{grand:>{col_width},}")

    # Country count per period
    print(f"\n{'# Countries':<25}", end="")
    for label in period_labels:
        print(f"{len(country_counts[label]):>{col_width},}", end="")
    print(f"{len(sorted_countries):>{col_width},}")


if __name__ == "__main__":
    main()
