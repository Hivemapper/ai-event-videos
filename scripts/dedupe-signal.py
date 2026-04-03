#!/usr/bin/env python3
"""
Dedupe signal events — find egregious duplicates (3+ events for one incident).

Two events are potential duplicates if:
  1. Same event_type
  2. Within 100m of each other (haversine)
  3. Within 60 seconds of each other (timestamp)

Usage:
    python3 scripts/dedupe-signal.py [--limit N] [--min-cluster M] [--mark]

    --min-cluster: only report clusters with at least M events (default: 3)
    --mark: tag duplicates in DB with triage_result = 'duplicate'
"""

import argparse
import gzip
import json
import math
import os
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "labels.db"
EVENT_CACHE_DIR = PROJECT_ROOT / "data" / "event-cache"

BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def parse_ts(ts_str: str) -> float:
    if not ts_str:
        return 0
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0


def get_firmware(event_id: str) -> str | None:
    """Try to read firmware version from cached event JSON."""
    for ext in (".json.gz", ".json"):
        fpath = EVENT_CACHE_DIR / f"{event_id}{ext}"
        if not fpath.exists():
            continue
        try:
            if ext == ".json.gz":
                with gzip.open(fpath, "rt") as f:
                    data = json.loads(f.read())
            else:
                with open(fpath) as f:
                    data = json.load(f)
            return data.get("metadata", {}).get("FIRMWARE_VERSION")
        except Exception:
            pass
    return None


def pick_best(cluster: list[dict]) -> dict:
    """Pick the best event to keep from a cluster (largest speed drop)."""
    def score(e):
        smin = e.get("speed_min") or 0
        smax = e.get("speed_max") or 0
        return smax - smin
    return max(cluster, key=score)


def main():
    parser = argparse.ArgumentParser(description="Find egregious duplicate signal events")
    parser.add_argument("--limit", type=int, default=0, help="Limit signal events to check (0 = all)")
    parser.add_argument("--min-cluster", type=int, default=3, help="Min cluster size to report (default: 3)")
    parser.add_argument("--distance", type=float, default=100, help="Max distance in meters (default: 100)")
    parser.add_argument("--window", type=float, default=60, help="Max time window in seconds (default: 60)")
    parser.add_argument("--mark", action="store_true", help="Tag duplicates in DB as triage_result='duplicate'")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row

    query = """SELECT id, event_type, event_timestamp, lat, lon,
                      speed_min, speed_max, speed_mean, gnss_displacement_m
               FROM triage_results
               WHERE triage_result = 'signal'
                 AND lat IS NOT NULL AND lon IS NOT NULL
                 AND event_timestamp IS NOT NULL
               ORDER BY event_timestamp"""
    if args.limit:
        query += f" LIMIT {args.limit}"

    rows = conn.execute(query).fetchall()
    print(f"Loaded {len(rows)} signal events")
    print(f"Thresholds: {args.distance}m, {args.window}s, min cluster size {args.min_cluster}")
    print()

    # Group by event_type, sort by timestamp
    events_by_type: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        evt = dict(row)
        evt["_ts"] = parse_ts(evt["event_timestamp"])
        events_by_type[evt["event_type"]].append(evt)

    for etype in events_by_type:
        events_by_type[etype].sort(key=lambda e: e["_ts"])

    # Cluster
    all_clusters: list[list[dict]] = []
    seen: set[str] = set()

    for etype, events in events_by_type.items():
        for i, evt_a in enumerate(events):
            if evt_a["id"] in seen:
                continue
            cluster = [evt_a]
            seen.add(evt_a["id"])

            for j in range(i + 1, len(events)):
                evt_b = events[j]
                if evt_b["id"] in seen:
                    continue
                if evt_b["_ts"] - evt_a["_ts"] > args.window:
                    break
                # Check distance against any event already in cluster
                close = False
                for c in cluster:
                    if haversine(c["lat"], c["lon"], evt_b["lat"], evt_b["lon"]) <= args.distance:
                        close = True
                        break
                if close:
                    cluster.append(evt_b)
                    seen.add(evt_b["id"])

            if len(cluster) >= args.min_cluster:
                all_clusters.append(cluster)

    # Stats
    total_dupes = sum(len(c) - 1 for c in all_clusters)
    total_in_clusters = sum(len(c) for c in all_clusters)

    print(f"{BOLD}{'═' * 70}{RESET}")
    print(f"  Events checked:       {len(rows)}")
    print(f"  Egregious clusters:   {len(all_clusters)}  (≥{args.min_cluster} events each)")
    print(f"  Events in clusters:   {total_in_clusters}")
    print(f"  Duplicate events:     {RED}{total_dupes}{RESET}")
    print(f"  Dupe rate:            {YELLOW}{100 * total_dupes / len(rows):.1f}%{RESET}")
    print(f"{BOLD}{'═' * 70}{RESET}")
    print()

    # Cluster size distribution
    size_dist: dict[int, int] = defaultdict(int)
    for c in all_clusters:
        size_dist[len(c)] += 1
    if size_dist:
        print("Cluster sizes:")
        for size in sorted(size_dist):
            count = size_dist[size]
            dupes_at_size = (size - 1) * count
            print(f"  {size:>3} events × {count:>4} clusters = {dupes_at_size:>5} duplicates")
        print()

    # By event type
    type_dupes: dict[str, int] = defaultdict(int)
    type_clusters: dict[str, int] = defaultdict(int)
    for c in all_clusters:
        etype = c[0]["event_type"]
        type_dupes[etype] += len(c) - 1
        type_clusters[etype] += 1
    if type_dupes:
        print("By event type:")
        for etype in sorted(type_dupes, key=lambda t: -type_dupes[t]):
            type_total = len(events_by_type[etype])
            pct = 100 * type_dupes[etype] / type_total if type_total else 0
            print(f"  {etype:<30} {type_dupes[etype]:>5} dupes in {type_clusters[etype]:>4} clusters ({pct:.1f}% of {type_total})")
        print()

    # Firmware analysis on worst clusters
    all_clusters.sort(key=lambda c: -len(c))
    print(f"{'─' * 70}")
    print(f"Top 15 clusters:")
    print(f"{'─' * 70}")
    fw_counts: dict[str, int] = defaultdict(int)

    for ci, cluster in enumerate(all_clusters[:15]):
        etype = cluster[0]["event_type"]
        best = pick_best(cluster)
        fw = get_firmware(cluster[0]["id"])
        fw_str = fw or "?"
        if fw:
            fw_counts[fw] += 1

        span_m = haversine(cluster[0]["lat"], cluster[0]["lon"],
                           cluster[-1]["lat"], cluster[-1]["lon"])
        span_s = abs(cluster[-1]["_ts"] - cluster[0]["_ts"])

        speed = ""
        if best["speed_max"] is not None and best["speed_min"] is not None:
            speed = f"{best['speed_max']:.0f}→{best['speed_min']:.0f} mph"

        print(
            f"\n  {CYAN}#{ci+1}{RESET}  {len(cluster)} events  │  {etype}  │  {speed}  │  "
            f"{span_m:.0f}m / {span_s:.0f}s span  │  fw {fw_str}"
        )
        print(f"       keep: http://localhost:3000/event/{best['id']}")
        ts_short = (best["event_timestamp"] or "")[:19]
        print(f"       {DIM}{best['lat']:.5f}, {best['lon']:.5f}  {ts_short}{RESET}")

    # Firmware summary
    if fw_counts:
        print(f"\n{'─' * 70}")
        print("Firmware versions in top clusters:")
        for fw, count in sorted(fw_counts.items(), key=lambda x: -x[1]):
            print(f"  {fw}: {count} clusters")

    # Mark duplicates if requested
    if args.mark and all_clusters:
        print(f"\n{YELLOW}Marking {total_dupes} duplicates in DB...{RESET}")
        marked = 0
        for cluster in all_clusters:
            best = pick_best(cluster)
            for evt in cluster:
                if evt["id"] != best["id"]:
                    conn.execute(
                        "UPDATE triage_results SET triage_result = 'duplicate' WHERE id = ?",
                        (evt["id"],)
                    )
                    marked += 1
        conn.commit()
        print(f"{GREEN}Marked {marked} events as 'duplicate'{RESET}")

    conn.close()


if __name__ == "__main__":
    main()
