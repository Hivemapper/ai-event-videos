#!/usr/bin/env python3
"""Backfill road_class for signal events missing it via Mapbox Tilequery API."""

import os
import sqlite3
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "labels.db"

NON_DRIVABLE_ROADS = {"path", "pedestrian", "track"}
ROAD_CLASS_RANK = {
    "motorway": 10, "motorway_link": 9,
    "trunk": 8, "trunk_link": 7,
    "primary": 6, "primary_link": 5,
    "secondary": 4, "secondary_link": 3,
    "tertiary": 2, "tertiary_link": 1,
    "street": 0, "street_limited": 0,
    "service": -1, "path": -2, "pedestrian": -3, "track": -4,
}


def load_mapbox_token() -> str:
    token = os.environ.get("NEXT_PUBLIC_MAPBOX_TOKEN")
    if token:
        return token
    env_local = PROJECT_ROOT / ".env.local"
    if env_local.exists():
        for line in env_local.read_text().splitlines():
            if line.startswith("NEXT_PUBLIC_MAPBOX_TOKEN="):
                val = line.split("=", 1)[1].strip()
                if val and val != "your_mapbox_token_here":
                    return val
    raise RuntimeError("NEXT_PUBLIC_MAPBOX_TOKEN not found")


def query_road_class(lat: float, lon: float, token: str) -> str | None:
    try:
        url = (
            f"https://api.mapbox.com/v4/mapbox.mapbox-streets-v8/tilequery/"
            f"{lon},{lat}.json?layers=road&radius=50&limit=5&access_token={token}"
        )
        resp = requests.get(url, timeout=10)
        if not resp.ok:
            return None
        features = resp.json().get("features", [])
        if not features:
            return None

        best_class = None
        best_rank = -999
        for f in features:
            cls = f.get("properties", {}).get("class")
            if not cls:
                continue
            rank = ROAD_CLASS_RANK.get(cls, -5)
            if cls not in NON_DRIVABLE_ROADS and rank > best_rank:
                best_rank = rank
                best_class = cls
        if best_class is None:
            best_class = features[0].get("properties", {}).get("class")
        return best_class
    except Exception:
        return None


def main():
    token = load_mapbox_token()
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    # Add column if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(triage_results)").fetchall()}
    if "road_class" not in cols:
        conn.execute("ALTER TABLE triage_results ADD COLUMN road_class TEXT")
        conn.commit()

    rows = conn.execute(
        """SELECT id, lat, lon FROM triage_results
           WHERE triage_result = 'signal'
             AND road_class IS NULL
             AND lat IS NOT NULL AND lon IS NOT NULL"""
    ).fetchall()

    print(f"{len(rows)} signal events missing road_class")
    if not rows:
        return

    updated = 0
    errors = 0
    t0 = time.time()

    pending_updates: list[tuple[str, str]] = []

    for i, (eid, lat, lon) in enumerate(rows):
        road_class = query_road_class(lat, lon, token)
        if road_class:
            pending_updates.append((road_class, eid))
            updated += 1
        else:
            errors += 1

        if (i + 1) % 50 == 0 or i == len(rows) - 1:
            # Batch write with retry on lock
            for attempt in range(5):
                try:
                    for rc, eid_ in pending_updates:
                        conn.execute(
                            "UPDATE triage_results SET road_class = ? WHERE id = ?",
                            (rc, eid_),
                        )
                    conn.commit()
                    pending_updates.clear()
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e) and attempt < 4:
                        time.sleep(5 * (attempt + 1))
                    elif "locked" in str(e):
                        # Last resort: reconnect
                        conn.close()
                        conn = sqlite3.connect(str(DB_PATH))
                        conn.execute("PRAGMA journal_mode=WAL")
                        conn.execute("PRAGMA busy_timeout=30000")
                    else:
                        raise

            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            remaining = (len(rows) - i - 1) / rate if rate > 0 else 0
            print(f"  {i+1}/{len(rows)} — {updated} updated, {errors} no data — {rate:.0f} req/s, ~{remaining/60:.0f}m remaining    ", end="\r", flush=True)

    conn.close()
    elapsed = time.time() - t0
    print(f"\nDone: {updated} updated, {errors} no data in {elapsed:.0f}s ({len(rows)/elapsed:.0f} req/s)")


if __name__ == "__main__":
    main()
