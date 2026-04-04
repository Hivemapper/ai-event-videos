#!/usr/bin/env python3
"""Backfill lat/lon in triage_results from cached event JSONs."""

import gzip
import json
import os
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "labels.db"
EVENT_CACHE_DIR = PROJECT_ROOT / "data" / "event-cache"

def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    # Add columns if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(triage_results)").fetchall()}
    if "lat" not in cols:
        conn.execute("ALTER TABLE triage_results ADD COLUMN lat REAL")
    if "lon" not in cols:
        conn.execute("ALTER TABLE triage_results ADD COLUMN lon REAL")
    conn.commit()

    # Get IDs missing lat/lon
    rows = conn.execute(
        "SELECT id FROM triage_results WHERE lat IS NULL OR lon IS NULL"
    ).fetchall()
    missing = {r[0] for r in rows}
    print(f"{len(missing)} triage entries missing lat/lon")

    updated = 0
    for fname in os.listdir(EVENT_CACHE_DIR):
        eid = fname.replace(".json.gz", "").replace(".json", "")
        if eid not in missing:
            continue

        fpath = EVENT_CACHE_DIR / fname
        try:
            if fname.endswith(".gz"):
                with gzip.open(fpath, "rt") as f:
                    data = json.loads(f.read())
            else:
                with open(fpath) as f:
                    data = json.load(f)
        except Exception:
            continue

        loc = data.get("location", {})
        lat = loc.get("lat")
        lon = loc.get("lon")
        if lat is not None and lon is not None:
            conn.execute(
                "UPDATE triage_results SET lat = ?, lon = ? WHERE id = ?",
                (lat, lon, eid),
            )
            updated += 1
            if updated % 1000 == 0:
                conn.commit()
                print(f"  updated {updated}...", end="\r")

    conn.commit()
    conn.close()
    print(f"Backfilled {updated} entries with lat/lon")

if __name__ == "__main__":
    main()
