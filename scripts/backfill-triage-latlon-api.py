#!/usr/bin/env python3
"""Backfill lat/lon in triage_results by re-querying the Bee Maps search API."""

import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "labels.db"


def load_api_key() -> str:
    key = os.environ.get("BEEMAPS_API_KEY")
    if key:
        return key
    env_local = PROJECT_ROOT / ".env.local"
    if env_local.exists():
        for line in env_local.read_text().splitlines():
            if line.startswith("BEEMAPS_API_KEY="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("BEEMAPS_API_KEY not found")


def api_request(method: str, url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", 30)
    wait = 30
    while True:
        resp = requests.request(method, url, **kwargs)
        if resp.status_code != 403:
            return resp
        print(f"  403 rate-limited — waiting {wait}s...")
        time.sleep(wait)
        wait = min(wait * 2, 300)


def main():
    api_key = load_api_key()
    auth = f"Basic {api_key}"
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
    missing_ids = set(
        r[0] for r in conn.execute(
            "SELECT id FROM triage_results WHERE lat IS NULL OR lon IS NULL"
        ).fetchall()
    )
    print(f"{len(missing_ids)} triage entries missing lat/lon")
    if not missing_ids:
        return

    # Get date range of missing entries
    row = conn.execute(
        "SELECT MIN(event_timestamp), MAX(event_timestamp) FROM triage_results WHERE lat IS NULL OR lon IS NULL"
    ).fetchone()
    print(f"Date range: {row[0]} to {row[1]}")

    # Query search API in 31-day chunks across the full date range
    end = datetime.now(timezone.utc)
    # Go back far enough to cover earliest missing event
    start = datetime.fromisoformat(row[0].replace("Z", "+00:00")) - timedelta(days=1)
    max_chunk = timedelta(days=31)

    chunks = []
    chunk_end = end
    while chunk_end > start:
        chunk_start = max(start, chunk_end - max_chunk)
        chunks.append((chunk_start, chunk_end))
        chunk_end = chunk_start

    updated = 0
    seen = 0

    for ci, (chunk_start, chunk_end) in enumerate(chunks):
        offset = 0
        while True:
            body = {
                "startDate": chunk_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
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

            for evt in events:
                seen += 1
                eid = evt["id"]
                if eid not in missing_ids:
                    continue
                loc = evt.get("location", {})
                lat = loc.get("lat")
                lon = loc.get("lon")
                if lat is not None and lon is not None:
                    conn.execute(
                        "UPDATE triage_results SET lat = ?, lon = ? WHERE id = ?",
                        (lat, lon, eid),
                    )
                    updated += 1
                    missing_ids.discard(eid)

            if updated % 500 == 0 and updated > 0:
                conn.commit()

            print(f"  chunk {ci+1}/{len(chunks)} offset {offset}: scanned {seen}, updated {updated}, remaining {len(missing_ids)}    ", end="\r")
            offset += len(events)

            if len(events) < 500:
                break

            if not missing_ids:
                break

        conn.commit()
        if not missing_ids:
            break

    conn.commit()
    conn.close()
    print(f"\nBackfilled {updated} entries with lat/lon")


if __name__ == "__main__":
    main()
