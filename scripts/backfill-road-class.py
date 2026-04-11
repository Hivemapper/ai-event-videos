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


def _load_env_var(name: str) -> str | None:
    val = os.environ.get(name)
    if val:
        return val
    env_local = PROJECT_ROOT / ".env.local"
    if env_local.exists():
        for line in env_local.read_text().splitlines():
            if line.startswith(f"{name}="):
                return line.split("=", 1)[1].strip()
    return None


def get_db_conn():
    """Connect to Turso if configured, otherwise local SQLite."""
    turso_url = _load_env_var("TURSO_DATABASE_URL")
    turso_token = _load_env_var("TURSO_AUTH_TOKEN")

    if turso_url and turso_token:
        try:
            import libsql_experimental as libsql
            http_url = turso_url.replace("libsql://", "https://")
            conn = libsql.connect("backfill-road-class.db", sync_url=http_url, auth_token=turso_token)
            conn.sync()
            print(f"  DB: Turso (embedded replica)")
            return conn, True
        except ImportError:
            pass
        try:
            import libsql_client
            http_url = turso_url.replace("libsql://", "https://")
            conn = libsql_client.create_client_sync(url=http_url, auth_token=turso_token)
            print(f"  DB: Turso (HTTP)")
            return conn, True
        except ImportError:
            print("Warning: No libsql library found, falling back to local SQLite")

    print(f"  DB: Local SQLite ({DB_PATH})")
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn, False


def load_mapbox_token() -> str:
    for var in ("MAPBOX_TOKEN", "NEXT_PUBLIC_MAPBOX_TOKEN"):
        token = _load_env_var(var)
        if token and token != "your_mapbox_token_here":
            return token
    raise RuntimeError("MAPBOX_TOKEN / NEXT_PUBLIC_MAPBOX_TOKEN not found")


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
    conn, is_turso = get_db_conn()

    # Add column if missing (skip for Turso — column already exists)
    if not is_turso:
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

    for i, row in enumerate(rows):
        eid = row[0] if isinstance(row, tuple) else row["id"]
        lat = row[1] if isinstance(row, tuple) else row["lat"]
        lon = row[2] if isinstance(row, tuple) else row["lon"]

        road_class = query_road_class(lat, lon, token)
        if road_class:
            pending_updates.append((road_class, eid))
            updated += 1
        else:
            errors += 1

        if (i + 1) % 50 == 0 or i == len(rows) - 1:
            for rc, eid_ in pending_updates:
                conn.execute(
                    "UPDATE triage_results SET road_class = ? WHERE id = ?",
                    (rc, eid_),
                )
            conn.commit()
            if is_turso:
                conn.sync()
            pending_updates.clear()

            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            remaining = (len(rows) - i - 1) / rate if rate > 0 else 0
            print(f"  {i+1}/{len(rows)} — {updated} updated, {errors} no data — {rate:.0f} req/s, ~{remaining/60:.0f}m remaining    ", end="\r", flush=True)

    elapsed = time.time() - t0
    print(f"\nDone: {updated} updated, {errors} no data in {elapsed:.0f}s ({len(rows)/elapsed:.0f} req/s)")


if __name__ == "__main__":
    main()
