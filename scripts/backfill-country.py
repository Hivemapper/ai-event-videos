#!/usr/bin/env python3
"""Backfill country and city for triage events using geo-cache and Mapbox reverse geocoding."""

import json
import os
import sqlite3
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "labels.db"
GEO_CACHE_PATH = PROJECT_ROOT / "data" / "geo-cache.json"
CITY_CACHE_PATH = PROJECT_ROOT / "data" / "city-cache.json"


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
            conn = libsql.connect("backfill-country.db", sync_url=http_url, auth_token=turso_token)
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


def load_mapbox_token() -> str | None:
    for var in ("MAPBOX_TOKEN", "NEXT_PUBLIC_MAPBOX_TOKEN"):
        token = os.environ.get(var)
        if token:
            return token
    env_local = PROJECT_ROOT / ".env.local"
    if env_local.exists():
        for line in env_local.read_text().splitlines():
            for prefix in ("MAPBOX_TOKEN=", "NEXT_PUBLIC_MAPBOX_TOKEN="):
                if line.startswith(prefix):
                    val = line.split("=", 1)[1].strip()
                    if val and val != "your_mapbox_token_here":
                        return val
    return None


def reverse_geocode(lat: float, lon: float, token: str) -> tuple[str | None, str | None]:
    """Mapbox reverse geocode returning (country, city)."""
    try:
        resp = requests.get(
            f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
            f"?types=place,country&access_token={token}",
            timeout=10,
        )
        if resp.status_code != 200:
            return None, None
        features = resp.json().get("features", [])
        country = None
        city = None
        for f in features:
            place_type = f.get("place_type", [])
            name = f.get("place_name") or f.get("text")
            if "country" in place_type:
                country = name
            elif "place" in place_type:
                # place_name includes region/country suffix — use "text" for just the city name
                city = f.get("text") or name
        return country, city
    except Exception:
        return None, None


def main():
    # Load caches
    geo_cache: dict[str, str] = {}
    if GEO_CACHE_PATH.exists():
        geo_cache = json.load(open(GEO_CACHE_PATH))
    print(f"Loaded geo-cache: {len(geo_cache)} entries")

    city_cache: dict[str, str] = {}
    if CITY_CACHE_PATH.exists():
        city_cache = json.load(open(CITY_CACHE_PATH))
    print(f"Loaded city-cache: {len(city_cache)} entries")

    mapbox_token = load_mapbox_token()
    if not mapbox_token:
        print("Warning: No Mapbox token — will only use caches (no API fallback)")

    conn, is_turso = get_db_conn()

    # Reconnect threshold — Turso streams die after ~30k writes
    RECONNECT_EVERY = 15000

    # Add columns if missing (skip for Turso — columns already exist)
    if not is_turso:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(triage_results)").fetchall()}
        for col in ("country", "city"):
            if col not in cols:
                conn.execute(f"ALTER TABLE triage_results ADD COLUMN {col} TEXT")
                conn.commit()
                print(f"Added '{col}' column to triage_results")

    # --- Phase 1: Country from geo-cache ---
    rows_no_country = conn.execute(
        """SELECT id, lat, lon FROM triage_results
           WHERE country IS NULL AND lat IS NOT NULL AND lon IS NOT NULL"""
    ).fetchall()
    print(f"\n{len(rows_no_country)} events missing country")

    if rows_no_country:
        updated = 0
        writes_since_reconnect = 0
        for i in range(0, len(rows_no_country), 500):
            batch = rows_no_country[i : i + 500]
            for row in batch:
                eid = row[0] if isinstance(row, tuple) else row["id"]
                lat = row[1] if isinstance(row, tuple) else row["lat"]
                lon = row[2] if isinstance(row, tuple) else row["lon"]
                key = f"{round(lat, 2)},{round(lon, 2)}"
                country = geo_cache.get(key)
                if country and country != "Unknown":
                    conn.execute("UPDATE triage_results SET country = ? WHERE id = ?", (country, eid))
                    updated += 1
                    writes_since_reconnect += 1
            conn.commit()
            if is_turso:
                conn.sync()
                if writes_since_reconnect >= RECONNECT_EVERY:
                    conn, is_turso = get_db_conn()
                    writes_since_reconnect = 0
            if (i // 500) % 10 == 0:
                print(f"  Country: {updated} updated so far ({i + len(batch)}/{len(rows_no_country)})...", end="\r", flush=True)
        print(f"  Set country from geo-cache: {updated}                          ")

    # --- Phase 2: City from city-cache ---
    rows_no_city = conn.execute(
        """SELECT id, lat, lon FROM triage_results
           WHERE city IS NULL AND lat IS NOT NULL AND lon IS NOT NULL"""
    ).fetchall()
    print(f"{len(rows_no_city)} events missing city")

    if rows_no_city:
        updated = 0
        writes_since_reconnect = 0
        for i in range(0, len(rows_no_city), 500):
            batch = rows_no_city[i : i + 500]
            for row in batch:
                eid = row[0] if isinstance(row, tuple) else row["id"]
                lat = row[1] if isinstance(row, tuple) else row["lat"]
                lon = row[2] if isinstance(row, tuple) else row["lon"]
                key = f"{round(lat, 2)},{round(lon, 2)}"
                city = city_cache.get(key)
                if city:
                    conn.execute("UPDATE triage_results SET city = ? WHERE id = ?", (city, eid))
                    updated += 1
                    writes_since_reconnect += 1
            conn.commit()
            if is_turso:
                conn.sync()
                if writes_since_reconnect >= RECONNECT_EVERY:
                    conn, is_turso = get_db_conn()
                    writes_since_reconnect = 0
            if (i // 500) % 10 == 0:
                print(f"  City: {updated} updated so far ({i + len(batch)}/{len(rows_no_city)})...", end="\r", flush=True)
        print(f"  Set city from city-cache: {updated}                          ")

    # --- Phase 3: Query Mapbox for unique coords still missing city or country ---
    missing = conn.execute(
        """SELECT DISTINCT ROUND(lat,2) as rlat, ROUND(lon,2) as rlon
           FROM triage_results
           WHERE (city IS NULL OR country IS NULL)
             AND lat IS NOT NULL AND lon IS NOT NULL"""
    ).fetchall()
    print(f"\n{len(missing)} unique coords need Mapbox lookup")

    if missing and mapbox_token:
        t0 = time.time()
        queried = 0
        new_countries = 0
        new_cities = 0

        for i, row in enumerate(missing):
            rlat = row[0] if isinstance(row, tuple) else row["rlat"]
            rlon = row[1] if isinstance(row, tuple) else row["rlon"]
            key = f"{rlat},{rlon}"
            country, city = reverse_geocode(rlat, rlon, mapbox_token)

            if country and key not in geo_cache:
                geo_cache[key] = country
                new_countries += 1
            if city:
                city_cache[key] = city
                new_cities += 1

            # Update all rows matching this rounded coord
            if country or city:
                updates = []
                params = []
                if country:
                    updates.append("country = COALESCE(country, ?)")
                    params.append(country)
                if city:
                    updates.append("city = COALESCE(city, ?)")
                    params.append(city)
                params.extend([rlat, rlon])
                conn.execute(
                    f"""UPDATE triage_results SET {', '.join(updates)}
                        WHERE ROUND(lat,2) = ? AND ROUND(lon,2) = ?""",
                    params,
                )

            queried += 1
            if queried % 50 == 0 or i == len(missing) - 1:
                conn.commit()
                if is_turso:
                    conn.sync()
                elapsed = time.time() - t0
                rate = queried / elapsed if elapsed > 0 else 0
                remaining = (len(missing) - queried) / rate if rate > 0 else 0
                print(
                    f"  {queried}/{len(missing)} — {new_cities} cities, {new_countries} countries — "
                    f"{rate:.0f} req/s, ~{remaining/60:.0f}m remaining    ",
                    end="\r", flush=True,
                )

        conn.commit()
        if is_turso:
            conn.sync()
        print()

        # Save updated caches
        with open(GEO_CACHE_PATH, "w") as f:
            json.dump(geo_cache, f, separators=(",", ":"))
        with open(CITY_CACHE_PATH, "w") as f:
            json.dump(city_cache, f, separators=(",", ":"))
        print(f"Saved caches: {len(geo_cache)} country, {len(city_cache)} city entries")

    elif missing:
        print(f"Skipped {len(missing)} coords (no Mapbox token)")

    # --- Summary ---
    stats = conn.execute(
        """SELECT
             COUNT(*) as total,
             COUNT(country) as has_country,
             COUNT(city) as has_city
           FROM triage_results WHERE lat IS NOT NULL"""
    ).fetchone()
    total = stats[0] if isinstance(stats, tuple) else stats["total"]
    has_country = stats[1] if isinstance(stats, tuple) else stats["has_country"]
    has_city = stats[2] if isinstance(stats, tuple) else stats["has_city"]
    print(f"\nFinal: {total} events with coords — {has_country} have country, {has_city} have city")


if __name__ == "__main__":
    main()
