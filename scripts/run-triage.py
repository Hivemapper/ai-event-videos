#!/usr/bin/env python3
"""
Phase 0 Triage — classify events as Missing Video, Ghost, Open Road, or Signal.

Usage:
    python3 scripts/run-triage.py <num_events> [--days N]

Examples:
    python3 scripts/run-triage.py 500
    python3 scripts/run-triage.py 500 --days 7
"""

import argparse
import gzip
import json
import math
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, stdev

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "labels.db"
EVENT_CACHE_DIR = PROJECT_ROOT / "data" / "event-cache"
GEO_CACHE_PATH = PROJECT_ROOT / "data" / "geo-cache.json"
CITY_CACHE_PATH = PROJECT_ROOT / "data" / "city-cache.json"

_GEO_CACHE: dict[str, str] | None = None
_CITY_CACHE: dict[str, str] | None = None


class _TursoCursor:
    def __init__(self, result_set):
        self._rows = result_set.rows if result_set else []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _TursoDb:
    """HTTP wrapper around libsql_client with a sqlite3-like interface."""

    def __init__(self, client):
        self._client = client

    def execute(self, sql, params=None):
        import libsql_client
        if params:
            rs = self._client.execute(libsql_client.Statement(sql, list(params)))
        else:
            rs = self._client.execute(sql)
        return _TursoCursor(rs)

    def commit(self):
        pass

    def close(self):
        self._client.close()


def get_db_conn():
    """Connect to Turso if configured, otherwise local SQLite."""
    turso_url = os.environ.get("TURSO_DATABASE_URL")
    turso_token = os.environ.get("TURSO_AUTH_TOKEN")

    # Also check .env.local
    if not turso_url:
        env_local = PROJECT_ROOT / ".env.local"
        if env_local.exists():
            for line in env_local.read_text().splitlines():
                if line.startswith("TURSO_DATABASE_URL="):
                    turso_url = line.split("=", 1)[1].strip()
                elif line.startswith("TURSO_AUTH_TOKEN="):
                    turso_token = line.split("=", 1)[1].strip()

    if turso_url and turso_token:
        # Prefer HTTP mode (no local replica file, avoids WAL corruption on crash)
        try:
            import libsql_client
            http_url = turso_url.replace("libsql://", "https://")
            client = libsql_client.create_client_sync(url=http_url, auth_token=turso_token)
            conn = _TursoDb(client)
            print(f"  {CYAN}Connected to Turso HTTP: {turso_url.split('//')[1][:40]}…{RESET}")
            return conn
        except ImportError:
            pass
        # Fall back to embedded replica
        import libsql_experimental as libsql  # type: ignore
        conn = libsql.connect("local.db", sync_url=turso_url, auth_token=turso_token)
        conn.sync()
        print(f"  {CYAN}Connected to Turso: {turso_url.split('//')[1][:40]}…{RESET}")
        return conn

    print(f"  Using local SQLite: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

MS_TO_MPH = 2.237
G = 9.81  # m/s²
MIN_VIDEO_SIZE = 500 * 1024  # 500KB — files at or below this aren't real videos

# US-sanctioned countries — events from these are rejected at triage
SANCTIONED_COUNTRIES = {
    "Cuba", "Iran", "North Korea", "Belarus", "Russia",
    "Yemen", "Nicaragua", "Libya", "Crimea",
}

# Event types that imply speed change
SPEED_CHANGE_TYPES = {
    "HARSH_BRAKING", "AGGRESSIVE_ACCELERATION", "HIGH_G_FORCE",
    "SWERVING", "HARSH_CORNERING",
}

# ANSI colors
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
CYAN = "\033[36m"
RESET = "\033[0m"


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


def load_mapbox_token() -> str | None:
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
    return None


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


def query_road_class(lat: float, lon: float, token: str) -> str | None:
    """Query Mapbox Tilequery for road class at a coordinate."""
    try:
        url = (
            f"https://api.mapbox.com/v4/mapbox.mapbox-streets-v8/tilequery/"
            f"{lon},{lat}.json?layers=road&radius=25&limit=5&access_token={token}"
        )
        resp = requests.get(url, timeout=10)
        if not resp.ok:
            return None
        features = resp.json().get("features", [])
        if not features:
            return None

        # Pick the highest-ranked drivable road
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
        # Fall back to first feature if all non-drivable
        if best_class is None:
            best_class = features[0].get("properties", {}).get("class")
        return best_class
    except Exception:
        return None


def lookup_location(lat: float, lon: float, token: str | None) -> tuple[str | None, str | None]:
    """Look up country and city from caches, with Mapbox reverse geocode fallback."""
    global _GEO_CACHE, _CITY_CACHE
    if _GEO_CACHE is None:
        _GEO_CACHE = json.load(open(GEO_CACHE_PATH)) if GEO_CACHE_PATH.exists() else {}
    if _CITY_CACHE is None:
        _CITY_CACHE = json.load(open(CITY_CACHE_PATH)) if CITY_CACHE_PATH.exists() else {}

    key = f"{round(lat, 2)},{round(lon, 2)}"
    country = _GEO_CACHE.get(key)
    if country == "Unknown":
        country = None
    city = _CITY_CACHE.get(key)

    # If both cached, return early
    if country and city:
        return country, city

    # Mapbox reverse geocode fallback
    if token:
        try:
            resp = requests.get(
                f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
                f"?types=place,country&access_token={token}",
                timeout=10,
            )
            if resp.status_code == 200:
                for f in resp.json().get("features", []):
                    place_type = f.get("place_type", [])
                    if "country" in place_type and not country:
                        country = f.get("place_name") or f.get("text")
                        if country:
                            _GEO_CACHE[key] = country
                    elif "place" in place_type and not city:
                        city = f.get("text") or f.get("place_name")
                        if city:
                            _CITY_CACHE[key] = city
        except Exception:
            pass

    return country, city


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in meters between two GPS points."""
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def compute_heading(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Bearing in degrees from point 1 to point 2."""
    dlon = math.radians(lon2 - lon1)
    lat1r, lat2r = math.radians(lat1), math.radians(lat2)
    x = math.sin(dlon) * math.cos(lat2r)
    y = (math.cos(lat1r) * math.sin(lat2r) -
         math.sin(lat1r) * math.cos(lat2r) * math.cos(dlon))
    return math.degrees(math.atan2(x, y)) % 360


def derive_speed_from_gnss(gnss: list[dict]) -> list[dict]:
    """Compute speed array from consecutive GNSS points (haversine distance / dt)."""
    if len(gnss) < 2:
        return []
    result = []
    for i in range(1, len(gnss)):
        prev, curr = gnss[i - 1], gnss[i]
        dt = (curr["timestamp"] - prev["timestamp"]) / 1000  # seconds
        if dt <= 0:
            continue
        dist = haversine(prev["lat"], prev["lon"], curr["lat"], curr["lon"])
        result.append({"AVG_SPEED_MS": dist / dt, "TIMESTAMP": curr["timestamp"]})
    return result


def check_video(video_url: str | None) -> tuple[bool, int | None]:
    """Check video size via a 1-byte Range GET. Returns (is_valid, total_size).

    Uses `Range: bytes=0-0` instead of HEAD because the video CDN (Cloudflare)
    returns 405 for HEAD requests. The Range GET downloads only 1 byte and the
    `Content-Range` header reveals the full file size.

    A video is considered missing/invalid if:
      - No URL provided
      - Request fails or returns non-206
      - Total size is <= MIN_VIDEO_SIZE (500KB)
      - Response content-type is not video/*
    """
    if not video_url:
        return False, None
    try:
        resp = api_request(
            "GET",
            video_url,
            headers={"Range": "bytes=0-0"},
            timeout=10,
            allow_redirects=True,
        )
        if resp.status_code not in (200, 206):
            return False, None
        content_type = resp.headers.get("content-type", "")
        if "text/html" in content_type:
            return False, None

        # Extract total size from Content-Range: bytes 0-0/TOTAL
        content_range = resp.headers.get("content-range", "")
        if "/" in content_range:
            total_str = content_range.rsplit("/", 1)[1]
            if total_str != "*":
                size = int(total_str)
                return size > MIN_VIDEO_SIZE, size

        # Fallback: Content-Length on a 200 response (no range support)
        if resp.status_code == 200:
            length = resp.headers.get("content-length")
            if length:
                size = int(length)
                return size > MIN_VIDEO_SIZE, size

        # Can't determine size — assume valid
        return True, None
    except Exception:
        return False, None


def triage_event(event: dict) -> tuple[str, dict]:
    """Classify event as ghost, open_road, or signal.

    Returns (classification, details_dict).
    """
    event_type = event.get("type", "")
    speeds_raw = event.get("metadata", {}).get("SPEED_ARRAY", [])
    gnss = event.get("gnssData") or []
    imu = event.get("imuData") or []

    # Missing metadata — can't triage without telemetry
    missing = []
    if len(gnss) < 2:
        missing.append("no_gnss")
    if not imu:
        missing.append("no_imu")
    if missing:
        return "missing_metadata", {
            "event_type": event_type,
            "rules": missing,
            "speed_min": None, "speed_max": None,
            "speed_mean": None, "speed_stddev": None,
            "gnss_displacement_m": None,
        }

    # Derive speed from GNSS if SPEED_ARRAY is missing
    if not speeds_raw and len(gnss) >= 2:
        speeds_raw = derive_speed_from_gnss(gnss)

    # Extract speed values in mph
    speeds_mph = [s.get("AVG_SPEED_MS", 0) * MS_TO_MPH for s in speeds_raw]
    speed_timestamps = [s.get("TIMESTAMP", 0) for s in speeds_raw]

    details: dict = {
        "event_type": event_type,
        "speed_count": len(speeds_mph),
        "gnss_count": len(gnss),
        "imu_count": len(imu),
    }

    # Compute speed stats
    if speeds_mph:
        details["speed_min"] = round(min(speeds_mph), 1)
        details["speed_max"] = round(max(speeds_mph), 1)
        details["speed_mean"] = round(mean(speeds_mph), 1)
        details["speed_stddev"] = round(stdev(speeds_mph), 1) if len(speeds_mph) > 1 else 0.0
    else:
        details["speed_min"] = details["speed_max"] = details["speed_mean"] = details["speed_stddev"] = None

    # === GHOST CHECKS (any = ghost) ===
    ghost_rules: list[str] = []

    # Rule 0: Unrealistic speed (> 150 mph) — sensor glitch
    if speeds_mph and max(speeds_mph) > 150:
        ghost_rules.append("unrealistic_speed")

    # Rule 1: GNSS displacement < 5m
    if len(gnss) >= 2:
        displacement = haversine(
            gnss[0]["lat"], gnss[0]["lon"],
            gnss[-1]["lat"], gnss[-1]["lon"]
        )
        details["gnss_displacement_m"] = round(displacement, 1)
        if displacement < 5.0:
            ghost_rules.append("gnss_no_movement")

    # Rule 2: Zero speed entire clip
    if speeds_mph and all(s < 0.5 for s in speeds_mph):
        ghost_rules.append("zero_speed")

    # Rule 3: Constant speed + event implies speed change
    if speeds_mph and len(speeds_mph) > 1:
        sd = stdev(speeds_mph)
        if sd < 2.0 and event_type in SPEED_CHANGE_TYPES:
            ghost_rules.append("constant_speed")

    # Rule 4: Uncorrelated IMU spike
    if imu and speeds_raw:
        # Find largest acceleration spike
        accel_magnitudes = []
        for point in imu:
            acc = point.get("accelerometer") or {}
            if acc:
                mag = math.sqrt(acc.get("x", 0)**2 + acc.get("y", 0)**2 + acc.get("z", 0)**2)
                accel_magnitudes.append((point.get("timestamp", 0), abs(mag - G)))
            else:
                # Flat fields (acc_x, acc_y, acc_z)
                ax = point.get("acc_x", point.get("accel_x", 0)) or 0
                ay = point.get("acc_y", point.get("accel_y", 0)) or 0
                az = point.get("acc_z", point.get("accel_z", 0)) or 0
                mag = math.sqrt(float(ax)**2 + float(ay)**2 + float(az)**2)
                accel_magnitudes.append((point.get("timestamp", 0), abs(mag - G)))

        if accel_magnitudes:
            peak_ts, peak_mag = max(accel_magnitudes, key=lambda x: x[1])
            # Check if spike is isolated (< 0.5s of high accel)
            spike_duration = sum(1 for _, m in accel_magnitudes
                                 if m > peak_mag * 0.5) * (30000 / max(len(accel_magnitudes), 1))

            if spike_duration < 500 and peak_mag > 0.3 * G:
                # Check for corresponding GNSS change within 2s window
                window_speeds = [
                    s for s, t in zip(speeds_mph, speed_timestamps)
                    if abs(t - peak_ts) < 2000
                ]
                if len(window_speeds) >= 2:
                    speed_delta = max(window_speeds) - min(window_speeds)
                else:
                    speed_delta = 0

                # Check heading change
                heading_delta = 0.0
                if len(gnss) >= 2:
                    window_gnss = [g for g in gnss if abs(g["timestamp"] - peak_ts) < 2000]
                    if len(window_gnss) >= 2:
                        h1 = compute_heading(
                            window_gnss[0]["lat"], window_gnss[0]["lon"],
                            window_gnss[1]["lat"], window_gnss[1]["lon"]
                        )
                        h2 = compute_heading(
                            window_gnss[-2]["lat"], window_gnss[-2]["lon"],
                            window_gnss[-1]["lat"], window_gnss[-1]["lon"]
                        )
                        heading_delta = abs(h2 - h1)
                        if heading_delta > 180:
                            heading_delta = 360 - heading_delta

                if speed_delta < 2.0 and heading_delta < 5.0:
                    ghost_rules.append("uncorrelated_imu_spike")

    # Rule 5: Speed contradicts event type
    # Use max speed drop/gain anywhere in the clip, not just start vs end,
    # since the event may occur in the middle of the 30s clip.
    if speeds_mph and len(speeds_mph) >= 2:
        running_max = speeds_mph[0]
        largest_drop = 0.0
        running_min = speeds_mph[0]
        largest_gain = 0.0
        for s in speeds_mph[1:]:
            largest_drop = max(largest_drop, running_max - s)
            running_max = max(running_max, s)
            largest_gain = max(largest_gain, s - running_min)
            running_min = min(running_min, s)
        if event_type == "HARSH_BRAKING" and largest_drop < 2.0:
            ghost_rules.append("speed_contradicts_braking")
        elif event_type == "AGGRESSIVE_ACCELERATION" and largest_gain < 2.0:
            ghost_rules.append("speed_contradicts_acceleration")

    if ghost_rules:
        details["rules"] = ghost_rules
        return "ghost", details

    # === OPEN ROAD CHECKS (all required) ===
    # HIGH_SPEED events are never Open Road — the speed itself is the incident
    if event_type == "HIGH_SPEED":
        pass  # skip Open Road, fall through to Signal
    elif speeds_mph and len(speeds_mph) >= 2:
        open_road_pass: list[str] = []
        open_road_fail: list[str] = []

        # Rule 1: Min speed > 35 mph
        if min(speeds_mph) > 35:
            open_road_pass.append("min_speed_gt_35")
        else:
            open_road_fail.append("min_speed_gt_35")

        # Rule 2: Mean speed > 45 mph
        if mean(speeds_mph) > 45:
            open_road_pass.append("mean_speed_gt_45")
        else:
            open_road_fail.append("mean_speed_gt_45")

        # Rule 3: Speed std dev < 5 mph
        if stdev(speeds_mph) < 5:
            open_road_pass.append("speed_stable")
        else:
            open_road_fail.append("speed_stable")

        # Rule 4: Heading stability < 15 degrees total change
        total_heading_change = 0.0
        if len(gnss) >= 3:
            for i in range(1, len(gnss) - 1):
                h1 = compute_heading(gnss[i-1]["lat"], gnss[i-1]["lon"],
                                     gnss[i]["lat"], gnss[i]["lon"])
                h2 = compute_heading(gnss[i]["lat"], gnss[i]["lon"],
                                     gnss[i+1]["lat"], gnss[i+1]["lon"])
                delta = abs(h2 - h1)
                if delta > 180:
                    delta = 360 - delta
                total_heading_change += delta
            details["total_heading_change"] = round(total_heading_change, 1)

        if total_heading_change < 15:
            open_road_pass.append("heading_stable")
        else:
            open_road_fail.append("heading_stable")

        # Rule 5: No lateral accel > 0.3g
        has_lateral_spike = False
        if imu:
            for point in imu:
                acc = point.get("accelerometer") or {}
                ay = abs(float(acc.get("y", 0) if acc else
                               (point.get("acc_y") or point.get("accel_y") or 0)))
                if ay > 0.3 * G:
                    has_lateral_spike = True
                    break
        if not has_lateral_spike:
            open_road_pass.append("no_lateral_spike")
        else:
            open_road_fail.append("no_lateral_spike")

        # Rule 6: No longitudinal decel > 0.3g for > 0.5s
        has_sustained_decel = False
        if imu:
            decel_start = None
            for point in imu:
                acc = point.get("accelerometer") or {}
                ax = float(acc.get("x", 0) if acc else
                           (point.get("acc_x") or point.get("accel_x") or 0))
                ts = point.get("timestamp", 0)
                if ax < -0.3 * G:
                    if decel_start is None:
                        decel_start = ts
                    elif ts - decel_start > 500:
                        has_sustained_decel = True
                        break
                else:
                    decel_start = None
        if not has_sustained_decel:
            open_road_pass.append("no_sustained_decel")
        else:
            open_road_fail.append("no_sustained_decel")

        if not open_road_fail:
            details["rules"] = open_road_pass
            return "open_road", details

    # === SIGNAL ===
    details["rules"] = ["default"]
    return "signal", details


def api_request(method: str, url: str, **kwargs) -> requests.Response:
    """Make an API request, pausing on 403 until it succeeds."""
    kwargs.setdefault("timeout", 30)
    wait = 30
    while True:
        resp = requests.request(method, url, **kwargs)
        if resp.status_code != 403:
            return resp
        print(f"\n  {YELLOW}403 rate-limited — waiting {wait}s...{RESET}", flush=True)
        time.sleep(wait)
        wait = min(wait * 2, 300)


PERIODS = {
    1: ("2025-01-01", "2025-09-15", "Period 1: Jan 1 – Sep 15, 2025"),
    2: ("2025-09-15", "2026-01-20", "Period 2: Mid-Sep 2025 – Jan 20, 2026"),
    3: ("2026-01-20", "2026-02-25", "Period 3: Jan 20 – Feb 25, 2026 (fw ≥6.65.2)"),
    4: ("2026-02-25", "2026-03-15", "Period 4: Feb 25 – Mar 15, 2026 (fw ≥6.68.4)"),
    5: ("2026-03-15", "2099-01-01", "Period 5: Mar 15, 2026 onward (fw ≥6.69.4)"),
}


def fetch_events(api_key: str, limit: int, days: int, offset: int = 0,
                 start_date: datetime | None = None, end_date: datetime | None = None) -> list[dict]:
    """Fetch recent events from Bee Maps. Splits into 31-day chunks if needed."""
    auth = f"Basic {api_key}"
    if start_date and end_date:
        start = start_date
        end = end_date
    else:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
    max_chunk = timedelta(days=31)

    # Build 31-day chunks (newest first)
    chunks: list[tuple[datetime, datetime]] = []
    chunk_end = end
    while chunk_end > start:
        chunk_start = max(start, chunk_end - max_chunk)
        chunks.append((chunk_start, chunk_end))
        chunk_end = chunk_start

    all_events: list[dict] = []
    page_size = min(limit, 500)

    range_label = f"{start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}"
    print(f"Fetching {limit} events from {range_label} ({len(chunks)} chunk(s))...")

    for chunk_start, chunk_end in chunks:
        chunk_offset = offset if not all_events else 0
        while len(all_events) < limit:
            body = {
                "startDate": chunk_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "endDate": chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "limit": page_size,
                "offset": chunk_offset,
            }
            resp = api_request(
                "POST",
                "https://beemaps.com/api/developer/aievents/search",
                headers={"Content-Type": "application/json", "Authorization": auth},
                json=body,
            )
            resp.raise_for_status()
            data = resp.json()
            events = data.get("events", [])
            if not events:
                break
            all_events.extend(events)
            chunk_offset += len(events)
            print(f"  fetched {len(all_events)}/{limit}...", end="\r")
            if len(events) < page_size:
                break
        if len(all_events) >= limit:
            break

    print(f"  fetched {len(all_events)} events total    ")
    return all_events[:limit]


def fetch_event_detail(api_key: str, event_id: str) -> dict:
    """Fetch single event with GNSS + IMU data. Caches gzipped to data/event-cache/."""
    # Check cache first (gzipped, then fall back to uncompressed)
    cache_gz = EVENT_CACHE_DIR / f"{event_id}.json.gz"
    cache_plain = EVENT_CACHE_DIR / f"{event_id}.json"
    if cache_gz.exists():
        with gzip.open(cache_gz, "rt") as f:
            return json.loads(f.read())
    if cache_plain.exists():
        return json.loads(cache_plain.read_text())

    auth = f"Basic {api_key}"
    resp = api_request(
        "GET",
        f"https://beemaps.com/api/developer/aievents/{event_id}"
        f"?includeGnssData=true&includeImuData=true",
        headers={"Content-Type": "application/json", "Authorization": auth},
    )
    resp.raise_for_status()
    data = resp.json()

    # Cache the full response (gzipped)
    EVENT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with gzip.open(cache_gz, "wt") as f:
        f.write(json.dumps(data))

    return data


def ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS triage_results (
            id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            triage_result TEXT NOT NULL,
            rules_triggered TEXT NOT NULL DEFAULT '[]',
            speed_min REAL,
            speed_max REAL,
            speed_mean REAL,
            speed_stddev REAL,
            gnss_displacement_m REAL,
            video_size INTEGER,
            event_timestamp TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    # Migrations: add columns if missing (existing DBs)
    for col, defn in [
        ("video_size", "INTEGER"),
        ("lat", "REAL"),
        ("lon", "REAL"),
        ("road_class", "TEXT"),
        ("country", "TEXT"),
        ("city", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE triage_results ADD COLUMN {col} {defn}")
        except Exception:
            pass  # Column already exists
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Phase 0 Triage — classify events without video")
    parser.add_argument("num_events", type=int, help="Number of events to triage")
    parser.add_argument("--days", type=int, default=30, help="Look back N days (default: 30)")
    parser.add_argument("--period", type=int, choices=[1, 2, 3, 4, 5],
                        help="Filter to a specific data period (1-5)")
    args = parser.parse_args()

    api_key = load_api_key()
    mapbox_token = load_mapbox_token()
    if not mapbox_token:
        print(f"{YELLOW}Warning: No Mapbox token found — road_class will be skipped{RESET}")
    conn = get_db_conn()
    ensure_table(conn)

    # Check already triaged
    existing = set(
        r[0] for r in conn.execute("SELECT id FROM triage_results").fetchall()
    )

    # Resolve date range from --period or --days
    start_date = None
    end_date = None
    if args.period:
        p_start, p_end, p_label = PERIODS[args.period]
        start_date = datetime.fromisoformat(p_start).replace(tzinfo=timezone.utc)
        end_date = min(datetime.fromisoformat(p_end).replace(tzinfo=timezone.utc),
                       datetime.now(timezone.utc))
        print(f"{BOLD}{p_label}{RESET}")
        print(f"  Date range: {p_start} → {end_date.strftime('%Y-%m-%d')}")

    # Fetch all events in the date range, then filter to untriaged
    print(f"{len(existing)} already triaged, looking for {args.num_events} new events...")
    all_events = fetch_events(api_key, args.num_events + len(existing), args.days,
                              start_date=start_date, end_date=end_date)
    to_triage = [e for e in all_events if e["id"] not in existing][:args.num_events]
    print(f"  found {len(to_triage)} new events (scanned {len(all_events)} total)\n")

    if not to_triage:
        print("Nothing to triage.")
        return

    counts = defaultdict(int)
    t0 = time.time()

    for i, evt_summary in enumerate(to_triage):
        eid = evt_summary["id"]
        etype = evt_summary.get("type", "UNKNOWN")
        video_url = evt_summary.get("videoUrl")

        # Step 1: Range-check the video (cheap, 1 byte)
        video_valid, video_size = check_video(video_url)

        if not video_valid:
            result = "missing_video"
            rules = ["no_video_url"] if not video_url else []
            if video_size is not None and video_size <= MIN_VIDEO_SIZE:
                rules = [f"file_too_small_{video_size}B"]
            elif video_url and video_size is None:
                rules = ["video_unreachable"]
            details = {
                "event_type": etype,
                "rules": rules,
                "video_size": video_size,
                "speed_min": None, "speed_max": None,
                "speed_mean": None, "speed_stddev": None,
                "gnss_displacement_m": None,
            }
            # No need to fetch full event detail — skip the API call
            event_timestamp = evt_summary.get("timestamp")
        else:
            # Step 2: Fetch full event with telemetry for Ghost/Open Road/Signal
            try:
                event = fetch_event_detail(api_key, eid)
            except Exception as exc:
                print(f"  [{i+1}/{len(to_triage)}] {eid} — fetch error: {exc}")
                continue

            result, details = triage_event(event)
            details["video_size"] = video_size
            event_timestamp = event.get("timestamp")

        counts[result] += 1

        # Color code
        if result == "sanctioned":
            color = RED
        elif result in ("missing_video", "missing_metadata"):
            color = BLUE
        elif result == "ghost":
            color = RED
        elif result == "open_road":
            color = YELLOW
        else:
            color = GREEN

        rules = details.get("rules", [])
        speed_info = ""
        if details.get("speed_min") is not None:
            speed_info = f" ({details['speed_min']}-{details['speed_max']} mph)"
        size_info = ""
        if details.get("video_size") is not None:
            size_kb = details["video_size"] / 1024
            size_info = f" [{size_kb:.0f}KB]"

        # Query road class from Mapbox
        loc = evt_summary.get("location", {})
        evt_lat = loc.get("lat")
        evt_lon = loc.get("lon")
        road_class = None
        if mapbox_token and evt_lat and evt_lon:
            road_class = query_road_class(evt_lat, evt_lon, mapbox_token)

        # Look up country and city
        country = None
        city = None
        if evt_lat and evt_lon:
            country, city = lookup_location(evt_lat, evt_lon, mapbox_token)

        # Reject events from sanctioned countries
        if country and country in SANCTIONED_COUNTRIES:
            result = "sanctioned"
            rules = details.get("rules", [])
            rules.append(f"sanctioned_country:{country}")
            details["rules"] = rules

        road_info = f" {road_class}" if road_class else ""
        location_info = ""
        if city and country:
            location_info = f" [{city}, {country}]"
        elif country:
            location_info = f" [{country}]"
        print(
            f"  [{i+1}/{len(to_triage)}] {color}{result:>14}{RESET}  "
            f"{etype:<25} {eid[:16]}…{speed_info}{size_info}{road_info}{location_info}  "
            f"{DIM}{', '.join(rules)}{RESET}"
        )

        # Save to DB
        conn.execute(
            """INSERT OR REPLACE INTO triage_results
               (id, event_type, triage_result, rules_triggered,
                speed_min, speed_max, speed_mean, speed_stddev,
                gnss_displacement_m, video_size, event_timestamp, lat, lon, road_class, country, city, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
            (
                eid, etype, result, json.dumps(rules),
                details.get("speed_min"), details.get("speed_max"),
                details.get("speed_mean"), details.get("speed_stddev"),
                details.get("gnss_displacement_m"),
                details.get("video_size"),
                event_timestamp,
                evt_lat, evt_lon, road_class, country, city,
            ),
        )
        if (i + 1) % 10 == 0:
            conn.commit()
            if hasattr(conn, "sync"):
                conn.sync()

    conn.commit()
    if hasattr(conn, "sync"):
        conn.sync()
    conn.close()

    elapsed = time.time() - t0
    total = sum(counts.values())

    print(f"\n{BOLD}{'═' * 50}{RESET}")
    print(f"  {BLUE}No Video:  {counts['missing_video']:>4}{RESET}")
    print(f"  {BLUE}No Meta:   {counts['missing_metadata']:>4}{RESET}")
    print(f"  {RED}Ghost:     {counts['ghost']:>4}{RESET}")
    print(f"  {YELLOW}Open Road: {counts['open_road']:>4}{RESET}")
    print(f"  {GREEN}Signal:    {counts['signal']:>4}{RESET}")
    print(f"  Total:     {total:>4}  ({elapsed:.0f}s, {elapsed/max(total,1):.1f}s/event)")
    print(f"{BOLD}{'═' * 50}{RESET}")

    # Dedupe signal events
    conn = get_db_conn()
    dedupe_signals(conn)
    conn.close()


def dedupe_signals(conn: sqlite3.Connection, distance_m: float = 100, window_s: float = 60, min_cluster: int = 3):
    """Find and mark egregious duplicate signal events (3+ for one incident)."""
    from datetime import datetime as _dt

    print(f"\n{BOLD}Running dedupe on signal events...{RESET}")

    rows = conn.execute(
        """SELECT id, event_type, event_timestamp, lat, lon, speed_min, speed_max
           FROM triage_results
           WHERE triage_result = 'signal'
             AND lat IS NOT NULL AND lon IS NOT NULL
             AND event_timestamp IS NOT NULL
           ORDER BY event_timestamp"""
    ).fetchall()

    if not rows:
        print("  No signal events to dedupe.")
        return

    def parse_ts(ts_str):
        try:
            return _dt.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0

    # Group by event_type, sort by timestamp
    events_by_type: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        evt = {"id": r[0], "event_type": r[1], "event_timestamp": r[2],
               "lat": r[3], "lon": r[4], "speed_min": r[5], "speed_max": r[6]}
        evt["_ts"] = parse_ts(evt["event_timestamp"])
        events_by_type[evt["event_type"]].append(evt)

    for etype in events_by_type:
        events_by_type[etype].sort(key=lambda e: e["_ts"])

    # Cluster
    clusters: list[list[dict]] = []
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
                if evt_b["_ts"] - evt_a["_ts"] > window_s:
                    break
                close = any(
                    haversine(c["lat"], c["lon"], evt_b["lat"], evt_b["lon"]) <= distance_m
                    for c in cluster
                )
                if close:
                    cluster.append(evt_b)
                    seen.add(evt_b["id"])
            if len(cluster) >= min_cluster:
                clusters.append(cluster)

    total_dupes = sum(len(c) - 1 for c in clusters)

    if not clusters:
        print(f"  No egregious duplicates found ({len(rows)} signal events checked).")
        return

    # Mark duplicates — keep the best per cluster (largest speed drop)
    marked = 0
    for cluster in clusters:
        best = max(cluster, key=lambda e: (e["speed_max"] or 0) - (e["speed_min"] or 0))
        for evt in cluster:
            if evt["id"] != best["id"]:
                conn.execute(
                    "UPDATE triage_results SET triage_result = 'duplicate' WHERE id = ? AND triage_result = 'signal'",
                    (evt["id"],)
                )
                marked += 1
    conn.commit()
    if hasattr(conn, "sync"):
        conn.sync()

    print(f"  {len(rows)} signal events checked")
    print(f"  {len(clusters)} clusters found (≥{min_cluster} events each)")
    print(f"  {YELLOW}{marked} marked as duplicate{RESET}")


if __name__ == "__main__":
    main()
