#!/usr/bin/env python3
"""
Phase 0 Triage — classify events as Missing Video, Ghost, Open Road, or Signal.

Usage:
    python3 scripts/run-triage.py <num_events> [--days N] [--period {4,5,6,7}]

Examples:
    python3 scripts/run-triage.py 500 --period 7
    python3 scripts/run-triage.py 500 --days 7
"""

import argparse
import gzip
import json
import math
import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, stdev

import requests

from frame_timing_qc import FILTER_OUT, is_firmware_eligible, parse_firmware_version, probe_video

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "labels.db"
EVENT_CACHE_DIR = PROJECT_ROOT / "data" / "event-cache"
GEO_CACHE_PATH = PROJECT_ROOT / "data" / "geo-cache.json"
CITY_CACHE_PATH = PROJECT_ROOT / "data" / "city-cache.json"
EVENT_SEARCH_URL = "https://beemaps.com/api/developer/aievents/search"
TRIAGE_FETCH_CONCURRENCY = max(1, min(8, int(os.environ.get("TRIAGE_FETCH_CONCURRENCY", "4"))))
TRIAGE_PROCESS_WORKERS = max(1, min(8, int(os.environ.get("TRIAGE_PROCESS_WORKERS", "1"))))
RATE_LIMIT_STATUS_CODES = {403, 429}
TRANSIENT_STATUS_CODES = {502, 503, 504}

_GEO_CACHE: dict[str, str] | None = None
_CITY_CACHE: dict[str, str] | None = None
_VIDEO_CHECK_CACHE: dict[str, tuple[bool, int | None]] = {}
_VIDEO_CHECK_LOCKS: dict[str, threading.Lock] = {}
_VIDEO_QC_CACHE: dict[str, dict] = {}
_VIDEO_QC_LOCKS: dict[str, threading.Lock] = {}
_CACHE_LOCK = threading.Lock()


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
MIN_STANDARD_VIDEO_DURATION_SEC = 28.0
MIN_NON_LINEAR_BITRATE_BPS = 3_300_000.0
NON_LINEAR_SHORT_RULE = "short_video_lt_28s"
NON_LINEAR_BITRATE_RULE = "bitrate_lt_3_3mbps"

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
    with _CACHE_LOCK:
        cached = _VIDEO_CHECK_CACHE.get(video_url)
        if cached is not None:
            return cached
        video_lock = _VIDEO_CHECK_LOCKS.setdefault(video_url, threading.Lock())

    with video_lock:
        with _CACHE_LOCK:
            cached = _VIDEO_CHECK_CACHE.get(video_url)
            if cached is not None:
                return cached
        result = _check_video_uncached(video_url)
        with _CACHE_LOCK:
            _VIDEO_CHECK_CACHE[video_url] = result
        return result


def _check_video_uncached(video_url: str) -> tuple[bool, int | None]:
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


def probe_video_cached(
    video_url: str,
    *,
    video_id: str,
    firmware_version: str | None,
) -> dict:
    with _CACHE_LOCK:
        cached = _VIDEO_QC_CACHE.get(video_url)
        if cached is not None:
            qc = dict(cached)
            qc["video_id"] = video_id
            qc["firmware_version"] = firmware_version
            return qc
        qc_lock = _VIDEO_QC_LOCKS.setdefault(video_url, threading.Lock())

    with qc_lock:
        with _CACHE_LOCK:
            cached = _VIDEO_QC_CACHE.get(video_url)
            if cached is not None:
                qc = dict(cached)
                qc["video_id"] = video_id
                qc["firmware_version"] = firmware_version
                return qc
        qc = probe_video(video_url, video_id=video_id, firmware_version=firmware_version)
        with _CACHE_LOCK:
            _VIDEO_QC_CACHE[video_url] = dict(qc)
        return qc


def compute_video_length_sec(gnss: list[dict]) -> float | None:
    """Estimate clip length from GNSS timestamps in seconds."""
    if len(gnss) < 2:
        return None
    first = next((p.get("timestamp") for p in gnss if isinstance(p.get("timestamp"), (int, float))), None)
    last = next((p.get("timestamp") for p in reversed(gnss) if isinstance(p.get("timestamp"), (int, float))), None)
    if first is None or last is None or last <= first:
        return None
    return (last - first) / 1000


def compute_video_stats(event: dict, video_size: int | None) -> tuple[float | None, float | None]:
    """Return (duration_seconds, bitrate_bps) using GNSS duration + file size."""
    duration_sec = compute_video_length_sec(event.get("gnssData") or [])
    bitrate_bps = None
    if duration_sec and duration_sec > 0 and video_size:
        bitrate_bps = (video_size * 8) / duration_sec
    return (
        round(duration_sec, 3) if duration_sec is not None else None,
        round(bitrate_bps, 1) if bitrate_bps is not None else None,
    )


def get_non_linear_video_rules(
    event_timestamp: str | None,
    duration_sec: float | None,
    bitrate_bps: float | None,
) -> list[str]:
    """Period 6+ videos are non-linear when they are short or too low bitrate."""
    if not event_timestamp:
        return []
    try:
        if parse_iso_timestamp(event_timestamp) < parse_iso_timestamp(PERIODS[6][0]):
            return []
    except Exception:
        return []

    rules: list[str] = []
    if duration_sec is not None and duration_sec < MIN_STANDARD_VIDEO_DURATION_SEC:
        rules.append(NON_LINEAR_SHORT_RULE)
    if bitrate_bps is not None and bitrate_bps < MIN_NON_LINEAR_BITRATE_BPS:
        rules.append(NON_LINEAR_BITRATE_RULE)
    return rules


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


def _retry_after_seconds(resp: requests.Response, fallback: float) -> float:
    header = resp.headers.get("retry-after")
    if not header:
        return fallback
    try:
        return max(1.0, float(header))
    except ValueError:
        return fallback


def api_request(method: str, url: str, **kwargs) -> requests.Response:
    """Make an API request, backing off on API or Cloudflare throttles."""
    kwargs.setdefault("timeout", 30)
    wait = 30.0
    transient_attempts = 0
    while True:
        try:
            resp = requests.request(method, url, **kwargs)
        except requests.RequestException as exc:
            transient_attempts += 1
            if transient_attempts > 3:
                raise
            print(
                f"\n  {YELLOW}request failed — waiting {wait:.0f}s... {exc}{RESET}",
                flush=True,
            )
            time.sleep(wait)
            wait = min(wait * 2, 300)
            continue

        if resp.status_code in RATE_LIMIT_STATUS_CODES:
            retry_after = _retry_after_seconds(resp, wait)
            print(
                f"\n  {YELLOW}{resp.status_code} rate-limited — waiting {retry_after:.0f}s...{RESET}",
                flush=True,
            )
            time.sleep(retry_after)
            wait = min(max(wait * 2, retry_after * 2), 300)
            continue

        if resp.status_code in TRANSIENT_STATUS_CODES and transient_attempts < 3:
            transient_attempts += 1
            print(
                f"\n  {YELLOW}{resp.status_code} from API — waiting {wait:.0f}s before retry...{RESET}",
                flush=True,
            )
            time.sleep(wait)
            wait = min(wait * 2, 300)
            continue

        return resp


PERIODS = {
    1: ("2025-01-01T00:00:00.000Z", "2025-09-15T00:00:00.000Z", "Period 1: Jan 1 – Sep 15, 2025"),
    2: ("2025-09-15T00:00:00.000Z", "2026-01-20T00:00:00.000Z", "Period 2: Mid-Sep 2025 – Jan 20, 2026"),
    3: ("2026-01-20T00:00:00.000Z", "2026-02-25T00:00:00.000Z", "Period 3: Jan 20 – Feb 25, 2026 (fw ≥6.65.2)"),
    4: ("2026-02-25T00:00:00.000Z", "2026-03-15T00:00:00.000Z", "Period 4: Feb 25 – Mar 15, 2026 (fw ≥6.68.4)"),
    5: ("2026-03-15T00:00:00.000Z", "2026-04-17T00:00:00.000Z", "Period 5: Mar 15 – Apr 17, 2026 (fw ≥6.69.4)"),
    6: ("2026-04-17T00:00:00.000Z", "2026-04-22T20:45:00.500Z", "Period 6: Apr 17 – Apr 22, 2026 20:45 UTC (fw ≥7.0.12)"),
    7: ("2026-04-22T20:45:00.500Z", "2099-01-01T00:00:00.000Z", "Period 7: Apr 22, 2026 20:45 UTC onward (fw ≥7.4.3)"),
}
MIN_TRIAGE_PERIOD = 4
SUPPORTED_TRIAGE_PERIODS = sorted(period for period in PERIODS if period >= MIN_TRIAGE_PERIOD)
TRIAGE_MIN_START_ISO = PERIODS[MIN_TRIAGE_PERIOD][0]


def parse_iso_timestamp(value: str) -> datetime:
    """Parse Bee Maps period boundaries with or without fractional seconds."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def format_api_timestamp(value: datetime) -> str:
    """Format timestamps for Bee Maps search while preserving millisecond boundaries."""
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def fetch_event_page(
    auth: str,
    chunk_start: datetime,
    chunk_end: datetime,
    page_size: int,
    page_offset: int,
) -> tuple[int, list[dict], int | None]:
    body = {
        "startDate": format_api_timestamp(chunk_start),
        "endDate": format_api_timestamp(chunk_end),
        "limit": page_size,
        "offset": page_offset,
    }
    resp = api_request(
        "POST",
        EVENT_SEARCH_URL,
        headers={"Content-Type": "application/json", "Authorization": auth},
        json=body,
    )
    resp.raise_for_status()
    data = resp.json()
    total = data.get("pagination", {}).get("total")
    return page_offset, data.get("events", []), int(total) if total is not None else None


def fetch_events(api_key: str, limit: int, days: int, offset: int = 0,
                 start_date: datetime | None = None, end_date: datetime | None = None,
                 exclude_ids: set[str] | None = None,
                 firmware_filter: str | None = None) -> tuple[list[dict], int]:
    """Fetch candidate events from Bee Maps, optionally filtering already-triaged IDs."""
    auth = f"Basic {api_key}"
    if start_date and end_date:
        start = start_date
        end = end_date
    else:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
    start = max(start, parse_iso_timestamp(TRIAGE_MIN_START_ISO))
    max_chunk = timedelta(days=31)

    # Build 31-day chunks (newest first)
    chunks: list[tuple[datetime, datetime]] = []
    chunk_end = end
    while chunk_end > start:
        chunk_start = max(start, chunk_end - max_chunk)
        chunks.append((chunk_start, chunk_end))
        chunk_end = chunk_start

    candidates: list[dict] = []
    seen_ids = set(exclude_ids or set())
    scanned = 0
    page_size = min(max(limit, 1), 500)

    range_label = f"{start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}"
    filter_bits = []
    if exclude_ids is not None:
        filter_bits.append("untriaged")
    if firmware_filter:
        filter_bits.append(f"firmware {firmware_filter}")
    filter_label = f" {' '.join(filter_bits)}" if filter_bits else ""
    print(
        f"Fetching up to {limit}{filter_label} events from {range_label} "
        f"({len(chunks)} chunk(s), {TRIAGE_FETCH_CONCURRENCY} workers)..."
    )

    for chunk_start, chunk_end in chunks:
        chunk_offset = offset if scanned == 0 else 0
        page_offset, events, total = fetch_event_page(
            auth,
            chunk_start,
            chunk_end,
            page_size,
            chunk_offset,
        )
        scanned += len(events)
        for event in events:
            event_id = event.get("id")
            if not event_id or event_id in seen_ids:
                continue
            seen_ids.add(event_id)
            if firmware_filter and get_firmware_version(event) != firmware_filter:
                continue
            candidates.append(event)
            if len(candidates) >= limit:
                break
        total_for_chunk = total if total is not None else chunk_offset + len(events)
        print(
            f"  scanned {scanned} events, found {len(candidates)}/{limit}{filter_label}...",
            end="\r",
        )
        if len(candidates) >= limit:
            break
        if not events or len(events) < page_size:
            continue

        offsets = list(range(page_offset + len(events), total_for_chunk, page_size))
        for start_idx in range(0, len(offsets), TRIAGE_FETCH_CONCURRENCY):
            batch_offsets = offsets[start_idx:start_idx + TRIAGE_FETCH_CONCURRENCY]
            batch_results: dict[int, list[dict]] = {}
            with ThreadPoolExecutor(max_workers=TRIAGE_FETCH_CONCURRENCY) as executor:
                futures = [
                    executor.submit(
                        fetch_event_page,
                        auth,
                        chunk_start,
                        chunk_end,
                        page_size,
                        page_offset,
                    )
                    for page_offset in batch_offsets
                ]
                for future in as_completed(futures):
                    page_offset, page_events, _ = future.result()
                    batch_results[page_offset] = page_events

            for page_offset in sorted(batch_results):
                events = batch_results[page_offset]
                scanned += len(events)
                for event in events:
                    event_id = event.get("id")
                    if not event_id or event_id in seen_ids:
                        continue
                    seen_ids.add(event_id)
                    if firmware_filter and get_firmware_version(event) != firmware_filter:
                        continue
                    candidates.append(event)
                    if len(candidates) >= limit:
                        break
                print(
                    f"  scanned {scanned} events, found {len(candidates)}/{limit}{filter_label}...",
                    end="\r",
                )
                if len(candidates) >= limit:
                    break
            if not events:
                break
            if len(events) < page_size:
                break
            if len(candidates) >= limit:
                break
        if len(candidates) >= limit:
            break

    print(f"  found {len(candidates)} candidates after scanning {scanned} events total    ")
    return candidates[:limit], scanned


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


def fetch_event_detail_outcome(api_key: str, event_id: str) -> tuple[dict | None, str | None]:
    """Fetch event detail for triage; return an error string instead of raising."""
    try:
        return fetch_event_detail(api_key, event_id), None
    except Exception as exc:
        return None, str(exc)


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
            video_length_sec REAL,
            bitrate_bps REAL,
            firmware_version TEXT,
            firmware_version_num INTEGER,
            event_timestamp TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS video_frame_timing_qc (
            video_id TEXT PRIMARY KEY,
            firmware_version TEXT,
            bucket TEXT NOT NULL,
            frame_count INTEGER NOT NULL,
            duration_s REAL NOT NULL,
            effective_fps REAL NOT NULL,
            gap_pct REAL NOT NULL,
            single_gaps INTEGER NOT NULL,
            double_gaps INTEGER NOT NULL,
            triple_plus_gaps INTEGER NOT NULL DEFAULT 0,
            max_delta_ms REAL NOT NULL,
            late_frames INTEGER NOT NULL DEFAULT 0,
            max_late_frames_per_2s INTEGER NOT NULL DEFAULT 0,
            late_frame_clusters INTEGER NOT NULL DEFAULT 0,
            non_monotonic_deltas INTEGER NOT NULL DEFAULT 0,
            failed_rules TEXT NOT NULL DEFAULT '[]',
            probe_status TEXT NOT NULL DEFAULT 'ok',
            probe_error TEXT,
            deltas_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    # Migrations: add columns if missing (existing DBs)
    for col, defn in [
        ("video_size", "INTEGER"),
        ("video_length_sec", "REAL"),
        ("bitrate_bps", "REAL"),
        ("firmware_version", "TEXT"),
        ("firmware_version_num", "INTEGER"),
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
    for col, defn in [
        ("triple_plus_gaps", "INTEGER NOT NULL DEFAULT 0"),
        ("max_late_frames_per_2s", "INTEGER NOT NULL DEFAULT 0"),
        ("late_frame_clusters", "INTEGER NOT NULL DEFAULT 0"),
    ]:
        try:
            conn.execute(f"ALTER TABLE video_frame_timing_qc ADD COLUMN {col} {defn}")
        except Exception:
            pass  # Column already exists
    conn.execute("CREATE INDEX IF NOT EXISTS idx_triage_results_event_timestamp ON triage_results (event_timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_triage_results_result_timestamp ON triage_results (triage_result, event_timestamp)")
    conn.commit()


def get_firmware_version(event: dict) -> str | None:
    metadata = event.get("metadata") if isinstance(event, dict) else None
    if not isinstance(metadata, dict):
        return None
    value = metadata.get("FIRMWARE_VERSION")
    return value if isinstance(value, str) and value.strip() else None


def firmware_version_to_num(value: str | None) -> int | None:
    version = parse_firmware_version(value)
    if version is None:
        return None
    major, minor, patch = version
    return major * 1_000_000 + minor * 1_000 + patch


def is_period_firmware_eligible(period: int | None, firmware_version: str | None) -> bool:
    if period == 7:
        return is_firmware_eligible(firmware_version)
    return True


def should_skip_period_firmware(period: int | None, firmware_version: str | None) -> bool:
    """Return true only when firmware is known and below the period gate."""
    if period != 7:
        return False
    return firmware_version is not None and not is_firmware_eligible(firmware_version)


def save_frame_timing_qc(conn, video_id: str, qc: dict) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO video_frame_timing_qc
           (video_id, firmware_version, bucket, frame_count, duration_s,
            effective_fps, gap_pct, single_gaps, double_gaps, triple_plus_gaps, max_delta_ms,
            late_frames, max_late_frames_per_2s, late_frame_clusters,
            non_monotonic_deltas, failed_rules, probe_status,
            probe_error, deltas_json, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))""",
        (
            video_id,
            qc.get("firmware_version"),
            qc.get("bucket"),
            qc.get("frame_count"),
            qc.get("duration_s"),
            qc.get("effective_fps"),
            qc.get("gap_pct"),
            qc.get("single_gaps"),
            qc.get("double_gaps"),
            qc.get("triple_plus_gaps", 0),
            qc.get("max_delta_ms"),
            qc.get("late_frames", 0),
            qc.get("max_late_frames_per_2s", 0),
            qc.get("late_frame_clusters", 0),
            qc.get("non_monotonic_deltas", 0),
            json.dumps(qc.get("failed_rules") or []),
            qc.get("probe_status", "ok"),
            qc.get("probe_error"),
            json.dumps(qc.get("deltas_ms") or []),
        ),
    )


def save_triage_row(conn, row_params: tuple) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO triage_results
           (id, event_type, triage_result, rules_triggered,
            speed_min, speed_max, speed_mean, speed_stddev,
            gnss_displacement_m, video_size, video_length_sec, bitrate_bps,
            firmware_version, firmware_version_num,
            event_timestamp, lat, lon, road_class, country, city, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
        row_params,
    )


def build_firmware_skip_outcome(evt_summary: dict, firmware_version: str | None) -> dict:
    eid = evt_summary["id"]
    etype = evt_summary.get("type", "UNKNOWN")
    loc = evt_summary.get("location") or {}
    firmware_version_num = firmware_version_to_num(firmware_version)
    rule = (
        f"period_7_firmware_below_7_4_3:{firmware_version}"
        if firmware_version
        else "period_7_firmware_unknown"
    )

    return {
        "count_key": "skipped_firmware",
        "summary": (
            f"{DIM}{'skipped':>14}{RESET}  "
            f"{etype:<25} {eid[:16]}… "
            f"{DIM}period 7 requires fw ≥7.4.3; found {firmware_version or 'unknown'}{RESET}"
        ),
        "row_params": (
            eid,
            etype,
            "skipped_firmware",
            json.dumps([rule]),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            firmware_version,
            firmware_version_num,
            evt_summary.get("timestamp"),
            loc.get("lat"),
            loc.get("lon"),
            None,
            None,
            None,
        ),
    }


def analyze_triage_candidate(
    evt_summary: dict,
    api_key: str,
    mapbox_token: str | None,
    period: int | None,
) -> dict:
    eid = evt_summary["id"]
    etype = evt_summary.get("type", "UNKNOWN")
    video_url = evt_summary.get("videoUrl")
    event = None
    firmware_version = None
    firmware_version_num = None
    qc = None
    qc_bucket = None
    detail_fetch_error = None

    if period == 7:
        firmware_version = get_firmware_version(evt_summary)
        firmware_version_num = firmware_version_to_num(firmware_version)
        if should_skip_period_firmware(period, firmware_version):
            return build_firmware_skip_outcome(evt_summary, firmware_version)

    if not video_url:
        event, detail_fetch_error = fetch_event_detail_outcome(api_key, eid)
        if event:
            video_url = event.get("videoUrl")
            detail_firmware_version = get_firmware_version(event)
            if detail_firmware_version:
                firmware_version = detail_firmware_version
                firmware_version_num = firmware_version_to_num(firmware_version)
            if not is_period_firmware_eligible(period, firmware_version):
                return build_firmware_skip_outcome(
                    {**evt_summary, "timestamp": event.get("timestamp") or evt_summary.get("timestamp")},
                    firmware_version,
                )

    video_valid, video_size = check_video(video_url)

    if not video_valid and event is None:
        event, detail_fetch_error = fetch_event_detail_outcome(api_key, eid)
        if event:
            detail_video_url = event.get("videoUrl")
            detail_firmware_version = get_firmware_version(event)
            if detail_firmware_version:
                firmware_version = detail_firmware_version
                firmware_version_num = firmware_version_to_num(firmware_version)
            if not is_period_firmware_eligible(period, firmware_version):
                return build_firmware_skip_outcome(
                    {**evt_summary, "timestamp": event.get("timestamp") or evt_summary.get("timestamp")},
                    firmware_version,
                )
            if detail_video_url and detail_video_url != video_url:
                video_url = detail_video_url
                video_valid, video_size = check_video(video_url)

    if not video_valid:
        if detail_fetch_error and event is None:
            return {"summary": f"{eid} — detail fetch error before missing-video classification: {detail_fetch_error}"}
        result = "missing_video"
        rules = ["no_video_url_after_detail"] if not video_url else []
        if video_size is not None and video_size <= MIN_VIDEO_SIZE:
            rules = [f"file_too_small_{video_size}B"]
        elif video_url and video_size is None:
            rules = ["video_unreachable_after_detail"]
        rules.append("detail_confirmed_missing_video")
        details = {
            "event_type": etype,
            "rules": rules,
            "video_size": video_size,
            "video_length_sec": None,
            "bitrate_bps": None,
            "speed_min": None, "speed_max": None,
            "speed_mean": None, "speed_stddev": None,
            "gnss_displacement_m": None,
        }
        event_timestamp = evt_summary.get("timestamp")
    else:
        if event is None:
            event, detail_fetch_error = fetch_event_detail_outcome(api_key, eid)
        if event is None:
            return {"summary": f"{eid} — fetch error: {detail_fetch_error}"}

        detail_firmware_version = get_firmware_version(event)
        if detail_firmware_version:
            firmware_version = detail_firmware_version
            firmware_version_num = firmware_version_to_num(firmware_version)
        if not is_period_firmware_eligible(period, firmware_version):
            return build_firmware_skip_outcome(
                {**evt_summary, "timestamp": event.get("timestamp") or evt_summary.get("timestamp")},
                firmware_version,
            )

        base_result, details = triage_event(event)
        result = base_result
        details["video_size"] = video_size
        event_timestamp = event.get("timestamp") or evt_summary.get("timestamp")
        if firmware_version is None:
            firmware_version = get_firmware_version(event)
            firmware_version_num = firmware_version_to_num(firmware_version)
        video_length_sec, bitrate_bps = compute_video_stats(event, video_size)
        details["video_length_sec"] = video_length_sec
        details["bitrate_bps"] = bitrate_bps

        rules = list(details.get("rules", []))
        video_rules = get_non_linear_video_rules(
            event_timestamp,
            video_length_sec,
            bitrate_bps,
        )
        if video_rules:
            result = "non_linear"
            rules.extend(video_rules)
            details["rules"] = list(dict.fromkeys(rules))

        if base_result != "missing_metadata" and is_firmware_eligible(firmware_version):
            qc = probe_video_cached(video_url, video_id=eid, firmware_version=firmware_version)
            qc_bucket = qc.get("bucket")
            if qc_bucket == FILTER_OUT:
                result = "non_linear"
                rules = list(details.get("rules", []))
                qc_rules = qc.get("failed_rules") or ["frame_timing_filter_out"]
                details["rules"] = list(
                    dict.fromkeys([*rules, *[f"frame_timing:{rule}" for rule in qc_rules]])
                )

    if result == "sanctioned":
        color = RED
    elif result in ("missing_video", "missing_metadata"):
        color = BLUE
    elif result == "ghost":
        color = RED
    elif result == "open_road":
        color = YELLOW
    elif result == "non_linear":
        color = CYAN
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
    qc_info = f" [fw {firmware_version} qc {qc_bucket}]" if qc_bucket else ""

    loc = evt_summary.get("location") or {}
    evt_lat = loc.get("lat")
    evt_lon = loc.get("lon")
    road_class = None
    if mapbox_token and evt_lat and evt_lon:
        road_class = query_road_class(evt_lat, evt_lon, mapbox_token)

    country = None
    city = None
    if evt_lat and evt_lon:
        country, city = lookup_location(evt_lat, evt_lon, mapbox_token)

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

    return {
        "count_key": result,
        "qc": qc,
        "summary": (
            f"{color}{result:>14}{RESET}  "
            f"{etype:<25} {eid[:16]}…{speed_info}{size_info}{qc_info}{road_info}{location_info}  "
            f"{DIM}{', '.join(rules)}{RESET}"
        ),
        "row_params": (
            eid, etype, result, json.dumps(rules),
            details.get("speed_min"), details.get("speed_max"),
            details.get("speed_mean"), details.get("speed_stddev"),
            details.get("gnss_displacement_m"),
            details.get("video_size"),
            details.get("video_length_sec"),
            details.get("bitrate_bps"),
            firmware_version,
            firmware_version_num,
            event_timestamp,
            evt_lat, evt_lon, road_class, country, city,
        ),
    }


def load_existing_triage_ids(
    conn,
    start_date: datetime | None,
    end_date: datetime | None,
) -> set[str]:
    """Load already-seen event IDs for the active source date window."""
    if start_date and end_date:
        rows = conn.execute(
            """SELECT id FROM triage_results
               WHERE event_timestamp IS NULL
                  OR (julianday(event_timestamp) >= julianday(?)
                      AND julianday(event_timestamp) < julianday(?))""",
            (format_api_timestamp(start_date), format_api_timestamp(end_date)),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT id FROM triage_results
               WHERE event_timestamp IS NULL
                  OR julianday(event_timestamp) >= julianday(?)""",
            (TRIAGE_MIN_START_ISO,),
        ).fetchall()
    return {r[0] for r in rows}


def main():
    parser = argparse.ArgumentParser(description="Phase 0 Triage — classify events without video")
    parser.add_argument("num_events", type=int, help="Number of events to triage")
    parser.add_argument("--days", type=int, default=30, help="Look back N days (default: 30)")
    parser.add_argument("--period", type=int, choices=SUPPORTED_TRIAGE_PERIODS,
                        help="Filter to a specific supported data period (4-7)")
    parser.add_argument("--firmware",
                        help="Only triage events whose Bee Maps search metadata FIRMWARE_VERSION exactly matches this value")
    parser.add_argument("--dedupe", action="store_true",
                        help="Run the expensive signal duplicate post-pass after triage")
    args = parser.parse_args()

    api_key = load_api_key()
    mapbox_token = load_mapbox_token()
    if not mapbox_token:
        print(f"{YELLOW}Warning: No Mapbox token found — road_class will be skipped{RESET}")
    conn = get_db_conn()
    ensure_table(conn)

    # Resolve date range from --period or --days
    if args.period:
        p_start, p_end, p_label = PERIODS[args.period]
        start_date = parse_iso_timestamp(p_start)
        end_date = min(parse_iso_timestamp(p_end), datetime.now(timezone.utc))
        print(f"{BOLD}{p_label}{RESET}")
        print(f"  Date range: {p_start} → {format_api_timestamp(end_date)}")
    else:
        end_date = datetime.now(timezone.utc)
        start_date = max(end_date - timedelta(days=args.days), parse_iso_timestamp(TRIAGE_MIN_START_ISO))

    # Check already triaged within the same source window.
    existing = load_existing_triage_ids(conn, start_date, end_date)

    # Fetch candidate events in the date range, skipping already-triaged IDs as pages arrive.
    print(f"{len(existing)} already triaged, looking for {args.num_events} new events...")
    to_triage, scanned = fetch_events(api_key, args.num_events, args.days,
                                      start_date=start_date, end_date=end_date,
                                      exclude_ids=existing,
                                      firmware_filter=args.firmware)
    print(f"  found {len(to_triage)} new events (scanned {scanned} total)\n")

    if not to_triage:
        print("Nothing to triage.")
        return

    counts = defaultdict(int)
    t0 = time.time()

    print(f"Processing {len(to_triage)} candidates with {TRIAGE_PROCESS_WORKERS} worker(s)...")
    processed = 0

    def handle_outcome(outcome: dict) -> None:
        nonlocal processed
        processed += 1
        count_key = outcome.get("count_key")
        if count_key:
            counts[count_key] += 1
        qc = outcome.get("qc")
        row_params = outcome.get("row_params")
        if qc and row_params:
            save_frame_timing_qc(conn, row_params[0], qc)
        if row_params:
            save_triage_row(conn, row_params)
        print(f"  [{processed}/{len(to_triage)}] {outcome.get('summary', 'unknown result')}")
        if processed % 10 == 0:
            conn.commit()
            if hasattr(conn, "sync"):
                conn.sync()

    if TRIAGE_PROCESS_WORKERS == 1:
        for evt_summary in to_triage:
            handle_outcome(
                analyze_triage_candidate(evt_summary, api_key, mapbox_token, args.period)
            )
    else:
        with ThreadPoolExecutor(max_workers=TRIAGE_PROCESS_WORKERS) as executor:
            futures = [
                executor.submit(
                    analyze_triage_candidate,
                    evt_summary,
                    api_key,
                    mapbox_token,
                    args.period,
                )
                for evt_summary in to_triage
            ]
            for future in as_completed(futures):
                handle_outcome(future.result())

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
    print(f"  {CYAN}NonLinear: {counts['non_linear']:>4}{RESET}")
    print(f"  {GREEN}Signal:    {counts['signal']:>4}{RESET}")
    if counts["skipped_firmware"]:
        print(f"  {DIM}FW Skip:   {counts['skipped_firmware']:>4}{RESET}")
    print(f"  Total:     {total:>4}  ({elapsed:.0f}s, {elapsed/max(total,1):.1f}s/event)")
    print(f"{BOLD}{'═' * 50}{RESET}")

    if args.dedupe or os.environ.get("TRIAGE_RUN_DEDUPE") == "1":
        conn = get_db_conn()
        dedupe_signals(conn)
        conn.close()
    else:
        print(f"\n{DIM}Skipping signal dedupe post-pass; run with --dedupe when needed.{RESET}")


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
