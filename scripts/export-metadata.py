#!/usr/bin/env python3
"""
Export full JSON metadata for each processed video.

Combines:
  - Bee Maps API event data (type, location, GNSS, IMU, metadata)
  - Triage results (classification, speed stats, road class)
  - VRU detections (frame detections + segments)
  - Scene attributes (weather)
  - Time of day (computed from sun position)
  - Country (reverse geocoded)
  - Detection run info (model, timing)
  - Blur status (S3 URL if blurred)

Output: data/metadata/{eventId}.json (one file per video)

Usage:
    python3 scripts/export-metadata.py
    python3 scripts/export-metadata.py --limit 100
    python3 scripts/export-metadata.py --event-id 698f4badae71cbe9f847580a
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "metadata"

API_BASE_URL = "https://beemaps.com/api/developer/aievents"


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


def load_api_key() -> str:
    key = _load_env_var("BEEMAPS_API_KEY")
    if not key:
        raise RuntimeError("BEEMAPS_API_KEY not found")
    return key


class _TursoCursor:
    def __init__(self, result_set):
        self._rows = result_set.rows if result_set else []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _TursoDb:
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


def get_db():
    turso_url = _load_env_var("TURSO_DATABASE_URL")
    turso_token = _load_env_var("TURSO_AUTH_TOKEN")
    if turso_url and turso_token:
        try:
            import libsql_client
            http_url = turso_url.replace("libsql://", "https://")
            client = libsql_client.create_client_sync(url=http_url, auth_token=turso_token)
            return _TursoDb(client)
        except ImportError:
            pass
    import sqlite3
    db_path = PROJECT_ROOT / "data" / "labels.db"
    return sqlite3.connect(str(db_path))


def api_request(method: str, url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", 60)
    wait = 30
    for attempt in range(6):
        resp = requests.request(method, url, **kwargs)
        if resp.status_code not in (403, 429):
            return resp
        time.sleep(wait)
        wait = min(wait * 2, 120)
    return resp

BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RED = "\033[31m"
RESET = "\033[0m"


# ---------------------------------------------------------------------------
# Sun position / time-of-day (port from src/lib/sun.ts)
# ---------------------------------------------------------------------------

def _sun_declination(day_of_year: int) -> float:
    return -23.45 * math.cos(math.radians(360 / 365 * (day_of_year + 10)))


def _hour_angle(lat: float, decl: float, elevation_deg: float) -> float | None:
    lat_rad = math.radians(lat)
    decl_rad = math.radians(decl)
    cos_ha = (math.sin(math.radians(elevation_deg)) - math.sin(lat_rad) * math.sin(decl_rad)) / (
        math.cos(lat_rad) * math.cos(decl_rad)
    )
    if cos_ha < -1 or cos_ha > 1:
        return None  # sun never reaches this elevation
    return math.degrees(math.acos(cos_ha))


def get_time_of_day(timestamp: str, lat: float, lon: float) -> dict:
    """Compute time of day: Night, Dawn, Day, or Dusk."""
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    day_of_year = dt.timetuple().tm_yday

    decl = _sun_declination(day_of_year)

    # Solar noon in hours UTC
    solar_noon = 12.0 - lon / 15.0

    # Hour angles for different sun elevations
    ha_sunrise = _hour_angle(lat, decl, -0.833)  # sunrise/sunset
    ha_civil = _hour_angle(lat, decl, -6.0)       # civil twilight (dawn/dusk)

    if ha_sunrise is None:
        # Polar day or polar night
        noon_elev = 90 - abs(lat - decl)
        return {"timeOfDay": "Day" if noon_elev > 0 else "Night"}

    sunrise = solar_noon - ha_sunrise / 15.0
    sunset = solar_noon + ha_sunrise / 15.0

    if ha_civil is not None:
        dawn = solar_noon - ha_civil / 15.0
        dusk = solar_noon + ha_civil / 15.0
    else:
        dawn = sunrise
        dusk = sunset

    hour = dt.hour + dt.minute / 60.0 + dt.second / 3600.0

    if hour < dawn or hour > dusk:
        tod = "Night"
    elif hour < sunrise:
        tod = "Dawn"
    elif hour <= sunset:
        tod = "Day"
    else:
        tod = "Dusk"

    return {
        "timeOfDay": tod,
        "sunrise_utc": f"{int(sunrise):02d}:{int((sunrise % 1) * 60):02d}",
        "sunset_utc": f"{int(sunset):02d}:{int((sunset % 1) * 60):02d}",
        "dawn_utc": f"{int(dawn):02d}:{int((dawn % 1) * 60):02d}",
        "dusk_utc": f"{int(dusk):02d}:{int((dusk % 1) * 60):02d}",
    }


# ---------------------------------------------------------------------------
# Country lookup
# ---------------------------------------------------------------------------

_COUNTRY_DATA = None


def _load_country_data():
    global _COUNTRY_DATA
    if _COUNTRY_DATA is not None:
        return
    geo_path = PROJECT_ROOT / "data" / "countries-110m.json"
    if not geo_path.exists():
        geo_path = PROJECT_ROOT / "public" / "data" / "countries-110m.json"
    if not geo_path.exists():
        geo_path = PROJECT_ROOT / "public" / "countries-110m.json"
    if geo_path.exists():
        _COUNTRY_DATA = json.loads(geo_path.read_text())
    else:
        _COUNTRY_DATA = {}


def _point_in_polygon(lat: float, lon: float, coords: list) -> bool:
    """Ray-casting point-in-polygon test."""
    n = len(coords)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = coords[i]
        xj, yj = coords[j]
        if ((yi > lat) != (yj > lat)) and (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def get_country(lat: float, lon: float) -> str | None:
    """Look up country name from coordinates using GeoJSON."""
    _load_country_data()
    if not _COUNTRY_DATA or "features" not in _COUNTRY_DATA:
        return None

    for feature in _COUNTRY_DATA["features"]:
        geom = feature.get("geometry", {})
        props = feature.get("properties", {})
        geom_type = geom.get("type")
        coords = geom.get("coordinates", [])

        if geom_type == "Polygon":
            if _point_in_polygon(lat, lon, coords[0]):
                return props.get("ADMIN") or props.get("NAME")
        elif geom_type == "MultiPolygon":
            for polygon in coords:
                if _point_in_polygon(lat, lon, polygon[0]):
                    return props.get("ADMIN") or props.get("NAME")
    return None


# Also use geo-cache if available
NON_DRIVABLE_ROADS = {"path", "pedestrian", "track"}


def _query_road_class(lon: float, lat: float, token: str) -> str | None:
    """Fetch road class from Mapbox Tilequery API."""
    try:
        url = (
            f"https://api.mapbox.com/v4/mapbox.mapbox-streets-v8/tilequery/{lon},{lat}.json"
            f"?layers=road&radius=15&limit=5&access_token={token}"
        )
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        features = resp.json().get("features", [])
        if not features:
            return None
        # Prefer closest drivable road
        for f in features:
            cls = (f.get("properties") or {}).get("class")
            if cls and cls not in NON_DRIVABLE_ROADS:
                return cls
        return (features[0].get("properties") or {}).get("class")
    except Exception:
        return None


_GEO_CACHE = None


def _load_geo_cache():
    global _GEO_CACHE
    if _GEO_CACHE is not None:
        return
    cache_path = PROJECT_ROOT / "data" / "geo-cache.json"
    if cache_path.exists():
        _GEO_CACHE = json.loads(cache_path.read_text())
    else:
        _GEO_CACHE = {}


def get_country_cached(lat: float, lon: float) -> str | None:
    """Try geo-cache first, then GeoJSON fallback, then Mapbox reverse geocode."""
    _load_geo_cache()
    key = f"{round(lat, 2)},{round(lon, 2)}"
    cached = _GEO_CACHE.get(key)
    if cached and cached != "Unknown":
        return cached

    # GeoJSON fallback
    country = get_country(lat, lon)
    if country:
        return country

    # Mapbox reverse geocode fallback (prefer sk token)
    token = _load_env_var("MAPBOX_TOKEN") or _load_env_var("NEXT_PUBLIC_MAPBOX_TOKEN")
    if token:
        try:
            resp = requests.get(
                f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
                f"?types=country&limit=1&access_token={token}",
                timeout=10,
            )
            if resp.status_code == 200:
                features = resp.json().get("features", [])
                if features:
                    country = features[0].get("place_name")
                    if country:
                        # Update in-memory cache
                        _GEO_CACHE[key] = country
                        return country
        except Exception:
            pass

    return None


_CITY_CACHE: dict[str, str | None] = {}


def get_city_cached(lat: float, lon: float) -> str | None:
    """Reverse geocode city/place name via Mapbox."""
    key = f"{round(lat, 2)},{round(lon, 2)}"
    if key in _CITY_CACHE:
        return _CITY_CACHE[key]

    token = _load_env_var("MAPBOX_TOKEN") or _load_env_var("NEXT_PUBLIC_MAPBOX_TOKEN")
    if not token:
        return None

    try:
        resp = requests.get(
            f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
            f"?types=place&limit=1&access_token={token}",
            timeout=10,
        )
        if resp.status_code == 200:
            features = resp.json().get("features", [])
            if features:
                city = features[0].get("text")
                _CITY_CACHE[key] = city
                return city
    except Exception:
        pass

    _CITY_CACHE[key] = None
    return None


# ---------------------------------------------------------------------------
# DB queries
# ---------------------------------------------------------------------------

def get_completed_video_ids(conn, limit: int = 0) -> list[str]:
    """Get video IDs with completed detection runs."""
    sql = """SELECT DISTINCT dr.video_id
             FROM detection_runs dr
             WHERE dr.status = 'completed'
             ORDER BY dr.completed_at DESC"""
    if limit > 0:
        sql += f" LIMIT {limit}"
    return [r[0] if isinstance(r, tuple) else r["video_id"] for r in conn.execute(sql).fetchall()]


def get_triage(conn, video_id: str) -> dict | None:
    row = conn.execute(
        """SELECT event_type, triage_result, rules_triggered,
                  speed_min, speed_max, speed_mean, speed_stddev,
                  gnss_displacement_m, video_size, event_timestamp,
                  lat, lon, road_class, country, city
           FROM triage_results WHERE id = ?""",
        (video_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "eventType": row[0], "triageResult": row[1],
        "rulesTriggered": json.loads(row[2]) if row[2] else [],
        "speedMin": row[3], "speedMax": row[4],
        "speedMean": row[5], "speedStddev": row[6],
        "gnssDisplacementM": row[7], "videoSize": row[8],
        "eventTimestamp": row[9],
        "lat": row[10], "lon": row[11], "roadClass": row[12],
        "country": row[13], "city": row[14],
    }


def get_detection_run(conn, video_id: str) -> dict | None:
    row = conn.execute(
        """SELECT id, model_name, status, config_json, detection_count,
                  machine_id, started_at, completed_at
           FROM detection_runs
           WHERE video_id = ? AND status = 'completed'
           ORDER BY completed_at DESC LIMIT 1""",
        (video_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "runId": row[0], "modelName": row[1], "status": row[2],
        "config": json.loads(row[3]) if row[3] else {},
        "detectionCount": row[4], "machineId": row[5],
        "startedAt": row[6], "completedAt": row[7],
    }


def get_frame_detections(conn, video_id: str, run_id: str | None = None) -> list[dict]:
    if run_id:
        rows = conn.execute(
            """SELECT frame_ms, label, confidence, x_min, y_min, x_max, y_max,
                      frame_width, frame_height
               FROM frame_detections WHERE video_id = ? AND run_id = ?
               ORDER BY frame_ms""",
            (video_id, run_id),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT frame_ms, label, confidence, x_min, y_min, x_max, y_max,
                      frame_width, frame_height
               FROM frame_detections WHERE video_id = ?
               ORDER BY frame_ms""",
            (video_id,),
        ).fetchall()
    return [
        {
            "frameMs": r[0], "label": r[1], "confidence": round(r[2], 4),
            "bbox": {"xMin": round(r[3], 1), "yMin": round(r[4], 1),
                     "xMax": round(r[5], 1), "yMax": round(r[6], 1)},
            "frameWidth": r[7], "frameHeight": r[8],
        }
        for r in rows
    ]


def get_detection_segments(conn, video_id: str, run_id: str | None = None) -> list[dict]:
    if run_id:
        rows = conn.execute(
            """SELECT label, start_ms, end_ms, max_confidence, support_level, source
               FROM video_detection_segments WHERE video_id = ? AND run_id = ?
               ORDER BY start_ms""",
            (video_id, run_id),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT label, start_ms, end_ms, max_confidence, support_level, source
               FROM video_detection_segments WHERE video_id = ?
               ORDER BY start_ms""",
            (video_id,),
        ).fetchall()
    return [
        {
            "label": r[0], "startMs": r[1], "endMs": r[2],
            "maxConfidence": round(r[3], 4), "supportLevel": r[4], "source": r[5],
        }
        for r in rows
    ]


def get_scene_attributes(conn, video_id: str) -> dict:
    rows = conn.execute(
        """SELECT attribute, value, confidence
           FROM scene_attributes WHERE video_id = ?""",
        (video_id,),
    ).fetchall()
    return {r[0]: {"value": r[1], "confidence": r[2]} for r in rows}


def get_blur_status(conn, video_id: str) -> dict | None:
    try:
        row = conn.execute(
            "SELECT status, face_count, s3_url FROM blur_runs WHERE video_id = ?",
            (video_id,),
        ).fetchone()
        if not row:
            return None
        return {"status": row[0], "faceCount": row[1], "s3Url": row[2]}
    except Exception:
        return None


def get_clip_summary(conn, video_id: str) -> str | None:
    try:
        row = conn.execute(
            "SELECT summary FROM clip_summaries WHERE video_id = ?", (video_id,),
        ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def generate_summary(meta: dict) -> str:
    """Generate a deterministic summary from metadata fields."""
    parts = []

    # Event type
    event_block = meta.get("event") or {}
    event_type = event_block.get("type") or meta.get("event", {}).get("type")
    if event_type:
        parts.append(event_type.replace("_", " ").capitalize() + " event")
    else:
        parts.append("Driving event")

    # Road class
    road_class = event_block.get("roadClass")
    if road_class:
        parts.append(f"on a {road_class.replace('_', ' ')} road")

    # Location (city, country)
    city = event_block.get("city")
    country = event_block.get("country")
    location_str = ", ".join(filter(None, [city, country]))
    if location_str:
        parts[-1] = parts[-1] + f" in {location_str}" if len(parts) > 1 else f"in {location_str}"

    # Speed
    speed_min = event_block.get("speedMin")
    speed_max = event_block.get("speedMax")
    if speed_min is not None and speed_max is not None:
        if abs(speed_max - speed_min) < 3:
            parts.append(f"at {round(speed_min)} mph")
        else:
            parts.append(f"at {round(speed_min)}-{round(speed_max)} mph")

    # Time of day
    tod = event_block.get("timeOfDay")
    if tod:
        parts.append(f"during {tod.lower()}")

    # VRU detections
    vru_labels = meta.get("vruLabelsDetected") or []
    if vru_labels:
        segments = meta.get("detectionSegments") or []
        label_counts = {}
        for seg in segments:
            label = seg.get("label", "")
            label_counts[label] = label_counts.get(label, 0) + 1

        det_parts = []
        for label in vru_labels:
            count = label_counts.get(label, 1)
            if count > 1:
                det_parts.append(f"{count} {label}s")
            else:
                det_parts.append(f"{count} {label}")
        parts.append(f"with {', '.join(det_parts)} detected")

    return ". ".join(". ".join(parts).split(". ")).rstrip(".") + "."




# ---------------------------------------------------------------------------
# Bee Maps API fetch
# ---------------------------------------------------------------------------

EVENT_CACHE_DIR = PROJECT_ROOT / "data" / "event-cache"


def load_event_from_cache(video_id: str) -> dict | None:
    """Load event data from local gzipped cache."""
    import gzip
    cache_path = EVENT_CACHE_DIR / f"{video_id}.json.gz"
    if cache_path.exists():
        with gzip.open(cache_path) as f:
            return json.load(f)
    return None


def fetch_event_from_api(api_key: str, video_id: str) -> dict | None:
    """Fetch full event data from Bee Maps API."""
    try:
        resp = api_request(
            "GET",
            f"{API_BASE_URL}/{video_id}?includeGnssData=true&includeImuData=true",
            headers={"Authorization": api_key, "Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def load_event(api_key: str | None, video_id: str) -> dict | None:
    """Load event from cache first, fall back to API."""
    event = load_event_from_cache(video_id)
    if event:
        return event
    if api_key:
        return fetch_event_from_api(api_key, video_id)
    return None


# ---------------------------------------------------------------------------
# Build metadata
# ---------------------------------------------------------------------------

def build_metadata(conn, api_key: str, video_id: str) -> dict:
    """Build the complete metadata JSON for a video."""
    meta: dict = {"id": video_id}

    # 1. Event data (cache first, then API)
    event = load_event(api_key, video_id)
    if event:
        meta["event"] = {
            "type": event.get("type"),
            "timestamp": event.get("timestamp"),
            "location": event.get("location"),
            "videoUrl": event.get("videoUrl"),
            "metadata": event.get("metadata"),
        }
        if event.get("gnssData"):
            meta["gnssData"] = event["gnssData"]
        if event.get("imuData"):
            meta["imuData"] = event["imuData"]
    else:
        meta["event"] = None

    # 2. Triage
    triage = get_triage(conn, video_id)
    meta["triage"] = triage

    # 3. Detection run
    det_run = get_detection_run(conn, video_id)
    meta["detectionRun"] = det_run

    run_id = det_run["runId"] if det_run else None

    # 4. Frame detections
    meta["frameDetections"] = get_frame_detections(conn, video_id, run_id)

    # 5. Detection segments
    meta["detectionSegments"] = get_detection_segments(conn, video_id, run_id)

    # 6. Scene attributes
    meta["sceneAttributes"] = get_scene_attributes(conn, video_id)

    # 7. Time of day
    lat = lon = timestamp = None
    if event and event.get("location"):
        lat = event["location"].get("lat")
        lon = event["location"].get("lon")
        timestamp = event.get("timestamp")
    elif triage:
        lat = triage.get("lat")
        lon = triage.get("lon")
        timestamp = triage.get("eventTimestamp")

    if lat and lon and timestamp:
        meta["timeOfDay"] = get_time_of_day(timestamp, lat, lon)
    else:
        meta["timeOfDay"] = None

    # 8. Country
    if lat and lon:
        meta["country"] = get_country_cached(lat, lon)
    else:
        meta["country"] = None

    # 9. Blur status
    meta["blur"] = get_blur_status(conn, video_id)

    # 10. VRU summary (before summary so generate_summary can use it)
    vru_labels = sorted(set(d["label"] for d in meta["detectionSegments"]))
    meta["vruLabelsDetected"] = vru_labels

    # 11. Summary — use DB if available, otherwise generate from metadata
    meta["summary"] = get_clip_summary(conn, video_id) or generate_summary(meta)

    # 13. Export timestamp
    meta["exportedAt"] = datetime.now(timezone.utc).isoformat()

    return meta


def build_production_metadata(conn, api_key: str, video_id: str) -> dict:
    """Build production metadata JSON — restructured for delivery.

    Differences from build_metadata:
    - event block includes speedMin/Max/Mean/Stddev, roadClass, timeOfDay, country
    - No triage block (triageResult, displacement, rulesTriggered removed)
    """
    meta: dict = {"id": video_id}

    # 1. Event data from API
    event = load_event(api_key, video_id)
    triage = get_triage(conn, video_id)

    # Resolve lat/lon/timestamp for derived fields
    lat = lon = timestamp = None
    if event and event.get("location"):
        lat = event["location"].get("lat")
        lon = event["location"].get("lon")
        timestamp = event.get("timestamp")
    elif triage:
        lat = triage.get("lat")
        lon = triage.get("lon")
        timestamp = triage.get("eventTimestamp")

    # Build event block with speed stats, road class, timeOfDay, country
    if event:
        event_metadata = dict(event.get("metadata") or {})
        event_metadata["camera"] = "bee"
        event_block: dict = {
            "type": event.get("type"),
            "timestamp": event.get("timestamp"),
            "location": event.get("location"),
            "videoUrl": event.get("videoUrl"),
            "metadata": event_metadata,
        }
    else:
        event_block = {
            "type": triage.get("eventType") if triage else None,
            "timestamp": triage.get("eventTimestamp") if triage else None,
            "location": {"lat": lat, "lon": lon} if lat and lon else None,
            "videoUrl": None,
            "metadata": {"camera": "bee"},
        }

    # Speed stats from triage
    if triage:
        event_block["speedMin"] = triage.get("speedMin")
        event_block["speedMax"] = triage.get("speedMax")
        event_block["speedMean"] = triage.get("speedMean")
        event_block["speedStddev"] = triage.get("speedStddev")
        event_block["roadClass"] = triage.get("roadClass")
    else:
        event_block["speedMin"] = None
        event_block["speedMax"] = None
        event_block["speedMean"] = None
        event_block["speedStddev"] = None
        event_block["roadClass"] = None

    # Fetch road class from Mapbox Tilequery if missing (prefer sk token)
    if not event_block["roadClass"] and lat and lon:
        token = _load_env_var("MAPBOX_TOKEN") or _load_env_var("NEXT_PUBLIC_MAPBOX_TOKEN")
        if token:
            event_block["roadClass"] = _query_road_class(lon, lat, token)

    # Time of day (just the label: "Day", "Night", "Dawn", or "Dusk")
    if lat and lon and timestamp:
        tod_info = get_time_of_day(timestamp, lat, lon)
        event_block["timeOfDay"] = tod_info.get("timeOfDay") if tod_info else None
    else:
        event_block["timeOfDay"] = None

    # Country — prefer triage DB value, fall back to geocoding
    event_block["country"] = (triage or {}).get("country") or None
    if not event_block["country"] and lat and lon:
        event_block["country"] = get_country_cached(lat, lon)

    # City — prefer triage DB value, fall back to geocoding
    event_block["city"] = (triage or {}).get("city") or None
    if not event_block["city"] and lat and lon:
        event_block["city"] = get_city_cached(lat, lon)

    meta["event"] = event_block

    # 2. Detection run (internal — not included in output, but needed for run_id)
    det_run = get_detection_run(conn, video_id)
    run_id = det_run["runId"] if det_run else None

    # 3. Frame detections + segments
    meta["frameDetections"] = get_frame_detections(conn, video_id, run_id)
    meta["detectionSegments"] = [
        {k: v for k, v in seg.items() if k != "source"}
        for seg in get_detection_segments(conn, video_id, run_id)
    ]

    # 4. VRU summary (before summary so generate_summary can use it)
    meta["vruLabelsDetected"] = sorted(set(d["label"] for d in meta["detectionSegments"]))

    # 5. Summary — placed after country/city so generate_summary can use them
    meta["summary"] = get_clip_summary(conn, video_id) or generate_summary(meta)

    # 8. Export timestamp
    meta["exportedAt"] = datetime.now(timezone.utc).isoformat()

    # 9. GNSS + IMU (last — large arrays)
    if event and event.get("gnssData"):
        meta["gnssData"] = event["gnssData"]
    else:
        meta["gnssData"] = []
    if event and event.get("imuData"):
        meta["imuData"] = event["imuData"]
    else:
        meta["imuData"] = []

    return meta


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Export full metadata JSON per video")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of videos (0=all)")
    parser.add_argument("--event-id", type=str, help="Export a single event")
    parser.add_argument("--skip-api", action="store_true", help="Skip Bee Maps API fetch (DB only)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing metadata files")
    parser.add_argument("--production", action="store_true", help="Use production metadata format")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    conn = get_db()
    api_key = None
    if not args.skip_api:
        api_key = load_api_key()

    if args.event_id:
        video_ids = [args.event_id]
    else:
        video_ids = get_completed_video_ids(conn, args.limit)

    print(f"{BOLD}Metadata Export{RESET}")
    print(f"  Videos: {len(video_ids)}")
    print(f"  Output: {OUTPUT_DIR}")
    print()

    exported = 0
    skipped = 0
    failed = 0

    for i, vid in enumerate(video_ids):
        out_path = OUTPUT_DIR / f"{vid}.json"

        if out_path.exists() and not args.overwrite:
            skipped += 1
            continue

        try:
            builder = build_production_metadata if args.production else build_metadata
            meta = builder(conn, api_key, vid)
            out_path.write_text(json.dumps(meta, indent=2, default=str))
            exported += 1

            if (exported) % 50 == 0 or exported == 1:
                print(f"  {DIM}{exported} exported, {skipped} skipped, {failed} failed ({i+1}/{len(video_ids)}){RESET}")

        except Exception as e:
            failed += 1
            print(f"  {RED}{vid[:16]}… failed: {e}{RESET}")

    conn.close()

    print(f"\n{GREEN}Done: {exported} exported, {skipped} skipped, {failed} failed{RESET}")
    print(f"  Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
