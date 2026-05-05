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
  - Delivered video metadata (codec, container, dimensions, frames, bitrate, size)

Output:
  - default exports: data/metadata/{eventId}.json
  - production exports: data/metadata/{optional-prefix}/{eventId}.json

Usage:
    python3 scripts/export-metadata.py
    python3 scripts/export-metadata.py --limit 100
    python3 scripts/export-metadata.py --event-id 698f4badae71cbe9f847580a
    python3 scripts/export-metadata.py --event-id 698f4badae71cbe9f847580a --production --video-path produced.mp4
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "metadata"
MAP_FEATURE_CACHE_DIR = PROJECT_ROOT / "data" / "map-feature-cache"
PUBLIC_VIDEO_DIR = PROJECT_ROOT / "public" / "videos"
LOGGER = logging.getLogger("export_metadata")

DEVELOPER_API_BASE_URL = "https://beemaps.com/api/developer"
API_BASE_URL = f"{DEVELOPER_API_BASE_URL}/aievents"
MAP_DATA_URL = f"{DEVELOPER_API_BASE_URL}/map-data"

EARTH_RADIUS_M = 6_371_000
MAP_FEATURE_CACHE_VERSION = 1
MAP_FEATURE_CORRIDOR_RADIUS_M = 50.0
MAP_FEATURE_SIMPLIFY_TOLERANCE_M = 8.0
MAP_FEATURE_MAX_CHUNK_POINTS = 40
MAP_FEATURE_MAX_CHUNK_LENGTH_M = 700.0
MAP_FEATURE_CAP_SEGMENTS = 8
VIDEO_METADATA_PROBE_TIMEOUT_S = int(os.environ.get("VIDEO_METADATA_PROBE_TIMEOUT_S", "180"))
PTS_CFR_MIN_SPREAD_US = 1000

PRODUCTION_S3_BUCKET = os.environ.get("PRODUCTION_S3_BUCKET", "hivemapper-blurred-ai-event-videos")
PRODUCTION_S3_REGION = os.environ.get("PRODUCTION_S3_REGION", "us-west-2")
PRODUCTION_S3_PREFIX = os.environ.get("PRODUCTION_S3_PREFIX", "")
SAFE_S3_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")
SAFE_S3_FILENAME_RE = re.compile(r"^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)?$")
LOCAL_EDITED_EVENT_ID_RE = re.compile(r"^[A-Za-z0-9]{24}-.+")

METADATA_VRU_LABELS = {
    "person",
    "pedestrian",
    "child",
    "kids",
    "construction worker",
    "person wearing safety vest",
    "work-zone-person",
    "work zone person",
    "bicycle",
    "bicycle rider",
    "cyclist",
    "motorcycle",
    "motorcycle rider",
    "motorcyclist",
    "scooter",
    "scooter rider",
    "electric scooter",
    "electric kick scooter",
    "wheelchair",
    "stroller",
    "skateboard",
    "skateboarder",
    "animal",
    "cat",
    "dog",
    "deer",
    "bird",
    "horse",
    "sheep",
    "cow",
    "bear",
    "elephant",
    "zebra",
    "giraffe",
}


class VideoTimingValidationError(ValueError):
    """Raised when a probed video cannot be shipped with timing metadata."""


def is_metadata_vru_label(label) -> bool:
    if not isinstance(label, str):
        return False
    normalized = re.sub(r"\s+", " ", label.strip().lower().replace("_", " "))
    return normalized in METADATA_VRU_LABELS


def _safe_s3_segment(value: str, label: str) -> str:
    segment = str(value).strip()
    if not segment or not SAFE_S3_SEGMENT_RE.fullmatch(segment):
        raise ValueError(
            f"{label} must contain only letters, numbers, hyphens, and underscores: {value!r}"
        )
    return segment


def _safe_s3_filename(value: str, label: str) -> str:
    filename = str(value).strip()
    if not filename or not SAFE_S3_FILENAME_RE.fullmatch(filename):
        raise ValueError(
            f"{label} must contain only letters, numbers, hyphens, underscores, and a file extension dot: {value!r}"
        )
    return filename


def _normalize_s3_prefix(prefix: str | None) -> str:
    if not prefix:
        return ""
    return "/".join(
        _safe_s3_segment(part, "PRODUCTION_S3_PREFIX segment")
        for part in prefix.split("/")
        if part.strip()
    )


def _join_s3_key(prefix: str | None, filename_segment: str) -> str:
    clean_prefix = _normalize_s3_prefix(prefix)
    filename = _safe_s3_filename(filename_segment, "production filename")
    return f"{clean_prefix}/{filename}" if clean_prefix else filename


def production_video_key(video_id: str) -> str:
    return _join_s3_key(PRODUCTION_S3_PREFIX, f"{_safe_s3_segment(video_id, 'video_id')}.mp4")


def production_metadata_key(video_id: str) -> str:
    return _join_s3_key(PRODUCTION_S3_PREFIX, f"{_safe_s3_segment(video_id, 'video_id')}.json")


def production_video_url(video_id: str) -> str:
    key = production_video_key(video_id)
    escaped_key = quote(key, safe="/._-")
    return f"https://{PRODUCTION_S3_BUCKET}.s3.{PRODUCTION_S3_REGION}.amazonaws.com/{escaped_key}"


def validate_delivery_video_url(video_url: str) -> str:
    parsed = urlparse(video_url)
    path_segments = [segment for segment in parsed.path.split("/") if segment]
    if not path_segments:
        raise ValueError("production video URL must include an S3 object path")
    for segment in path_segments[:-1]:
        _safe_s3_segment(segment, "production video URL path segment")
    _safe_s3_filename(path_segments[-1], "production video URL filename")
    return video_url


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


def _beemaps_auth_header(api_key: str) -> str:
    return api_key if api_key.startswith("Basic ") else f"Basic {api_key}"


BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RED = "\033[31m"
RESET = "\033[0m"


# ---------------------------------------------------------------------------
# Map feature corridor export
# ---------------------------------------------------------------------------

def _as_float(value) -> float | None:
    try:
        if value is None:
            return None
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return None
        return result
    except (TypeError, ValueError):
        return None


def _as_int(value) -> int | None:
    number = _as_float(value)
    if number is None:
        return None
    return int(number)


def _first_video_stream(probe: dict) -> dict:
    for stream in probe.get("streams") or []:
        if isinstance(stream, dict) and stream.get("codec_type") == "video":
            return stream
    return {}


def _container_from_probe(probe: dict, source: str | Path | None) -> str | None:
    format_name = str((probe.get("format") or {}).get("format_name") or "")
    names = [name.strip() for name in format_name.split(",") if name.strip()]
    if "mp4" in names:
        return "mp4"

    source_text = str(source or "")
    try:
        parsed = urlparse(source_text)
        source_path = parsed.path if parsed.scheme else source_text
    except Exception:
        source_path = source_text
    suffix = Path(source_path).suffix.lower().lstrip(".")
    if suffix == "mp4":
        return "mp4"
    return names[0] if names else suffix or None


def _local_file_size(source: str | Path | None) -> int | None:
    if source is None:
        return None
    if isinstance(source, Path):
        path = source
    else:
        parsed = urlparse(str(source))
        if parsed.scheme:
            return None
        path = Path(source)
    try:
        if path.exists():
            return path.stat().st_size
    except OSError:
        return None
    return None


def video_metadata_from_probe(probe: dict, source: str | Path | None = None) -> dict:
    """Convert ffprobe JSON into the production metadata video block."""
    stream = _first_video_stream(probe)
    if not stream:
        raise RuntimeError("ffprobe did not return a video stream")

    format_info = probe.get("format") or {}
    frame_count = _as_int(stream.get("nb_read_frames")) or _as_int(stream.get("nb_frames"))
    size_bytes = _local_file_size(source) or _as_int(format_info.get("size"))
    bitrate_bps = _as_int(format_info.get("bit_rate")) or _as_int(stream.get("bit_rate"))
    duration = _as_float(format_info.get("duration"))
    if not bitrate_bps and size_bytes and duration and duration > 0:
        bitrate_bps = int((size_bytes * 8) / duration)

    return {
        "codec": stream.get("codec_name"),
        "container": _container_from_probe(probe, source),
        "width": _as_int(stream.get("width")),
        "height": _as_int(stream.get("height")),
        "frame_count": frame_count,
        "bitrate_bps": bitrate_bps,
        "size_bytes": size_bytes,
    }


def probe_video_metadata(source: str | Path) -> dict:
    cmd = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-count_frames",
        "-show_entries",
        "format=format_name,duration,size,bit_rate:"
        "stream=codec_type,codec_name,width,height,bit_rate,nb_frames,nb_read_frames",
        "-of", "json",
        str(source),
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=VIDEO_METADATA_PROBE_TIMEOUT_S,
    )
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "ffprobe failed").strip()
        raise RuntimeError(stderr)

    try:
        probe = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"ffprobe returned invalid JSON: {exc}") from exc
    return video_metadata_from_probe(probe, source)


def _frame_time_to_us(frame_time: float, origin: float) -> int:
    return int(round((frame_time - origin) * 1_000_000))


def pts_us_from_frame_times(frame_times: list[float]) -> list[int]:
    if not frame_times:
        raise VideoTimingValidationError("ffprobe returned no frame PTS values")

    origin = frame_times[0]
    pts_us = [_frame_time_to_us(frame_time, origin) for frame_time in frame_times]
    pts_us[0] = 0
    return pts_us


def extract_pts_us(video_path: str | Path) -> list[int]:
    """Extract per-frame presentation timestamps as microsecond offsets."""
    cmd = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries",
        "frame=best_effort_timestamp_time",
        "-of", "json",
        str(video_path),
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=VIDEO_METADATA_PROBE_TIMEOUT_S,
    )
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "ffprobe failed").strip()
        raise RuntimeError(stderr)

    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"ffprobe returned invalid JSON: {exc}") from exc

    frames = payload.get("frames")
    if not isinstance(frames, list):
        raise VideoTimingValidationError("ffprobe did not return frame PTS values")

    frame_times: list[float] = []
    missing_indices: list[int] = []
    for index, frame in enumerate(frames):
        if not isinstance(frame, dict):
            missing_indices.append(index)
            continue
        value = frame.get("best_effort_timestamp_time")
        if value is None or value == "N/A":
            missing_indices.append(index)
            continue
        try:
            timestamp = float(value)
        except (TypeError, ValueError):
            missing_indices.append(index)
            continue
        if not math.isfinite(timestamp):
            missing_indices.append(index)
            continue
        frame_times.append(timestamp)

    if missing_indices:
        LOGGER.warning(
            "Skipping clip because %s frame(s) are missing best_effort_timestamp_time; first indices: %s",
            len(missing_indices),
            missing_indices[:10],
        )
        raise VideoTimingValidationError("video frame PTS is missing or invalid")

    return pts_us_from_frame_times(frame_times)


def validate_metadata(metadata: dict, *, allow_low_pts_spread: bool = False) -> None:
    video = metadata.get("video")
    pts_us = metadata.get("pts_us")
    if not isinstance(video, dict):
        raise VideoTimingValidationError("metadata.video must be an object")
    if not isinstance(pts_us, list) or not pts_us:
        raise VideoTimingValidationError("metadata.pts_us must be a non-empty array")
    if not all(isinstance(value, int) for value in pts_us):
        raise VideoTimingValidationError("metadata.pts_us must contain integer microseconds only")

    frame_count = video.get("frame_count")
    if not isinstance(frame_count, int) or frame_count <= 0:
        raise VideoTimingValidationError("metadata.video.frame_count must be a positive integer")

    if pts_us[0] != 0:
        raise VideoTimingValidationError("First PTS must be 0")
    if len(pts_us) != frame_count:
        LOGGER.error(
            "PTS array length %s does not match video.frame_count %s",
            len(pts_us),
            frame_count,
        )
        raise VideoTimingValidationError("PTS array length must match frame_count")
    if any(pts_us[index] >= pts_us[index + 1] for index in range(len(pts_us) - 1)):
        raise VideoTimingValidationError("PTS values must be monotonically increasing")

    if len(pts_us) >= 2:
        deltas = [
            pts_us[index + 1] - pts_us[index]
            for index in range(len(pts_us) - 1)
        ]
        spread_us = max(deltas) - min(deltas)
        if spread_us < PTS_CFR_MIN_SPREAD_US and not allow_low_pts_spread:
            raise VideoTimingValidationError(
                f"Delta spread is {spread_us}us (<{PTS_CFR_MIN_SPREAD_US}us) - "
                "file appears CFR re-encoded. Real sensor timestamps are required for shipping."
            )


def resolve_local_video_path(video_id: str, video_url: str | None) -> Path | None:
    """Resolve locally edited /videos URLs to their synced public MP4."""
    candidates: list[Path] = []
    if video_url:
        parsed = urlparse(video_url)
        path_part = parsed.path if parsed.scheme in {"http", "https"} else video_url
        if path_part.startswith("/videos/"):
            candidates.append(PUBLIC_VIDEO_DIR / unquote(Path(path_part).name))

    candidates.append(PUBLIC_VIDEO_DIR / f"{video_id}.mp4")

    public_root = PUBLIC_VIDEO_DIR.resolve()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
            resolved.relative_to(public_root)
        except ValueError:
            continue
        if resolved.exists():
            return resolved
    return None


def video_source_candidates(
    video_id: str,
    event: dict | None,
    delivery_video_url: str,
    video_path: str | Path | None = None,
) -> list[str | Path]:
    event_video_url = event.get("videoUrl") if isinstance(event, dict) else None
    local_video_path = resolve_local_video_path(video_id, event_video_url)
    candidates: list[str | Path] = []
    seen: set[str] = set()

    def add_candidate(source: str | Path | None) -> None:
        if not source:
            return
        key = str(source)
        if key in seen:
            return
        seen.add(key)
        candidates.append(source)

    add_candidate(Path(video_path) if video_path else None)
    add_candidate(delivery_video_url)
    add_candidate(local_video_path)
    add_candidate(event_video_url)
    return candidates


def build_video_metadata_payload(
    video_id: str,
    event: dict | None,
    delivery_video_url: str,
    video_path: str | Path | None = None,
    allow_low_pts_spread: bool = False,
) -> dict:
    """Build and validate the production video metadata payload."""
    candidates = video_source_candidates(video_id, event, delivery_video_url, video_path)

    errors: list[str] = []
    for source in candidates:
        try:
            payload = {
                "video": probe_video_metadata(source),
                "pts_us": extract_pts_us(source),
            }
            validate_metadata(payload, allow_low_pts_spread=allow_low_pts_spread)
            return payload
        except VideoTimingValidationError as exc:
            raise RuntimeError(
                f"Video timing validation failed for {video_id} from {source}: {exc}"
            ) from exc
        except Exception as exc:
            errors.append(f"{source}: {exc}")

    details = "; ".join(errors) if errors else "no video source candidates"
    raise RuntimeError(f"Could not build video metadata for {video_id}: {details}")


def build_video_metadata(
    video_id: str,
    event: dict | None,
    delivery_video_url: str,
    video_path: str | Path | None = None,
) -> dict:
    """Build the top-level video block, preferring the delivered production MP4."""
    return build_video_metadata_payload(video_id, event, delivery_video_url, video_path)["video"]


def _valid_gnss_points(gnss_data: list | None) -> list[dict]:
    points: list[dict] = []
    for point in gnss_data or []:
        if not isinstance(point, dict):
            continue
        lat = _as_float(point.get("lat"))
        lon = _as_float(point.get("lon"))
        if lat is None or lon is None:
            continue
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        if points and points[-1]["lat"] == lat and points[-1]["lon"] == lon:
            continue
        points.append({
            "lat": lat,
            "lon": lon,
            "timestamp": _as_float(point.get("timestamp")),
        })
    return points


def _project(lat: float, lon: float, lat0: float, lon0: float) -> tuple[float, float]:
    lat0_rad = math.radians(lat0)
    x = EARTH_RADIUS_M * math.radians(lon - lon0) * math.cos(lat0_rad)
    y = EARTH_RADIUS_M * math.radians(lat - lat0)
    return x, y


def _unproject(x: float, y: float, lat0: float, lon0: float) -> tuple[float, float]:
    lat = lat0 + math.degrees(y / EARTH_RADIUS_M)
    cos_lat0 = max(math.cos(math.radians(lat0)), 1e-9)
    lon = lon0 + math.degrees(x / (EARTH_RADIUS_M * cos_lat0))
    return lat, lon


def _project_path(points: list[dict]) -> tuple[list[dict], float, float]:
    lat0 = sum(p["lat"] for p in points) / len(points)
    lon0 = sum(p["lon"] for p in points) / len(points)
    projected: list[dict] = []
    cumulative = 0.0
    prev: dict | None = None
    for point in points:
        x, y = _project(point["lat"], point["lon"], lat0, lon0)
        projected_point = {**point, "x": x, "y": y, "alongTrackMeters": cumulative}
        if prev is not None:
            cumulative += math.hypot(x - prev["x"], y - prev["y"])
            projected_point["alongTrackMeters"] = cumulative
        projected.append(projected_point)
        prev = projected_point
    return projected, lat0, lon0


def _xy_distance(a: dict, b: dict) -> float:
    return math.hypot(a["x"] - b["x"], a["y"] - b["y"])


def _point_segment_distance(point: dict, start: dict, end: dict) -> float:
    dx = end["x"] - start["x"]
    dy = end["y"] - start["y"]
    seg_len2 = dx * dx + dy * dy
    if seg_len2 == 0:
        return _xy_distance(point, start)
    t = ((point["x"] - start["x"]) * dx + (point["y"] - start["y"]) * dy) / seg_len2
    t = max(0.0, min(1.0, t))
    nearest_x = start["x"] + t * dx
    nearest_y = start["y"] + t * dy
    return math.hypot(point["x"] - nearest_x, point["y"] - nearest_y)


def _simplify_path(points: list[dict], tolerance_m: float) -> list[dict]:
    if len(points) <= 2:
        return points

    keep = {0, len(points) - 1}
    stack = [(0, len(points) - 1)]
    while stack:
        start_idx, end_idx = stack.pop()
        if end_idx <= start_idx + 1:
            continue

        max_dist = -1.0
        max_idx = start_idx
        for idx in range(start_idx + 1, end_idx):
            dist = _point_segment_distance(points[idx], points[start_idx], points[end_idx])
            if dist > max_dist:
                max_dist = dist
                max_idx = idx

        if max_dist > tolerance_m:
            keep.add(max_idx)
            stack.append((start_idx, max_idx))
            stack.append((max_idx, end_idx))

    return [points[idx] for idx in sorted(keep)]


def _split_path_chunks(points: list[dict]) -> list[list[dict]]:
    if len(points) <= 1:
        return [points]

    chunks: list[list[dict]] = []
    current = [points[0]]
    current_len = 0.0

    for idx in range(1, len(points)):
        prev = points[idx - 1]
        point = points[idx]
        seg_len = _xy_distance(prev, point)
        should_split = (
            len(current) >= MAP_FEATURE_MAX_CHUNK_POINTS
            or (current_len > 0 and current_len + seg_len > MAP_FEATURE_MAX_CHUNK_LENGTH_M)
        )

        if should_split and len(current) >= 2:
            chunks.append(current)
            current = [current[-1], point]
            current_len = seg_len
        else:
            current.append(point)
            current_len += seg_len

    if len(current) >= 2 or not chunks:
        chunks.append(current)
    return chunks


def _segment_normal(start: dict, end: dict) -> tuple[float, float] | None:
    dx = end["x"] - start["x"]
    dy = end["y"] - start["y"]
    length = math.hypot(dx, dy)
    if length == 0:
        return None
    return -dy / length, dx / length


def _vertex_offset(points: list[dict], idx: int, radius_m: float) -> tuple[float, float, float]:
    if idx == 0:
        normal = _segment_normal(points[0], points[1])
        return (normal[0], normal[1], radius_m) if normal else (0.0, 1.0, radius_m)

    if idx == len(points) - 1:
        normal = _segment_normal(points[-2], points[-1])
        return (normal[0], normal[1], radius_m) if normal else (0.0, 1.0, radius_m)

    prev_normal = _segment_normal(points[idx - 1], points[idx])
    next_normal = _segment_normal(points[idx], points[idx + 1])
    if not prev_normal:
        prev_normal = next_normal
    if not next_normal:
        next_normal = prev_normal
    if not prev_normal or not next_normal:
        return 0.0, 1.0, radius_m

    nx = prev_normal[0] + next_normal[0]
    ny = prev_normal[1] + next_normal[1]
    norm_len = math.hypot(nx, ny)
    if norm_len < 1e-6:
        return next_normal[0], next_normal[1], radius_m

    nx /= norm_len
    ny /= norm_len
    dot = max(nx * next_normal[0] + ny * next_normal[1], 0.35)
    miter_len = min(radius_m / dot, radius_m * 2.0)
    return nx, ny, miter_len


def _arc_xy(
    center: dict,
    radius_m: float,
    start_angle: float,
    end_angle: float,
    include_start: bool = False,
) -> list[tuple[float, float]]:
    start_idx = 0 if include_start else 1
    points: list[tuple[float, float]] = []
    for idx in range(start_idx, MAP_FEATURE_CAP_SEGMENTS + 1):
        t = idx / MAP_FEATURE_CAP_SEGMENTS
        angle = start_angle + (end_angle - start_angle) * t
        points.append((
            center["x"] + radius_m * math.cos(angle),
            center["y"] + radius_m * math.sin(angle),
        ))
    return points


def _circle_polygon(lat: float, lon: float, radius_m: float, num_points: int = 32) -> list[list[float]]:
    coords: list[list[float]] = []
    lat_rad = math.radians(lat)
    for idx in range(num_points + 1):
        angle = 2 * math.pi * idx / num_points
        dlat = (radius_m / EARTH_RADIUS_M) * math.cos(angle)
        dlon = (radius_m / (EARTH_RADIUS_M * max(math.cos(lat_rad), 1e-9))) * math.sin(angle)
        coords.append([
            lon + math.degrees(dlon),
            lat + math.degrees(dlat),
        ])
    return coords


def _corridor_polygon(points: list[dict], lat0: float, lon0: float, radius_m: float) -> list[list[float]]:
    clean_points: list[dict] = []
    for point in points:
        if clean_points and _xy_distance(clean_points[-1], point) < 0.5:
            continue
        clean_points.append(point)

    if len(clean_points) == 1:
        return _circle_polygon(clean_points[0]["lat"], clean_points[0]["lon"], radius_m)

    left: list[tuple[float, float]] = []
    right: list[tuple[float, float]] = []
    for idx, point in enumerate(clean_points):
        nx, ny, offset_len = _vertex_offset(clean_points, idx, radius_m)
        left.append((point["x"] + nx * offset_len, point["y"] + ny * offset_len))
        right.append((point["x"] - nx * offset_len, point["y"] - ny * offset_len))

    start_angle = math.atan2(
        clean_points[1]["y"] - clean_points[0]["y"],
        clean_points[1]["x"] - clean_points[0]["x"],
    )
    end_angle = math.atan2(
        clean_points[-1]["y"] - clean_points[-2]["y"],
        clean_points[-1]["x"] - clean_points[-2]["x"],
    )

    polygon_xy: list[tuple[float, float]] = []
    polygon_xy.extend(left)
    polygon_xy.extend(_arc_xy(clean_points[-1], radius_m, end_angle + math.pi / 2, end_angle - math.pi / 2))
    polygon_xy.extend(reversed(right[:-1]))
    polygon_xy.extend(_arc_xy(clean_points[0], radius_m, start_angle - math.pi / 2, start_angle - 3 * math.pi / 2))

    coords: list[list[float]] = []
    for x, y in polygon_xy:
        lat, lon = _unproject(x, y, lat0, lon0)
        coords.append([lon, lat])

    if coords and coords[0] != coords[-1]:
        coords.append(coords[0])
    return coords


def _nearest_path_measure(lat: float, lon: float, path: list[dict], lat0: float, lon0: float) -> dict:
    x, y = _project(lat, lon, lat0, lon0)
    first_ts = path[0].get("timestamp")

    if len(path) == 1:
        dist = math.hypot(x - path[0]["x"], y - path[0]["y"])
        timestamp = path[0].get("timestamp")
        return {
            "distanceToPathMeters": dist,
            "nearestGnssTimestamp": timestamp,
            "offsetSeconds": (timestamp - first_ts) / 1000 if timestamp is not None and first_ts is not None else None,
            "alongTrackMeters": 0.0,
        }

    best: dict | None = None
    for idx in range(1, len(path)):
        start = path[idx - 1]
        end = path[idx]
        dx = end["x"] - start["x"]
        dy = end["y"] - start["y"]
        seg_len2 = dx * dx + dy * dy
        if seg_len2 == 0:
            continue

        t = ((x - start["x"]) * dx + (y - start["y"]) * dy) / seg_len2
        t = max(0.0, min(1.0, t))
        nearest_x = start["x"] + t * dx
        nearest_y = start["y"] + t * dy
        dist = math.hypot(x - nearest_x, y - nearest_y)

        start_ts = start.get("timestamp")
        end_ts = end.get("timestamp")
        timestamp = None
        offset_seconds = None
        if start_ts is not None and end_ts is not None:
            timestamp = start_ts + (end_ts - start_ts) * t
            if first_ts is not None:
                offset_seconds = (timestamp - first_ts) / 1000

        along = start["alongTrackMeters"] + math.sqrt(seg_len2) * t
        candidate = {
            "distanceToPathMeters": dist,
            "nearestGnssTimestamp": timestamp,
            "offsetSeconds": offset_seconds,
            "alongTrackMeters": along,
        }
        if best is None or candidate["distanceToPathMeters"] < best["distanceToPathMeters"]:
            best = candidate

    return best or {
        "distanceToPathMeters": None,
        "nearestGnssTimestamp": None,
        "offsetSeconds": None,
        "alongTrackMeters": None,
    }


def _rounded(value, digits: int = 1):
    return round(value, digits) if isinstance(value, (int, float)) else value


def _json_safe(value):
    return json.loads(json.dumps(value, default=str))


def _feature_fingerprint(feature: dict) -> str:
    props = feature.get("properties") or {}
    position = feature.get("position") or {}
    lat = _as_float(position.get("lat"))
    lon = _as_float(position.get("lon"))
    stable_id = (
        feature.get("id")
        or feature.get("mapFeatureId")
        or props.get("id")
        or props.get("mapFeatureId")
        or props.get("osmId")
    )
    if stable_id:
        return str(stable_id)
    props_key = json.dumps(props, sort_keys=True, default=str)
    return f"{feature.get('class')}:{round(lat or 0, 7)}:{round(lon or 0, 7)}:{props_key}"


def _build_feature_record(feature: dict, path: list[dict], lat0: float, lon0: float) -> dict | None:
    position = feature.get("position") or {}
    lat = _as_float(position.get("lat"))
    lon = _as_float(position.get("lon"))
    feature_class = feature.get("class")
    if lat is None or lon is None or not feature_class:
        return None

    nearest = _nearest_path_measure(lat, lon, path, lat0, lon0)
    props = _json_safe(feature.get("properties") or {})
    record: dict = {
        "class": feature_class,
        "position": {"lat": lat, "lon": lon},
        "distanceToPathMeters": _rounded(nearest["distanceToPathMeters"]),
        "nearestGnssTimestamp": _rounded(nearest["nearestGnssTimestamp"], 3),
        "offsetSeconds": _rounded(nearest["offsetSeconds"], 2),
        "alongTrackMeters": _rounded(nearest["alongTrackMeters"]),
    }
    if props:
        record["properties"] = props

    speed_limit = props.get("speedLimit")
    if speed_limit is not None:
        record["speedLimit"] = speed_limit
        record["unit"] = props.get("unit") or "mph"

    return record


def _load_map_feature_cache(video_id: str) -> dict | None:
    cache_path = MAP_FEATURE_CACHE_DIR / f"{video_id}.json"
    if not cache_path.exists():
        return None
    try:
        data = json.loads(cache_path.read_text())
        if data.get("cacheVersion") == MAP_FEATURE_CACHE_VERSION:
            data.pop("cacheVersion", None)
            return data
    except Exception:
        return None
    return None


def _write_map_feature_cache(video_id: str, data: dict) -> None:
    MAP_FEATURE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = MAP_FEATURE_CACHE_DIR / f"{video_id}.json"
    cached = {"cacheVersion": MAP_FEATURE_CACHE_VERSION, **data}
    cache_path.write_text(json.dumps(cached, indent=2, default=str))


def _query_map_features_for_polygon(api_key: str, polygon: list[list[float]]) -> list[dict]:
    resp = api_request(
        "POST",
        MAP_DATA_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": _beemaps_auth_header(api_key),
        },
        json={
            "type": ["mapFeatures"],
            "geometry": {
                "type": "Polygon",
                "coordinates": [polygon],
            },
        },
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"map-data API returned {resp.status_code}: {resp.text[:300]}")
    data = resp.json()
    return data.get("mapFeatureResults", {}).get("data") or []


def build_map_features_metadata(
    api_key: str | None,
    video_id: str,
    event: dict | None,
    fallback_lat: float | None,
    fallback_lon: float | None,
) -> dict:
    """Fetch Bee Maps map features along a buffered GNSS corridor."""
    cached = _load_map_feature_cache(video_id)
    if cached is not None:
        return cached

    base_query: dict = {
        "radiusMeters": MAP_FEATURE_CORRIDOR_RADIUS_M,
        "source": "gnssData",
    }

    if not api_key:
        return {
            "status": "unavailable",
            "source": "beemaps.map-data",
            "query": base_query,
            "error": "BEEMAPS_API_KEY not found",
        }

    try:
        gnss_points = _valid_gnss_points((event or {}).get("gnssData"))
        method = "gnss_corridor"

        if len(gnss_points) >= 2:
            path, lat0, lon0 = _project_path(gnss_points)
            simplified = _simplify_path(path, MAP_FEATURE_SIMPLIFY_TOLERANCE_M)
            chunks = _split_path_chunks(simplified)
            polygons = [
                _corridor_polygon(chunk, lat0, lon0, MAP_FEATURE_CORRIDOR_RADIUS_M)
                for chunk in chunks
            ]
            query = {
                **base_query,
                "method": method,
                "sourceGnssPointCount": len(gnss_points),
                "simplifiedPointCount": len(simplified),
                "simplifyToleranceMeters": MAP_FEATURE_SIMPLIFY_TOLERANCE_M,
                "chunkCount": len(polygons),
                "chunkMaxLengthMeters": MAP_FEATURE_MAX_CHUNK_LENGTH_M,
                "chunkMaxPoints": MAP_FEATURE_MAX_CHUNK_POINTS,
                "polygonVertexCounts": [len(p) for p in polygons],
            }
        elif fallback_lat is not None and fallback_lon is not None:
            method = "event_location_fallback"
            fallback = [{"lat": fallback_lat, "lon": fallback_lon, "timestamp": None}]
            path, lat0, lon0 = _project_path(fallback)
            polygons = [_circle_polygon(fallback_lat, fallback_lon, MAP_FEATURE_CORRIDOR_RADIUS_M)]
            query = {
                **base_query,
                "method": method,
                "source": "event.location",
                "sourceGnssPointCount": len(gnss_points),
                "chunkCount": 1,
                "polygonVertexCounts": [len(polygons[0])],
            }
        else:
            result = {
                "status": "skipped",
                "source": "beemaps.map-data",
                "query": base_query,
                "reason": "No GNSS path or fallback event location available",
                "features": [],
                "speedLimits": [],
            }
            _write_map_feature_cache(video_id, result)
            return result

        raw_by_key: dict[str, dict] = {}
        errors: list[str] = []
        for idx, polygon in enumerate(polygons):
            try:
                for raw_feature in _query_map_features_for_polygon(api_key, polygon):
                    if isinstance(raw_feature, dict):
                        raw_by_key[_feature_fingerprint(raw_feature)] = raw_feature
            except Exception as exc:
                errors.append(f"chunk {idx + 1}: {str(exc)[:300]}")

        if not raw_by_key and errors:
            return {
                "status": "unavailable",
                "source": "beemaps.map-data",
                "query": query,
                "error": "; ".join(errors[:3]),
                "features": [],
                "speedLimits": [],
            }

        features: list[dict] = []
        for raw_feature in raw_by_key.values():
            record = _build_feature_record(raw_feature, path, lat0, lon0)
            if record:
                features.append(record)

        features.sort(key=lambda f: (
            f.get("alongTrackMeters") if f.get("alongTrackMeters") is not None else float("inf"),
            f.get("distanceToPathMeters") if f.get("distanceToPathMeters") is not None else float("inf"),
            f.get("class") or "",
        ))

        speed_limits = [
            {
                "value": feature["speedLimit"],
                "unit": feature.get("unit", "mph"),
                "position": feature["position"],
                "distanceToPathMeters": feature.get("distanceToPathMeters"),
                "nearestGnssTimestamp": feature.get("nearestGnssTimestamp"),
                "offsetSeconds": feature.get("offsetSeconds"),
                "alongTrackMeters": feature.get("alongTrackMeters"),
            }
            for feature in features
            if feature.get("class") == "speed-sign" and feature.get("speedLimit") is not None
        ]

        result = {
            "status": "partial" if errors else "completed",
            "source": "beemaps.map-data",
            "query": query,
            "features": features,
            "speedLimits": speed_limits,
        }
        if errors:
            result["errors"] = errors[:5]
        else:
            _write_map_feature_cache(video_id, result)
        return result
    except Exception as exc:
        return {
            "status": "unavailable",
            "source": "beemaps.map-data",
            "query": base_query,
            "error": str(exc)[:500],
            "features": [],
            "speedLimits": [],
        }


def compact_map_features_metadata(map_features: dict | None) -> dict | None:
    """Return production-safe map feature metadata, or None when no features exist."""
    if not isinstance(map_features, dict):
        return None

    features = [
        feature
        for feature in map_features.get("features") or []
        if isinstance(feature, dict)
    ]
    if not features:
        return None

    compact: dict = {"features": features}
    speed_limits = [
        speed_limit
        for speed_limit in map_features.get("speedLimits") or []
        if isinstance(speed_limit, dict)
    ]
    if speed_limits:
        compact["speedLimits"] = speed_limits

    return compact


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
        if is_metadata_vru_label(r[1])
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
        if is_metadata_vru_label(r[0])
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


def load_local_metadata_event(video_id: str) -> dict | None:
    """Load local edited/trimmed event metadata when the video id is a clip id."""
    if not LOCAL_EDITED_EVENT_ID_RE.fullmatch(video_id):
        return None

    metadata_path = OUTPUT_DIR / f"{video_id}.json"
    if not metadata_path.exists():
        return None

    with metadata_path.open("r", encoding="utf-8") as f:
        metadata = json.load(f)

    event = dict(metadata.get("event") or {})
    event["id"] = event.get("id") or metadata.get("id") or video_id
    event.setdefault("type", "UNKNOWN")
    event.setdefault("timestamp", "")
    event.setdefault("location", {"lat": 0, "lon": 0})
    event.setdefault("metadata", {})
    event.setdefault("videoUrl", f"http://localhost:3000/videos/{video_id}.mp4")
    if metadata.get("gnssData"):
        event["gnssData"] = metadata["gnssData"]
    if metadata.get("imuData"):
        event["imuData"] = metadata["imuData"]
    return event


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
            headers={"Authorization": _beemaps_auth_header(api_key), "Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def load_event(api_key: str | None, video_id: str) -> dict | None:
    """Load event from cache first, fall back to API."""
    local_event = load_local_metadata_event(video_id)
    if local_event:
        return local_event

    event = load_event_from_cache(video_id)
    if event:
        return event
    if api_key:
        return fetch_event_from_api(api_key, video_id)
    return None


# ---------------------------------------------------------------------------
# Build metadata
# ---------------------------------------------------------------------------

def normalize_event_metadata(value) -> dict:
    """Bee Maps may return metadata as either an object or a JSON string."""
    if isinstance(value, dict):
        keys = list(value.keys())
        if keys and all(isinstance(k, str) and k.isdigit() for k in keys):
            try:
                raw = "".join(str(value[str(i)]) for i in range(len(keys)))
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


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
            "metadata": normalize_event_metadata(event.get("metadata")),
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


def build_production_metadata(
    conn,
    api_key: str,
    video_id: str,
    video_url_override: str | None = None,
    video_path: str | Path | None = None,
    allow_low_pts_spread: bool = False,
) -> dict:
    """Build production metadata JSON — restructured for delivery.

    Differences from build_metadata:
    - event block includes speedMin/Max/Mean/Stddev, roadClass, timeOfDay, country
    - No triage block (triageResult, displacement, rulesTriggered removed)
    """
    meta: dict = {"id": video_id}
    delivery_video_url = (
        validate_delivery_video_url(video_url_override)
        if video_url_override
        else production_video_url(video_id)
    )

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
        event_metadata = normalize_event_metadata(event.get("metadata"))
        event_metadata["camera"] = "bee"
        event_block: dict = {
            "type": event.get("type"),
            "timestamp": event.get("timestamp"),
            "location": event.get("location"),
            "videoUrl": delivery_video_url,
            "metadata": event_metadata,
        }
    else:
        event_block = {
            "type": triage.get("eventType") if triage else None,
            "timestamp": triage.get("eventTimestamp") if triage else None,
            "location": {"lat": lat, "lon": lon} if lat and lon else None,
            "videoUrl": delivery_video_url,
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

    # 2. Delivered video technical metadata and per-frame display timestamps.
    video_payload = build_video_metadata_payload(
        video_id,
        event,
        delivery_video_url,
        video_path,
        allow_low_pts_spread=allow_low_pts_spread,
    )
    meta["video"] = video_payload["video"]
    meta["pts_us"] = video_payload["pts_us"]

    # 3. Map features along the driven path (speed-limit signs, signals, etc.)
    map_features = compact_map_features_metadata(
        build_map_features_metadata(api_key, video_id, event, lat, lon)
    )
    if map_features:
        meta["mapFeatures"] = map_features

    # 4. Detection run (internal — not included in output, but needed for run_id)
    det_run = get_detection_run(conn, video_id)
    run_id = det_run["runId"] if det_run else None

    # 5. Frame detections + segments
    meta["frameDetections"] = get_frame_detections(conn, video_id, run_id)
    meta["detectionSegments"] = [
        {k: v for k, v in seg.items() if k != "source"}
        for seg in get_detection_segments(conn, video_id, run_id)
    ]

    # 6. VRU labels detected
    meta["vruLabelsDetected"] = sorted(set(d["label"] for d in meta["detectionSegments"]))

    # 7. Summary — generate early so it appears near top of output
    meta["summary"] = get_clip_summary(conn, video_id) or generate_summary(meta)

    # Reorder: put summary right after id, before event
    ordered: dict = {"id": meta["id"], "summary": meta["summary"]}
    for k, v in meta.items():
        if k not in ("id", "summary"):
            ordered[k] = v

    # 8. Export timestamp
    ordered["exportedAt"] = datetime.now(timezone.utc).isoformat()

    # 9. GNSS + IMU (last — large arrays)
    if event and event.get("gnssData"):
        ordered["gnssData"] = event["gnssData"]
    else:
        ordered["gnssData"] = []
    if event and event.get("imuData"):
        ordered["imuData"] = event["imuData"]
    else:
        ordered["imuData"] = []

    return ordered


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
    parser.add_argument("--video-url", help="Override event.videoUrl when exporting production metadata")
    parser.add_argument("--video-path", help="Local delivered MP4 path to probe for production video metadata")
    parser.add_argument(
        "--allow-low-pts-spread",
        action="store_true",
        help="Allow production metadata export when PTS delta spread is below the CFR guard",
    )
    args = parser.parse_args()

    if args.video_url and not args.production:
        parser.error("--video-url only applies with --production")
    if args.video_url and not args.event_id:
        parser.error("--video-url requires --event-id")
    if args.video_path and not args.production:
        parser.error("--video-path only applies with --production")
    if args.video_path and not args.event_id:
        parser.error("--video-path requires --event-id")
    if args.allow_low_pts_spread and not args.production:
        parser.error("--allow-low-pts-spread only applies with --production")
    if args.allow_low_pts_spread and not args.event_id:
        parser.error("--allow-low-pts-spread requires --event-id")

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
        out_path = OUTPUT_DIR / production_metadata_key(vid) if args.production else OUTPUT_DIR / f"{vid}.json"

        if out_path.exists() and not args.overwrite:
            skipped += 1
            continue

        try:
            if args.production:
                meta = build_production_metadata(
                    conn,
                    api_key,
                    vid,
                    args.video_url,
                    args.video_path,
                    allow_low_pts_spread=args.allow_low_pts_spread,
                )
            else:
                meta = build_metadata(conn, api_key, vid)
            out_path.parent.mkdir(parents=True, exist_ok=True)
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
