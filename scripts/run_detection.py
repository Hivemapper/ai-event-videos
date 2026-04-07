#!/usr/bin/env python3
"""
Unified detection runner — spawned by the Next.js API.

Accepts --run-id, reads run config from the DB, downloads the video,
extracts frames, runs the appropriate model, saves detections, and
updates the run status throughout.

Supported models:
  - gdino-base-clip  (Grounding DINO base + OpenCLIP verification)
  - mm-gdino         (MM-Grounding-DINO large, V3Det-trained)
  - yolo-world       (YOLO-World v2 open vocabulary)
  - yolo11x          (YOLO11x standard COCO-80)
  - yolo26x          (YOLO26x standard COCO-80)

Usage:
    cd /Users/tylerlu/Projects/ai-event-videos
    source .venv/bin/activate
    python scripts/run_detection.py --run-id <UUID>
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import sqlite3
import sys
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import cv2
import numpy as np
import requests
import torch
from PIL import Image

try:
    import av as _av
    HAS_PYAV = True
except ImportError:
    HAS_PYAV = False

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "labels.db"
CACHE_DIR = PROJECT_ROOT / "data" / "pipeline-video-cache"


def api_request(method: str, url: str, **kwargs) -> requests.Response:
    """Make an HTTP request with 403 backoff retry."""
    kwargs.setdefault("timeout", 60)
    wait = 30
    for attempt in range(6):  # up to ~5 min total backoff
        resp = requests.request(method, url, **kwargs)
        if resp.status_code != 403:
            return resp
        print(f"    [!] 403 rate-limited — waiting {wait}s (attempt {attempt + 1})...")
        time.sleep(wait)
        wait = min(wait * 2, 120)
    return resp  # return last 403 if all retries exhausted
PIPELINE_VERSION = "vru-yolo-v2"
FRAMES_PER_VIDEO = 45

API_BASE_URL = "https://beemaps.com/api/developer/aievents"
BEEMAPS_MAP_DATA_URL = "https://beemaps.com/api/developer/map-data"





# ---------------------------------------------------------------------------
# GDINO configuration (copied from run_gdino_clip_pipeline.py)
# ---------------------------------------------------------------------------

GDINO_TEXT_PROMPT = (
    "person. bicycle. motorcycle. wheelchair. stroller. "
    "crosswalk. dog. electric kick scooter."
)
GDINO_BOX_THRESHOLD = 0.30
GDINO_TEXT_THRESHOLD = 0.25

# CLIP verification config
CLIP_MODEL_NAME = "ViT-B-32"
CLIP_PRETRAINED = "datacomp_xl_s13b_b90k"
CLIP_MIN_SIMILARITY = 0.26  # below this, drop the detection entirely

# Ambiguous classes and their CLIP candidate labels.
# For each ambiguous GDINO label, we run CLIP with these candidates.
# The first candidate is the "agreeing" label — if CLIP picks it, GDINO's
# label stands. Otherwise, the detection is relabeled to the CLIP winner.
AMBIGUOUS_CLASSES: dict[str, list[str]] = {
    "stroller": [
        "baby stroller",
        "shopping cart",
        "wheelchair",
    ],
    "electric kick scooter": [
        "electric kick scooter",
        "bicycle",
        "skateboard",
    ],
}

# Map from CLIP winning label back to a canonical GDINO-style label for the DB
CLIP_LABEL_REMAP: dict[str, str | None] = {
    "baby stroller": "stroller",
    "shopping cart": None,  # drop — not a VRU
    "wheelchair": "wheelchair",
    "electric kick scooter": "scooter",
    "bicycle": "bicycle",
    "skateboard": None,  # drop
}

# Non-ambiguous classes — pass through without CLIP verification
NON_AMBIGUOUS_CLASSES = {
    "person", "bicycle", "motorcycle", "wheelchair",
    "crosswalk", "dog", "scooter",
}

# Animal detection filtering — only dog is in the detection prompt
ANIMAL_LABELS = {"dog"}
ANIMAL_MIN_CONFIDENCE = 0.55
ANIMAL_MIN_FRAMES = 2
ANIMAL_FRAME_GAP_MS = 2000

# All known valid labels (non-ambiguous + ambiguous)
ALL_KNOWN_LABELS = NON_AMBIGUOUS_CLASSES | set(AMBIGUOUS_CLASSES.keys())

# ---------------------------------------------------------------------------
# YOLO-World configuration (copied from run_yolo_world_single.py)
# ---------------------------------------------------------------------------

VRU_CLASSES = [
    "person", "bicycle", "motorcycle", "scooter",
    "wheelchair", "stroller", "person wearing safety vest",
    "skateboard", "dog", "traffic cone",
    "car", "truck", "bus",
]

YOLO_WORLD_CONF = 0.15

# ---------------------------------------------------------------------------
# YOLO11x COCO label map (copied from test_open_vocab.py / vru_pipeline_worker.py)
# ---------------------------------------------------------------------------

COCO_LABEL_MAP: dict[str, str] = {
    "person": "person",
    "bicycle": "bicycle",
    "motorcycle": "motorcycle",
    "car": "car",
    "truck": "truck",
    "bus": "bus",
    "train": "train",
    "stop sign": "stop sign",
    "cat": "cat",
    "dog": "dog",
    "skateboard": "skateboard",
}

YOLO11X_CONF = 0.25


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_env_var(name: str) -> str | None:
    """Load env var from environment or .env.local."""
    val = os.environ.get(name)
    if val:
        return val
    env_path = PROJECT_ROOT / ".env.local"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            if k.strip() == name:
                return v.strip()
    return None


def load_api_key() -> str:
    key = _load_env_var("BEEMAPS_API_KEY")
    if key:
        return key if key.startswith("Basic ") else f"Basic {key}"
    raise RuntimeError("BEEMAPS_API_KEY not found in env or .env.local")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_device() -> str:
    if torch.cuda.is_available():
        print(f"  [GPU] CUDA is available ({torch.cuda.get_device_name(0)})")
        return "cuda"
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        print(f"  [GPU] MPS (Apple Silicon GPU) is available")
        return "mps"
    print(f"  [GPU] No GPU available — using CPU")
    return "cpu"


def free_gpu():
    """Force-free MPS/CUDA memory."""
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    if hasattr(torch, "mps") and hasattr(torch.mps, "empty_cache"):
        torch.mps.empty_cache()


def download_video(video_url: str) -> Path | None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    hashed = hashlib.md5(video_url.encode("utf-8")).hexdigest()
    path = CACHE_DIR / f"{hashed}.mp4"
    if path.exists():
        path.touch()
        return path
    try:
        with api_request("GET", video_url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return path
    except Exception as exc:
        print(f"    [!] Download failed: {exc}")
        if path.exists():
            path.unlink(missing_ok=True)
        return None


def _extract_frames_pyav(video_path: Path, num_frames: int) -> list[tuple[int, Any, int]]:
    """Extract frames using PyAV (faster seeking than OpenCV)."""
    container = _av.open(str(video_path))
    stream = container.streams.video[0]
    total_frames = stream.frames or 0
    fps = float(stream.average_rate or 30)

    # Fallback: estimate total frames from duration
    if total_frames <= 0 and stream.duration and stream.time_base:
        total_frames = int(float(stream.duration * stream.time_base) * fps)
    if total_frames <= 0:
        container.close()
        return []

    margin = max(1, total_frames // 20)
    usable = total_frames - 2 * margin
    if usable <= 0 or num_frames == 1:
        indices = [total_frames // 2]
    else:
        step = usable / (num_frames - 1)
        indices = [margin + int(i * step) for i in range(num_frames)]

    time_base = float(stream.time_base)
    frames = []
    for idx in indices:
        t_sec = idx / fps
        target_ts = int(t_sec / time_base)
        container.seek(target_ts, stream=stream, backward=True, any_frame=False)
        for frame in container.decode(video=0):
            bgr = frame.to_ndarray(format="bgr24")
            frame_ms = int(round(t_sec * 1000))
            frames.append((frame_ms, bgr, idx))
            break

    container.close()
    return frames


def _extract_frames_cv2(video_path: Path, num_frames: int) -> list[tuple[int, Any, int]]:
    """Fallback: extract frames using OpenCV."""
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return []
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    if total_frames <= 0:
        cap.release()
        return []
    margin = max(1, total_frames // 20)
    usable = total_frames - 2 * margin
    if usable <= 0 or num_frames == 1:
        indices = [total_frames // 2]
    else:
        step = usable / (num_frames - 1)
        indices = [margin + int(i * step) for i in range(num_frames)]
    frames = []
    for idx in indices:
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok, frame = cap.read()
        if ok:
            frame_ms = int(round((idx / fps) * 1000))
            frames.append((frame_ms, frame, idx))
    cap.release()
    return frames


def extract_frames(video_path: Path, num_frames: int) -> list[tuple[int, Any, int]]:
    """Extract frames — uses PyAV if available, falls back to OpenCV."""
    if HAS_PYAV:
        try:
            t0 = time.time()
            frames = _extract_frames_pyav(video_path, num_frames)
            print(f"    [PyAV] Extracted {len(frames)} frames in {time.time()-t0:.1f}s")
            return frames
        except Exception as exc:
            print(f"    [!] PyAV failed ({exc}), falling back to OpenCV")
    t0 = time.time()
    frames = _extract_frames_cv2(video_path, num_frames)
    print(f"    [OpenCV] Extracted {len(frames)} frames in {time.time()-t0:.1f}s")
    return frames


# ---------------------------------------------------------------------------
# Database — uses Turso if env vars are set, otherwise local SQLite
# ---------------------------------------------------------------------------

_is_turso = False


class TursoCursor:
    """Mimics sqlite3.Cursor for libsql_client results."""

    def __init__(self, result_set):
        self._rs = result_set
        self._rows = result_set.rows if result_set else []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class TursoDb:
    """HTTP fallback wrapper around libsql_client that provides a sqlite3-like interface."""

    def __init__(self, client):
        self._client = client

    def execute(self, sql, params=None):
        import libsql_client
        if params:
            stmt = libsql_client.Statement(sql, list(params))
            rs = self._client.execute(stmt)
        else:
            rs = self._client.execute(sql)
        return TursoCursor(rs)

    def executemany(self, sql, rows):
        self.batch_insert(sql, list(rows))

    def executescript(self, sql):
        import libsql_client
        stmts = [s.strip() for s in sql.split(';') if s.strip()]
        if stmts:
            self._client.batch([libsql_client.Statement(s) for s in stmts])

    def batch_insert(self, sql, rows):
        import libsql_client
        stmts = [libsql_client.Statement(sql, list(row)) for row in rows]
        CHUNK = 500
        for i in range(0, len(stmts), CHUNK):
            self._client.batch(stmts[i:i + CHUNK])

    def commit(self):
        pass  # libsql_client auto-commits

    def sync(self):
        pass  # No-op for HTTP mode

    def close(self):
        self._client.close()


def get_db():
    """Connect to Turso via embedded replica, HTTP fallback, or local SQLite."""
    global _is_turso
    turso_url = _load_env_var("TURSO_DATABASE_URL")
    turso_token = _load_env_var("TURSO_AUTH_TOKEN")

    if turso_url and turso_token:
        # Try embedded replica first (better performance, local caching)
        try:
            import libsql_experimental as libsql  # type: ignore
            conn = libsql.connect(
                str(DB_PATH),  # local replica file
                sync_url=turso_url,
                auth_token=turso_token,
            )
            conn.sync()
            _is_turso = True
            print(f"  DB: Turso embedded replica ({turso_url.split('//')[1].split('.')[0]})")
        except Exception:
            # Fall back to HTTP mode via libsql_client
            try:
                import libsql_client
                http_url = turso_url.replace("libsql://", "https://")
                client = libsql_client.create_client_sync(url=http_url, auth_token=turso_token)
                conn = TursoDb(client)
                _is_turso = True
                print(f"  DB: Turso HTTP ({turso_url.split('//')[1].split('.')[0]})")
            except ImportError:
                print("  [!] Neither libsql_experimental nor libsql_client installed, falling back to local SQLite")
                print("      Install with: pip install libsql-client")
                conn = sqlite3.connect(str(DB_PATH))
    else:
        conn = sqlite3.connect(str(DB_PATH))
        print("  DB: Using local SQLite")

    if not _is_turso:
        conn.execute("PRAGMA journal_mode=WAL")

    # Ensure columns exist on frame_detections
    if not _is_turso:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(frame_detections)").fetchall()]
        if "model_name" not in cols:
            conn.execute(
                "ALTER TABLE frame_detections ADD COLUMN model_name TEXT NOT NULL DEFAULT 'yolo11x'"
            )
            conn.commit()
        if "run_id" not in cols:
            conn.execute("ALTER TABLE frame_detections ADD COLUMN run_id TEXT")
            conn.commit()

    return conn


def commit_and_sync(conn) -> None:
    """Commit changes to local DB and sync to Turso if connected."""
    conn.commit()
    if _is_turso and hasattr(conn, "sync"):
        conn.sync()


# ---------------------------------------------------------------------------
# Detection run DB helpers
# ---------------------------------------------------------------------------

def get_detection_run(conn, run_id: str) -> dict | None:
    """Read a detection_runs row and return as dict, or None if not found."""
    row = conn.execute(
        "SELECT id, video_id, model_name, status, config_json, detection_count, "
        "worker_pid, started_at, completed_at, last_heartbeat_at, last_error, created_at "
        "FROM detection_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "id": row[0],
        "video_id": row[1],
        "model_name": row[2],
        "status": row[3],
        "config_json": row[4],
        "detection_count": row[5],
        "worker_pid": row[6],
        "started_at": row[7],
        "completed_at": row[8],
        "last_heartbeat_at": row[9],
        "last_error": row[10],
        "created_at": row[11],
    }


def update_run_status(
    conn,
    run_id: str,
    status: str,
    *,
    detection_count: int | None = None,
    last_error: str | None = None,
) -> None:
    """Update detection_runs row status and optional fields."""
    fields = ["status = ?", "last_heartbeat_at = ?"]
    params: list[Any] = [status, utc_now()]

    if status == "running":
        fields.append("started_at = CASE WHEN started_at IS NULL THEN ? ELSE started_at END")
        params.append(utc_now())

    if status in ("completed", "failed"):
        fields.append("completed_at = ?")
        params.append(utc_now())

    if detection_count is not None:
        fields.append("detection_count = ?")
        params.append(detection_count)

    if last_error is not None:
        fields.append("last_error = ?")
        params.append(last_error)

    params.append(run_id)
    conn.execute(
        f"UPDATE detection_runs SET {', '.join(fields)} WHERE id = ?",
        tuple(params),
    )
    commit_and_sync(conn)


def update_heartbeat(conn, run_id: str, *, sync: bool = True) -> None:
    """Update last_heartbeat_at for the run."""
    conn.execute(
        "UPDATE detection_runs SET last_heartbeat_at = ? WHERE id = ?",
        (utc_now(), run_id),
    )
    if sync:
        commit_and_sync(conn)
    else:
        conn.commit()


# ---------------------------------------------------------------------------
# Save detections (with run_id)
# ---------------------------------------------------------------------------

def save_detections(
    conn,
    video_id: str,
    frame_ms: int,
    detections: list[dict],
    model_name: str,
    run_id: str,
) -> int:
    sql = """INSERT INTO frame_detections (
            video_id, frame_ms, label, x_min, y_min, x_max, y_max,
            confidence, frame_width, frame_height, pipeline_version, model_name, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    rows = [
        (
            video_id, frame_ms, d["label"],
            d["x_min"], d["y_min"], d["x_max"], d["y_max"],
            d["confidence"], d["frame_width"], d["frame_height"],
            PIPELINE_VERSION, model_name, run_id,
        )
        for d in detections
    ]
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def ensure_video_pipeline_state(conn, video_id: str, day: str, model_name: str) -> None:
    existing = conn.execute(
        "SELECT 1 FROM video_pipeline_state WHERE video_id = ?", (video_id,)
    ).fetchone()
    if not existing:
        conn.execute(
            """INSERT INTO video_pipeline_state (
                video_id, day, status, pipeline_version, model_name, labels_applied, completed_at
            ) VALUES (?, ?, 'processed', ?, ?, '[]', datetime('now'))""",
            (video_id, day, PIPELINE_VERSION, model_name),
        )
        commit_and_sync(conn)


# ---------------------------------------------------------------------------
# GDINO label normalization (copied from run_gdino_clip_pipeline.py)
# ---------------------------------------------------------------------------

def normalize_gdino_label(raw_label: str) -> str | None:
    """Normalize GDINO's raw label output.

    Returns a canonical label string, or None if the label is garbage
    (BPE artifacts like '##clist', compound labels like 'truck bus', etc.).
    """
    label = raw_label.strip().lower()

    # Drop BPE tokenizer artifacts
    if "##" in label:
        return None

    # Exact match to a known label
    if label in ALL_KNOWN_LABELS:
        return label

    # Check for compound labels (e.g., "truck bus", "car bus", "person construction worker")
    # If a known label is an exact substring, use it — but only for unambiguous single-word matches
    # to avoid "person construction worker" -> "person" when it should be "construction worker"
    for known in sorted(ALL_KNOWN_LABELS, key=len, reverse=True):
        if known in label and " " in known:
            # Multi-word match (e.g., "construction worker" in "person construction worker")
            return known

    # Single-word known label as exact token
    tokens = label.split()
    for token in tokens:
        if token in NON_AMBIGUOUS_CLASSES:
            return token

    # Unrecognized — drop it
    return None


# ---------------------------------------------------------------------------
# Ego-vehicle filter — remove detections of the car's own hood/dashboard/pillars
# ---------------------------------------------------------------------------

EGO_VEHICLE_LABELS = {"car", "truck", "bus"}
EGO_BOTTOM_MARGIN = 0.08   # y_max must be within bottom 8% of frame
EGO_MIN_WIDTH_RATIO = 0.50 # box must span at least 50% of frame width
EGO_MIN_Y_RATIO = 0.55     # box top (y_min) must be in bottom 45% of frame


def filter_ego_vehicle(detections: list[dict]) -> tuple[list[dict], int]:
    """Remove detections that are likely the ego vehicle's hood/dashboard.

    Two heuristics:
    1) Wide box anchored to bottom (hood/dashboard visible at bottom of frame)
    2) Box covers most of the frame (GDINO detects the whole scene as 'car'
       when hood/windshield frame is visible)
    """
    kept = []
    dropped = 0
    for d in detections:
        if d["label"] in EGO_VEHICLE_LABELS:
            w = d["frame_width"]
            h = d["frame_height"]
            box_width = d["x_max"] - d["x_min"]
            box_height = d["y_max"] - d["y_min"]

            # Heuristic 1: wide box anchored to bottom
            bottom_near_edge = d["y_max"] >= h * (1 - EGO_BOTTOM_MARGIN)
            wide_enough = box_width >= w * EGO_MIN_WIDTH_RATIO
            low_enough = d["y_min"] >= h * EGO_MIN_Y_RATIO
            if bottom_near_edge and wide_enough and low_enough:
                dropped += 1
                continue

            # Heuristic 2: box covers most of the frame (>80% width AND >80% height)
            if box_width >= w * 0.80 and box_height >= h * 0.80:
                dropped += 1
                continue

        kept.append(d)
    return kept, dropped


# ---------------------------------------------------------------------------
# Near-collision vehicle filter — keep only vehicles involved in near-miss
# ---------------------------------------------------------------------------

import math

VEHICLE_LABELS = {"car", "truck", "bus"}
VEHICLE_HEIGHTS: dict[str, float] = {"car": 1.5, "truck": 3.0, "bus": 3.2}
BEE_HFOV_DEG = 142.0
VEHICLE_CLOSE_DISTANCE_M = 8.0       # Criterion A/C: close distance
VEHICLE_VERY_CLOSE_M = 4.0           # Criterion D: extremely close
VEHICLE_GROWTH_RATE_MIN = 1.8        # Criterion B: min bbox area growth
VEHICLE_LARGE_AREA_FRAC = 0.04       # Criterion C: min area / frame area
VEHICLE_CENTER_BAND = (0.30, 0.70)   # Criterion C: horizontal center range
VEHICLE_MATCH_MAX_PX = 250           # tracking: max center distance
DECEL_WINDOW_MS = 3000               # ±3s around peak deceleration


def _estimate_vehicle_distance(det: dict) -> float:
    """Estimate distance to a detected vehicle using pinhole camera model."""
    known_height = VEHICLE_HEIGHTS.get(det["label"], 1.5)
    half_fov_rad = math.radians(BEE_HFOV_DEG / 2)
    focal_px = (det["frame_width"] / 2) / math.tan(half_fov_rad)
    bbox_h = det["y_max"] - det["y_min"]
    if bbox_h < 1:
        return 999.0
    distance = (known_height * focal_px) / bbox_h
    # Barrel distortion correction for edge objects
    center_x = det["frame_width"] / 2
    cx = (det["x_min"] + det["x_max"]) / 2
    offset_frac = abs(cx - center_x) / center_x if center_x > 0 else 0
    if offset_frac > 0.75:
        distance *= 0.77
    return max(1.0, distance)


def _extract_decel_window(event: dict) -> tuple[int, int] | None:
    """Find the time window around peak deceleration from SPEED_ARRAY.

    Returns (start_ms, end_ms) relative to video start, or None if no speed data.
    """
    speeds = event.get("metadata", {}).get("SPEED_ARRAY", [])
    if len(speeds) < 2:
        return None

    # Find peak deceleration (largest speed drop between consecutive entries)
    best_decel = 0.0
    best_ts = None
    event_start_ms = speeds[0].get("TIMESTAMP", 0)

    for i in range(1, len(speeds)):
        prev_speed = speeds[i - 1].get("AVG_SPEED_MS", 0)
        curr_speed = speeds[i].get("AVG_SPEED_MS", 0)
        decel = prev_speed - curr_speed  # positive = decelerating
        if decel > best_decel:
            best_decel = decel
            best_ts = speeds[i].get("TIMESTAMP", 0)

    if best_ts is None or best_decel <= 0:
        return None

    # Convert to video-relative ms
    peak_ms = best_ts - event_start_ms
    return (max(0, peak_ms - DECEL_WINDOW_MS), peak_ms + DECEL_WINDOW_MS)


def _build_vehicle_tracks(vehicle_dets: list[dict]) -> list[list[int]]:
    """Group vehicle detections into tracks across frames.

    Returns list of tracks, each track is a list of indices into vehicle_dets.
    """
    # Group by frame_ms
    frames: dict[int, list[int]] = {}
    for i, d in enumerate(vehicle_dets):
        frames.setdefault(d["frame_ms"], []).append(i)

    sorted_timestamps = sorted(frames.keys())
    open_tracks: list[list[int]] = []
    all_tracks: list[list[int]] = []

    for ts in sorted_timestamps:
        frame_indices = frames[ts]
        matched_tracks: set[int] = set()
        matched_dets: set[int] = set()

        # Build cost pairs
        costs: list[tuple[int, int, float]] = []
        for ti, track in enumerate(open_tracks):
            last_d = vehicle_dets[track[-1]]
            last_cx = (last_d["x_min"] + last_d["x_max"]) / 2
            last_cy = (last_d["y_min"] + last_d["y_max"]) / 2
            for di_pos, orig_idx in enumerate(frame_indices):
                d = vehicle_dets[orig_idx]
                if d["label"] != last_d["label"]:
                    continue
                cx = (d["x_min"] + d["x_max"]) / 2
                cy = (d["y_min"] + d["y_max"]) / 2
                dist = math.sqrt((cx - last_cx) ** 2 + (cy - last_cy) ** 2)
                if dist <= VEHICLE_MATCH_MAX_PX:
                    costs.append((ti, di_pos, dist))

        costs.sort(key=lambda x: x[2])
        for ti, di_pos, _ in costs:
            if ti in matched_tracks or di_pos in matched_dets:
                continue
            matched_tracks.add(ti)
            matched_dets.add(di_pos)
            open_tracks[ti].append(frame_indices[di_pos])

        # Close unmatched tracks
        for ti in range(len(open_tracks) - 1, -1, -1):
            if ti not in matched_tracks:
                all_tracks.append(open_tracks.pop(ti))

        # Open new tracks for unmatched detections
        for di_pos, orig_idx in enumerate(frame_indices):
            if di_pos not in matched_dets:
                open_tracks.append([orig_idx])

    all_tracks.extend(open_tracks)
    return all_tracks


def filter_non_collision_vehicles(
    detections: list[dict], event: dict
) -> tuple[list[dict], int]:
    """Remove car/truck/bus detections unless they indicate a near-collision.

    A vehicle detection is kept if ANY of these criteria is met:
      A) Close (< 8m) AND within the deceleration window
      B) Track bbox area grows 1.8x+ (vehicle approaching fast)
      C) Close (< 8m) AND large (>= 4% of frame) AND centered
      D) Very close (< 4m) regardless of other factors

    Returns (filtered_detections, num_vehicles_removed).
    """
    vehicle_dets = [(i, d) for i, d in enumerate(detections) if d["label"] in VEHICLE_LABELS]
    if not vehicle_dets:
        return detections, 0

    # Pre-compute deceleration window from speed data
    decel_window = _extract_decel_window(event)

    # Indices of original detections to keep
    keep_vehicle_indices: set[int] = set()

    # Extract just the vehicle dets for tracking
    veh_only = [d for _, d in vehicle_dets]
    veh_orig_indices = [i for i, _ in vehicle_dets]

    # --- Single-frame criteria (A, C, D) ---
    for vi, (orig_idx, d) in enumerate(vehicle_dets):
        dist = _estimate_vehicle_distance(d)
        fw, fh = d["frame_width"], d["frame_height"]
        bbox_area = (d["x_max"] - d["x_min"]) * (d["y_max"] - d["y_min"])
        frame_area = fw * fh
        cx_frac = ((d["x_min"] + d["x_max"]) / 2) / fw if fw > 0 else 0.5

        # D: Very close — always keep
        if dist < VEHICLE_VERY_CLOSE_M:
            keep_vehicle_indices.add(orig_idx)
            continue

        # A: Close + within deceleration window
        if dist < VEHICLE_CLOSE_DISTANCE_M and decel_window is not None:
            frame_ms = d["frame_ms"]
            if decel_window[0] <= frame_ms <= decel_window[1]:
                keep_vehicle_indices.add(orig_idx)
                continue

        # C: Close + large + centered
        if (dist < VEHICLE_CLOSE_DISTANCE_M
                and frame_area > 0
                and bbox_area / frame_area >= VEHICLE_LARGE_AREA_FRAC
                and VEHICLE_CENTER_BAND[0] <= cx_frac <= VEHICLE_CENTER_BAND[1]):
            keep_vehicle_indices.add(orig_idx)

    # --- Cross-frame criterion (B): rapid approach ---
    tracks = _build_vehicle_tracks(veh_only)
    for track_local_indices in tracks:
        if len(track_local_indices) < 2:
            continue
        areas = [
            (veh_only[li]["x_max"] - veh_only[li]["x_min"])
            * (veh_only[li]["y_max"] - veh_only[li]["y_min"])
            for li in track_local_indices
        ]
        min_area = max(min(areas), 1.0)
        growth = max(areas) / min_area
        if growth >= VEHICLE_GROWTH_RATE_MIN:
            for li in track_local_indices:
                keep_vehicle_indices.add(veh_orig_indices[li])

    # Build result: non-vehicle detections + kept vehicles
    vehicle_index_set = {i for i, _ in vehicle_dets}
    result = []
    for i, d in enumerate(detections):
        if i in vehicle_index_set:
            if i in keep_vehicle_indices:
                result.append(d)
        else:
            result.append(d)

    removed = len(vehicle_dets) - len(keep_vehicle_indices)
    return result, removed


# ---------------------------------------------------------------------------
# VRU near-miss scoring — detect if driver almost hit a person/cyclist/etc.
# ---------------------------------------------------------------------------

VRU_LABELS_FOR_NEARMISS = {"person", "cyclist", "motorcyclist", "scooter", "bicycle", "dog"}
VRU_KNOWN_HEIGHTS: dict[str, float] = {
    "person": 1.7, "cyclist": 1.8, "motorcyclist": 1.6,
    "scooter": 1.7, "bicycle": 1.2, "dog": 0.6,
}
VRU_TYPE_WEIGHTS: dict[str, float] = {
    "person": 1.0, "cyclist": 0.9, "motorcyclist": 0.7,
    "scooter": 0.8, "bicycle": 0.5, "dog": 0.6,
}
NEARMISS_VERY_CLOSE_M = 5.0
NEARMISS_CLOSE_M = 10.0
NEARMISS_MEDIUM_M = 20.0


def _estimate_vru_distance(det: dict) -> float:
    """Estimate distance to a VRU using pinhole camera model."""
    label = det["label"]
    known_height = VRU_KNOWN_HEIGHTS.get(label, 1.7)
    half_fov_rad = math.radians(BEE_HFOV_DEG / 2)
    focal_px = (det["frame_width"] / 2) / math.tan(half_fov_rad)
    bbox_h = det["y_max"] - det["y_min"]
    if bbox_h < 1:
        return 999.0
    distance = (known_height * focal_px) / bbox_h
    center_x = det["frame_width"] / 2
    cx = (det["x_min"] + det["x_max"]) / 2
    offset_frac = abs(cx - center_x) / center_x if center_x > 0 else 0
    if offset_frac > 0.75:
        distance *= 0.77
    return max(1.0, distance)


def compute_nearmiss_score(
    detections: list[dict], event: dict
) -> tuple[float, dict]:
    """Compute VRU near-miss score from detections + event speed data.

    Signals:
      - VRU proximity (pinhole distance)
      - VRU bbox growing across frames (approaching)
      - Coincides with deceleration window
      - VRU centered in driver's path

    Returns (score 0-1, details_dict).
    """
    vru_dets = [d for d in detections if d["label"] in VRU_LABELS_FOR_NEARMISS]
    if not vru_dets:
        return 0.0, {"reason": "no VRU detections"}

    decel_window = _extract_decel_window(event)

    # Extract speed info
    speeds = event.get("metadata", {}).get("SPEED_ARRAY", [])
    max_mph = 0.0
    if speeds:
        max_mph = max(s.get("AVG_SPEED_MS", 0) for s in speeds) * 2.237

    # Track VRUs across frames
    tracks = _build_vehicle_tracks(vru_dets)  # reuse existing tracker

    best_score = 0.0
    best_details: dict = {}

    for track_indices in tracks:
        if not track_indices:
            continue

        track_dets = [vru_dets[i] for i in track_indices]
        label = track_dets[0]["label"]

        # Compute min distance across track
        distances = [_estimate_vru_distance(d) for d in track_dets]
        min_distance = min(distances)

        # Proximity score
        if min_distance < NEARMISS_VERY_CLOSE_M:
            proximity_mult = 2.0
            proximity_label = "very_close"
        elif min_distance < NEARMISS_CLOSE_M:
            proximity_mult = 1.5
            proximity_label = "close"
        elif min_distance < NEARMISS_MEDIUM_M:
            proximity_mult = 1.0
            proximity_label = "medium"
        else:
            proximity_mult = 0.3
            proximity_label = "far"

        # Growth rate (approaching?)
        areas = [
            (d["x_max"] - d["x_min"]) * (d["y_max"] - d["y_min"])
            for d in track_dets
        ]
        growth = max(areas) / max(min(areas), 1.0) if len(areas) >= 2 else 1.0
        is_approaching = growth >= 1.5

        # Centered in frame?
        cx_fracs = [
            ((d["x_min"] + d["x_max"]) / 2) / d["frame_width"]
            for d in track_dets
        ]
        is_centered = any(0.25 <= cx <= 0.75 for cx in cx_fracs)

        # During deceleration?
        during_decel = False
        if decel_window:
            for d in track_dets:
                if decel_window[0] <= d["frame_ms"] <= decel_window[1]:
                    during_decel = True
                    break

        # Confidence
        max_conf = max(d["confidence"] for d in track_dets)

        # Composite score for this track
        type_weight = VRU_TYPE_WEIGHTS.get(label, 0.5)
        motion_mult = 1.5 if is_approaching else 0.7
        decel_mult = 1.5 if during_decel else 0.8
        center_mult = 1.3 if is_centered else 0.8

        raw_score = type_weight * proximity_mult * motion_mult * decel_mult * center_mult * max_conf

        # Speed factor — faster = more dangerous
        speed_factor = min(2.0, max(0.5, max_mph / 25.0))
        track_score = min(1.0, raw_score * speed_factor / 4.0)

        if track_score > best_score:
            best_score = track_score
            best_details = {
                "label": label,
                "min_distance_m": round(min_distance, 1),
                "proximity": proximity_label,
                "approaching": is_approaching,
                "growth_rate": round(growth, 2),
                "centered": is_centered,
                "during_decel": during_decel,
                "max_confidence": round(max_conf, 3),
                "speed_mph": round(max_mph, 1),
                "track_frames": len(track_dets),
            }

    return round(best_score, 3), best_details


# ---------------------------------------------------------------------------
# Riderless bike/motorcycle penalty — lower confidence when no person nearby
# ---------------------------------------------------------------------------

RIDEABLE_LABELS = {"bicycle", "motorcycle", "scooter", "cyclist", "motorcyclist"}
PERSON_LABELS = {"person", "construction worker"}
RIDERLESS_CONFIDENCE_FACTOR = 0.3  # multiply confidence by this when no rider
RIDER_OVERLAP_MARGIN_PX = 50       # how far apart person can be to count as rider


def _boxes_overlap_or_near(a: dict, b: dict, margin: float) -> bool:
    """Check if two bboxes overlap or are within margin pixels of each other."""
    return not (
        a["x_max"] + margin < b["x_min"]
        or b["x_max"] + margin < a["x_min"]
        or a["y_max"] + margin < b["y_min"]
        or b["y_max"] + margin < a["y_min"]
    )


def penalize_riderless(detections: list[dict]) -> int:
    """Reduce confidence of bicycle/motorcycle detections with no nearby person.

    Mutates detections in-place. Returns count of penalized detections.
    """
    # Group by frame_ms
    by_frame: dict[int, list[dict]] = defaultdict(list)
    for d in detections:
        by_frame[d["frame_ms"]].append(d)

    penalized = 0
    for frame_ms, frame_dets in by_frame.items():
        persons = [d for d in frame_dets if d["label"] in PERSON_LABELS]
        for d in frame_dets:
            if d["label"] not in RIDEABLE_LABELS:
                continue
            has_rider = any(
                _boxes_overlap_or_near(d, p, RIDER_OVERLAP_MARGIN_PX)
                for p in persons
            )
            if not has_rider:
                d["confidence"] *= RIDERLESS_CONFIDENCE_FACTOR
                penalized += 1

    return penalized


# ---------------------------------------------------------------------------
# Animal false positive filter — higher threshold + multi-frame consistency
# ---------------------------------------------------------------------------

def _filter_animal_detections(detections: list[dict]) -> list[dict]:
    """Remove animal detections that are below confidence threshold or appear
    in only a single frame (likely false positives from shadows/debris).

    Non-animal detections pass through unchanged.
    """
    # Separate animal vs non-animal
    animals: list[dict] = []
    others: list[dict] = []
    for d in detections:
        if d["label"] in ANIMAL_LABELS:
            animals.append(d)
        else:
            others.append(d)

    if not animals:
        return detections

    # Step 1: confidence threshold
    animals = [d for d in animals if d["confidence"] >= ANIMAL_MIN_CONFIDENCE]

    # Step 2: multi-frame consistency — group by label, require >= 2 frames
    by_label: dict[str, list[dict]] = defaultdict(list)
    for d in animals:
        by_label[d["label"]].append(d)

    kept_animals: list[dict] = []
    for label, dets in by_label.items():
        # Sort by time
        dets.sort(key=lambda d: d["frame_ms"])
        # Find unique frame timestamps
        frame_times = sorted(set(d["frame_ms"] for d in dets))

        if len(frame_times) < ANIMAL_MIN_FRAMES:
            continue  # only seen in 1 frame — discard

        # Check that at least 2 frames are within the gap threshold
        has_consecutive = False
        for i in range(1, len(frame_times)):
            if frame_times[i] - frame_times[i - 1] <= ANIMAL_FRAME_GAP_MS:
                has_consecutive = True
                break

        if has_consecutive:
            kept_animals.extend(dets)

    return others + kept_animals


# ---------------------------------------------------------------------------
# Class-agnostic NMS (copied from run_gdino_clip_pipeline.py)
# ---------------------------------------------------------------------------

def apply_nms(detections: list[dict], iou_threshold: float = 0.5) -> list[dict]:
    """Apply class-agnostic Non-Maximum Suppression to a list of detection dicts.

    GDINO with multi-class prompts can produce overlapping boxes for the same
    object with different labels (e.g., same person detected as both "person"
    and "cyclist"). This removes duplicates by keeping the highest-confidence box.

    Args:
        detections: list of dicts, each with x_min, y_min, x_max, y_max, confidence.
        iou_threshold: IoU threshold above which the lower-confidence box is suppressed.

    Returns:
        Filtered list of detection dicts.
    """
    if len(detections) <= 1:
        return detections

    boxes = torch.tensor(
        [[d["x_min"], d["y_min"], d["x_max"], d["y_max"]] for d in detections],
        dtype=torch.float32,
    )
    scores = torch.tensor(
        [d["confidence"] for d in detections],
        dtype=torch.float32,
    )

    try:
        from torchvision.ops import nms as tv_nms
        keep_indices = tv_nms(boxes, scores, iou_threshold).tolist()
    except ImportError:
        # Manual NMS fallback using numpy
        x1 = boxes[:, 0].numpy()
        y1 = boxes[:, 1].numpy()
        x2 = boxes[:, 2].numpy()
        y2 = boxes[:, 3].numpy()
        scores_np = scores.numpy()
        areas = (x2 - x1) * (y2 - y1)

        order = scores_np.argsort()[::-1]
        keep_indices: list[int] = []

        while order.size > 0:
            idx = int(order[0])
            keep_indices.append(idx)

            xx1 = np.maximum(x1[idx], x1[order[1:]])
            yy1 = np.maximum(y1[idx], y1[order[1:]])
            xx2 = np.minimum(x2[idx], x2[order[1:]])
            yy2 = np.minimum(y2[idx], y2[order[1:]])

            inter = np.maximum(0.0, xx2 - xx1) * np.maximum(0.0, yy2 - yy1)
            iou = inter / (areas[idx] + areas[order[1:]] - inter + 1e-6)

            remaining = np.where(iou <= iou_threshold)[0]
            order = order[remaining + 1]

    return [detections[i] for i in sorted(keep_indices)]


# ---------------------------------------------------------------------------
# GDINO pass (copied from run_gdino_clip_pipeline.py)
# ---------------------------------------------------------------------------

def run_gdino_pass(
    frames: list[tuple[int, Any, int]],
    conn,
    run_id: str,
    batch_size: int = 8,
) -> list[dict]:
    """
    Load GDINO-tiny (float16), run on all frames in batches, unload.
    Returns a flat list of detection dicts, each with an extra 'frame_ms' key.
    """
    from transformers import AutoModelForZeroShotObjectDetection, AutoProcessor

    print("\n  --- GDINO-tiny pass ---")
    model_id = "IDEA-Research/grounding-dino-tiny"
    print(f"  Loading {model_id}...")
    processor = AutoProcessor.from_pretrained(model_id)
    model = AutoModelForZeroShotObjectDetection.from_pretrained(model_id)
    model.eval()

    device = get_device()
    use_half = False  # MPS does not support float16 matmul reliably

    if device == "mps":
        try:
            model = model.to("mps")
            dummy = processor(
                images=Image.new("RGB", (64, 64)), text="test.", return_tensors="pt"
            )
            dummy = {k: v.to("mps") if hasattr(v, "to") else v for k, v in dummy.items()}
            with torch.no_grad():
                model(**dummy)
            print(f"  [GDINO] Confirmed running on MPS GPU (float32)")
        except Exception as exc:
            print(f"  [!] GDINO MPS FAILED ({exc}), falling back to CPU")
            model = model.to("cpu")
            device = "cpu"

    # Note: torch.compile adds ~60s compilation overhead on first batch.
    # Only worthwhile in persistent server mode, not one-shot runs.

    print(f"  Running on {device.upper()} (fp32, batch_size={batch_size}, frames={len(frames)})")

    all_detections: list[dict] = []
    nms_suppressed_total = 0
    ego_suppressed_total = 0
    frames_processed = 0
    t0 = time.time()

    GDINO_MAX_SIZE = 640  # downscale for faster inference

    for batch_start in range(0, len(frames), batch_size):
        batch_frames = frames[batch_start:batch_start + batch_size]
        pil_images = []
        for f in batch_frames:
            img = Image.fromarray(cv2.cvtColor(f[1], cv2.COLOR_BGR2RGB))
            # Downscale large images for faster inference
            if img.width > GDINO_MAX_SIZE:
                ratio = GDINO_MAX_SIZE / img.width
                img = img.resize((GDINO_MAX_SIZE, int(img.height * ratio)), Image.BILINEAR)
            pil_images.append(img)

        # Process batch — all images use the same text prompt
        inputs = processor(
            images=pil_images,
            text=[GDINO_TEXT_PROMPT] * len(pil_images),
            return_tensors="pt",
        )
        inputs = {k: v.to(device) if hasattr(v, "to") else v for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)

        # Post-process each image in the batch
        target_sizes = [(f[1].shape[0], f[1].shape[1]) for f in batch_frames]
        results = processor.post_process_grounded_object_detection(
            outputs,
            inputs["input_ids"],
            threshold=GDINO_BOX_THRESHOLD,
            text_threshold=GDINO_TEXT_THRESHOLD,
            target_sizes=target_sizes,
        )

        # Extract detections for each frame in the batch
        for j, (frame_ms, frame_bgr, frame_idx) in enumerate(batch_frames):
            h, w = frame_bgr.shape[:2]
            frame_detections: list[dict] = []

            if results and j < len(results):
                r = results[j]
                labels = r.get("text_labels", r.get("labels", []))
                scores = r.get("scores", torch.tensor([]))
                boxes = r.get("boxes", torch.tensor([]))
                for label, score, box in zip(labels, scores, boxes):
                    conf = float(score.item()) if hasattr(score, "item") else float(score)
                    coords = box.cpu().tolist()
                    # Normalize the label — drop BPE garbage and compound labels
                    raw_label = label.strip()
                    canonical = normalize_gdino_label(raw_label)
                    if canonical is None:
                        continue  # garbage label, skip

                    frame_detections.append({
                        "frame_ms": frame_ms,
                        "label": canonical,
                        "confidence": conf,
                        "x_min": coords[0],
                        "y_min": coords[1],
                        "x_max": coords[2],
                        "y_max": coords[3],
                        "frame_width": w,
                        "frame_height": h,
                    })

            # Apply class-agnostic NMS per frame to remove duplicate/overlapping boxes
            before_nms = len(frame_detections)
            frame_detections = apply_nms(frame_detections, iou_threshold=0.5)
            nms_suppressed_total += before_nms - len(frame_detections)

            # Filter out ego-vehicle (hood/dashboard/pillar) detections
            frame_detections, ego_dropped = filter_ego_vehicle(frame_detections)
            if ego_dropped:
                ego_suppressed_total += ego_dropped

            all_detections.extend(frame_detections)

            frames_processed += 1
            if frames_processed % 10 == 0 or frames_processed == len(frames):
                elapsed = time.time() - t0
                print(f"    Frame {frames_processed}/{len(frames)} -- {len(all_detections)} detections so far ({elapsed:.1f}s)")
                # Heartbeat every 10 frames
                update_heartbeat(conn, run_id)

    # Unload
    del model, processor
    free_gpu()
    ego_msg = f", {ego_suppressed_total} ego-vehicle filtered" if ego_suppressed_total else ""
    print(f"  GDINO-base done: {len(all_detections)} detections ({nms_suppressed_total} NMS suppressed{ego_msg}). Model unloaded.")
    return all_detections


# ---------------------------------------------------------------------------
# CLIP verification pass (copied from run_gdino_clip_pipeline.py)
# ---------------------------------------------------------------------------

def run_clip_verification(
    detections: list[dict],
    frames_by_ms: dict[int, Any],
    conn,
    run_id: str,
) -> tuple[list[dict], dict]:
    """
    Load OpenCLIP, verify ambiguous detections, unload.

    Args:
        detections: list of detection dicts (no frame_bgr — look up via frames_by_ms)
        frames_by_ms: dict mapping frame_ms -> BGR numpy array
        conn: database connection (for heartbeat updates)
        run_id: detection run ID (for heartbeat updates)

    Returns:
        verified_detections: list of dicts ready for DB
        stats: dict with counts for reporting
    """
    try:
        import open_clip
        OPEN_CLIP_AVAILABLE = True
    except ImportError:
        OPEN_CLIP_AVAILABLE = False

    if not OPEN_CLIP_AVAILABLE:
        print("\n  [!] open_clip not installed. Install with:")
        print("      pip install open-clip-torch")
        print("  Skipping CLIP verification -- all GDINO detections will be kept as-is.")
        return list(detections), {
            "kept": len(detections),
            "relabeled": 0,
            "dropped": 0,
            "clip_skipped": True,
        }

    print("\n  --- OpenCLIP verification pass ---")
    print(f"  Loading {CLIP_MODEL_NAME} (pretrained: {CLIP_PRETRAINED})...")

    device = get_device()
    clip_half = False  # MPS does not support float16 matmul reliably
    try:
        clip_model, _, preprocess = open_clip.create_model_and_transforms(
            CLIP_MODEL_NAME, pretrained=CLIP_PRETRAINED, device=device,
        )
        tokenizer = open_clip.get_tokenizer(CLIP_MODEL_NAME)
        if device == "mps":
            print(f"  [CLIP] Confirmed running on MPS GPU (float32)")
    except Exception as exc:
        if device == "mps":
            print(f"  [!] CLIP MPS FAILED ({exc})")
            print(f"  [!] Falling back to CPU — verification will be slower")
            device = "cpu"
            clip_model, _, preprocess = open_clip.create_model_and_transforms(
                CLIP_MODEL_NAME, pretrained=CLIP_PRETRAINED, device=device,
            )
            tokenizer = open_clip.get_tokenizer(CLIP_MODEL_NAME)
        else:
            raise
    clip_model.eval()
    print(f"  Running CLIP on {device.upper()}")

    # Pre-compute text features for all candidate label sets using prompt ensembling.
    # Averaging embeddings from multiple prompt templates yields more robust
    # classification than a single template.
    CLIP_PROMPT_TEMPLATES = [
        "a dashcam photo of a {label}",
        "a photo of a {label}",
        "a {label}",
    ]
    precomputed_text_features: dict[str, Any] = {}
    for ambiguous_label, candidates in AMBIGUOUS_CLASSES.items():
        if isinstance(candidates, list):
            ensembled_feats_list = []
            for c in candidates:
                template_feats = []
                for tmpl in CLIP_PROMPT_TEMPLATES:
                    prompt = tmpl.format(label=c)
                    tokens = tokenizer([prompt]).to(device)
                    with torch.no_grad():
                        feat = clip_model.encode_text(tokens)
                        feat = feat / feat.norm(dim=-1, keepdim=True)
                    template_feats.append(feat)
                # Average the feature vectors from all templates, then re-normalize
                avg_feat = torch.mean(torch.cat(template_feats, dim=0), dim=0, keepdim=True)
                avg_feat = avg_feat / avg_feat.norm(dim=-1, keepdim=True)
                ensembled_feats_list.append(avg_feat)
            precomputed_text_features[ambiguous_label] = torch.cat(ensembled_feats_list, dim=0)

    verified: list[dict] = []
    stats: dict[str, int] = defaultdict(int)
    stats["kept"] = 0
    stats["relabeled"] = 0
    stats["dropped"] = 0

    # Per-class tracking for the summary
    relabel_detail: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    t0 = time.time()
    ambiguous_count = 0
    total = len(detections)

    for i, det in enumerate(detections):
        label = det["label"]
        out = dict(det)

        if label in NON_AMBIGUOUS_CLASSES:
            verified.append(out)
            stats["kept"] += 1
            continue

        if label not in AMBIGUOUS_CLASSES:
            # Unknown label — keep as-is (already normalized by GDINO pass)
            verified.append(out)
            stats["kept"] += 1
            continue

        # This detection needs CLIP verification
        ambiguous_count += 1
        candidates = AMBIGUOUS_CLASSES[label]

        # Look up the frame
        frame_bgr = frames_by_ms.get(det["frame_ms"])
        if frame_bgr is None:
            stats["dropped"] += 1
            continue

        # Crop the bounding box from the frame
        h, w = frame_bgr.shape[:2]
        x1 = max(0, int(det["x_min"]))
        y1 = max(0, int(det["y_min"]))
        x2 = min(w, int(det["x_max"]))
        y2 = min(h, int(det["y_max"]))

        if x2 <= x1 or y2 <= y1:
            stats["dropped"] += 1
            continue

        crop_bgr = frame_bgr[y1:y2, x1:x2]
        crop_pil = Image.fromarray(cv2.cvtColor(crop_bgr, cv2.COLOR_BGR2RGB))
        crop_tensor = preprocess(crop_pil).unsqueeze(0).to(device)

        # Run CLIP (text features are pre-computed)
        with torch.no_grad():
            image_features = clip_model.encode_image(crop_tensor)
            image_features = image_features / image_features.norm(dim=-1, keepdim=True)
            text_features = precomputed_text_features[label]
            similarities = (image_features @ text_features.T).squeeze(0)

        sims = similarities.cpu().tolist()
        best_idx = int(similarities.argmax().item())
        best_sim = sims[best_idx]
        best_candidate = candidates[best_idx]

        # Check minimum similarity first — drop garbage before margin check
        if best_sim < CLIP_MIN_SIMILARITY:
            stats["dropped"] += 1
            continue

        # If margin between top and second candidate is too small, classification is uncertain
        sorted_sims = sorted(sims, reverse=True)
        if len(sorted_sims) > 1 and (sorted_sims[0] - sorted_sims[1]) < 0.03:
            # Uncertain — default to safe label only for person-like classes
            if label in ("construction worker", "cyclist"):
                out["label"] = "person"
                relabel_detail[label]["person (uncertain)"] += 1
            else:
                # For non-person classes (stroller), keep original GDINO label
                relabel_detail[label][label + " (uncertain)"] += 1
            verified.append(out)
            stats["relabeled"] += 1
            continue

        # Map the CLIP winner back to a canonical label
        new_label = CLIP_LABEL_REMAP.get(best_candidate, label)

        if new_label is None:
            # Remap says drop (e.g., shopping cart)
            stats["dropped"] += 1
            continue

        if new_label == label:
            verified.append(out)
            stats["kept"] += 1
        else:
            out["label"] = new_label
            verified.append(out)
            stats["relabeled"] += 1
            relabel_detail[label][new_label] += 1

        if (ambiguous_count % 50 == 0):
            print(f"    CLIP verified {ambiguous_count} ambiguous detections...")
            update_heartbeat(conn, run_id)

    elapsed = time.time() - t0

    # Unload CLIP
    del clip_model, preprocess, tokenizer, precomputed_text_features
    free_gpu()

    print(f"  CLIP verification done in {elapsed:.1f}s")
    print(f"    Ambiguous detections checked: {ambiguous_count}")
    print(f"    Kept (confirmed): {stats['kept']}")
    print(f"    Relabeled: {stats['relabeled']}")
    print(f"    Dropped (low similarity): {stats['dropped']}")

    if relabel_detail:
        print(f"    Relabel breakdown:")
        for orig_label, remap_counts in sorted(relabel_detail.items()):
            for new_label, count in sorted(remap_counts.items()):
                print(f"      {orig_label} -> {new_label}: {count}")

    stats_out = dict(stats)
    stats_out["relabel_detail"] = dict(relabel_detail)  # type: ignore[assignment]
    return verified, stats_out


# ---------------------------------------------------------------------------
# Model dispatch: GDINO + CLIP
# ---------------------------------------------------------------------------

def run_gdino_clip(
    frames: list[tuple[int, Any, int]],
    conn,
    run_id: str,
    config: dict,
) -> list[dict]:
    """Run GDINO-base detection followed by CLIP verification."""

    # Override thresholds from config if provided
    global GDINO_BOX_THRESHOLD, GDINO_TEXT_THRESHOLD, CLIP_MIN_SIMILARITY
    if "boxThreshold" in config:
        GDINO_BOX_THRESHOLD = float(config["boxThreshold"])
    if "textThreshold" in config:
        GDINO_TEXT_THRESHOLD = float(config["textThreshold"])
    if "clipMinSimilarity" in config:
        CLIP_MIN_SIMILARITY = float(config["clipMinSimilarity"])

    # Pass 1: GDINO (with configurable batch size)
    batch_size = int(config.get("batchSize", 8))
    gdino_detections = run_gdino_pass(frames, conn, run_id, batch_size=batch_size)

    # Build frames lookup for CLIP pass
    frames_by_ms = {frame_ms: frame_bgr for frame_ms, frame_bgr, _ in frames}

    # Pass 2: CLIP verification
    clip_enabled = config.get("clipVerification", True)
    if clip_enabled:
        print(f"\n  Running CLIP verification on {len(gdino_detections)} GDINO detections...")
        verified_detections, clip_stats = run_clip_verification(
            gdino_detections, frames_by_ms, conn, run_id
        )
    else:
        print("\n  CLIP verification disabled by config, keeping all GDINO detections")
        verified_detections = gdino_detections
        clip_stats = {"kept": len(gdino_detections), "relabeled": 0, "dropped": 0}

    # Print summary
    before_count = len(gdino_detections)
    after_count = len(verified_detections)
    print(f"\n  GDINO: {before_count} raw -> {after_count} after CLIP "
          f"(kept={clip_stats.get('kept', 0)}, relabeled={clip_stats.get('relabeled', 0)}, "
          f"dropped={clip_stats.get('dropped', 0)})")

    return verified_detections


# ---------------------------------------------------------------------------
# CLIP scene classification — classify weather, road type, etc.
# ---------------------------------------------------------------------------

SCENE_ATTRIBUTES_DAY: list[str] = [
    "clear sunny weather",
    "overcast cloudy sky",
    "rainy weather with wet road",
    "snowy weather with snow on road",
    "foggy or misty conditions",
]

# Short canonical labels for storage
SCENE_LABEL_MAP: dict[str, str] = {
    "clear sunny weather": "clear skies",
    "overcast cloudy sky": "overcast",
    "rainy weather with wet road": "rain",
    "snowy weather with snow on road": "snow",
    "foggy or misty conditions": "fog",
}

# Minimum margin between top-1 and top-2 cosine similarity to report weather
SCENE_MIN_MARGIN = 0.02

SCENE_PROMPT_TEMPLATES = [
    "a dashcam photo showing {label}",
    "a photo of {label}",
    "{label}",
]

SCENE_NUM_SAMPLE_FRAMES = 3  # classify beginning, middle, end


def run_scene_classification(
    frames: list[tuple[int, Any, int]],
    conn,
    video_id: str,
    run_id: str,
    is_night: bool = False,
) -> dict[str, dict]:
    """Classify scene attributes using OpenCLIP on a few sampled frames.

    Returns dict mapping attribute name -> {"value": str, "confidence": float}.
    """
    try:
        import open_clip
    except ImportError:
        print("  [!] open_clip not installed, skipping scene classification")
        return {}

    if not frames:
        return {}

    # Sample frames: first, middle, last
    n = len(frames)
    sample_indices = [0, n // 2, n - 1] if n >= 3 else list(range(n))
    sample_frames = [frames[i] for i in sample_indices]

    print(f"\n  --- CLIP scene classification ({len(sample_frames)} frames) ---")

    device = get_device()
    try:
        clip_model, _, preprocess = open_clip.create_model_and_transforms(
            CLIP_MODEL_NAME, pretrained=CLIP_PRETRAINED, device=device,
        )
        tokenizer = open_clip.get_tokenizer(CLIP_MODEL_NAME)
    except Exception as exc:
        if device == "mps":
            print(f"  [!] MPS failed ({exc}), falling back to CPU")
            device = "cpu"
            clip_model, _, preprocess = open_clip.create_model_and_transforms(
                CLIP_MODEL_NAME, pretrained=CLIP_PRETRAINED, device=device,
            )
            tokenizer = open_clip.get_tokenizer(CLIP_MODEL_NAME)
        else:
            raise
    clip_model.eval()

    # Encode sample frames
    frame_features_list = []
    for frame_ms, frame_bgr, _ in sample_frames:
        pil_img = Image.fromarray(cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB))
        img_tensor = preprocess(pil_img).unsqueeze(0).to(device)
        with torch.no_grad():
            feat = clip_model.encode_image(img_tensor)
            feat = feat / feat.norm(dim=-1, keepdim=True)
        frame_features_list.append(feat)

    # Average frame features
    avg_frame_feat = torch.mean(torch.cat(frame_features_list, dim=0), dim=0, keepdim=True)
    avg_frame_feat = avg_frame_feat / avg_frame_feat.norm(dim=-1, keepdim=True)

    results: dict[str, dict] = {}

    # Skip weather classification at night — CLIP is unreliable in low-light dashcam footage
    if is_night:
        print("    weather          skipped (nighttime)")
    else:
        candidates = SCENE_ATTRIBUTES_DAY

        # Classify weather
        candidate_features = []
        for c in candidates:
            template_feats = []
            for tmpl in SCENE_PROMPT_TEMPLATES:
                prompt = tmpl.format(label=c)
                tokens = tokenizer([prompt]).to(device)
                with torch.no_grad():
                    feat = clip_model.encode_text(tokens)
                    feat = feat / feat.norm(dim=-1, keepdim=True)
                template_feats.append(feat)
            avg_feat = torch.mean(torch.cat(template_feats, dim=0), dim=0, keepdim=True)
            avg_feat = avg_feat / avg_feat.norm(dim=-1, keepdim=True)
            candidate_features.append(avg_feat)

        text_feats = torch.cat(candidate_features, dim=0)
        sims = (avg_frame_feat @ text_feats.T).squeeze(0)

        # Sort by similarity descending
        sorted_sims, sorted_indices = torch.sort(sims, descending=True)
        best_idx = sorted_indices[0].item()
        best_sim = float(sorted_sims[0].item())
        second_sim = float(sorted_sims[1].item()) if len(sorted_sims) > 1 else 0.0
        margin = best_sim - second_sim

        best_candidate = candidates[best_idx]
        canonical = SCENE_LABEL_MAP.get(best_candidate, best_candidate)

        if margin >= SCENE_MIN_MARGIN:
            results["weather"] = {"value": canonical, "confidence": round(best_sim, 3)}
            print(f"    weather          {canonical:<12} (confidence: {best_sim:.3f}, margin: {margin:.3f})")
        else:
            print(f"    weather          skipped — low margin ({canonical} {best_sim:.3f} vs {margin:.3f})")

    # Unload CLIP
    del clip_model, preprocess, tokenizer
    free_gpu()

    # Clear old weather attribute for this run, then save new results
    conn.execute("DELETE FROM scene_attributes WHERE run_id = ? AND attribute = 'weather'", (run_id,))
    conn.commit()
    _save_scene_attributes(conn, video_id, run_id, results)

    return results


def _save_scene_attributes(
    conn, video_id: str, run_id: str, attributes: dict[str, dict]
) -> None:
    """Save scene classification results to the scene_attributes table."""
    # Ensure table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scene_attributes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            run_id TEXT,
            attribute TEXT NOT NULL,
            value TEXT NOT NULL,
            confidence REAL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_scene_attributes_video
        ON scene_attributes (video_id)
    """)
    conn.commit()

    # Delete only the specific attributes being written (not all for this run)
    for attr in attributes:
        conn.execute("DELETE FROM scene_attributes WHERE run_id = ? AND attribute = ?", (run_id, attr))
    for attr, data in attributes.items():
        conn.execute(
            "INSERT INTO scene_attributes (video_id, run_id, attribute, value, confidence) VALUES (?, ?, ?, ?, ?)",
            (video_id, run_id, attr, data["value"], data["confidence"]),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Timeline generation — hybrid: detections + Claude Vision for key frames
# ---------------------------------------------------------------------------

TIMELINE_MODEL = "claude-haiku-4-5-20251001"


TIMELINE_KEY_FRAMES = 15


def _select_key_frames(
    frames: list[tuple[int, Any, int]],
    detections: list[dict],
    event: dict,
) -> list[tuple[int, Any]]:
    """Select up to TIMELINE_KEY_FRAMES frames for Claude Vision.

    Strategy: evenly space frames across the clip, then add extra density
    around the braking window and closest VRU detection.
    Returns list of (frame_ms, frame_bgr) tuples.
    """
    frames_by_ms = {ms: bgr for ms, bgr, _ in frames}
    all_ms = sorted(frames_by_ms.keys())
    if not all_ms:
        return []

    target = min(TIMELINE_KEY_FRAMES, len(all_ms))
    selected: dict[int, Any] = {}

    # 1. Evenly-spaced baseline (~60% of budget)
    baseline_count = max(target * 3 // 5, 3)
    for i in range(baseline_count):
        idx = round(i / (baseline_count - 1) * (len(all_ms) - 1))
        ms = all_ms[idx]
        selected[ms] = frames_by_ms[ms]

    # 2. Dense frames around braking window
    decel_window = _extract_decel_window(event)
    if decel_window:
        decel_start, decel_end = decel_window
        # Sample densely from 1s before braking to 1s after
        window_start = max(decel_start - 1000, all_ms[0])
        window_end = min(decel_end + 1000, all_ms[-1])
        window_frames = [ms for ms in all_ms if window_start <= ms <= window_end]
        # Pick up to 5 evenly-spaced frames from the window
        dense_count = min(5, len(window_frames))
        for i in range(dense_count):
            idx = round(i / max(dense_count - 1, 1) * (len(window_frames) - 1))
            ms = window_frames[idx]
            selected[ms] = frames_by_ms[ms]

    # 3. Frame with closest VRU detection
    vru_dets = [d for d in detections if d["label"] in VRU_LABELS_FOR_NEARMISS]
    if vru_dets:
        closest = min(vru_dets, key=lambda d: _estimate_vru_distance(d))
        nearest_ms = min(all_ms, key=lambda ms: abs(ms - closest["frame_ms"]))
        selected[nearest_ms] = frames_by_ms[nearest_ms]

    # Trim to target if we overshot (keep evenly spaced subset)
    if len(selected) > target:
        sorted_ms = sorted(selected.keys())
        step = len(sorted_ms) / target
        keep = [sorted_ms[round(i * step)] for i in range(target)]
        selected = {ms: selected[ms] for ms in keep}

    return sorted(selected.items(), key=lambda x: x[0])


def _frame_to_base64(frame_bgr) -> str:
    """Convert BGR numpy array to base64 JPEG string."""
    import base64
    _, buffer = cv2.imencode(".jpg", frame_bgr, [cv2.IMWRITE_JPEG_QUALITY, 80])
    return base64.b64encode(buffer).decode("utf-8")


def generate_timeline(
    frames: list[tuple[int, Any, int]],
    detections: list[dict],
    event: dict,
    conn,
    video_id: str,
    run_id: str,
    scene_attrs: dict,
) -> list[dict] | None:
    """Generate a narrative timeline using Claude Vision on key frames.

    Returns list of timeline entries or None if unavailable.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY") or ""
    # Also check .env.local
    if not api_key:
        env_local = PROJECT_ROOT / ".env.local"
        if env_local.exists():
            for line in env_local.read_text().splitlines():
                if line.startswith("ANTHROPIC_API_KEY="):
                    api_key = line.split("=", 1)[1].strip()
                    break

    if not api_key:
        print("  [!] ANTHROPIC_API_KEY not set, skipping timeline generation")
        return None

    key_frames = _select_key_frames(frames, detections, event)
    if not key_frames:
        return None

    print(f"\n  --- Timeline generation ({len(key_frames)} key frames) ---")

    # Build detection summary per time window
    duration_ms = max(d["frame_ms"] for d in detections) if detections else 30000
    duration_s = duration_ms / 1000

    # Speed context
    speeds = event.get("metadata", {}).get("SPEED_ARRAY", [])
    max_mph = min_mph = 0.0
    if speeds:
        vals = [s.get("AVG_SPEED_MS", 0) * 2.237 for s in speeds]
        max_mph, min_mph = max(vals), min(vals)

    # Detection summary
    label_counts: dict[str, int] = defaultdict(int)
    for d in detections:
        label_counts[d["label"]] += 1
    det_summary = ", ".join(f"{v}x {k}" for k, v in sorted(label_counts.items(), key=lambda x: -x[1])[:6])

    weather_val = scene_attrs.get("weather", {}).get("value", "unknown") if scene_attrs else "unknown"

    # Build Claude Vision request
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    content: list[dict] = []

    # System context — speed/event data + VRU timing (factual, not scene labels).
    # We include VRU detections with timestamps so Claude can reference them,
    # but NOT scene labels (crosswalk, traffic light) to avoid hallucination.
    vru_timing_lines = []
    vru_by_label: dict[str, list[dict]] = defaultdict(list)
    for d in detections:
        if d["label"] in VRU_LABELS_FOR_NEARMISS and d["confidence"] >= 0.35:
            vru_by_label[d["label"]].append(d)
    for label, dets in sorted(vru_by_label.items()):
        times = sorted(set(d["frame_ms"] for d in dets))
        if times:
            start_s = times[0] / 1000
            end_s = times[-1] / 1000
            max_conf = max(d["confidence"] for d in dets)
            if end_s - start_s > 0.5:
                vru_timing_lines.append(f"- {label} detected {start_s:.1f}–{end_s:.1f}s (confidence {max_conf:.0%})")
            else:
                vru_timing_lines.append(f"- {label} detected at {start_s:.1f}s (confidence {max_conf:.0%})")

    vru_section = ""
    if vru_timing_lines:
        vru_section = "\nVulnerable road users detected:\n" + "\n".join(vru_timing_lines) + "\n"

    context_text = (
        f"You are analyzing a {duration_s:.0f}-second dashcam video from a safety event.\n\n"
        f"Event type: {event.get('type', 'UNKNOWN')}\n"
        f"Speed: {max_mph:.0f} → {min_mph:.0f} mph\n"
        f"Weather: {weather_val}\n"
        f"{vru_section}\n"
        f"Describe ONLY what you can see in the frames. Do not assume or infer things not visible.\n\n"
        f"Here are {len(key_frames)} key frames from the clip:"
    )
    content.append({"type": "text", "text": context_text})

    # Add frames
    for frame_ms, frame_bgr in key_frames:
        t_sec = frame_ms / 1000
        # Speed at this moment
        frame_speed = 0.0
        if speeds:
            event_start = speeds[0].get("TIMESTAMP", 0)
            for s in speeds:
                if (s["TIMESTAMP"] - event_start) <= frame_ms:
                    frame_speed = s.get("AVG_SPEED_MS", 0) * 2.237

        content.append({
            "type": "text",
            "text": f"\nFrame at {t_sec:.1f}s (speed: {frame_speed:.0f} mph):",
        })
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": _frame_to_base64(frame_bgr),
            },
        })

    content.append({
        "type": "text",
        "text": (
            f"\nGenerate a timeline with 4-6 rows covering the full {duration_s:.0f}s clip.\n\n"
            "Rules:\n"
            "- Say 'driver' not 'dashcam vehicle'\n"
            "- Keep event labels to 2-4 words\n"
            "- Keep details to ONE short sentence (max 15 words)\n"
            "- Only describe what is clearly visible in the frames\n"
            "- Do NOT describe ambiguous objects as animals. Piles of dirt, debris, shadows, bags, "
            "and other roadside objects are often mistaken for animals in dashcam footage. "
            "Only mention an animal if you are highly confident it is one (clear shape, legs, movement).\n\n"
            "Return ONLY valid JSON — an array of objects:\n"
            '  startSec (number), endSec (number), event (short label), details (1 short sentence)\n\n'
            "Example:\n"
            '[{"startSec":0,"endSec":6,"event":"Approach intersection","details":"Driver approaches lit intersection with traffic ahead."}]\n\n'
            "Cover the full clip duration."
        ),
    })

    try:
        response = client.messages.create(
            model=TIMELINE_MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": content}],
        )

        # Parse JSON from response
        raw_text = response.content[0].text.strip()
        # Handle markdown code blocks
        if raw_text.startswith("```"):
            raw_text = raw_text.split("\n", 1)[1]
            if raw_text.endswith("```"):
                raw_text = raw_text[:-3].strip()

        timeline = json.loads(raw_text)

        # Save to DB
        conn.execute("""
            CREATE TABLE IF NOT EXISTS clip_timelines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT NOT NULL,
                run_id TEXT,
                timeline_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
        conn.execute("DELETE FROM clip_timelines WHERE run_id = ?", (run_id,))
        conn.execute(
            "INSERT INTO clip_timelines (video_id, run_id, timeline_json) VALUES (?, ?, ?)",
            (video_id, run_id, json.dumps(timeline)),
        )
        conn.commit()

        print(f"  Timeline: {len(timeline)} segments generated")
        for seg in timeline:
            print(f"    {seg.get('startSec', '?')}-{seg.get('endSec', '?')}s: "
                  f"{seg.get('event', '?')} — {seg.get('details', '')[:60]}")

        return timeline

    except Exception as exc:
        print(f"  [!] Timeline generation failed: {exc}")
        return None


# ---------------------------------------------------------------------------
# Model dispatch: MM-Grounding-DINO
# ---------------------------------------------------------------------------

# MM-GDINO uses list-of-lists text format (trained on V3Det with 13,204 categories)
MM_GDINO_TEXT_LABELS = [
    "person", "bicycle", "motorcycle", "car", "truck", "bus",
    "electric scooter", "wheelchair", "stroller", "skateboard",
    "dog", "cat", "traffic cone", "construction worker",
]

MM_GDINO_BOX_THRESHOLD = 0.30
MM_GDINO_TEXT_THRESHOLD = 0.25


def run_mm_gdino(
    frames: list[tuple[int, Any, int]],
    conn,
    run_id: str,
    config: dict,
) -> list[dict]:
    """Run MM-Grounding-DINO large (V3Det-trained) detection.

    MM-GDINO is trained on V3Det (13,204 categories) so it has strong
    recognition for common classes without needing CLIP verification.
    Uses list-of-lists text format instead of period-separated strings.
    """
    from transformers import AutoModelForZeroShotObjectDetection, AutoProcessor

    # Override thresholds from config if provided
    box_threshold = float(config.get("boxThreshold", MM_GDINO_BOX_THRESHOLD))
    text_threshold = float(config.get("textThreshold", MM_GDINO_TEXT_THRESHOLD))
    batch_size = int(config.get("batchSize", 1))  # batch=1 is faster (no padding overhead)

    # Use MPS if available (2.5x faster than CPU), fall back to CPU
    forced_device = config.get("device", None)

    # Support base and large variants (tiny has broken bbox output)
    variant = config.get("variant", "base")  # "base" or "large"
    model_ids = {
        "base": "openmmlab-community/mm_grounding_dino_base_all",
        "large": "openmmlab-community/mm_grounding_dino_large_all",
    }
    model_id = model_ids.get(variant, model_ids["base"])
    print(f"\n  --- MM-Grounding-DINO ({variant}) pass ---")
    print(f"  Loading {model_id}...")
    processor = AutoProcessor.from_pretrained(model_id)
    model = AutoModelForZeroShotObjectDetection.from_pretrained(model_id)
    model.eval()

    device = forced_device or get_device()
    if device == "mps":
        try:
            model = model.to("mps")
            # Smoke test
            dummy = processor(
                images=Image.new("RGB", (64, 64)),
                text=[["test"]],
                return_tensors="pt",
            )
            dummy = {k: v.to("mps") if hasattr(v, "to") else v for k, v in dummy.items()}
            with torch.no_grad():
                model(**dummy)
        except Exception as exc:
            print(f"  [!] MPS failed ({exc}), falling back to CPU")
            model = model.to("cpu")
            device = "cpu"
    else:
        model = model.to(device)
    print(f"  Running on {device.upper()} (batch_size={batch_size})")
    print(f"  Config: box_threshold={box_threshold}, text_threshold={text_threshold}")

    # Build the text labels in list-of-lists format (one list per image in batch)
    text_labels_single = [MM_GDINO_TEXT_LABELS]

    all_detections: list[dict] = []
    nms_suppressed_total = 0
    ego_suppressed_total = 0
    frames_processed = 0
    t0 = time.time()

    for batch_start in range(0, len(frames), batch_size):
        batch_frames = frames[batch_start:batch_start + batch_size]
        t_batch = time.time()

        # Preprocessing: convert BGR to PIL
        t_pre = time.time()
        pil_images = [Image.fromarray(cv2.cvtColor(f[1], cv2.COLOR_BGR2RGB)) for f in batch_frames]
        t_pre_done = time.time()

        # Tokenize
        t_tok = time.time()
        text_input = [MM_GDINO_TEXT_LABELS] * len(pil_images)
        inputs = processor(
            images=pil_images,
            text=text_input,
            return_tensors="pt",
        )
        inputs = {k: v.to(device) if hasattr(v, "to") else v for k, v in inputs.items()}
        t_tok_done = time.time()

        # Inference
        t_inf = time.time()
        with torch.no_grad():
            outputs = model(**inputs)
        t_inf_done = time.time()

        # Post-process
        t_post = time.time()
        target_sizes = [(f[1].shape[0], f[1].shape[1]) for f in batch_frames]
        results = processor.post_process_grounded_object_detection(
            outputs,
            inputs["input_ids"],
            threshold=box_threshold,
            text_threshold=text_threshold,
            target_sizes=target_sizes,
        )
        t_post_done = time.time()

        # Extract detections for each frame in the batch
        for j, (frame_ms, frame_bgr, frame_idx) in enumerate(batch_frames):
            h, w = frame_bgr.shape[:2]
            frame_detections: list[dict] = []

            if results and j < len(results):
                r = results[j]
                labels = r.get("text_labels", r.get("labels", []))
                scores = r.get("scores", torch.tensor([]))
                boxes = r.get("boxes", torch.tensor([]))
                for label, score, box in zip(labels, scores, boxes):
                    conf = float(score.item()) if hasattr(score, "item") else float(score)
                    coords = box.cpu().tolist()
                    cls_name = label.strip().lower()
                    if not cls_name:
                        continue

                    frame_detections.append({
                        "frame_ms": frame_ms,
                        "label": cls_name,
                        "confidence": conf,
                        "x_min": coords[0],
                        "y_min": coords[1],
                        "x_max": coords[2],
                        "y_max": coords[3],
                        "frame_width": w,
                        "frame_height": h,
                    })

            # Apply class-agnostic NMS per frame
            before_nms = len(frame_detections)
            frame_detections = apply_nms(frame_detections, iou_threshold=0.5)
            nms_suppressed_total += before_nms - len(frame_detections)

            # Filter out ego-vehicle (hood/dashboard/pillar) detections
            frame_detections, ego_dropped = filter_ego_vehicle(frame_detections)
            if ego_dropped:
                ego_suppressed_total += ego_dropped

            all_detections.extend(frame_detections)
            frames_processed += 1

        # Per-batch timing
        t_batch_done = time.time()
        batch_num = batch_start // batch_size + 1
        total_batches = (len(frames) + batch_size - 1) // batch_size
        elapsed = time.time() - t0
        avg_per_frame = elapsed / frames_processed if frames_processed else 0
        eta = avg_per_frame * (len(frames) - frames_processed)
        print(
            f"    Batch {batch_num}/{total_batches} ({len(batch_frames)} frames) -- "
            f"{len(all_detections)} detections -- "
            f"preprocess={((t_pre_done - t_pre) * 1000):.0f}ms "
            f"tokenize={((t_tok_done - t_tok) * 1000):.0f}ms "
            f"inference={((t_inf_done - t_inf) * 1000):.0f}ms "
            f"postprocess={((t_post_done - t_post) * 1000):.0f}ms "
            f"total={((t_batch_done - t_batch) * 1000):.0f}ms "
            f"({elapsed:.0f}s elapsed, ~{eta:.0f}s remaining)"
        )
        update_heartbeat(conn, run_id)

    # Unload
    del model, processor
    free_gpu()
    print(f"  MM-GDINO done: {len(all_detections)} detections ({nms_suppressed_total} suppressed by NMS). Model unloaded.")
    return all_detections


# ---------------------------------------------------------------------------
# Model dispatch: YOLO-World
# ---------------------------------------------------------------------------

def run_yolo_world(
    frames: list[tuple[int, Any, int]],
    conn,
    run_id: str,
    config: dict,
) -> list[dict]:
    """Run YOLO-World open-vocabulary detection."""
    from ultralytics import YOLO

    conf_threshold = float(config.get("confidence", YOLO_WORLD_CONF))

    print("\n  --- YOLO-World pass ---")
    print("  Loading yolov8l-worldv2...")
    model = YOLO("yolov8l-worldv2.pt")
    model.set_classes(VRU_CLASSES)

    device = get_device()
    model.to(device)
    print(f"  Running on {device.upper()}")

    all_detections: list[dict] = []
    t0 = time.time()

    for i, (frame_ms, frame_bgr, frame_idx) in enumerate(frames):
        h, w = frame_bgr.shape[:2]
        results = model.predict(frame_bgr, conf=conf_threshold, verbose=False)[0]
        for box in results.boxes:
            cls_name = results.names[int(box.cls[0].item())]
            conf = float(box.conf[0].item())
            coords = box.xyxy[0].tolist()
            all_detections.append({
                "frame_ms": frame_ms,
                "label": cls_name,
                "confidence": conf,
                "x_min": coords[0],
                "y_min": coords[1],
                "x_max": coords[2],
                "y_max": coords[3],
                "frame_width": w,
                "frame_height": h,
            })

        if (i + 1) % 10 == 0 or i == len(frames) - 1:
            elapsed = time.time() - t0
            print(f"    Frame {i + 1}/{len(frames)} -- {len(all_detections)} detections ({elapsed:.1f}s)")
            update_heartbeat(conn, run_id)

    del model
    free_gpu()

    # Filter ego-vehicle detections
    all_detections, ego_dropped = filter_ego_vehicle(all_detections)
    ego_msg = f", {ego_dropped} ego-vehicle filtered" if ego_dropped else ""
    print(f"  YOLO-World done: {len(all_detections)} detections{ego_msg}")
    return all_detections


# ---------------------------------------------------------------------------
# Model dispatch: YOLO11x (standard COCO-80)
# ---------------------------------------------------------------------------

def enhance_night_frame(frame_bgr: Any) -> Any:
    """Apply CLAHE enhancement for low-light dashcam frames."""
    lab = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
    l = clahe.apply(l)
    enhanced = cv2.merge([l, a, b])
    return cv2.cvtColor(enhanced, cv2.COLOR_LAB2BGR)


def is_dark_frame(frame_bgr: Any, threshold: float = 80.0) -> bool:
    """Check if a frame is dark (night/low-light) based on mean brightness."""
    gray = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2GRAY)
    return float(gray.mean()) < threshold


def run_yolo11x(
    frames: list[tuple[int, Any, int]],
    conn,
    run_id: str,
    config: dict,
) -> list[dict]:
    """Run standard YOLO11x with COCO label mapping.

    Quick wins applied:
    - augment=True for test-time augmentation (+2.8% small object AP)
    - CLAHE preprocessing for dark/night frames
    - imgsz=1280 (higher res for better small object detection)
    """
    from ultralytics import YOLO

    conf_threshold = float(config.get("confidence", YOLO11X_CONF))
    use_tta = bool(config.get("augment", False))  # TTA off by default (2-3x slower, may be unreliable on MPS)
    imgsz = int(config.get("imgsz", 1280))

    print("\n  --- YOLO11x (COCO-80) pass ---")
    print(f"  Config: conf={conf_threshold}, augment={use_tta}, imgsz={imgsz}")
    print("  Loading yolo11x.pt...")
    model = YOLO("yolo11x.pt")

    device = get_device()
    model.to(device)
    print(f"  Running on {device.upper()}")

    all_detections: list[dict] = []
    enhanced_count = 0
    t0 = time.time()

    for i, (frame_ms, frame_bgr, frame_idx) in enumerate(frames):
        h, w = frame_bgr.shape[:2]

        # Apply CLAHE per-frame for dark frames (sub-ms check)
        if is_dark_frame(frame_bgr):
            input_frame = enhance_night_frame(frame_bgr)
            enhanced_count += 1
        else:
            input_frame = frame_bgr

        results = model.predict(
            input_frame,
            verbose=False,
            conf=conf_threshold,
            device=device,
            imgsz=imgsz,
            augment=use_tta,
        )[0]

        if results.boxes is not None:
            names = results.names
            for box in results.boxes:
                cls_idx = int(box.cls[0].item())
                cls_name = names.get(cls_idx, str(cls_idx))
                mapped = COCO_LABEL_MAP.get(cls_name)
                if mapped is None:
                    continue
                conf = float(box.conf[0].item())
                coords = box.xyxy[0].tolist()
                all_detections.append({
                    "frame_ms": frame_ms,
                    "label": mapped,
                    "confidence": conf,
                    "x_min": coords[0],
                    "y_min": coords[1],
                    "x_max": coords[2],
                    "y_max": coords[3],
                    "frame_width": w,
                    "frame_height": h,
                })

        if (i + 1) % 10 == 0 or i == len(frames) - 1:
            elapsed = time.time() - t0
            print(f"    Frame {i + 1}/{len(frames)} -- {len(all_detections)} detections ({elapsed:.1f}s)")
            update_heartbeat(conn, run_id)

    del model
    free_gpu()

    if enhanced_count > 0:
        print(f"  CLAHE enhanced {enhanced_count}/{len(frames)} dark frames")
    # Filter ego-vehicle detections
    all_detections, ego_dropped = filter_ego_vehicle(all_detections)
    ego_msg = f", {ego_dropped} ego-vehicle filtered" if ego_dropped else ""
    print(f"  YOLO11x done: {len(all_detections)} detections{ego_msg}")
    return all_detections


# ---------------------------------------------------------------------------
# YOLO26x runner (same as YOLO11x but with yolo26x model)
# ---------------------------------------------------------------------------

def run_yolo26x(
    frames: list[tuple[int, Any, int]],
    conn,
    run_id: str,
    config: dict,
) -> list[dict]:
    """Run YOLO26x with COCO label mapping.

    YOLO26 (Jan 2026) — improved small object detection, 43% faster CPU inference,
    NMS-free architecture. Same COCO-80 classes as YOLO11x.
    """
    from ultralytics import YOLO

    conf_threshold = float(config.get("confidence", YOLO11X_CONF))
    use_tta = bool(config.get("augment", False))
    imgsz = int(config.get("imgsz", 1280))

    print("\n  --- YOLO26x (COCO-80) pass ---")
    print(f"  Config: conf={conf_threshold}, augment={use_tta}, imgsz={imgsz}")
    print("  Loading yolo26x.pt...")
    model = YOLO("yolo26x.pt")

    device = get_device()
    model.to(device)
    print(f"  Running on {device.upper()}")

    all_detections: list[dict] = []
    enhanced_count = 0
    t0 = time.time()

    for i, (frame_ms, frame_bgr, frame_idx) in enumerate(frames):
        h, w = frame_bgr.shape[:2]

        if is_dark_frame(frame_bgr):
            input_frame = enhance_night_frame(frame_bgr)
            enhanced_count += 1
        else:
            input_frame = frame_bgr

        results = model.predict(
            input_frame,
            verbose=False,
            conf=conf_threshold,
            device=device,
            imgsz=imgsz,
            augment=use_tta,
        )[0]

        if results.boxes is not None:
            names = results.names
            for box in results.boxes:
                cls_idx = int(box.cls[0].item())
                cls_name = names.get(cls_idx, str(cls_idx))
                mapped = COCO_LABEL_MAP.get(cls_name)
                if mapped is None:
                    continue
                conf = float(box.conf[0].item())
                coords = box.xyxy[0].tolist()
                all_detections.append({
                    "frame_ms": frame_ms,
                    "label": mapped,
                    "confidence": conf,
                    "x_min": coords[0],
                    "y_min": coords[1],
                    "x_max": coords[2],
                    "y_max": coords[3],
                    "frame_width": w,
                    "frame_height": h,
                })

        if (i + 1) % 10 == 0 or i == len(frames) - 1:
            elapsed = time.time() - t0
            print(f"    Frame {i + 1}/{len(frames)} -- {len(all_detections)} detections ({elapsed:.1f}s)")
            update_heartbeat(conn, run_id)

    del model
    free_gpu()

    if enhanced_count > 0:
        print(f"  CLAHE enhanced {enhanced_count}/{len(frames)} dark frames")
    # Filter ego-vehicle detections
    all_detections, ego_dropped = filter_ego_vehicle(all_detections)
    ego_msg = f", {ego_dropped} ego-vehicle filtered" if ego_dropped else ""
    print(f"  YOLO26x done: {len(all_detections)} detections{ego_msg}")
    return all_detections


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unified detection runner")
    parser.add_argument("--run-id", required=True, help="Detection run UUID from detection_runs table")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = args.run_id

    print("=" * 70)
    print(f"Detection Runner  --  run_id={run_id}")
    print("=" * 70)

    # Connect to DB
    print("\n[1/6] Connecting to database...")
    conn = get_db()

    # Read run config
    print(f"\n[2/6] Reading detection run {run_id}...")
    run = get_detection_run(conn, run_id)
    if run is None:
        print(f"ERROR: detection_runs row not found for id={run_id}", file=sys.stderr)
        conn.close()
        return 1

    if run["status"] != "queued":
        print(f"ERROR: run status is '{run['status']}', expected 'queued'", file=sys.stderr)
        conn.close()
        return 1

    video_id = run["video_id"]
    model_name = run["model_name"]
    config = json.loads(run["config_json"] or "{}")
    num_frames = int(config.get("framesPerVideo", FRAMES_PER_VIDEO))

    print(f"  video_id:   {video_id}")
    print(f"  model_name: {model_name}")
    print(f"  config:     {json.dumps(config)}")
    print(f"  frames:     {num_frames}")

    # Update status to running and sync so UI sees it immediately
    update_run_status(conn, run_id, "running")

    try:
        # Fetch event from API
        print(f"\n[3/6] Fetching event {video_id} from API...")
        api_key = load_api_key()
        resp = api_request(
            "GET",
            f"{API_BASE_URL}/{video_id}",
            headers={
                "Content-Type": "application/json",
                "Authorization": api_key,
            },
        )
        resp.raise_for_status()
        event = resp.json()
        video_url = event.get("videoUrl")
        if not video_url:
            # Mark as missing_video so it won't be picked again
            try:
                conn.execute(
                    "UPDATE triage_results SET triage_result = 'missing_video', "
                    "rules_triggered = '[\"no_video_url_at_detection\"]' WHERE id = ?",
                    (video_id,)
                )
                conn.commit()
            except Exception:
                pass
            raise RuntimeError(f"Event {video_id} has no videoUrl")
        event_timestamp = event.get("timestamp", "")
        day = event_timestamp[:10] or "2026-01-01"
        event_lat = event.get("location", {}).get("lat")
        event_lon = event.get("location", {}).get("lon")

        # Determine if event is at night using UTC hour + longitude-based offset
        is_night = False
        if event_timestamp:
            try:
                utc_dt = datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))
                # Rough local hour: shift UTC by longitude / 15 degrees per hour
                tz_offset_hours = (event_lon / 15.0) if event_lon else 0
                local_hour = (utc_dt.hour + tz_offset_hours) % 24
                is_night = local_hour < 6 or local_hour >= 18
                print(f"  local hour: ~{local_hour:.0f} ({'night' if is_night else 'day'})")
            except Exception:
                pass
        print(f"  type: {event.get('type', '?')}")
        print(f"  day:  {day}")
        if event_lat and event_lon:
            print(f"  loc:  {event_lat:.6f}, {event_lon:.6f}")

        # Download video
        print(f"\n[4/6] Downloading video...")
        video_path = download_video(video_url)
        if not video_path:
            raise RuntimeError("Video download failed")
        print(f"  Cached at: {video_path}")

        # Extract frames
        print(f"\n[5/6] Extracting {num_frames} frames...")
        frames = extract_frames(video_path, num_frames)
        print(f"  Extracted {len(frames)} frames")
        if not frames:
            raise RuntimeError("No frames extracted from video")

        # Ensure video pipeline state
        ensure_video_pipeline_state(conn, video_id, day, model_name)

        # Run model
        print(f"\n[6/6] Running model: {model_name}")

        if model_name == "gdino-base-clip":
            detections = run_gdino_clip(frames, conn, run_id, config)
        elif model_name == "mm-gdino":
            detections = run_mm_gdino(frames, conn, run_id, config)
        elif model_name == "yolo-world":
            detections = run_yolo_world(frames, conn, run_id, config)
        elif model_name == "yolo11x":
            detections = run_yolo11x(frames, conn, run_id, config)
        elif model_name == "yolo26x":
            detections = run_yolo26x(frames, conn, run_id, config)
        else:
            raise RuntimeError(f"Unknown model_name: {model_name}")

        # Filter out irrelevant vehicle detections (parked cars, distant traffic)
        before_vf = len(detections)
        detections, vehicles_removed = filter_non_collision_vehicles(detections, event)
        if vehicles_removed:
            print(f"\n  Vehicle near-collision filter: removed {vehicles_removed} "
                  f"irrelevant vehicle detections ({before_vf} -> {len(detections)})")

        # Penalize riderless bikes/motorcycles (likely parked)
        riderless_count = penalize_riderless(detections)
        if riderless_count:
            print(f"  Riderless penalty: {riderless_count} bike/motorcycle detections "
                  f"had confidence reduced (no person nearby)")

        # Filter animal false positives: require higher confidence + multi-frame
        before_animal = len(detections)
        detections = _filter_animal_detections(detections)
        animal_removed = before_animal - len(detections)
        if animal_removed:
            print(f"\n  Animal filter: removed {animal_removed} low-confidence/single-frame "
                  f"animal detections ({before_animal} -> {len(detections)})")

        # Save all detections using parameterized batch insert
        print(f"\n  Saving {len(detections)} detections to DB...")
        t_save = time.time()

        insert_sql = (
            "INSERT INTO frame_detections (video_id, frame_ms, label, x_min, y_min, x_max, y_max, "
            "confidence, frame_width, frame_height, pipeline_version, model_name, run_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        insert_rows = [
            (
                video_id, d["frame_ms"], d["label"],
                d["x_min"], d["y_min"], d["x_max"], d["y_max"],
                d["confidence"], d["frame_width"], d["frame_height"],
                PIPELINE_VERSION, model_name, run_id,
            )
            for d in detections
        ]

        t_build = time.time()
        conn.executemany(insert_sql, insert_rows)
        conn.commit()
        t_insert = time.time()
        total_saved = len(detections)
        print(f"  Saved {total_saved} detections for {video_id}")
        print(f"    Build rows: {(t_build - t_save) * 1000:.0f}ms | Execute: {(t_insert - t_build) * 1000:.0f}ms | Total: {(t_insert - t_save) * 1000:.0f}ms")

        # Per-class breakdown
        label_counts: dict[str, int] = defaultdict(int)
        for det in detections:
            label_counts[det["label"]] += 1
        print(f"\n  Per-class breakdown:")
        for lbl, cnt in sorted(label_counts.items(), key=lambda x: -x[1]):
            print(f"    {lbl:<25} {cnt:>6}")

        # Aggregate detections into time segments
        if detections:
            frame_timestamps = sorted(set(d["frame_ms"] for d in detections))
            if len(frame_timestamps) >= 2:
                gaps = sorted(
                    frame_timestamps[i + 1] - frame_timestamps[i]
                    for i in range(len(frame_timestamps) - 1)
                )
                median_gap = gaps[len(gaps) // 2]
            else:
                median_gap = 1000
            gap_tolerance = int(median_gap * 1.5)
            print(f"\n  Aggregating segments (median gap={median_gap}ms, tolerance={gap_tolerance}ms)...")

            # Group detections by label and merge consecutive frames
            by_label: dict[str, list[dict]] = defaultdict(list)
            for d in detections:
                by_label[d["label"]].append(d)

            segments: list[tuple] = []
            for label, dets in by_label.items():
                dets.sort(key=lambda d: d["frame_ms"])
                seg_start = dets[0]["frame_ms"]
                seg_end = dets[0]["frame_ms"]
                seg_max_conf = dets[0]["confidence"]
                for i in range(1, len(dets)):
                    if dets[i]["frame_ms"] - seg_end <= gap_tolerance:
                        seg_end = dets[i]["frame_ms"]
                        seg_max_conf = max(seg_max_conf, dets[i]["confidence"])
                    else:
                        segments.append((
                            video_id, label, seg_start, seg_end,
                            seg_max_conf, "supported", PIPELINE_VERSION, model_name, run_id,
                        ))
                        seg_start = dets[i]["frame_ms"]
                        seg_end = dets[i]["frame_ms"]
                        seg_max_conf = dets[i]["confidence"]
                segments.append((
                    video_id, label, seg_start, seg_end,
                    seg_max_conf, "supported", PIPELINE_VERSION, model_name, run_id,
                ))

            # Ensure run_id column exists (skip for Turso — schema managed by Node.js)
            if not _is_turso:
                seg_cols = [r[1] for r in conn.execute("PRAGMA table_info(video_detection_segments)").fetchall()]
                if "run_id" not in seg_cols:
                    conn.execute("ALTER TABLE video_detection_segments ADD COLUMN run_id TEXT")
                    conn.commit()

            conn.execute("DELETE FROM video_detection_segments WHERE run_id = ?", (run_id,))
            seg_sql = (
                "INSERT INTO video_detection_segments "
                "(video_id, label, start_ms, end_ms, max_confidence, support_level, pipeline_version, source, run_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            conn.executemany(seg_sql, segments)
            conn.commit()
            unique_labels = set(s[1] for s in segments)
            print(f"  Saved {len(segments)} segments for {len(unique_labels)} labels")
            for label in sorted(unique_labels):
                label_segs = [s for s in segments if s[1] == label]
                for s in label_segs:
                    start_s = s[2] / 1000
                    end_s = s[3] / 1000
                    print(f"    {label:<25} {start_s:.1f}s - {end_s:.1f}s  (conf: {s[4]:.2f})")

        # Scene classification using CLIP
        try:
            scene_attrs = run_scene_classification(frames, conn, video_id, run_id, is_night=is_night)
        except Exception as scene_exc:
            print(f"  [!] Scene classification failed: {scene_exc}")
            scene_attrs = {}

        # Timeline generation (Claude Vision) — disabled for speed
        # generate_timeline(frames, detections, event, conn, video_id, run_id, scene_attrs)

        # Mark as completed and sync so UI sees it immediately
        update_run_status(conn, run_id, "completed", detection_count=total_saved)
        print(f"\n  Run completed: {total_saved} detections")
        if scene_attrs:
            parts = [f"{k}={d['value']} ({d['confidence']:.0%})" for k, d in scene_attrs.items()]
            print(f"  Scene: {', '.join(parts)}")

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        print(f"\nERROR: {error_msg}", file=sys.stderr)
        traceback.print_exc()
        update_run_status(conn, run_id, "failed", last_error=error_msg[:1000])
        return 1

    finally:
        # Sync all local writes to Turso in one batch
        if _is_turso and hasattr(conn, "sync"):
            try:
                print(f"\n  Syncing to Turso...")
                conn.sync()
                print(f"  Synced successfully.")
            except Exception as sync_exc:
                print(f"  [!] Turso sync failed: {sync_exc}")
        conn.close()

    print(f"\n{'=' * 70}")
    print("DONE")
    print(f"  View: http://localhost:3001/event/{video_id}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
