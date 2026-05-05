#!/usr/bin/env python3
"""
Production pipeline — redacts faces and license plates in queued event videos.

Runs the privacy detector on source frames, applies stable ffmpeg redaction
masks without rewriting the source timing model, and uploads video + metadata to
S3.

Usage:
    python3 scripts/prod-pipeline.py
    python3 scripts/prod-pipeline.py --workers 2
    python3 scripts/prod-pipeline.py --limit 10   # process 10 videos then stop

Requires:
    pip install mediapipe boto3
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlparse

import cv2
import requests

try:
    from ultralytics import YOLO as _YOLO
    HAS_YOLO = True
except ImportError:
    HAS_YOLO = False

try:
    import torch
except ImportError:
    pass

try:
    import boto3
except ImportError:
    print("boto3 not installed. Run: pip install boto3")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_detection import (
    get_db, load_api_key, api_request, download_video,
    API_BASE_URL,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "pipeline-video-cache"
LOCAL_METADATA_DIR = PROJECT_ROOT / "data" / "metadata"
PUBLIC_VIDEO_DIR = PROJECT_ROOT / "public" / "videos"
EDITED_EVENTS_DIR = PROJECT_ROOT / "data" / "edited-events"

S3_BUCKET = os.environ.get("PRODUCTION_S3_BUCKET", "hivemapper-blurred-ai-event-videos")
S3_REGION = os.environ.get("PRODUCTION_S3_REGION", "us-west-2")
S3_KEY_PREFIX = os.environ.get("PRODUCTION_S3_PREFIX", "")
SAFE_S3_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")
SAFE_S3_FILENAME_RE = re.compile(r"^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)?$")
LOCAL_EDITED_EVENT_ID_RE = re.compile(r"^[A-Za-z0-9]{24}-.+")

PERSON_LABELS = {"person", "construction worker", "pedestrian"}

# Face detection
FACE_MIN_CONFIDENCE = 0.3
FACE_BOX_PADDING = 0.25
# Only blur faces on persons taller than this (pixels). ~100px = close/medium range.
MIN_PERSON_HEIGHT_PX = 100

# License plate detection
PLATE_MIN_CONFIDENCE = 0.15
PLATE_BOX_PADDING = 0.04
PRIVACY_DETECT_EVERY_N_FRAMES = int(os.environ.get("PRODUCTION_PRIVACY_DETECT_EVERY_N_FRAMES", "1"))
PRIVACY_TRACK_MAX_GAP_FRAMES = int(os.environ.get("PRODUCTION_PRIVACY_TRACK_MAX_GAP_FRAMES", "6"))
PRIVACY_TRACK_IOU_THRESHOLD = float(os.environ.get("PRODUCTION_PRIVACY_TRACK_IOU_THRESHOLD", "0.03"))
PRIVACY_TRACK_CENTER_DISTANCE_FACTOR = float(os.environ.get("PRODUCTION_PRIVACY_TRACK_CENTER_DISTANCE_FACTOR", "2.0"))
PRIVACY_TRACK_SMOOTHING_ALPHA = float(os.environ.get("PRODUCTION_PRIVACY_TRACK_SMOOTHING_ALPHA", "0.9"))
PRIVACY_TRACK_SMOOTH_WINDOW_FRAMES = int(os.environ.get("PRODUCTION_PRIVACY_TRACK_SMOOTH_WINDOW_FRAMES", "3"))
PRIVACY_TRACK_SMOOTH_CURRENT_WEIGHT = float(os.environ.get("PRODUCTION_PRIVACY_TRACK_SMOOTH_CURRENT_WEIGHT", "0.85"))
PRIVACY_FACE_STABILIZED_EXTRA_PADDING = float(os.environ.get("PRODUCTION_PRIVACY_FACE_STABILIZED_EXTRA_PADDING", "0.12"))
PRIVACY_PLATE_STABILIZED_EXTRA_PADDING = float(os.environ.get("PRODUCTION_PRIVACY_PLATE_STABILIZED_EXTRA_PADDING", "0.1"))
PRIVACY_FACE_MIN_WIDTH_PX = int(os.environ.get("PRODUCTION_PRIVACY_FACE_MIN_WIDTH_PX", "28"))
PRIVACY_FACE_MIN_HEIGHT_PX = int(os.environ.get("PRODUCTION_PRIVACY_FACE_MIN_HEIGHT_PX", "28"))
PRIVACY_PLATE_MIN_WIDTH_PX = int(os.environ.get("PRODUCTION_PRIVACY_PLATE_MIN_WIDTH_PX", "24"))
PRIVACY_PLATE_MIN_HEIGHT_PX = int(os.environ.get("PRODUCTION_PRIVACY_PLATE_MIN_HEIGHT_PX", "10"))
PRIVACY_FACE_CORNER_RADIUS_RATIO = float(os.environ.get("PRODUCTION_PRIVACY_FACE_CORNER_RADIUS_RATIO", "0.45"))
PRIVACY_PLATE_CORNER_RADIUS_RATIO = float(os.environ.get("PRODUCTION_PRIVACY_PLATE_CORNER_RADIUS_RATIO", "0.3"))
PRIVACY_REDACTION_FILL_COLOR = os.environ.get("PRODUCTION_PRIVACY_REDACTION_FILL_COLOR", "0x8a8a8a@0.95")
TIMESTAMP_TOLERANCE_SEC = 0.0005
DURATION_TOLERANCE_SEC = 0.01
HEVC_BITRATE_MARGIN = float(os.environ.get("PRODUCTION_HEVC_BITRATE_MARGIN", "1.03"))
MAX_REDACTED_SIZE_RATIO = float(os.environ.get("PRODUCTION_MAX_REDACTED_SIZE_RATIO", "1.35"))
HEVC_QUALITY_RETRY_ATTEMPTS = int(os.environ.get("PRODUCTION_HEVC_QUALITY_RETRY_ATTEMPTS", "2"))
HEVC_QUALITY_RETRY_MIN_MARGIN = float(os.environ.get("PRODUCTION_HEVC_QUALITY_RETRY_MIN_MARGIN", "1.15"))
HEVC_QUALITY_RETRY_MAX_MARGIN = float(os.environ.get("PRODUCTION_HEVC_QUALITY_RETRY_MAX_MARGIN", "1.30"))
LOCAL_TEST_METADATA_HOST = "local-production-quality-test.s3.us-west-2.amazonaws.com"
ALLOW_LOW_PTS_SPREAD_EVENT_IDS = {
    value.strip()
    for value in os.environ.get("PRODUCTION_ALLOW_LOW_PTS_SPREAD_EVENT_IDS", "").split(",")
    if value.strip()
}

# Privacy blur skip thresholds (speeds in mph)
SKIP_SPEED_MIN_ANY_MPH = 35         # skip blur if min speed >= 35 mph (any time)
SKIP_SPEED_MIN_NIGHT_MPH = 20       # skip blur if min speed >= 20 mph at night
SKIP_SPEED_MIN_MOTORWAY_MPH = 30    # skip blur if min speed >= 30 mph on motorway
SKIP_MOTORWAY_ROAD_CLASSES = {"motorway", "trunk"}
PRODUCTION_PRIORITY_MANUAL_VRU = 0
PRODUCTION_PRIORITY_DEFAULT = 100

BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RED = "\033[31m"
RESET = "\033[0m"


def get_machine_id() -> str:
    try:
        return subprocess.check_output(
            ["scutil", "--get", "ComputerName"], timeout=2, stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        pass
    return platform.node().split(".")[0] or "unknown"


MACHINE_ID = get_machine_id()


def get_s3_client():
    return boto3.client("s3", region_name=S3_REGION)


def safe_s3_segment(value: str, label: str) -> str:
    segment = str(value).strip()
    if not segment or not SAFE_S3_SEGMENT_RE.fullmatch(segment):
        raise ValueError(
            f"{label} must contain only letters, numbers, hyphens, and underscores: {value!r}"
        )
    return segment


def safe_s3_filename(value: str, label: str) -> str:
    filename = str(value).strip()
    if not filename or not SAFE_S3_FILENAME_RE.fullmatch(filename):
        raise ValueError(
            f"{label} must contain only letters, numbers, hyphens, underscores, and a file extension dot: {value!r}"
        )
    return filename


def normalize_s3_prefix(prefix: str | None) -> str:
    if not prefix:
        return ""
    return "/".join(
        safe_s3_segment(part, "PRODUCTION_S3_PREFIX segment")
        for part in prefix.split("/")
        if part.strip()
    )


def s3_object_key(filename_segment: str) -> str:
    prefix = normalize_s3_prefix(S3_KEY_PREFIX)
    filename = safe_s3_filename(filename_segment, "production filename")
    return f"{prefix}/{filename}" if prefix else filename


def video_s3_key(video_id: str) -> str:
    return s3_object_key(f"{safe_s3_segment(video_id, 'video_id')}.mp4")


def metadata_s3_key(video_id: str) -> str:
    return s3_object_key(f"{safe_s3_segment(video_id, 'video_id')}.json")


def s3_https_url(key: str) -> str:
    escaped_key = quote(key, safe="/._-")
    return f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{escaped_key}"


def allow_low_pts_spread_for_video(video_id: str) -> bool:
    """Return whether this exact video may bypass the CFR-like PTS spread guard."""
    return video_id in ALLOW_LOW_PTS_SPREAD_EVENT_IDS


def video_already_blurred(s3, video_id: str) -> bool:
    """Check if a blurred video already exists in S3."""
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=video_s3_key(video_id))
        return True
    except s3.exceptions.ClientError:
        return False


def load_local_event(video_id: str) -> dict | None:
    """Load a locally edited/trimmed event from data/metadata when present."""
    if not LOCAL_EDITED_EVENT_ID_RE.fullmatch(video_id):
        return None

    metadata_path = LOCAL_METADATA_DIR / f"{video_id}.json"
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


def load_event_for_production(api_key: str, video_id: str) -> dict:
    """Load event data for production, preferring local edited clip metadata."""
    local_event = load_local_event(video_id)
    if local_event is not None:
        print(f"    Event: using local edited metadata for {video_id}")
        return local_event

    resp = api_request(
        "GET",
        f"{API_BASE_URL}/{video_id}",
        headers={"Authorization": api_key, "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def resolve_local_video_path(video_id: str, video_url: str | None) -> Path | None:
    """Resolve local /videos URLs for edited clips to the synced public file."""
    candidates: list[Path] = []
    if video_url:
        parsed = urlparse(video_url)
        path_part = parsed.path if parsed.scheme in {"http", "https"} else video_url
        if path_part.startswith("/videos/"):
            filename = unquote(Path(path_part).name)
            candidates.append(PUBLIC_VIDEO_DIR / filename)

    candidates.append(PUBLIC_VIDEO_DIR / f"{video_id}.mp4")

    for candidate in candidates:
        try:
            resolved = candidate.resolve()
            resolved.relative_to(PUBLIC_VIDEO_DIR.resolve())
        except ValueError:
            continue
        if resolved.exists():
            return resolved
    return None


def should_cleanup_video_path(path: Path) -> bool:
    """Only delete transient downloads, never local edited/public source clips."""
    try:
        resolved = path.resolve()
        project = PROJECT_ROOT.resolve()
        relative = resolved.relative_to(project)
    except ValueError:
        return True

    if len(relative.parts) >= 2 and relative.parts[:2] == ("public", "videos"):
        return False
    if len(relative.parts) >= 2 and relative.parts[:2] == ("data", "edited-events"):
        return False
    return True


def get_videos_needing_blur(conn, limit: int = 100) -> list[str]:
    """Get only manually-prioritized production queue entries.

    The production worker must not auto-consume the broader eligible queue.
    Priority jobs are explicit production_runs rows with priority 0.
    """
    rows = conn.execute(
        """SELECT pr.video_id
           FROM production_runs pr
           WHERE pr.status = 'queued'
             AND COALESCE(pr.priority, ?) = ?
           ORDER BY pr.created_at ASC
           LIMIT ?""",
        (PRODUCTION_PRIORITY_DEFAULT, PRODUCTION_PRIORITY_MANUAL_VRU, limit),
    ).fetchall()
    return [r[0] if isinstance(r, tuple) else r["video_id"] for r in rows]


def claim_videos_needing_blur(conn, limit: int = 1) -> list[str]:
    """Atomically claim manually-prioritized production queue entries."""
    claimed: list[str] = []

    for _ in range(max(1, limit)):
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        row = conn.execute(
            """UPDATE production_runs
               SET status='processing',
                   started_at=?,
                   last_heartbeat_at=?,
                   machine_id=?
               WHERE video_id = (
                   SELECT video_id
                   FROM production_runs
                   WHERE status='queued'
                     AND COALESCE(priority, ?) = ?
                   ORDER BY created_at ASC
                   LIMIT 1
               )
                 AND status='queued'
               RETURNING video_id""",
            (
                now,
                now,
                MACHINE_ID,
                PRODUCTION_PRIORITY_DEFAULT,
                PRODUCTION_PRIORITY_MANUAL_VRU,
            ),
        ).fetchone()
        conn.commit()
        if not row:
            break
        claimed.append(row[0] if isinstance(row, tuple) else row["video_id"])

    return claimed


def get_person_detections(conn, video_id: str) -> list[dict]:
    """Get all person detections for a video."""
    rows = conn.execute(
        """SELECT frame_ms, label, x_min, y_min, x_max, y_max,
                  confidence, frame_width, frame_height
           FROM frame_detections
           WHERE video_id = ? AND label IN ('person', 'construction worker', 'pedestrian')
           ORDER BY frame_ms""",
        (video_id,),
    ).fetchall()
    return [
        {
            "frame_ms": r[0], "label": r[1],
            "x_min": r[2], "y_min": r[3], "x_max": r[4], "y_max": r[5],
            "confidence": r[6], "frame_width": r[7], "frame_height": r[8],
        }
        for r in rows
    ]


def get_triage_info(conn, video_id: str) -> dict | None:
    """Get triage data for a video (speed, location, timestamp)."""
    row = conn.execute(
        """SELECT speed_min, lat, lon, event_timestamp, road_class
           FROM triage_results WHERE id = ?""",
        (video_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "speed_min": row[0], "lat": row[1], "lon": row[2],
        "event_timestamp": row[3], "road_class": row[4],
    }


def check_skip_reason(conn, video_id: str) -> str | None:
    """Check if a video can skip privacy blurring based on speed/time-of-day.
    Returns a skip reason string, or None if blurring is needed."""
    triage = get_triage_info(conn, video_id)
    if not triage or triage["speed_min"] is None:
        return None

    speed_min_mph = triage["speed_min"]

    # Rule 1: Any video with continuous speed >= 50 mph
    if speed_min_mph >= SKIP_SPEED_MIN_ANY_MPH:
        return f"speed_min={speed_min_mph:.0f}mph >= {SKIP_SPEED_MIN_ANY_MPH}mph"

    # Rule 2: Motorway/trunk with continuous speed >= 40 mph
    road_class = triage.get("road_class") or ""
    if speed_min_mph >= SKIP_SPEED_MIN_MOTORWAY_MPH and road_class in SKIP_MOTORWAY_ROAD_CLASSES:
        return f"{road_class} + speed_min={speed_min_mph:.0f}mph >= {SKIP_SPEED_MIN_MOTORWAY_MPH}mph"

    # Rule 3: Night-time video with continuous speed >= 40 mph
    if speed_min_mph >= SKIP_SPEED_MIN_NIGHT_MPH:
        lat, lon, ts = triage["lat"], triage["lon"], triage["event_timestamp"]
        if lat is not None and lon is not None and ts:
            mod = _get_export_metadata_module()
            tod_info = mod.get_time_of_day(ts, lat, lon)
            if tod_info and tod_info.get("timeOfDay") == "Night":
                return f"night + speed_min={speed_min_mph:.0f}mph >= {SKIP_SPEED_MIN_NIGHT_MPH}mph"

    return None


# Privacy detection model (faces + license plates in one pass)
_privacy_model = None
_privacy_model_failed = False

PRIVACY_MODEL_PATH = PROJECT_ROOT / "data" / "models" / "privacy_v8_all.pt"
# Class IDs in the privacy model: 0=face, 2=license-plate
PRIVACY_FACE_CLS = 0
PRIVACY_PLATE_CLS = 2


def _get_privacy_model():
    global _privacy_model, _privacy_model_failed
    if _privacy_model_failed:
        return None
    if _privacy_model is None:
        if not PRIVACY_MODEL_PATH.exists():
            print(f"    {YELLOW}Privacy model not found: {PRIVACY_MODEL_PATH}{RESET}")
            _privacy_model_failed = True
            return None
        try:
            print(f"    Loading privacy model (face + plate)...")
            _privacy_model = _YOLO(str(PRIVACY_MODEL_PATH))
            print(f"    Privacy model loaded")
        except Exception as e:
            print(f"    {YELLOW}Privacy model failed: {e}{RESET}")
            _privacy_model_failed = True
            return None
    return _privacy_model


def run_json_command(cmd: list[str], timeout: int = 120) -> dict[str, Any]:
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"{cmd[0]} failed: {stderr}")
    return json.loads(result.stdout or "{}")


def probe_video(video_path: Path, include_packets: bool = True) -> dict[str, Any]:
    entries = (
        "format=filename,format_name,duration,size,bit_rate:"
        "stream=index,codec_type,codec_name,codec_tag_string,profile,pix_fmt,width,height,avg_frame_rate,r_frame_rate,"
        "time_base,duration,bit_rate,nb_frames"
    )
    if include_packets:
        entries = entries + ":packet=pts_time,dts_time,duration_time,flags,size,pos"
        cmd = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_packets",
            "-show_entries",
            entries,
            "-of", "json",
            str(video_path),
        ]
    else:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", entries,
            "-of", "json",
            str(video_path),
        ]
    return run_json_command(cmd, timeout=180)


def first_video_stream(probe: dict[str, Any]) -> dict[str, Any]:
    for stream in probe.get("streams", []):
        if stream.get("codec_type") == "video":
            return stream
    return {}


def packet_times(probe: dict[str, Any]) -> tuple[list[float], list[float]]:
    pts: list[float] = []
    durations: list[float] = []
    for packet in probe.get("packets", []):
        if "pts_time" not in packet:
            continue
        pts.append(float(packet["pts_time"]))
        durations.append(float(packet.get("duration_time") or 0))
    if pts:
        origin = min(pts)
        pts = [round(value - origin, 6) for value in pts]
    return pts, durations


def sequence_max_delta(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right):
        return None
    if not left:
        return 0
    return max(abs(a - b) for a, b in zip(left, right))


def timestamp_intervals(timestamps: list[float]) -> list[float]:
    return [
        round(timestamps[index + 1] - timestamps[index], 6)
        for index in range(len(timestamps) - 1)
    ]


def rounded_unique(values: list[float]) -> list[float]:
    return sorted(set(round(value, 6) for value in values))


def probe_frame_timing(video_path: Path) -> list[dict[str, float]]:
    data = run_json_command(
        [
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_frames",
            "-show_entries",
            "frame=best_effort_timestamp_time,pkt_pts_time,pkt_duration_time",
            "-of", "json",
            str(video_path),
        ],
        timeout=180,
    )
    frames = data.get("frames", [])
    if not frames:
        raise RuntimeError("ffprobe returned no video frame timing")

    raw_times: list[float] = []
    raw_durations: list[float | None] = []
    for frame in frames:
        timestamp = frame.get("best_effort_timestamp_time") or frame.get("pkt_pts_time")
        if timestamp is None:
            raise RuntimeError("video frame is missing source timestamp")
        raw_times.append(float(timestamp))
        duration = frame.get("pkt_duration_time")
        raw_durations.append(float(duration) if duration is not None else None)

    origin = raw_times[0]
    timings: list[dict[str, float]] = []
    for index, timestamp in enumerate(raw_times):
        duration = raw_durations[index]
        if duration is None or duration <= 0:
            if index + 1 < len(raw_times):
                duration = max(raw_times[index + 1] - timestamp, 0)
            elif index > 0:
                duration = max(timestamp - raw_times[index - 1], 0)
            else:
                duration = 0
        timings.append({
            "time_s": timestamp - origin,
            "duration_s": duration,
        })
    return timings


def detect_faces_and_plates(video_path: Path) -> tuple[list[dict], list[dict]]:
    """Run privacy model on source frames to detect faces and license plates.
    Returns raw (face_boxes, plate_boxes); the blur stage stabilizes them."""
    model = _get_privacy_model()
    if model is None:
        raise RuntimeError("Privacy model unavailable; refusing to produce unredacted video")

    frame_timing = probe_frame_timing(video_path)

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        raise RuntimeError(f"OpenCV could not read {video_path}")

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if total_frames <= 0:
        raise RuntimeError("OpenCV returned no video frames")
    if len(frame_timing) < total_frames:
        raise RuntimeError(
            f"ffprobe frame timing count ({len(frame_timing)}) is less than OpenCV frame count ({total_frames})"
        )
    face_boxes = []
    plate_boxes = []

    detect_stride = max(1, PRIVACY_DETECT_EVERY_N_FRAMES)
    model_confidence = min(FACE_MIN_CONFIDENCE, PLATE_MIN_CONFIDENCE)

    # Read sequentially; seeking is slow on HEVC and can disturb frame order.
    for i in range(total_frames):
        ret, frame = cap.read()
        if not ret:
            break
        if i % detect_stride != 0:
            continue

        h, w = frame.shape[:2]
        timing = frame_timing[i]
        frame_ms = int(round(timing["time_s"] * 1000))
        start_s = timing["time_s"]
        end_s = timing["time_s"] + max(timing["duration_s"], 0.001)

        results = model.predict(frame, conf=model_confidence, imgsz=640, verbose=False)
        for result in results:
            for box in result.boxes:
                cls = int(box.cls[0]) if box.cls is not None else -1
                if cls not in (PRIVACY_FACE_CLS, PRIVACY_PLATE_CLS):
                    continue
                confidence = float(box.conf[0]) if box.conf is not None else 1.0

                x1, y1, x2, y2 = box.xyxy[0].tolist()
                bw = x2 - x1
                bh = y2 - y1

                if cls == PRIVACY_FACE_CLS:
                    if confidence < FACE_MIN_CONFIDENCE:
                        continue
                    # Skip tiny faces (< ~15px)
                    if bh < 15:
                        continue
                    pad_x = bw * FACE_BOX_PADDING
                    pad_y = bh * FACE_BOX_PADDING
                    bx = max(0, x1 - pad_x)
                    by = max(0, y1 - pad_y)
                    pw = min(w - bx, bw + 2 * pad_x)
                    ph = min(h - by, bh + 2 * pad_y)
                    face_boxes.append({
                        "label": "face",
                        "frame_index": i,
                        "frame_ms": frame_ms,
                        "start_s": start_s,
                        "end_s": end_s,
                        "x": int(bx), "y": int(by),
                        "w": int(pw), "h": int(ph),
                        "confidence": confidence,
                    })

                elif cls == PRIVACY_PLATE_CLS:
                    if confidence < PLATE_MIN_CONFIDENCE:
                        continue
                    pad_x = bw * PLATE_BOX_PADDING
                    pad_y = bh * PLATE_BOX_PADDING
                    bx = max(0, x1 - pad_x)
                    by = max(0, y1 - pad_y)
                    pw = min(w - bx, bw + 2 * pad_x)
                    ph = min(h - by, bh + 2 * pad_y)
                    plate_boxes.append({
                        "label": "plate",
                        "frame_index": i,
                        "frame_ms": frame_ms,
                        "start_s": start_s,
                        "end_s": end_s,
                        "x": int(bx), "y": int(by),
                        "w": int(pw), "h": int(ph),
                        "confidence": confidence,
                    })

    cap.release()
    return face_boxes, plate_boxes


def _nearest_frame_index(frame_timing: list[dict[str, float]], time_s: float) -> int:
    if not frame_timing:
        return 0
    return min(
        range(len(frame_timing)),
        key=lambda index: abs(frame_timing[index]["time_s"] - time_s),
    )


def _box_xyxy(box: dict) -> tuple[float, float, float, float]:
    x1 = float(box["x"])
    y1 = float(box["y"])
    return x1, y1, x1 + float(box["w"]), y1 + float(box["h"])


def _box_iou(left: dict, right: dict) -> float:
    left_x1, left_y1, left_x2, left_y2 = _box_xyxy(left)
    right_x1, right_y1, right_x2, right_y2 = _box_xyxy(right)
    inter_x1 = max(left_x1, right_x1)
    inter_y1 = max(left_y1, right_y1)
    inter_x2 = min(left_x2, right_x2)
    inter_y2 = min(left_y2, right_y2)
    inter_w = max(0.0, inter_x2 - inter_x1)
    inter_h = max(0.0, inter_y2 - inter_y1)
    intersection = inter_w * inter_h
    if intersection <= 0:
        return 0.0
    left_area = max(0.0, left_x2 - left_x1) * max(0.0, left_y2 - left_y1)
    right_area = max(0.0, right_x2 - right_x1) * max(0.0, right_y2 - right_y1)
    union = left_area + right_area - intersection
    return intersection / union if union > 0 else 0.0


def _box_center_distance(left: dict, right: dict) -> float:
    left_cx = float(left["x"]) + float(left["w"]) / 2
    left_cy = float(left["y"]) + float(left["h"]) / 2
    right_cx = float(right["x"]) + float(right["w"]) / 2
    right_cy = float(right["y"]) + float(right["h"]) / 2
    return ((left_cx - right_cx) ** 2 + (left_cy - right_cy) ** 2) ** 0.5


def _track_match_score(last_box: dict, candidate: dict) -> float | None:
    iou = _box_iou(last_box, candidate)
    distance = _box_center_distance(last_box, candidate)
    reference_size = max(
        float(last_box["w"]),
        float(last_box["h"]),
        float(candidate["w"]),
        float(candidate["h"]),
        1.0,
    )
    max_distance = max(24.0, reference_size * PRIVACY_TRACK_CENTER_DISTANCE_FACTOR)
    if iou < PRIVACY_TRACK_IOU_THRESHOLD and distance > max_distance:
        return None
    distance_bonus = max(0.0, 1.0 - (distance / max_distance)) * 0.05
    return iou + distance_bonus


def _is_same_frame_duplicate(left: dict, right: dict) -> bool:
    iou = _box_iou(left, right)
    if iou >= 0.15:
        return True
    distance = _box_center_distance(left, right)
    reference_size = max(
        float(left["w"]),
        float(left["h"]),
        float(right["w"]),
        float(right["h"]),
        1.0,
    )
    return distance <= reference_size * 0.5


def _dedupe_privacy_boxes_per_frame(boxes: list[dict]) -> list[dict]:
    grouped: dict[tuple[int, str], list[dict]] = {}
    for box in boxes:
        key = (int(box["frame_index"]), str(box["label"]))
        grouped.setdefault(key, []).append(box)

    deduped: list[dict] = []
    for group in grouped.values():
        kept: list[dict] = []
        for box in sorted(group, key=lambda item: float(item.get("confidence", 0)), reverse=True):
            if any(_is_same_frame_duplicate(box, existing) for existing in kept):
                continue
            kept.append(box)
        deduped.extend(kept)
    return deduped


def _lerp(left: float, right: float, ratio: float) -> float:
    return left + (right - left) * ratio


def _smooth_privacy_track(boxes: list[dict]) -> list[dict]:
    smoothed: list[dict] = []
    previous: dict | None = None
    previous_frame: int | None = None
    alpha = max(0.0, min(1.0, PRIVACY_TRACK_SMOOTHING_ALPHA))
    for box in sorted(boxes, key=lambda item: int(item["frame_index"])):
        frame_index = int(box["frame_index"])
        if (
            previous is None
            or previous_frame is None
            or frame_index - previous_frame > PRIVACY_TRACK_MAX_GAP_FRAMES
        ):
            current = dict(box)
        else:
            current = dict(box)
            for key in ("x", "y", "w", "h"):
                current[key] = _lerp(float(previous[key]), float(box[key]), alpha)
        smoothed.append(current)
        previous = current
        previous_frame = frame_index
    return smoothed


def _fill_privacy_track_gaps(boxes: list[dict]) -> list[dict]:
    if not boxes:
        return []
    filled: list[dict] = [dict(boxes[0])]
    previous = boxes[0]
    for current in boxes[1:]:
        previous_frame = int(previous["frame_index"])
        current_frame = int(current["frame_index"])
        gap = current_frame - previous_frame
        if 1 < gap <= PRIVACY_TRACK_MAX_GAP_FRAMES:
            for frame_index in range(previous_frame + 1, current_frame):
                ratio = (frame_index - previous_frame) / gap
                interpolated = dict(current)
                interpolated["frame_index"] = frame_index
                interpolated["interpolated"] = True
                for key in ("x", "y", "w", "h"):
                    interpolated[key] = _lerp(float(previous[key]), float(current[key]), ratio)
                filled.append(interpolated)
        if gap > 0:
            filled.append(dict(current))
        previous = current
    return filled


def _center_smooth_privacy_track(boxes: list[dict]) -> list[dict]:
    if not boxes:
        return []
    window = max(1, PRIVACY_TRACK_SMOOTH_WINDOW_FRAMES)
    if window <= 1:
        return [dict(box) for box in boxes]
    radius = window // 2
    ordered = sorted(boxes, key=lambda item: int(item["frame_index"]))
    smoothed: list[dict] = []
    for index, box in enumerate(ordered):
        frame_index = int(box["frame_index"])
        neighbors = [
            candidate
            for candidate in ordered[max(0, index - radius): index + radius + 1]
            if abs(int(candidate["frame_index"]) - frame_index) <= radius
        ]
        if not neighbors:
            smoothed.append(dict(box))
            continue
        current = dict(box)
        current_weight = max(0.0, min(1.0, PRIVACY_TRACK_SMOOTH_CURRENT_WEIGHT))
        other_neighbors = [candidate for candidate in neighbors if candidate is not box]
        for key in ("x", "y", "w", "h"):
            if not other_neighbors:
                current[key] = float(box[key])
                continue
            other_weight = (1.0 - current_weight) / len(other_neighbors)
            current[key] = (
                float(box[key]) * current_weight
                + sum(float(candidate[key]) * other_weight for candidate in other_neighbors)
            )
        smoothed.append(current)
    return smoothed


def _union_privacy_boxes(boxes: list[dict]) -> dict:
    x1 = min(float(box["x"]) for box in boxes)
    y1 = min(float(box["y"]) for box in boxes)
    x2 = max(float(box["x"]) + float(box["w"]) for box in boxes)
    y2 = max(float(box["y"]) + float(box["h"]) for box in boxes)
    return {
        **boxes[0],
        "x": x1,
        "y": y1,
        "w": x2 - x1,
        "h": y2 - y1,
    }


def _expand_privacy_box(box: dict) -> dict:
    x = float(box["x"])
    y = float(box["y"])
    w = float(box["w"])
    h = float(box["h"])
    cx = x + w / 2
    cy = y + h / 2
    label = str(box.get("label") or "")
    if label == "face":
        min_w = float(PRIVACY_FACE_MIN_WIDTH_PX)
        min_h = float(PRIVACY_FACE_MIN_HEIGHT_PX)
        extra_padding = PRIVACY_FACE_STABILIZED_EXTRA_PADDING
    else:
        min_w = float(PRIVACY_PLATE_MIN_WIDTH_PX)
        min_h = float(PRIVACY_PLATE_MIN_HEIGHT_PX)
        extra_padding = PRIVACY_PLATE_STABILIZED_EXTRA_PADDING
    w = max(w, min_w)
    h = max(h, min_h)
    x = cx - w / 2
    y = cy - h / 2
    pad_x = max(1.0, w * extra_padding)
    pad_y = max(1.0, h * extra_padding)
    return {
        **box,
        "x": x - pad_x,
        "y": y - pad_y,
        "w": w + 2 * pad_x,
        "h": h + 2 * pad_y,
    }


def build_privacy_tracks(
    raw_boxes: list[dict],
    frame_timing: list[dict[str, float]],
) -> list[dict[str, Any]]:
    """Track, interpolate, and smooth raw privacy detections before redaction."""
    if not raw_boxes:
        return []

    normalized: list[dict] = []
    max_frame_index = len(frame_timing) - 1
    for raw_box in raw_boxes:
        if float(raw_box.get("w", 0)) <= 0 or float(raw_box.get("h", 0)) <= 0:
            continue
        frame_index = raw_box.get("frame_index")
        if frame_index is None:
            frame_index = _nearest_frame_index(
                frame_timing,
                float(raw_box.get("frame_ms", 0)) / 1000,
            )
        frame_index = int(frame_index)
        if frame_index < 0 or frame_index > max_frame_index:
            continue
        normalized.append({
            **raw_box,
            "label": str(raw_box.get("label") or "privacy"),
            "frame_index": frame_index,
        })
    normalized = _dedupe_privacy_boxes_per_frame(normalized)

    tracks: list[dict[str, Any]] = []
    for box in sorted(normalized, key=lambda item: (int(item["frame_index"]), str(item["label"]))):
        best_track: dict[str, Any] | None = None
        best_score: float | None = None
        box_frame = int(box["frame_index"])
        for track in tracks:
            if track["label"] != box["label"]:
                continue
            gap = box_frame - int(track["last_frame_index"])
            if gap <= 0 or gap > PRIVACY_TRACK_MAX_GAP_FRAMES:
                continue
            score = _track_match_score(track["last_box"], box)
            if score is None:
                continue
            if best_score is None or score > best_score:
                best_score = score
                best_track = track

        if best_track is None:
            tracks.append({
                "label": box["label"],
                "boxes": [box],
                "last_box": box,
                "last_frame_index": box_frame,
            })
        else:
            best_track["boxes"].append(box)
            best_track["last_box"] = box
            best_track["last_frame_index"] = box_frame

    stabilized_tracks: list[dict[str, Any]] = []
    for track_id, track in enumerate(tracks):
        smoothed = _smooth_privacy_track(track["boxes"])
        filled = _fill_privacy_track_gaps(smoothed)
        centered = _center_smooth_privacy_track(filled)
        stabilized_tracks.append({
            "id": track_id,
            "label": track["label"],
            "boxes": sorted(centered, key=lambda item: int(item["frame_index"])),
        })
    return stabilized_tracks


def stabilize_privacy_boxes(
    raw_boxes: list[dict],
    frame_timing: list[dict[str, float]],
) -> list[dict]:
    """Return per-frame stabilized boxes used to build the privacy mask."""
    stabilized: list[dict] = []
    for track in build_privacy_tracks(raw_boxes, frame_timing):
        for box in track["boxes"]:
            frame_index = int(box["frame_index"])
            timing = frame_timing[frame_index]
            stabilized.append({
                **box,
                "track_id": track["id"],
                "frame_ms": int(round(float(timing["time_s"]) * 1000)),
                "start_s": float(timing["time_s"]),
                "end_s": float(timing["time_s"]) + max(float(timing["duration_s"]), 0.001),
            })
    return sorted(
        stabilized,
        key=lambda box: (int(box["frame_index"]), str(box.get("label", "")), int(box.get("track_id", 0))),
    )


def has_hevc_encoder() -> bool:
    """Check whether ffmpeg has the required HEVC encoder."""
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-h", "encoder=libx265"],
            capture_output=True, text=True, timeout=10,
        )
        return result.returncode == 0 and "Encoder libx265" in result.stdout
    except Exception:
        return False


def video_bitrate_bps(probe: dict[str, Any]) -> int:
    format_info = probe.get("format", {})
    stream = first_video_stream(probe)
    for value in (format_info.get("bit_rate"), stream.get("bit_rate")):
        if value:
            return int(float(value))

    size = int(format_info.get("size") or 0)
    duration = float(format_info.get("duration") or 0)
    if size > 0 and duration > 0:
        return int((size * 8) / duration)
    return 0


def target_hevc_bitrate_bps(source_probe: dict[str, Any]) -> int:
    source_bitrate = video_bitrate_bps(source_probe)
    if source_bitrate <= 0:
        raise RuntimeError("Could not determine source bitrate for HEVC production encode")
    return max(source_bitrate, int(source_bitrate * HEVC_BITRATE_MARGIN))


def _clamp_box(box: dict, width: int, height: int) -> dict | None:
    if width < 4 or height < 4:
        return None
    x = max(1, min(int(box["x"]), width - 2))
    y = max(1, min(int(box["y"]), height - 2))
    w = max(1, min(int(box["w"]), width - x - 1))
    h = max(1, min(int(box["h"]), height - y - 1))
    if w <= 0 or h <= 0:
        return None
    return {
        **box,
        "x": x,
        "y": y,
        "w": w,
        "h": h,
        "start_s": max(0.0, float(box.get("start_s", box["frame_ms"] / 1000))),
        "end_s": max(0.001, float(box.get("end_s", box["frame_ms"] / 1000 + 0.1))),
    }


def ass_time(seconds: float, *, ceil: bool = False) -> str:
    """Format seconds as ASS subtitle time, using centisecond precision."""
    import math

    safe_seconds = max(0.0, seconds)
    raw_centiseconds = safe_seconds * 100
    centiseconds = (
        int(math.ceil(raw_centiseconds - 1e-9))
        if ceil
        else int(math.floor(raw_centiseconds + 1e-9))
    )
    hours, remainder = divmod(centiseconds, 360000)
    minutes, remainder = divmod(remainder, 6000)
    whole_seconds, centiseconds = divmod(remainder, 100)
    return f"{hours}:{minutes:02d}:{whole_seconds:02d}.{centiseconds:02d}"


def ass_privacy_color_and_alpha(fill_color: str) -> tuple[str, str]:
    """Convert ffmpeg 0xRRGGBB@opacity into ASS BGR color and alpha values."""
    color_part, _, opacity_part = fill_color.partition("@")
    color_part = color_part.strip()
    if color_part.startswith("0x"):
        color_part = color_part[2:]
    if len(color_part) != 6:
        raise ValueError(f"Unsupported privacy fill color: {fill_color!r}")
    red = int(color_part[0:2], 16)
    green = int(color_part[2:4], 16)
    blue = int(color_part[4:6], 16)
    opacity = float(opacity_part) if opacity_part else 1.0
    opacity = max(0.0, min(1.0, opacity))
    alpha = int(round((1.0 - opacity) * 255))
    return f"&H{blue:02X}{green:02X}{red:02X}&", f"&H{alpha:02X}&"


def rounded_rectangle_ass_path(width: int, height: int, radius: int) -> str:
    """Return an ASS vector drawing path for a rounded rectangle."""
    w = max(1, int(width))
    h = max(1, int(height))
    max_radius = max(0, min(w, h) // 2)
    r = max(0, min(int(radius), max_radius))
    if r <= 1:
        return f"m 0 0 l {w} 0 l {w} {h} l 0 {h} l 0 0"

    # Cubic bezier approximation for quarter-circle corners.
    c = max(1, int(round(r * 0.5522847498)))
    return " ".join([
        f"m {r} 0",
        f"l {w - r} 0",
        f"b {w - r + c} 0 {w} {r - c} {w} {r}",
        f"l {w} {h - r}",
        f"b {w} {h - r + c} {w - r + c} {h} {w - r} {h}",
        f"l {r} {h}",
        f"b {r - c} {h} 0 {h - r + c} 0 {h - r}",
        f"l 0 {r}",
        f"b 0 {r - c} {r - c} 0 {r} 0",
    ])


def privacy_box_corner_radius(box: dict) -> int:
    ratio = (
        PRIVACY_FACE_CORNER_RADIUS_RATIO
        if str(box.get("label") or "") == "face"
        else PRIVACY_PLATE_CORNER_RADIUS_RATIO
    )
    return int(round(min(int(box["w"]), int(box["h"])) * ratio))


def build_privacy_ass_script(privacy_boxes: list[dict], width: int, height: int) -> tuple[str, int]:
    """Build ASS vector overlays with real rounded corners for privacy masks."""
    color, alpha = ass_privacy_color_and_alpha(PRIVACY_REDACTION_FILL_COLOR)
    lines = [
        "[Script Info]",
        "ScriptType: v4.00+",
        f"PlayResX: {width}",
        f"PlayResY: {height}",
        "ScaledBorderAndShadow: yes",
        "WrapStyle: 0",
        "",
        "[V4+ Styles]",
        (
            "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
            "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
            "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
            "Alignment, MarginL, MarginR, MarginV, Encoding"
        ),
        (
            "Style: Privacy,Arial,20,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,"
            "0,0,0,0,100,100,0,0,1,0,0,7,0,0,0,1"
        ),
        "",
        "[Events]",
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
    ]

    redaction_count = 0
    for raw_box in privacy_boxes:
        box = _clamp_box(_expand_privacy_box(raw_box), width, height)
        if box is None:
            continue
        x, y, w, h = int(box["x"]), int(box["y"]), int(box["w"]), int(box["h"])
        if min(w, h) < 3:
            continue
        start_s = float(box["start_s"])
        end_s = max(float(box["end_s"]), start_s + 0.001)
        start = ass_time(start_s)
        end = ass_time(end_s, ceil=True)
        if start == end:
            end = ass_time(start_s + 0.01, ceil=True)
        radius = privacy_box_corner_radius(box)
        path = rounded_rectangle_ass_path(w, h, radius)
        drawing = (
            rf"{{\an7\pos({x},{y})\bord0\shad0\1c{color}\alpha{alpha}\p1}}"
            f"{path}"
        )
        lines.append(f"Dialogue: 0,{start},{end},Privacy,,0,0,0,,{drawing}")
        redaction_count += 1

    if redaction_count == 0:
        raise RuntimeError("No valid privacy boxes after clamping")
    return "\n".join(lines) + "\n", redaction_count


def build_ass_redaction_filter(ass_path: Path, width: int, height: int) -> tuple[str, str]:
    """Build the single-input ffmpeg filter that burns ASS vector masks."""
    escaped_path = str(ass_path).replace("\\", "\\\\").replace("'", r"\'")
    return f"[0:v]ass=filename='{escaped_path}':original_size={width}x{height}[v]", "v"


def build_quality_hevc_ffmpeg_command(
    video_path: Path,
    output_path: Path,
    filter_complex: str | None,
    output_label: str,
    target_bitrate_bps: int,
    filter_complex_script: Path | None = None,
    force_cbr: bool = False,
) -> list[str]:
    maxrate_bps = target_bitrate_bps if force_cbr else int(target_bitrate_bps * 1.25)
    bufsize_bps = int(target_bitrate_bps * 2)
    if filter_complex_script is not None:
        filter_args = ["-filter_complex_script", str(filter_complex_script)]
    elif filter_complex:
        filter_args = ["-filter_complex", filter_complex]
    else:
        raise ValueError("filter_complex or filter_complex_script is required")
    command = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-i", str(video_path),
        *filter_args,
        "-map", f"[{output_label}]",
        "-map", "0:a?",
        "-c:v", "libx265",
        "-preset", "slow",
        "-b:v", str(target_bitrate_bps),
    ]
    if force_cbr:
        command.extend([
            "-minrate", str(target_bitrate_bps),
            "-maxrate", str(maxrate_bps),
            "-bufsize", str(bufsize_bps),
            "-x265-params", "nal-hrd=cbr:filler=1",
        ])
    else:
        command.extend([
            "-maxrate", str(maxrate_bps),
            "-bufsize", str(bufsize_bps),
        ])
    command.extend([
        "-tag:v", "hvc1",
        "-pix_fmt", "yuv420p",
        "-fps_mode", "passthrough",
        "-enc_time_base", "-1",
        "-c:a", "copy",
        "-map_metadata", "0",
        "-movflags", "+faststart",
        str(output_path),
    ])
    return command


def validate_production_video(
    source_path: Path,
    produced_path: Path,
    *,
    redaction_applied: bool,
) -> dict[str, Any]:
    source_probe = probe_video(source_path)
    produced_probe = probe_video(produced_path)
    source_stream = first_video_stream(source_probe)
    produced_stream = first_video_stream(produced_probe)
    source_pts, source_durations = packet_times(source_probe)
    produced_pts, produced_durations = packet_times(produced_probe)
    source_packet_display_pts = sorted(source_pts)
    produced_packet_display_pts = sorted(produced_pts)
    source_packet_intervals = timestamp_intervals(source_packet_display_pts)
    produced_packet_intervals = timestamp_intervals(produced_packet_display_pts)
    source_frame_timing = probe_frame_timing(source_path)
    produced_frame_timing = probe_frame_timing(produced_path)
    source_frame_pts = [round(frame["time_s"], 6) for frame in source_frame_timing]
    produced_frame_pts = [round(frame["time_s"], 6) for frame in produced_frame_timing]
    source_frame_intervals = timestamp_intervals(source_frame_pts)
    produced_frame_intervals = timestamp_intervals(produced_frame_pts)

    packet_pts_delta = sequence_max_delta(source_packet_display_pts, produced_packet_display_pts)
    packet_interval_delta = sequence_max_delta(source_packet_intervals, produced_packet_intervals)
    packet_duration_metadata_delta = sequence_max_delta(
        sorted(source_durations),
        sorted(produced_durations),
    )
    frame_pts_delta = sequence_max_delta(source_frame_pts, produced_frame_pts)
    frame_interval_delta = sequence_max_delta(source_frame_intervals, produced_frame_intervals)
    source_size = int(source_probe.get("format", {}).get("size") or 0)
    produced_size = int(produced_probe.get("format", {}).get("size") or 0)
    source_bitrate = video_bitrate_bps(source_probe)
    produced_bitrate = video_bitrate_bps(produced_probe)
    source_duration = float(source_probe.get("format", {}).get("duration") or 0)
    produced_duration = float(produced_probe.get("format", {}).get("duration") or 0)
    source_frame_count = len(source_pts)
    produced_frame_count = len(produced_pts)
    source_frame_timing_count = len(source_frame_timing)
    produced_frame_timing_count = len(produced_frame_timing)

    codec_ok = (
        produced_stream.get("codec_name") == "hevc"
        if redaction_applied
        else produced_stream.get("codec_name") == source_stream.get("codec_name")
    )
    frame_count_ok = source_frame_count > 0 and source_frame_count == produced_frame_count
    frame_timing_count_ok = (
        source_frame_timing_count > 0
        and source_frame_timing_count == produced_frame_timing_count
    )
    packet_pts_ok = packet_pts_delta is not None and packet_pts_delta <= TIMESTAMP_TOLERANCE_SEC
    packet_intervals_ok = (
        packet_interval_delta is not None
        and packet_interval_delta <= TIMESTAMP_TOLERANCE_SEC
    )
    frame_pts_ok = frame_pts_delta is not None and frame_pts_delta <= TIMESTAMP_TOLERANCE_SEC
    frame_intervals_ok = (
        frame_interval_delta is not None
        and frame_interval_delta <= TIMESTAMP_TOLERANCE_SEC
    )
    duration_ok = abs(source_duration - produced_duration) <= DURATION_TOLERANCE_SEC
    no_cfr_rewrite = frame_pts_ok and frame_intervals_ok and packet_pts_ok and packet_intervals_ok
    size_ratio = (produced_size / source_size) if source_size else None
    bitrate_at_least_source_ok = produced_bitrate >= source_bitrate if source_bitrate > 0 else False
    size_close_to_source_ok = (
        size_ratio is not None
        and size_ratio >= 0.95
        and (
            not redaction_applied
            or size_ratio <= MAX_REDACTED_SIZE_RATIO
        )
    )
    quality_path_ok = (
        produced_stream.get("codec_name") == "hevc"
        and bitrate_at_least_source_ok
        and size_close_to_source_ok
    ) if redaction_applied else True

    comparison = {
        "passed": (
            codec_ok
            and frame_count_ok
            and frame_timing_count_ok
            and packet_pts_ok
            and packet_intervals_ok
            and frame_pts_ok
            and frame_intervals_ok
            and duration_ok
            and quality_path_ok
        ),
        "redactionApplied": redaction_applied,
        "source": {
            "path": str(source_path),
            "codec": source_stream.get("codec_name"),
            "frameCount": source_frame_count,
            "frameTimingCount": source_frame_timing_count,
            "durationSec": source_duration,
            "sizeBytes": source_size,
            "bitrateBps": source_bitrate,
            "uniquePacketDurationsSec": rounded_unique(source_durations),
            "uniquePacketIntervalsSec": rounded_unique(source_packet_intervals),
            "uniqueFrameIntervalsSec": rounded_unique(source_frame_intervals),
        },
        "produced": {
            "path": str(produced_path),
            "codec": produced_stream.get("codec_name"),
            "frameCount": produced_frame_count,
            "frameTimingCount": produced_frame_timing_count,
            "durationSec": produced_duration,
            "sizeBytes": produced_size,
            "bitrateBps": produced_bitrate,
            "uniquePacketDurationsSec": rounded_unique(produced_durations),
            "uniquePacketIntervalsSec": rounded_unique(produced_packet_intervals),
            "uniqueFrameIntervalsSec": rounded_unique(produced_frame_intervals),
        },
        "checks": {
            "codecOk": codec_ok,
            "frameCountOk": frame_count_ok,
            "frameTimingCountOk": frame_timing_count_ok,
            "packetPtsOk": packet_pts_ok,
            "packetIntervalsOk": packet_intervals_ok,
            "framePtsOk": frame_pts_ok,
            "frameIntervalsOk": frame_intervals_ok,
            "durationOk": duration_ok,
            "noCfrRewrite": no_cfr_rewrite,
            "bitrateAtLeastSourceOk": bitrate_at_least_source_ok,
            "sizeCloseToSourceOk": size_close_to_source_ok,
            "qualityPathOk": quality_path_ok,
        },
        "deltas": {
            "maxPacketPtsDeltaSec": packet_pts_delta,
            "maxPacketIntervalDeltaSec": packet_interval_delta,
            "maxPacketDurationMetadataDeltaSec": packet_duration_metadata_delta,
            "maxFramePtsDeltaSec": frame_pts_delta,
            "maxFrameIntervalDeltaSec": frame_interval_delta,
            "durationDeltaSec": produced_duration - source_duration,
            "sizeDeltaBytes": produced_size - source_size,
            "sizeRatio": size_ratio,
            "bitrateDeltaBps": produced_bitrate - source_bitrate,
            "bitrateRatio": (produced_bitrate / source_bitrate) if source_bitrate else None,
        },
    }
    return comparison


def require_valid_production_video(source_path: Path, produced_path: Path, *, redaction_applied: bool) -> dict[str, Any]:
    comparison = validate_production_video(
        source_path,
        produced_path,
        redaction_applied=redaction_applied,
    )
    if not comparison["passed"]:
        failed = [name for name, ok in comparison["checks"].items() if not ok]
        raise RuntimeError(f"Production video validation failed: {', '.join(failed)}")
    return comparison


QUALITY_RETRY_CHECKS = {
    "bitrateAtLeastSourceOk",
    "sizeCloseToSourceOk",
    "qualityPathOk",
}


def failed_validation_checks(comparison: dict[str, Any]) -> list[str]:
    return [
        name
        for name, ok in comparison.get("checks", {}).items()
        if not ok
    ]


def is_retryable_quality_undershoot(comparison: dict[str, Any]) -> bool:
    """True when the redacted encode only failed the bitrate/size quality floor."""
    failed = set(failed_validation_checks(comparison))
    if not failed or not failed.issubset(QUALITY_RETRY_CHECKS):
        return False

    size_ratio = comparison.get("deltas", {}).get("sizeRatio")
    if isinstance(size_ratio, (int, float)) and size_ratio > MAX_REDACTED_SIZE_RATIO:
        return False

    produced_bitrate = comparison.get("produced", {}).get("bitrateBps") or 0
    source_bitrate = comparison.get("source", {}).get("bitrateBps") or 0
    return source_bitrate > 0 and produced_bitrate < source_bitrate


def quality_retry_target_bitrate_bps(
    comparison: dict[str, Any],
    previous_target_bps: int,
) -> int:
    source_bitrate = int(comparison.get("source", {}).get("bitrateBps") or 0)
    produced_bitrate = int(comparison.get("produced", {}).get("bitrateBps") or 0)
    if source_bitrate <= 0:
        return previous_target_bps

    max_margin = min(
        HEVC_QUALITY_RETRY_MAX_MARGIN,
        max(1.0, MAX_REDACTED_SIZE_RATIO * 0.97),
    )
    min_target = int(source_bitrate * HEVC_QUALITY_RETRY_MIN_MARGIN)
    max_target = int(source_bitrate * max_margin)
    if produced_bitrate > 0:
        corrected = int(previous_target_bps * (source_bitrate / produced_bitrate) * 1.05)
    else:
        corrected = min_target

    return min(max(corrected, min_target), max_target)


def run_hevc_redaction_encode(
    video_path: Path,
    output_path: Path,
    filter_complex: str,
    output_label: str,
    target_bitrate_bps: int,
    *,
    force_cbr: bool = False,
) -> None:
    cmd = build_quality_hevc_ffmpeg_command(
        video_path,
        output_path,
        filter_complex,
        output_label,
        target_bitrate_bps,
        force_cbr=force_cbr,
    )
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        mode = "CBR" if force_cbr else "VBR"
        raise RuntimeError(f"HEVC {mode} redaction encode failed (exit {result.returncode}): {stderr}")


def blur_with_tracking(video_path: Path, blur_boxes: list[dict], output_path: Path) -> bool:
    """Apply privacy redaction while preserving source timing with high-quality HEVC."""
    if not blur_boxes:
        shutil.copy2(video_path, output_path)
        return True

    source_probe = probe_video(video_path, include_packets=False)
    target_bitrate = target_hevc_bitrate_bps(source_probe)
    frame_timing = probe_frame_timing(video_path)

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return False

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()

    if not has_hevc_encoder():
        raise RuntimeError("ffmpeg libx265 HEVC encoder is unavailable")

    stabilized_boxes = stabilize_privacy_boxes(blur_boxes, frame_timing)
    if not stabilized_boxes:
        raise RuntimeError("No valid privacy boxes after tracking stabilization")

    ass_script, redaction_count = build_privacy_ass_script(stabilized_boxes, width, height)
    ass_script_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            prefix="production-redaction-",
            suffix=".ass",
            delete=False,
        ) as ass_file:
            ass_file.write(ass_script)
            ass_script_path = Path(ass_file.name)

        filter_complex, output_label = build_ass_redaction_filter(ass_script_path, width, height)
        run_hevc_redaction_encode(
            video_path,
            output_path,
            filter_complex,
            output_label,
            target_bitrate,
        )
        comparison = validate_production_video(
            video_path,
            output_path,
            redaction_applied=True,
        )
        retry_index = 0
        while (
            not comparison["passed"]
            and retry_index < HEVC_QUALITY_RETRY_ATTEMPTS
            and is_retryable_quality_undershoot(comparison)
        ):
            failed = ", ".join(failed_validation_checks(comparison))
            produced_mbps = (comparison["produced"]["bitrateBps"] or 0) / 1_000_000
            source_mbps = (comparison["source"]["bitrateBps"] or 0) / 1_000_000
            size_ratio = comparison["deltas"].get("sizeRatio")
            size_ratio_text = (
                f"{size_ratio:.2f}"
                if isinstance(size_ratio, (int, float))
                else "unknown"
            )
            retry_target = quality_retry_target_bitrate_bps(comparison, target_bitrate)
            if retry_target <= target_bitrate:
                break
            print(
                f"    Quality retry {retry_index + 1}: {failed}; "
                f"produced {produced_mbps:.2f} Mbps vs source {source_mbps:.2f} Mbps, "
                f"size ratio {size_ratio_text}; forcing CBR at {retry_target / 1_000_000:.2f} Mbps"
            )
            output_path.unlink(missing_ok=True)
            run_hevc_redaction_encode(
                video_path,
                output_path,
                filter_complex,
                output_label,
                retry_target,
                force_cbr=True,
            )
            target_bitrate = retry_target
            comparison = validate_production_video(
                video_path,
                output_path,
                redaction_applied=True,
            )
            retry_index += 1
        if retry_index > 0 and comparison["passed"]:
            size_ratio = comparison["deltas"].get("sizeRatio")
            size_ratio_text = (
                f"{size_ratio:.2f}"
                if isinstance(size_ratio, (int, float))
                else "unknown"
            )
            print(
                f"    Quality retry passed at {target_bitrate / 1_000_000:.2f} Mbps "
                f"(size ratio {size_ratio_text})"
            )
    finally:
        if ass_script_path:
            ass_script_path.unlink(missing_ok=True)

    print(
        f"    Redacted {len(blur_boxes)} detections as {redaction_count} rounded vector masks "
        f"across {total_frames} source frames "
        f"at target {target_bitrate / 1_000_000:.2f} Mbps"
    )
    return output_path.exists()


def upload_to_s3(s3, video_id: str, file_path: Path) -> str:
    """Upload blurred video to S3. Returns the S3 URL."""
    key = video_s3_key(video_id)
    with open(file_path, "rb") as f:
        s3.put_object(
            Bucket=S3_BUCKET, Key=key,
            Body=f, ContentType="video/mp4",
            IfNoneMatch="*",
        )
    return s3_https_url(key)


def ensure_production_runs_table(conn):
    """Create production_runs tracking table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS production_runs (
            id TEXT PRIMARY KEY,
            video_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'queued',
            privacy_status TEXT NOT NULL DEFAULT 'pending',
            metadata_status TEXT NOT NULL DEFAULT 'pending',
            upload_status TEXT NOT NULL DEFAULT 'pending',
            priority INTEGER NOT NULL DEFAULT 100,
            skip_reason TEXT,
            s3_video_key TEXT,
            s3_metadata_key TEXT,
            machine_id TEXT,
            started_at TEXT,
            completed_at TEXT,
            last_heartbeat_at TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_production_runs_status
        ON production_runs (status)
    """)
    # Add skip_reason column if table already existed without it
    try:
        conn.execute("ALTER TABLE production_runs ADD COLUMN skip_reason TEXT")
    except Exception:
        pass  # column already exists
    try:
        conn.execute("ALTER TABLE production_runs ADD COLUMN priority INTEGER NOT NULL DEFAULT 100")
    except Exception:
        pass  # column already exists
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_production_runs_queue_priority
        ON production_runs (status, priority, created_at)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS production_worker_heartbeats (
            machine_id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'idle',
            current_video_id TEXT,
            last_heartbeat_at TEXT NOT NULL,
            started_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def update_worker_heartbeat(
    conn,
    *,
    status: str = "idle",
    current_video_id: str | None = None,
) -> None:
    now = utc_now()
    conn.execute(
        """INSERT INTO production_worker_heartbeats (
               machine_id, status, current_video_id, last_heartbeat_at, started_at, updated_at
           )
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(machine_id) DO UPDATE SET
               status=?,
               current_video_id=?,
               last_heartbeat_at=?,
               updated_at=?""",
        (
            MACHINE_ID,
            status,
            current_video_id,
            now,
            now,
            now,
            status,
            current_video_id,
            now,
            now,
        ),
    )
    conn.commit()


def update_production_heartbeat(conn, video_id: str) -> None:
    """Mark the active production worker fresh for dashboard server counts."""
    now = utc_now()
    conn.execute(
        "UPDATE production_runs SET last_heartbeat_at=? WHERE video_id=? AND status='processing'",
        (now, video_id),
    )
    update_worker_heartbeat(conn, status="processing", current_video_id=video_id)
    conn.commit()


_export_metadata_mod = None
_export_metadata_mod_mtime_ns = None


def _get_export_metadata_module():
    global _export_metadata_mod, _export_metadata_mod_mtime_ns
    mod_path = Path(__file__).resolve().parent / "export-metadata.py"
    mtime_ns = mod_path.stat().st_mtime_ns
    if _export_metadata_mod is None or _export_metadata_mod_mtime_ns != mtime_ns:
        import importlib.util
        spec = importlib.util.spec_from_file_location("export_metadata", mod_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Could not load export-metadata.py from {mod_path}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        if not hasattr(mod, "build_production_metadata"):
            raise RuntimeError(
                f"export-metadata.py loaded from {mod_path} but "
                f"build_production_metadata not found. "
                f"Available: {[a for a in dir(mod) if not a.startswith('_')]}"
            )
        _export_metadata_mod = mod
        _export_metadata_mod_mtime_ns = mtime_ns
    return _export_metadata_mod


def generate_and_upload_metadata(
    s3,
    conn,
    api_key: str,
    video_id: str,
    produced_video_path: Path | None = None,
    allow_low_pts_spread: bool = False,
) -> str | None:
    """Generate production metadata JSON and upload to S3. Returns S3 URL."""
    mod = _get_export_metadata_module()
    build_production_metadata = mod.build_production_metadata

    meta = build_production_metadata(
        conn,
        api_key,
        video_id,
        s3_https_url(video_s3_key(video_id)),
        produced_video_path,
        allow_low_pts_spread=allow_low_pts_spread,
    )
    json_bytes = json.dumps(meta, indent=2, default=str, ensure_ascii=False).encode("utf-8")
    key = metadata_s3_key(video_id)
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json_bytes, ContentType="application/json",
        IfNoneMatch="*",
    )
    return s3_https_url(key)


def build_local_test_metadata(conn, api_key: str, video_id: str, produced_path: Path) -> dict[str, Any]:
    """Build production-shaped metadata for a local dry-run artifact."""
    mod = _get_export_metadata_module()
    placeholder_url = f"https://{LOCAL_TEST_METADATA_HOST}/{safe_s3_filename(f'{video_id}.mp4', 'video_id')}"
    meta = mod.build_production_metadata(conn, api_key, video_id, placeholder_url, produced_path)
    meta["localProductionTest"] = {
        "producedPath": str(produced_path),
        "videoUrlPlaceholder": placeholder_url,
        "uploadedToS3": False,
    }
    return meta


def require_exportable_video_metadata(
    video_path: Path,
    *,
    allow_low_pts_spread: bool = False,
) -> None:
    """Validate video timing metadata before a produced MP4 can be uploaded."""
    mod = _get_export_metadata_module()
    payload = {
        "video": mod.probe_video_metadata(video_path),
        "pts_us": mod.extract_pts_us(video_path),
    }
    mod.validate_metadata(payload, allow_low_pts_spread=allow_low_pts_spread)


def write_comparison_text(comparison: dict[str, Any], output_dir: Path) -> None:
    lines = [
        f"Production quality test: {'PASS' if comparison['passed'] else 'FAIL'}",
        "",
        f"Source codec:   {comparison['source']['codec']}",
        f"Produced codec: {comparison['produced']['codec']}",
        f"Source frames:  {comparison['source']['frameCount']}",
        f"Produced frames:{comparison['produced']['frameCount']}",
        f"Source size:    {comparison['source']['sizeBytes']} bytes",
        f"Produced size:  {comparison['produced']['sizeBytes']} bytes",
        f"Size ratio:     {comparison['deltas']['sizeRatio']}",
        f"Source bitrate: {comparison['source']['bitrateBps']} bps",
        f"Produced bitrate:{comparison['produced']['bitrateBps']} bps",
        f"Bitrate ratio:  {comparison['deltas']['bitrateRatio']}",
        "",
        "Checks:",
    ]
    for name, ok in comparison["checks"].items():
        lines.append(f"  {name}: {'PASS' if ok else 'FAIL'}")
    privacy = comparison.get("privacy", {})
    if privacy:
        lines.extend([
            "",
            "Privacy:",
            f"  skipReason: {privacy.get('skipReason')}",
            f"  faceBoxes: {privacy.get('faceBoxes')}",
            f"  plateBoxes: {privacy.get('plateBoxes')}",
            f"  redactionApplied: {privacy.get('redactionApplied')}",
        ])
    lines.extend([
        "",
        "Artifacts:",
        f"  {output_dir / 'source.ffprobe.json'}",
        f"  {output_dir / 'produced.ffprobe.json'}",
        f"  {output_dir / 'produced.mp4'}",
        f"  {output_dir / 'produced.json'}",
        f"  {output_dir / 'comparison.json'}",
    ])
    (output_dir / "comparison.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2, default=str, ensure_ascii=False) + "\n", encoding="utf-8")


def process_video_local_test(conn, video_id: str, output_dir: Path) -> bool:
    """Run production locally without S3 upload or production_runs updates."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "source.ffprobe.json",
        "produced.ffprobe.json",
        "produced.mp4",
        "produced.json",
        "comparison.json",
        "comparison.txt",
    ):
        (output_dir / name).unlink(missing_ok=True)

    video_path = None
    produced_path = output_dir / "produced.mp4"
    try:
        api_key = load_api_key()
        event = load_event_for_production(api_key, video_id)
        video_url = event.get("videoUrl")
        if not video_url:
            raise RuntimeError("No video URL")

        t_step = time.time()
        video_path = resolve_local_video_path(video_id, video_url)
        if video_path:
            print(f"    Download: using local video {video_path}")
        else:
            video_path = download_video(video_url)
        if not video_path:
            raise RuntimeError("Video download failed")
        print(f"    Source ready: {time.time() - t_step:.1f}s — {video_path}")

        source_probe = probe_video(video_path)
        write_json(output_dir / "source.ffprobe.json", source_probe)

        skip_reason = check_skip_reason(conn, video_id)
        face_boxes: list[dict] = []
        plate_boxes: list[dict] = []
        redaction_applied = False
        if skip_reason:
            print(f"    Skipping blur: {skip_reason}")
            shutil.copy2(video_path, produced_path)
        else:
            t_step = time.time()
            face_boxes, plate_boxes = detect_faces_and_plates(video_path)
            print(
                f"    Detection: {time.time() - t_step:.1f}s — "
                f"{len(face_boxes)} faces, {len(plate_boxes)} plates"
            )
            all_blur_boxes = face_boxes + plate_boxes
            if all_blur_boxes:
                t_step = time.time()
                ok = blur_with_tracking(video_path, all_blur_boxes, produced_path)
                if not ok or not produced_path.exists():
                    raise RuntimeError("Blur encoding failed")
                redaction_applied = True
                print(f"    Blur+encode: {time.time() - t_step:.1f}s")
            else:
                shutil.copy2(video_path, produced_path)
                print("    No privacy boxes detected; copied original video")

        comparison = validate_production_video(
            video_path,
            produced_path,
            redaction_applied=redaction_applied,
        )
        comparison["privacy"] = {
            "skipReason": skip_reason,
            "faceBoxes": len(face_boxes),
            "plateBoxes": len(plate_boxes),
            "redactionApplied": redaction_applied,
        }
        write_json(output_dir / "produced.ffprobe.json", probe_video(produced_path))
        write_json(output_dir / "comparison.json", comparison)
        write_comparison_text(comparison, output_dir)

        metadata = build_local_test_metadata(conn, api_key, video_id, produced_path)
        write_json(output_dir / "produced.json", metadata)

        print(f"    Local test artifacts: {output_dir}")
        if not comparison["passed"]:
            failed = [name for name, ok in comparison["checks"].items() if not ok]
            print(f"    Validation failed: {', '.join(failed)}")
        return comparison["passed"]
    finally:
        if video_path and video_path.exists() and should_cleanup_video_path(video_path):
            video_path.unlink(missing_ok=True)


def generate_summary_from_db(conn, video_id: str) -> str | None:
    """Generate a deterministic summary from triage + detection data in DB.
    No API calls needed — uses only local DB data."""
    triage = get_triage_info(conn, video_id)
    if not triage:
        return None

    parts = []

    # Event type
    row = conn.execute(
        "SELECT event_type FROM triage_results WHERE id = ?", (video_id,)
    ).fetchone()
    event_type = row[0] if row else None
    if event_type:
        parts.append(event_type.replace("_", " ").capitalize() + " event")
    else:
        parts.append("Driving event")

    # Road class
    road_class = triage.get("road_class")
    if road_class:
        parts.append(f"on a {road_class.replace('_', ' ')} road")

    # Location — use DB columns if available (from backfill), no geocoding API calls
    lat, lon = triage.get("lat"), triage.get("lon")
    try:
        loc_row = conn.execute(
            "SELECT country, city FROM triage_results WHERE id = ?", (video_id,)
        ).fetchone()
        if loc_row:
            country, city = loc_row[0], loc_row[1]
            location_str = ", ".join(filter(None, [city, country]))
            if location_str:
                parts[-1] = parts[-1] + f" in {location_str}" if len(parts) > 1 else f"in {location_str}"
    except Exception:
        pass

    # Speed
    speed_min = triage.get("speed_min")
    row2 = conn.execute(
        "SELECT speed_max FROM triage_results WHERE id = ?", (video_id,)
    ).fetchone()
    speed_max = row2[0] if row2 else None
    if speed_min is not None and speed_max is not None:
        if abs(speed_max - speed_min) < 3:
            parts.append(f"at {round(speed_min)} mph")
        else:
            parts.append(f"at {round(speed_min)}-{round(speed_max)} mph")

    # Time of day
    ts = triage.get("event_timestamp")
    if lat is not None and lon is not None and ts:
        try:
            mod = _get_export_metadata_module()
            tod_info = mod.get_time_of_day(ts, lat, lon)
            if tod_info:
                tod = tod_info.get("timeOfDay")
                if tod:
                    parts.append(f"during {tod.lower()}")
        except Exception:
            pass

    # VRU detections
    try:
        det_rows = conn.execute(
            """SELECT label, COUNT(*) as cnt
               FROM frame_detections fd
               JOIN detection_runs dr ON dr.id = fd.run_id
               WHERE fd.video_id = ? AND dr.status = 'completed'
               GROUP BY label""",
            (video_id,),
        ).fetchall()
        if det_rows:
            det_parts = []
            for dr in det_rows:
                label, cnt = dr[0], dr[1]
                # Summarize by unique label presence, not frame count
                det_parts.append(label)
            if det_parts:
                parts.append(f"with {', '.join(det_parts)} detected")
    except Exception:
        pass

    return " ".join(parts).rstrip(".") + "."


def process_video(s3, conn, video_id: str) -> bool:
    """Process a single video: pre-scan, detect faces/plates, blur, upload. Returns True on success."""
    video_path = None
    output_path = None

    try:
        # Mark as processing
        run_id = str(uuid.uuid4())
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        allow_low_pts_spread = allow_low_pts_spread_for_video(video_id)
        if allow_low_pts_spread:
            print(
                f"    {YELLOW}Allowing low PTS spread for explicit override video {video_id}{RESET}"
            )
        conn.execute(
            """INSERT INTO production_runs (
                   id, video_id, status, machine_id, started_at,
                   last_heartbeat_at, created_at
               )
               VALUES (?, ?, 'processing', ?, ?, ?, ?)
               ON CONFLICT(video_id) DO UPDATE SET
                   status='processing',
                   started_at=?,
                   last_heartbeat_at=?,
                   machine_id=?,
                   completed_at=NULL,
                   last_error=NULL""",
            (run_id, video_id, MACHINE_ID, now, now, now, now, now, MACHINE_ID),
        )
        conn.commit()

        # Step 1: Generate summary if missing (from DB data only, no API call)
        t_step = time.time()
        existing = conn.execute(
            "SELECT 1 FROM clip_summaries WHERE video_id = ?", (video_id,)
        ).fetchone()
        if not existing:
            summary_text = generate_summary_from_db(conn, video_id)
            if summary_text:
                conn.execute(
                    """INSERT INTO clip_summaries (video_id, summary) VALUES (?, ?)
                       ON CONFLICT(video_id) DO UPDATE SET summary=?, updated_at=datetime('now')""",
                    (video_id, summary_text, summary_text),
                )
                conn.commit()
                print(f"    Summary ({time.time() - t_step:.1f}s): {summary_text}")
        else:
            row = conn.execute("SELECT summary FROM clip_summaries WHERE video_id = ?", (video_id,)).fetchone()
            if row:
                print(f"    Summary ({time.time() - t_step:.1f}s): {row[0]}")
        update_production_heartbeat(conn, video_id)

        # Step 2: Check speed/time-of-day skip criteria (before downloading video)
        t_step = time.time()
        skip_reason = check_skip_reason(conn, video_id)
        if skip_reason:
            print(f"    Skipping blur: {skip_reason}")
        else:
            print(f"    Skip check: {time.time() - t_step:.1f}s")
        update_production_heartbeat(conn, video_id)

        # Fetch event to get video URL. Local edited clips do not exist in Bee Maps,
        # so prefer their local metadata file when the suffixed clip id is used.
        api_key = load_api_key()
        event = load_event_for_production(api_key, video_id)
        video_url = event.get("videoUrl")
        if not video_url:
            raise RuntimeError("No video URL")

        # Download video
        t_step = time.time()
        video_path = resolve_local_video_path(video_id, video_url)
        if video_path:
            print(f"    Download: using local video {video_path}")
        else:
            video_path = download_video(video_url)
        if not video_path:
            raise RuntimeError("Video download failed")
        if should_cleanup_video_path(video_path):
            print(f"    Download: {time.time() - t_step:.1f}s")
        update_production_heartbeat(conn, video_id)

        s3_url = None

        if skip_reason:
            t_step = time.time()
            require_exportable_video_metadata(
                video_path,
                allow_low_pts_spread=allow_low_pts_spread,
            )
            s3_url = upload_to_s3(s3, video_id, video_path)
            print(f"    Upload (privacy skipped): {time.time() - t_step:.1f}s — {s3_url}")
            update_production_heartbeat(conn, video_id)
        else:
            # Detect faces + license plates in a single pass (every other frame)
            t_step = time.time()
            face_boxes, plate_boxes = detect_faces_and_plates(video_path)
            print(f"    Detection: {time.time() - t_step:.1f}s — {len(face_boxes)} faces, {len(plate_boxes)} plates")
            update_production_heartbeat(conn, video_id)

            # Combine all blur regions
            all_blur_boxes = face_boxes + plate_boxes

            if all_blur_boxes:
                # Blur faces + plates and re-encode
                t_step = time.time()
                output_path = video_path.with_suffix(".blurred.mp4")
                ok = blur_with_tracking(video_path, all_blur_boxes, output_path)
                if not ok or not output_path.exists():
                    raise RuntimeError("Blur encoding failed")
                require_valid_production_video(video_path, output_path, redaction_applied=True)
                require_exportable_video_metadata(
                    output_path,
                    allow_low_pts_spread=allow_low_pts_spread,
                )
                print(f"    Blur+encode: {time.time() - t_step:.1f}s")
                update_production_heartbeat(conn, video_id)

                # Upload blurred video to S3
                t_step = time.time()
                s3_url = upload_to_s3(s3, video_id, output_path)
                print(f"    Upload: {time.time() - t_step:.1f}s — {s3_url}")
                update_production_heartbeat(conn, video_id)
            else:
                # No blur needed — upload original video as-is
                t_step = time.time()
                require_exportable_video_metadata(
                    video_path,
                    allow_low_pts_spread=allow_low_pts_spread,
                )
                s3_url = upload_to_s3(s3, video_id, video_path)
                print(f"    Upload (no blur): {time.time() - t_step:.1f}s — {s3_url}")
                update_production_heartbeat(conn, video_id)

        # Generate and upload metadata
        t_step = time.time()
        produced_video_path = output_path if output_path and output_path.exists() else video_path
        meta_url = generate_and_upload_metadata(
            s3,
            conn,
            api_key,
            video_id,
            produced_video_path,
            allow_low_pts_spread=allow_low_pts_spread,
        )
        print(f"    Metadata: {time.time() - t_step:.1f}s — {meta_url}")
        update_production_heartbeat(conn, video_id)

        # Mark complete
        video_key = video_s3_key(video_id) if s3_url else None
        meta_key = metadata_s3_key(video_id)
        conn.execute(
            """UPDATE production_runs SET status='completed',
               privacy_status=?, metadata_status='completed', upload_status='completed',
               skip_reason=?, s3_video_key=?, s3_metadata_key=?, completed_at=?,
               last_heartbeat_at=?, last_error=NULL WHERE video_id=?""",
            (
                "skipped" if skip_reason else "completed",
                skip_reason,
                video_key,
                meta_key,
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                video_id,
            ),
        )
        conn.commit()
        return True

    except Exception as exc:
        print(f"    {RED}Error: {exc}{RESET}")
        traceback.print_exc()
        conn.execute(
            "UPDATE production_runs SET status='failed', last_error=?, last_heartbeat_at=? WHERE video_id=?",
            (str(exc)[:1000], time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), video_id),
        )
        conn.commit()
        return False
    finally:
        if video_path and video_path.exists() and should_cleanup_video_path(video_path):
            video_path.unlink(missing_ok=True)
        if output_path and output_path.exists():
            output_path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description="Production privacy redaction pipeline")
    parser.add_argument("--poll", type=float, default=5, help="Poll interval in seconds (default: 5)")
    parser.add_argument("--limit", type=int, default=0, help="Process N videos then stop (0=unlimited)")
    parser.add_argument("--event-id", type=str, help="Process a single event ID")
    parser.add_argument(
        "--local-test-output-dir",
        type=Path,
        help="Run a single event through production locally and write artifacts without S3/DB completion updates",
    )
    args = parser.parse_args()
    if args.local_test_output_dir and not args.event_id:
        parser.error("--local-test-output-dir requires --event-id")

    print(f"{BOLD}{'═' * 60}{RESET}")
    print(f"  Production Pipeline")
    print(f"  Machine: {MACHINE_ID}")
    print(f"  S3 Bucket: {S3_BUCKET}")
    print(f"  Queue: priority jobs only")
    print(f"{BOLD}{'═' * 60}{RESET}")

    conn = get_db()

    if args.local_test_output_dir:
        print(f"  {YELLOW}Local test mode: no S3 upload and no production_runs completion update{RESET}")
        ok = process_video_local_test(conn, args.event_id, args.local_test_output_dir)
        conn.close()
        status = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
        print(f"\n{BOLD}Local production quality test: {status}{RESET}")
        sys.exit(0 if ok else 1)

    ensure_production_runs_table(conn)
    update_worker_heartbeat(conn, status="idle")

    s3 = get_s3_client()

    target_prefix = normalize_s3_prefix(S3_KEY_PREFIX)
    target = f"s3://{S3_BUCKET}/{target_prefix}/" if target_prefix else f"s3://{S3_BUCKET}/"
    print(f"  {GREEN}S3 target: {target}{RESET}")

    print(f"  Polling for videos needing production processing...")
    print(f"  Press Ctrl+C to stop\n")

    running = True
    def handle_signal(sig, frame):
        nonlocal running
        print(f"\n{YELLOW}Shutting down...{RESET}")
        running = False
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    processed = 0

    while running:
        if args.event_id:
            video_ids = [args.event_id]
        else:
            video_ids = claim_videos_needing_blur(conn, limit=1)
        if not video_ids:
            update_worker_heartbeat(conn, status="idle")
            if args.limit > 0 or args.event_id:
                break
            time.sleep(args.poll)
            continue

        for video_id in video_ids:
            if not running:
                break

            # Skip if already in S3 (unless --event-id forces reprocess)
            if not args.event_id and video_already_blurred(s3, video_id):
                conn.execute(
                    """INSERT INTO production_runs (id, video_id, status, completed_at, created_at)
                       VALUES (?, ?, 'completed', ?, ?)
                       ON CONFLICT(video_id) DO UPDATE SET status='completed'""",
                    (str(uuid.uuid4()), video_id, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                     time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
                )
                conn.commit()
                continue

            print(f"\n{CYAN}Process {video_id[:20]}…{RESET}")
            update_worker_heartbeat(conn, status="processing", current_video_id=video_id)
            t0 = time.time()
            ok = process_video(s3, conn, video_id)
            elapsed = time.time() - t0

            status = f"{GREEN}done{RESET}" if ok else f"{RED}failed{RESET}"
            print(f"  {video_id[:16]}… {status} in {elapsed:.1f}s")

            processed += 1
            update_worker_heartbeat(conn, status="idle")
            if args.event_id or (args.limit > 0 and processed >= args.limit):
                running = False
                break

    conn.close()
    print(f"\n{BOLD}Processed {processed} videos.{RESET}")


if __name__ == "__main__":
    main()
