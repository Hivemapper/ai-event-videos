#!/usr/bin/env python3
"""
Face blur pipeline — blurs faces in videos where persons were detected.

Queries frame_detections for person labels, runs MediaPipe face detection
on person crops, and re-encodes the video with FFmpeg blur filters.
Uploads blurred video to S3.

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
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
from pathlib import Path

import cv2
import numpy as np
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

S3_BUCKET = "hivemapper-blurred-ai-event-videos"
S3_REGION = "us-west-2"

PERSON_LABELS = {"person", "construction worker", "pedestrian"}

# Face detection
FACE_MIN_CONFIDENCE = 0.3
FACE_BOX_PADDING = 0.6
# Only blur faces on persons taller than this (pixels). ~100px = close/medium range.
MIN_PERSON_HEIGHT_PX = 100

# License plate detection
PLATE_MIN_CONFIDENCE = 0.15
PLATE_BOX_PADDING = 0.1

# Privacy blur skip thresholds (speeds in mph)
SKIP_SPEED_MIN_ANY_MPH = 35         # skip blur if min speed >= 35 mph (any time)
SKIP_SPEED_MIN_NIGHT_MPH = 20       # skip blur if min speed >= 20 mph at night
SKIP_SPEED_MIN_MOTORWAY_MPH = 30    # skip blur if min speed >= 30 mph on motorway
SKIP_MOTORWAY_ROAD_CLASSES = {"motorway", "trunk"}

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
    from botocore import UNSIGNED
    from botocore.config import Config
    return boto3.client("s3", region_name=S3_REGION, config=Config(signature_version=UNSIGNED))


def video_already_blurred(s3, video_id: str) -> bool:
    """Check if a blurred video already exists in S3."""
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=f"{video_id}.mp4")
        return True
    except s3.exceptions.ClientError:
        return False


def get_videos_needing_blur(conn, limit: int = 100) -> list[str]:
    """Get video IDs that need privacy blurring but don't have a blur run yet.
    Includes:
    - Videos with completed detection runs (from VRU pipeline)
    - Signal-triaged videos that were never queued for VRU detection
    """
    rows = conn.execute(
        """SELECT video_id FROM (
               -- Videos with completed VRU detection runs
               SELECT DISTINCT dr.video_id, dr.completed_at AS sort_date
               FROM detection_runs dr
               WHERE dr.status = 'completed'
                 AND dr.video_id NOT IN (SELECT video_id FROM production_runs)
               UNION
               -- Signal-triaged videos without any detection run
               SELECT tr.id AS video_id, tr.created_at AS sort_date
               FROM triage_results tr
               WHERE tr.triage_result = 'signal'
                 AND tr.id NOT IN (SELECT video_id FROM production_runs)
                 AND tr.id NOT IN (SELECT video_id FROM detection_runs)
           )
           ORDER BY sort_date DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [r[0] if isinstance(r, tuple) else r["video_id"] for r in rows]


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


def detect_faces_and_plates(video_path: Path) -> tuple[list[dict], list[dict]]:
    """Run privacy model on every other frame to detect faces and license plates.
    Returns (face_boxes, plate_boxes)."""
    model = _get_privacy_model()
    if model is None:
        return [], []

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return [], []

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    face_boxes = []
    plate_boxes = []

    # Read sequentially, process every other frame (seeking is slow on HEVC)
    for i in range(total_frames):
        ret, frame = cap.read()
        if not ret:
            break
        if i % 2 != 0:
            continue

        h, w = frame.shape[:2]
        frame_ms = int(i * 1000 / fps)

        results = model.predict(frame, conf=FACE_MIN_CONFIDENCE, imgsz=640, verbose=False)
        for result in results:
            for box in result.boxes:
                cls = int(box.cls[0]) if box.cls is not None else -1
                if cls not in (PRIVACY_FACE_CLS, PRIVACY_PLATE_CLS):
                    continue

                x1, y1, x2, y2 = box.xyxy[0].tolist()
                bw = x2 - x1
                bh = y2 - y1

                if cls == PRIVACY_FACE_CLS:
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
                        "frame_ms": frame_ms,
                        "x": int(bx), "y": int(by),
                        "w": int(pw), "h": int(ph),
                    })

                elif cls == PRIVACY_PLATE_CLS:
                    pad_x = bw * PLATE_BOX_PADDING
                    pad_y = bh * PLATE_BOX_PADDING
                    bx = max(0, x1 - pad_x)
                    by = max(0, y1 - pad_y)
                    pw = min(w - bx, bw + 2 * pad_x)
                    ph = min(h - by, bh + 2 * pad_y)
                    plate_boxes.append({
                        "frame_ms": frame_ms,
                        "x": int(bx), "y": int(by),
                        "w": int(pw), "h": int(ph),
                    })

    cap.release()
    return face_boxes, plate_boxes


def _has_nvenc() -> bool:
    """Check if NVENC hardware encoder is actually usable (not just listed)."""
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-f", "lavfi", "-i", "nullsrc=s=64x64:d=0.1",
             "-c:v", "h264_nvenc", "-f", "null", "-"],
            capture_output=True, text=True, timeout=10,
        )
        return result.returncode == 0
    except Exception:
        return False


_nvenc_available: bool | None = None


def blur_with_tracking(video_path: Path, blur_boxes: list[dict], output_path: Path) -> bool:
    """Re-encode video with blur regions, piping frames directly to ffmpeg.

    Uses NVENC hardware encoding on GPU if available, otherwise libx264.
    Single-pass: read frames with OpenCV, apply blur, pipe raw to ffmpeg.
    """
    global _nvenc_available

    if not blur_boxes:
        import shutil
        shutil.copy2(video_path, output_path)
        return True

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return False

    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    # Pre-compute: for each frame, which blur regions are active
    BLUR_SPREAD = 3  # blur N frames before and after each detection (detection runs every 2nd frame)
    blur_per_frame: dict[int, list[tuple]] = {}
    for bb in blur_boxes:
        frame_idx = int(bb["frame_ms"] * fps / 1000)
        x, y, w, h = bb["x"], bb["y"], bb["w"], bb["h"]
        x = max(0, min(x, width - 1))
        y = max(0, min(y, height - 1))
        w = max(1, min(w, width - x))
        h = max(1, min(h, height - y))
        rect = (x, y, w, h)
        for offset in range(-BLUR_SPREAD, BLUR_SPREAD + 1):
            fi = frame_idx + offset
            if 0 <= fi < total_frames:
                blur_per_frame.setdefault(fi, []).append(rect)

    # Detect NVENC once
    if _nvenc_available is None:
        _nvenc_available = _has_nvenc()
        print(f"    NVENC: {'available' if _nvenc_available else 'not available, using libx264'}")

    # Build ffmpeg command — pipe raw BGR frames in, encode directly to output
    if _nvenc_available:
        encoder_args = ["-c:v", "h264_nvenc", "-preset", "p4", "-cq", "23", "-profile:v", "high"]
    else:
        encoder_args = ["-c:v", "libx264", "-preset", "fast", "-crf", "23"]

    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        # Raw video input from pipe
        "-f", "rawvideo", "-pix_fmt", "bgr24",
        "-s", f"{width}x{height}", "-r", str(fps),
        "-i", "pipe:0",
        # Audio from original
        "-i", str(video_path),
        "-map", "0:v", "-map", "1:a?",
        *encoder_args,
        "-pix_fmt", "yuv420p",
        "-c:a", "copy",
        "-movflags", "+faststart",
        str(output_path),
    ]

    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)

    blurred_frame_count = 0
    for fi in range(total_frames):
        ret, frame = cap.read()
        if not ret:
            break

        if fi in blur_per_frame:
            blurred_frame_count += 1
            for (x, y, w, h) in blur_per_frame[fi]:
                roi = frame[y:y+h, x:x+w]
                if roi.size > 0:
                    k = max(15, min(99, (w + h) // 2 | 1))
                    frame[y:y+h, x:x+w] = cv2.GaussianBlur(roi, (k, k), k // 3)

        proc.stdin.write(frame.tobytes())

    cap.release()
    proc.stdin.close()
    proc.wait(timeout=300)

    print(f"    Blurred {blurred_frame_count}/{total_frames} frames")
    return output_path.exists() and proc.returncode == 0


def upload_to_s3(s3, video_id: str, file_path: Path) -> str:
    """Upload blurred video to S3. Returns the S3 URL."""
    key = f"{video_id}.mp4"
    with open(file_path, "rb") as f:
        s3.put_object(
            Bucket=S3_BUCKET, Key=key,
            Body=f, ContentType="video/mp4",
        )
    return f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{key}"


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
    conn.commit()


_export_metadata_mod = None


def _get_export_metadata_module():
    global _export_metadata_mod
    if _export_metadata_mod is None:
        import importlib.util
        mod_path = Path(__file__).resolve().parent / "export-metadata.py"
        spec = importlib.util.spec_from_file_location("export_metadata", mod_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        if not hasattr(mod, "build_production_metadata"):
            raise RuntimeError(
                f"export-metadata.py loaded from {mod_path} but "
                f"build_production_metadata not found. "
                f"Available: {[a for a in dir(mod) if not a.startswith('_')]}"
            )
        _export_metadata_mod = mod
    return _export_metadata_mod


def generate_and_upload_metadata(s3, conn, api_key: str, video_id: str) -> str | None:
    """Generate production metadata JSON and upload to S3. Returns S3 URL."""
    mod = _get_export_metadata_module()
    build_production_metadata = mod.build_production_metadata

    meta = build_production_metadata(conn, api_key, video_id)
    json_bytes = json.dumps(meta, indent=2, default=str, ensure_ascii=False).encode("utf-8")
    key = f"{video_id}.json"
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json_bytes, ContentType="application/json",
    )
    return f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{key}"


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
        conn.execute(
            """INSERT INTO production_runs (id, video_id, status, machine_id, started_at, created_at)
               VALUES (?, ?, 'processing', ?, ?, ?)
               ON CONFLICT(video_id) DO UPDATE SET status='processing', started_at=?, machine_id=?""",
            (run_id, video_id, MACHINE_ID, now, now, now, MACHINE_ID),
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

        # Step 2: Check speed/time-of-day skip criteria (before downloading video)
        t_step = time.time()
        skip_reason = check_skip_reason(conn, video_id)
        if skip_reason:
            print(f"    Skipping blur: {skip_reason}")
            api_key = load_api_key()
            meta_url = generate_and_upload_metadata(s3, conn, api_key, video_id)
            print(f"    Metadata uploaded: {meta_url}")
            meta_key = f"{video_id}.json"
            conn.execute(
                """UPDATE production_runs SET status='completed', privacy_status='skipped',
                   metadata_status='completed', upload_status='completed',
                   skip_reason=?, s3_metadata_key=?, completed_at=? WHERE video_id=?""",
                (skip_reason, meta_key, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), video_id),
            )
            conn.commit()
            return True

        print(f"    Skip check: {time.time() - t_step:.1f}s")

        # Fetch event to get video URL
        api_key = load_api_key()
        resp = api_request(
            "GET",
            f"{API_BASE_URL}/{video_id}",
            headers={"Authorization": api_key, "Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        event = resp.json()
        video_url = event.get("videoUrl")
        if not video_url:
            raise RuntimeError("No video URL")

        # Download video
        t_step = time.time()
        video_path = download_video(video_url)
        if not video_path:
            raise RuntimeError("Video download failed")
        print(f"    Download: {time.time() - t_step:.1f}s")

        # Detect faces + license plates in a single pass (every other frame)
        t_step = time.time()
        face_boxes, plate_boxes = detect_faces_and_plates(video_path)
        print(f"    Detection: {time.time() - t_step:.1f}s — {len(face_boxes)} faces, {len(plate_boxes)} plates")

        # Combine all blur regions
        all_blur_boxes = face_boxes + plate_boxes
        s3_url = None

        if all_blur_boxes:
            # Blur faces + plates and re-encode
            t_step = time.time()
            output_path = video_path.with_suffix(".blurred.mp4")
            ok = blur_with_tracking(video_path, all_blur_boxes, output_path)
            if not ok or not output_path.exists():
                raise RuntimeError("Blur encoding failed")
            print(f"    Blur+encode: {time.time() - t_step:.1f}s")

            # Upload blurred video to S3
            t_step = time.time()
            s3_url = upload_to_s3(s3, video_id, output_path)
            print(f"    Upload: {time.time() - t_step:.1f}s — {s3_url}")
        else:
            # No blur needed — upload original video as-is
            t_step = time.time()
            s3_url = upload_to_s3(s3, video_id, video_path)
            print(f"    Upload (no blur): {time.time() - t_step:.1f}s — {s3_url}")

        # Generate and upload metadata
        t_step = time.time()
        meta_url = generate_and_upload_metadata(s3, conn, api_key, video_id)
        print(f"    Metadata: {time.time() - t_step:.1f}s — {meta_url}")

        # Mark complete
        video_key = f"{video_id}.mp4" if s3_url else None
        meta_key = f"{video_id}.json"
        conn.execute(
            """UPDATE production_runs SET status='completed',
               privacy_status='completed', metadata_status='completed', upload_status='completed',
               s3_video_key=?, s3_metadata_key=?, completed_at=? WHERE video_id=?""",
            (video_key, meta_key, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), video_id),
        )
        conn.commit()
        return True

    except Exception as exc:
        print(f"    {RED}Error: {exc}{RESET}")
        traceback.print_exc()
        conn.execute(
            "UPDATE production_runs SET status='failed', last_error=? WHERE video_id=?",
            (str(exc)[:1000], video_id),
        )
        conn.commit()
        return False
    finally:
        if video_path and video_path.exists():
            video_path.unlink(missing_ok=True)
        if output_path and output_path.exists():
            output_path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description="Face blur pipeline")
    parser.add_argument("--poll", type=float, default=5, help="Poll interval in seconds (default: 5)")
    parser.add_argument("--limit", type=int, default=0, help="Process N videos then stop (0=unlimited)")
    parser.add_argument("--event-id", type=str, help="Process a single event ID")
    args = parser.parse_args()

    print(f"{BOLD}{'═' * 60}{RESET}")
    print(f"  Production Pipeline")
    print(f"  Machine: {MACHINE_ID}")
    print(f"  S3 Bucket: {S3_BUCKET}")
    print(f"{BOLD}{'═' * 60}{RESET}")

    conn = get_db()
    ensure_production_runs_table(conn)

    s3 = get_s3_client()

    print(f"  {GREEN}S3 target: s3://{S3_BUCKET}/{RESET}")

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
            video_ids = get_videos_needing_blur(conn, limit=10)
        if not video_ids:
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
            t0 = time.time()
            ok = process_video(s3, conn, video_id)
            elapsed = time.time() - t0

            status = f"{GREEN}done{RESET}" if ok else f"{RED}failed{RESET}"
            print(f"  {video_id[:16]}… {status} in {elapsed:.1f}s")

            processed += 1
            if args.event_id or (args.limit > 0 and processed >= args.limit):
                running = False
                break

    conn.close()
    print(f"\n{BOLD}Processed {processed} videos.{RESET}")


if __name__ == "__main__":
    main()
