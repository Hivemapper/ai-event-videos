#!/usr/bin/env python3
"""
Face blur pipeline — blurs faces in videos where persons were detected.

Queries frame_detections for person labels, runs MediaPipe face detection
on person crops, and re-encodes the video with FFmpeg blur filters.
Uploads blurred video to S3.

Usage:
    python3 scripts/face-blur-pipeline.py
    python3 scripts/face-blur-pipeline.py --workers 2
    python3 scripts/face-blur-pipeline.py --limit 10   # process 10 videos then stop

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
    from transformers import AutoModelForZeroShotObjectDetection, AutoProcessor
    import torch
    HAS_GDINO = True
except ImportError:
    HAS_GDINO = False

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
FACE_BOX_PADDING = 0.15
# Only blur faces on persons taller than this (pixels). ~100px = close/medium range.
MIN_PERSON_HEIGHT_PX = 100

# License plate detection
PLATE_MIN_CONFIDENCE = 0.15
PLATE_BOX_PADDING = 0.1

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


def get_videos_with_persons(conn, limit: int = 100) -> list[str]:
    """Get video IDs that have person detections but no blurred video yet."""
    rows = conn.execute(
        """SELECT fd.video_id, COUNT(*) as det_count
           FROM frame_detections fd
           JOIN detection_runs dr ON dr.id = fd.run_id
           WHERE fd.label IN ('person', 'construction worker', 'pedestrian')
             AND dr.status = 'completed'
             AND fd.video_id NOT IN (
               SELECT video_id FROM blur_runs
             )
           GROUP BY fd.video_id
           ORDER BY det_count DESC
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


# Cache models globally so they load once
_face_model = None
_face_model_failed = False
_gdino_model = None
_gdino_processor = None
_gdino_device = None
_gdino_failed = False


def _download_yolo_face_model() -> str:
    """Download YOLO face detection model."""
    import urllib.request
    model_dir = PROJECT_ROOT / "data" / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    model_path = model_dir / "yolov8n-face-lindevs.pt"
    if not model_path.exists():
        # lindevs YOLOv8 face model trained on WIDERFace
        url = "https://github.com/lindevs/yolov8-face/releases/download/1.0.1/yolov8n-face-lindevs.pt"
        print(f"    Downloading YOLO face model...")
        urllib.request.urlretrieve(url, str(model_path))
    return str(model_path)


def _get_face_model():
    global _face_model, _face_model_failed
    if _face_model_failed:
        return None
    if _face_model is None:
        try:
            model_path = _download_yolo_face_model()
            print(f"    Loading YOLO face model...")
            _face_model = _YOLO(model_path)
        except Exception as e:
            print(f"    {YELLOW}YOLO face model failed: {e}{RESET}")
            _face_model_failed = True
            return None
    return _face_model


def detect_faces(video_path: Path) -> list[dict]:
    """Run YOLO face detection on every frame of the video."""
    model = _get_face_model()
    if model is None:
        return []

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return []

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    face_boxes = []

    for i in range(total_frames):
        cap.set(cv2.CAP_PROP_POS_FRAMES, i)
        ret, frame = cap.read()
        if not ret:
            break

        h, w = frame.shape[:2]
        frame_ms = int(i * 1000 / fps)

        results = model.predict(frame, conf=FACE_MIN_CONFIDENCE, imgsz=640, verbose=False)
        for result in results:
            for box in result.boxes:
                x1, y1, x2, y2 = box.xyxy[0].tolist()
                fw = x2 - x1
                fh = y2 - y1

                # Skip tiny faces (< ~15px, too small to identify)
                if fh < 15:
                    continue

                # Skip non-face-shaped detections (faces are roughly square)
                aspect = fh / fw if fw > 0 else 99
                if aspect > 2.0 or aspect < 0.4:
                    continue

                pad_x = fw * FACE_BOX_PADDING
                pad_y = fh * FACE_BOX_PADDING
                bx = max(0, x1 - pad_x)
                by = max(0, y1 - pad_y)
                bw = min(w - bx, fw + 2 * pad_x)
                bh = min(h - by, fh + 2 * pad_y)

                face_boxes.append({
                    "frame_ms": frame_ms,
                    "x": int(bx), "y": int(by),
                    "w": int(bw), "h": int(bh),
                })

    cap.release()
    return face_boxes


def _get_gdino():
    """Load GDINO model for license plate detection."""
    global _gdino_model, _gdino_processor, _gdino_device, _gdino_failed
    if _gdino_failed:
        return None, None, None
    if _gdino_model is None and HAS_GDINO:
        try:
            print(f"    Loading GDINO for plate detection...")
            model_id = "IDEA-Research/grounding-dino-base"
            _gdino_processor = AutoProcessor.from_pretrained(model_id)
            _gdino_model = AutoModelForZeroShotObjectDetection.from_pretrained(model_id)
            _gdino_model.eval()
            _gdino_device = "cuda" if torch.cuda.is_available() else "cpu"
            _gdino_model = _gdino_model.to(_gdino_device)
            print(f"    GDINO loaded on {_gdino_device.upper()}")
        except Exception as e:
            print(f"    {YELLOW}GDINO failed: {e}{RESET}")
            _gdino_failed = True
            return None, None, None
    return _gdino_model, _gdino_processor, _gdino_device


PLATE_TEXT_PROMPT = "license plate."
PLATE_GDINO_BOX_THRESHOLD = 0.2
PLATE_GDINO_TEXT_THRESHOLD = 0.2


def detect_license_plates(video_path: Path) -> list[dict]:
    """Run GDINO license plate detection on sampled frames."""
    model, processor, device = _get_gdino()
    if model is None:
        return []

    from PIL import Image as PILImage

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return []

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    plate_boxes = []

    # Sample every ~3 frames — GDINO is slower but more accurate
    sample_step = max(1, total_frames // 300)

    for i in range(0, total_frames, sample_step):
        cap.set(cv2.CAP_PROP_POS_FRAMES, i)
        ret, frame = cap.read()
        if not ret:
            break

        h, w = frame.shape[:2]
        frame_ms = int(i * 1000 / fps)

        pil_img = PILImage.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        inputs = processor(images=pil_img, text=PLATE_TEXT_PROMPT, return_tensors="pt")
        inputs = {k: v.to(device) if hasattr(v, "to") else v for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)

        results = processor.post_process_grounded_object_detection(
            outputs, inputs["input_ids"],
            threshold=PLATE_GDINO_BOX_THRESHOLD,
            text_threshold=PLATE_GDINO_TEXT_THRESHOLD,
            target_sizes=[(h, w)],
        )

        if results:
            r = results[0]
            boxes = r.get("boxes", torch.tensor([]))
            scores = r.get("scores", torch.tensor([]))
            for box, score in zip(boxes, scores):
                x1, y1, x2, y2 = box.cpu().tolist()

                # Skip detections in upper 40% of frame
                if (y1 + y2) / 2 < h * 0.4:
                    continue

                pw = x2 - x1
                ph = y2 - y1

                # Skip oversized detections — plates are small (max ~15% of frame width)
                if pw > w * 0.15 or ph > h * 0.10:
                    continue

                # Skip wrong aspect ratio — plates are wider than tall
                if pw > 0 and ph / pw > 1.0:
                    continue

                pad_x = pw * PLATE_BOX_PADDING
                pad_y = ph * PLATE_BOX_PADDING
                x1 = max(0, x1 - pad_x)
                y1 = max(0, y1 - pad_y)
                pw = min(w - x1, pw + 2 * pad_x)
                ph = min(h - y1, ph + 2 * pad_y)

                plate_boxes.append({
                    "frame_ms": frame_ms,
                    "x": int(x1), "y": int(y1),
                    "w": int(pw), "h": int(ph),
                })

    cap.release()
    return plate_boxes


def blur_with_tracking(video_path: Path, blur_boxes: list[dict], output_path: Path) -> bool:
    """Re-encode video with tracked blur regions.

    Uses OpenCV CSRT trackers: initializes a tracker at each detection frame,
    then tracks the region forward (and backward) through all frames so blur
    follows faces/plates smoothly.
    """
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
    # We'll do a forward pass with trackers

    # Group blur boxes by frame index, extending to neighboring frames
    BLUR_SPREAD = 5  # blur N frames before and after each detection
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

    # Re-encode with blur applied per frame
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(output_path), fourcc, fps, (width, height))

    blurred_frame_count = 0
    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
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

        writer.write(frame)

    cap.release()
    writer.release()
    print(f"    Blurred {blurred_frame_count}/{total_frames} frames, {len(blur_per_frame)} unique frames in map")

    # Re-mux with ffmpeg for proper mp4 container + copy audio
    final_path = output_path.with_suffix(".final.mp4")
    cmd = [
        "ffmpeg", "-y",
        "-i", str(output_path),
        "-i", str(video_path),
        "-map", "0:v", "-map", "1:a?",
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "copy",
        str(final_path),
    ]
    try:
        subprocess.run(cmd, capture_output=True, timeout=300)
        if final_path.exists():
            final_path.rename(output_path)
            return True
    except Exception:
        pass

    return output_path.exists()


def upload_to_s3(s3, video_id: str, file_path: Path) -> str:
    """Upload blurred video to S3. Returns the S3 URL."""
    key = f"{video_id}.mp4"
    with open(file_path, "rb") as f:
        s3.put_object(
            Bucket=S3_BUCKET, Key=key,
            Body=f, ContentType="video/mp4",
        )
    return f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{key}"


def ensure_blur_runs_table(conn):
    """Create blur_runs tracking table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS blur_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'queued',
            face_count INTEGER,
            s3_url TEXT,
            machine_id TEXT,
            started_at TEXT,
            completed_at TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def process_video(s3, conn, video_id: str) -> bool:
    """Process a single video: detect faces, blur, upload. Returns True on success."""
    video_path = None
    output_path = None

    try:
        # Mark as running
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        conn.execute(
            """INSERT INTO blur_runs (video_id, status, machine_id, started_at)
               VALUES (?, 'running', ?, ?)
               ON CONFLICT(video_id) DO UPDATE SET status='running', started_at=?, machine_id=?""",
            (video_id, MACHINE_ID, now, now, MACHINE_ID),
        )
        conn.commit()

        # Get person detections
        detections = get_person_detections(conn, video_id)
        if not detections:
            conn.execute(
                "UPDATE blur_runs SET status='completed', face_count=0, completed_at=? WHERE video_id=?",
                (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), video_id),
            )
            conn.commit()
            print(f"    No person detections, skipping")
            return True

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
        video_path = download_video(video_url)
        if not video_path:
            raise RuntimeError("Video download failed")

        # Detect faces on every frame
        face_boxes = detect_faces(video_path)
        print(f"    {len(face_boxes)} faces detected")

        # Detect license plates
        plate_boxes = detect_license_plates(video_path)
        print(f"    {len(plate_boxes)} license plates detected")

        # Combine all blur regions
        all_blur_boxes = face_boxes + plate_boxes

        if not all_blur_boxes:
            # Nothing to blur — mark complete, no upload needed
            conn.execute(
                "UPDATE blur_runs SET status='completed', face_count=0, completed_at=? WHERE video_id=?",
                (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), video_id),
            )
            conn.commit()
            return True

        # Blur faces + plates and re-encode
        output_path = video_path.with_suffix(".blurred.mp4")
        ok = blur_with_tracking(video_path, all_blur_boxes, output_path)
        if not ok or not output_path.exists():
            raise RuntimeError("Blur encoding failed")

        # Upload blurred to S3
        s3_url = upload_to_s3(s3, video_id, output_path)
        print(f"    Uploaded to S3: {s3_url}")

        # Mark complete
        conn.execute(
            "UPDATE blur_runs SET status='completed', face_count=?, s3_url=?, completed_at=? WHERE video_id=?",
            (len(all_blur_boxes), s3_url, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), video_id),
        )
        conn.commit()
        return True

    except Exception as exc:
        print(f"    {RED}Error: {exc}{RESET}")
        traceback.print_exc()
        conn.execute(
            "UPDATE blur_runs SET status='failed', last_error=? WHERE video_id=?",
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
    print(f"  Face Blur Pipeline")
    print(f"  Machine: {MACHINE_ID}")
    print(f"  S3 Bucket: {S3_BUCKET}")
    print(f"{BOLD}{'═' * 60}{RESET}")

    conn = get_db()
    ensure_blur_runs_table(conn)

    s3 = get_s3_client()

    print(f"  {GREEN}S3 target: s3://{S3_BUCKET}/{RESET}")

    print(f"  Polling for videos with person detections...")
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
            video_ids = get_videos_with_persons(conn, limit=10)
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
                    """INSERT INTO blur_runs (video_id, status, completed_at)
                       VALUES (?, 'completed', ?)
                       ON CONFLICT(video_id) DO UPDATE SET status='completed'""",
                    (video_id, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
                )
                conn.commit()
                continue

            print(f"\n{CYAN}Blur {video_id[:20]}…{RESET}")
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
