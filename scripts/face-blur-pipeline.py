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
    import mediapipe as mp
except ImportError:
    print("mediapipe not installed. Run: pip install mediapipe")
    sys.exit(1)

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

# Face detection confidence threshold
FACE_MIN_CONFIDENCE = 0.4

# Expand face box by this factor for better coverage
FACE_BOX_PADDING = 0.3

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
        """SELECT DISTINCT fd.video_id
           FROM frame_detections fd
           JOIN detection_runs dr ON dr.id = fd.run_id
           WHERE fd.label IN ('person', 'construction worker', 'pedestrian')
             AND dr.status = 'completed'
             AND fd.video_id NOT IN (
               SELECT video_id FROM blur_runs WHERE status = 'completed'
             )
           ORDER BY dr.completed_at DESC
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


def detect_faces_in_persons(video_path: Path, detections: list[dict]) -> list[dict]:
    """Run MediaPipe face detection on person crops, return face bounding boxes in video coords."""
    face_detector = mp.solutions.face_detection.FaceDetection(
        model_selection=1,  # full range model
        min_detection_confidence=FACE_MIN_CONFIDENCE,
    )

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return []

    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    face_boxes = []  # list of {frame_ms, x, y, w, h} in video pixel coords

    # Group detections by frame_ms
    by_frame: dict[int, list[dict]] = {}
    for det in detections:
        by_frame.setdefault(det["frame_ms"], []).append(det)

    for frame_ms, frame_dets in sorted(by_frame.items()):
        # Seek to frame
        cap.set(cv2.CAP_PROP_POS_MSEC, frame_ms)
        ret, frame = cap.read()
        if not ret:
            continue

        h, w = frame.shape[:2]

        for det in frame_dets:
            # Crop person region
            px1 = max(0, int(det["x_min"]))
            py1 = max(0, int(det["y_min"]))
            px2 = min(w, int(det["x_max"]))
            py2 = min(h, int(det["y_max"]))
            if px2 <= px1 or py2 <= py1:
                continue

            crop = frame[py1:py2, px1:px2]
            crop_rgb = cv2.cvtColor(crop, cv2.COLOR_BGR2RGB)
            results = face_detector.process(crop_rgb)

            if results.detections:
                for face in results.detections:
                    bbox = face.location_data.relative_bounding_box
                    crop_h, crop_w = crop.shape[:2]

                    # Convert relative coords to video pixel coords
                    fx = px1 + bbox.xmin * crop_w
                    fy = py1 + bbox.ymin * crop_h
                    fw = bbox.width * crop_w
                    fh = bbox.height * crop_h

                    # Add padding
                    pad_x = fw * FACE_BOX_PADDING
                    pad_y = fh * FACE_BOX_PADDING
                    fx = max(0, fx - pad_x)
                    fy = max(0, fy - pad_y)
                    fw = min(w - fx, fw + 2 * pad_x)
                    fh = min(h - fy, fh + 2 * pad_y)

                    face_boxes.append({
                        "frame_ms": frame_ms,
                        "x": int(fx), "y": int(fy),
                        "w": int(fw), "h": int(fh),
                    })

    cap.release()
    face_detector.close()
    return face_boxes


def build_ffmpeg_blur(video_path: Path, face_boxes: list[dict], output_path: Path) -> bool:
    """Re-encode video with blur applied to face regions using FFmpeg."""
    if not face_boxes:
        # No faces found — just copy the file
        import shutil
        shutil.copy2(video_path, output_path)
        return True

    cap = cv2.VideoCapture(str(video_path))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    duration = total_frames / fps if fps > 0 else 30.0
    cap.release()

    # Build FFmpeg filter_complex with boxblur for each face region
    # Group face boxes and create enable conditions based on time windows
    # Each face gets a blur filter enabled for a time window around its frame_ms

    filters = []
    last_output = "0:v"

    for i, face in enumerate(face_boxes):
        t = face["frame_ms"] / 1000.0
        # Enable blur for a window around the detection frame
        # Use +/- half the gap between sampled frames (typically ~1s)
        t_start = max(0, t - 0.5)
        t_end = min(duration, t + 0.5)

        x, y, w, h = face["x"], face["y"], face["w"], face["h"]
        # Clamp to video bounds
        x = max(0, min(x, width - 1))
        y = max(0, min(y, height - 1))
        w = max(1, min(w, width - x))
        h = max(1, min(h, height - y))

        input_label = last_output
        output_label = f"v{i}"

        # Crop the face region, blur it, overlay it back
        filters.append(
            f"[{input_label}]crop={w}:{h}:{x}:{y},boxblur=20:5[blur{i}];"
            f"[{input_label if i == 0 else last_output}][blur{i}]overlay={x}:{y}:"
            f"enable='between(t,{t_start:.3f},{t_end:.3f})'[{output_label}]"
        )
        last_output = output_label

    # For many face boxes, FFmpeg filter chains get complex.
    # Use a simpler approach: process frame by frame with OpenCV
    if len(face_boxes) > 50:
        return _blur_with_opencv(video_path, face_boxes, output_path)

    filter_complex = ";".join(filters)

    cmd = [
        "ffmpeg", "-y", "-i", str(video_path),
        "-filter_complex", filter_complex,
        "-map", f"[{last_output}]",
        "-map", "0:a?",
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "copy",
        str(output_path),
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print(f"    FFmpeg failed, falling back to OpenCV: {result.stderr[-200:]}")
            return _blur_with_opencv(video_path, face_boxes, output_path)
        return True
    except subprocess.TimeoutExpired:
        print("    FFmpeg timed out, falling back to OpenCV")
        return _blur_with_opencv(video_path, face_boxes, output_path)


def _blur_with_opencv(video_path: Path, face_boxes: list[dict], output_path: Path) -> bool:
    """Fallback: re-encode with OpenCV, applying blur frame by frame."""
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return False

    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(output_path), fourcc, fps, (width, height))

    # Index face boxes by frame_ms for fast lookup
    faces_by_ms: dict[int, list[dict]] = {}
    for fb in face_boxes:
        faces_by_ms.setdefault(fb["frame_ms"], []).append(fb)

    frame_idx = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break

        current_ms = int(frame_idx * 1000 / fps)

        # Find faces active at this time (within 500ms of a detection)
        for det_ms, faces in faces_by_ms.items():
            if abs(current_ms - det_ms) <= 500:
                for face in faces:
                    x, y, w, h = face["x"], face["y"], face["w"], face["h"]
                    x = max(0, min(x, width - 1))
                    y = max(0, min(y, height - 1))
                    w = max(1, min(w, width - x))
                    h = max(1, min(h, height - y))
                    roi = frame[y:y+h, x:x+w]
                    blurred = cv2.GaussianBlur(roi, (99, 99), 30)
                    frame[y:y+h, x:x+w] = blurred

        writer.write(frame)
        frame_idx += 1

    cap.release()
    writer.release()

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
    s3.upload_file(
        str(file_path), S3_BUCKET, key,
        ExtraArgs={"ContentType": "video/mp4"},
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

        print(f"    {len(detections)} person detections across {len(set(d['frame_ms'] for d in detections))} frames")

        # Detect faces in person crops
        face_boxes = detect_faces_in_persons(video_path, detections)
        print(f"    {len(face_boxes)} faces detected")

        if not face_boxes:
            # No faces found — mark complete, no upload needed
            conn.execute(
                "UPDATE blur_runs SET status='completed', face_count=0, completed_at=? WHERE video_id=?",
                (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), video_id),
            )
            conn.commit()
            return True

        # Blur faces and re-encode
        output_path = video_path.with_suffix(".blurred.mp4")
        ok = build_ffmpeg_blur(video_path, face_boxes, output_path)
        if not ok or not output_path.exists():
            raise RuntimeError("Blur encoding failed")

        # Upload to S3
        s3_url = upload_to_s3(s3, video_id, output_path)
        print(f"    Uploaded to S3: {s3_url}")

        # Mark complete
        conn.execute(
            "UPDATE blur_runs SET status='completed', face_count=?, s3_url=?, completed_at=? WHERE video_id=?",
            (len(face_boxes), s3_url, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), video_id),
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
    args = parser.parse_args()

    print(f"{BOLD}{'═' * 60}{RESET}")
    print(f"  Face Blur Pipeline")
    print(f"  Machine: {MACHINE_ID}")
    print(f"  S3 Bucket: {S3_BUCKET}")
    print(f"{BOLD}{'═' * 60}{RESET}")

    conn = get_db()
    ensure_blur_runs_table(conn)

    s3 = get_s3_client()

    # Verify S3 access
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f"  {GREEN}S3 bucket accessible{RESET}")
    except Exception as e:
        print(f"  {RED}S3 bucket not accessible: {e}{RESET}")
        sys.exit(1)

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
        video_ids = get_videos_with_persons(conn, limit=10)
        if not video_ids:
            if args.limit > 0:
                break
            time.sleep(args.poll)
            continue

        for video_id in video_ids:
            if not running:
                break

            # Skip if already in S3
            if video_already_blurred(s3, video_id):
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
            if args.limit > 0 and processed >= args.limit:
                running = False
                break

    conn.close()
    print(f"\n{BOLD}Processed {processed} videos.{RESET}")


if __name__ == "__main__":
    main()
