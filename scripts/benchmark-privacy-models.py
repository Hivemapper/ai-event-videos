#!/usr/bin/env python3
"""
Benchmark privacy_v8 plate detection models against the current GDINO approach.

Downloads a sample of recent event videos, extracts frames, and runs each model
to compare detection counts, confidence scores, and inference speed.

Outputs annotated frames to /tmp/privacy-benchmark/ for visual inspection.

Usage:
    python scripts/benchmark-privacy-models.py [--num-events 5] [--frames-per-video 30]
"""

import argparse
import hashlib
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import cv2
import numpy as np
import requests
import torch
from ultralytics import YOLO

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MODELS = {
    "privacy_v8_all":  Path("/Users/as/Downloads/privacy_v8_all.pt"),
    "privacy_v8_asia": Path("/Users/as/Downloads/privacy_v8_asia.pt"),
    "privacy_v8_eu":   Path("/Users/as/Downloads/privacy_v8_eu.pt"),
    "privacy_v8_us":   Path("/Users/as/Downloads/privacy_v8_us.pt"),
}

API_BASE_URL = "https://beemaps.com/api/developer/aievents"
OUTPUT_DIR = Path("/tmp/privacy-benchmark")
VIDEO_CACHE = Path("/tmp/privacy-benchmark/videos")

# Current prod thresholds for comparison
PLATE_MIN_CONFIDENCE = 0.15
PLATE_BOX_PADDING = 0.1

# Colors for each model (BGR)
MODEL_COLORS = {
    "privacy_v8_all":  (0, 255, 0),    # green
    "privacy_v8_asia": (0, 255, 255),   # yellow
    "privacy_v8_eu":   (255, 0, 0),     # blue
    "privacy_v8_us":   (0, 0, 255),     # red
}


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def load_api_key() -> str:
    """Load Bee Maps API key from .env.local."""
    env_path = Path(__file__).resolve().parent.parent / ".env.local"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("BEEMAPS_API_KEY="):
                return line.split("=", 1)[1].strip()
    key = os.environ.get("BEEMAPS_API_KEY", "")
    if not key:
        sys.exit("No BEEMAPS_API_KEY found in .env.local or environment")
    return key


def api_get(url: str, api_key: str, **kwargs) -> requests.Response:
    return requests.get(url, headers={"Authorization": f"Basic {api_key}"}, **kwargs)


def api_post(url: str, api_key: str, body: dict) -> dict:
    resp = requests.post(
        url, json=body, headers={
            "Authorization": f"Basic {api_key}",
            "Content-Type": "application/json",
        },
    )
    resp.raise_for_status()
    return resp.json()


def is_daytime(timestamp_str: str, lat: float, lon: float) -> bool:
    """Check if event occurred during daytime using solar elevation approximation."""
    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    # Day of year
    n = dt.timetuple().tm_yday
    # Solar declination (radians)
    decl = math.radians(-23.44 * math.cos(math.radians(360 / 365 * (n + 10))))
    # Hour angle: approximate solar noon from longitude
    solar_noon_utc = 12.0 - lon / 15.0
    hour = dt.hour + dt.minute / 60.0
    hour_angle = math.radians(15.0 * (hour - solar_noon_utc))
    # Solar elevation
    lat_rad = math.radians(lat)
    sin_elev = (math.sin(lat_rad) * math.sin(decl) +
                math.cos(lat_rad) * math.cos(decl) * math.cos(hour_angle))
    elevation_deg = math.degrees(math.asin(max(-1, min(1, sin_elev))))
    return elevation_deg > 5  # sun at least 5° above horizon


def fetch_sample_events(api_key: str, num: int) -> list[dict]:
    """Fetch random events with video URLs by sampling from random offsets."""
    import random

    events = []
    seen_ids = set()
    event_types = ["HARSH_BRAKING", "HIGH_SPEED", "HARSH_ACCELERATION"]

    for event_type in event_types:
        if len(events) >= num:
            break

        # First, get total count
        probe = api_post(f"{API_BASE_URL}/search", api_key, {
            "startDate": "2026-03-15T00:00:00.000Z",
            "endDate": "2026-04-10T23:59:59.999Z",
            "types": [event_type],
            "limit": 1,
            "offset": 0,
        })
        total = probe.get("pagination", {}).get("total", 0)
        if total == 0:
            continue

        # Pick random offsets within the total
        max_offset = max(0, total - 50)
        attempts = 0
        while len(events) < num and attempts < 10:
            offset = random.randint(0, max_offset)
            data = api_post(f"{API_BASE_URL}/search", api_key, {
                "startDate": "2026-03-15T00:00:00.000Z",
                "endDate": "2026-04-10T23:59:59.999Z",
                "types": [event_type],
                "limit": 50,
                "offset": offset,
            })
            batch = data.get("events", [])
            random.shuffle(batch)
            for e in batch:
                eid = e.get("id", "")
                if eid in seen_ids or not e.get("videoUrl"):
                    continue
                # Filter to daytime only
                loc = e.get("location", {})
                ts = e.get("timestamp", "")
                if ts and loc.get("lat") is not None:
                    if not is_daytime(ts, loc["lat"], loc.get("lon", 0)):
                        continue
                seen_ids.add(eid)
                events.append(e)
                if len(events) >= num:
                    break
            attempts += 1

    random.shuffle(events)
    return events[:num]


def download_video(video_url: str, api_key: str) -> Path | None:
    VIDEO_CACHE.mkdir(parents=True, exist_ok=True)
    hashed = hashlib.md5(video_url.encode()).hexdigest()
    path = VIDEO_CACHE / f"{hashed}.mp4"
    if path.exists():
        return path
    try:
        resp = api_get(video_url, api_key, stream=True, timeout=120)
        resp.raise_for_status()
        with path.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        return path
    except Exception as e:
        print(f"  [!] Download failed: {e}")
        path.unlink(missing_ok=True)
        return None


# ---------------------------------------------------------------------------
# Frame extraction
# ---------------------------------------------------------------------------

def extract_frames(video_path: Path, num_frames: int) -> list[tuple[int, np.ndarray]]:
    """Extract evenly-spaced frames from video. Returns [(frame_idx, bgr_array), ...]."""
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return []

    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if total <= 0:
        cap.release()
        return []

    step = max(1, total // num_frames)
    frames = []

    for i in range(0, total, step):
        if len(frames) >= num_frames:
            break
        cap.set(cv2.CAP_PROP_POS_FRAMES, i)
        ret, frame = cap.read()
        if ret:
            frames.append((i, frame))

    cap.release()
    return frames


# ---------------------------------------------------------------------------
# Model benchmarking
# ---------------------------------------------------------------------------

BLUR_CLASSES = {0, 2}  # face, license-plate — the only classes we want to blur
CLASS_NAMES = {0: "face", 1: "person", 2: "license-plate", 3: "car", 4: "bus",
               5: "truck", 6: "motorcycle", 7: "bicycle"}


def run_model(model: YOLO, frame: np.ndarray, conf: float = 0.15,
              classes: set[int] | None = None) -> list[dict]:
    """Run YOLO model on a single frame and return detections."""
    results = model.predict(frame, conf=conf, imgsz=640, verbose=False)
    detections = []
    for result in results:
        for box in result.boxes:
            cls = int(box.cls[0]) if box.cls is not None else -1
            if classes is not None and cls not in classes:
                continue
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            detections.append({
                "x1": x1, "y1": y1, "x2": x2, "y2": y2,
                "conf": float(box.conf[0]),
                "cls": cls,
                "cls_name": CLASS_NAMES.get(cls, "?"),
            })
    return detections


def draw_detections(frame: np.ndarray, detections: dict[str, list[dict]]) -> np.ndarray:
    """Draw bounding boxes from all models on a single frame, color-coded."""
    annotated = frame.copy()
    for model_name, dets in detections.items():
        color = MODEL_COLORS.get(model_name, (255, 255, 255))
        for det in dets:
            x1, y1, x2, y2 = int(det["x1"]), int(det["y1"]), int(det["x2"]), int(det["y2"])
            cv2.rectangle(annotated, (x1, y1), (x2, y2), color, 2)
            cls_label = det.get("cls_name", "?")
            label = f"{model_name.replace('privacy_v8_', '')} {cls_label}: {det['conf']:.2f}"
            cv2.putText(annotated, label, (x1, y1 - 5),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.4, color, 1)
    return annotated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Benchmark privacy plate models")
    parser.add_argument("--num-events", type=int, default=5,
                        help="Number of events to test (default: 5)")
    parser.add_argument("--frames-per-video", type=int, default=30,
                        help="Frames to sample per video (default: 30)")
    parser.add_argument("--conf", type=float, default=0.15,
                        help="Confidence threshold (default: 0.15)")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    api_key = load_api_key()

    # Determine device
    if torch.cuda.is_available():
        device = "cuda"
    elif torch.backends.mps.is_available():
        device = "mps"
    else:
        device = "cpu"
    print(f"Device: {device}")

    # Load all models
    print("\n=== Loading models ===")
    loaded_models: dict[str, YOLO] = {}
    for name, path in MODELS.items():
        if not path.exists():
            print(f"  SKIP {name}: {path} not found")
            continue
        t0 = time.time()
        m = YOLO(str(path))
        load_time = time.time() - t0
        loaded_models[name] = m
        print(f"  {name}: loaded in {load_time:.2f}s")

    if not loaded_models:
        sys.exit("No models found!")

    # Fetch sample events
    print(f"\n=== Fetching {args.num_events} sample events ===")
    events = fetch_sample_events(api_key, args.num_events)
    print(f"  Got {len(events)} events")

    # Results tracking
    results_summary = []

    for idx, event in enumerate(events):
        event_id = event.get("id", "unknown")
        event_type = event.get("type", "?")
        video_url = event.get("videoUrl")
        country = event.get("country", "??")

        print(f"\n--- Event {idx+1}/{len(events)}: {event_id[:16]}... ({event_type}, {country}) ---")

        if not video_url:
            print("  No video URL, skipping")
            continue

        video_path = download_video(video_url, api_key)
        if not video_path:
            continue

        frames = extract_frames(video_path, args.frames_per_video)
        print(f"  Extracted {len(frames)} frames")

        event_dir = OUTPUT_DIR / event_id[:16]
        event_dir.mkdir(exist_ok=True)

        event_results = {"event_id": event_id, "type": event_type, "country": country}

        for model_name, model in loaded_models.items():
            total_dets = 0
            total_time = 0.0
            all_confs = []

            for frame_idx, frame in frames:
                t0 = time.time()
                dets = run_model(model, frame, conf=args.conf, classes=BLUR_CLASSES)
                elapsed = time.time() - t0
                total_time += elapsed
                total_dets += len(dets)
                all_confs.extend([d["conf"] for d in dets])

            avg_ms = (total_time / len(frames) * 1000) if frames else 0
            avg_conf = sum(all_confs) / len(all_confs) if all_confs else 0

            event_results[model_name] = {
                "detections": total_dets,
                "avg_ms_per_frame": round(avg_ms, 1),
                "avg_confidence": round(avg_conf, 3),
                "total_time_s": round(total_time, 2),
            }
            print(f"  {model_name}: {total_dets} detections, "
                  f"{avg_ms:.1f}ms/frame, avg conf={avg_conf:.3f}")

        # Save annotated frames for a few sample frames (every 10th extracted)
        sample_indices = list(range(0, len(frames), max(1, len(frames) // 5)))[:5]
        for si in sample_indices:
            frame_idx, frame = frames[si]
            all_dets = {}
            for model_name, model in loaded_models.items():
                all_dets[model_name] = run_model(model, frame, conf=args.conf,
                                                  classes=BLUR_CLASSES)
            annotated = draw_detections(frame, all_dets)
            out_path = event_dir / f"frame_{frame_idx:05d}.jpg"
            cv2.imwrite(str(out_path), annotated)

        results_summary.append(event_results)

    # Print summary table
    print("\n" + "=" * 90)
    print("SUMMARY")
    print("=" * 90)
    print(f"{'Event':<18} {'Type':<20} {'Country':<5}", end="")
    for name in loaded_models:
        short = name.replace("privacy_v8_", "")
        print(f" | {short:>5} dets  {short:>5} ms", end="")
    print()
    print("-" * 90)

    totals = {name: {"dets": 0, "time": 0.0, "frames": 0} for name in loaded_models}

    for r in results_summary:
        print(f"{r['event_id'][:16]:<18} {r['type']:<20} {r['country']:<5}", end="")
        for name in loaded_models:
            m = r.get(name, {})
            dets = m.get("detections", 0)
            ms = m.get("avg_ms_per_frame", 0)
            print(f" | {dets:>5}     {ms:>7.1f}", end="")
            totals[name]["dets"] += dets
            totals[name]["time"] += m.get("total_time_s", 0)
        print()

    print("-" * 90)
    print(f"{'TOTAL':<44}", end="")
    for name in loaded_models:
        t = totals[name]
        print(f" | {t['dets']:>5}     {t['time']:>6.1f}s", end="")
    print()

    # Save results JSON
    results_path = OUTPUT_DIR / "results.json"
    with open(results_path, "w") as f:
        json.dump(results_summary, f, indent=2)
    print(f"\nAnnotated frames: {OUTPUT_DIR}/")
    print(f"Results JSON: {results_path}")


if __name__ == "__main__":
    main()
