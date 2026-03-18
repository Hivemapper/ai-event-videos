#!/usr/bin/env python3
"""
M2 Open-Vocabulary Detection Viability Test

Compares YOLO11x (standard COCO-trained) vs YOLO-World (yolov8l-worldv2)
to test whether open-vocabulary detection can find VRU categories that
standard COCO classes cannot: electric scooters, wheelchairs, construction
workers, strollers, traffic cones, etc.

Usage:
    cd /Users/tylerlu/Projects/ai-event-videos
    source .venv/bin/activate
    python scripts/test_open_vocab.py
"""

from __future__ import annotations

import hashlib
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import cv2
import requests
import torch
from ultralytics import YOLO

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE_URL = "https://beemaps.com/api/developer/aievents"

# Standard COCO label map (same as pipeline worker)
COCO_LABEL_MAP: dict[str, str] = {
    "person": "person",
    "bicycle": "bicycle",
    "motorcycle": "motorcycle",
    "car": "car",
    "truck": "truck",
    "bus": "bus",
    "train": "train",
    "traffic light": "traffic light",
    "stop sign": "stop sign",
    "cat": "cat",
    "dog": "dog",
    "skateboard": "skateboard",
}

# YOLO-World custom classes (open vocabulary)
WORLD_CLASSES = [
    "person",
    "car",
    "truck",
    "bus",
    "motorcycle",
    "bicycle",
    "traffic light",
    "stop sign",
    "electric scooter",
    "wheelchair",
    "stroller",
    "construction worker",
    "traffic cone",
    "child",
    "crosswalk",
    "speed limit sign",
]

# Classes that YOLO-World can detect but standard COCO YOLO cannot.
# Note: "traffic cone" is confirmed NOT in the COCO-80 class list (class 75 is
# "vase", not "traffic cone"), so it belongs here as an open-vocab class.
OPEN_VOCAB_CLASSES = {
    "electric scooter",
    "wheelchair",
    "stroller",
    "construction worker",
    "traffic cone",
    "child",
    "crosswalk",
    "speed limit sign",
}

# Confidence thresholds
YOLO11X_CONF = 0.25
WORLD_CONF = 0.15

# Event types to fetch -- diverse mix for broad coverage
EVENT_TYPES_TO_FETCH: list[tuple[str, int]] = [
    ("HARSH_BRAKING", 3),
    ("SWERVING", 3),
    ("HIGH_SPEED", 2),
    ("STOP_SIGN_VIOLATION", 2),
]

FRAMES_PER_VIDEO = 5
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "pipeline-video-cache"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_api_key() -> str:
    """Load BEEMAPS_API_KEY from environment or .env.local."""
    key = os.environ.get("BEEMAPS_API_KEY")
    if key:
        return key if key.startswith("Basic ") else f"Basic {key}"

    env_path = PROJECT_ROOT / ".env.local"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            name, _, value = line.partition("=")
            if name.strip() == "BEEMAPS_API_KEY":
                value = value.strip()
                return value if value.startswith("Basic ") else f"Basic {value}"

    print("ERROR: BEEMAPS_API_KEY not found in environment or .env.local", file=sys.stderr)
    sys.exit(1)


def get_device() -> str:
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def download_video(video_url: str) -> Path | None:
    """Download video to cache, return path. Returns None on failure."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    hashed = hashlib.md5(video_url.encode("utf-8")).hexdigest()
    path = CACHE_DIR / f"{hashed}.mp4"

    if path.exists():
        path.touch()
        return path

    try:
        with requests.get(video_url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return path
    except Exception as exc:
        print(f"    [!] Video download failed: {exc}")
        if path.exists():
            path.unlink(missing_ok=True)
        return None


def extract_frames(video_path: Path, num_frames: int) -> list[Any]:
    """Extract evenly spaced frames from a video."""
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        print(f"    [!] Could not open video: {video_path}")
        return []

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if total_frames <= 0:
        cap.release()
        return []

    # Evenly spaced frame indices (avoid first/last frame for better content)
    margin = max(1, total_frames // 20)
    usable = total_frames - 2 * margin
    if usable <= 0:
        indices = [total_frames // 2]
    elif num_frames == 1:
        indices = [total_frames // 2]
    else:
        step = usable / (num_frames - 1) if num_frames > 1 else 0
        indices = [margin + int(i * step) for i in range(num_frames)]

    frames = []
    for idx in indices:
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok, frame = cap.read()
        if ok:
            frames.append((idx, frame))

    cap.release()
    return frames


@dataclass
class Detection:
    class_name: str
    confidence: float


@dataclass
class FrameResult:
    event_id: str
    frame_index: int
    yolo11x_detections: list[Detection] = field(default_factory=list)
    world_detections: list[Detection] = field(default_factory=list)


def run_yolo11x(model: YOLO, frame: Any, device: str) -> list[Detection]:
    """Run standard YOLO11x and return detections mapped through COCO_LABEL_MAP."""
    results = model.predict(
        frame, verbose=False, conf=YOLO11X_CONF, device=device, imgsz=960
    )
    detections = []
    if results and results[0].boxes is not None:
        result = results[0]
        names = result.names
        for box in result.boxes:
            cls_idx = int(box.cls[0].item())
            cls_name = names.get(cls_idx, str(cls_idx))
            mapped = COCO_LABEL_MAP.get(cls_name)
            if mapped is None:
                continue
            conf = float(box.conf[0].item())
            detections.append(Detection(class_name=mapped, confidence=conf))
    return detections


def run_world(model: YOLO, frame: Any, device: str) -> list[Detection]:
    """Run YOLO-World with custom open-vocab classes."""
    results = model.predict(
        frame, verbose=False, conf=WORLD_CONF, device=device, imgsz=960
    )
    detections = []
    if results and results[0].boxes is not None:
        result = results[0]
        names = result.names
        for box in result.boxes:
            cls_idx = int(box.cls[0].item())
            cls_name = names.get(cls_idx, str(cls_idx))
            conf = float(box.conf[0].item())
            detections.append(Detection(class_name=cls_name, confidence=conf))
    return detections


def summarize_detections(detections: list[Detection]) -> dict[str, tuple[int, float]]:
    """Return {class_name: (count, avg_confidence)}."""
    counts: dict[str, list[float]] = defaultdict(list)
    for d in detections:
        counts[d.class_name].append(d.confidence)
    return {
        name: (len(confs), sum(confs) / len(confs))
        for name, confs in sorted(counts.items())
    }


def print_frame_comparison(result: FrameResult) -> None:
    """Print side-by-side comparison for a single frame."""
    yolo_summary = summarize_detections(result.yolo11x_detections)
    world_summary = summarize_detections(result.world_detections)

    print(f"\n  Frame #{result.frame_index}:")
    print(f"  {'YOLO11x (COCO)':<40} {'YOLO-World (Open Vocab)':<40}")
    print(f"  {'-' * 40} {'-' * 40}")

    # Collect all class names from both
    all_classes = sorted(set(yolo_summary.keys()) | set(world_summary.keys()))

    for cls in all_classes:
        yolo_str = ""
        world_str = ""
        marker = ""

        if cls in yolo_summary:
            count, avg_conf = yolo_summary[cls]
            yolo_str = f"{cls}: {count}x @ {avg_conf:.3f}"
        else:
            yolo_str = f"{'':>30}"

        if cls in world_summary:
            count, avg_conf = world_summary[cls]
            world_str = f"{cls}: {count}x @ {avg_conf:.3f}"
        else:
            world_str = ""

        # Highlight open-vocab classes that YOLO-World found but YOLO11x couldn't
        if cls in OPEN_VOCAB_CLASSES and cls in world_summary:
            marker = " ** OPEN-VOCAB **"

        print(f"  {yolo_str:<40} {world_str:<40}{marker}")


# ---------------------------------------------------------------------------
# API fetching
# ---------------------------------------------------------------------------

def fetch_events(api_key: str) -> list[dict[str, Any]]:
    """Fetch 10 diverse events from the Bee Maps API."""
    all_events: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    # Use a recent date range (last 30 days)
    from datetime import datetime, timedelta, timezone
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=30)
    start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    for event_type, count in EVENT_TYPES_TO_FETCH:
        print(f"  Fetching {count} {event_type} events...")
        try:
            resp = requests.post(
                f"{API_BASE_URL}/search",
                json={
                    "startDate": start_str,
                    "endDate": end_str,
                    "types": [event_type],
                    "limit": count * 3,  # fetch extra in case some fail
                    "offset": 0,
                },
                headers={
                    "Content-Type": "application/json",
                    "Authorization": api_key,
                },
                timeout=60,
            )
            resp.raise_for_status()
            payload = resp.json()
            events = payload.get("events", [])
            added = 0
            for event in events:
                if added >= count:
                    break
                eid = event.get("id", "")
                if eid not in seen_ids and event.get("videoUrl"):
                    seen_ids.add(eid)
                    all_events.append(event)
                    added += 1
            print(f"    Got {added} events (API returned {len(events)} total)")
        except Exception as exc:
            print(f"    [!] Failed to fetch {event_type}: {exc}")

    return all_events


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print("=" * 80)
    print("M2 Open-Vocabulary Detection Viability Test")
    print("=" * 80)
    print()

    # Load API key
    api_key = load_api_key()
    print("[1/5] API key loaded")

    # Get device
    device = get_device()
    print(f"[2/5] Compute device: {device}")

    # Load models
    print("[3/5] Loading models...")
    yolo11x_path = PROJECT_ROOT / "yolo11x.pt"
    world_path = PROJECT_ROOT / "yolov8l-worldv2.pt"

    if not yolo11x_path.exists():
        print(f"  ERROR: {yolo11x_path} not found", file=sys.stderr)
        return 1
    if not world_path.exists():
        print(f"  ERROR: {world_path} not found", file=sys.stderr)
        return 1

    print(f"  Loading YOLO11x from {yolo11x_path.name}...")
    model_yolo11x = YOLO(str(yolo11x_path))
    model_yolo11x.to(device)

    print(f"  Loading YOLO-World from {world_path.name}...")
    model_world = YOLO(str(world_path))
    model_world.set_classes(WORLD_CLASSES)
    model_world.to(device)
    print("  Models loaded.")

    # Fetch events
    print()
    print("[4/5] Fetching events from Bee Maps API...")
    events = fetch_events(api_key)
    print(f"  Total events to process: {len(events)}")

    if not events:
        print("  No events fetched. Exiting.")
        return 1

    # Process each event
    print()
    print("[5/5] Running detection comparison...")
    print()

    all_results: list[FrameResult] = []
    # Track open-vocab stats across all frames
    open_vocab_stats: dict[str, list[float]] = defaultdict(list)
    open_vocab_frame_counts: dict[str, int] = defaultdict(int)

    for i, event in enumerate(events):
        event_id = event["id"]
        event_type = event.get("type", "UNKNOWN")
        video_url = event.get("videoUrl", "")

        print(f"{'=' * 70}")
        print(f"Event {i + 1}/{len(events)}: {event_id} ({event_type})")
        print(f"{'=' * 70}")

        if not video_url:
            print("  [!] No video URL, skipping.")
            continue

        # Download video
        print(f"  Downloading video...")
        t0 = time.time()
        video_path = download_video(video_url)
        if video_path is None:
            print("  [!] Download failed, skipping event.")
            continue
        dl_time = time.time() - t0
        size_mb = video_path.stat().st_size / (1024 * 1024)
        print(f"  Downloaded: {size_mb:.1f} MB in {dl_time:.1f}s")

        # Extract frames
        frames = extract_frames(video_path, FRAMES_PER_VIDEO)
        if not frames:
            print("  [!] No frames extracted, skipping.")
            continue
        print(f"  Extracted {len(frames)} frames")

        # Run both models on each frame
        for frame_idx, frame_data in frames:
            fr = FrameResult(event_id=event_id, frame_index=frame_idx)

            # YOLO11x
            fr.yolo11x_detections = run_yolo11x(model_yolo11x, frame_data, device)

            # YOLO-World
            fr.world_detections = run_world(model_world, frame_data, device)

            all_results.append(fr)

            # Track open-vocab detections
            world_summary = summarize_detections(fr.world_detections)
            for cls_name in OPEN_VOCAB_CLASSES:
                if cls_name in world_summary:
                    count, avg_conf = world_summary[cls_name]
                    open_vocab_stats[cls_name].append(avg_conf)
                    open_vocab_frame_counts[cls_name] += 1

            # Print comparison
            print_frame_comparison(fr)

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print()
    print(f"Total frames analyzed: {len(all_results)}")
    print(f"Total events processed: {len(events)}")
    print()

    # Overall detection counts for both models
    yolo11x_total: dict[str, int] = defaultdict(int)
    world_total: dict[str, int] = defaultdict(int)
    for fr in all_results:
        for d in fr.yolo11x_detections:
            yolo11x_total[d.class_name] += 1
        for d in fr.world_detections:
            world_total[d.class_name] += 1

    print("--- Shared classes (both models can detect) ---")
    shared_classes = sorted(set(COCO_LABEL_MAP.values()) & set(WORLD_CLASSES))
    print(f"  {'Class':<25} {'YOLO11x count':<18} {'World count':<18}")
    print(f"  {'-' * 25} {'-' * 18} {'-' * 18}")
    for cls in shared_classes:
        y_count = yolo11x_total.get(cls, 0)
        w_count = world_total.get(cls, 0)
        print(f"  {cls:<25} {y_count:<18} {w_count:<18}")
    print(f"\n  Note: YOLO-World ran at conf={WORLD_CONF} vs YOLO11x at conf={YOLO11X_CONF}."
          " Shared-class counts are not directly comparable.")

    print()
    print("--- Open-vocabulary classes (YOLO-World only) ---")
    print(f"  {'Class':<25} {'Frames detected':<18} {'Avg confidence':<18} {'Verdict':<15}")
    print(f"  {'-' * 25} {'-' * 18} {'-' * 18} {'-' * 15}")

    for cls in sorted(OPEN_VOCAB_CLASSES):
        frame_count = open_vocab_frame_counts.get(cls, 0)
        if frame_count > 0:
            avg_conf = sum(open_vocab_stats[cls]) / len(open_vocab_stats[cls])
        else:
            avg_conf = 0.0

        # Verdict logic
        if frame_count >= 5 and avg_conf > 0.3:
            verdict = "VIABLE"
        elif frame_count > 0:
            verdict = "MARGINAL"
        else:
            verdict = "NOT DETECTED"

        conf_str = f"{avg_conf:.3f}" if frame_count > 0 else "N/A"
        print(f"  {cls:<25} {frame_count:<18} {conf_str:<18} {verdict:<15}")

    print()
    print("--- Verdict Legend ---")
    print("  VIABLE:     Detected in 5+ frames with avg confidence > 0.3")
    print("  MARGINAL:   Detected but low confidence or rare")
    print("  NOT DETECTED: Never detected in sample (may appear in other scenes)")
    print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
