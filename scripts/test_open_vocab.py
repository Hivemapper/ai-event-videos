#!/usr/bin/env python3
"""
M2 Open-Vocabulary Detection Viability Test

Compares four models:
  1. YOLO11x (standard COCO-trained)
  2. YOLO-World (yolov8l-worldv2, open vocabulary)
  3. Grounding DINO (IDEA-Research/grounding-dino-base, open vocabulary)
  4. MM Grounding DINO (openmmlab-community/mm_grounding_dino_large_all, open vocabulary)

Tests whether open-vocabulary detection can find VRU categories that
standard COCO classes cannot: electric scooters, wheelchairs, construction
workers, strollers, traffic cones, etc.

NOTE: MM-GDINO-large-all was trained on broad data including COCO, so it is
NOT a zero-shot model for shared classes (person, car, etc.). Open-vocab
class comparisons (stroller, electric scooter, etc.) are more meaningful.

Fetches events from geo-filtered US city polygons for diverse coverage.

Memory: ~4-6 GB with all 4 models loaded simultaneously. 16 GB+ recommended.

Usage:
    cd /Users/tylerlu/Projects/ai-event-videos
    source .venv/bin/activate
    pip install --upgrade transformers  # MM-GDINO requires recent transformers
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
from PIL import Image
from ultralytics import YOLO

# Try to import transformers for Grounding DINO / MM-GDINO (graceful fallback)
GDINO_AVAILABLE = False
MM_GDINO_AVAILABLE = False
try:
    from transformers import AutoModelForZeroShotObjectDetection, AutoProcessor

    GDINO_AVAILABLE = True
    # MM Grounding DINO uses the same Auto classes but needs a newer transformers
    # version that ships the MMGroundingDino model implementation.
    try:
        from transformers import MMGroundingDinoForObjectDetection  # noqa: F401

        MM_GDINO_AVAILABLE = True
    except ImportError:
        print("[INFO] transformers version does not include MM Grounding DINO support.")
        print("       Upgrade with: pip install --upgrade transformers")
except ImportError:
    print("[INFO] transformers not installed -- Grounding DINO & MM-GDINO will be skipped.")
    print("       Install with: pip install transformers")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE_URL = "https://beemaps.com/api/developer/aievents"

# US cities with bounding-box polygons (lon/lat pairs, closed ring)
US_CITIES: list[tuple[str, list[list[float]]]] = [
    ("LA", [[-118.40, 33.90], [-118.15, 33.90], [-118.15, 34.10], [-118.40, 34.10], [-118.40, 33.90]]),
    ("Chicago", [[-87.75, 41.80], [-87.60, 41.80], [-87.60, 41.95], [-87.75, 41.95], [-87.75, 41.80]]),
    ("Austin", [[-97.80, 30.20], [-97.65, 30.20], [-97.65, 30.35], [-97.80, 30.35], [-97.80, 30.20]]),
    ("Denver", [[-105.05, 39.65], [-104.85, 39.65], [-104.85, 39.80], [-105.05, 39.80], [-105.05, 39.65]]),
    ("NYC", [[-74.05, 40.68], [-73.90, 40.68], [-73.90, 40.82], [-74.05, 40.82], [-74.05, 40.68]]),
    ("SF", [[-122.52, 37.70], [-122.35, 37.70], [-122.35, 37.82], [-122.52, 37.82], [-122.52, 37.70]]),
]

EVENTS_PER_CITY = 3

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

# Grounding DINO prompt (period-separated labels)
GDINO_TEXT_PROMPT = (
    "person. car. truck. bus. motorcycle. bicycle. traffic light. stop sign. "
    "electric scooter. wheelchair. stroller. construction worker. traffic cone. "
    "child. crosswalk. speed limit sign."
)
GDINO_BOX_THRESHOLD = 0.25
GDINO_TEXT_THRESHOLD = 0.25

# Classes that open-vocab models can detect but standard COCO YOLO cannot.
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
    gdino_detections: list[Detection] = field(default_factory=list)
    mm_gdino_detections: list[Detection] = field(default_factory=list)


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


def run_gdino(
    processor: Any,
    model: Any,
    frame: Any,
    gdino_device: str,
) -> list[Detection]:
    """Run Grounding DINO on a BGR cv2 frame and return detections."""
    # Convert BGR numpy array to RGB PIL Image
    pil_image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
    height, width = frame.shape[:2]

    inputs = processor(
        images=pil_image, text=GDINO_TEXT_PROMPT, return_tensors="pt"
    )
    # Move inputs to the correct device
    inputs = {k: v.to(gdino_device) if hasattr(v, "to") else v for k, v in inputs.items()}

    with torch.no_grad():
        outputs = model(**inputs)

    results = processor.post_process_grounded_object_detection(
        outputs,
        inputs["input_ids"],
        threshold=GDINO_BOX_THRESHOLD,
        text_threshold=GDINO_TEXT_THRESHOLD,
        target_sizes=[(height, width)],
    )

    detections = []
    if results:
        r = results[0]
        labels = r.get("text_labels", r.get("labels", []))
        scores = r.get("scores", torch.tensor([]))
        for label, score in zip(labels, scores):
            conf = float(score.item()) if hasattr(score, "item") else float(score)
            detections.append(Detection(class_name=label.strip(), confidence=conf))

    return detections


# MM Grounding DINO text labels — list-of-lists format accepted by the
# processor, which internally converts them to period-separated text.
MM_GDINO_TEXT_LABELS = [
    [
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
]


def run_mm_gdino(
    processor: Any,
    model: Any,
    frame: Any,
    mm_gdino_device: str,
) -> list[Detection]:
    """Run MM Grounding DINO on a BGR cv2 frame and return detections."""
    # Convert BGR numpy array to RGB PIL Image
    pil_image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
    height, width = frame.shape[:2]

    inputs = processor(
        images=pil_image, text=MM_GDINO_TEXT_LABELS, return_tensors="pt"
    )
    # Move inputs to the correct device
    inputs = {k: v.to(mm_gdino_device) if hasattr(v, "to") else v for k, v in inputs.items()}

    with torch.no_grad():
        outputs = model(**inputs)

    results = processor.post_process_grounded_object_detection(
        outputs,
        inputs["input_ids"],
        threshold=GDINO_BOX_THRESHOLD,
        text_threshold=GDINO_TEXT_THRESHOLD,
        target_sizes=[(height, width)],
    )

    detections = []
    if results:
        r = results[0]
        labels = r.get("text_labels", r.get("labels", []))
        scores = r.get("scores", torch.tensor([]))
        for label, score in zip(labels, scores):
            conf = float(score.item()) if hasattr(score, "item") else float(score)
            detections.append(Detection(class_name=label.strip(), confidence=conf))

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


COL_WIDTH = 35


def _fmt_det(cls: str, summary: dict[str, tuple[int, float]]) -> str:
    """Format a single class detection string for a column."""
    if cls in summary:
        count, avg_conf = summary[cls]
        return f"{cls}: {count}x @ {avg_conf:.3f}"
    return ""


def print_frame_comparison(
    result: FrameResult, gdino_enabled: bool, mm_gdino_enabled: bool
) -> None:
    """Print side-by-side comparison for a single frame (2-4 columns)."""
    yolo_summary = summarize_detections(result.yolo11x_detections)
    world_summary = summarize_detections(result.world_detections)
    gdino_summary = summarize_detections(result.gdino_detections) if gdino_enabled else {}
    mm_gdino_summary = (
        summarize_detections(result.mm_gdino_detections) if mm_gdino_enabled else {}
    )

    print(f"\n  Frame #{result.frame_index}:")
    # Build header columns
    headers = [f"{'YOLO11x (COCO)':<{COL_WIDTH}}", f"{'YOLO-World (Open Vocab)':<{COL_WIDTH}}"]
    if gdino_enabled:
        headers.append(f"{'GDINO (Open Vocab)':<{COL_WIDTH}}")
    if mm_gdino_enabled:
        headers.append(f"{'MM-GDINO (Open Vocab)':<{COL_WIDTH}}")
    print(f"  {' '.join(headers)}")
    print(f"  {' '.join('-' * COL_WIDTH for _ in headers)}")

    # Collect all class names from all models
    all_classes = sorted(
        set(yolo_summary.keys())
        | set(world_summary.keys())
        | set(gdino_summary.keys())
        | set(mm_gdino_summary.keys())
    )

    for cls in all_classes:
        cols = [
            f"{_fmt_det(cls, yolo_summary):<{COL_WIDTH}}",
            f"{_fmt_det(cls, world_summary):<{COL_WIDTH}}",
        ]
        if gdino_enabled:
            cols.append(f"{_fmt_det(cls, gdino_summary):<{COL_WIDTH}}")
        if mm_gdino_enabled:
            cols.append(f"{_fmt_det(cls, mm_gdino_summary):<{COL_WIDTH}}")

        # Highlight open-vocab classes found by any open-vocab model
        marker = ""
        if cls in OPEN_VOCAB_CLASSES and (
            cls in world_summary or cls in gdino_summary or cls in mm_gdino_summary
        ):
            marker = " ** OPEN-VOCAB **"

        print(f"  {' '.join(cols)}{marker}")


# ---------------------------------------------------------------------------
# API fetching
# ---------------------------------------------------------------------------

def fetch_events(api_key: str) -> list[dict[str, Any]]:
    """Fetch events from US city geo-polygons via the Bee Maps API."""
    all_events: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    # Date range: March 1 to March 18, 2026 (17 days, under 31-day API max)
    start_str = "2026-03-01T00:00:00.000Z"
    end_str = "2026-03-18T23:59:59.000Z"

    for city_name, polygon in US_CITIES:
        print(f"  Fetching up to {EVENTS_PER_CITY} events from {city_name}...")
        try:
            resp = requests.post(
                f"{API_BASE_URL}/search",
                json={
                    "startDate": start_str,
                    "endDate": end_str,
                    "limit": EVENTS_PER_CITY * 3,  # fetch extra in case some lack video
                    "polygon": polygon,
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
                if added >= EVENTS_PER_CITY:
                    break
                eid = event.get("id", "")
                if eid not in seen_ids and event.get("videoUrl"):
                    seen_ids.add(eid)
                    all_events.append(event)
                    added += 1
            print(f"    Got {added} events (API returned {len(events)} total)")
        except Exception as exc:
            print(f"    [!] Failed to fetch events for {city_name}: {exc}")

    return all_events


# ---------------------------------------------------------------------------
# Grounding DINO loader
# ---------------------------------------------------------------------------

def load_gdino() -> tuple[Any, Any, str] | None:
    """
    Attempt to load Grounding DINO model and processor.
    Returns (processor, model, device_str) or None on failure.
    Tries MPS first, falls back to CPU.
    """
    if not GDINO_AVAILABLE:
        return None

    model_name = "IDEA-Research/grounding-dino-base"
    print(f"  Loading Grounding DINO ({model_name})...")

    try:
        processor = AutoProcessor.from_pretrained(model_name)
        model = AutoModelForZeroShotObjectDetection.from_pretrained(model_name)
        model.eval()

        # Try MPS first
        gdino_device = "cpu"
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            try:
                model = model.to("mps")
                # Quick smoke test to ensure MPS works
                dummy_img = Image.new("RGB", (64, 64))
                dummy_inputs = processor(images=dummy_img, text="test.", return_tensors="pt")
                dummy_inputs = {
                    k: v.to("mps") if hasattr(v, "to") else v
                    for k, v in dummy_inputs.items()
                }
                with torch.no_grad():
                    model(**dummy_inputs)
                gdino_device = "mps"
                print(f"    Grounding DINO running on MPS")
            except Exception as mps_exc:
                print(f"    [!] MPS failed for GDINO ({mps_exc}), falling back to CPU")
                model = model.to("cpu")
                gdino_device = "cpu"
        else:
            model = model.to("cpu")

        if gdino_device == "cpu":
            print(f"    Grounding DINO running on CPU")

        return processor, model, gdino_device
    except Exception as exc:
        print(f"    [!] Failed to load Grounding DINO: {exc}")
        print(f"    Continuing with YOLO models only.")
        return None


# ---------------------------------------------------------------------------
# MM Grounding DINO loader
# ---------------------------------------------------------------------------

def load_mm_gdino() -> tuple[Any, Any, str] | None:
    """
    Attempt to load MM Grounding DINO model and processor.
    Returns (processor, model, device_str) or None on failure.
    Tries MPS first, falls back to CPU.
    """
    if not MM_GDINO_AVAILABLE:
        return None

    model_name = "openmmlab-community/mm_grounding_dino_large_all"
    print(f"  Loading MM Grounding DINO ({model_name})...")

    try:
        processor = AutoProcessor.from_pretrained(model_name)
        model = AutoModelForZeroShotObjectDetection.from_pretrained(model_name)
        model.eval()

        # Try MPS first
        mm_gdino_device = "cpu"
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            try:
                model = model.to("mps")
                # Quick smoke test to ensure MPS works
                dummy_img = Image.new("RGB", (64, 64))
                dummy_inputs = processor(
                    images=dummy_img, text=[["test"]], return_tensors="pt"
                )
                dummy_inputs = {
                    k: v.to("mps") if hasattr(v, "to") else v
                    for k, v in dummy_inputs.items()
                }
                with torch.no_grad():
                    model(**dummy_inputs)
                mm_gdino_device = "mps"
                print(f"    MM Grounding DINO running on MPS")
            except Exception as mps_exc:
                print(f"    [!] MPS failed for MM-GDINO ({mps_exc}), falling back to CPU")
                model = model.to("cpu")
                mm_gdino_device = "cpu"
        else:
            model = model.to("cpu")

        if mm_gdino_device == "cpu":
            print(f"    MM Grounding DINO running on CPU")

        return processor, model, mm_gdino_device
    except Exception as exc:
        print(f"    [!] Failed to load MM Grounding DINO: {exc}")
        print(f"    Continuing without MM-GDINO.")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print("=" * 80)
    print("M2 Open-Vocabulary Detection Viability Test")
    print("  Models: YOLO11x  |  YOLO-World  |  GDINO  |  MM-GDINO")
    print("  Data:   Geo-filtered US city events")
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

    # Attempt to load Grounding DINO
    gdino_result = load_gdino()
    gdino_enabled = gdino_result is not None
    if gdino_enabled:
        gdino_processor, gdino_model, gdino_device = gdino_result
    else:
        gdino_processor = gdino_model = gdino_device = None

    # Attempt to load MM Grounding DINO
    mm_gdino_result = load_mm_gdino()
    mm_gdino_enabled = mm_gdino_result is not None
    if mm_gdino_enabled:
        mm_gdino_processor, mm_gdino_model, mm_gdino_device = mm_gdino_result
    else:
        mm_gdino_processor = mm_gdino_model = mm_gdino_device = None

    model_count = 2 + int(gdino_enabled) + int(mm_gdino_enabled)
    print(f"  {model_count} model(s) loaded.")

    # Fetch events
    print()
    print("[4/5] Fetching events from Bee Maps API (US city geo-filters)...")
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
    # Track open-vocab stats across all frames per model
    world_open_vocab_stats: dict[str, list[float]] = defaultdict(list)
    world_open_vocab_frame_counts: dict[str, int] = defaultdict(int)
    gdino_open_vocab_stats: dict[str, list[float]] = defaultdict(list)
    gdino_open_vocab_frame_counts: dict[str, int] = defaultdict(int)
    mm_gdino_open_vocab_stats: dict[str, list[float]] = defaultdict(list)
    mm_gdino_open_vocab_frame_counts: dict[str, int] = defaultdict(int)

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

        # Run all models on each frame
        for frame_idx, frame_data in frames:
            fr = FrameResult(event_id=event_id, frame_index=frame_idx)

            # YOLO11x
            fr.yolo11x_detections = run_yolo11x(model_yolo11x, frame_data, device)

            # YOLO-World
            fr.world_detections = run_world(model_world, frame_data, device)

            # Grounding DINO
            if gdino_enabled:
                try:
                    fr.gdino_detections = run_gdino(
                        gdino_processor, gdino_model, frame_data, gdino_device
                    )
                except Exception as exc:
                    print(f"    [!] GDINO inference failed on frame {frame_idx}: {exc}")
                    fr.gdino_detections = []

            # MM Grounding DINO
            if mm_gdino_enabled:
                try:
                    fr.mm_gdino_detections = run_mm_gdino(
                        mm_gdino_processor, mm_gdino_model, frame_data, mm_gdino_device
                    )
                except Exception as exc:
                    print(f"    [!] MM-GDINO inference failed on frame {frame_idx}: {exc}")
                    fr.mm_gdino_detections = []

            all_results.append(fr)

            # Track open-vocab detections for YOLO-World
            world_summary = summarize_detections(fr.world_detections)
            for cls_name in OPEN_VOCAB_CLASSES:
                if cls_name in world_summary:
                    count, avg_conf = world_summary[cls_name]
                    world_open_vocab_stats[cls_name].append(avg_conf)
                    world_open_vocab_frame_counts[cls_name] += 1

            # Track open-vocab detections for Grounding DINO
            if gdino_enabled:
                gdino_summary = summarize_detections(fr.gdino_detections)
                for cls_name in OPEN_VOCAB_CLASSES:
                    if cls_name in gdino_summary:
                        count, avg_conf = gdino_summary[cls_name]
                        gdino_open_vocab_stats[cls_name].append(avg_conf)
                        gdino_open_vocab_frame_counts[cls_name] += 1

            # Track open-vocab detections for MM Grounding DINO
            if mm_gdino_enabled:
                mm_gdino_summary = summarize_detections(fr.mm_gdino_detections)
                for cls_name in OPEN_VOCAB_CLASSES:
                    if cls_name in mm_gdino_summary:
                        count, avg_conf = mm_gdino_summary[cls_name]
                        mm_gdino_open_vocab_stats[cls_name].append(avg_conf)
                        mm_gdino_open_vocab_frame_counts[cls_name] += 1

            # Print comparison
            print_frame_comparison(fr, gdino_enabled, mm_gdino_enabled)

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
    print(f"Cities queried: {', '.join(c[0] for c in US_CITIES)}")
    model_names = ["YOLO11x", "YOLO-World"]
    if gdino_enabled:
        model_names.append("Grounding DINO")
    if mm_gdino_enabled:
        model_names.append("MM Grounding DINO")
    print(f"Models: {', '.join(model_names)}")
    print()

    # Overall detection counts for all models
    yolo11x_total: dict[str, int] = defaultdict(int)
    world_total: dict[str, int] = defaultdict(int)
    gdino_total: dict[str, int] = defaultdict(int)
    mm_gdino_total: dict[str, int] = defaultdict(int)
    for fr in all_results:
        for d in fr.yolo11x_detections:
            yolo11x_total[d.class_name] += 1
        for d in fr.world_detections:
            world_total[d.class_name] += 1
        for d in fr.gdino_detections:
            gdino_total[d.class_name] += 1
        for d in fr.mm_gdino_detections:
            mm_gdino_total[d.class_name] += 1

    print("--- Shared classes (all models can detect) ---")
    shared_classes = sorted(set(COCO_LABEL_MAP.values()) & set(WORLD_CLASSES))
    # Build header dynamically
    hdr = f"  {'Class':<25} {'YOLO11x':<14} {'World':<14}"
    sep = f"  {'-' * 25} {'-' * 14} {'-' * 14}"
    if gdino_enabled:
        hdr += f" {'GDINO':<14}"
        sep += f" {'-' * 14}"
    if mm_gdino_enabled:
        hdr += f" {'MM-GDINO':<14}"
        sep += f" {'-' * 14}"
    print(hdr)
    print(sep)
    for cls in shared_classes:
        row = f"  {cls:<25} {yolo11x_total.get(cls, 0):<14} {world_total.get(cls, 0):<14}"
        if gdino_enabled:
            row += f" {gdino_total.get(cls, 0):<14}"
        if mm_gdino_enabled:
            row += f" {mm_gdino_total.get(cls, 0):<14}"
        print(row)

    print(f"\n  Note: Confidence thresholds differ -- YOLO11x={YOLO11X_CONF}, "
          f"World={WORLD_CONF}, GDINO/MM-GDINO box/text="
          f"{GDINO_BOX_THRESHOLD}/{GDINO_TEXT_THRESHOLD}."
          " Counts are not directly comparable.")
    if mm_gdino_enabled:
        print("  Note: MM-GDINO-large-all was trained on COCO data — shared-class"
              " counts are NOT zero-shot. Open-vocab classes are more meaningful.")

    print()
    print("--- Open-vocabulary classes (YOLO-World, GDINO, MM-GDINO) ---")
    # Build header dynamically
    ov_hdr = f"  {'Class':<22} {'World frm':<12} {'World conf':<12}"
    ov_sep = f"  {'-' * 22} {'-' * 12} {'-' * 12}"
    if gdino_enabled:
        ov_hdr += f" {'GDINO frm':<12} {'GDINO conf':<12}"
        ov_sep += f" {'-' * 12} {'-' * 12}"
    if mm_gdino_enabled:
        ov_hdr += f" {'MM-GD frm':<12} {'MM-GD conf':<12}"
        ov_sep += f" {'-' * 12} {'-' * 12}"
    ov_hdr += f" {'Verdict':<15}"
    ov_sep += f" {'-' * 15}"
    print(ov_hdr)
    print(ov_sep)

    for cls in sorted(OPEN_VOCAB_CLASSES):
        w_frame_count = world_open_vocab_frame_counts.get(cls, 0)
        w_avg_conf = (
            sum(world_open_vocab_stats[cls]) / len(world_open_vocab_stats[cls])
            if w_frame_count > 0
            else 0.0
        )

        g_frame_count = gdino_open_vocab_frame_counts.get(cls, 0) if gdino_enabled else 0
        g_avg_conf = (
            sum(gdino_open_vocab_stats[cls]) / len(gdino_open_vocab_stats[cls])
            if g_frame_count > 0
            else 0.0
        )

        mg_frame_count = (
            mm_gdino_open_vocab_frame_counts.get(cls, 0) if mm_gdino_enabled else 0
        )
        mg_avg_conf = (
            sum(mm_gdino_open_vocab_stats[cls]) / len(mm_gdino_open_vocab_stats[cls])
            if mg_frame_count > 0
            else 0.0
        )

        # Verdict: best of all open-vocab models
        best_frames = max(w_frame_count, g_frame_count, mg_frame_count)
        best_conf = max(w_avg_conf, g_avg_conf, mg_avg_conf)

        # Verdict logic
        if best_frames >= 5 and best_conf > 0.3:
            verdict = "VIABLE"
        elif best_frames > 0:
            verdict = "MARGINAL"
        else:
            verdict = "NOT DETECTED"

        w_conf_str = f"{w_avg_conf:.3f}" if w_frame_count > 0 else "N/A"
        row = f"  {cls:<22} {w_frame_count:<12} {w_conf_str:<12}"
        if gdino_enabled:
            g_conf_str = f"{g_avg_conf:.3f}" if g_frame_count > 0 else "N/A"
            row += f" {g_frame_count:<12} {g_conf_str:<12}"
        if mm_gdino_enabled:
            mg_conf_str = f"{mg_avg_conf:.3f}" if mg_frame_count > 0 else "N/A"
            row += f" {mg_frame_count:<12} {mg_conf_str:<12}"
        row += f" {verdict:<15}"
        print(row)

    print()
    print("--- Verdict Legend ---")
    print("  VIABLE:       Detected in 5+ frames with avg confidence > 0.3 (by best model)")
    print("  MARGINAL:     Detected but low confidence or rare")
    print("  NOT DETECTED: Never detected in sample (may appear in other scenes)")
    print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
