#!/usr/bin/env python3
"""
VRU Near-Miss Detection Pipeline
=================================
Finds dashcam events where the driver almost hits a Vulnerable Road User
(pedestrian, cyclist, scooter, motorcycle, animal).

Uses Grounding DINO for open-vocabulary VRU detection, then tracks detections
across frames to determine motion and proximity to the vehicle.

Usage:
    # Validate on known near-miss examples
    python3 vru_nearmiss_pipeline.py --event-ids 695878c0fc45de1c69825ef7,69a80a312df6d571a3b6fe33

    # Full run: scan 2000 events from last 90 days
    python3 vru_nearmiss_pipeline.py --days 90 --max-events 2000

    # Resume after interruption
    python3 vru_nearmiss_pipeline.py --days 90 --resume-from 500
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import cv2
import requests
import torch
from PIL import Image
from transformers import AutoProcessor, AutoModelForZeroShotObjectDetection

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE_URL = "https://beemaps.com/api/developer/aievents"
VIDEO_CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "pipeline-video-cache"
EVENT_TYPES = ["HARSH_BRAKING", "SWERVING"]

# Grounding DINO
DINO_MODEL_ID = "IDEA-Research/grounding-dino-base"
VRU_PROMPTS = "person walking. person standing. cyclist. bicycle rider. motorcycle rider. scooter rider. dog. cat. child. animal on road. deer."
DINO_BOX_THRESHOLD = 0.30
DINO_TEXT_THRESHOLD = 0.25

# Frame sampling
COARSE_FRAMES = 8
DENSE_FPS = 2.0
DENSE_WINDOW_S = 2.0

# Camera (Bee dashcam)
BEE_HFOV_DEG = 142.0

# Known VRU heights (meters)
VRU_HEIGHTS = {
    "person": 1.7,
    "pedestrian": 1.7,
    "cyclist": 1.8,
    "bicycle rider": 1.8,
    "motorcycle rider": 1.6,
    "scooter rider": 1.7,
    "child": 1.0,
    "dog": 0.5,
    "cat": 0.3,
    "animal": 0.7,
    "deer": 1.0,
}

# Proximity thresholds (meters)
VERY_CLOSE_M = 5.0
CLOSE_M = 10.0
MEDIUM_M = 20.0

# Motion detection
MOTION_DISPLACEMENT_FRAC = 0.08  # bbox center moves > 8% of bbox width = moving
MOTION_IOU_THRESHOLD = 0.5       # IoU < 0.5 = moving

# Tracking
MAX_MATCH_DISTANCE_PX = 200
MIN_BBOX_HEIGHT_PX = 20  # filter tiny detections (signs, billboards)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class VRUDetection:
    label: str
    confidence: float
    bbox: tuple  # (x1, y1, x2, y2) in pixels
    frame_index: int
    timestamp_ms: int

    @property
    def cx(self) -> float:
        return (self.bbox[0] + self.bbox[2]) / 2

    @property
    def cy(self) -> float:
        return (self.bbox[1] + self.bbox[3]) / 2

    @property
    def width(self) -> float:
        return self.bbox[2] - self.bbox[0]

    @property
    def height(self) -> float:
        return self.bbox[3] - self.bbox[1]


@dataclass
class VRUTrack:
    track_id: str
    label: str
    detections: list[VRUDetection] = field(default_factory=list)
    is_moving: bool = False
    min_distance_m: float = 999.0
    proximity: str = "far"
    max_confidence: float = 0.0
    duration_ms: int = 0


@dataclass
class EventResult:
    event_id: str
    event_type: str
    lat: float
    lon: float
    timestamp: str
    max_mph: float
    min_mph: float
    risk_score: float = 0.0
    tracks: list[VRUTrack] = field(default_factory=list)
    closest_vru_m: float = 999.0
    moving_vru_count: int = 0
    close_moving_count: int = 0

    @property
    def vru_summary(self) -> str:
        if not self.tracks:
            return "none"
        parts = []
        for t in sorted(self.tracks, key=lambda t: t.min_distance_m):
            motion = "moving" if t.is_moving else "static"
            parts.append(f"{t.label} {t.proximity} {t.min_distance_m:.0f}m {motion} ({t.max_confidence:.0%})")
        return "; ".join(parts)


# ---------------------------------------------------------------------------
# API helpers (reused from intersection pipeline)
# ---------------------------------------------------------------------------

def load_api_key() -> str:
    key = os.environ.get("BEEMAPS_API_KEY")
    if key:
        return key
    env_path = Path(__file__).resolve().parent.parent / ".env.local"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("BEEMAPS_API_KEY="):
                return line.split("=", 1)[1].strip()
    print("Error: BEEMAPS_API_KEY not found", file=sys.stderr)
    sys.exit(1)


def auth_header(key: str) -> str:
    return key if key.startswith("Basic ") else f"Basic {key}"


def fetch_event_detail(api_key: str, event_id: str) -> dict | None:
    """Fetch a single event by ID from the Bee Maps API."""
    try:
        resp = requests.get(
            f"{API_BASE_URL}/{event_id}",
            headers={"Authorization": auth_header(api_key)},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"    Warning: Could not fetch event {event_id}: {exc}", file=sys.stderr)
        return None


def fetch_events(api_key: str, days: int, max_events: int) -> list[dict]:
    """Fetch HARSH_BRAKING + SWERVING events across 30-day API windows."""
    all_events = []
    seen_ids = set()
    end = datetime.now(timezone.utc)
    remaining_days = days
    window_size = 30

    print(f"Fetching {', '.join(EVENT_TYPES)} events ({days} days, up to {max_events})...", file=sys.stderr)

    while remaining_days > 0 and len(all_events) < max_events:
        window_days = min(remaining_days, window_size)
        window_end = end
        window_start = end - timedelta(days=window_days)
        start_str = window_start.strftime("%Y-%m-%dT00:00:00.000Z")
        end_str = window_end.strftime("%Y-%m-%dT23:59:59.999Z")

        print(f"  Window: {start_str[:10]} → {end_str[:10]}...", file=sys.stderr)

        offset = 0
        batch_size = 500
        while len(all_events) < max_events:
            resp = requests.post(
                f"{API_BASE_URL}/search",
                json={
                    "startDate": start_str,
                    "endDate": end_str,
                    "types": EVENT_TYPES,
                    "limit": batch_size,
                    "offset": offset,
                },
                headers={
                    "Content-Type": "application/json",
                    "Authorization": auth_header(api_key),
                },
                timeout=60,
            )
            resp.raise_for_status()
            page = resp.json().get("events", [])
            if not page:
                break

            for e in page:
                if e["id"] not in seen_ids:
                    seen_ids.add(e["id"])
                    all_events.append(e)

            offset += len(page)
            if len(page) < batch_size:
                break

        print(f"    {len(all_events)} total so far", file=sys.stderr)
        end = window_start
        remaining_days -= window_days

    all_events = all_events[:max_events]
    print(f"  Total: {len(all_events)} events", file=sys.stderr)
    return all_events


def extract_speed(event: dict) -> tuple[float, float]:
    """Extract max/min speed in mph from event metadata."""
    speeds = event.get("metadata", {}).get("SPEED_ARRAY", [])
    if speeds:
        vals = [s.get("AVG_SPEED_MS", 0) for s in speeds]
        return max(vals) * 2.237, min(vals) * 2.237
    return 0.0, 0.0


# ---------------------------------------------------------------------------
# Video download / cache
# ---------------------------------------------------------------------------

_local_video_dirs: list[Path] = []


def find_local_video(event_id: str) -> Path | None:
    for d in _local_video_dirs:
        candidate = d / f"{event_id}.mp4"
        if candidate.exists():
            return candidate
    return None


def download_video(video_url: str, event_id: str = "") -> Path | None:
    if event_id:
        local = find_local_video(event_id)
        if local:
            return local

    if not video_url:
        return None

    hashed = hashlib.md5(video_url.encode("utf-8")).hexdigest()
    path = VIDEO_CACHE_DIR / f"{hashed}.mp4"
    if path.exists():
        return path

    VIDEO_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with requests.get(video_url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return path
    except Exception as exc:
        print(f"    Warning: Failed to download video: {exc}", file=sys.stderr)
        if path.exists():
            path.unlink(missing_ok=True)
        return None


# ---------------------------------------------------------------------------
# Grounding DINO VRU Detector
# ---------------------------------------------------------------------------

class VRUDetector:
    def __init__(self):
        # MPS has compatibility issues with Grounding DINO
        self.device = "cpu"
        print(f"Loading Grounding DINO ({DINO_MODEL_ID}) on {self.device}...", file=sys.stderr)
        self.processor = AutoProcessor.from_pretrained(DINO_MODEL_ID)
        self.model = AutoModelForZeroShotObjectDetection.from_pretrained(DINO_MODEL_ID).to(self.device)
        self.model.eval()
        print("  Model ready.", file=sys.stderr)

    @torch.no_grad()
    def detect_frame(self, frame_bgr, frame_index: int, timestamp_ms: int) -> list[VRUDetection]:
        frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
        image = Image.fromarray(frame_rgb)

        inputs = self.processor(
            images=image, text=VRU_PROMPTS, return_tensors="pt"
        ).to(self.device)

        outputs = self.model(**inputs)

        results = self.processor.post_process_grounded_object_detection(
            outputs, inputs.input_ids,
            threshold=DINO_BOX_THRESHOLD,
            text_threshold=DINO_TEXT_THRESHOLD,
            target_sizes=[image.size[::-1]],
        )

        detections = []
        if results:
            r = results[0]
            for box, score, label in zip(r["boxes"], r["scores"], r["labels"]):
                bbox = tuple(box.tolist())
                height = bbox[3] - bbox[1]
                # Filter tiny detections (likely signs/billboards, not real VRUs)
                if height < MIN_BBOX_HEIGHT_PX:
                    continue
                # Filter detections in top 15% of frame (likely signs, not VRUs on road)
                frame_h = image.size[1]
                if bbox[1] < frame_h * 0.15 and bbox[3] < frame_h * 0.30:
                    continue

                detections.append(VRUDetection(
                    label=label.strip(),
                    confidence=score.item(),
                    bbox=bbox,
                    frame_index=frame_index,
                    timestamp_ms=timestamp_ms,
                ))

        return detections


# ---------------------------------------------------------------------------
# Two-pass video analysis
# ---------------------------------------------------------------------------

def analyze_video(detector: VRUDetector, video_path: Path) -> list[VRUDetection]:
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return []

    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if total_frames <= 0:
        cap.release()
        return []

    def read_frame(idx: int):
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok, frame = cap.read()
        return frame if ok else None

    def frame_to_ms(idx: int) -> int:
        return int((idx / fps) * 1000)

    # --- Pass 1: Coarse scan (8 evenly-spaced frames) ---
    coarse_indices = [int(i * total_frames / COARSE_FRAMES) for i in range(COARSE_FRAMES)]
    coarse_indices = [min(i, total_frames - 1) for i in coarse_indices]

    coarse_detections = []
    for idx in coarse_indices:
        frame = read_frame(idx)
        if frame is None:
            continue
        dets = detector.detect_frame(frame, idx, frame_to_ms(idx))
        coarse_detections.extend(dets)

    if not coarse_detections:
        cap.release()
        return []

    # --- Pass 2: Dense scan around VRU detections ---
    dense_stride = max(1, int(fps / DENSE_FPS))
    window_frames = int(DENSE_WINDOW_S * fps)

    # Build dense windows around each coarse VRU detection
    analyzed_indices = set(coarse_indices)
    dense_indices = []
    for d in coarse_detections:
        start = max(0, d.frame_index - window_frames)
        end = min(total_frames, d.frame_index + window_frames)
        for idx in range(start, end, dense_stride):
            if idx not in analyzed_indices:
                dense_indices.append(idx)
                analyzed_indices.add(idx)

    dense_indices.sort()

    all_detections = list(coarse_detections)
    for idx in dense_indices:
        frame = read_frame(idx)
        if frame is None:
            continue
        dets = detector.detect_frame(frame, idx, frame_to_ms(idx))
        all_detections.extend(dets)

    cap.release()
    return all_detections


# ---------------------------------------------------------------------------
# VRU Tracking (ported from actor-matching.ts)
# ---------------------------------------------------------------------------

def label_similarity(a: str, b: str) -> float:
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    if not words_a and not words_b:
        return 1.0
    union = words_a | words_b
    if not union:
        return 0.0
    overlap = len(words_a & words_b)
    return overlap / len(union)


def normalize_vru_label(label: str) -> str:
    """Map Grounding DINO labels to canonical VRU types."""
    label_lower = label.lower().strip()
    if any(w in label_lower for w in ["person", "pedestrian", "walking", "standing"]):
        return "person"
    if any(w in label_lower for w in ["child", "kid"]):
        return "child"
    if any(w in label_lower for w in ["cyclist", "bicycle"]):
        return "cyclist"
    if "motorcycle" in label_lower:
        return "motorcycle rider"
    if "scooter" in label_lower:
        return "scooter rider"
    if any(w in label_lower for w in ["deer"]):
        return "deer"
    if any(w in label_lower for w in ["dog", "cat", "animal"]):
        return label_lower.split()[0]  # "dog", "cat", "animal"
    return label_lower


def build_tracks(detections: list[VRUDetection]) -> list[VRUTrack]:
    """Greedy bipartite matching across frames (port of actor-matching.ts buildTracks)."""
    if not detections:
        return []

    # Group detections by frame/timestamp, sorted by time
    frames: dict[int, list[VRUDetection]] = {}
    for d in detections:
        frames.setdefault(d.timestamp_ms, []).append(d)

    sorted_timestamps = sorted(frames.keys())

    open_tracks: list[dict] = []
    closed_tracks: list[dict] = []
    next_id = 1

    for ts in sorted_timestamps:
        frame_dets = frames[ts]

        # Build cost matrix: (track_idx, det_idx, cost)
        candidates = []
        for ti, track in enumerate(open_tracks):
            last = track["last_det"]
            for di, det in enumerate(frame_dets):
                # Hard gate: similar VRU type
                if normalize_vru_label(det.label) != normalize_vru_label(last.label):
                    continue

                # Pixel distance between bbox centers
                dist = math.sqrt((det.cx - last.cx) ** 2 + (det.cy - last.cy) ** 2)
                if dist > MAX_MATCH_DISTANCE_PX:
                    continue

                sim = label_similarity(det.label, last.label)
                cost = dist - sim * 0.3 * MAX_MATCH_DISTANCE_PX
                candidates.append((ti, di, cost))

        candidates.sort(key=lambda x: x[2])

        matched_tracks = set()
        matched_dets = set()

        for ti, di, cost in candidates:
            if ti in matched_tracks or di in matched_dets:
                continue
            matched_tracks.add(ti)
            matched_dets.add(di)
            open_tracks[ti]["dets"].append(frame_dets[di])
            open_tracks[ti]["last_det"] = frame_dets[di]

        # Close unmatched tracks
        for ti in range(len(open_tracks) - 1, -1, -1):
            if ti not in matched_tracks:
                closed_tracks.append(open_tracks.pop(ti))

        # Open new tracks for unmatched detections
        for di, det in enumerate(frame_dets):
            if di not in matched_dets:
                open_tracks.append({
                    "id": f"track-{next_id}",
                    "label": normalize_vru_label(det.label),
                    "dets": [det],
                    "last_det": det,
                })
                next_id += 1

    # Close remaining
    closed_tracks.extend(open_tracks)

    # Convert to VRUTrack
    tracks = []
    for t in closed_tracks:
        if len(t["dets"]) < 1:
            continue
        track = VRUTrack(
            track_id=t["id"],
            label=t["label"],
            detections=t["dets"],
            max_confidence=max(d.confidence for d in t["dets"]),
            duration_ms=t["dets"][-1].timestamp_ms - t["dets"][0].timestamp_ms,
        )
        tracks.append(track)

    return tracks


# ---------------------------------------------------------------------------
# Motion analysis
# ---------------------------------------------------------------------------

def compute_iou(a: VRUDetection, b: VRUDetection) -> float:
    x1 = max(a.bbox[0], b.bbox[0])
    y1 = max(a.bbox[1], b.bbox[1])
    x2 = min(a.bbox[2], b.bbox[2])
    y2 = min(a.bbox[3], b.bbox[3])
    inter = max(0, x2 - x1) * max(0, y2 - y1)
    area_a = a.width * a.height
    area_b = b.width * b.height
    union = area_a + area_b - inter
    return inter / union if union > 0 else 0.0


def analyze_motion(track: VRUTrack) -> bool:
    """Determine if a VRU track shows motion."""
    if len(track.detections) < 2:
        return False

    dets = sorted(track.detections, key=lambda d: d.timestamp_ms)

    for i in range(1, len(dets)):
        prev, curr = dets[i - 1], dets[i]
        # Bbox center displacement relative to bbox width
        dx = abs(curr.cx - prev.cx)
        dy = abs(curr.cy - prev.cy)
        displacement = math.sqrt(dx ** 2 + dy ** 2)
        ref_width = max(prev.width, curr.width, 1)
        frac = displacement / ref_width

        if frac > MOTION_DISPLACEMENT_FRAC:
            return True

        # IoU check
        iou = compute_iou(prev, curr)
        if iou < MOTION_IOU_THRESHOLD:
            return True

    return False


# ---------------------------------------------------------------------------
# Distance estimation
# ---------------------------------------------------------------------------

def estimate_distance(det: VRUDetection, frame_height: int, frame_width: int) -> float:
    """Estimate distance in meters using pinhole camera model."""
    label = normalize_vru_label(det.label)
    known_height = VRU_HEIGHTS.get(label, 1.7)

    # Focal length from HFOV: focal_px = (frame_width / 2) / tan(HFOV / 2)
    half_fov_rad = math.radians(BEE_HFOV_DEG / 2)
    focal_px = (frame_width / 2) / math.tan(half_fov_rad)

    bbox_h = det.height
    if bbox_h < 1:
        return 999.0

    distance = (known_height * focal_px) / bbox_h

    # Edge correction: objects in outer 25% of frame appear smaller due to barrel distortion
    center_x = frame_width / 2
    offset_frac = abs(det.cx - center_x) / center_x
    if offset_frac > 0.75:
        distance *= 0.77  # correct for barrel distortion compression

    return max(1.0, distance)


def classify_proximity(distance_m: float) -> str:
    if distance_m < VERY_CLOSE_M:
        return "very_close"
    elif distance_m < CLOSE_M:
        return "close"
    elif distance_m < MEDIUM_M:
        return "medium"
    return "far"


# ---------------------------------------------------------------------------
# Risk scoring
# ---------------------------------------------------------------------------

TYPE_WEIGHTS = {
    "person": 1.0,
    "child": 1.2,
    "cyclist": 0.9,
    "bicycle rider": 0.9,
    "motorcycle rider": 0.7,
    "scooter rider": 0.8,
    "dog": 0.6,
    "cat": 0.5,
}

PROXIMITY_MULTIPLIERS = {
    "very_close": 2.0,
    "close": 1.5,
    "medium": 1.0,
    "far": 0.3,
}


def compute_risk_score(tracks: list[VRUTrack], max_mph: float) -> float:
    if not tracks:
        return 0.0

    score = 0.0
    for track in tracks:
        base = TYPE_WEIGHTS.get(track.label, 0.5)
        prox = PROXIMITY_MULTIPLIERS.get(track.proximity, 0.5)
        motion = 1.5 if track.is_moving else 0.5
        conf = track.max_confidence
        score += base * prox * motion * conf

    # Vehicle speed factor
    speed_factor = min(2.0, max(0.5, max_mph / 25.0))
    return min(1.0, score * speed_factor / 3.0)


# ---------------------------------------------------------------------------
# Process a single event
# ---------------------------------------------------------------------------

def process_event(
    detector: VRUDetector,
    event: dict,
    api_key: str,
) -> EventResult | None:
    event_id = event["id"]
    max_mph, min_mph = extract_speed(event)

    result = EventResult(
        event_id=event_id,
        event_type=event.get("type", ""),
        lat=event["location"]["lat"],
        lon=event["location"]["lon"],
        timestamp=event["timestamp"],
        max_mph=max_mph,
        min_mph=min_mph,
    )

    # Download video
    video_url = event.get("videoUrl", "")
    video_path = download_video(video_url, event_id=event_id)
    if video_path is None:
        return None

    # Get frame dimensions
    cap = cv2.VideoCapture(str(video_path))
    frame_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)) or 1280
    frame_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)) or 720
    cap.release()

    # Two-pass analysis
    t0 = time.time()
    detections = analyze_video(detector, video_path)
    analysis_time = time.time() - t0

    if not detections:
        return result  # No VRUs found

    # Track VRUs across frames
    tracks = build_tracks(detections)

    # Analyze each track for motion and proximity
    for track in tracks:
        track.is_moving = analyze_motion(track)
        distances = [estimate_distance(d, frame_h, frame_w) for d in track.detections]
        track.min_distance_m = min(distances)
        track.proximity = classify_proximity(track.min_distance_m)

    # Filter: keep tracks with confidence and reasonable size
    tracks = [t for t in tracks if t.max_confidence >= 0.30]

    result.tracks = tracks
    result.closest_vru_m = min((t.min_distance_m for t in tracks), default=999.0)
    result.moving_vru_count = sum(1 for t in tracks if t.is_moving)
    result.close_moving_count = sum(
        1 for t in tracks
        if t.is_moving and t.proximity in ("very_close", "close")
    )
    result.risk_score = compute_risk_score(tracks, max_mph)

    n_dets = len(detections)
    n_tracks = len(tracks)
    print(f" {analysis_time:.0f}s — {n_dets} detections, {n_tracks} tracks, risk={result.risk_score:.3f}", file=sys.stderr)

    return result


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_results(results: list[EventResult], output_path: str) -> None:
    scored = [r for r in results if r.risk_score > 0]
    scored.sort(key=lambda r: r.risk_score, reverse=True)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow([
            "event_id", "risk_score", "event_type", "closest_vru_m",
            "moving_vru_count", "close_moving_count",
            "max_mph", "min_mph", "lat", "lon", "timestamp", "vru_summary",
        ])
        for r in scored:
            writer.writerow([
                r.event_id,
                f"{r.risk_score:.3f}",
                r.event_type,
                f"{r.closest_vru_m:.1f}",
                r.moving_vru_count,
                r.close_moving_count,
                f"{r.max_mph:.1f}",
                f"{r.min_mph:.1f}",
                f"{r.lat:.6f}",
                f"{r.lon:.6f}",
                r.timestamp,
                r.vru_summary,
            ])

    print(f"\nWrote {len(scored)} near-miss results to {output_path}", file=sys.stderr)


def print_summary(results: list[EventResult]) -> None:
    scored = [r for r in results if r.risk_score > 0]
    scored.sort(key=lambda r: r.risk_score, reverse=True)
    top = scored[:30]

    if not top:
        print("\nNo VRU near-miss events found.")
        return

    total_with_vru = sum(1 for r in results if r.tracks)
    total_moving = sum(1 for r in results if r.moving_vru_count > 0)
    total_close_moving = sum(1 for r in results if r.close_moving_count > 0)

    print(f"\n{'=' * 110}")
    print(f"VRU NEAR-MISS DETECTION RESULTS")
    print(f"{'=' * 110}")
    print(f"Events scanned: {len(results)}  |  VRU detected: {total_with_vru}  |  Moving VRU: {total_moving}  |  Close+Moving: {total_close_moving}")
    print(f"\nTOP {len(top)} EVENTS:")
    print(f"{'#':<4} {'Risk':<7} {'Type':<16} {'Closest':<9} {'Moving':<8} {'Speed':<14} {'Event ID':<28} {'VRU Summary'}")
    print("-" * 110)

    for i, r in enumerate(top):
        speed = f"{r.max_mph:.0f}→{r.min_mph:.0f}mph"
        print(f"{i+1:<4} {r.risk_score:<7.3f} {r.event_type:<16} {r.closest_vru_m:<9.1f} {r.moving_vru_count:<8} {speed:<14} {r.event_id:<28} {r.vru_summary[:50]}")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def _fetch_and_process_batch(
    api_key: str, start_str: str, end_str: str,
    batch_size: int, offset: int, detector: VRUDetector,
    results: list[EventResult], event_num: list[int], max_events: int,
    seen_ids: set[str], output_path: str | None,
) -> tuple[int, bool]:
    """Fetch one page of events and process them immediately (URLs are fresh).
    Returns (page_size, should_continue)."""
    resp = requests.post(
        f"{API_BASE_URL}/search",
        json={
            "startDate": start_str,
            "endDate": end_str,
            "types": EVENT_TYPES,
            "limit": batch_size,
            "offset": offset,
        },
        headers={
            "Content-Type": "application/json",
            "Authorization": auth_header(api_key),
        },
        timeout=60,
    )
    resp.raise_for_status()
    page = resp.json().get("events", [])
    if not page:
        return 0, False

    for event in page:
        if event["id"] in seen_ids:
            continue
        seen_ids.add(event["id"])

        if event_num[0] >= max_events:
            return len(page), False

        event_num[0] += 1
        eid = event["id"]
        etype = event.get("type", "?")
        print(f"  [{event_num[0]}/{max_events}] {eid} ({etype})...", file=sys.stderr, end="", flush=True)

        result = process_event(detector, event, api_key)
        if result is None:
            print(" no video", file=sys.stderr)
            continue

        if not result.tracks:
            print(" no VRUs", file=sys.stderr)

        results.append(result)

        # Periodic progress save
        if event_num[0] % 50 == 0 and output_path:
            write_results(results, output_path + ".partial")
            print(f"  --- Progress saved ({event_num[0]}/{max_events}) ---", file=sys.stderr)

    return len(page), event_num[0] < max_events


def run_pipeline(args: argparse.Namespace) -> list[EventResult]:
    api_key = load_api_key()

    # Register local video directories
    script_dir = Path(__file__).resolve().parent
    repo_dir = script_dir.parent
    for vdir in [script_dir / "data", repo_dir / "public" / "videos" / "highlights"]:
        if vdir.is_dir():
            _local_video_dirs.append(vdir)

    # Load detector first
    detector = VRUDetector()

    results: list[EventResult] = []

    if args.event_ids:
        # Fetch specific events by ID
        ids = [eid.strip() for eid in args.event_ids.split(",")]
        print(f"Fetching {len(ids)} specific events...", file=sys.stderr)
        for i, eid in enumerate(ids):
            e = fetch_event_detail(api_key, eid)
            if not e:
                print(f"  Warning: Could not fetch {eid}", file=sys.stderr)
                continue
            print(f"  [{i+1}/{len(ids)}] {eid} ({e.get('type','?')})...", file=sys.stderr, end="", flush=True)
            result = process_event(detector, e, api_key)
            if result is None:
                print(" no video", file=sys.stderr)
                continue
            if not result.tracks:
                print(" no VRUs", file=sys.stderr)
            results.append(result)
        return results

    # Streaming mode: fetch small batches and process immediately
    # so video URLs don't expire before we download them
    end = datetime.now(timezone.utc)
    remaining_days = args.days
    window_size = 30
    batch_size = 20  # small batches — process before URLs expire
    seen_ids: set[str] = set()
    event_num = [0]  # mutable counter for nested function

    print(f"Streaming {', '.join(EVENT_TYPES)} events ({args.days} days, up to {args.max_events})...", file=sys.stderr)
    print(f"  Processing each batch immediately (video URLs expire quickly)\n", file=sys.stderr)

    while remaining_days > 0 and event_num[0] < args.max_events:
        window_days = min(remaining_days, window_size)
        window_end = end
        window_start = end - timedelta(days=window_days)
        start_str = window_start.strftime("%Y-%m-%dT00:00:00.000Z")
        end_str = window_end.strftime("%Y-%m-%dT23:59:59.999Z")

        print(f"  === Window: {start_str[:10]} → {end_str[:10]} ===", file=sys.stderr)

        offset = 0
        while event_num[0] < args.max_events:
            page_size, should_continue = _fetch_and_process_batch(
                api_key, start_str, end_str,
                batch_size, offset, detector,
                results, event_num, args.max_events,
                seen_ids, args.output,
            )
            if page_size == 0 or not should_continue:
                break
            offset += page_size

        end = window_start
        remaining_days -= window_days

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Find VRU near-miss events using Grounding DINO detection + tracking",
    )
    parser.add_argument("--event-ids", type=str, default=None,
                        help="Comma-separated event IDs to analyze (for validation)")
    parser.add_argument("--days", type=int, default=90,
                        help="Number of days to search (default: 90)")
    parser.add_argument("--max-events", type=int, default=2000,
                        help="Maximum events to fetch (default: 2000)")
    parser.add_argument("--output", type=str, default="scripts/data/vru_nearmiss_results.tsv",
                        help="Output TSV file path")
    parser.add_argument("--resume-from", type=int, default=0,
                        help="Resume from Nth event (0-indexed)")
    args = parser.parse_args()

    results = run_pipeline(args)
    write_results(results, args.output)
    print_summary(results)


if __name__ == "__main__":
    main()
