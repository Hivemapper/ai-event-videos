#!/usr/bin/env python3
"""
Intersection Braking Detection Pipeline
========================================
Finds harsh braking events that occurred at intersections using two signals:

1. **Overture Maps connectors** — ≥3 road connectors within 20m of the event
   GPS coordinate indicates an intersection in the road network.

2. **Grounding DINO visual confirmation** — Extracts frames from the event
   video and detects intersection features (stop signs, traffic lights,
   crosswalks, etc.) using open-vocabulary object detection.

Events are scored by combining both signals and ranked by confidence.

Usage:
    # Set env var or use .env.local
    export BEEMAPS_API_KEY="your-key-here"

    # Run with defaults (last 30 days, up to 500 events)
    python3 intersection_braking_pipeline.py

    # Custom date range and limits
    python3 intersection_braking_pipeline.py --days 60 --max-events 1000

    # Use cached event JSON files instead of API
    python3 intersection_braking_pipeline.py --from-cache scripts/data/

    # Skip Overture check (visual-only mode)
    python3 intersection_braking_pipeline.py --skip-overture
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
import tempfile
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

# Overture Maps thresholds
MIN_CONNECTORS = 3          # minimum road connectors to consider as intersection
CONNECTOR_RADIUS_DEG = 0.0003  # ~30m at mid-latitudes

# Grounding DINO detection config
DINO_MODEL_ID = "IDEA-Research/grounding-dino-base"
INTERSECTION_PROMPTS = "stop sign. traffic light. crosswalk. pedestrian crossing. yield sign. traffic signal."
DINO_BOX_THRESHOLD = 0.25
DINO_TEXT_THRESHOLD = 0.20
FRAMES_TO_SAMPLE = 8        # number of evenly-spaced frames to analyze per video

# Scoring weights
OVERTURE_WEIGHT = 0.4       # weight for Overture connector signal
VISUAL_WEIGHT = 0.6         # weight for Grounding DINO visual signal


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BrakingEvent:
    id: str
    lat: float
    lon: float
    timestamp: str
    max_mph: float = 0.0
    min_mph: float = 0.0
    video_url: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass
class Detection:
    label: str
    confidence: float
    bbox: tuple  # (x1, y1, x2, y2)
    frame_index: int


@dataclass
class IntersectionResult:
    event: BrakingEvent
    connector_count: int = 0
    detections: list[Detection] = field(default_factory=list)
    overture_score: float = 0.0
    visual_score: float = 0.0
    combined_score: float = 0.0

    @property
    def detection_summary(self) -> str:
        if not self.detections:
            return "none"
        labels = {}
        for d in self.detections:
            if d.label not in labels or d.confidence > labels[d.label]:
                labels[d.label] = d.confidence
        return "; ".join(f"{l} ({c:.0%})" for l, c in sorted(labels.items(), key=lambda x: -x[1]))


# ---------------------------------------------------------------------------
# API: Fetch harsh braking events
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
    print("Error: BEEMAPS_API_KEY not found in env or .env.local", file=sys.stderr)
    sys.exit(1)


def auth_header(key: str) -> str:
    return key if key.startswith("Basic ") else f"Basic {key}"


def _fetch_window(api_key: str, start_str: str, end_str: str, max_events: int) -> list[BrakingEvent]:
    """Fetch harsh braking events for a single ≤31-day window."""
    events = []
    offset = 0
    batch_size = 500

    while len(events) < max_events:
        resp = requests.post(
            f"{API_BASE_URL}/search",
            json={
                "startDate": start_str,
                "endDate": end_str,
                "types": ["HARSH_BRAKING"],
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
        payload = resp.json()
        page = payload.get("events", [])
        if not page:
            break

        for e in page:
            speeds = e.get("metadata", {}).get("SPEED_ARRAY", [])
            if speeds:
                speed_vals = [s.get("AVG_SPEED_MS", 0) for s in speeds]
                max_mph = max(speed_vals) * 2.237
                min_mph = min(speed_vals) * 2.237
            else:
                max_mph = 0.0
                min_mph = 0.0

            events.append(BrakingEvent(
                id=e["id"],
                lat=e["location"]["lat"],
                lon=e["location"]["lon"],
                timestamp=e["timestamp"],
                max_mph=max_mph,
                min_mph=min_mph,
                video_url=e.get("videoUrl", ""),
                metadata=e.get("metadata", {}),
            ))

        offset += len(page)
        if len(page) < batch_size:
            break

    return events[:max_events]


def fetch_braking_events(api_key: str, days: int, max_events: int) -> list[BrakingEvent]:
    """Fetch harsh braking events, iterating across 30-day windows (API limit: 31 days)."""
    all_events = []
    seen_ids = set()
    end = datetime.now(timezone.utc)
    remaining_days = days
    window_size = 30  # stay under 31-day API limit

    print(f"Fetching harsh braking events ({days} days, up to {max_events})...", file=sys.stderr)

    while remaining_days > 0 and len(all_events) < max_events:
        window_days = min(remaining_days, window_size)
        window_end = end
        window_start = end - timedelta(days=window_days)

        start_str = window_start.strftime("%Y-%m-%dT00:00:00.000Z")
        end_str = window_end.strftime("%Y-%m-%dT23:59:59.999Z")

        needed = max_events - len(all_events)
        print(f"  Window: {start_str[:10]} → {end_str[:10]} (need {needed} more)...", file=sys.stderr)

        window_events = _fetch_window(api_key, start_str, end_str, needed)

        # Deduplicate across windows
        for ev in window_events:
            if ev.id not in seen_ids:
                seen_ids.add(ev.id)
                all_events.append(ev)

        print(f"  Got {len(window_events)} in window, {len(all_events)} total", file=sys.stderr)

        end = window_start  # move to next older window
        remaining_days -= window_days

    all_events = all_events[:max_events]
    print(f"  Total: {len(all_events)} harsh braking events", file=sys.stderr)
    return all_events


def load_events_from_cache(cache_dir: str) -> list[BrakingEvent]:
    """Load events from cached JSON files."""
    events = []
    cache_path = Path(cache_dir)
    json_files = sorted(cache_path.glob("*.json"))
    print(f"Loading {len(json_files)} cached events from {cache_dir}...", file=sys.stderr)

    for jf in json_files:
        e = json.loads(jf.read_text())
        if e.get("type") != "HARSH_BRAKING":
            continue
        speeds = e.get("metadata", {}).get("SPEED_ARRAY", [])
        if speeds:
            speed_vals = [s.get("AVG_SPEED_MS", 0) for s in speeds]
            max_mph = max(speed_vals) * 2.237
            min_mph = min(speed_vals) * 2.237
        else:
            max_mph, min_mph = 0.0, 0.0

        events.append(BrakingEvent(
            id=e["id"],
            lat=e["location"]["lat"],
            lon=e["location"]["lon"],
            timestamp=e["timestamp"],
            max_mph=max_mph,
            min_mph=min_mph,
            video_url=e.get("videoUrl", ""),
            metadata=e.get("metadata", {}),
        ))

    print(f"  Loaded {len(events)} harsh braking events", file=sys.stderr)
    return events


# ---------------------------------------------------------------------------
# Overture Maps intersection check via DuckDB
# ---------------------------------------------------------------------------

def check_overture_intersections(events: list[BrakingEvent]) -> dict[str, int]:
    """Query Overture Maps connectors to find events near road intersections.

    Returns dict mapping event_id -> connector_count.
    """
    if not events:
        return {}

    print(f"Checking {len(events)} events against Overture Maps connectors...", file=sys.stderr)

    # Group events by ~1-degree tiles for efficient parquet scanning
    from collections import defaultdict
    regions: dict[tuple[int, int], list[BrakingEvent]] = defaultdict(list)
    for e in events:
        key = (round(e.lat), round(e.lon))
        regions[key].append(e)

    # Build VALUES rows
    values_rows = []
    for e in events:
        values_rows.append(f"('{e.id}', {e.lat}, {e.lon})")
    values_sql = ",\n".join(values_rows)

    # Build UNION ALL of connector queries per region
    connector_unions = []
    for (rlat, rlon), region_events in regions.items():
        min_lat = min(ev.lat for ev in region_events) - 0.001
        max_lat = max(ev.lat for ev in region_events) + 0.001
        min_lon = min(ev.lon for ev in region_events) - 0.001
        max_lon = max(ev.lon for ev in region_events) + 0.001
        connector_unions.append(f"""
  SELECT ST_X(geometry) as clon, ST_Y(geometry) as clat
  FROM read_parquet('s3://overturemaps-us-west-2/release/2026-03-18.0/theme=transportation/type=connector/*',
    filename=true, hive_partitioning=1)
  WHERE bbox.xmin > {min_lon} AND bbox.xmax < {max_lon}
    AND bbox.ymin > {min_lat} AND bbox.ymax < {max_lat}""")

    connectors_sql = "\nUNION ALL\n".join(connector_unions)
    r = CONNECTOR_RADIUS_DEG

    sql = f"""
LOAD spatial;
LOAD httpfs;
SET s3_region='us-west-2';

CREATE TEMP TABLE events(id VARCHAR, lat DOUBLE, lon DOUBLE);
INSERT INTO events VALUES
{values_sql};

CREATE TEMP TABLE connectors AS
{connectors_sql};

SELECT
  e.id,
  COUNT(*) as connectors
FROM events e
JOIN connectors c
  ON c.clon BETWEEN e.lon - {r} AND e.lon + {r}
  AND c.clat BETWEEN e.lat - {r} AND e.lat + {r}
GROUP BY e.id
ORDER BY connectors DESC;
"""

    result = subprocess.run(
        ["duckdb", "-csv", "-header"],
        input=sql,
        capture_output=True,
        text=True,
        timeout=600,
    )

    if result.returncode != 0:
        print(f"  DuckDB error: {result.stderr[:500]}", file=sys.stderr)
        return {}

    connector_counts = {}
    lines = result.stdout.strip().split("\n")
    if len(lines) > 1:
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) == 2:
                connector_counts[parts[0]] = int(parts[1])

    intersection_count = sum(1 for c in connector_counts.values() if c >= MIN_CONNECTORS)
    print(f"  Found {intersection_count} events with ≥{MIN_CONNECTORS} connectors (likely intersections)", file=sys.stderr)
    return connector_counts


# ---------------------------------------------------------------------------
# Grounding DINO visual detection
# ---------------------------------------------------------------------------

class IntersectionDetector:
    def __init__(self, device: str = "auto"):
        if device == "auto":
            if torch.backends.mps.is_available():
                device = "mps"
            elif torch.cuda.is_available():
                device = "cuda"
            else:
                device = "cpu"

        # MPS has issues with some ops in Grounding DINO, fall back to CPU
        if device == "mps":
            print("  Note: Using CPU for Grounding DINO (MPS has compatibility issues)", file=sys.stderr)
            device = "cpu"

        self.device = device
        print(f"Loading Grounding DINO ({DINO_MODEL_ID}) on {device}...", file=sys.stderr)
        self.processor = AutoProcessor.from_pretrained(DINO_MODEL_ID)
        self.model = AutoModelForZeroShotObjectDetection.from_pretrained(DINO_MODEL_ID).to(device)
        self.model.eval()
        print("  Model ready.", file=sys.stderr)

    @torch.no_grad()
    def detect_frame(self, frame_bgr, frame_index: int) -> list[Detection]:
        """Run detection on a single BGR frame (from OpenCV)."""
        frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
        image = Image.fromarray(frame_rgb)

        inputs = self.processor(
            images=image,
            text=INTERSECTION_PROMPTS,
            return_tensors="pt",
        ).to(self.device)

        outputs = self.model(**inputs)

        results = self.processor.post_process_grounded_object_detection(
            outputs,
            inputs.input_ids,
            threshold=DINO_BOX_THRESHOLD,
            text_threshold=DINO_TEXT_THRESHOLD,
            target_sizes=[image.size[::-1]],
        )

        detections = []
        if results:
            r = results[0]
            for box, score, label in zip(r["boxes"], r["scores"], r["labels"]):
                detections.append(Detection(
                    label=label.strip(),
                    confidence=score.item(),
                    bbox=tuple(box.tolist()),
                    frame_index=frame_index,
                ))

        return detections

    def analyze_video(self, video_path: str | Path) -> list[Detection]:
        """Extract evenly-spaced frames and detect intersection features."""
        cap = cv2.VideoCapture(str(video_path))
        if not cap.isOpened():
            print(f"    Warning: Could not open video {video_path}", file=sys.stderr)
            return []

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total_frames <= 0:
            cap.release()
            return []

        # Sample frames evenly across the video
        indices = [int(i * total_frames / FRAMES_TO_SAMPLE) for i in range(FRAMES_TO_SAMPLE)]
        indices = [min(i, total_frames - 1) for i in indices]

        all_detections = []
        for idx in indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
            ok, frame = cap.read()
            if not ok:
                continue
            dets = self.detect_frame(frame, idx)
            all_detections.extend(dets)

        cap.release()
        return all_detections


# ---------------------------------------------------------------------------
# Video download / cache
# ---------------------------------------------------------------------------

def get_cached_video(video_url: str) -> Path | None:
    """Check if video is already in the pipeline cache."""
    hashed = hashlib.md5(video_url.encode("utf-8")).hexdigest()
    path = VIDEO_CACHE_DIR / f"{hashed}.mp4"
    if path.exists():
        return path
    return None


# Extra directories to search for local video files by event ID
_local_video_dirs: list[Path] = []


def find_local_video(event_id: str) -> Path | None:
    """Check local directories for a video file matching the event ID."""
    for d in _local_video_dirs:
        candidate = d / f"{event_id}.mp4"
        if candidate.exists():
            return candidate
    return None


def download_video(video_url: str, event_id: str = "") -> Path | None:
    """Download video to cache, or find it locally. Return path."""
    # First check local files by event ID
    if event_id:
        local = find_local_video(event_id)
        if local:
            return local

    if not video_url:
        return None

    cached = get_cached_video(video_url)
    if cached:
        return cached

    VIDEO_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    hashed = hashlib.md5(video_url.encode("utf-8")).hexdigest()
    path = VIDEO_CACHE_DIR / f"{hashed}.mp4"

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
# Scoring
# ---------------------------------------------------------------------------

def compute_scores(result: IntersectionResult) -> None:
    """Compute overture, visual, and combined scores for a result."""
    # Overture score: sigmoid-like scaling based on connector count
    c = result.connector_count
    if c >= 5:
        result.overture_score = 1.0
    elif c >= MIN_CONNECTORS:
        result.overture_score = 0.5 + 0.5 * ((c - MIN_CONNECTORS) / (5 - MIN_CONNECTORS))
    else:
        result.overture_score = c / MIN_CONNECTORS * 0.3

    # Visual score: based on best detection confidences across intersection-relevant classes
    if result.detections:
        # Group by label, take max confidence per label
        best_per_label = {}
        for d in result.detections:
            if d.label not in best_per_label or d.confidence > best_per_label[d.label]:
                best_per_label[d.label] = d.confidence

        # More distinct intersection features = higher score
        n_features = len(best_per_label)
        avg_conf = sum(best_per_label.values()) / n_features
        result.visual_score = min(1.0, avg_conf * (1 + 0.2 * (n_features - 1)))
    else:
        result.visual_score = 0.0

    # Combined score
    result.combined_score = (
        OVERTURE_WEIGHT * result.overture_score +
        VISUAL_WEIGHT * result.visual_score
    )


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(args: argparse.Namespace) -> list[IntersectionResult]:
    # Register local video directories for cache lookups
    script_dir = Path(__file__).resolve().parent
    repo_dir = script_dir.parent
    for vdir in [
        script_dir / "data",
        repo_dir / "public" / "videos" / "highlights",
    ]:
        if vdir.is_dir():
            _local_video_dirs.append(vdir)

    # Step 1: Get events
    if args.from_cache:
        _local_video_dirs.append(Path(args.from_cache))
        events = load_events_from_cache(args.from_cache)
    else:
        api_key = load_api_key()
        events = fetch_braking_events(api_key, args.days, args.max_events)

    if not events:
        print("No events found.", file=sys.stderr)
        return []

    # Step 2: Overture Maps connector check
    if args.skip_overture:
        connector_counts = {}
        print("Skipping Overture Maps check (--skip-overture)", file=sys.stderr)
    else:
        connector_counts = check_overture_intersections(events)

    # Build results, filter to intersection candidates
    results = []
    for e in events:
        r = IntersectionResult(event=e, connector_count=connector_counts.get(e.id, 0))
        results.append(r)

    # If we have Overture data, prioritize events with connectors
    if not args.skip_overture:
        # Sort by connector count descending — process high-confidence first
        results.sort(key=lambda r: r.connector_count, reverse=True)

    # Step 3: Visual confirmation with Grounding DINO
    detector = IntersectionDetector()

    # Only visually analyze events that have some intersection signal,
    # or all events if --skip-overture
    candidates = results if args.skip_overture else [r for r in results if r.connector_count >= 2]
    print(f"\nRunning visual detection on {len(candidates)} candidate videos...", file=sys.stderr)

    for i, result in enumerate(candidates):
        e = result.event
        print(f"  [{i+1}/{len(candidates)}] Event {e.id} ({result.connector_count} connectors)...", file=sys.stderr, end="")

        video_path = download_video(e.video_url, event_id=e.id)
        if video_path is None:
            print(" no video", file=sys.stderr)
            continue

        t0 = time.time()
        result.detections = detector.analyze_video(video_path)
        dt = time.time() - t0

        if result.detections:
            labels = {d.label for d in result.detections}
            print(f" {dt:.1f}s — detected: {', '.join(sorted(labels))}", file=sys.stderr)
        else:
            print(f" {dt:.1f}s — no intersection features", file=sys.stderr)

    # Step 4: Score and rank
    for r in results:
        compute_scores(r)

    results.sort(key=lambda r: r.combined_score, reverse=True)
    return results


def write_results(results: list[IntersectionResult], output_path: str) -> None:
    """Write results to TSV file."""
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow([
            "event_id", "combined_score", "overture_score", "visual_score",
            "connectors", "max_mph", "min_mph", "lat", "lon",
            "timestamp", "visual_detections",
        ])
        for r in results:
            if r.combined_score < 0.1:
                continue
            writer.writerow([
                r.event.id,
                f"{r.combined_score:.3f}",
                f"{r.overture_score:.3f}",
                f"{r.visual_score:.3f}",
                r.connector_count,
                f"{r.event.max_mph:.1f}",
                f"{r.event.min_mph:.1f}",
                f"{r.event.lat:.6f}",
                f"{r.event.lon:.6f}",
                r.event.timestamp,
                r.detection_summary,
            ])

    scored = sum(1 for r in results if r.combined_score >= 0.1)
    print(f"\nWrote {scored} results to {output_path}", file=sys.stderr)


def print_summary(results: list[IntersectionResult]) -> None:
    """Print top results to stdout."""
    top = [r for r in results if r.combined_score >= 0.1][:30]
    if not top:
        print("\nNo intersection braking events found.")
        return

    print(f"\n{'='*100}")
    print(f"TOP INTERSECTION BRAKING EVENTS ({len(top)} shown)")
    print(f"{'='*100}")
    print(f"{'#':<4} {'Score':<7} {'Overt':<7} {'Visual':<7} {'Conn':<6} {'Speed':<14} {'Event ID':<28} {'Detections'}")
    print("-" * 100)

    for i, r in enumerate(top):
        speed = f"{r.event.max_mph:.0f}→{r.event.min_mph:.0f}mph"
        print(f"{i+1:<4} {r.combined_score:<7.3f} {r.overture_score:<7.3f} {r.visual_score:<7.3f} {r.connector_count:<6} {speed:<14} {r.event.id:<28} {r.detection_summary}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Find harsh braking events at intersections using Overture Maps + Grounding DINO",
    )
    parser.add_argument("--days", type=int, default=30, help="Number of days to search (default: 30)")
    parser.add_argument("--max-events", type=int, default=500, help="Maximum events to fetch (default: 500)")
    parser.add_argument("--from-cache", type=str, default=None, help="Load events from cached JSON directory instead of API")
    parser.add_argument("--skip-overture", action="store_true", help="Skip Overture Maps check (visual-only mode)")
    parser.add_argument("--output", type=str, default="intersection_braking_results.tsv", help="Output TSV file path")
    args = parser.parse_args()

    results = run_pipeline(args)
    write_results(results, args.output)
    print_summary(results)


if __name__ == "__main__":
    main()
