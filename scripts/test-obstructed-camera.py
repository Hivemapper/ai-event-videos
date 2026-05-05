#!/usr/bin/env python3
"""
Test obstructed camera detection using edge density and color variance.

Samples frames from videos and computes simple image quality metrics to
distinguish between road-facing dashcam footage and obstructed/interior shots.

Usage:
    python3 scripts/test-obstructed-camera.py <event_id> [event_id2 ...]
    python3 scripts/test-obstructed-camera.py --random 20
"""

import argparse
import hashlib
import os
import sys
from pathlib import Path

import cv2
import numpy as np
import requests

API_BASE_URL = "https://beemaps.com/api/developer/aievents"
VIDEO_CACHE = Path("/tmp/obstructed-test/videos")


def load_api_key() -> str:
    env_path = Path(__file__).resolve().parent.parent / ".env.local"
    for line in env_path.read_text().splitlines():
        if line.startswith("BEEMAPS_API_KEY="):
            return line.split("=", 1)[1].strip()
    sys.exit("No BEEMAPS_API_KEY")


def api_get(url, key, **kw):
    return requests.get(url, headers={"Authorization": f"Basic {key}"}, **kw)


def api_post(url, key, body):
    r = requests.post(url, json=body,
                      headers={"Authorization": f"Basic {key}", "Content-Type": "application/json"})
    r.raise_for_status()
    return r.json()


def download_video(video_url: str, api_key: str) -> Path | None:
    VIDEO_CACHE.mkdir(parents=True, exist_ok=True)
    h = hashlib.md5(video_url.encode()).hexdigest()
    p = VIDEO_CACHE / f"{h}.mp4"
    if p.exists():
        return p
    try:
        r = api_get(video_url, api_key, stream=True, timeout=120)
        r.raise_for_status()
        with p.open("wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
        return p
    except Exception as e:
        print(f"  Download failed: {e}")
        p.unlink(missing_ok=True)
        return None


def analyze_frame(frame: np.ndarray) -> dict:
    """Compute quality metrics for a single frame."""
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    h, w = gray.shape

    # Edge density: ratio of edge pixels to total pixels
    edges = cv2.Canny(gray, 50, 150)
    edge_density = np.count_nonzero(edges) / (h * w)

    # Color variance: std dev across all channels
    color_std = float(np.std(frame))

    # Brightness: mean intensity
    brightness = float(np.mean(gray))

    # Laplacian variance (focus/sharpness measure)
    laplacian_var = float(cv2.Laplacian(gray, cv2.CV_64F).var())

    # Saturation: mean saturation in HSV
    hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
    saturation = float(np.mean(hsv[:, :, 1]))

    # Sky detection: % of top 1/3 that is bright (>180) — road scenes have sky
    top_third = gray[:h // 3, :]
    sky_ratio = float(np.count_nonzero(top_third > 180)) / top_third.size

    return {
        "edge_density": round(edge_density, 4),
        "color_std": round(color_std, 1),
        "brightness": round(brightness, 1),
        "laplacian_var": round(laplacian_var, 1),
        "saturation": round(saturation, 1),
        "sky_ratio": round(sky_ratio, 4),
    }


def analyze_video(video_path: Path, num_samples: int = 5) -> dict:
    """Sample frames and compute average metrics."""
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return {}

    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if total <= 0:
        cap.release()
        return {}

    step = max(1, total // num_samples)
    metrics_list = []

    for i in range(0, total, step):
        if len(metrics_list) >= num_samples:
            break
        cap.set(cv2.CAP_PROP_POS_FRAMES, i)
        ret, frame = cap.read()
        if not ret:
            break
        metrics_list.append(analyze_frame(frame))

    cap.release()

    if not metrics_list:
        return {}

    # Average all metrics
    avg = {}
    for key in metrics_list[0]:
        vals = [m[key] for m in metrics_list]
        avg[key] = round(sum(vals) / len(vals), 4)

    return avg


def is_obstructed(metrics: dict) -> tuple[bool, str]:
    """Classify whether the video appears obstructed based on metrics."""
    if not metrics:
        return True, "no metrics"

    # Strong signals — any one of these alone means obstructed
    # (very specific, unlikely to false-positive on normal road scenes)

    # Nearly black frame — camera completely blocked
    if metrics["brightness"] < 30 and metrics["edge_density"] < 0.01:
        return True, f"black frame (bright={metrics['brightness']:.1f}, edge={metrics['edge_density']:.4f})"

    # Zero edges + low sharpness — lens covered or pointing at flat surface
    if metrics["edge_density"] < 0.005:
        return True, f"no edges ({metrics['edge_density']:.4f})"

    # Very low saturation + low edges — monochrome interior (floor mats, seats)
    if metrics["saturation"] < 30 and metrics["edge_density"] < 0.05:
        return True, f"desaturated interior (sat={metrics['saturation']:.1f}, edge={metrics['edge_density']:.4f})"

    # Soft signals — need 2+ to flag
    reasons = []

    if metrics["edge_density"] < 0.02:
        reasons.append(f"low edges ({metrics['edge_density']:.4f})")

    if metrics["laplacian_var"] < 50:
        reasons.append(f"blurry ({metrics['laplacian_var']:.1f})")

    if metrics["color_std"] < 25:
        reasons.append(f"uniform color ({metrics['color_std']:.1f})")

    if metrics["sky_ratio"] < 0.01 and metrics["brightness"] < 70:
        reasons.append(f"no sky, dark ({metrics['sky_ratio']:.4f})")

    if metrics["edge_density"] < 0.03 and metrics["laplacian_var"] < 80:
        reasons.append(f"flat+blurry")

    if len(reasons) >= 2:
        return True, "; ".join(reasons)

    return False, "OK"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("event_ids", nargs="*", help="Event IDs to test")
    parser.add_argument("--random", type=int, default=0,
                        help="Fetch N random events to test")
    args = parser.parse_args()

    api_key = load_api_key()

    event_ids = list(args.event_ids)

    if args.random > 0:
        import random
        data = api_post(f"{API_BASE_URL}/search", api_key, {
            "startDate": "2026-04-01T00:00:00.000Z",
            "endDate": "2026-04-13T23:59:59.999Z",
            "types": ["HARSH_BRAKING"],
            "limit": 200,
            "offset": random.randint(0, 5000),
        })
        candidates = [e["id"] for e in data.get("events", []) if e.get("videoUrl")]
        random.shuffle(candidates)
        event_ids.extend(candidates[:args.random])

    print(f"Testing {len(event_ids)} events\n")
    print(f"{'Event ID':<28} {'Edge':>6} {'Color':>6} {'Bright':>6} {'Laplac':>7} {'Satur':>6} {'Sky':>7}  {'Result'}")
    print("-" * 110)

    obstructed_count = 0
    for eid in event_ids:
        # Fetch event
        try:
            resp = requests.get(f"{API_BASE_URL}/{eid}",
                                headers={"Authorization": f"Basic {api_key}"})
            resp.raise_for_status()
            event = resp.json()
        except Exception as e:
            print(f"{eid:<28} FETCH ERROR: {e}")
            continue

        video_url = event.get("videoUrl")
        if not video_url:
            print(f"{eid:<28} NO VIDEO")
            continue

        video_path = download_video(video_url, api_key)
        if not video_path:
            continue

        metrics = analyze_video(video_path)
        obstructed, reason = is_obstructed(metrics)
        if obstructed:
            obstructed_count += 1

        status = f"OBSTRUCTED: {reason}" if obstructed else "OK"
        print(f"{eid:<28} {metrics.get('edge_density', 0):>6.4f} {metrics.get('color_std', 0):>6.1f} "
              f"{metrics.get('brightness', 0):>6.1f} {metrics.get('laplacian_var', 0):>7.1f} "
              f"{metrics.get('saturation', 0):>6.1f} {metrics.get('sky_ratio', 0):>7.4f}  {status}")

    print(f"\n{obstructed_count}/{len(event_ids)} flagged as obstructed")


if __name__ == "__main__":
    main()
