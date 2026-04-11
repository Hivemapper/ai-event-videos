#!/usr/bin/env python3
"""Benchmark privacy models against g-force highlight events. Flat output folder."""

import hashlib
import json
import os
import random
import re
import sys
import time
from pathlib import Path

import cv2
import numpy as np
import requests
import torch
from ultralytics import YOLO

MODELS = {
    "privacy_v8_all":  Path("/Users/as/Downloads/privacy_v8_all.pt"),
    "privacy_v8_asia": Path("/Users/as/Downloads/privacy_v8_asia.pt"),
    "privacy_v8_eu":   Path("/Users/as/Downloads/privacy_v8_eu.pt"),
    "privacy_v8_us":   Path("/Users/as/Downloads/privacy_v8_us.pt"),
}

API_BASE_URL = "https://beemaps.com/api/developer/aievents"
OUTPUT_DIR = Path(os.path.expanduser("~/Desktop/privacy-benchmark-gforce"))
VIDEO_CACHE = Path("/tmp/privacy-benchmark/videos")

BLUR_CLASSES = {0, 2}  # face, license-plate
CLASS_NAMES = {0: "face", 1: "person", 2: "license-plate", 3: "car", 4: "bus",
               5: "truck", 6: "motorcycle", 7: "bicycle"}

MODEL_COLORS = {
    "privacy_v8_all":  (0, 255, 0),
    "privacy_v8_asia": (0, 255, 255),
    "privacy_v8_eu":   (255, 0, 0),
    "privacy_v8_us":   (0, 0, 255),
}


def load_api_key() -> str:
    env_path = Path(__file__).resolve().parent.parent / ".env.local"
    for line in env_path.read_text().splitlines():
        if line.startswith("BEEMAPS_API_KEY="):
            return line.split("=", 1)[1].strip()
    sys.exit("No BEEMAPS_API_KEY")


def api_get(url, key, **kw):
    return requests.get(url, headers={"Authorization": f"Basic {key}"}, **kw)


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
        print(f"  [!] Download failed: {e}")
        p.unlink(missing_ok=True)
        return None


def extract_frames(video_path: Path, num: int) -> list[tuple[int, np.ndarray]]:
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return []
    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if total <= 0:
        cap.release()
        return []
    step = max(1, total // num)
    frames = []
    for i in range(0, total, step):
        if len(frames) >= num:
            break
        cap.set(cv2.CAP_PROP_POS_FRAMES, i)
        ret, frame = cap.read()
        if ret:
            frames.append((i, frame))
    cap.release()
    return frames


def run_model(model, frame, conf=0.15):
    results = model.predict(frame, conf=conf, imgsz=640, verbose=False)
    dets = []
    for result in results:
        for box in result.boxes:
            cls = int(box.cls[0]) if box.cls is not None else -1
            if cls not in BLUR_CLASSES:
                continue
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            dets.append({
                "x1": x1, "y1": y1, "x2": x2, "y2": y2,
                "conf": float(box.conf[0]),
                "cls": cls, "cls_name": CLASS_NAMES.get(cls, "?"),
            })
    return dets


def draw_detections(frame, detections: dict[str, list[dict]]) -> np.ndarray:
    out = frame.copy()
    for model_name, dets in detections.items():
        color = MODEL_COLORS.get(model_name, (255, 255, 255))
        for d in dets:
            x1, y1, x2, y2 = int(d["x1"]), int(d["y1"]), int(d["x2"]), int(d["y2"])
            cv2.rectangle(out, (x1, y1), (x2, y2), color, 2)
            label = f"{model_name.replace('privacy_v8_', '')} {d['cls_name']}: {d['conf']:.2f}"
            cv2.putText(out, label, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.4, color, 1)
    return out


def load_gforce_events() -> list[dict]:
    """Load g-force highlight events with id and location."""
    hl_path = Path(__file__).resolve().parent.parent / "src/lib/highlights.ts"
    text = hl_path.read_text()
    start = text.index('"Highest G-Force"')
    next_sec = text.find("title:", start + 100)
    chunk = text[start:next_sec] if next_sec != -1 else text[start:]
    ids = re.findall(r'id:\s*"([a-f0-9]+)"', chunk)
    locs = re.findall(r'location:\s*"([^"]+)"', chunk)
    results = []
    for i, eid in enumerate(ids):
        loc = locs[i] if i < len(locs) else ""
        results.append({"id": eid, "location": loc})
    return results


# Region mapping for model selection
ASIA_COUNTRIES = {"Australia", "Japan", "South Korea", "China", "India", "Thailand",
                  "Indonesia", "Philippines", "Vietnam", "Malaysia", "Singapore",
                  "Taiwan", "Hong Kong", "New Zealand"}
US_COUNTRIES = {"USA", "United States", "Mexico", "México", "Canada", "Brazil",
                "Colombia", "Argentina", "Chile", "Peru", "Uruguay"}
# Everything else (especially European) → eu


def pick_regional_model(location: str) -> str:
    """Pick the correct regional model based on event location."""
    country = location.split(",")[-1].strip()
    if country in US_COUNTRIES:
        return "privacy_v8_us"
    if country in ASIA_COUNTRIES:
        return "privacy_v8_asia"
    # Default to EU for European countries and unknowns with alpha chars
    if any(c.isalpha() for c in country):
        return "privacy_v8_eu"
    # Numeric-only (coordinates) — use all
    return "privacy_v8_all"


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-events", type=int, default=10)
    parser.add_argument("--frames-per-video", type=int, default=30)
    parser.add_argument("--conf", type=float, default=0.15)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    api_key = load_api_key()

    device = "cuda" if torch.cuda.is_available() else ("mps" if torch.backends.mps.is_available() else "cpu")
    print(f"Device: {device}")

    print("\n=== Loading models ===")
    loaded = {}
    for name, path in MODELS.items():
        if not path.exists():
            continue
        loaded[name] = YOLO(str(path))
        print(f"  {name}: loaded")

    all_events = load_gforce_events()
    sample = random.sample(all_events, min(args.num_events, len(all_events)))
    print(f"\n=== Sampling {len(sample)} g-force events from {len(all_events)} total ===")

    results_summary = []

    for idx, hl_event in enumerate(sample):
        event_id = hl_event["id"]
        location = hl_event["location"]
        regional_name = pick_regional_model(location)
        print(f"\n--- Event {idx+1}/{len(sample)}: {event_id[:16]}... ---")
        print(f"  Location: {location} → model: {regional_name}")

        # Fetch event details
        try:
            resp = requests.get(f"{API_BASE_URL}/{event_id}",
                                headers={"Authorization": f"Basic {api_key}"})
            resp.raise_for_status()
            event = resp.json()
        except Exception as e:
            print(f"  [!] Fetch failed: {e}")
            continue

        video_url = event.get("videoUrl")
        if not video_url:
            print("  No video URL, skipping")
            continue

        video_path = download_video(video_url, api_key)
        if not video_path:
            continue

        frames = extract_frames(video_path, args.frames_per_video)
        print(f"  Extracted {len(frames)} frames")

        # Run only: "all" model vs correct regional model
        models_to_run = {"privacy_v8_all": loaded["privacy_v8_all"]}
        if regional_name in loaded and regional_name != "privacy_v8_all":
            models_to_run[regional_name] = loaded[regional_name]

        event_results = {
            "event_id": event_id, "type": event.get("type", "?"),
            "location": location, "regional_model": regional_name,
        }

        for model_name, model in models_to_run.items():
            total_dets = 0
            total_time = 0.0
            all_confs = []
            for _, frame in frames:
                t0 = time.time()
                dets = run_model(model, frame, conf=args.conf)
                total_time += time.time() - t0
                total_dets += len(dets)
                all_confs.extend([d["conf"] for d in dets])

            avg_ms = (total_time / len(frames) * 1000) if frames else 0
            avg_conf = sum(all_confs) / len(all_confs) if all_confs else 0
            event_results[model_name] = {
                "detections": total_dets,
                "avg_ms_per_frame": round(avg_ms, 1),
                "avg_confidence": round(avg_conf, 3),
            }
            short = model_name.replace("privacy_v8_", "")
            print(f"  {short}: {total_dets} dets, {avg_ms:.1f}ms/frame, conf={avg_conf:.3f}")

        # Save annotated frames — flat, every 5th frame
        sample_indices = list(range(0, len(frames), max(1, len(frames) // 5)))[:5]
        for si in sample_indices:
            frame_idx, frame = frames[si]
            all_dets = {}
            for mn, m in models_to_run.items():
                all_dets[mn] = run_model(m, frame, conf=args.conf)
            annotated = draw_detections(frame, all_dets)
            out_path = OUTPUT_DIR / f"{event_id[:16]}_frame{frame_idx:05d}.jpg"
            cv2.imwrite(str(out_path), annotated)

        results_summary.append(event_results)

    # Summary
    print("\n" + "=" * 110)
    print("SUMMARY: all model vs correct regional model")
    print("=" * 110)
    print(f"{'Event':<18} {'Location':<28} {'Region':<6} | {'all dets':>8} {'all ms':>7} | {'regional':>8} {'reg ms':>7} | {'diff':>5}")
    print("-" * 110)

    all_total = 0
    reg_total = 0
    for r in results_summary:
        loc = r.get("location", "?")[:26]
        reg = r.get("regional_model", "?").replace("privacy_v8_", "")
        a = r.get("privacy_v8_all", {})
        regional_key = r.get("regional_model", "")
        rr = r.get(regional_key, a)  # fallback to all if regional == all
        a_dets = a.get("detections", 0)
        r_dets = rr.get("detections", 0)
        a_ms = a.get("avg_ms_per_frame", 0)
        r_ms = rr.get("avg_ms_per_frame", 0)
        diff = r_dets - a_dets
        diff_str = f"+{diff}" if diff > 0 else str(diff)
        print(f"{r['event_id'][:16]:<18} {loc:<28} {reg:<6} | {a_dets:>8} {a_ms:>6.1f}ms | {r_dets:>8} {r_ms:>6.1f}ms | {diff_str:>5}")
        all_total += a_dets
        reg_total += r_dets

    print("-" * 110)
    diff = reg_total - all_total
    diff_str = f"+{diff}" if diff > 0 else str(diff)
    print(f"{'TOTAL':<53} | {all_total:>8}         | {reg_total:>8}         | {diff_str:>5}")
    print()

    with open(OUTPUT_DIR / "results.json", "w") as f:
        json.dump(results_summary, f, indent=2)
    print(f"\nAll frames: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
