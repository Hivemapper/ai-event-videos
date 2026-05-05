#!/usr/bin/env python3
"""Benchmark privacy_v8_all.pt for both face and license-plate detection.
Pulls 20 random events from EU + Mexico, daytime only."""

import hashlib
import json
import math
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import cv2
import numpy as np
import requests
import torch
from ultralytics import YOLO

MODEL_PATH = Path("/Users/as/Downloads/privacy_v8_all.pt")
API_BASE_URL = "https://beemaps.com/api/developer/aievents"
OUTPUT_DIR = Path(os.path.expanduser("~/Desktop/privacy-benchmark-faces"))
VIDEO_CACHE = Path("/tmp/privacy-benchmark/videos")

BLUR_CLASSES = {0, 2}  # face, license-plate
CLASS_NAMES = {0: "face", 1: "person", 2: "license-plate", 3: "car", 4: "bus",
               5: "truck", 6: "motorcycle", 7: "bicycle"}

# Colors: green for face, red for license-plate
CLASS_COLORS = {0: (0, 255, 0), 2: (0, 0, 255)}


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


def is_daytime(ts: str, lat: float, lon: float) -> bool:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    n = dt.timetuple().tm_yday
    decl = math.radians(-23.44 * math.cos(math.radians(360 / 365 * (n + 10))))
    solar_noon_utc = 12.0 - lon / 15.0
    hour = dt.hour + dt.minute / 60.0
    ha = math.radians(15.0 * (hour - solar_noon_utc))
    lat_r = math.radians(lat)
    sin_e = math.sin(lat_r) * math.sin(decl) + math.cos(lat_r) * math.cos(decl) * math.cos(ha)
    return math.degrees(math.asin(max(-1, min(1, sin_e)))) > 5


# EU + Mexico bounding boxes for filtering
REGIONS = {
    "EU": {"bbox": [-12, 35, 40, 72]},       # Western/Central Europe
    "Mexico": {"bbox": [-118, 14, -86, 33]},  # Mexico
}


def fetch_events(api_key: str, num: int) -> list[dict]:
    """Fetch random daytime events from EU and Mexico regions."""
    events = []
    seen = set()
    types = ["HARSH_BRAKING", "HIGH_SPEED", "HARSH_ACCELERATION", "HIGH_G_FORCE"]

    for region_name, region in REGIONS.items():
        bbox = region["bbox"]
        for etype in types:
            if len(events) >= num:
                break
            # Get total
            try:
                probe = api_post(f"{API_BASE_URL}/search", api_key, {
                    "startDate": "2026-03-15T00:00:00.000Z",
                    "endDate": "2026-04-10T23:59:59.999Z",
                    "types": [etype],
                    "bbox": bbox,
                    "limit": 1, "offset": 0,
                })
            except Exception:
                continue
            total = probe.get("pagination", {}).get("total", 0)
            if total == 0:
                continue

            attempts = 0
            while len(events) < num and attempts < 5:
                offset = random.randint(0, max(0, total - 50))
                try:
                    data = api_post(f"{API_BASE_URL}/search", api_key, {
                        "startDate": "2026-03-15T00:00:00.000Z",
                        "endDate": "2026-04-10T23:59:59.999Z",
                        "types": [etype],
                        "bbox": bbox,
                        "limit": 50, "offset": offset,
                    })
                except Exception:
                    attempts += 1
                    continue
                batch = data.get("events", [])
                random.shuffle(batch)
                for e in batch:
                    eid = e.get("id", "")
                    if eid in seen or not e.get("videoUrl"):
                        continue
                    loc = e.get("location", {})
                    ts = e.get("timestamp", "")
                    if ts and loc.get("lat") is not None:
                        if not is_daytime(ts, loc["lat"], loc.get("lon", 0)):
                            continue
                    seen.add(eid)
                    e["_region"] = region_name
                    events.append(e)
                    if len(events) >= num:
                        break
                attempts += 1

    random.shuffle(events)
    return events[:num]


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


def draw_detections(frame, dets: list[dict]) -> np.ndarray:
    out = frame.copy()
    for d in dets:
        color = CLASS_COLORS.get(d["cls"], (255, 255, 255))
        x1, y1, x2, y2 = int(d["x1"]), int(d["y1"]), int(d["x2"]), int(d["y2"])
        cv2.rectangle(out, (x1, y1), (x2, y2), color, 2)
        label = f"{d['cls_name']}: {d['conf']:.2f}"
        cv2.putText(out, label, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.4, color, 1)
    return out


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-events", type=int, default=20)
    parser.add_argument("--frames-per-video", type=int, default=30)
    parser.add_argument("--conf", type=float, default=0.15)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    api_key = load_api_key()

    device = "cuda" if torch.cuda.is_available() else ("mps" if torch.backends.mps.is_available() else "cpu")
    print(f"Device: {device}")

    model = YOLO(str(MODEL_PATH))
    print(f"Model loaded: {MODEL_PATH.name}")

    print(f"\n=== Fetching {args.num_events} daytime events from EU + Mexico ===")
    events = fetch_events(api_key, args.num_events)
    eu_count = sum(1 for e in events if e.get("_region") == "EU")
    mx_count = sum(1 for e in events if e.get("_region") == "Mexico")
    print(f"  Got {len(events)} events (EU: {eu_count}, Mexico: {mx_count})")

    results_summary = []

    for idx, event in enumerate(events):
        eid = event.get("id", "?")
        etype = event.get("type", "?")
        region = event.get("_region", "?")
        loc = event.get("location", {})
        lat = loc.get("lat", 0)
        lon = loc.get("lon", 0)
        print(f"\n--- Event {idx+1}/{len(events)}: {eid[:16]}... ({etype}, {region}, {lat:.2f},{lon:.2f}) ---")

        video_url = event.get("videoUrl")
        if not video_url:
            print("  No video URL, skipping")
            continue

        video_path = download_video(video_url, api_key)
        if not video_path:
            continue

        frames = extract_frames(video_path, args.frames_per_video)
        print(f"  {len(frames)} frames")

        face_dets = 0
        plate_dets = 0
        face_confs = []
        plate_confs = []
        total_time = 0.0

        for _, frame in frames:
            t0 = time.time()
            dets = run_model(model, frame, conf=args.conf)
            total_time += time.time() - t0
            for d in dets:
                if d["cls"] == 0:
                    face_dets += 1
                    face_confs.append(d["conf"])
                elif d["cls"] == 2:
                    plate_dets += 1
                    plate_confs.append(d["conf"])

        avg_ms = (total_time / len(frames) * 1000) if frames else 0
        avg_face_conf = sum(face_confs) / len(face_confs) if face_confs else 0
        avg_plate_conf = sum(plate_confs) / len(plate_confs) if plate_confs else 0

        print(f"  faces: {face_dets} (conf={avg_face_conf:.3f})  plates: {plate_dets} (conf={avg_plate_conf:.3f})  {avg_ms:.1f}ms/frame")

        # Save annotated frames — 5 per video
        sample_indices = list(range(0, len(frames), max(1, len(frames) // 5)))[:5]
        for si in sample_indices:
            fi, frame = frames[si]
            dets = run_model(model, frame, conf=args.conf)
            annotated = draw_detections(frame, dets)
            out_path = OUTPUT_DIR / f"{eid[:16]}_frame{fi:05d}.jpg"
            cv2.imwrite(str(out_path), annotated)

        results_summary.append({
            "event_id": eid, "type": etype, "region": region,
            "faces": face_dets, "plates": plate_dets,
            "avg_face_conf": round(avg_face_conf, 3),
            "avg_plate_conf": round(avg_plate_conf, 3),
            "avg_ms": round(avg_ms, 1),
        })

    # Summary
    print("\n" + "=" * 110)
    print("SUMMARY — privacy_v8_all.pt (faces=green, plates=red)")
    print("=" * 110)
    print(f"{'Event':<18} {'Type':<20} {'Region':<8} | {'Faces':>5} {'f_conf':>6} | {'Plates':>6} {'p_conf':>6} | {'ms/fr':>6}")
    print("-" * 110)

    tot_faces = tot_plates = 0
    for r in results_summary:
        print(f"{r['event_id'][:16]:<18} {r['type']:<20} {r['region']:<8} | "
              f"{r['faces']:>5} {r['avg_face_conf']:>6.3f} | "
              f"{r['plates']:>6} {r['avg_plate_conf']:>6.3f} | "
              f"{r['avg_ms']:>5.1f}ms")
        tot_faces += r["faces"]
        tot_plates += r["plates"]

    print("-" * 110)
    print(f"{'TOTAL':<48} | {tot_faces:>5}        | {tot_plates:>6}        |")
    print(f"\n{len(results_summary)} videos, {tot_faces} face detections, {tot_plates} plate detections")

    with open(OUTPUT_DIR / "results.json", "w") as f:
        json.dump(results_summary, f, indent=2)
    print(f"All frames: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
