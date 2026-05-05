#!/usr/bin/env python3
"""
Persistent detection server — loads GDINO + CLIP once, processes queued runs.

Polls detection_runs table for status='queued' runs and processes them
without reloading models. Saves ~15s model load per video.

Usage:
    python3 scripts/detection-server.py
    python3 scripts/detection-server.py --poll 5
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import platform
import signal
import sqlite3
import subprocess
import sys
import time
import traceback
import threading
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

import cv2
import numpy as np
import requests
import torch
from PIL import Image

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_detection import (
    PROJECT_ROOT, DB_PATH, CACHE_DIR, FRAMES_PER_VIDEO, DEFAULT_FRAME_STRIDE, PIPELINE_VERSION,
    API_BASE_URL,
    GDINO_TEXT_PROMPT, GDINO_BOX_THRESHOLD, GDINO_TEXT_THRESHOLD,
    CLIP_MODEL_NAME, CLIP_PRETRAINED, CLIP_MIN_SIMILARITY,
    AMBIGUOUS_CLASSES, NON_AMBIGUOUS_CLASSES, CLIP_LABEL_REMAP,
    ANIMAL_LABELS, ANIMAL_MIN_CONFIDENCE, ANIMAL_MIN_FRAMES, ANIMAL_FRAME_GAP_MS,
    get_device, free_gpu, download_video, extract_frames,
    load_api_key, utc_now,
    parse_frame_target, parse_frame_stride, fetch_event_for_detection,
    should_cleanup_video_path,
    normalize_gdino_label, apply_nms, filter_ego_vehicle,
    filter_non_collision_vehicles, penalize_riderless, _filter_animal_detections,
    update_run_status, get_detection_run,
    get_db,
)

BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RED = "\033[31m"
RESET = "\033[0m"

MODEL_NAME = "gdino-base+clip"
MIN_PRIORITY_FIRMWARE_NUM = 7_004_003
DETECTION_PRIORITY_DEFAULT = 100


class ModelCache:
    """Holds pre-loaded GDINO + CLIP models."""

    def __init__(self):
        self.gdino_model = None
        self.gdino_processor = None
        self.clip_model = None
        self.clip_preprocess = None
        self.clip_tokenizer = None
        self.device = "cpu"
        self.use_half = False
        self.clip_half = False
        self.precomputed_text_features: dict[str, Any] = {}

    def load_gdino(self):
        from transformers import AutoModelForZeroShotObjectDetection, AutoProcessor

        print(f"\n  {CYAN}Loading GDINO-base...{RESET}")
        model_id = "IDEA-Research/grounding-dino-base"
        self.gdino_processor = AutoProcessor.from_pretrained(model_id)
        self.gdino_model = AutoModelForZeroShotObjectDetection.from_pretrained(model_id)
        self.gdino_model.eval()

        self.device = get_device()
        self.use_half = False  # GDINO's BERT encoder doesn't support float16

        if self.device in ("cuda", "mps"):
            try:
                self.gdino_model = self.gdino_model.to(self.device)
                dummy = self.gdino_processor(
                    images=Image.new("RGB", (64, 64)), text="test.", return_tensors="pt"
                )
                dummy = {k: v.to(self.device) if hasattr(v, "to") else v for k, v in dummy.items()}
                with torch.no_grad():
                    self.gdino_model(**dummy)
                print(f"  {GREEN}[GDINO] Running on {self.device.upper()} (float32){RESET}")
            except Exception as exc:
                self.gdino_model = self.gdino_model.to("cpu")
                self.device = "cpu"
                print(f"  {RED}[GDINO] GPU failed ({exc}), running on CPU{RESET}")

        # Skip torch.compile — incompatible with transformers 5.x on CUDA
        # The CUDA GPU acceleration alone provides sufficient speedup

    def load_clip(self):
        try:
            import open_clip
        except ImportError:
            print(f"  {YELLOW}[CLIP] open_clip not installed, skipping{RESET}")
            return

        print(f"  {CYAN}Loading CLIP {CLIP_MODEL_NAME}...{RESET}")
        self.clip_half = self.device == "cuda"
        try:
            self.clip_model, _, self.clip_preprocess = open_clip.create_model_and_transforms(
                CLIP_MODEL_NAME, pretrained=CLIP_PRETRAINED, device=self.device,
            )
            self.clip_tokenizer = open_clip.get_tokenizer(CLIP_MODEL_NAME)
            if self.clip_half:
                self.clip_model = self.clip_model.half()
            if self.device != "cpu":
                print(f"  {GREEN}[CLIP] Running on {self.device.upper()}{RESET}")
        except Exception as exc:
            if self.device != "cpu":
                print(f"  {RED}[CLIP] {self.device} failed, trying CPU{RESET}")
                self.clip_model, _, self.clip_preprocess = open_clip.create_model_and_transforms(
                    CLIP_MODEL_NAME, pretrained=CLIP_PRETRAINED, device="cpu",
                )
                self.clip_tokenizer = open_clip.get_tokenizer(CLIP_MODEL_NAME)
                self.clip_half = False
            else:
                raise
        self.clip_model.eval()

        # Pre-compute text features
        self._precompute_clip_text_features()

    def _precompute_clip_text_features(self):
        CLIP_PROMPT_TEMPLATES = [
            "a dashcam photo of a {label}",
            "a photo of a {label}",
            "a {label}",
        ]
        clip_device = self.device if self.clip_model is not None else "cpu"
        for ambiguous_label, candidates in AMBIGUOUS_CLASSES.items():
            if isinstance(candidates, list):
                ensembled = []
                for c in candidates:
                    feats = []
                    for tmpl in CLIP_PROMPT_TEMPLATES:
                        tokens = self.clip_tokenizer([tmpl.format(label=c)]).to(clip_device)
                        with torch.no_grad():
                            feat = self.clip_model.encode_text(tokens)
                            feat = feat / feat.norm(dim=-1, keepdim=True)
                        feats.append(feat)
                    avg = torch.mean(torch.cat(feats, dim=0), dim=0, keepdim=True)
                    avg = avg / avg.norm(dim=-1, keepdim=True)
                    ensembled.append(avg)
                self.precomputed_text_features[ambiguous_label] = torch.cat(ensembled, dim=0)

    def run_gdino(self, frames: list[tuple[int, Any, int]], batch_size: int = 8) -> list[dict]:
        """Run GDINO on frames using cached model."""
        GDINO_MAX_SIZE = 1080
        all_detections: list[dict] = []
        t0 = time.time()

        for batch_start in range(0, len(frames), batch_size):
            batch_frames = frames[batch_start:batch_start + batch_size]
            pil_images = []
            for f in batch_frames:
                img = Image.fromarray(cv2.cvtColor(f[1], cv2.COLOR_BGR2RGB))
                if img.width > GDINO_MAX_SIZE:
                    ratio = GDINO_MAX_SIZE / img.width
                    img = img.resize((GDINO_MAX_SIZE, int(img.height * ratio)), Image.BILINEAR)
                pil_images.append(img)

            inputs = self.gdino_processor(
                images=pil_images,
                text=[GDINO_TEXT_PROMPT] * len(pil_images),
                return_tensors="pt",
            )
            inputs = {k: v.to(self.device) if hasattr(v, "to") else v for k, v in inputs.items()}

            with torch.no_grad():
                outputs = self.gdino_model(**inputs)

            target_sizes = [(f[1].shape[0], f[1].shape[1]) for f in batch_frames]
            results = self.gdino_processor.post_process_grounded_object_detection(
                outputs, inputs["input_ids"],
                threshold=GDINO_BOX_THRESHOLD,
                text_threshold=GDINO_TEXT_THRESHOLD,
                target_sizes=target_sizes,
            )

            for j, (frame_ms, frame_bgr, frame_idx) in enumerate(batch_frames):
                h, w = frame_bgr.shape[:2]
                frame_dets: list[dict] = []
                if results and j < len(results):
                    r = results[j]
                    labels = r.get("text_labels", r.get("labels", []))
                    scores = r.get("scores", torch.tensor([]))
                    boxes = r.get("boxes", torch.tensor([]))
                    for label, score, box in zip(labels, scores, boxes):
                        conf = float(score.item()) if hasattr(score, "item") else float(score)
                        coords = box.cpu().tolist()
                        canonical = normalize_gdino_label(label)
                        if canonical is None:
                            continue
                        frame_dets.append({
                            "frame_ms": frame_ms,
                            "label": canonical,
                            "confidence": conf,
                            "x_min": coords[0], "y_min": coords[1],
                            "x_max": coords[2], "y_max": coords[3],
                            "frame_width": w, "frame_height": h,
                        })
                frame_dets = apply_nms(frame_dets, iou_threshold=0.5)
                frame_dets, _ = filter_ego_vehicle(frame_dets)
                all_detections.extend(frame_dets)

            processed = min(batch_start + batch_size, len(frames))
            if processed % 10 == 0 or processed == len(frames):
                print(f"    Frame {processed}/{len(frames)} -- {len(all_detections)} detections ({time.time()-t0:.1f}s)")

        print(f"  GDINO done: {len(all_detections)} detections in {time.time()-t0:.1f}s")
        return all_detections

    def run_clip_verify(self, detections: list[dict], frames_by_ms: dict[int, Any]) -> list[dict]:
        """Run CLIP verification on ambiguous detections using cached model."""
        if self.clip_model is None:
            return detections

        verified: list[dict] = []
        for det in detections:
            label = det["label"]
            if label in NON_AMBIGUOUS_CLASSES or label not in AMBIGUOUS_CLASSES:
                verified.append(det)
                continue

            frame_bgr = frames_by_ms.get(det["frame_ms"])
            if frame_bgr is None:
                continue

            h, w = frame_bgr.shape[:2]
            x1, y1 = max(0, int(det["x_min"])), max(0, int(det["y_min"]))
            x2, y2 = min(w, int(det["x_max"])), min(h, int(det["y_max"]))
            if x2 <= x1 or y2 <= y1:
                continue

            crop = Image.fromarray(cv2.cvtColor(frame_bgr[y1:y2, x1:x2], cv2.COLOR_BGR2RGB))
            tensor = self.clip_preprocess(crop).unsqueeze(0).to(self.device)
            if self.clip_half:
                tensor = tensor.half()

            with torch.no_grad():
                img_feat = self.clip_model.encode_image(tensor)
                img_feat = img_feat / img_feat.norm(dim=-1, keepdim=True)
                text_feat = self.precomputed_text_features[label]
                sims = (img_feat @ text_feat.T).squeeze(0).cpu().tolist()

            candidates = AMBIGUOUS_CLASSES[label]
            best_idx = int(np.argmax(sims))
            best_sim = sims[best_idx]

            if best_sim < CLIP_MIN_SIMILARITY:
                continue

            new_label = CLIP_LABEL_REMAP.get(candidates[best_idx], label)
            if new_label is None:
                continue

            out = dict(det)
            out["label"] = new_label
            verified.append(out)

        print(f"  CLIP: {len(detections)} -> {len(verified)} detections")
        return verified


def process_run(cache: ModelCache, run_id: str) -> bool:
    """Process a single detection run using cached models. Returns True on success."""
    conn = get_db()
    run = get_detection_run(conn, run_id)
    if not run:
        print(f"  {RED}Run not found{RESET}")
        conn.close()
        return False

    video_id = run["video_id"]
    config = json.loads(run["config_json"] or "{}")
    num_frames = parse_frame_target(config.get("framesPerVideo", FRAMES_PER_VIDEO))
    frame_stride = parse_frame_stride(config.get("frameStride"))

    video_path = None
    try:
        # Fetch event
        event = fetch_event_for_detection(video_id, include_sensor=True)

        # Download video
        video_url = event.get("videoUrl")
        if not video_url:
            raise RuntimeError("No video URL")
        video_path = download_video(video_url)
        if not video_path:
            raise RuntimeError("Video download failed")

        # Extract frames
        frames = extract_frames(video_path, num_frames, frame_stride=frame_stride)
        if not frames:
            raise RuntimeError("No frames extracted")

        # Run GDINO (cached model)
        detections = cache.run_gdino(frames, batch_size=8)

        # Run CLIP verification (cached model)
        frames_by_ms = {ms: bgr for ms, bgr, _ in frames}
        detections = cache.run_clip_verify(detections, frames_by_ms)

        # Post-processing filters
        detections, _ = filter_non_collision_vehicles(detections, event)
        penalize_riderless(detections)
        detections = _filter_animal_detections(detections)

        # Save to DB
        insert_sql = """INSERT INTO frame_detections
           (video_id, frame_ms, label, x_min, y_min, x_max, y_max,
            confidence, frame_width, frame_height, pipeline_version, model_name, run_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        insert_rows = [
            (
                video_id,
                det["frame_ms"],
                det["label"],
                det["x_min"],
                det["y_min"],
                det["x_max"],
                det["y_max"],
                det["confidence"],
                det["frame_width"],
                det["frame_height"],
                PIPELINE_VERSION,
                MODEL_NAME,
                run_id,
            )
            for det in detections
        ]
        total_saved = len(insert_rows)
        if insert_rows:
            conn.executemany(insert_sql, insert_rows)
            conn.commit()

        # Aggregate detections into time segments
        segments: list[tuple] = []
        if detections:
            frame_timestamps = sorted(set(d["frame_ms"] for d in detections))
            if len(frame_timestamps) >= 2:
                gaps = sorted(
                    frame_timestamps[i + 1] - frame_timestamps[i]
                    for i in range(len(frame_timestamps) - 1)
                )
                median_gap = gaps[len(gaps) // 2]
            else:
                median_gap = 1000
            gap_tolerance = int(median_gap * 1.5)

            by_label: dict[str, list[dict]] = defaultdict(list)
            for d in detections:
                by_label[d["label"]].append(d)

            for label, dets in by_label.items():
                dets.sort(key=lambda d: d["frame_ms"])
                seg_start = dets[0]["frame_ms"]
                seg_end = dets[0]["frame_ms"]
                seg_max_conf = dets[0]["confidence"]
                for i in range(1, len(dets)):
                    if dets[i]["frame_ms"] - seg_end <= gap_tolerance:
                        seg_end = dets[i]["frame_ms"]
                        seg_max_conf = max(seg_max_conf, dets[i]["confidence"])
                    else:
                        segments.append((
                            video_id, label, seg_start, seg_end,
                            seg_max_conf, "supported", PIPELINE_VERSION, MODEL_NAME, run_id,
                        ))
                        seg_start = dets[i]["frame_ms"]
                        seg_end = dets[i]["frame_ms"]
                        seg_max_conf = dets[i]["confidence"]
                segments.append((
                    video_id, label, seg_start, seg_end,
                    seg_max_conf, "supported", PIPELINE_VERSION, MODEL_NAME, run_id,
                ))

        conn.execute("DELETE FROM video_detection_segments WHERE run_id = ?", (run_id,))
        seg_sql = (
            "INSERT INTO video_detection_segments "
            "(video_id, label, start_ms, end_ms, max_confidence, support_level, pipeline_version, source, run_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        if segments:
            conn.executemany(seg_sql, segments)
            conn.commit()

        update_run_status(conn, run_id, "completed", detection_count=total_saved)
        print(f"  {GREEN}Completed: {total_saved} detections, {len(segments)} segments{RESET}")
        return True

    except Exception as exc:
        print(f"  {RED}Error: {exc}{RESET}")
        traceback.print_exc()
        update_run_status(conn, run_id, "failed", last_error=str(exc)[:1000])
        return False
    finally:
        # Clean up cached downloads, but preserve local clipped/public source files.
        if video_path and video_path.exists() and should_cleanup_video_path(video_path):
            video_path.unlink(missing_ok=True)
        conn.close()


def get_machine_id() -> str:
    """Get a human-readable machine identifier."""
    try:
        # Try macOS ComputerName first
        return subprocess.check_output(
            ["scutil", "--get", "ComputerName"], timeout=2, stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        pass
    # Fall back to hostname (e.g. "ip-10-0-18-49" on EC2)
    return platform.node().split(".")[0] or "unknown"


MACHINE_ID = get_machine_id()


def claim_next_run(cache: ModelCache, args_ref=None) -> str | None:
    """Find and claim the next video to process. Returns run_id or None."""
    conn = get_db()
    try:
        # First check for existing queued detection runs (e.g. from web UI)
        rows = conn.execute(
            """SELECT dr.id AS id
               FROM detection_runs dr
               LEFT JOIN triage_results t ON t.id = dr.video_id
               LEFT JOIN video_frame_timing_qc q ON q.video_id = dr.video_id
               WHERE dr.status = 'queued'
               ORDER BY
                 COALESCE(dr.priority, ?) ASC,
                 CASE
                   WHEN t.firmware_version_num >= ?
                    AND q.bucket IN ('perfect', 'ok')
                   THEN 0 ELSE 1
                 END ASC,
                 CASE WHEN t.firmware_version_num >= ? THEN 0 ELSE 1 END ASC,
                 CASE WHEN q.bucket IN ('perfect', 'ok') THEN 0 ELSE 1 END ASC,
                 dr.created_at ASC
               LIMIT 10""",
            (
                DETECTION_PRIORITY_DEFAULT,
                MIN_PRIORITY_FIRMWARE_NUM,
                MIN_PRIORITY_FIRMWARE_NUM,
            ),
        ).fetchall()

        for row in rows:
            run_id = row[0] if isinstance(row, tuple) else row["id"]
            conn.execute(
                "UPDATE detection_runs SET status = 'running', started_at = ?, worker_pid = ?, machine_id = ? WHERE id = ? AND status = 'queued'",
                (utc_now(), os.getpid(), MACHINE_ID, run_id)
            )
            conn.commit()
            claimed = conn.execute(
                "SELECT status, worker_pid, machine_id FROM detection_runs WHERE id = ?",
                (run_id,)
            ).fetchone()
            if not claimed:
                continue
            status = claimed[0] if isinstance(claimed, tuple) else claimed["status"]
            worker_pid = claimed[1] if isinstance(claimed, tuple) else claimed["worker_pid"]
            machine_id = claimed[2] if isinstance(claimed, tuple) else claimed["machine_id"]
            if status == "running" and int(worker_pid or 0) == os.getpid() and machine_id == MACHINE_ID:
                return run_id

        # Pick from triage_results: signal events with no detection run yet
        candidate = conn.execute(
            """SELECT t.id AS id FROM triage_results t
               LEFT JOIN video_frame_timing_qc q ON q.video_id = t.id
               WHERE t.triage_result = 'signal'
                 AND (t.road_class IS NULL OR t.road_class != 'motorway')
                 AND (t.speed_min IS NULL OR t.speed_min < 45)
                 AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
               ORDER BY
                 CASE
                   WHEN t.firmware_version_num >= ?
                    AND q.bucket IN ('perfect', 'ok')
                   THEN 0 ELSE 1
                 END ASC,
                 CASE WHEN t.firmware_version_num >= ? THEN 0 ELSE 1 END ASC,
                 CASE WHEN q.bucket IN ('perfect', 'ok') THEN 0 ELSE 1 END ASC,
                 CASE WHEN t.event_timestamp IS NULL THEN 1 ELSE 0 END ASC,
                 julianday(t.event_timestamp) DESC,
                 t.created_at DESC
               LIMIT 1""",
            (MIN_PRIORITY_FIRMWARE_NUM, MIN_PRIORITY_FIRMWARE_NUM),
        ).fetchone()

        if not candidate:
            return None

        video_id = candidate[0] if isinstance(candidate, tuple) else candidate["id"]
        run_id = str(uuid.uuid4())
        config = json.dumps({
            "modelDisplayName": "GDINO Base (grounding-dino-base) + CLIP",
            "type": "Open-vocabulary (detect anything described in text)",
            "device": "CUDA",
            "framesPerVideo": args_ref.frames,
            "frameSampling": "every_n_frames" if args_ref.frame_stride else "max",
            "frameStride": args_ref.frame_stride,
        })
        conn.execute(
            """INSERT INTO detection_runs (id, video_id, model_name, status, config_json, machine_id, created_at, started_at, worker_pid)
               VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?)""",
            (run_id, video_id, MODEL_NAME, config, MACHINE_ID, utc_now(), utc_now(), os.getpid())
        )
        conn.commit()
        return run_id
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Persistent detection server")
    parser.add_argument("--poll", type=float, default=2, help="Poll interval (default: 2s)")
    parser.add_argument("--workers", type=int, default=1, help="Parallel workers (default: 1)")
    parser.add_argument("--frames", type=int, default=FRAMES_PER_VIDEO, help=f"Maximum frames per video (default: {FRAMES_PER_VIDEO})")
    parser.add_argument("--frame-stride", type=int, default=DEFAULT_FRAME_STRIDE, help=f"Sample every N source frames (default: {DEFAULT_FRAME_STRIDE})")
    args = parser.parse_args()
    args.frame_stride = parse_frame_stride(args.frame_stride)

    print(f"{BOLD}{'═' * 60}{RESET}")
    print(f"  Detection Server")
    print(f"  Machine: {MACHINE_ID}")
    print(f"  PID: {os.getpid()}")
    print(f"  Workers: {args.workers}")
    print(f"  Frames:  {args.frames}")
    if args.frame_stride:
        print(f"  Stride:  every {args.frame_stride} source frames")
    else:
        print("  Stride:  disabled")
    print(f"{BOLD}{'═' * 60}{RESET}")

    # Load models once
    t0 = time.time()
    cache = ModelCache()
    cache.load_gdino()
    cache.load_clip()
    load_time = time.time() - t0
    print(f"\n  {GREEN}Models loaded in {load_time:.1f}s{RESET}")
    print(f"  Polling for queued runs every {args.poll}s...")
    print(f"  Press Ctrl+C to stop\n")

    running = True
    def handle_signal(sig, frame):
        nonlocal running
        print(f"\n{YELLOW}Shutting down...{RESET}")
        running = False
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    runs_processed = 0
    lock = threading.Lock()
    active_workers = 0

    def worker_fn(run_id: str):
        nonlocal runs_processed, active_workers
        t_run = time.time()
        ok = process_run(cache, run_id)
        elapsed = time.time() - t_run
        with lock:
            if ok:
                runs_processed += 1
            active_workers -= 1
            status = f"{GREEN}done{RESET}" if ok else f"{RED}failed{RESET}"
            print(f"  {run_id[:12]}… {status} in {elapsed:.1f}s  [{active_workers}/{args.workers} active]")

    while running:
        with lock:
            slots = args.workers - active_workers

        if slots <= 0:
            time.sleep(0.5)
            continue

        try:
            run_id = claim_next_run(cache, args_ref=args)
        except Exception as exc:
            print(f"\n{RED}Queue poll failed: {exc}{RESET}", flush=True)
            traceback.print_exc()
            time.sleep(max(args.poll, 5))
            continue

        if not run_id:
            time.sleep(args.poll)
            continue

        with lock:
            active_workers += 1
        print(f"\n{CYAN}Run {run_id[:16]}…{RESET}  [{active_workers}/{args.workers} active]")

        if args.workers == 1:
            # Single worker: run inline (simpler, no threading overhead)
            worker_fn(run_id)
        else:
            t = threading.Thread(target=worker_fn, args=(run_id,), daemon=True)
            t.start()

    # Wait for active workers to finish
    while active_workers > 0:
        time.sleep(1)

    print(f"\n{BOLD}Server stopped. Processed {runs_processed} runs.{RESET}")
    del cache
    free_gpu()


if __name__ == "__main__":
    main()
