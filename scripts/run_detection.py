#!/usr/bin/env python3
"""
Unified detection runner — spawned by the Next.js API.

Accepts --run-id, reads run config from the DB, downloads the video,
extracts frames, runs the appropriate model, saves detections, and
updates the run status throughout.

Supported models:
  - gdino-base-clip  (Grounding DINO base + OpenCLIP verification)
  - mm-gdino         (MM-Grounding-DINO large, V3Det-trained)
  - yolo-world       (YOLO-World v2 open vocabulary)
  - yolo11x          (YOLO11x standard COCO-80)
  - yolo26x          (YOLO26x standard COCO-80)

Usage:
    cd /Users/tylerlu/Projects/ai-event-videos
    source .venv/bin/activate
    python scripts/run_detection.py --run-id <UUID>
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import sqlite3
import sys
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import cv2
import numpy as np
import requests
import torch
from PIL import Image

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "labels.db"
CACHE_DIR = PROJECT_ROOT / "data" / "pipeline-video-cache"
PIPELINE_VERSION = "vru-yolo-v2"
FRAMES_PER_VIDEO = 75

API_BASE_URL = "https://beemaps.com/api/developer/aievents"

# ---------------------------------------------------------------------------
# GDINO configuration (copied from run_gdino_clip_pipeline.py)
# ---------------------------------------------------------------------------

GDINO_TEXT_PROMPT = (
    "person. bicycle. motorcycle. person on electric scooter. "
    "electric kick scooter. wheelchair. stroller. "
    "person wearing safety vest. skateboard. dog. traffic cone. "
    "car. truck. bus."
)
GDINO_BOX_THRESHOLD = 0.30
GDINO_TEXT_THRESHOLD = 0.25

# CLIP verification config
CLIP_MODEL_NAME = "ViT-B-32"
CLIP_PRETRAINED = "datacomp_xl_s13b_b90k"
CLIP_MIN_SIMILARITY = 0.26  # below this, drop the detection entirely

# Ambiguous classes and their CLIP candidate labels.
# For each ambiguous GDINO label, we run CLIP with these candidates.
# The first candidate is the "agreeing" label — if CLIP picks it, GDINO's
# label stands. Otherwise, the detection is relabeled to the CLIP winner.
AMBIGUOUS_CLASSES: dict[str, list[str]] = {
    "person wearing safety vest": [
        "construction worker with safety vest and hard hat",
        "pedestrian",
        "person walking",
    ],
    "person riding bicycle": [
        "person riding a bicycle",
        "pedestrian",
        "person on motorcycle",
    ],
    "stroller": [
        "baby stroller",
        "shopping cart",
        "wheelchair",
    ],
    "person on electric scooter": [
        "person riding electric scooter",
        "pedestrian",
        "person on motorcycle",
    ],
    "electric kick scooter": [
        "electric kick scooter",
        "bicycle",
        "skateboard",
    ],
}

# Map from CLIP winning label back to a canonical GDINO-style label for the DB
CLIP_LABEL_REMAP: dict[str, str | None] = {
    "construction worker with safety vest and hard hat": "construction worker",
    "pedestrian": "person",
    "person walking": "person",
    "person riding a bicycle": "cyclist",
    "person on motorcycle": "motorcyclist",
    "baby stroller": "stroller",
    "shopping cart": None,  # drop — not a VRU
    "wheelchair": "wheelchair",
    "person riding electric scooter": "scooter",
    "electric kick scooter": "scooter",
    "bicycle": "bicycle",
    "skateboard": "skateboard",
}

# Non-ambiguous classes — pass through without CLIP verification
NON_AMBIGUOUS_CLASSES = {
    "car", "truck", "bus", "motorcycle", "person", "dog",
    "traffic cone", "skateboard", "scooter", "bicycle",
    "wheelchair", "motorcyclist",
}

# All known valid labels (non-ambiguous + ambiguous)
ALL_KNOWN_LABELS = NON_AMBIGUOUS_CLASSES | set(AMBIGUOUS_CLASSES.keys())

# ---------------------------------------------------------------------------
# YOLO-World configuration (copied from run_yolo_world_single.py)
# ---------------------------------------------------------------------------

VRU_CLASSES = [
    "person", "bicycle", "motorcycle", "scooter",
    "wheelchair", "stroller", "person wearing safety vest",
    "skateboard", "dog", "traffic cone",
    "car", "truck", "bus",
]

YOLO_WORLD_CONF = 0.15

# ---------------------------------------------------------------------------
# YOLO11x COCO label map (copied from test_open_vocab.py / vru_pipeline_worker.py)
# ---------------------------------------------------------------------------

COCO_LABEL_MAP: dict[str, str] = {
    "person": "person",
    "bicycle": "bicycle",
    "motorcycle": "motorcycle",
    "car": "car",
    "truck": "truck",
    "bus": "bus",
    "train": "train",
    "stop sign": "stop sign",
    "cat": "cat",
    "dog": "dog",
    "skateboard": "skateboard",
}

YOLO11X_CONF = 0.25


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_env_var(name: str) -> str | None:
    """Load env var from environment or .env.local."""
    val = os.environ.get(name)
    if val:
        return val
    env_path = PROJECT_ROOT / ".env.local"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            if k.strip() == name:
                return v.strip()
    return None


def load_api_key() -> str:
    key = _load_env_var("BEEMAPS_API_KEY")
    if key:
        return key if key.startswith("Basic ") else f"Basic {key}"
    raise RuntimeError("BEEMAPS_API_KEY not found in env or .env.local")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_device() -> str:
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def free_gpu():
    """Force-free MPS/CUDA memory."""
    gc.collect()
    if hasattr(torch, "mps") and hasattr(torch.mps, "empty_cache"):
        torch.mps.empty_cache()


def download_video(video_url: str) -> Path | None:
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
        print(f"    [!] Download failed: {exc}")
        if path.exists():
            path.unlink(missing_ok=True)
        return None


def extract_frames(video_path: Path, num_frames: int) -> list[tuple[int, Any, int]]:
    """Returns list of (frame_ms, frame_bgr, frame_idx)."""
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return []
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    if total_frames <= 0:
        cap.release()
        return []
    margin = max(1, total_frames // 20)
    usable = total_frames - 2 * margin
    if usable <= 0 or num_frames == 1:
        indices = [total_frames // 2]
    else:
        step = usable / (num_frames - 1)
        indices = [margin + int(i * step) for i in range(num_frames)]
    frames = []
    for idx in indices:
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok, frame = cap.read()
        if ok:
            frame_ms = int(round((idx / fps) * 1000))
            frames.append((frame_ms, frame, idx))
    cap.release()
    return frames


# ---------------------------------------------------------------------------
# Database — uses Turso if env vars are set, otherwise local SQLite
# ---------------------------------------------------------------------------

class TursoCursor:
    """Mimics sqlite3.Cursor for libsql_client results."""

    def __init__(self, result_set):
        self._rs = result_set
        self._rows = result_set.rows if result_set else []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class TursoDb:
    """Wrapper around libsql_client that provides a sqlite3-like interface."""

    def __init__(self, client):
        self._client = client

    def execute(self, sql, params=None):
        import libsql_client
        if params:
            stmt = libsql_client.Statement(sql, list(params))
            rs = self._client.execute(stmt)
        else:
            rs = self._client.execute(sql)
        return TursoCursor(rs)

    def executemany(self, sql, rows):
        # Don't use this — use batch_insert instead
        raise NotImplementedError("Use batch_insert() for bulk inserts with TursoDb")

    def executescript(self, sql):
        import libsql_client
        # Split on semicolons and batch
        stmts = [s.strip() for s in sql.split(';') if s.strip()]
        if stmts:
            self._client.batch([libsql_client.Statement(s) for s in stmts])

    def batch_insert(self, sql, rows):
        """Efficient bulk insert using batch()."""
        import libsql_client
        stmts = [libsql_client.Statement(sql, list(row)) for row in rows]
        # batch() has a limit, chunk into groups of 500
        CHUNK = 500
        for i in range(0, len(stmts), CHUNK):
            self._client.batch(stmts[i:i + CHUNK])

    def commit(self):
        pass  # libsql_client auto-commits

    def sync(self):
        pass  # No-op for direct HTTP

    def close(self):
        self._client.close()


def get_db():
    turso_url = _load_env_var("TURSO_DATABASE_URL")
    turso_token = _load_env_var("TURSO_AUTH_TOKEN")

    if turso_url and turso_token:
        try:
            import libsql_client
            # libsql_client needs https:// not libsql://
            http_url = turso_url.replace("libsql://", "https://")
            client = libsql_client.create_client_sync(
                url=http_url,
                auth_token=turso_token,
            )
            conn = TursoDb(client)
            print(f"  DB: Connected to Turso ({turso_url.split('//')[1].split('.')[0]})")
        except ImportError:
            print("  [!] libsql_client not installed, falling back to local SQLite")
            print("      Install with: pip install libsql-client")
            conn = sqlite3.connect(str(DB_PATH))
    else:
        conn = sqlite3.connect(str(DB_PATH))
        print("  DB: Using local SQLite")

    if not isinstance(conn, TursoDb):
        conn.execute("PRAGMA journal_mode=WAL")

    # Ensure run_id column exists on frame_detections
    cols = [r[1] for r in conn.execute("PRAGMA table_info(frame_detections)").fetchall()]
    if "model_name" not in cols:
        conn.execute(
            "ALTER TABLE frame_detections ADD COLUMN model_name TEXT NOT NULL DEFAULT 'yolo11x'"
        )
        conn.commit()
    if "run_id" not in cols:
        conn.execute("ALTER TABLE frame_detections ADD COLUMN run_id TEXT")
        conn.commit()

    return conn


def commit_and_sync(conn) -> None:
    """Commit and, for sqlite3, flush to disk. TursoDb auto-commits."""
    conn.commit()


# ---------------------------------------------------------------------------
# Detection run DB helpers
# ---------------------------------------------------------------------------

def get_detection_run(conn, run_id: str) -> dict | None:
    """Read a detection_runs row and return as dict, or None if not found."""
    row = conn.execute(
        "SELECT id, video_id, model_name, status, config_json, detection_count, "
        "worker_pid, started_at, completed_at, last_heartbeat_at, last_error, created_at "
        "FROM detection_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "id": row[0],
        "video_id": row[1],
        "model_name": row[2],
        "status": row[3],
        "config_json": row[4],
        "detection_count": row[5],
        "worker_pid": row[6],
        "started_at": row[7],
        "completed_at": row[8],
        "last_heartbeat_at": row[9],
        "last_error": row[10],
        "created_at": row[11],
    }


def update_run_status(
    conn,
    run_id: str,
    status: str,
    *,
    detection_count: int | None = None,
    last_error: str | None = None,
) -> None:
    """Update detection_runs row status and optional fields."""
    fields = ["status = ?", "last_heartbeat_at = ?"]
    params: list[Any] = [status, utc_now()]

    if status == "running":
        fields.append("started_at = CASE WHEN started_at IS NULL THEN ? ELSE started_at END")
        params.append(utc_now())

    if status in ("completed", "failed"):
        fields.append("completed_at = ?")
        params.append(utc_now())

    if detection_count is not None:
        fields.append("detection_count = ?")
        params.append(detection_count)

    if last_error is not None:
        fields.append("last_error = ?")
        params.append(last_error)

    params.append(run_id)
    conn.execute(
        f"UPDATE detection_runs SET {', '.join(fields)} WHERE id = ?",
        tuple(params),
    )
    commit_and_sync(conn)


def update_heartbeat(conn, run_id: str, *, sync: bool = True) -> None:
    """Update last_heartbeat_at for the run."""
    conn.execute(
        "UPDATE detection_runs SET last_heartbeat_at = ? WHERE id = ?",
        (utc_now(), run_id),
    )
    if sync:
        commit_and_sync(conn)
    else:
        conn.commit()


# ---------------------------------------------------------------------------
# Save detections (with run_id)
# ---------------------------------------------------------------------------

def save_detections(
    conn,
    video_id: str,
    frame_ms: int,
    detections: list[dict],
    model_name: str,
    run_id: str,
) -> int:
    sql = """INSERT INTO frame_detections (
            video_id, frame_ms, label, x_min, y_min, x_max, y_max,
            confidence, frame_width, frame_height, pipeline_version, model_name, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    rows = [
        (
            video_id, frame_ms, d["label"],
            d["x_min"], d["y_min"], d["x_max"], d["y_max"],
            d["confidence"], d["frame_width"], d["frame_height"],
            PIPELINE_VERSION, model_name, run_id,
        )
        for d in detections
    ]
    if isinstance(conn, TursoDb):
        conn.batch_insert(sql, rows)
    else:
        conn.executemany(sql, rows)
    # Don't sync here — caller batches inserts and syncs once at the end
    conn.commit()
    return len(rows)


def ensure_video_pipeline_state(conn, video_id: str, day: str, model_name: str) -> None:
    existing = conn.execute(
        "SELECT 1 FROM video_pipeline_state WHERE video_id = ?", (video_id,)
    ).fetchone()
    if not existing:
        conn.execute(
            """INSERT INTO video_pipeline_state (
                video_id, day, status, pipeline_version, model_name, labels_applied, completed_at
            ) VALUES (?, ?, 'processed', ?, ?, '[]', datetime('now'))""",
            (video_id, day, PIPELINE_VERSION, model_name),
        )
        commit_and_sync(conn)


# ---------------------------------------------------------------------------
# GDINO label normalization (copied from run_gdino_clip_pipeline.py)
# ---------------------------------------------------------------------------

def normalize_gdino_label(raw_label: str) -> str | None:
    """Normalize GDINO's raw label output.

    Returns a canonical label string, or None if the label is garbage
    (BPE artifacts like '##clist', compound labels like 'truck bus', etc.).
    """
    label = raw_label.strip().lower()

    # Drop BPE tokenizer artifacts
    if "##" in label:
        return None

    # Exact match to a known label
    if label in ALL_KNOWN_LABELS:
        return label

    # Check for compound labels (e.g., "truck bus", "car bus", "person construction worker")
    # If a known label is an exact substring, use it — but only for unambiguous single-word matches
    # to avoid "person construction worker" -> "person" when it should be "construction worker"
    for known in sorted(ALL_KNOWN_LABELS, key=len, reverse=True):
        if known in label and " " in known:
            # Multi-word match (e.g., "construction worker" in "person construction worker")
            return known

    # Single-word known label as exact token
    tokens = label.split()
    for token in tokens:
        if token in NON_AMBIGUOUS_CLASSES:
            return token

    # Unrecognized — drop it
    return None


# ---------------------------------------------------------------------------
# Class-agnostic NMS (copied from run_gdino_clip_pipeline.py)
# ---------------------------------------------------------------------------

def apply_nms(detections: list[dict], iou_threshold: float = 0.5) -> list[dict]:
    """Apply class-agnostic Non-Maximum Suppression to a list of detection dicts.

    GDINO with multi-class prompts can produce overlapping boxes for the same
    object with different labels (e.g., same person detected as both "person"
    and "cyclist"). This removes duplicates by keeping the highest-confidence box.

    Args:
        detections: list of dicts, each with x_min, y_min, x_max, y_max, confidence.
        iou_threshold: IoU threshold above which the lower-confidence box is suppressed.

    Returns:
        Filtered list of detection dicts.
    """
    if len(detections) <= 1:
        return detections

    boxes = torch.tensor(
        [[d["x_min"], d["y_min"], d["x_max"], d["y_max"]] for d in detections],
        dtype=torch.float32,
    )
    scores = torch.tensor(
        [d["confidence"] for d in detections],
        dtype=torch.float32,
    )

    try:
        from torchvision.ops import nms as tv_nms
        keep_indices = tv_nms(boxes, scores, iou_threshold).tolist()
    except ImportError:
        # Manual NMS fallback using numpy
        x1 = boxes[:, 0].numpy()
        y1 = boxes[:, 1].numpy()
        x2 = boxes[:, 2].numpy()
        y2 = boxes[:, 3].numpy()
        scores_np = scores.numpy()
        areas = (x2 - x1) * (y2 - y1)

        order = scores_np.argsort()[::-1]
        keep_indices: list[int] = []

        while order.size > 0:
            idx = int(order[0])
            keep_indices.append(idx)

            xx1 = np.maximum(x1[idx], x1[order[1:]])
            yy1 = np.maximum(y1[idx], y1[order[1:]])
            xx2 = np.minimum(x2[idx], x2[order[1:]])
            yy2 = np.minimum(y2[idx], y2[order[1:]])

            inter = np.maximum(0.0, xx2 - xx1) * np.maximum(0.0, yy2 - yy1)
            iou = inter / (areas[idx] + areas[order[1:]] - inter + 1e-6)

            remaining = np.where(iou <= iou_threshold)[0]
            order = order[remaining + 1]

    return [detections[i] for i in sorted(keep_indices)]


# ---------------------------------------------------------------------------
# GDINO pass (copied from run_gdino_clip_pipeline.py)
# ---------------------------------------------------------------------------

def run_gdino_pass(
    frames: list[tuple[int, Any, int]],
    conn,
    run_id: str,
    batch_size: int = 8,
) -> list[dict]:
    """
    Load GDINO-base, run on all frames in batches, unload.
    Returns a flat list of detection dicts, each with an extra 'frame_ms' key.
    """
    from transformers import AutoModelForZeroShotObjectDetection, AutoProcessor

    print("\n  --- GDINO-base pass ---")
    model_id = "IDEA-Research/grounding-dino-base"
    print(f"  Loading {model_id}...")
    processor = AutoProcessor.from_pretrained(model_id)
    model = AutoModelForZeroShotObjectDetection.from_pretrained(model_id)
    model.eval()

    device = get_device()
    if device == "mps":
        try:
            model = model.to("mps")
            dummy = processor(
                images=Image.new("RGB", (64, 64)), text="test.", return_tensors="pt"
            )
            dummy = {k: v.to("mps") if hasattr(v, "to") else v for k, v in dummy.items()}
            with torch.no_grad():
                model(**dummy)
        except Exception as exc:
            print(f"  [!] MPS failed ({exc}), falling back to CPU")
            model = model.to("cpu")
            device = "cpu"
    print(f"  Running on {device.upper()} (batch_size={batch_size})")

    all_detections: list[dict] = []
    nms_suppressed_total = 0
    frames_processed = 0
    t0 = time.time()

    for batch_start in range(0, len(frames), batch_size):
        batch_frames = frames[batch_start:batch_start + batch_size]
        pil_images = [Image.fromarray(cv2.cvtColor(f[1], cv2.COLOR_BGR2RGB)) for f in batch_frames]

        # Process batch — all images use the same text prompt
        inputs = processor(
            images=pil_images,
            text=[GDINO_TEXT_PROMPT] * len(pil_images),
            return_tensors="pt",
        )
        inputs = {k: v.to(device) if hasattr(v, "to") else v for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)

        # Post-process each image in the batch
        target_sizes = [(f[1].shape[0], f[1].shape[1]) for f in batch_frames]
        results = processor.post_process_grounded_object_detection(
            outputs,
            inputs["input_ids"],
            threshold=GDINO_BOX_THRESHOLD,
            text_threshold=GDINO_TEXT_THRESHOLD,
            target_sizes=target_sizes,
        )

        # Extract detections for each frame in the batch
        for j, (frame_ms, frame_bgr, frame_idx) in enumerate(batch_frames):
            h, w = frame_bgr.shape[:2]
            frame_detections: list[dict] = []

            if results and j < len(results):
                r = results[j]
                labels = r.get("text_labels", r.get("labels", []))
                scores = r.get("scores", torch.tensor([]))
                boxes = r.get("boxes", torch.tensor([]))
                for label, score, box in zip(labels, scores, boxes):
                    conf = float(score.item()) if hasattr(score, "item") else float(score)
                    coords = box.cpu().tolist()
                    # Normalize the label — drop BPE garbage and compound labels
                    raw_label = label.strip()
                    canonical = normalize_gdino_label(raw_label)
                    if canonical is None:
                        continue  # garbage label, skip

                    frame_detections.append({
                        "frame_ms": frame_ms,
                        "label": canonical,
                        "confidence": conf,
                        "x_min": coords[0],
                        "y_min": coords[1],
                        "x_max": coords[2],
                        "y_max": coords[3],
                        "frame_width": w,
                        "frame_height": h,
                    })

            # Apply class-agnostic NMS per frame to remove duplicate/overlapping boxes
            before_nms = len(frame_detections)
            frame_detections = apply_nms(frame_detections, iou_threshold=0.5)
            nms_suppressed_total += before_nms - len(frame_detections)
            all_detections.extend(frame_detections)

            frames_processed += 1
            if frames_processed % 10 == 0 or frames_processed == len(frames):
                elapsed = time.time() - t0
                print(f"    Frame {frames_processed}/{len(frames)} -- {len(all_detections)} detections so far ({elapsed:.1f}s)")
                # Heartbeat every 10 frames
                update_heartbeat(conn, run_id)

    # Unload
    del model, processor
    free_gpu()
    print(f"  GDINO-base done: {len(all_detections)} raw detections ({nms_suppressed_total} suppressed by NMS). Model unloaded.")
    return all_detections


# ---------------------------------------------------------------------------
# CLIP verification pass (copied from run_gdino_clip_pipeline.py)
# ---------------------------------------------------------------------------

def run_clip_verification(
    detections: list[dict],
    frames_by_ms: dict[int, Any],
    conn,
    run_id: str,
) -> tuple[list[dict], dict]:
    """
    Load OpenCLIP, verify ambiguous detections, unload.

    Args:
        detections: list of detection dicts (no frame_bgr — look up via frames_by_ms)
        frames_by_ms: dict mapping frame_ms -> BGR numpy array
        conn: database connection (for heartbeat updates)
        run_id: detection run ID (for heartbeat updates)

    Returns:
        verified_detections: list of dicts ready for DB
        stats: dict with counts for reporting
    """
    try:
        import open_clip
        OPEN_CLIP_AVAILABLE = True
    except ImportError:
        OPEN_CLIP_AVAILABLE = False

    if not OPEN_CLIP_AVAILABLE:
        print("\n  [!] open_clip not installed. Install with:")
        print("      pip install open-clip-torch")
        print("  Skipping CLIP verification -- all GDINO detections will be kept as-is.")
        return list(detections), {
            "kept": len(detections),
            "relabeled": 0,
            "dropped": 0,
            "clip_skipped": True,
        }

    print("\n  --- OpenCLIP verification pass ---")
    print(f"  Loading {CLIP_MODEL_NAME} (pretrained: {CLIP_PRETRAINED})...")

    device = get_device()
    try:
        clip_model, _, preprocess = open_clip.create_model_and_transforms(
            CLIP_MODEL_NAME, pretrained=CLIP_PRETRAINED, device=device,
        )
        tokenizer = open_clip.get_tokenizer(CLIP_MODEL_NAME)
    except Exception as exc:
        if device == "mps":
            print(f"  [!] MPS failed ({exc}), falling back to CPU")
            device = "cpu"
            clip_model, _, preprocess = open_clip.create_model_and_transforms(
                CLIP_MODEL_NAME, pretrained=CLIP_PRETRAINED, device=device,
            )
            tokenizer = open_clip.get_tokenizer(CLIP_MODEL_NAME)
        else:
            raise
    clip_model.eval()
    print(f"  Running on {device.upper()}")

    # Pre-compute text features for all candidate label sets using prompt ensembling.
    # Averaging embeddings from multiple prompt templates yields more robust
    # classification than a single template.
    CLIP_PROMPT_TEMPLATES = [
        "a dashcam photo of a {label}",
        "a photo of a {label}",
        "a {label}",
    ]
    precomputed_text_features: dict[str, Any] = {}
    for ambiguous_label, candidates in AMBIGUOUS_CLASSES.items():
        if isinstance(candidates, list):
            ensembled_feats_list = []
            for c in candidates:
                template_feats = []
                for tmpl in CLIP_PROMPT_TEMPLATES:
                    prompt = tmpl.format(label=c)
                    tokens = tokenizer([prompt]).to(device)
                    with torch.no_grad():
                        feat = clip_model.encode_text(tokens)
                        feat = feat / feat.norm(dim=-1, keepdim=True)
                    template_feats.append(feat)
                # Average the feature vectors from all templates, then re-normalize
                avg_feat = torch.mean(torch.cat(template_feats, dim=0), dim=0, keepdim=True)
                avg_feat = avg_feat / avg_feat.norm(dim=-1, keepdim=True)
                ensembled_feats_list.append(avg_feat)
            precomputed_text_features[ambiguous_label] = torch.cat(ensembled_feats_list, dim=0)

    verified: list[dict] = []
    stats: dict[str, int] = defaultdict(int)
    stats["kept"] = 0
    stats["relabeled"] = 0
    stats["dropped"] = 0

    # Per-class tracking for the summary
    relabel_detail: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    t0 = time.time()
    ambiguous_count = 0
    total = len(detections)

    for i, det in enumerate(detections):
        label = det["label"]
        out = dict(det)

        if label in NON_AMBIGUOUS_CLASSES:
            verified.append(out)
            stats["kept"] += 1
            continue

        if label not in AMBIGUOUS_CLASSES:
            # Unknown label — keep as-is (already normalized by GDINO pass)
            verified.append(out)
            stats["kept"] += 1
            continue

        # This detection needs CLIP verification
        ambiguous_count += 1
        candidates = AMBIGUOUS_CLASSES[label]

        # Look up the frame
        frame_bgr = frames_by_ms.get(det["frame_ms"])
        if frame_bgr is None:
            stats["dropped"] += 1
            continue

        # Crop the bounding box from the frame
        h, w = frame_bgr.shape[:2]
        x1 = max(0, int(det["x_min"]))
        y1 = max(0, int(det["y_min"]))
        x2 = min(w, int(det["x_max"]))
        y2 = min(h, int(det["y_max"]))

        if x2 <= x1 or y2 <= y1:
            stats["dropped"] += 1
            continue

        crop_bgr = frame_bgr[y1:y2, x1:x2]
        crop_pil = Image.fromarray(cv2.cvtColor(crop_bgr, cv2.COLOR_BGR2RGB))
        crop_tensor = preprocess(crop_pil).unsqueeze(0).to(device)

        # Run CLIP (text features are pre-computed)
        with torch.no_grad():
            image_features = clip_model.encode_image(crop_tensor)
            image_features = image_features / image_features.norm(dim=-1, keepdim=True)
            text_features = precomputed_text_features[label]
            similarities = (image_features @ text_features.T).squeeze(0)

        sims = similarities.cpu().tolist()
        best_idx = int(similarities.argmax().item())
        best_sim = sims[best_idx]
        best_candidate = candidates[best_idx]

        # Check minimum similarity first — drop garbage before margin check
        if best_sim < CLIP_MIN_SIMILARITY:
            stats["dropped"] += 1
            continue

        # If margin between top and second candidate is too small, classification is uncertain
        sorted_sims = sorted(sims, reverse=True)
        if len(sorted_sims) > 1 and (sorted_sims[0] - sorted_sims[1]) < 0.03:
            # Uncertain — default to safe label only for person-like classes
            if label in ("construction worker", "cyclist"):
                out["label"] = "person"
                relabel_detail[label]["person (uncertain)"] += 1
            else:
                # For non-person classes (stroller), keep original GDINO label
                relabel_detail[label][label + " (uncertain)"] += 1
            verified.append(out)
            stats["relabeled"] += 1
            continue

        # Map the CLIP winner back to a canonical label
        new_label = CLIP_LABEL_REMAP.get(best_candidate, label)

        if new_label is None:
            # Remap says drop (e.g., shopping cart)
            stats["dropped"] += 1
            continue

        if new_label == label:
            verified.append(out)
            stats["kept"] += 1
        else:
            out["label"] = new_label
            verified.append(out)
            stats["relabeled"] += 1
            relabel_detail[label][new_label] += 1

        if (ambiguous_count % 50 == 0):
            print(f"    CLIP verified {ambiguous_count} ambiguous detections...")
            update_heartbeat(conn, run_id)

    elapsed = time.time() - t0

    # Unload CLIP
    del clip_model, preprocess, tokenizer, precomputed_text_features
    free_gpu()

    print(f"  CLIP verification done in {elapsed:.1f}s")
    print(f"    Ambiguous detections checked: {ambiguous_count}")
    print(f"    Kept (confirmed): {stats['kept']}")
    print(f"    Relabeled: {stats['relabeled']}")
    print(f"    Dropped (low similarity): {stats['dropped']}")

    if relabel_detail:
        print(f"    Relabel breakdown:")
        for orig_label, remap_counts in sorted(relabel_detail.items()):
            for new_label, count in sorted(remap_counts.items()):
                print(f"      {orig_label} -> {new_label}: {count}")

    stats_out = dict(stats)
    stats_out["relabel_detail"] = dict(relabel_detail)  # type: ignore[assignment]
    return verified, stats_out


# ---------------------------------------------------------------------------
# Model dispatch: GDINO + CLIP
# ---------------------------------------------------------------------------

def run_gdino_clip(
    frames: list[tuple[int, Any, int]],
    conn,
    run_id: str,
    config: dict,
) -> list[dict]:
    """Run GDINO-base detection followed by CLIP verification."""

    # Override thresholds from config if provided
    global GDINO_BOX_THRESHOLD, GDINO_TEXT_THRESHOLD, CLIP_MIN_SIMILARITY
    if "boxThreshold" in config:
        GDINO_BOX_THRESHOLD = float(config["boxThreshold"])
    if "textThreshold" in config:
        GDINO_TEXT_THRESHOLD = float(config["textThreshold"])
    if "clipMinSimilarity" in config:
        CLIP_MIN_SIMILARITY = float(config["clipMinSimilarity"])

    # Pass 1: GDINO (with configurable batch size)
    batch_size = int(config.get("batchSize", 8))
    gdino_detections = run_gdino_pass(frames, conn, run_id, batch_size=batch_size)

    # Build frames lookup for CLIP pass
    frames_by_ms = {frame_ms: frame_bgr for frame_ms, frame_bgr, _ in frames}

    # Pass 2: CLIP verification
    clip_enabled = config.get("clipVerification", True)
    if clip_enabled:
        print(f"\n  Running CLIP verification on {len(gdino_detections)} GDINO detections...")
        verified_detections, clip_stats = run_clip_verification(
            gdino_detections, frames_by_ms, conn, run_id
        )
    else:
        print("\n  CLIP verification disabled by config, keeping all GDINO detections")
        verified_detections = gdino_detections
        clip_stats = {"kept": len(gdino_detections), "relabeled": 0, "dropped": 0}

    # Print summary
    before_count = len(gdino_detections)
    after_count = len(verified_detections)
    print(f"\n  GDINO: {before_count} raw -> {after_count} after CLIP "
          f"(kept={clip_stats.get('kept', 0)}, relabeled={clip_stats.get('relabeled', 0)}, "
          f"dropped={clip_stats.get('dropped', 0)})")

    return verified_detections


# ---------------------------------------------------------------------------
# Model dispatch: MM-Grounding-DINO
# ---------------------------------------------------------------------------

# MM-GDINO uses list-of-lists text format (trained on V3Det with 13,204 categories)
MM_GDINO_TEXT_LABELS = [
    "person", "bicycle", "motorcycle", "car", "truck", "bus",
    "electric scooter", "wheelchair", "stroller", "skateboard",
    "dog", "cat", "traffic cone", "construction worker",
]

MM_GDINO_BOX_THRESHOLD = 0.30
MM_GDINO_TEXT_THRESHOLD = 0.25


def run_mm_gdino(
    frames: list[tuple[int, Any, int]],
    conn,
    run_id: str,
    config: dict,
) -> list[dict]:
    """Run MM-Grounding-DINO large (V3Det-trained) detection.

    MM-GDINO is trained on V3Det (13,204 categories) so it has strong
    recognition for common classes without needing CLIP verification.
    Uses list-of-lists text format instead of period-separated strings.
    """
    from transformers import AutoModelForZeroShotObjectDetection, AutoProcessor

    # Override thresholds from config if provided
    box_threshold = float(config.get("boxThreshold", MM_GDINO_BOX_THRESHOLD))
    text_threshold = float(config.get("textThreshold", MM_GDINO_TEXT_THRESHOLD))
    batch_size = int(config.get("batchSize", 1))  # batch=1 is faster (no padding overhead)

    # Use MPS if available (2.5x faster than CPU), fall back to CPU
    forced_device = config.get("device", None)

    # Support base and large variants (tiny has broken bbox output)
    variant = config.get("variant", "base")  # "base" or "large"
    model_ids = {
        "base": "openmmlab-community/mm_grounding_dino_base_all",
        "large": "openmmlab-community/mm_grounding_dino_large_all",
    }
    model_id = model_ids.get(variant, model_ids["base"])
    print(f"\n  --- MM-Grounding-DINO ({variant}) pass ---")
    print(f"  Loading {model_id}...")
    processor = AutoProcessor.from_pretrained(model_id)
    model = AutoModelForZeroShotObjectDetection.from_pretrained(model_id)
    model.eval()

    device = forced_device or get_device()
    if device == "mps":
        try:
            model = model.to("mps")
            # Smoke test
            dummy = processor(
                images=Image.new("RGB", (64, 64)),
                text=[["test"]],
                return_tensors="pt",
            )
            dummy = {k: v.to("mps") if hasattr(v, "to") else v for k, v in dummy.items()}
            with torch.no_grad():
                model(**dummy)
        except Exception as exc:
            print(f"  [!] MPS failed ({exc}), falling back to CPU")
            model = model.to("cpu")
            device = "cpu"
    else:
        model = model.to(device)
    print(f"  Running on {device.upper()} (batch_size={batch_size})")
    print(f"  Config: box_threshold={box_threshold}, text_threshold={text_threshold}")

    # Build the text labels in list-of-lists format (one list per image in batch)
    text_labels_single = [MM_GDINO_TEXT_LABELS]

    all_detections: list[dict] = []
    nms_suppressed_total = 0
    frames_processed = 0
    t0 = time.time()

    for batch_start in range(0, len(frames), batch_size):
        batch_frames = frames[batch_start:batch_start + batch_size]
        t_batch = time.time()

        # Preprocessing: convert BGR to PIL
        t_pre = time.time()
        pil_images = [Image.fromarray(cv2.cvtColor(f[1], cv2.COLOR_BGR2RGB)) for f in batch_frames]
        t_pre_done = time.time()

        # Tokenize
        t_tok = time.time()
        text_input = [MM_GDINO_TEXT_LABELS] * len(pil_images)
        inputs = processor(
            images=pil_images,
            text=text_input,
            return_tensors="pt",
        )
        inputs = {k: v.to(device) if hasattr(v, "to") else v for k, v in inputs.items()}
        t_tok_done = time.time()

        # Inference
        t_inf = time.time()
        with torch.no_grad():
            outputs = model(**inputs)
        t_inf_done = time.time()

        # Post-process
        t_post = time.time()
        target_sizes = [(f[1].shape[0], f[1].shape[1]) for f in batch_frames]
        results = processor.post_process_grounded_object_detection(
            outputs,
            inputs["input_ids"],
            threshold=box_threshold,
            text_threshold=text_threshold,
            target_sizes=target_sizes,
        )
        t_post_done = time.time()

        # Extract detections for each frame in the batch
        for j, (frame_ms, frame_bgr, frame_idx) in enumerate(batch_frames):
            h, w = frame_bgr.shape[:2]
            frame_detections: list[dict] = []

            if results and j < len(results):
                r = results[j]
                labels = r.get("text_labels", r.get("labels", []))
                scores = r.get("scores", torch.tensor([]))
                boxes = r.get("boxes", torch.tensor([]))
                for label, score, box in zip(labels, scores, boxes):
                    conf = float(score.item()) if hasattr(score, "item") else float(score)
                    coords = box.cpu().tolist()
                    cls_name = label.strip().lower()
                    if not cls_name:
                        continue

                    frame_detections.append({
                        "frame_ms": frame_ms,
                        "label": cls_name,
                        "confidence": conf,
                        "x_min": coords[0],
                        "y_min": coords[1],
                        "x_max": coords[2],
                        "y_max": coords[3],
                        "frame_width": w,
                        "frame_height": h,
                    })

            # Apply class-agnostic NMS per frame
            before_nms = len(frame_detections)
            frame_detections = apply_nms(frame_detections, iou_threshold=0.5)
            nms_suppressed_total += before_nms - len(frame_detections)
            all_detections.extend(frame_detections)
            frames_processed += 1

        # Per-batch timing
        t_batch_done = time.time()
        batch_num = batch_start // batch_size + 1
        total_batches = (len(frames) + batch_size - 1) // batch_size
        elapsed = time.time() - t0
        avg_per_frame = elapsed / frames_processed if frames_processed else 0
        eta = avg_per_frame * (len(frames) - frames_processed)
        print(
            f"    Batch {batch_num}/{total_batches} ({len(batch_frames)} frames) -- "
            f"{len(all_detections)} detections -- "
            f"preprocess={((t_pre_done - t_pre) * 1000):.0f}ms "
            f"tokenize={((t_tok_done - t_tok) * 1000):.0f}ms "
            f"inference={((t_inf_done - t_inf) * 1000):.0f}ms "
            f"postprocess={((t_post_done - t_post) * 1000):.0f}ms "
            f"total={((t_batch_done - t_batch) * 1000):.0f}ms "
            f"({elapsed:.0f}s elapsed, ~{eta:.0f}s remaining)"
        )
        update_heartbeat(conn, run_id)

    # Unload
    del model, processor
    free_gpu()
    print(f"  MM-GDINO done: {len(all_detections)} detections ({nms_suppressed_total} suppressed by NMS). Model unloaded.")
    return all_detections


# ---------------------------------------------------------------------------
# Model dispatch: YOLO-World
# ---------------------------------------------------------------------------

def run_yolo_world(
    frames: list[tuple[int, Any, int]],
    conn,
    run_id: str,
    config: dict,
) -> list[dict]:
    """Run YOLO-World open-vocabulary detection."""
    from ultralytics import YOLO

    conf_threshold = float(config.get("confidence", YOLO_WORLD_CONF))

    print("\n  --- YOLO-World pass ---")
    print("  Loading yolov8l-worldv2...")
    model = YOLO("yolov8l-worldv2.pt")
    model.set_classes(VRU_CLASSES)

    device = get_device()
    model.to(device)
    print(f"  Running on {device.upper()}")

    all_detections: list[dict] = []
    t0 = time.time()

    for i, (frame_ms, frame_bgr, frame_idx) in enumerate(frames):
        h, w = frame_bgr.shape[:2]
        results = model.predict(frame_bgr, conf=conf_threshold, verbose=False)[0]
        for box in results.boxes:
            cls_name = results.names[int(box.cls[0].item())]
            conf = float(box.conf[0].item())
            coords = box.xyxy[0].tolist()
            all_detections.append({
                "frame_ms": frame_ms,
                "label": cls_name,
                "confidence": conf,
                "x_min": coords[0],
                "y_min": coords[1],
                "x_max": coords[2],
                "y_max": coords[3],
                "frame_width": w,
                "frame_height": h,
            })

        if (i + 1) % 10 == 0 or i == len(frames) - 1:
            elapsed = time.time() - t0
            print(f"    Frame {i + 1}/{len(frames)} -- {len(all_detections)} detections ({elapsed:.1f}s)")
            update_heartbeat(conn, run_id)

    del model
    free_gpu()

    print(f"  YOLO-World done: {len(all_detections)} detections")
    return all_detections


# ---------------------------------------------------------------------------
# Model dispatch: YOLO11x (standard COCO-80)
# ---------------------------------------------------------------------------

def enhance_night_frame(frame_bgr: Any) -> Any:
    """Apply CLAHE enhancement for low-light dashcam frames."""
    lab = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
    l = clahe.apply(l)
    enhanced = cv2.merge([l, a, b])
    return cv2.cvtColor(enhanced, cv2.COLOR_LAB2BGR)


def is_dark_frame(frame_bgr: Any, threshold: float = 80.0) -> bool:
    """Check if a frame is dark (night/low-light) based on mean brightness."""
    gray = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2GRAY)
    return float(gray.mean()) < threshold


def run_yolo11x(
    frames: list[tuple[int, Any, int]],
    conn,
    run_id: str,
    config: dict,
) -> list[dict]:
    """Run standard YOLO11x with COCO label mapping.

    Quick wins applied:
    - augment=True for test-time augmentation (+2.8% small object AP)
    - CLAHE preprocessing for dark/night frames
    - imgsz=1280 (higher res for better small object detection)
    """
    from ultralytics import YOLO

    conf_threshold = float(config.get("confidence", YOLO11X_CONF))
    use_tta = bool(config.get("augment", False))  # TTA off by default (2-3x slower, may be unreliable on MPS)
    imgsz = int(config.get("imgsz", 1280))

    print("\n  --- YOLO11x (COCO-80) pass ---")
    print(f"  Config: conf={conf_threshold}, augment={use_tta}, imgsz={imgsz}")
    print("  Loading yolo11x.pt...")
    model = YOLO("yolo11x.pt")

    device = get_device()
    model.to(device)
    print(f"  Running on {device.upper()}")

    all_detections: list[dict] = []
    enhanced_count = 0
    t0 = time.time()

    for i, (frame_ms, frame_bgr, frame_idx) in enumerate(frames):
        h, w = frame_bgr.shape[:2]

        # Apply CLAHE per-frame for dark frames (sub-ms check)
        if is_dark_frame(frame_bgr):
            input_frame = enhance_night_frame(frame_bgr)
            enhanced_count += 1
        else:
            input_frame = frame_bgr

        results = model.predict(
            input_frame,
            verbose=False,
            conf=conf_threshold,
            device=device,
            imgsz=imgsz,
            augment=use_tta,
        )[0]

        if results.boxes is not None:
            names = results.names
            for box in results.boxes:
                cls_idx = int(box.cls[0].item())
                cls_name = names.get(cls_idx, str(cls_idx))
                mapped = COCO_LABEL_MAP.get(cls_name)
                if mapped is None:
                    continue
                conf = float(box.conf[0].item())
                coords = box.xyxy[0].tolist()
                all_detections.append({
                    "frame_ms": frame_ms,
                    "label": mapped,
                    "confidence": conf,
                    "x_min": coords[0],
                    "y_min": coords[1],
                    "x_max": coords[2],
                    "y_max": coords[3],
                    "frame_width": w,
                    "frame_height": h,
                })

        if (i + 1) % 10 == 0 or i == len(frames) - 1:
            elapsed = time.time() - t0
            print(f"    Frame {i + 1}/{len(frames)} -- {len(all_detections)} detections ({elapsed:.1f}s)")
            update_heartbeat(conn, run_id)

    del model
    free_gpu()

    if enhanced_count > 0:
        print(f"  CLAHE enhanced {enhanced_count}/{len(frames)} dark frames")
    print(f"  YOLO11x done: {len(all_detections)} detections")
    return all_detections


# ---------------------------------------------------------------------------
# YOLO26x runner (same as YOLO11x but with yolo26x model)
# ---------------------------------------------------------------------------

def run_yolo26x(
    frames: list[tuple[int, Any, int]],
    conn,
    run_id: str,
    config: dict,
) -> list[dict]:
    """Run YOLO26x with COCO label mapping.

    YOLO26 (Jan 2026) — improved small object detection, 43% faster CPU inference,
    NMS-free architecture. Same COCO-80 classes as YOLO11x.
    """
    from ultralytics import YOLO

    conf_threshold = float(config.get("confidence", YOLO11X_CONF))
    use_tta = bool(config.get("augment", False))
    imgsz = int(config.get("imgsz", 1280))

    print("\n  --- YOLO26x (COCO-80) pass ---")
    print(f"  Config: conf={conf_threshold}, augment={use_tta}, imgsz={imgsz}")
    print("  Loading yolo26x.pt...")
    model = YOLO("yolo26x.pt")

    device = get_device()
    model.to(device)
    print(f"  Running on {device.upper()}")

    all_detections: list[dict] = []
    enhanced_count = 0
    t0 = time.time()

    for i, (frame_ms, frame_bgr, frame_idx) in enumerate(frames):
        h, w = frame_bgr.shape[:2]

        if is_dark_frame(frame_bgr):
            input_frame = enhance_night_frame(frame_bgr)
            enhanced_count += 1
        else:
            input_frame = frame_bgr

        results = model.predict(
            input_frame,
            verbose=False,
            conf=conf_threshold,
            device=device,
            imgsz=imgsz,
            augment=use_tta,
        )[0]

        if results.boxes is not None:
            names = results.names
            for box in results.boxes:
                cls_idx = int(box.cls[0].item())
                cls_name = names.get(cls_idx, str(cls_idx))
                mapped = COCO_LABEL_MAP.get(cls_name)
                if mapped is None:
                    continue
                conf = float(box.conf[0].item())
                coords = box.xyxy[0].tolist()
                all_detections.append({
                    "frame_ms": frame_ms,
                    "label": mapped,
                    "confidence": conf,
                    "x_min": coords[0],
                    "y_min": coords[1],
                    "x_max": coords[2],
                    "y_max": coords[3],
                    "frame_width": w,
                    "frame_height": h,
                })

        if (i + 1) % 10 == 0 or i == len(frames) - 1:
            elapsed = time.time() - t0
            print(f"    Frame {i + 1}/{len(frames)} -- {len(all_detections)} detections ({elapsed:.1f}s)")
            update_heartbeat(conn, run_id)

    del model
    free_gpu()

    if enhanced_count > 0:
        print(f"  CLAHE enhanced {enhanced_count}/{len(frames)} dark frames")
    print(f"  YOLO26x done: {len(all_detections)} detections")
    return all_detections


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unified detection runner")
    parser.add_argument("--run-id", required=True, help="Detection run UUID from detection_runs table")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = args.run_id

    print("=" * 70)
    print(f"Detection Runner  --  run_id={run_id}")
    print("=" * 70)

    # Connect to DB
    print("\n[1/6] Connecting to database...")
    conn = get_db()

    # Read run config
    print(f"\n[2/6] Reading detection run {run_id}...")
    run = get_detection_run(conn, run_id)
    if run is None:
        print(f"ERROR: detection_runs row not found for id={run_id}", file=sys.stderr)
        conn.close()
        return 1

    if run["status"] != "queued":
        print(f"ERROR: run status is '{run['status']}', expected 'queued'", file=sys.stderr)
        conn.close()
        return 1

    video_id = run["video_id"]
    model_name = run["model_name"]
    config = json.loads(run["config_json"] or "{}")
    num_frames = int(config.get("framesPerVideo", FRAMES_PER_VIDEO))

    print(f"  video_id:   {video_id}")
    print(f"  model_name: {model_name}")
    print(f"  config:     {json.dumps(config)}")
    print(f"  frames:     {num_frames}")

    # Update status to running
    update_run_status(conn, run_id, "running")

    try:
        # Fetch event from API
        print(f"\n[3/6] Fetching event {video_id} from API...")
        api_key = load_api_key()
        resp = requests.get(
            f"{API_BASE_URL}/{video_id}",
            headers={
                "Content-Type": "application/json",
                "Authorization": api_key,
            },
            timeout=60,
        )
        resp.raise_for_status()
        event = resp.json()
        video_url = event.get("videoUrl")
        if not video_url:
            raise RuntimeError(f"Event {video_id} has no videoUrl")
        day = event.get("timestamp", "")[:10] or "2026-01-01"
        print(f"  type: {event.get('type', '?')}")
        print(f"  day:  {day}")

        # Download video
        print(f"\n[4/6] Downloading video...")
        video_path = download_video(video_url)
        if not video_path:
            raise RuntimeError("Video download failed")
        print(f"  Cached at: {video_path}")

        # Extract frames
        print(f"\n[5/6] Extracting {num_frames} frames...")
        frames = extract_frames(video_path, num_frames)
        print(f"  Extracted {len(frames)} frames")
        if not frames:
            raise RuntimeError("No frames extracted from video")

        # Ensure video pipeline state
        ensure_video_pipeline_state(conn, video_id, day, model_name)

        # Run model
        print(f"\n[6/6] Running model: {model_name}")

        if model_name == "gdino-base-clip":
            detections = run_gdino_clip(frames, conn, run_id, config)
        elif model_name == "mm-gdino":
            detections = run_mm_gdino(frames, conn, run_id, config)
        elif model_name == "yolo-world":
            detections = run_yolo_world(frames, conn, run_id, config)
        elif model_name == "yolo11x":
            detections = run_yolo11x(frames, conn, run_id, config)
        elif model_name == "yolo26x":
            detections = run_yolo26x(frames, conn, run_id, config)
        else:
            raise RuntimeError(f"Unknown model_name: {model_name}")

        # Save all detections using parameterized batch insert
        print(f"\n  Saving {len(detections)} detections to DB...")
        t_save = time.time()

        insert_sql = (
            "INSERT INTO frame_detections (video_id, frame_ms, label, x_min, y_min, x_max, y_max, "
            "confidence, frame_width, frame_height, pipeline_version, model_name, run_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        insert_rows = [
            (
                video_id, d["frame_ms"], d["label"],
                d["x_min"], d["y_min"], d["x_max"], d["y_max"],
                d["confidence"], d["frame_width"], d["frame_height"],
                PIPELINE_VERSION, model_name, run_id,
            )
            for d in detections
        ]

        t_build = time.time()
        if isinstance(conn, TursoDb):
            conn.batch_insert(insert_sql, insert_rows)
        else:
            # sqlite3: use executemany inside a transaction for speed
            conn.execute("BEGIN")
            conn.executemany(insert_sql, insert_rows)
            conn.execute("COMMIT")
        t_insert = time.time()
        total_saved = len(detections)
        print(f"  Saved {total_saved} detections for {video_id}")
        print(f"    Build rows: {(t_build - t_save) * 1000:.0f}ms | Execute: {(t_insert - t_build) * 1000:.0f}ms | Total: {(t_insert - t_save) * 1000:.0f}ms")

        # Per-class breakdown
        label_counts: dict[str, int] = defaultdict(int)
        for det in detections:
            label_counts[det["label"]] += 1
        print(f"\n  Per-class breakdown:")
        for lbl, cnt in sorted(label_counts.items(), key=lambda x: -x[1]):
            print(f"    {lbl:<25} {cnt:>6}")

        # Mark as completed
        update_run_status(conn, run_id, "completed", detection_count=total_saved)
        print(f"\n  Run completed: {total_saved} detections")

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        print(f"\nERROR: {error_msg}", file=sys.stderr)
        traceback.print_exc()
        update_run_status(conn, run_id, "failed", last_error=error_msg[:1000])
        return 1

    finally:
        conn.close()

    print(f"\n{'=' * 70}")
    print("DONE")
    print(f"  View: http://localhost:3001/event/{video_id}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
