#!/usr/bin/env python3
"""Daily auto-drain VRU pipeline worker."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

try:
    import cv2
    import torch
    from ultralytics import YOLO
except Exception as exc:  # pragma: no cover - runtime dependency guard
    print(f"Missing runtime dependency: {exc}", file=sys.stderr)
    raise


API_BASE_URL = "https://beemaps.com/api/developer/aievents"
CONFIDENCE_THRESHOLD = 0.70
FRAME_CONFIDENCE_THRESHOLD = 0.25
TARGET_ANALYSIS_FPS = 5.0
MERGE_GAP_MS = 1000
MIN_SEGMENT_FRAMES = 2
MAX_CACHE_AGE_SECONDS = 24 * 60 * 60
MAX_CACHE_BYTES = 25 * 1024 * 1024 * 1024

LABEL_MAP: dict[str, tuple[str, str]] = {
    "person": ("pedestrian", "supported"),
    "bicycle": ("bicycle", "supported"),
    "motorcycle": ("motorcycle", "supported"),
    "cat": ("animal", "supported"),
    "dog": ("animal", "supported"),
    "bird": ("animal", "supported"),
    "horse": ("animal", "supported"),
    "sheep": ("animal", "supported"),
    "cow": ("animal", "supported"),
    "elephant": ("animal", "supported"),
    "bear": ("animal", "supported"),
    "zebra": ("animal", "supported"),
    "giraffe": ("animal", "supported"),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def auth_header(value: str) -> str:
    return value if value.startswith("Basic ") else f"Basic {value}"


def day_bounds(day: str) -> tuple[str, str]:
    return (
        f"{day}T00:00:00.000Z",
        f"{day}T23:59:59.999Z",
    )


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def get_device() -> str:
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


@dataclass
class Segment:
    label: str
    support_level: str
    start_ms: int
    end_ms: int
    max_confidence: float
    frame_count: int


class PipelineDb:
    def __init__(self, db_path: str, pipeline_version: str, model_name: str):
        self.pipeline_version = pipeline_version
        self.model_name = model_name
        self.conn = sqlite3.connect(db_path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA foreign_keys = ON")

    def close(self) -> None:
        self.conn.close()

    def get_run(self, run_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM pipeline_runs WHERE id = ?",
            (run_id,),
        ).fetchone()

    def update_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        cursor_offset: int | None = None,
        totals: dict[str, Any] | None = None,
        error: str | None = None,
        completed: bool = False,
    ) -> None:
        run = self.get_run(run_id)
        current_totals = json.loads(run["totals_json"] or "{}") if run else {}
        next_totals = current_totals if totals is None else totals
        fields: list[str] = ["last_heartbeat_at = ?"]
        params: list[Any] = [utc_now()]

        if status is not None:
            fields.append("status = ?")
            params.append(status)
            if status == "running" and not run["started_at"]:
                fields.append("started_at = ?")
                params.append(utc_now())
        if cursor_offset is not None:
            fields.append("cursor_offset = ?")
            params.append(cursor_offset)
        if totals is not None:
            fields.append("totals_json = ?")
            params.append(json.dumps(next_totals))
        if error is not None:
            fields.append("last_error = ?")
            params.append(error)
        if completed:
            fields.append("completed_at = ?")
            params.append(utc_now())

        params.append(run_id)
        self.conn.execute(
            f"UPDATE pipeline_runs SET {', '.join(fields)} WHERE id = ?",
            params,
        )
        self.conn.commit()

    def mark_seen_video(self, run_id: str, video_id: str) -> bool:
        cursor = self.conn.execute(
            """
            INSERT OR IGNORE INTO pipeline_run_seen_videos (run_id, video_id)
            VALUES (?, ?)
            """,
            (run_id, video_id),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def has_seen_video(self, run_id: str, video_id: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1 FROM pipeline_run_seen_videos
            WHERE run_id = ? AND video_id = ?
            """,
            (run_id, video_id),
        ).fetchone()
        return row is not None

    def current_video_state(self, video_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM video_pipeline_state WHERE video_id = ?",
            (video_id,),
        ).fetchone()

    def upsert_video_state(
        self,
        video_id: str,
        day: str,
        status: str,
        *,
        labels_applied: list[str] | None = None,
        error: str | None = None,
        started: bool = False,
        completed: bool = False,
    ) -> None:
        current = self.current_video_state(video_id)
        queued_at = current["queued_at"] if current else None
        started_at = current["started_at"] if current else None
        completed_at = current["completed_at"] if current else None

        if status == "queued" and not queued_at:
            queued_at = utc_now()
        if started and not started_at:
            started_at = utc_now()
        if completed:
            completed_at = utc_now()

        payload = (
            video_id,
            day,
            status,
            self.pipeline_version,
            self.model_name,
            json.dumps(labels_applied or []),
            queued_at,
            started_at,
            completed_at,
            utc_now(),
            error,
        )
        self.conn.execute(
            """
            INSERT INTO video_pipeline_state (
              video_id, day, status, pipeline_version, model_name,
              labels_applied, queued_at, started_at, completed_at,
              last_heartbeat_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
              day = excluded.day,
              status = excluded.status,
              pipeline_version = excluded.pipeline_version,
              model_name = excluded.model_name,
              labels_applied = excluded.labels_applied,
              queued_at = excluded.queued_at,
              started_at = excluded.started_at,
              completed_at = excluded.completed_at,
              last_heartbeat_at = excluded.last_heartbeat_at,
              last_error = excluded.last_error
            """,
            payload,
        )
        self.conn.commit()

    def replace_segments(
        self,
        video_id: str,
        segments: list[Segment],
    ) -> list[str]:
        labels_applied = sorted({segment.label for segment in segments})
        self.conn.execute(
            "DELETE FROM video_detection_segments WHERE video_id = ?",
            (video_id,),
        )
        for segment in segments:
            self.conn.execute(
                """
                INSERT INTO video_detection_segments (
                  video_id, label, start_ms, end_ms, max_confidence,
                  support_level, pipeline_version, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'yolo')
                """,
                (
                    video_id,
                    segment.label,
                    segment.start_ms,
                    segment.end_ms,
                    segment.max_confidence,
                    segment.support_level,
                    self.pipeline_version,
                ),
            )
        self.conn.commit()
        return labels_applied


class PipelineWorker:
    def __init__(
        self,
        *,
        run_id: str,
        day: str,
        batch_size: int,
        db_path: str,
        pipeline_version: str,
        model_name: str,
    ):
        bee_maps_key = os.environ.get("BEEMAPS_API_KEY")
        if not bee_maps_key:
            raise RuntimeError("BEEMAPS_API_KEY is required")

        self.run_id = run_id
        self.day = day
        self.batch_size = batch_size
        self.db = PipelineDb(db_path, pipeline_version, model_name)
        self.pipeline_version = pipeline_version
        self.model_name = model_name
        self.device = get_device()
        self.model = YOLO(model_name)
        self.model.to(self.device)
        self.auth_header = auth_header(bee_maps_key)
        self.cache_dir = Path.cwd() / "data" / "pipeline-video-cache"
        ensure_dir(self.cache_dir)
        self.totals = {
            "totalDiscovered": 0,
            "totalProcessed": 0,
            "totalFailed": 0,
            "totalStale": 0,
            "totalSkipped": 0,
            "currentVideoId": None,
            "currentVideoIndex": 0,
            "remaining": 0,
            "lastPageSize": 0,
            "reconciliationPasses": 0,
            "throughputPerHour": 0,
        }

    def refresh_run(self) -> sqlite3.Row:
        run = self.db.get_run(self.run_id)
        if run is None:
            raise RuntimeError(f"Run {self.run_id} not found")
        return run

    def wait_until_runnable(self) -> str:
        while True:
            run = self.refresh_run()
            status = run["status"]
            if status in ("queued", "running"):
                if status != "running":
                    self.db.update_run(self.run_id, status="running", totals=self.totals)
                else:
                    self.db.update_run(self.run_id, totals=self.totals)
                return "running"
            if status == "paused":
                self.db.update_run(self.run_id, totals=self.totals)
                time.sleep(2)
                continue
            return status

    def compute_throughput(self, started_at: str | None) -> float:
        if not started_at or self.totals["totalProcessed"] == 0:
            return 0
        elapsed_hours = max(
            1 / 3600,
            (datetime.now(timezone.utc) - datetime.fromisoformat(started_at.replace("Z", "+00:00"))).total_seconds() / 3600,
        )
        return round(self.totals["totalProcessed"] / elapsed_hours, 2)

    def fetch_page(self, offset: int) -> tuple[list[dict[str, Any]], int]:
        start_date, end_date = day_bounds(self.day)
        response = requests.post(
            f"{API_BASE_URL}/search",
            json={
                "startDate": start_date,
                "endDate": end_date,
                "limit": self.batch_size,
                "offset": offset,
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": self.auth_header,
            },
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        events = payload.get("events", [])
        total = payload.get("pagination", {}).get("total", len(events))
        return events, total

    def download_video(self, video_url: str) -> Path:
        hashed = hashlib.md5(video_url.encode("utf-8")).hexdigest()
        path = self.cache_dir / f"{hashed}.mp4"
        if path.exists():
            path.touch()
            return path

        with requests.get(video_url, stream=True, timeout=120) as response:
            response.raise_for_status()
            with path.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)

        self.cleanup_cache()
        return path

    def cleanup_cache(self) -> None:
        now = time.time()
        files = [entry for entry in self.cache_dir.glob("*.mp4") if entry.is_file()]
        total_size = sum(entry.stat().st_size for entry in files)

        for entry in files:
            age_seconds = now - entry.stat().st_mtime
            if age_seconds > MAX_CACHE_AGE_SECONDS:
                total_size -= entry.stat().st_size
                entry.unlink(missing_ok=True)

        if total_size <= MAX_CACHE_BYTES:
            return

        remaining_files = sorted(
            [entry for entry in self.cache_dir.glob("*.mp4") if entry.is_file()],
            key=lambda candidate: candidate.stat().st_mtime,
        )
        for entry in remaining_files:
            if total_size <= MAX_CACHE_BYTES:
                break
            total_size -= entry.stat().st_size
            entry.unlink(missing_ok=True)

    def process_video(self, event: dict[str, Any]) -> list[Segment]:
        video_path = self.download_video(event["videoUrl"])
        capture = cv2.VideoCapture(str(video_path))
        if not capture.isOpened():
            raise RuntimeError("Failed to open downloaded video")

        fps = capture.get(cv2.CAP_PROP_FPS) or 0
        if fps <= 0:
            fps = 30.0
        frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        sample_stride = max(1, int(round(fps / TARGET_ANALYSIS_FPS)))
        hits: dict[str, list[tuple[int, float, str]]] = {}

        frame_index = 0
        sampled_frames = 0
        while True:
            ok, frame = capture.read()
            if not ok:
                break
            if frame_index % sample_stride != 0:
                frame_index += 1
                continue

            timestamp_ms = int((frame_index / fps) * 1000)
            results = self.model.predict(
                frame,
                verbose=False,
                conf=FRAME_CONFIDENCE_THRESHOLD,
                device=self.device,
                imgsz=960,
            )
            best_labels: dict[str, tuple[float, str]] = {}
            if results and results[0].boxes is not None:
                result = results[0]
                names = result.names
                for box in result.boxes:
                    class_index = int(box.cls[0].item())
                    class_name = names.get(class_index, str(class_index))
                    mapped = LABEL_MAP.get(class_name)
                    if mapped is None:
                        continue
                    label, support_level = mapped
                    confidence = float(box.conf[0].item())
                    current = best_labels.get(label)
                    if current is None or confidence > current[0]:
                        best_labels[label] = (confidence, support_level)

            for label, (confidence, support_level) in best_labels.items():
                hits.setdefault(label, []).append((timestamp_ms, confidence, support_level))

            sampled_frames += 1
            if sampled_frames % 25 == 0:
                self.db.update_run(self.run_id, totals=self.totals)
                self.db.upsert_video_state(
                    event["id"],
                    self.day,
                    "running",
                    started=True,
                )
            frame_index += 1

        capture.release()
        if frame_count == 0:
            raise RuntimeError("Video contained no frames")

        return self.merge_hits(hits)

    def merge_hits(self, hits: dict[str, list[tuple[int, float, str]]]) -> list[Segment]:
        segments: list[Segment] = []
        for label, values in hits.items():
            values.sort(key=lambda item: item[0])
            active: Segment | None = None
            last_timestamp = None
            for timestamp_ms, confidence, support_level in values:
                if active is None:
                    active = Segment(
                        label=label,
                        support_level=support_level,
                        start_ms=timestamp_ms,
                        end_ms=timestamp_ms,
                        max_confidence=confidence,
                        frame_count=1,
                    )
                    last_timestamp = timestamp_ms
                    continue

                if last_timestamp is not None and timestamp_ms - last_timestamp <= MERGE_GAP_MS:
                    active.end_ms = timestamp_ms
                    active.frame_count += 1
                    active.max_confidence = max(active.max_confidence, confidence)
                else:
                    if (
                        active.frame_count >= MIN_SEGMENT_FRAMES
                        and active.max_confidence >= CONFIDENCE_THRESHOLD
                    ):
                        segments.append(active)
                    active = Segment(
                        label=label,
                        support_level=support_level,
                        start_ms=timestamp_ms,
                        end_ms=timestamp_ms,
                        max_confidence=confidence,
                        frame_count=1,
                    )
                last_timestamp = timestamp_ms

            if (
                active is not None
                and active.frame_count >= MIN_SEGMENT_FRAMES
                and active.max_confidence >= CONFIDENCE_THRESHOLD
            ):
                segments.append(active)

        segments.sort(key=lambda segment: segment.start_ms)
        return segments

    def video_is_currently_processed(self, video_id: str) -> bool:
        state = self.db.current_video_state(video_id)
        return bool(
            state
            and state["status"] == "processed"
            and state["pipeline_version"] == self.pipeline_version
        )

    def process_event(self, event: dict[str, Any]) -> None:
        video_id = event["id"]
        if self.video_is_currently_processed(video_id):
            self.totals["totalSkipped"] += 1
            return

        self.db.upsert_video_state(video_id, self.day, "queued")
        self.db.upsert_video_state(video_id, self.day, "running", started=True)
        self.totals["currentVideoId"] = video_id
        self.totals["currentVideoIndex"] += 1
        self.db.update_run(self.run_id, totals=self.totals)

        try:
            segments = self.process_video(event)
            labels_applied = self.db.replace_segments(video_id, segments)
            self.db.upsert_video_state(
                video_id,
                self.day,
                "processed",
                labels_applied=labels_applied,
                completed=True,
            )
            self.totals["totalProcessed"] += 1
        except Exception as exc:
            self.db.upsert_video_state(
                video_id,
                self.day,
                "failed",
                labels_applied=[],
                error=str(exc),
                completed=True,
            )
            self.totals["totalFailed"] += 1

        run = self.refresh_run()
        self.totals["throughputPerHour"] = self.compute_throughput(run["started_at"])
        self.db.update_run(self.run_id, totals=self.totals)

    def process_pass(self, *, reconciliation: bool) -> str:
        offset = 0 if reconciliation else int(self.refresh_run()["cursor_offset"])
        if reconciliation:
            self.totals["reconciliationPasses"] = 1
        while True:
            status = self.wait_until_runnable()
            if status not in ("running", "queued"):
                return status

            events, total = self.fetch_page(offset)
            self.totals["totalDiscovered"] = max(self.totals["totalDiscovered"], total)
            self.totals["lastPageSize"] = len(events)
            self.totals["remaining"] = max(
                0,
                total
                - self.totals["totalProcessed"]
                - self.totals["totalSkipped"]
                - self.totals["totalFailed"],
            )
            self.db.update_run(self.run_id, totals=self.totals, cursor_offset=offset)

            if not events:
                return "exhausted"

            processed_in_page = 0
            for event in events:
                status = self.wait_until_runnable()
                if status not in ("running", "queued"):
                    return status

                video_id = event["id"]
                is_new_for_run = self.db.mark_seen_video(self.run_id, video_id)
                if not is_new_for_run:
                    continue

                processed_in_page += 1
                self.process_event(event)

            offset += len(events)
            if not reconciliation:
                self.db.update_run(self.run_id, totals=self.totals, cursor_offset=offset)

            if processed_in_page == 0 and len(events) < self.batch_size:
                return "exhausted"

    def run(self) -> int:
        try:
            primary_status = self.process_pass(reconciliation=False)
            if primary_status not in ("exhausted",):
                self.db.update_run(self.run_id, status=primary_status, totals=self.totals)
                return 0 if primary_status == "cancelled" else 1

            reconciliation_status = self.process_pass(reconciliation=True)
            if reconciliation_status not in ("exhausted",):
                self.db.update_run(self.run_id, status=reconciliation_status, totals=self.totals)
                return 0 if reconciliation_status == "cancelled" else 1

            self.totals["currentVideoId"] = None
            self.totals["remaining"] = 0
            run = self.refresh_run()
            self.totals["throughputPerHour"] = self.compute_throughput(run["started_at"])
            self.db.update_run(
                self.run_id,
                status="completed",
                totals=self.totals,
                completed=True,
                error=None,
            )
            return 0
        except Exception as exc:
            self.db.update_run(
                self.run_id,
                status="failed",
                totals=self.totals,
                error=str(exc),
                completed=True,
            )
            print(f"Pipeline worker failed: {exc}", file=sys.stderr)
            return 1
        finally:
            self.db.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VRU daily pipeline worker")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--day", required=True)
    parser.add_argument("--batch-size", type=int, required=True)
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--pipeline-version", required=True)
    parser.add_argument("--model-name", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    worker = PipelineWorker(
        run_id=args.run_id,
        day=args.day,
        batch_size=args.batch_size,
        db_path=args.db_path,
        pipeline_version=args.pipeline_version,
        model_name=args.model_name,
    )
    return worker.run()


if __name__ == "__main__":
    raise SystemExit(main())
