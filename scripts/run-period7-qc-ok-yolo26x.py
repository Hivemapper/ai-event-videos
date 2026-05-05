#!/usr/bin/env python3
"""Run YOLO26x vehicle-capable detection for Period 7 signal events with QC OK.

This is a narrow batch runner for the triage view:

    /triage/signal?period=7

It targets rows whose frame timing QC bucket is exactly "ok", creates a
model-specific detection_runs row, and invokes scripts/run_detection.py
sequentially so the existing UI and pipeline tables see the results.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_detection.py"
LOG_DIR = PROJECT_ROOT / "data" / "pipeline-logs"

MODEL_NAME = "yolo26x"
PERIOD7_START = "2026-04-22T20:45:00.500Z"
PERIOD7_END = "2099-01-01T00:00:00.000Z"
MIN_PERIOD7_FIRMWARE_NUM = 7_004_003

RUN_CONFIG: dict[str, Any] = {
    "modelDisplayName": "YOLO26x (COCO-80)",
    "type": "Closed-vocabulary (COCO-80 classes only)",
    "device": "MPS (GPU)",
    "classes": [
        "person",
        "bicycle",
        "motorcycle",
        "car",
        "truck",
        "bus",
        "stop sign",
        "cat",
        "dog",
        "skateboard",
    ],
    "features": ["CLAHE night enhancement", "imgsz=1280"],
    "estimatedTime": "Up to 300 frames; every 5th frame on standard clips",
    "framesPerVideo": 180,
    "frameSampling": "every_n_frames",
    "frameStride": 5,
    "source": "period7-signal-qc-ok-vehicle-nearmiss",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_env() -> None:
    env_path = PROJECT_ROOT / ".env.local"
    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def resolve_python() -> str:
    venv_python = PROJECT_ROOT / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable or "python3"


def machine_id() -> str:
    try:
        return subprocess.check_output(
            ["scutil", "--get", "ComputerName"],
            text=True,
            timeout=2,
        ).strip()
    except Exception:
        return socket.gethostname().split(".")[0] or "unknown"


def make_client():
    import libsql_client

    turso_url = os.environ.get("TURSO_DATABASE_URL")
    if not turso_url:
        raise RuntimeError("TURSO_DATABASE_URL is required")
    return libsql_client.create_client_sync(
        url=turso_url.replace("libsql://", "https://"),
        auth_token=os.environ.get("TURSO_AUTH_TOKEN"),
    )


def execute(client, sql: str, args: list[Any] | None = None):
    import libsql_client

    if args:
        return client.execute(libsql_client.Statement(sql, list(args)))
    return client.execute(sql)


def row_get(row: Any, key: str, index: int = 0) -> Any:
    try:
        return row[key]
    except Exception:
        return row[index]


def target_ids(client, *, include_perfect: bool) -> list[str]:
    buckets = ("ok", "perfect") if include_perfect else ("ok",)
    placeholders = ",".join("?" for _ in buckets)
    result = execute(
        client,
        f"""
        SELECT t.id
        FROM triage_results t
        JOIN video_frame_timing_qc q ON q.video_id = t.id
        WHERE t.triage_result = 'signal'
          AND julianday(t.event_timestamp) >= julianday(?)
          AND julianday(t.event_timestamp) < julianday(?)
          AND t.firmware_version_num >= ?
          AND q.bucket IN ({placeholders})
        ORDER BY julianday(t.event_timestamp) DESC
        """,
        [PERIOD7_START, PERIOD7_END, MIN_PERIOD7_FIRMWARE_NUM, *buckets],
    )
    return [str(row_get(row, "id", 0)) for row in result.rows]


def existing_model_run(client, video_id: str) -> tuple[str, str] | None:
    result = execute(
        client,
        """
        SELECT id, status
        FROM detection_runs
        WHERE video_id = ?
          AND model_name = ?
          AND status IN ('queued', 'running', 'completed')
        ORDER BY created_at DESC
        LIMIT 1
        """,
        [video_id, MODEL_NAME],
    )
    if not result.rows:
        return None
    row = result.rows[0]
    return str(row_get(row, "id", 0)), str(row_get(row, "status", 1))


def create_claimed_run(client, video_id: str, machine: str, *, include_perfect: bool) -> str:
    run_id = str(uuid.uuid4())
    now = utc_now()
    config = dict(RUN_CONFIG)
    config["period"] = 7
    config["fpsQcBucket"] = "ok+perfect" if include_perfect else "ok"
    execute(
        client,
        """
        INSERT INTO detection_runs
          (id, video_id, model_name, status, config_json, machine_id,
           created_at, started_at, last_heartbeat_at)
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            video_id,
            MODEL_NAME,
            json.dumps(config, separators=(",", ":")),
            machine,
            now,
            now,
            now,
        ],
    )
    return run_id


def mark_failed(client, run_id: str, message: str) -> None:
    now = utc_now()
    execute(
        client,
        """
        UPDATE detection_runs
        SET status = 'failed',
            completed_at = ?,
            last_heartbeat_at = ?,
            last_error = ?
        WHERE id = ?
        """,
        [now, now, message[:1000], run_id],
    )


def get_status(client, run_id: str) -> tuple[str, int | None, str | None]:
    result = execute(
        client,
        "SELECT status, detection_count, last_error FROM detection_runs WHERE id = ?",
        [run_id],
    )
    if not result.rows:
        return "missing", None, "run row not found"
    row = result.rows[0]
    count = row_get(row, "detection_count", 1)
    return (
        str(row_get(row, "status", 0)),
        int(count) if count is not None else None,
        row_get(row, "last_error", 2),
    )


def run_one(
    client,
    python: str,
    video_id: str,
    index: int,
    total: int,
    machine: str,
    *,
    include_perfect: bool,
) -> bool:
    existing = existing_model_run(client, video_id)
    if existing:
        run_id, status = existing
        print(
            f"[{index}/{total}] skip {video_id} -- existing {MODEL_NAME} run {run_id} ({status})",
            flush=True,
        )
        return True

    run_id = create_claimed_run(client, video_id, machine, include_perfect=include_perfect)
    per_run_log = LOG_DIR / f"detection-{run_id}.log"
    print(f"[{index}/{total}] start {video_id} -- run {run_id}", flush=True)
    print(f"          log {per_run_log}", flush=True)

    with per_run_log.open("w") as log:
        proc = subprocess.Popen(
            [python, str(SCRIPT_PATH), "--run-id", run_id, "--allow-running"],
            cwd=str(PROJECT_ROOT),
            stdout=log,
            stderr=log,
            env={
                **os.environ,
                "PYTHONUNBUFFERED": "1",
                "AI_EVENT_VIDEOS_TURSO_HTTP_ONLY": "1",
            },
        )
        execute(client, "UPDATE detection_runs SET worker_pid = ? WHERE id = ?", [proc.pid, run_id])
        ret = proc.wait()

    status, count, error = get_status(client, run_id)
    if ret == 0 and status == "completed":
        print(f"[{index}/{total}] done {video_id} -- {count or 0} detections", flush=True)
        return True

    if status not in {"completed", "failed"}:
        mark_failed(client, run_id, f"run_detection exited {ret}")
        status, count, error = get_status(client, run_id)
    print(f"[{index}/{total}] failed {video_id} -- {status}: {error or f'exit {ret}'}", flush=True)
    return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Period 7 QC OK YOLO26x detection")
    parser.add_argument("--limit", type=int, default=0, help="Only process first N matching events")
    parser.add_argument("--delay", type=float, default=5.0, help="Seconds between videos")
    parser.add_argument("--dry-run", action="store_true", help="Print targets and exit")
    parser.add_argument("--include-perfect", action="store_true", help="Include QC perfect rows too")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env()
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    client = make_client()
    ids = target_ids(client, include_perfect=args.include_perfect)
    if args.limit > 0:
        ids = ids[: args.limit]

    python = resolve_python()
    machine = machine_id()
    print("Period 7 signal vehicle-near-miss batch")
    print(f"  targets: {len(ids)}")
    print(f"  model:   {MODEL_NAME}")
    print("  frames:  180, every 5th frame")
    print(f"  qc:      {'ok + perfect' if args.include_perfect else 'ok'}")
    print(f"  machine: {machine}")
    print(f"  delay:   {args.delay}s between videos")
    print()

    if args.dry_run:
        for index, video_id in enumerate(ids, start=1):
            existing = existing_model_run(client, video_id)
            suffix = f" existing={existing[0]}:{existing[1]}" if existing else ""
            print(f"{index:03d} {video_id}{suffix}")
        client.close()
        return 0

    started = time.time()
    succeeded = 0
    failed = 0
    try:
        for index, video_id in enumerate(ids, start=1):
            ok = run_one(
                client,
                python,
                video_id,
                index,
                len(ids),
                machine,
                include_perfect=args.include_perfect,
            )
            if ok:
                succeeded += 1
            else:
                failed += 1
            if index < len(ids):
                time.sleep(args.delay)
    finally:
        client.close()

    elapsed = (time.time() - started) / 60
    print(
        f"Finished Period 7 QC OK {MODEL_NAME} batch: "
        f"{succeeded} ok, {failed} failed in {elapsed:.1f}m",
        flush=True,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
