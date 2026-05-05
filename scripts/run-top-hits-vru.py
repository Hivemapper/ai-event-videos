#!/usr/bin/env python3
"""Run every-frame VRU detection for the current Top Hits list."""

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

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_detection.py"
LOG_DIR = PROJECT_ROOT / "data" / "pipeline-logs"
DEFAULT_TOP_HITS_API = "http://localhost:3000/api/top-hits"
MODEL_NAME = "gdino-base-clip"
RUN_SOURCE = "top-hits-every-frame"

RUN_CONFIG: dict[str, Any] = {
    "modelDisplayName": "GDINO Base + CLIP",
    "type": "Open-vocabulary (detect anything described in text)",
    "device": "MPS (GPU)",
    "prompt": (
        "person. bicycle. motorcycle. wheelchair. stroller. "
        "person wearing safety vest. skateboard. dog."
    ),
    "features": ["OpenCLIP verification (filters false positives)"],
    "estimatedTime": "Up to 300 frames; every 5th frame on standard clips",
    "framesPerVideo": 300,
    "frameSampling": "every_n_frames",
    "frameStride": 5,
    "source": RUN_SOURCE,
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
    venv_python = PROJECT_ROOT / ".venv" / "bin" / "python3"
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
    http_url = turso_url.replace("libsql://", "https://")
    return libsql_client.create_client_sync(
        url=http_url,
        auth_token=os.environ.get("TURSO_AUTH_TOKEN"),
    )


def execute(client, sql: str, args: list[Any] | None = None):
    import libsql_client

    if args:
        return client.execute(libsql_client.Statement(sql, args))
    return client.execute(sql)


def row_get(row: Any, key: str, index: int = 0) -> Any:
    try:
        return row[key]
    except Exception:
        return row[index]


def fetch_top_hit_ids(client, api_url: str, timeout: float) -> list[str]:
    try:
        response = requests.get(api_url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        ids = payload.get("ids") or []
        if ids:
            return [str(item) for item in ids if item]
    except Exception as exc:
        print(f"[top-hits] API unavailable ({exc}); falling back to Turso top_hits table", flush=True)

    result = execute(
        client,
        "SELECT event_id FROM top_hits ORDER BY row_id DESC",
    )
    return [str(row_get(row, "event_id")) for row in result.rows if row_get(row, "event_id")]


def is_covered_config(config_json: str | None) -> bool:
    if not config_json:
        return False
    try:
        config = json.loads(config_json)
    except Exception:
        return False
    frames = config.get("framesPerVideo")
    try:
        return int(frames) >= 180
    except Exception:
        return config.get("frameSampling") in {"all", "max", "every_n_frames"}


def existing_covered_run(client, video_id: str) -> tuple[str, str] | None:
    result = execute(
        client,
        """SELECT id, status, config_json FROM detection_runs
           WHERE video_id = ?
             AND status IN ('queued', 'running', 'completed')
           ORDER BY created_at DESC""",
        [video_id],
    )
    for row in result.rows:
        if is_covered_config(row_get(row, "config_json", 2)):
            return str(row_get(row, "id", 0)), str(row_get(row, "status", 1))
    return None


def create_claimed_run(client, video_id: str, machine: str) -> str:
    run_id = str(uuid.uuid4())
    now = utc_now()
    config = dict(RUN_CONFIG)
    config["topHitId"] = video_id
    execute(
        client,
        """INSERT INTO detection_runs
             (id, video_id, model_name, status, config_json, machine_id,
              created_at, started_at, last_heartbeat_at)
           VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?)""",
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


def get_run_status(client, run_id: str) -> tuple[str, int | None, str | None]:
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


def mark_failed(client, run_id: str, message: str) -> None:
    execute(
        client,
        """UPDATE detection_runs
           SET status = 'failed',
               completed_at = ?,
               last_heartbeat_at = ?,
               last_error = ?
           WHERE id = ?""",
        [utc_now(), utc_now(), message[:1000], run_id],
    )


def run_one(client, python: str, video_id: str, index: int, total: int, machine: str) -> bool:
    existing = existing_covered_run(client, video_id)
    if existing:
        run_id, status = existing
        print(f"[{index}/{total}] skip {video_id} -- existing 180-frame run {run_id} ({status})", flush=True)
        return True

    run_id = create_claimed_run(client, video_id, machine)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"detection-{run_id}.log"
    print(f"[{index}/{total}] start {video_id} -- run {run_id}", flush=True)
    print(f"          log {log_path}", flush=True)

    with log_path.open("w") as log:
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
        execute(
            client,
            "UPDATE detection_runs SET worker_pid = ? WHERE id = ?",
            [proc.pid, run_id],
        )
        ret = proc.wait()

    status, count, error = get_run_status(client, run_id)
    if ret == 0 and status == "completed":
        print(f"[{index}/{total}] done {video_id} -- {count or 0} detections", flush=True)
        return True

    if status not in {"failed", "completed"}:
        mark_failed(client, run_id, f"run_detection exited {ret}")
        status, count, error = get_run_status(client, run_id)
    print(f"[{index}/{total}] failed {video_id} -- {status}: {error or f'exit {ret}'}", flush=True)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Run 180-frame VRU detection on Top Hits")
    parser.add_argument("--limit", type=int, default=0, help="Only process the first N Top Hits")
    parser.add_argument("--api-url", default=DEFAULT_TOP_HITS_API, help="Top Hits API URL")
    parser.add_argument("--api-timeout", type=float, default=15, help="Top Hits API timeout seconds")
    parser.add_argument("--dry-run", action="store_true", help="Print the target list without running detection")
    args = parser.parse_args()

    load_env()
    client = make_client()
    ids = fetch_top_hit_ids(client, args.api_url, args.api_timeout)
    if args.limit > 0:
        ids = ids[:args.limit]
    ids = list(dict.fromkeys(ids))

    print(f"Top Hits 180-frame VRU batch")
    print(f"  targets: {len(ids)}")
    print(f"  model:   {MODEL_NAME}")
    print(f"  frames:  up to 180 per video, every 5th frame")
    print(f"  machine: {machine_id()}")
    print()

    if args.dry_run:
        for idx, video_id in enumerate(ids, start=1):
            existing = existing_covered_run(client, video_id)
            suffix = f" existing={existing[0]}:{existing[1]}" if existing else ""
            print(f"{idx:03d} {video_id}{suffix}")
        client.close()
        return 0

    python = resolve_python()
    machine = machine_id()
    started = time.time()
    succeeded = 0
    failed = 0
    try:
        for idx, video_id in enumerate(ids, start=1):
            ok = run_one(client, python, video_id, idx, len(ids), machine)
            if ok:
                succeeded += 1
            else:
                failed += 1
    finally:
        client.close()

    elapsed = time.time() - started
    print()
    print(f"Finished Top Hits VRU batch: {succeeded} ok, {failed} failed in {elapsed / 60:.1f}m")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
