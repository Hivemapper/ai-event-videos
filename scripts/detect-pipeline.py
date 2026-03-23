#!/usr/bin/env python3
"""
Batch detection pipeline runner with CLI dashboard.

Usage:
    python3 scripts/detect-pipeline.py <event_type> <event_num> [--workers N] [--model MODEL]

Examples:
    python3 scripts/detect-pipeline.py HARSH_BRAKING 300
    python3 scripts/detect-pipeline.py HARSH_BRAKING 300 --workers 3
    python3 scripts/detect-pipeline.py SWERVING 50 --model yolo11x
"""

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "labels.db"
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_detection.py"
LOG_DIR = PROJECT_ROOT / "data" / "pipeline-logs"

DEFAULT_MODEL = "gdino-base-clip"
DEFAULT_WORKERS = 2
DEFAULT_CONFIG = {
    "modelDisplayName": "GDINO Base + CLIP",
    "type": "Open-vocabulary (detect anything described in text)",
    "device": "MPS (GPU)",
    "prompt": (
        "person. bicycle. motorcycle. person on electric scooter. "
        "electric kick scooter. wheelchair. stroller. person wearing safety vest. "
        "skateboard. dog. traffic cone. car. truck. bus."
    ),
    "features": ["OpenCLIP verification (filters false positives)"],
}

# ANSI colors
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
CYAN = "\033[36m"
RESET = "\033[0m"
CLEAR_LINE = "\033[2K"
MOVE_UP = "\033[A"
HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"


def load_api_key() -> str:
    """Load Bee Maps API key from env or .env.local."""
    key = os.environ.get("BEEMAPS_API_KEY")
    if key:
        return key
    env_local = PROJECT_ROOT / ".env.local"
    if env_local.exists():
        for line in env_local.read_text().splitlines():
            if line.startswith("BEEMAPS_API_KEY="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("BEEMAPS_API_KEY not found in env or .env.local")


def resolve_python() -> str:
    """Find the Python executable, preferring local venv."""
    venv_python = PROJECT_ROOT / ".venv" / "bin" / "python3"
    if venv_python.exists():
        return str(venv_python)
    if os.environ.get("VIRTUAL_ENV"):
        venv_py = Path(os.environ["VIRTUAL_ENV"]) / "bin" / "python3"
        if venv_py.exists():
            return str(venv_py)
    return "python3"


def fetch_event_ids(api_key: str, event_type: str, limit: int) -> list[str]:
    """Fetch the most recent event IDs of the given type from Bee Maps."""
    auth = f"Basic {api_key}"
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)

    all_ids: list[str] = []
    offset = 0
    page_size = min(limit, 500)

    while len(all_ids) < limit:
        body = {
            "startDate": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "endDate": end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "types": [event_type],
            "limit": page_size,
            "offset": offset,
        }
        resp = requests.post(
            "https://beemaps.com/api/developer/aievents/search",
            headers={"Content-Type": "application/json", "Authorization": auth},
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        events = data.get("events", [])
        if not events:
            break
        all_ids.extend(e["id"] for e in events)
        offset += len(events)
        if len(events) < page_size:
            break

    return all_ids[:limit]


def get_already_processed(conn: sqlite3.Connection) -> set[str]:
    """Get video IDs that already have a completed detection run."""
    rows = conn.execute(
        "SELECT DISTINCT video_id FROM detection_runs WHERE status = 'completed'"
    ).fetchall()
    return {r[0] for r in rows}


def create_run(conn: sqlite3.Connection, video_id: str, model: str, config: dict) -> str:
    """Create a detection run entry directly in SQLite (bypasses single-run constraint)."""
    run_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO detection_runs (id, video_id, model_name, status, config_json, created_at) "
        "VALUES (?, ?, ?, 'queued', ?, datetime('now'))",
        (run_id, video_id, model, json.dumps(config)),
    )
    conn.commit()
    return run_id


class RunState:
    """Track state for a single detection run."""

    def __init__(self, run_id: str, video_id: str, index: int):
        self.run_id = run_id
        self.video_id = video_id
        self.index = index
        self.status = "queued"
        self.started_at: float | None = None
        self.completed_at: float | None = None
        self.detection_count: int | None = None
        self.error: str | None = None
        self.process: subprocess.Popen | None = None
        self.log_path: str = ""


class BatchRunner:
    """Manages parallel detection runs with a CLI dashboard."""

    def __init__(self, video_ids: list[str], model: str, config: dict, max_workers: int):
        self.model = model
        self.config = config
        self.max_workers = max_workers
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

        # Create all runs in DB
        conn = sqlite3.connect(str(DB_PATH))
        self.runs: list[RunState] = []
        for i, vid in enumerate(video_ids):
            run_id = create_run(conn, vid, model, config)
            self.runs.append(RunState(run_id, vid, i))
        conn.close()

        self.total = len(self.runs)
        self.completed = 0
        self.failed = 0
        self.active: list[RunState] = []
        self.queue = list(self.runs)  # runs waiting to start
        self.start_time = time.time()

        # Track recent completions for the log
        self.recent_log: list[str] = []
        self.max_log_lines = 8

    def start_run(self, run: RunState):
        """Spawn the detection worker for a run."""
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_path = LOG_DIR / f"detection-{run.run_id}.log"
        run.log_path = str(log_path)
        run.started_at = time.time()
        run.status = "running"

        python = resolve_python()
        log_fd = open(log_path, "w")
        proc = subprocess.Popen(
            [python, str(SCRIPT_PATH), "--run-id", run.run_id],
            cwd=str(PROJECT_ROOT),
            stdout=log_fd,
            stderr=log_fd,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        run.process = proc

    def check_run(self, run: RunState) -> bool:
        """Check if a run has finished. Returns True if done."""
        if run.process is None:
            return True
        ret = run.process.poll()
        if ret is None:
            return False

        run.completed_at = time.time()
        elapsed = run.completed_at - (run.started_at or run.completed_at)

        # Read final status from DB
        conn = sqlite3.connect(str(DB_PATH))
        row = conn.execute(
            "SELECT status, detection_count, last_error FROM detection_runs WHERE id = ?",
            (run.run_id,),
        ).fetchone()
        conn.close()

        if row:
            run.status = row[0]
            run.detection_count = row[1]
            run.error = row[2]
        else:
            run.status = "failed" if ret != 0 else "completed"

        with self.lock:
            if run.status == "completed":
                self.completed += 1
                det_str = f", {run.detection_count} detections" if run.detection_count else ""
                self.recent_log.append(
                    f"{GREEN}✓{RESET} {run.video_id[:12]}… completed in {elapsed:.0f}s{det_str}"
                )
            else:
                self.failed += 1
                err_short = (run.error or "unknown error")[:60]
                self.recent_log.append(
                    f"{RED}✗{RESET} {run.video_id[:12]}… failed: {err_short}"
                )
            # Trim log
            if len(self.recent_log) > self.max_log_lines:
                self.recent_log = self.recent_log[-self.max_log_lines :]

        return True

    def run(self):
        """Main loop: manage workers and render dashboard."""
        print(HIDE_CURSOR, end="", flush=True)
        dashboard_lines = 0

        try:
            while not self.stop_event.is_set():
                # Start new runs if workers available
                while len(self.active) < self.max_workers and self.queue:
                    run = self.queue.pop(0)
                    self.start_run(run)
                    self.active.append(run)

                # Check active runs
                still_active = []
                for run in self.active:
                    if not self.check_run(run):
                        still_active.append(run)
                self.active = still_active

                # Render dashboard
                dashboard_lines = self.render_dashboard(dashboard_lines)

                # Done?
                if not self.active and not self.queue:
                    break

                time.sleep(1)

        finally:
            print(SHOW_CURSOR, end="", flush=True)

        # Final render
        self.render_dashboard(dashboard_lines, final=True)
        print()

    def render_dashboard(self, prev_lines: int, final: bool = False) -> int:
        """Render the CLI dashboard. Returns number of lines printed."""
        # Move cursor up to overwrite previous dashboard
        if prev_lines > 0:
            sys.stdout.write(f"\033[{prev_lines}A")

        lines: list[str] = []
        elapsed = time.time() - self.start_time
        done = self.completed + self.failed
        remaining = self.total - done

        # Header
        lines.append(f"{CLEAR_LINE}{BOLD}{'═' * 60}{RESET}")
        lines.append(f"{CLEAR_LINE}{BOLD}  Detection Pipeline{RESET}  {DIM}({self.model}){RESET}")
        lines.append(f"{CLEAR_LINE}{BOLD}{'═' * 60}{RESET}")

        # Progress bar
        pct = done / self.total if self.total > 0 else 0
        bar_width = 40
        filled = int(bar_width * pct)
        bar = f"{'█' * filled}{'░' * (bar_width - filled)}"
        lines.append(f"{CLEAR_LINE}  [{bar}] {done}/{self.total} ({pct * 100:.0f}%)")

        # Stats
        elapsed_str = self._fmt_duration(elapsed)
        if done > 0:
            avg = elapsed / done
            eta = avg * remaining
            eta_str = self._fmt_duration(eta)
        else:
            avg = 0
            eta_str = "calculating…"

        lines.append(f"{CLEAR_LINE}")
        lines.append(
            f"{CLEAR_LINE}  {GREEN}✓ {self.completed} completed{RESET}  "
            f"{RED}{'✗ ' + str(self.failed) + ' failed' if self.failed else ''}{RESET}  "
            f"{BLUE}⟳ {len(self.active)} running{RESET}  "
            f"{DIM}◻ {len(self.queue)} queued{RESET}"
        )
        lines.append(
            f"{CLEAR_LINE}  {DIM}Elapsed: {elapsed_str}  "
            f"{'ETA: ' + eta_str if remaining > 0 else 'Done!'}"
            f"{'  Avg: ' + f'{avg:.0f}s/event' if avg > 0 else ''}{RESET}"
        )

        # Active workers
        lines.append(f"{CLEAR_LINE}")
        lines.append(f"{CLEAR_LINE}  {BOLD}Workers:{RESET}")
        for i in range(self.max_workers):
            if i < len(self.active):
                run = self.active[i]
                run_elapsed = time.time() - (run.started_at or time.time())
                lines.append(
                    f"{CLEAR_LINE}    {CYAN}▶{RESET} [{run.index + 1}/{self.total}] "
                    f"{run.video_id[:20]}… {DIM}({run_elapsed:.0f}s){RESET}"
                )
            else:
                lines.append(f"{CLEAR_LINE}    {DIM}  idle{RESET}")

        # Recent activity log
        lines.append(f"{CLEAR_LINE}")
        lines.append(f"{CLEAR_LINE}  {BOLD}Recent:{RESET}")
        if self.recent_log:
            for entry in self.recent_log:
                lines.append(f"{CLEAR_LINE}    {entry}")
        else:
            lines.append(f"{CLEAR_LINE}    {DIM}waiting for first completion…{RESET}")

        # Pad to consistent height
        target_height = 14 + self.max_workers + self.max_log_lines
        while len(lines) < target_height:
            lines.append(CLEAR_LINE)

        output = "\n".join(lines) + "\n"
        sys.stdout.write(output)
        sys.stdout.flush()

        return len(lines)

    @staticmethod
    def _fmt_duration(seconds: float) -> str:
        if seconds < 60:
            return f"{seconds:.0f}s"
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        if h > 0:
            return f"{h}h {m}m"
        return f"{m}m {s}s"


def main():
    parser = argparse.ArgumentParser(
        description="Batch detection pipeline runner",
        usage="python3 scripts/detect-pipeline.py <event_type> <event_num> [--workers N] [--model MODEL]",
    )
    parser.add_argument("event_type", help="Event type, e.g. HARSH_BRAKING")
    parser.add_argument("event_num", type=int, help="Number of unprocessed events to run")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Detection model (default: {DEFAULT_MODEL})")
    args = parser.parse_args()

    event_type = args.event_type.upper().replace("-", "_")
    event_num = args.event_num
    max_workers = args.workers
    model = args.model

    print(f"{BOLD}Detection Pipeline{RESET}")
    print(f"  Type:    {event_type}")
    print(f"  Target:  {event_num} events")
    print(f"  Workers: {max_workers}")
    print(f"  Model:   {model}")
    print()

    # Fetch events
    print(f"Fetching recent {event_type} events…", end="", flush=True)
    api_key = load_api_key()
    # Fetch more than needed since some may already be processed
    candidate_ids = fetch_event_ids(api_key, event_type, limit=event_num * 2)
    print(f" found {len(candidate_ids)}")

    # Filter out already processed
    print("Checking for already-processed events…", end="", flush=True)
    conn = sqlite3.connect(str(DB_PATH))
    processed = get_already_processed(conn)
    conn.close()

    unprocessed = [eid for eid in candidate_ids if eid not in processed]
    video_ids = unprocessed[:event_num]
    print(f" {len(processed)} processed, {len(unprocessed)} remaining")

    if not video_ids:
        print(f"\n{YELLOW}No unprocessed {event_type} events found.{RESET}")
        return

    print(f"\nStarting pipeline for {BOLD}{len(video_ids)}{RESET} events…\n")
    time.sleep(1)

    runner = BatchRunner(video_ids, model, DEFAULT_CONFIG, max_workers)

    # Handle Ctrl+C gracefully
    def handle_sigint(sig, frame):
        print(f"\n\n{YELLOW}Interrupted — killing active workers…{RESET}")
        runner.stop_event.set()
        for run in runner.active:
            if run.process and run.process.poll() is None:
                run.process.terminate()
        # Mark queued runs as cancelled
        conn = sqlite3.connect(str(DB_PATH))
        for run in runner.queue:
            conn.execute(
                "UPDATE detection_runs SET status = 'cancelled', completed_at = datetime('now') WHERE id = ?",
                (run.run_id,),
            )
        conn.commit()
        conn.close()
        print(SHOW_CURSOR, end="", flush=True)
        sys.exit(1)

    signal.signal(signal.SIGINT, handle_sigint)

    runner.run()

    # Summary
    elapsed = time.time() - runner.start_time
    print(f"{BOLD}{'═' * 60}{RESET}")
    print(f"  {GREEN}Completed: {runner.completed}{RESET}  {RED}{'Failed: ' + str(runner.failed) if runner.failed else ''}{RESET}")
    print(f"  Total time: {BatchRunner._fmt_duration(elapsed)}")
    if runner.completed > 0:
        print(f"  Average: {elapsed / runner.completed:.0f}s per event")
    print(f"{BOLD}{'═' * 60}{RESET}")


if __name__ == "__main__":
    main()
