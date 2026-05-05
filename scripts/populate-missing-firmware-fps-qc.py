#!/usr/bin/env python3
"""Populate frame-timing QC for recent rows missing firmware_version_num.

This is intentionally separate from run-triage because Period 7 triage skips
unknown firmware before probing video. Here we force the video timing probe for
that known gap and upsert video_frame_timing_qc rows.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "labels.db"
EVENT_CACHE_DIR = PROJECT_ROOT / "data" / "event-cache"
DEFAULT_START = "2026-04-25T00:00:00.000Z"
API_EVENT_BASE = "https://beemaps.com/api/developer/aievents"
AUTO_SIGNAL_RULE = "auto_signal_fps_qc_ok_missing_firmware"
AUTO_NON_LINEAR_PERFECT_RULE = "auto_non_linear_fps_qc_perfect_missing_firmware"
SIGNAL_BUCKETS = {"ok"}
NON_LINEAR_BUCKETS = {"perfect"}

sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
from frame_timing_qc import FILTER_OUT, probe_video  # noqa: E402


def load_env_file() -> None:
    env_path = PROJECT_ROOT / ".env.local"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        if key in os.environ:
            continue
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        os.environ[key] = value


def load_api_key() -> str:
    load_env_file()
    key = os.environ.get("BEEMAPS_API_KEY")
    if not key:
        raise RuntimeError("BEEMAPS_API_KEY not found")
    return key.removeprefix("Basic ").strip()


class TursoCursor:
    def __init__(self, result_set: Any):
        self._rows = result_set.rows if result_set else []

    def fetchone(self) -> Any | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[Any]:
        return list(self._rows)


class TursoDb:
    def __init__(self, client: Any, libsql_client: Any, url: str, auth_token: str):
        self._client = client
        self._libsql_client = libsql_client
        self._url = url
        self._auth_token = auth_token

    def _reconnect(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass
        self._client = self._libsql_client.create_client_sync(
            url=self._url,
            auth_token=self._auth_token,
        )

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] | None = None) -> TursoCursor:
        wait = 2.0
        for attempt in range(6):
            try:
                if params:
                    statement = self._libsql_client.Statement(sql, list(params))
                    return TursoCursor(self._client.execute(statement))
                return TursoCursor(self._client.execute(sql))
            except Exception as exc:
                if attempt >= 5:
                    raise
                print(f"Turso execute failed; retrying in {wait:.0f}s: {exc}", flush=True)
                self._reconnect()
                time.sleep(wait)
                wait = min(wait * 2, 30)
        raise RuntimeError("Turso execute retry loop exhausted")

    def commit(self) -> None:
        pass

    def close(self) -> None:
        self._client.close()


def get_db_conn() -> Any:
    load_env_file()
    turso_url = os.environ.get("TURSO_DATABASE_URL")
    turso_token = os.environ.get("TURSO_AUTH_TOKEN")
    if turso_url and turso_token:
        import libsql_client

        http_url = turso_url.replace("libsql://", "https://")
        client = libsql_client.create_client_sync(url=http_url, auth_token=turso_token)
        return TursoDb(client, libsql_client, http_url, turso_token)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def row_get(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    try:
        return row[key]
    except (KeyError, TypeError):
        return getattr(row, key, None)


def ensure_frame_timing_table(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS video_frame_timing_qc (
          video_id TEXT PRIMARY KEY,
          firmware_version TEXT,
          bucket TEXT NOT NULL,
          frame_count INTEGER NOT NULL,
          duration_s REAL NOT NULL,
          effective_fps REAL NOT NULL,
          gap_pct REAL NOT NULL,
          single_gaps INTEGER NOT NULL,
          double_gaps INTEGER NOT NULL,
          triple_plus_gaps INTEGER NOT NULL DEFAULT 0,
          max_delta_ms REAL NOT NULL,
          late_frames INTEGER NOT NULL DEFAULT 0,
          max_late_frames_per_2s INTEGER NOT NULL DEFAULT 0,
          late_frame_clusters INTEGER NOT NULL DEFAULT 0,
          non_monotonic_deltas INTEGER NOT NULL DEFAULT 0,
          failed_rules TEXT NOT NULL DEFAULT '[]',
          probe_status TEXT NOT NULL DEFAULT 'ok',
          probe_error TEXT,
          deltas_json TEXT NOT NULL DEFAULT '[]',
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    existing_columns = {
        str(row_get(row, "name"))
        for row in conn.execute("PRAGMA table_info(video_frame_timing_qc)").fetchall()
    }
    for column, definition in (
        ("triple_plus_gaps", "INTEGER NOT NULL DEFAULT 0"),
        ("max_late_frames_per_2s", "INTEGER NOT NULL DEFAULT 0"),
        ("late_frame_clusters", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if column in existing_columns:
            continue
        try:
            conn.execute(f"ALTER TABLE video_frame_timing_qc ADD COLUMN {column} {definition}")
        except Exception as exc:
            if "duplicate column" not in str(exc).lower():
                raise
    conn.commit()


def fetch_candidates(conn: Any, start: str, limit: int | None) -> list[dict[str, Any]]:
    limit_clause = "LIMIT ?" if limit is not None else ""
    params: list[Any] = [start]
    if limit is not None:
        params.append(limit)
    rows = conn.execute(
        f"""
        SELECT t.id, t.event_timestamp, t.triage_result, t.rules_triggered, t.firmware_version
        FROM triage_results t
        LEFT JOIN video_frame_timing_qc q ON q.video_id = t.id
        WHERE t.event_timestamp >= ?
          AND t.firmware_version_num IS NULL
          AND q.video_id IS NULL
        ORDER BY t.event_timestamp ASC, t.id ASC
        {limit_clause}
        """,
        params,
    ).fetchall()
    return [
        {
            "id": str(row_get(row, "id")),
            "event_timestamp": row_get(row, "event_timestamp"),
            "triage_result": row_get(row, "triage_result"),
            "rules_triggered": row_get(row, "rules_triggered"),
            "firmware_version": row_get(row, "firmware_version"),
        }
        for row in rows
    ]


def count_remaining(conn: Any, start: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM triage_results t
        LEFT JOIN video_frame_timing_qc q ON q.video_id = t.id
        WHERE t.event_timestamp >= ?
          AND t.firmware_version_num IS NULL
          AND q.video_id IS NULL
        """,
        (start,),
    ).fetchone()
    return int(row_get(row, "count") or 0)


def read_cached_event(event_id: str) -> dict[str, Any] | None:
    gz_path = EVENT_CACHE_DIR / f"{event_id}.json.gz"
    plain_path = EVENT_CACHE_DIR / f"{event_id}.json"
    if gz_path.exists():
        with gzip.open(gz_path, "rt", encoding="utf-8") as handle:
            return json.load(handle)
    if plain_path.exists():
        return json.loads(plain_path.read_text())
    return None


def write_cached_event(event_id: str, event: dict[str, Any]) -> None:
    EVENT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    gz_path = EVENT_CACHE_DIR / f"{event_id}.json.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as handle:
        json.dump(event, handle)


def api_get_json(url: str, *, headers: dict[str, str], timeout: int = 30) -> dict[str, Any]:
    wait = 10.0
    for attempt in range(5):
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.status_code in {403, 429, 502, 503, 504} and attempt < 4:
            retry_after = response.headers.get("retry-after")
            delay = float(retry_after) if retry_after and retry_after.isdigit() else wait
            time.sleep(max(1.0, delay))
            wait = min(wait * 2, 120)
            continue
        response.raise_for_status()
        return response.json()
    raise RuntimeError("unreachable")


def fetch_event(api_key: str, event_id: str) -> dict[str, Any]:
    cached = read_cached_event(event_id)
    if cached is not None:
        return cached
    event = api_get_json(
        f"{API_EVENT_BASE}/{event_id}",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {api_key}",
        },
    )
    write_cached_event(event_id, event)
    return event


def metadata_firmware(event: dict[str, Any], fallback: Any) -> str | None:
    metadata = event.get("metadata")
    if isinstance(metadata, dict):
        value = metadata.get("FIRMWARE_VERSION")
        if isinstance(value, str) and value.strip():
            return value.strip()
    if isinstance(fallback, str) and fallback.strip():
        return fallback.strip()
    return None


def failed_qc(video_id: str, firmware_version: str | None, reason: str, error: str | None = None) -> dict[str, Any]:
    return {
        "video_id": video_id,
        "source": None,
        "firmware_version": firmware_version,
        "bucket": FILTER_OUT,
        "frame_count": 0,
        "duration_s": 0.0,
        "effective_fps": 0.0,
        "gap_pct": 100.0,
        "single_gaps": 0,
        "double_gaps": 0,
        "triple_plus_gaps": 0,
        "max_delta_ms": 0.0,
        "late_frames": 0,
        "max_late_frames_per_2s": 0,
        "late_frame_clusters": 0,
        "non_monotonic_deltas": 0,
        "deltas_ms": [],
        "failed_rules": [reason],
        "probe_status": "failed",
        "probe_error": error or reason,
    }


def process_candidate(candidate: dict[str, Any], api_key: str, timeout: int) -> dict[str, Any]:
    event_id = candidate["id"]
    try:
        event = fetch_event(api_key, event_id)
    except Exception as exc:
        return {
            "id": event_id,
            "qc": failed_qc(event_id, candidate.get("firmware_version"), "event_fetch_failed", str(exc)),
        }

    firmware_version = metadata_firmware(event, candidate.get("firmware_version"))
    video_url = event.get("videoUrl")
    if not isinstance(video_url, str) or not video_url.strip():
        return {
            "id": event_id,
            "qc": failed_qc(event_id, firmware_version, "missing_video_url"),
        }

    qc = probe_video(
        video_url,
        video_id=event_id,
        firmware_version=firmware_version,
        timeout_s=timeout,
    )
    return {"id": event_id, "qc": qc}


def save_qc(conn: Any, video_id: str, qc: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO video_frame_timing_qc
          (video_id, firmware_version, bucket, frame_count, duration_s,
           effective_fps, gap_pct, single_gaps, double_gaps, triple_plus_gaps,
           max_delta_ms, late_frames, max_late_frames_per_2s, late_frame_clusters,
           non_monotonic_deltas, failed_rules, probe_status, probe_error,
           deltas_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        ON CONFLICT(video_id) DO UPDATE SET
          firmware_version = excluded.firmware_version,
          bucket = excluded.bucket,
          frame_count = excluded.frame_count,
          duration_s = excluded.duration_s,
          effective_fps = excluded.effective_fps,
          gap_pct = excluded.gap_pct,
          single_gaps = excluded.single_gaps,
          double_gaps = excluded.double_gaps,
          triple_plus_gaps = excluded.triple_plus_gaps,
          max_delta_ms = excluded.max_delta_ms,
          late_frames = excluded.late_frames,
          max_late_frames_per_2s = excluded.max_late_frames_per_2s,
          late_frame_clusters = excluded.late_frame_clusters,
          non_monotonic_deltas = excluded.non_monotonic_deltas,
          failed_rules = excluded.failed_rules,
          probe_status = excluded.probe_status,
          probe_error = excluded.probe_error,
          deltas_json = excluded.deltas_json,
          updated_at = datetime('now')
        """,
        (
            video_id,
            qc.get("firmware_version"),
            qc.get("bucket"),
            qc.get("frame_count", 0),
            qc.get("duration_s", 0.0),
            qc.get("effective_fps", 0.0),
            qc.get("gap_pct", 100.0),
            qc.get("single_gaps", 0),
            qc.get("double_gaps", 0),
            qc.get("triple_plus_gaps", 0),
            qc.get("max_delta_ms", 0.0),
            qc.get("late_frames", 0),
            qc.get("max_late_frames_per_2s", 0),
            qc.get("late_frame_clusters", 0),
            qc.get("non_monotonic_deltas", 0),
            json.dumps(qc.get("failed_rules") or []),
            qc.get("probe_status", "ok"),
            qc.get("probe_error"),
            json.dumps(qc.get("deltas_ms") or []),
        ),
    )


def parse_rules(value: Any) -> list[str]:
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return [rule for rule in parsed if isinstance(rule, str)] if isinstance(parsed, list) else []


def set_triage_from_qc_if_clear(
    conn: Any,
    *,
    video_id: str,
    current_triage_result: str | None,
    current_rules: str | None,
    qc: dict[str, Any],
    enabled: bool,
) -> str | None:
    """Update only rows whose sole stored triage blocker is firmware skip."""
    if not enabled:
        return None
    if current_triage_result != "skipped_firmware":
        return None
    if qc.get("probe_status") != "ok":
        return None

    bucket = qc.get("bucket")
    if bucket in SIGNAL_BUCKETS:
        next_result = "signal"
        next_rule = AUTO_SIGNAL_RULE
    elif bucket in NON_LINEAR_BUCKETS:
        next_result = "non_linear"
        next_rule = AUTO_NON_LINEAR_PERFECT_RULE
    else:
        return None

    rules = parse_rules(current_rules)
    rules = [rule for rule in rules if rule != AUTO_SIGNAL_RULE or next_rule == AUTO_SIGNAL_RULE]
    if next_rule not in rules:
        rules.append(next_rule)
    conn.execute(
        """
        UPDATE triage_results
        SET triage_result = ?,
            rules_triggered = ?
        WHERE id = ?
          AND triage_result = 'skipped_firmware'
        """,
        (next_result, json.dumps(rules), video_id),
    )
    return next_result


def summarize(conn: Any, start: str) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN q.bucket = 'perfect' THEN 1 ELSE 0 END) AS perfect,
          SUM(CASE WHEN q.bucket = 'ok' THEN 1 ELSE 0 END) AS ok,
          SUM(CASE WHEN q.bucket = 'filter_out' THEN 1 ELSE 0 END) AS filter_out,
          SUM(CASE WHEN q.probe_status = 'failed' THEN 1 ELSE 0 END) AS failed,
          SUM(CASE WHEN q.video_id IS NULL THEN 1 ELSE 0 END) AS missing_qc
        FROM triage_results t
        LEFT JOIN video_frame_timing_qc q ON q.video_id = t.id
        WHERE t.event_timestamp >= ?
          AND t.firmware_version_num IS NULL
        """,
        (start,),
    ).fetchone()
    return {
        "total": int(row_get(row, "total") or 0),
        "perfect": int(row_get(row, "perfect") or 0),
        "ok": int(row_get(row, "ok") or 0),
        "filter_out": int(row_get(row, "filter_out") or 0),
        "failed": int(row_get(row, "failed") or 0),
        "missing_qc": int(row_get(row, "missing_qc") or 0),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate video_frame_timing_qc rows for recent events missing firmware_version_num."
    )
    parser.add_argument("--start", default=DEFAULT_START, help=f"UTC lower bound; default {DEFAULT_START}")
    parser.add_argument("--limit", type=int, help="Process at most this many currently missing rows")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent event fetch/probe workers")
    parser.add_argument("--timeout", type=int, default=120, help="ffprobe timeout per video in seconds")
    parser.add_argument("--dry-run", action="store_true", help="Print the current target count and exit")
    parser.add_argument(
        "--no-triage-qc-update",
        action="store_true",
        help="Do not update skipped_firmware triage rows from FPS QC buckets",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start_dt = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    start = start_dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    conn = get_db_conn()
    ensure_frame_timing_table(conn)
    remaining = count_remaining(conn, start)
    print(f"Target rows still missing FPS QC since {start}: {remaining}")
    if args.dry_run or remaining == 0:
        print(json.dumps(summarize(conn, start), sort_keys=True))
        conn.close()
        return 0

    api_key = load_api_key()
    candidates = fetch_candidates(conn, start, args.limit)
    total = len(candidates)
    print(f"Processing {total} row(s) with {args.workers} worker(s)")

    update_triage_from_qc = not args.no_triage_qc_update
    counts = {
        "perfect": 0,
        "ok": 0,
        "filter_out": 0,
        "failed": 0,
        "promoted_signal": 0,
        "set_non_linear": 0,
    }
    processed = 0
    started = time.time()

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(process_candidate, candidate, api_key, args.timeout): candidate
            for candidate in candidates
        }
        for future in as_completed(futures):
            candidate = futures[future]
            event_id = candidate["id"]
            try:
                result = future.result()
                qc = result["qc"]
            except Exception as exc:
                qc = failed_qc(event_id, candidate.get("firmware_version"), "worker_failed", str(exc))

            save_qc(conn, event_id, qc)
            triage_update = set_triage_from_qc_if_clear(
                conn,
                video_id=event_id,
                current_triage_result=candidate.get("triage_result"),
                current_rules=candidate.get("rules_triggered"),
                qc=qc,
                enabled=update_triage_from_qc,
            )
            if triage_update == "signal":
                counts["promoted_signal"] += 1
            elif triage_update == "non_linear":
                counts["set_non_linear"] += 1
            conn.commit()
            processed += 1
            bucket = str(qc.get("bucket") or "filter_out")
            if bucket not in counts:
                counts[bucket] = 0
            counts[bucket] += 1
            if qc.get("probe_status") == "failed":
                counts["failed"] += 1

            elapsed = max(time.time() - started, 0.001)
            if processed == 1 or processed % 10 == 0 or processed == total:
                rate = processed / elapsed
                print(
                    f"[{processed}/{total}] {event_id} {bucket} "
                    f"status={qc.get('probe_status')} rate={rate:.2f}/s"
                )

    final = summarize(conn, start)
    print("Run counts:", json.dumps(counts, sort_keys=True))
    print("Current population:", json.dumps(final, sort_keys=True))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
