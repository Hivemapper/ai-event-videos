#!/usr/bin/env python3
"""Audit Period 7 rows that were classified as missing video.

This repairs false positives caused by Bee Maps search results that omit
`videoUrl` even though the event-detail endpoint has a playable video.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
RUN_TRIAGE_PATH = SCRIPTS_DIR / "run-triage.py"


def import_run_triage():
    sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location("run_triage", RUN_TRIAGE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {RUN_TRIAGE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def period_bounds(rt, period: int) -> tuple[str, str]:
    start_iso, end_iso, _ = rt.PERIODS[period]
    end_dt = min(rt.parse_iso_timestamp(end_iso), datetime.now(timezone.utc))
    return start_iso, rt.format_api_timestamp(end_dt)


def load_candidate_rows(conn, rt, period: int, limit: int | None):
    start_iso, end_iso = period_bounds(rt, period)
    sql = """
        SELECT id, event_type, rules_triggered, event_timestamp
        FROM triage_results
        WHERE triage_result = 'missing_video'
          AND rules_triggered NOT LIKE '%manual%'
          AND rules_triggered NOT LIKE '%detail_confirmed_missing_video%'
          AND event_timestamp IS NOT NULL
          AND julianday(event_timestamp) >= julianday(?)
          AND julianday(event_timestamp) < julianday(?)
        ORDER BY event_timestamp DESC
    """
    params: list[object] = [start_iso, end_iso]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, params).fetchall()


def audit_row(rt, api_key: str, mapbox_token: str | None, period: int, row) -> dict:
    event_id = row[0]
    event, err = rt.fetch_event_detail_outcome(api_key, event_id)
    if event is None:
        return {
            "event_id": event_id,
            "status": "error",
            "message": f"fetch_error {err}",
        }

    outcome = rt.analyze_triage_candidate(event, api_key, mapbox_token, period)
    row_params = outcome.get("row_params")
    if not row_params:
        return {
            "event_id": event_id,
            "status": "error",
            "message": f"no_update {outcome.get('summary', '')}",
        }

    next_result = row_params[2]
    if next_result == "missing_video":
        return {
            "event_id": event_id,
            "status": "still_missing",
            "message": "still_missing",
            "outcome": outcome,
            "row_params": row_params,
        }

    return {
        "event_id": event_id,
        "status": "fixed",
        "message": f"missing_video -> {next_result}",
        "outcome": outcome,
        "row_params": row_params,
    }


def reconnect_db(rt):
    conn = rt.get_db_conn()
    rt.ensure_table(conn)
    return conn


def save_result_with_retry(rt, conn, result: dict, *, save_qc: bool):
    row_params = result["row_params"]
    outcome = result.get("outcome") or {}
    delay = 2.0
    last_error: Exception | None = None

    for attempt in range(1, 5):
        try:
            if save_qc:
                qc = outcome.get("qc")
                if qc:
                    rt.save_frame_timing_qc(conn, row_params[0], qc)
            rt.save_triage_row(conn, row_params)
            return conn
        except Exception as exc:
            last_error = exc
            if attempt == 4:
                break
            print(
                f"    write failed on attempt {attempt}; reconnecting in {delay:.0f}s ({exc})",
                flush=True,
            )
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(delay)
            delay = min(delay * 2, 30)
            conn = reconnect_db(rt)

    raise last_error if last_error else RuntimeError("write failed")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recheck Period 7 missing_video triage rows against event-detail video URLs."
    )
    parser.add_argument("--period", type=int, default=7, choices=[7])
    parser.add_argument("--limit", type=int, help="Maximum rows to audit")
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, min(8, int(os.environ.get("TRIAGE_PROCESS_WORKERS", "1")))),
        help="Concurrent audit workers (default: TRIAGE_PROCESS_WORKERS or 1)",
    )
    parser.add_argument("--fix", action="store_true", help="Write repaired triage rows")
    args = parser.parse_args()

    rt = import_run_triage()
    api_key = rt.load_api_key()
    mapbox_token = rt.load_mapbox_token()
    conn = rt.get_db_conn()
    rt.ensure_table(conn)

    rows = load_candidate_rows(conn, rt, args.period, args.limit)
    print(f"Auditing {len(rows)} Period {args.period} missing_video row(s) with {args.workers} worker(s)")
    if not args.fix:
        print("Dry run only. Re-run with --fix to update false positives.")

    checked = 0
    fixed = 0
    still_missing = 0
    would_update = 0
    errors = 0

    def handle_result(result: dict) -> None:
        nonlocal checked, fixed, still_missing, would_update, errors, conn
        checked += 1
        event_id = result["event_id"]
        status = result["status"]
        print(f"  [{checked}/{len(rows)}] {event_id} {result['message']}", flush=True)

        if status == "error":
            errors += 1
            return
        if status == "still_missing":
            still_missing += 1
            if args.fix:
                conn = save_result_with_retry(rt, conn, result, save_qc=False)
                if still_missing % 25 == 0:
                    conn.commit()
            return

        fixed += 1
        if args.fix:
            conn = save_result_with_retry(rt, conn, result, save_qc=True)
            if fixed % 10 == 0:
                conn.commit()
        else:
            would_update += 1

    if args.workers == 1:
        for row in rows:
            handle_result(audit_row(rt, api_key, mapbox_token, args.period, row))
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(audit_row, rt, api_key, mapbox_token, args.period, row)
                for row in rows
            ]
            for future in as_completed(futures):
                handle_result(future.result())

    if args.fix:
        conn.commit()
    conn.close()

    print()
    print(f"Checked:       {checked}")
    print(f"False positive:{fixed:>8}")
    print(f"Still missing: {still_missing:>8}")
    print(f"Errors:        {errors:>8}")
    if would_update:
        print(f"Would update:  {would_update:>8}")


if __name__ == "__main__":
    main()
