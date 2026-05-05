#!/usr/bin/env python3
"""Recompute stored frame-timing QC buckets from saved delta JSON."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from frame_timing_qc import (  # noqa: E402
    FILTER_OUT,
    classify_metrics,
    late_frame_cluster_metrics,
)


def load_run_triage_module():
    module_path = SCRIPTS_DIR / "run-triage.py"
    spec = importlib.util.spec_from_file_location("run_triage_runtime", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_json_array(value: Any) -> list[Any]:
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def finite_deltas(value: Any) -> list[float]:
    deltas: list[float] = []
    for item in parse_json_array(value):
        try:
            delta = float(item)
        except (TypeError, ValueError):
            continue
        if math.isfinite(delta):
            deltas.append(delta)
    return deltas


def metric_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def metric_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def recompute_row(row: tuple[Any, ...]) -> tuple[str, list[str], int, int]:
    deltas = finite_deltas(row[13])
    max_late_frames_per_2s, late_frame_clusters = late_frame_cluster_metrics(deltas)
    metrics = {
        "frame_count": metric_int(row[2]),
        "duration_s": metric_float(row[3]),
        "effective_fps": metric_float(row[4]),
        "gap_pct": metric_float(row[5]),
        "single_gaps": metric_int(row[6]),
        "double_gaps": metric_int(row[7]),
        "triple_plus_gaps": metric_int(row[8]),
        "max_delta_ms": metric_float(row[9]),
        "late_frames": metric_int(row[10]),
        "max_late_frames_per_2s": max_late_frames_per_2s,
        "late_frame_clusters": late_frame_clusters,
        "non_monotonic_deltas": metric_int(row[11]),
    }
    bucket, failed_rules = classify_metrics(metrics)
    if metrics["non_monotonic_deltas"]:
        bucket = FILTER_OUT
        if "non_monotonic_timestamps" not in failed_rules:
            failed_rules.append("non_monotonic_timestamps")
    return bucket, failed_rules, max_late_frames_per_2s, late_frame_clusters


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing them")
    parser.add_argument(
        "--bucket-changes-only",
        action="store_true",
        help="Only write rows whose bucket changes",
    )
    parser.add_argument("--progress-every", type=int, default=500, help="Print write progress every N updates")
    parser.add_argument("--limit", type=int, help="Limit rows scanned")
    args = parser.parse_args()

    conn = None
    try:
        run_triage = load_run_triage_module()
        conn = run_triage.get_db_conn()
        run_triage.ensure_table(conn)

        where_clause = "probe_status = 'ok'"
        if args.bucket_changes_only:
            where_clause += " AND bucket IN ('ok', 'perfect')"
        sql = f"""
            SELECT video_id, bucket, frame_count, duration_s, effective_fps, gap_pct,
                   single_gaps, double_gaps, triple_plus_gaps, max_delta_ms, late_frames,
                   non_monotonic_deltas, failed_rules, deltas_json, probe_status,
                   max_late_frames_per_2s, late_frame_clusters
            FROM video_frame_timing_qc
            WHERE {where_clause}
        """
        if args.limit is not None:
            sql += f" LIMIT {max(args.limit, 0)}"
        rows = conn.execute(sql).fetchall()

        scanned = 0
        updates = 0
        ok_to_filter_out = 0
        cluster_failures = 0
        for row in rows:
            scanned += 1
            video_id = row[0]
            old_bucket = row[1]
            old_failed_rules = parse_json_array(row[12])
            old_max_late = metric_int(row[15])
            old_clusters = metric_int(row[16])
            bucket, failed_rules, max_late, clusters = recompute_row(row)
            if "late_frame_cluster_gte_5_in_2s" in failed_rules:
                cluster_failures += 1
            changed = (
                bucket != old_bucket
                or failed_rules != old_failed_rules
                or max_late != old_max_late
                or clusters != old_clusters
            )
            if not changed:
                continue
            bucket_changed = bucket != old_bucket
            if args.bucket_changes_only and not bucket_changed:
                continue
            updates += 1
            if old_bucket == "ok" and bucket == "filter_out":
                ok_to_filter_out += 1
            if args.dry_run:
                continue
            conn.execute(
                """UPDATE video_frame_timing_qc
                   SET bucket = ?,
                       failed_rules = ?,
                       max_late_frames_per_2s = ?,
                       late_frame_clusters = ?,
                       updated_at = datetime('now')
                   WHERE video_id = ?""",
                (bucket, json.dumps(failed_rules), max_late, clusters, video_id),
            )
            if args.progress_every > 0 and updates % args.progress_every == 0:
                print(f"Updated {updates} row(s)...", flush=True)

        if not args.dry_run:
            conn.commit()
            if hasattr(conn, "sync"):
                conn.sync()

        print(f"Scanned {scanned} stored QC row(s).")
        print(f"{'Would update' if args.dry_run else 'Updated'} {updates} row(s).")
        print(f"OK -> filter_out: {ok_to_filter_out}")
        print(f"Rows failing cluster rule: {cluster_failures}")
    finally:
        if conn is not None and hasattr(conn, "close"):
            conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
