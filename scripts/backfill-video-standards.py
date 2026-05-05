#!/usr/bin/env python3
"""Backfill triage video stats, firmware metadata, and period-aware video rules."""

import argparse
import gzip
import importlib.util
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUN_TRIAGE_PATH = PROJECT_ROOT / "scripts" / "run-triage.py"
EVENT_CACHE_DIR = PROJECT_ROOT / "data" / "event-cache"


def load_run_triage():
    spec = importlib.util.spec_from_file_location("run_triage", RUN_TRIAGE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {RUN_TRIAGE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rt = load_run_triage()


def load_cached_event(event_id: str) -> dict | None:
    for ext in (".json.gz", ".json"):
        path = EVENT_CACHE_DIR / f"{event_id}{ext}"
        if not path.exists():
            continue
        try:
            if ext == ".json.gz":
                with gzip.open(path, "rt") as f:
                    return json.loads(f.read())
            return json.loads(path.read_text())
        except Exception:
            return None
    return None


def parse_rules(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        return [rule for rule in parsed if isinstance(rule, str)] if isinstance(parsed, list) else []
    except Exception:
        return []


def uniq(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def main():
    parser = argparse.ArgumentParser(description="Backfill triage video stats, firmware, and non-linear video rules")
    parser.add_argument("--limit", type=int, default=0, help="Limit rows processed (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without writing")
    parser.add_argument("--before-ts", help="Only process rows with event_timestamp before this ISO timestamp")
    parser.add_argument("--after-ts", help="Only process rows with event_timestamp at or after this ISO timestamp")
    args = parser.parse_args()

    conn = rt.get_db_conn()
    rt.ensure_table(conn)

    query = """
        SELECT id, triage_result, rules_triggered, video_size, event_timestamp,
               video_length_sec, bitrate_bps, firmware_version, firmware_version_num
        FROM triage_results
        WHERE event_timestamp IS NOT NULL
    """
    params: list[object] = []
    if args.after_ts:
        query += " AND event_timestamp >= ?"
        params.append(args.after_ts)
    if args.before_ts:
        query += " AND event_timestamp < ?"
        params.append(args.before_ts)
    query += " ORDER BY event_timestamp DESC"
    if args.limit:
        query += f" LIMIT {args.limit}"

    rows = conn.execute(query, params).fetchall()
    print(f"Loaded {len(rows)} triage rows")

    updated_stats = 0
    updated_firmware = 0
    reclassified = 0
    missing_cache = 0

    for idx, row in enumerate(rows, start=1):
        event_id = row[0]
        triage_result = row[1]
        rules = parse_rules(row[2])
        video_size = row[3]
        event_timestamp = row[4]
        current_length = row[5]
        current_bitrate = row[6]
        current_firmware_version = row[7]
        current_firmware_num = row[8]

        cached = load_cached_event(event_id)
        if cached is None:
            missing_cache += 1
            continue

        duration_sec, bitrate_bps = rt.compute_video_stats(cached, video_size)
        cached_firmware_version = rt.get_firmware_version(cached)
        new_length = current_length if current_length is not None else duration_sec
        new_bitrate = current_bitrate if current_bitrate is not None else bitrate_bps
        new_firmware_version = current_firmware_version or cached_firmware_version
        new_firmware_num = current_firmware_num
        if new_firmware_num is None and new_firmware_version:
            new_firmware_num = rt.firmware_version_to_num(new_firmware_version)

        next_result = triage_result
        next_rules = list(rules)
        protected = "manual" in rules or triage_result in {"duplicate", "privacy"}

        video_rules = rt.get_non_linear_video_rules(event_timestamp, new_length, new_bitrate)
        if video_rules and not protected:
            next_result = "non_linear"
            next_rules = uniq([*next_rules, *video_rules])

        stats_changed = (
            (current_length is None and new_length is not None) or
            (current_bitrate is None and new_bitrate is not None)
        )
        firmware_changed = (
            (current_firmware_version is None and new_firmware_version is not None) or
            (current_firmware_num is None and new_firmware_num is not None)
        )
        result_changed = next_result != triage_result or next_rules != rules

        if not stats_changed and not firmware_changed and not result_changed:
            continue

        if stats_changed:
            updated_stats += 1
        if firmware_changed:
            updated_firmware += 1
        if result_changed and next_result != triage_result:
            reclassified += 1

        if not args.dry_run:
            conn.execute(
                """UPDATE triage_results
                   SET video_length_sec = COALESCE(video_length_sec, ?),
                       bitrate_bps = COALESCE(bitrate_bps, ?),
                       firmware_version = COALESCE(firmware_version, ?),
                       firmware_version_num = COALESCE(firmware_version_num, ?),
                       triage_result = ?,
                       rules_triggered = ?
                   WHERE id = ?""",
                (
                    new_length,
                    new_bitrate,
                    new_firmware_version,
                    new_firmware_num,
                    next_result,
                    json.dumps(next_rules),
                    event_id,
                ),
            )
            if idx % 250 == 0:
                conn.commit()
                if hasattr(conn, "sync"):
                    conn.sync()

        if idx % 1000 == 0 or idx == len(rows):
            print(
                f"  {idx}/{len(rows)} processed | "
                f"stats updated: {updated_stats} | firmware updated: {updated_firmware} | "
                f"reclassified: {reclassified} | missing cache: {missing_cache}"
            )

    if not args.dry_run:
        conn.commit()
        if hasattr(conn, "sync"):
            conn.sync()
    conn.close()

    print("\nDone")
    print(f"  Stats updated: {updated_stats}")
    print(f"  Firmware updated: {updated_firmware}")
    print(f"  Reclassified to non_linear: {reclassified}")
    print(f"  Missing cache: {missing_cache}")


if __name__ == "__main__":
    main()
