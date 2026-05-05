#!/usr/bin/env python3
"""Frame-timing QC for nominal 30 FPS MP4 clips.

The module is intentionally dependency-free so it can be imported by
`run-triage.py`, called from Next.js API routes, and used as a standalone CLI.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


MIN_FIRMWARE_VERSION = (7, 4, 3)
PERFECT_MIN_EFFECTIVE_FPS = 29.95
PERFECT_MAX_DELTA_MS = 50.0
OK_MIN_EFFECTIVE_FPS = 29.0
OK_MAX_DOUBLE_GAPS = 1
OK_MAX_WHOLE_DELTA_MS = 100
TRIPLE_PLUS_MIN_DELTA_MS = 130
LATE_FRAME_CLUSTER_WINDOW_S = 2.0
OK_MAX_LATE_FRAMES_PER_CLUSTER_WINDOW = 4
LATE_FRAME_CLUSTER_MIN_COUNT = OK_MAX_LATE_FRAMES_PER_CLUSTER_WINDOW + 1

PERFECT = "perfect"
OK = "ok"
FILTER_OUT = "filter_out"


def parse_firmware_version(value: Any) -> tuple[int, int, int] | None:
    """Parse a semantic firmware version like `7.4.3` or `v7.4.3-beta`."""
    if not isinstance(value, str):
        return None
    match = re.search(r"(\d+)\.(\d+)\.(\d+)", value.strip())
    if not match:
        return None
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def is_firmware_eligible(value: Any, minimum: tuple[int, int, int] = MIN_FIRMWARE_VERSION) -> bool:
    version = parse_firmware_version(value)
    return version is not None and version >= minimum


def _finite_timestamps(timestamps: Iterable[float]) -> list[float]:
    return [float(ts) for ts in timestamps if math.isfinite(float(ts))]


def _whole_delta_ms(value: Any) -> int:
    return int(float(value))


def _gap_counts_from_deltas(deltas_ms: Iterable[Any]) -> tuple[int, int, int]:
    whole_deltas = [_whole_delta_ms(dt) for dt in deltas_ms]
    single_gaps = sum(1 for dt in whole_deltas if 55 < dt <= 90)
    double_gaps = sum(1 for dt in whole_deltas if 90 < dt <= TRIPLE_PLUS_MIN_DELTA_MS)
    triple_plus_gaps = sum(1 for dt in whole_deltas if dt > TRIPLE_PLUS_MIN_DELTA_MS)
    return single_gaps, double_gaps, triple_plus_gaps


def late_frame_cluster_metrics(
    deltas_ms: Iterable[Any],
    *,
    window_s: float = LATE_FRAME_CLUSTER_WINDOW_S,
    min_cluster_size: int = LATE_FRAME_CLUSTER_MIN_COUNT,
) -> tuple[int, int]:
    """Return the densest late-frame window and count of non-overlapping clusters."""
    elapsed_s = 0.0
    late_times_s: list[float] = []
    for raw_delta in deltas_ms:
        try:
            delta_ms = float(raw_delta)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(delta_ms):
            continue
        elapsed_s += max(delta_ms, 0.0) / 1000
        if delta_ms > 50:
            late_times_s.append(elapsed_s)

    start = 0
    max_late_frames = 0
    for end, timestamp_s in enumerate(late_times_s):
        while timestamp_s - late_times_s[start] > window_s:
            start += 1
        max_late_frames = max(max_late_frames, end - start + 1)

    clusters = 0
    index = 0
    while index < len(late_times_s):
        window_end_s = late_times_s[index] + window_s
        next_index = index
        while next_index < len(late_times_s) and late_times_s[next_index] <= window_end_s:
            next_index += 1
        if next_index - index >= min_cluster_size:
            clusters += 1
            index = next_index
        else:
            index += 1

    return max_late_frames, clusters


def classify_metrics(metrics: dict[str, Any]) -> tuple[str, list[str]]:
    """Classify a metric dict into perfect/ok/filter_out."""
    frame_count = int(metrics["frame_count"])
    duration_s = float(metrics["duration_s"])
    effective_fps = float(metrics["effective_fps"])
    double_gaps = int(metrics["double_gaps"])
    triple_plus_gaps = int(metrics.get("triple_plus_gaps", 0))
    max_delta_ms = float(metrics["max_delta_ms"])
    max_late_frames_per_2s = int(metrics.get("max_late_frames_per_2s", 0))
    whole_max_delta_ms = int(max_delta_ms)
    doubles_plus = double_gaps + triple_plus_gaps

    if frame_count < 2 or duration_s <= 0:
        return FILTER_OUT, ["insufficient_timestamps"]

    is_perfect = (
        effective_fps >= PERFECT_MIN_EFFECTIVE_FPS
        and doubles_plus == 0
        and whole_max_delta_ms < PERFECT_MAX_DELTA_MS
    )
    if is_perfect:
        return PERFECT, []

    ok = (
        effective_fps >= OK_MIN_EFFECTIVE_FPS
        and double_gaps <= OK_MAX_DOUBLE_GAPS
        and whole_max_delta_ms <= OK_MAX_WHOLE_DELTA_MS
        and triple_plus_gaps == 0
        and max_late_frames_per_2s <= OK_MAX_LATE_FRAMES_PER_CLUSTER_WINDOW
    )
    if ok:
        return OK, []

    failed_rules: list[str] = []
    if effective_fps < OK_MIN_EFFECTIVE_FPS:
        failed_rules.append("effective_fps_lt_29")
    if double_gaps >= 2:
        failed_rules.append("double_gaps_gte_2")
    if whole_max_delta_ms > OK_MAX_WHOLE_DELTA_MS:
        failed_rules.append("max_delta_ms_gt_100")
    if triple_plus_gaps > 0:
        failed_rules.append("triple_plus_gaps_gt_0")
    if max_late_frames_per_2s > OK_MAX_LATE_FRAMES_PER_CLUSTER_WINDOW:
        failed_rules.append("late_frame_cluster_gte_5_in_2s")
    if not failed_rules:
        failed_rules.append("frame_timing_filter_out")
    return FILTER_OUT, failed_rules


def analyze_timestamps(
    timestamps: Iterable[float],
    *,
    video_id: str | None = None,
    source: str | None = None,
    firmware_version: str | None = None,
) -> dict[str, Any]:
    """Compute frame timing metrics from per-frame timestamps in seconds."""
    ts = _finite_timestamps(timestamps)
    deltas_ms = [
        round((ts[i] - ts[i - 1]) * 1000, 3)
        for i in range(1, len(ts))
    ]
    positive_deltas = [dt for dt in deltas_ms if dt > 0]

    frame_count = len(ts)
    duration_s = ts[-1] - ts[0] if frame_count >= 2 else 0.0
    effective_fps = frame_count / duration_s if duration_s > 0 else 0.0
    late_count = sum(1 for dt in deltas_ms if dt > 50)
    gap_pct = (late_count / max(frame_count - 1, 1)) * 100
    single_gaps, double_gaps, triple_plus_gaps = _gap_counts_from_deltas(deltas_ms)
    max_late_frames_per_2s, late_frame_clusters = late_frame_cluster_metrics(deltas_ms)
    max_delta_ms = max(positive_deltas) if positive_deltas else 0.0
    non_monotonic = sum(1 for dt in deltas_ms if dt <= 0)

    metrics: dict[str, Any] = {
        "video_id": video_id,
        "source": source,
        "firmware_version": firmware_version,
        "frame_count": frame_count,
        "duration_s": round(duration_s, 6),
        "effective_fps": round(effective_fps, 6),
        "gap_pct": round(gap_pct, 6),
        "single_gaps": single_gaps,
        "double_gaps": double_gaps,
        "triple_plus_gaps": triple_plus_gaps,
        "max_delta_ms": round(max_delta_ms, 3),
        "late_frames": late_count,
        "max_late_frames_per_2s": max_late_frames_per_2s,
        "late_frame_clusters": late_frame_clusters,
        "non_monotonic_deltas": non_monotonic,
        "deltas_ms": deltas_ms,
        "probe_status": "ok",
        "probe_error": None,
    }
    bucket, failed_rules = classify_metrics(metrics)
    if non_monotonic:
        bucket = FILTER_OUT
        failed_rules = [*failed_rules, "non_monotonic_timestamps"]
    metrics["bucket"] = bucket
    metrics["failed_rules"] = failed_rules
    return metrics


def _timestamp_from_frame(frame: dict[str, Any]) -> float | None:
    for key in ("best_effort_timestamp_time", "pts_time", "pkt_pts_time"):
        value = frame.get(key)
        if value is None or value == "N/A":
            continue
        try:
            ts = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(ts):
            return ts
    return None


def extract_timestamps_with_ffprobe(source: str, *, timeout_s: int = 120) -> list[float]:
    args = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "frame=best_effort_timestamp_time,pts_time,pkt_pts_time",
        "-of",
        "json",
        source,
    ]
    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        timeout=timeout_s,
        check=False,
    )
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "ffprobe failed").strip()
        raise RuntimeError(err)

    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"ffprobe returned invalid JSON: {exc}") from exc

    frames = payload.get("frames")
    if not isinstance(frames, list):
        raise RuntimeError("ffprobe did not return frame timestamps")

    timestamps = [
        ts
        for frame in frames
        if isinstance(frame, dict)
        for ts in [_timestamp_from_frame(frame)]
        if ts is not None
    ]
    if len(timestamps) < 2:
        raise RuntimeError(f"ffprobe returned {len(timestamps)} usable timestamp(s)")
    return timestamps


def probe_video(
    source: str,
    *,
    video_id: str | None = None,
    firmware_version: str | None = None,
    timeout_s: int = 120,
) -> dict[str, Any]:
    try:
        timestamps = extract_timestamps_with_ffprobe(source, timeout_s=timeout_s)
        return analyze_timestamps(
            timestamps,
            video_id=video_id,
            source=source,
            firmware_version=firmware_version,
        )
    except Exception as exc:
        return {
            "video_id": video_id,
            "source": source,
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
            "failed_rules": ["probe_failed"],
            "probe_status": "failed",
            "probe_error": str(exc),
        }


def _bucket_dir_name(bucket: str) -> str:
    return FILTER_OUT if bucket == FILTER_OUT else bucket


def _prompt_path(label: str) -> Path:
    value = input(f"{label}: ").strip()
    if not value:
        raise SystemExit(f"{label} is required")
    return Path(value).expanduser()


def run_batch(input_dir: Path | None, output_dir: Path | None, *, copy_files: bool = True) -> int:
    src_dir = input_dir or _prompt_path("Input directory")
    out_dir = output_dir or _prompt_path("Output directory")
    if not src_dir.exists() or not src_dir.is_dir():
        raise SystemExit(f"Input directory does not exist: {src_dir}")

    for bucket in (PERFECT, OK, FILTER_OUT):
        (out_dir / bucket).mkdir(parents=True, exist_ok=True)

    mp4s = sorted(p for p in src_dir.rglob("*.mp4") if p.is_file())
    jsonl_path = out_dir / "qc-results.jsonl"
    csv_path = out_dir / "qc-summary.csv"
    counts = {PERFECT: 0, OK: 0, FILTER_OUT: 0}

    with jsonl_path.open("w", encoding="utf-8") as jsonl, csv_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "relative_path",
            "bucket",
            "frame_count",
            "duration_s",
            "effective_fps",
            "gap_pct",
            "single_gaps",
            "double_gaps",
            "triple_plus_gaps",
            "max_delta_ms",
            "failed_rules",
            "max_late_frames_per_2s",
            "late_frame_clusters",
            "probe_status",
            "probe_error",
        ])
        for index, video_path in enumerate(mp4s, start=1):
            rel = video_path.relative_to(src_dir)
            result = probe_video(str(video_path), video_id=video_path.stem)
            bucket = str(result["bucket"])
            counts[bucket] = counts.get(bucket, 0) + 1
            result["relative_path"] = str(rel)
            jsonl.write(json.dumps(result, sort_keys=True) + "\n")

            writer.writerow([
                str(rel),
                bucket,
                result["frame_count"],
                result["duration_s"],
                result["effective_fps"],
                result["gap_pct"],
                result["single_gaps"],
                result["double_gaps"],
                result["triple_plus_gaps"],
                result["max_delta_ms"],
                ";".join(result.get("failed_rules") or []),
                result.get("max_late_frames_per_2s", 0),
                result.get("late_frame_clusters", 0),
                result["probe_status"],
                result.get("probe_error") or "",
            ])

            if copy_files:
                dest = out_dir / _bucket_dir_name(bucket) / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(video_path, dest)

            print(f"[{index}/{len(mp4s)}] {bucket:>10}  {rel}", flush=True)

    print(
        f"Done: {counts.get(PERFECT, 0)} perfect, "
        f"{counts.get(OK, 0)} ok, {counts.get(FILTER_OUT, 0)} filter_out"
    )
    print(f"Report: {jsonl_path}")
    print(f"Summary: {csv_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Frame-timing QC for nominal 30 FPS MP4 clips")
    subparsers = parser.add_subparsers(dest="command", required=True)

    probe = subparsers.add_parser("probe", help="Probe one video path or URL")
    probe.add_argument("source", help="Local MP4 path or remote video URL")
    probe.add_argument("--json", action="store_true", help="Print JSON output")
    probe.add_argument("--video-id", help="Optional video/event ID")
    probe.add_argument("--firmware-version", help="Optional firmware version for audit output")
    probe.add_argument("--timeout", type=int, default=120, help="ffprobe timeout in seconds")

    batch = subparsers.add_parser("batch", help="Classify all .mp4 files in a directory")
    batch.add_argument("--input-dir", type=Path, help="Directory containing .mp4 files")
    batch.add_argument("--output-dir", type=Path, help="Directory to receive bucket folders")
    batch.add_argument("--no-copy", action="store_true", help="Only write reports; do not copy videos")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "probe":
        result = probe_video(
            args.source,
            video_id=args.video_id,
            firmware_version=args.firmware_version,
            timeout_s=args.timeout,
        )
        if args.json:
            print(json.dumps(result, sort_keys=True))
        else:
            print(f"{result['bucket']}: {result['effective_fps']:.2f} fps, max Δt {result['max_delta_ms']:.1f}ms")
        return 0 if result["probe_status"] == "ok" else 2

    if args.command == "batch":
        return run_batch(args.input_dir, args.output_dir, copy_files=not args.no_copy)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
