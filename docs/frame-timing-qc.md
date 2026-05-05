# Frame Timing QC

`scripts/frame_timing_qc.py` classifies nominal 30 FPS videos from frame timestamps.
`scripts/run-triage.py` stores the resulting bucket in `video_frame_timing_qc`; if a
firmware-eligible triage run gets `filter_out`, triage marks the event `non_linear`.

## Event Detail Behavior

The event detail page reads cached frame timing QC with
`GET /api/videos/[videoId]/frame-timing-qc`. It does not auto-run ffprobe when a
video is opened. Starting a new probe is an explicit UI action because the probe
spawns the Python/ffprobe analyzer and writes `video_frame_timing_qc`.

## Buckets

- `perfect`: effective FPS is at least `29.95`, there are no double-or-larger gaps,
  and the largest frame delta is under `50ms`.
- `ok`: effective FPS is at least `29.0`, double gaps are at most `1`, triple-plus
  gaps are `0`, max frame delta is `<= 100ms` after ignoring the fractional
  milliseconds, and no 2-second window has more than `4` late frames.
- `filter_out`: the clip fails one or more `ok` requirements, has insufficient
  timestamps, or has non-monotonic timestamps.

## Gap Definitions

- Single gap: frame delta is `> 55ms` and `<= 90ms`.
- Double gap: frame delta is `> 90ms` and `<= 130ms`.
- Triple-plus gap: frame delta is `> 130ms`.
- Late-frame cluster: `5+` frame deltas over `50ms` inside any 2-second window.
  This catches bursts of one-frame drops around `66.7ms` even when max delta and
  double-gap counts look acceptable.

## Stored Bucket Updates

When the bucket rules change, run:

```bash
python3 scripts/rebucket_frame_timing_qc.py
```

Use `--dry-run` first when you want counts without updating `video_frame_timing_qc`.
Use `--bucket-changes-only` when the priority is making UI filters reflect the
new bucket boundary before filling non-critical stored counters on already-filtered
rows.

## Missing-Firmware Backfill

Period 7 triage can intentionally skip rows whose `firmware_version_num` is
missing, which means those rows do not get probed during normal triage. To force
frame-timing QC for recent missing-firmware rows and upsert
`video_frame_timing_qc`, run:

```bash
python3 scripts/populate-missing-firmware-fps-qc.py --start 2026-04-25T00:00:00.000Z
```

The script is resumable: it only selects rows with no existing
`video_frame_timing_qc` row. Use `--limit` for a small batch, `--workers` to tune
concurrency, and `--dry-run` to count the remaining target set without probing.
Turso HTTP writes are retried with reconnect/backoff so transient network
timeouts do not abort a long backfill.

When the probe produces `ok` for a row whose only current triage blocker is
`skipped_firmware`, the script promotes that triage row to `signal` and appends
`auto_signal_fps_qc_ok_missing_firmware` to `rules_triggered`. When the probe
produces `perfect` for such a row, the script marks it `non_linear` and appends
`auto_non_linear_fps_qc_perfect_missing_firmware`. Rows with other triage
outcomes, such as `missing_video`, existing `non_linear`, or `open_road`, are
left unchanged.
