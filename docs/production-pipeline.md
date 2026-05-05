# Production Pipeline

## Worker Heartbeats

`scripts/prod-pipeline.py` writes a heartbeat row to
`production_worker_heartbeats` when the EC2 worker starts, while it polls, and
while it processes a video. The production dashboard uses this table for
`Active Servers`, so a healthy idle EC2 worker remains visible even when no
video has completed in the last few minutes.

Video-level progress still lives in `production_runs`: processing rows get
`last_heartbeat_at` updates during long stages such as detection, encode,
upload, and metadata export. Dashboard `processing` counts only include
processing rows with a fresh heartbeat or start time from the last 10 minutes,
so old interrupted rows do not look like live work.

## Queue Claiming

EC2 workers claim explicit production jobs from `production_runs` with one
atomic `UPDATE ... RETURNING` statement before running privacy detection. The
claim moves exactly one `priority = 0` queued row to `processing`, records the
worker `machine_id`, and sets the processing heartbeat. This prevents multiple
EC2 workers that poll at the same time from processing and uploading the same
video.

## Video Quality Contract

Production outputs must preserve the source video timing model. If privacy
redaction is skipped or no privacy boxes are detected, production uploads the
original video bytes as-is.

If privacy redaction is required, production must not pipe OpenCV-decoded frames
into a constant-frame-rate encoder. The redaction path applies ffmpeg filters to
the original input and encodes with source-bitrate-targeted HEVC while preserving
packet timing:

- `libx265`
- target bitrate of at least the source bitrate, with a small default safety
  margin (`PRODUCTION_HEVC_BITRATE_MARGIN`, default `1.03`)
- `-tag:v hvc1`
- `-fps_mode passthrough`
- `-enc_time_base -1`
- copied audio and metadata

Privacy boxes are stabilized before ffmpeg redaction. The worker runs the
privacy detector on source frames, applies class-specific thresholds and compact
class-specific padding for faces and license plates, groups adjacent detections
into short tracks, interpolates brief detector misses, and smooths box
coordinates with a high current-detection weight so masks do not visibly lag
moving targets. ffmpeg then applies stable filled ASS vector masks with real
rounded-rectangle corners on the original source stream, avoiding the visibly
jittery look of many independent `delogo` windows while preserving the source
display timestamp timeline.

Before upload, the worker validates source vs produced video with packet-level
`ffprobe` data. Frame count, duration, normalized packet PTS sequence, frame PTS
sequence, derived timestamp intervals, codec, bitrate floor, and file-size ratio
must match the production contract. Packet duration metadata is recorded in the
comparison report because HEVC packet mux order and duration fields can differ
while preserving the display timestamp timeline. Validation failure fails the
production run instead of uploading a degraded artifact.

If a redacted encode fails only because x265 undershot the source bitrate or
minimum file-size floor, the worker retries the encode with bounded CBR-style
rate control. The retry preserves the same timing checks, uses `-minrate`,
matching `-maxrate`, and `x265-params` `nal-hrd=cbr:filler=1`, and caps the
target below the redacted size ceiling. This covers heavily redacted clips where
flat privacy masks make the video easier to compress while keeping the same
upload quality guard.

## Local Quality Test

Use local test mode to produce Desktop artifacts without touching S3 or marking
the `production_runs` row complete:

```bash
AI_EVENT_VIDEOS_TURSO_HTTP_ONLY=1 .venv/bin/python scripts/prod-pipeline.py \
  --event-id 69e940b85a640aa9e41859cd \
  --local-test-output-dir /Users/as/Desktop/production-quality-test-69e940b85a640aa9e41859cd
```

The output directory contains:

- `source.ffprobe.json`
- `produced.ffprobe.json`
- `produced.mp4`
- `produced.json`
- `comparison.json`
- `comparison.txt`

## Production Metadata

Production metadata includes top-level `video` and `pts_us` fields next to
`event`. The production worker passes the delivered MP4 path into
`scripts/export-metadata.py` so the block describes the uploaded artifact,
including redacted HEVC outputs:

```json
{
  "video": {
    "codec": "hevc",
    "container": "mp4",
    "width": 1280,
    "height": 720,
    "frame_count": 930,
    "bitrate_bps": 5611663,
    "size_bytes": 21779282
  }
}
```

`pts_us` is an array of integer microsecond display timestamps, one value per
video frame, extracted from `best_effort_timestamp_time` and normalized so the
first frame is `0`. The exporter validates that `len(pts_us)` equals
`video.frame_count`, values are strictly increasing, and frame interval spread is
at least `1000us` so CFR-reencoded files are rejected. The production worker runs
the same validation before uploading the produced MP4 to S3.

Long-running workers reload `scripts/export-metadata.py` when that file changes,
but a deployment that changes `scripts/prod-pipeline.py` itself still needs the
worker process restarted.

For one-off exports, use `--video-path /path/to/produced.mp4` with
`--production --event-id ...` when you want the metadata to describe a specific
local MP4. Without `--video-path`, the exporter probes the delivery URL first
and falls back to the local or source event video.

If an old known-good clip has low PTS delta spread and should still be exported
for review, pass `--allow-low-pts-spread` on that one-off export. This flag does
not change the production upload guard.

For a targeted production exception, set
`PRODUCTION_ALLOW_LOW_PTS_SPREAD_EVENT_IDS` to a comma-separated list of exact
event IDs before running `scripts/prod-pipeline.py --event-id <id>`. This bypass
is intentionally per-event and only relaxes the CFR-like PTS spread guard; other
production validation and upload checks still run.
