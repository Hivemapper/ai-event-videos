# VRU Detection Queue

VRU detection work is stored in `detection_runs`.

Manual runs started from an event detail page use `priority = 0`. The detector
worker checks explicit queued runs before deriving implicit work from
`triage_results`, and it orders queued runs by `priority ASC` before applying
the firmware/FPS quality priority rules.

Automatically discovered detection runs use the default `priority = 100`.
Use `createDetectionRun({ priority })` when a caller needs to place a run in a
specific lane; omit it for normal queue behavior.

The persistent detector (`scripts/detection-server.py`) writes frame detections
and video detection segments with batched inserts. Keep that behavior when
changing the worker save path; one HTTP write per detection can overload the
Turso HTTP fallback when several GPU workers finish at once.

Manual and queue-created GDINO runs default to a maximum of `300` sampled frames
per video with `frameStride = 5`. Clips shorter than the cap are analyzed at
that stride without needing a separate clipped-video path.
