# Overnight VRU 7.7.6 monitor

This repo includes an overnight monitor script for firmware **7.7.6** VRU processing.

Script: `scripts/overnight-vru-7-7-6-monitor.py`

## What it does

- EC2 fleet hygiene (us-west-2): keeps **2–3** running detector instances whose Name tag contains `vru-detect-fleet`, preferring public IPs `34.220.84.9`, `52.33.236.250`, `52.36.246.33` when available.
- Stops (or optionally terminates) excess instances, newest/largest first.
- Verifies kept servers have `tmux` session `detect` and tries to confirm `scripts/detection-server.py` is running.
- Runs triage for **Period 7**, exact firmware **7.7.6**.
- Queues up to **200** missing eligible detections in `detection_runs` for:
  - `triage_result = signal`
  - `road_class != motorway` (or null)
  - `speed_min is null or < 45`
  - no existing `detection_runs` in `queued/running/completed`
  - model: `gdino-base+clip`, `priority=10`, `framesPerVideo=180`, `frameStride=5`
- Appends a timestamped entry to `logs/overnight-vru-7.7.6-interesting.md`.

## Prereqs

- AWS CLI configured with permission to `DescribeInstances` + `StopInstances`/`TerminateInstances` in `us-west-2`.
- SSH access to the detector instances (expects `ubuntu@<public-ip>`).
- `.env.local` contains `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN`.
- Network access to Turso + Bee Maps API (triage hits Bee Maps).

## Failure modes / guardrails

- If Turso DNS is not resolvable (common in restricted sandboxes), the script fails fast during the Turso steps and records the error in `logs/overnight-vru-7.7.6-interesting.md`.
- The triage subprocess is run with a **180s timeout** to avoid hanging the monitor. A timeout is recorded as triage exit code **124** in the log.

## Run

Dry-run (no mutations):

`python3 scripts/overnight-vru-7-7-6-monitor.py --dry-run`

Full run (stops excess instances when >3 running):

`python3 scripts/overnight-vru-7-7-6-monitor.py --triage 500 --queue-limit 200`

Terminate excess instead of stopping:

`python3 scripts/overnight-vru-7-7-6-monitor.py --terminate-excess`
