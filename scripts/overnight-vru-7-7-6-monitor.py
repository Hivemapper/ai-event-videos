#!/usr/bin/env python3
"""
Overnight VRU 7.7.6 monitor + queue helper.

What it does (networked host required):
1) Audits EC2 VRU detector fleet (us-west-2) for instances tagged/name-matching
   vru-detect-fleet, keeping 2-3 active and preferring these public IPs when
   available: 34.220.84.9, 52.33.236.250, 52.36.246.33.
2) Stops or terminates excess instances (newest/largest first).
3) Verifies kept servers have a tmux session named "detect" running
   scripts/detection-server.py.
4) Runs Period 7 triage for exact firmware 7.7.6 using Turso HTTP config in
   .env.local (no local SQLite replica).
5) Queues up to 200 missing VRU detection_runs for eligible 7.7.6 signal rows
   (non-motorway, speed_min is null or < 45, and no queued/running/completed run).
6) Appends a timestamped entry to logs/overnight-vru-7.7.6-interesting.md
   including counts and a ranked "interesting" list.

Safety:
- Does not touch production host 54.149.110.164.
- Only mutates EC2 instances whose Name tag contains "vru-detect-fleet".
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import socket
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_PATH = PROJECT_ROOT / "logs" / "overnight-vru-7.7.6-interesting.md"
RUN_SOURCE = "overnight-vru-7.7.6"

KEEP_PUBLIC_IPS = ["34.220.84.9", "52.33.236.250", "52.36.246.33"]
FORBIDDEN_PUBLIC_IPS = {"54.149.110.164"}

AWS_REGION = "us-west-2"
FLEET_NAME_MATCH = "vru-detect-fleet"

MODEL_NAME = "gdino-base+clip"
RUN_CONFIG: dict[str, Any] = {
    "modelDisplayName": "GDINO Base (grounding-dino-base) + CLIP",
    "type": "Open-vocabulary (detect anything described in text)",
    "device": "CUDA",
    "prompt": (
        "person. bicycle. motorcycle. wheelchair. stroller. "
        "person wearing safety vest. skateboard. dog."
    ),
    "framesPerVideo": 180,
    "frameSampling": "every_n_frames",
    "frameStride": 5,
    "source": RUN_SOURCE,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_env() -> None:
    env_path = PROJECT_ROOT / ".env.local"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def require_cmd(cmd: str) -> str:
    resolved = shutil.which(cmd)
    if not resolved:
        raise RuntimeError(f"Required command not found in PATH: {cmd}")
    return resolved


def run(cmd: list[str], *, check: bool = True, capture: bool = False, text: bool = True, timeout: int | None = None):
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture,
        text=text,
        timeout=timeout,
    )


def aws_json(args: list[str]) -> Any:
    require_cmd("aws")
    cmd = ["aws", *args, "--region", AWS_REGION, "--output", "json"]
    proc = run(cmd, check=True, capture=True, timeout=60)
    return json.loads(proc.stdout or "{}")


def ssh_cmd(host: str, remote_cmd: str, timeout_s: int = 15) -> tuple[int, str, str]:
    require_cmd("ssh")
    cmd = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        f"ConnectTimeout={timeout_s}",
        host,
        remote_cmd,
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    return proc.returncode, (proc.stdout or "").strip(), (proc.stderr or "").strip()


@dataclass(frozen=True)
class Instance:
    instance_id: str
    instance_type: str
    launch_time: str
    public_ip: str | None
    name: str | None


_INSTANCE_SIZE_ORDER = [
    "nano",
    "micro",
    "small",
    "medium",
    "large",
    "xlarge",
    "2xlarge",
    "3xlarge",
    "4xlarge",
    "6xlarge",
    "8xlarge",
    "9xlarge",
    "10xlarge",
    "12xlarge",
    "16xlarge",
    "18xlarge",
    "24xlarge",
    "32xlarge",
    "48xlarge",
    "56xlarge",
    "metal",
]


def instance_size_rank(instance_type: str) -> int:
    # Examples: g6e.4xlarge, c7i.2xlarge, m7g.large
    suffix = instance_type.split(".", 1)[-1]
    try:
        return _INSTANCE_SIZE_ORDER.index(suffix)
    except ValueError:
        return -1


def list_fleet_instances() -> list[Instance]:
    payload = aws_json(
        [
            "ec2",
            "describe-instances",
            "--filters",
            f"Name=tag:Name,Values=*{FLEET_NAME_MATCH}*",
            "Name=instance-state-name,Values=running",
        ]
    )
    instances: list[Instance] = []
    for res in payload.get("Reservations") or []:
        for inst in res.get("Instances") or []:
            tags = {t.get("Key"): t.get("Value") for t in (inst.get("Tags") or []) if t.get("Key")}
            instances.append(
                Instance(
                    instance_id=str(inst.get("InstanceId")),
                    instance_type=str(inst.get("InstanceType") or ""),
                    launch_time=str(inst.get("LaunchTime") or ""),
                    public_ip=inst.get("PublicIpAddress"),
                    name=tags.get("Name"),
                )
            )
    # sanity: don't include forbidden production host by public IP if it ever appears
    for inst in instances:
        if inst.public_ip and inst.public_ip in FORBIDDEN_PUBLIC_IPS:
            raise RuntimeError(
                f"Refusing to operate: got forbidden public IP in fleet listing: {inst.public_ip} ({inst.instance_id})"
            )
    return instances


def choose_instances_to_keep(instances: list[Instance], target_min: int = 2, target_max: int = 3) -> tuple[list[Instance], list[Instance]]:
    if not instances:
        return [], []

    by_id = {inst.instance_id: inst for inst in instances}
    keep: list[Instance] = []

    # Prefer explicit public IPs when present
    for ip in KEEP_PUBLIC_IPS:
        match = next((inst for inst in instances if inst.public_ip == ip), None)
        if match and match.instance_id in by_id:
            keep.append(match)
            by_id.pop(match.instance_id, None)

    remaining = list(by_id.values())
    # Keep oldest/smallest among remaining to reduce churn; we stop newest/largest first.
    remaining.sort(key=lambda i: (i.launch_time, instance_size_rank(i.instance_type)))

    # Fill to at least target_min (and up to target_max)
    while len(keep) < target_min and remaining:
        keep.append(remaining.pop(0))
    while len(keep) < target_max and remaining:
        keep.append(remaining.pop(0))

    # Everything else is excess
    excess = [inst for inst in instances if inst.instance_id not in {k.instance_id for k in keep}]
    return keep, excess


def stop_or_terminate_excess(excess: list[Instance], *, terminate: bool, dry_run: bool) -> list[str]:
    if not excess:
        return []

    # Prefer newest/largest extras first
    ordered = sorted(
        excess,
        key=lambda i: (i.launch_time, instance_size_rank(i.instance_type)),
        reverse=True,
    )
    instance_ids = [i.instance_id for i in ordered]

    if dry_run:
        return instance_ids

    action = "terminate-instances" if terminate else "stop-instances"
    aws_json(["ec2", action, "--instance-ids", *instance_ids])
    return instance_ids


def verify_tmux_detect(public_ip: str) -> tuple[bool, str]:
    host = f"ubuntu@{public_ip}"
    code, out, err = ssh_cmd(host, "tmux has-session -t detect 2>/dev/null; echo $?", timeout_s=10)
    if code != 0:
        return False, f"ssh failed: {err or out}"
    if out.strip() != "0":
        # Try listing sessions for more context
        _, sessions_out, _ = ssh_cmd(host, "tmux ls 2>/dev/null || true", timeout_s=10)
        return False, f"missing tmux session detect (tmux ls: {sessions_out or 'none'})"

    # Verify the session contains detection-server.py (best-effort)
    _, pane_out, _ = ssh_cmd(
        host,
        "tmux capture-pane -pt detect -S -200 2>/dev/null | rg -n \"scripts/detection-server\\.py\" -m 1 || true",
        timeout_s=10,
    )
    if pane_out.strip():
        return True, "ok (detect + detection-server.py seen)"
    return True, "ok (detect exists; detection-server.py not confirmed in last 200 lines)"


def make_client():
    import libsql_client

    turso_url = os.environ.get("TURSO_DATABASE_URL")
    turso_token = os.environ.get("TURSO_AUTH_TOKEN")
    if not turso_url or not turso_token:
        raise RuntimeError("TURSO_DATABASE_URL and TURSO_AUTH_TOKEN are required (set in .env.local).")
    http_url = turso_url.replace("libsql://", "https://")
    # Fail fast in network-restricted sandboxes: avoid long DNS timeouts that can
    # leave the automation hanging.
    try:
        host = http_url.split("://", 1)[1].split("/", 1)[0]
        socket.getaddrinfo(host, 443)
    except Exception as exc:
        raise RuntimeError(f"Turso host DNS lookup failed for {http_url}: {exc}") from exc
    return libsql_client.create_client_sync(url=http_url, auth_token=turso_token)


def execute(client, sql: str, args: list[Any] | None = None):
    import libsql_client

    if args:
        return client.execute(libsql_client.Statement(sql, args))
    return client.execute(sql)


def row_get(row: Any, key: str, index: int = 0) -> Any:
    try:
        return row[key]
    except Exception:
        return row[index]


def run_triage(count: int) -> int:
    env = os.environ.copy()
    env["TRIAGE_FETCH_CONCURRENCY"] = env.get("TRIAGE_FETCH_CONCURRENCY", "4")
    env["TRIAGE_PROCESS_WORKERS"] = env.get("TRIAGE_PROCESS_WORKERS", "2")
    # guardrail: avoid embedded replica; the automation expects HTTP mode.
    env["AI_EVENT_VIDEOS_TURSO_HTTP_ONLY"] = "1"
    cmd = [
        sys.executable or "python3",
        str(PROJECT_ROOT / "scripts" / "run-triage.py"),
        str(count),
        "--period",
        "7",
        "--firmware",
        "7.7.6",
    ]
    try:
        proc = subprocess.run(cmd, cwd=str(PROJECT_ROOT), env=env, timeout=180)
        return proc.returncode
    except subprocess.TimeoutExpired:
        return 124


def queue_missing_runs(client, limit: int) -> list[str]:
    result = execute(
        client,
        """SELECT t.id
           FROM triage_results t
           WHERE t.triage_result = 'signal'
             AND t.firmware_version = '7.7.6'
             AND (t.road_class IS NULL OR t.road_class != 'motorway')
             AND (t.speed_min IS NULL OR t.speed_min < 45)
             AND NOT EXISTS (
               SELECT 1 FROM detection_runs dr
               WHERE dr.video_id = t.id
                 AND dr.status IN ('queued','running','completed')
             )
           ORDER BY
             CASE WHEN t.event_timestamp IS NULL THEN 1 ELSE 0 END ASC,
             julianday(t.event_timestamp) DESC,
             t.created_at DESC
           LIMIT ?""",
        [limit],
    )
    ids = [str(row_get(r, "id")) for r in result.rows if row_get(r, "id")]
    if not ids:
        return []

    now = utc_now()
    queued: list[str] = []
    for video_id in ids:
        run_id = str(uuid.uuid4())
        config = dict(RUN_CONFIG)
        config["videoId"] = video_id
        execute(
            client,
            """INSERT INTO detection_runs
                 (id, video_id, model_name, status, config_json, priority, created_at)
               VALUES (?, ?, ?, 'queued', ?, ?, ?)""",
            [
                run_id,
                video_id,
                MODEL_NAME,
                json.dumps(config, separators=(",", ":")),
                10,
                now,
            ],
        )
        queued.append(video_id)

    return queued


def fetch_queue_counts(client) -> dict[str, int]:
    rows = execute(
        client,
        """SELECT status, COUNT(1) AS n
           FROM detection_runs
           WHERE status IN ('queued','running','completed')
           GROUP BY status""",
    ).rows
    out: dict[str, int] = {"queued": 0, "running": 0, "completed": 0}
    for r in rows:
        status = str(row_get(r, "status", 0))
        n = int(row_get(r, "n", 1) or 0)
        out[status] = n
    return out


def select_interesting_completed(client, limit: int = 20) -> list[dict[str, Any]]:
    # Heuristic ranking for quick review: completed runs on firmware 7.7.6 signal rows
    # with QC bucket ok/perfect and higher detection_count.
    rs = execute(
        client,
        """SELECT
             t.id AS video_id,
             t.event_type,
             t.speed_min,
             t.speed_max,
             t.event_timestamp,
             q.bucket AS fps_qc,
             MAX(COALESCE(dr.detection_count, 0)) AS detection_count,
             MAX(dr.completed_at) AS completed_at
           FROM detection_runs dr
           JOIN triage_results t ON t.id = dr.video_id
           LEFT JOIN video_frame_timing_qc q ON q.video_id = t.id
           WHERE dr.status = 'completed'
             AND t.triage_result = 'signal'
             AND t.firmware_version = '7.7.6'
             AND (q.bucket IN ('perfect','ok'))
           GROUP BY
             t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, q.bucket
           ORDER BY
             MAX(COALESCE(dr.detection_count, 0)) DESC,
             julianday(MAX(dr.completed_at)) DESC
           LIMIT ?""",
        [limit],
    )
    out: list[dict[str, Any]] = []
    for row in rs.rows:
        out.append(
            {
                "video_id": str(row_get(row, "video_id", 0)),
                "event_type": str(row_get(row, "event_type", 1)),
                "speed_min": row_get(row, "speed_min", 2),
                "speed_max": row_get(row, "speed_max", 3),
                "event_timestamp": row_get(row, "event_timestamp", 4),
                "fps_qc": row_get(row, "fps_qc", 5),
                "detection_count": row_get(row, "detection_count", 6),
                "completed_at": row_get(row, "completed_at", 7),
            }
        )
    return out


def format_speed(val: Any) -> str:
    if val is None or val == "":
        return "n/a"
    try:
        mph = float(val) * 2.237
        return f"{mph:.0f} mph"
    except Exception:
        return "n/a"


def append_log(
    *,
    timestamp: str,
    fleet_instances: list[Instance] | None,
    kept: list[Instance] | None,
    excess_action: str,
    excess_ids: list[str],
    tmux_checks: dict[str, str],
    triage_cmd: str,
    triage_rc: int | None,
    queued_ids: list[str] | None,
    queue_counts: dict[str, int] | None,
    interesting: list[dict[str, Any]] | None,
    notes: list[str],
) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not LOG_PATH.exists():
        LOG_PATH.write_text("# Overnight VRU 7.7.6 Interesting Events\n")

    lines: list[str] = []
    lines.append("")
    lines.append(f"## {timestamp}")
    lines.append("")
    lines.append("Fleet:")
    if fleet_instances is None:
        lines.append("- Fleet check: unavailable (no AWS access)")
    else:
        lines.append(f"- Active VRU detector servers: {len(fleet_instances)}")
        kept_ips = ", ".join([i.public_ip for i in (kept or []) if i.public_ip]) or "n/a"
        lines.append(f"- Kept: {kept_ips}")
        if excess_ids:
            lines.append(f"- {excess_action.capitalize()} excess instances: {len(excess_ids)} ({', '.join(excess_ids)})")
        else:
            lines.append("- Excess instances: 0")

    if tmux_checks:
        lines.append("")
        lines.append("Kept tmux checks:")
        for ip, status in tmux_checks.items():
            lines.append(f"- {ip}: {status}")

    lines.append("")
    lines.append("Immediate triage pass:")
    lines.append(f"- Command: `{triage_cmd}`")
    if triage_rc is None:
        lines.append("- Result: skipped (no Turso/BeeMaps access)")
    else:
        lines.append(f"- Exit code: {triage_rc}")

    lines.append("")
    lines.append("Queue status:")
    if queued_ids is None:
        lines.append("- Queue: skipped (no Turso access)")
    else:
        lines.append(f"- Newly queued eligible firmware 7.7.6 signal rows: {len(queued_ids)}")
    if queue_counts is not None:
        lines.append(f"- Totals (queued/running/completed): {queue_counts.get('queued',0)}/{queue_counts.get('running',0)}/{queue_counts.get('completed',0)}")

    lines.append("")
    lines.append("Interesting completed firmware 7.7.6 signal events:")
    if not interesting:
        lines.append("- None new from this pass.")
    else:
        for item in interesting:
            vid = item["video_id"]
            lines.append(
                "- "
                + f"http://localhost:3000/event/{vid} "
                + f"| {item.get('event_type','?')} "
                + f"| FPS QC {item.get('fps_qc','?')} "
                + f"| det={item.get('detection_count','?')} "
                + f"| speed_min {format_speed(item.get('speed_min'))} "
                + f"| speed_max {format_speed(item.get('speed_max'))}"
            )

    if notes:
        lines.append("")
        lines.append("Notes:")
        for note in notes:
            lines.append(f"- {note}")

    LOG_PATH.write_text(LOG_PATH.read_text() + "\n".join(lines).rstrip() + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Overnight firmware 7.7.6 VRU monitor")
    parser.add_argument("--triage", type=int, default=500, help="Number of events to triage (default: 500)")
    parser.add_argument("--queue-limit", type=int, default=200, help="Max missing rows to queue (default: 200)")
    parser.add_argument("--terminate-excess", action="store_true", help="Terminate (not stop) excess instances")
    parser.add_argument("--dry-run", action="store_true", help="Do not mutate EC2 or Turso; only report")
    parser.add_argument("--skip-fleet", action="store_true", help="Skip EC2 fleet checks/actions")
    parser.add_argument("--skip-tmux", action="store_true", help="Skip tmux checks on kept servers")
    parser.add_argument("--skip-triage", action="store_true", help="Skip triage step")
    parser.add_argument("--skip-queue", action="store_true", help="Skip queue step")
    parser.add_argument("--skip-interesting", action="store_true", help="Skip interesting list query")
    args = parser.parse_args()

    load_env()
    timestamp = utc_now()
    notes: list[str] = []

    fleet_instances: list[Instance] | None = None
    kept: list[Instance] | None = None
    excess_ids: list[str] = []
    tmux_checks: dict[str, str] = {}

    if not args.skip_fleet:
        try:
            fleet_instances = list_fleet_instances()
            kept, excess = choose_instances_to_keep(fleet_instances)
            if len(fleet_instances) > 3:
                excess_ids = stop_or_terminate_excess(
                    excess,
                    terminate=args.terminate_excess,
                    dry_run=args.dry_run,
                )
        except Exception as exc:
            notes.append(f"Fleet check failed: {exc}")
            fleet_instances = None

    if kept and not args.skip_tmux:
        for inst in kept:
            if not inst.public_ip:
                continue
            if inst.public_ip in FORBIDDEN_PUBLIC_IPS:
                continue
            ok, msg = verify_tmux_detect(inst.public_ip)
            tmux_checks[inst.public_ip] = msg if ok else f"FAIL: {msg}"

    triage_cmd = "TRIAGE_FETCH_CONCURRENCY=4 TRIAGE_PROCESS_WORKERS=2 python3 scripts/run-triage.py 500 --period 7 --firmware 7.7.6"
    triage_rc: int | None = None
    if not args.skip_triage and not args.dry_run:
        try:
            triage_rc = run_triage(args.triage)
        except Exception as exc:
            notes.append(f"Triage failed: {exc}")

    queued_ids: list[str] | None = None
    queue_counts: dict[str, int] | None = None
    interesting: list[dict[str, Any]] | None = None

    if not (args.skip_queue and args.skip_interesting):
        if args.dry_run:
            queued_ids = []
            queue_counts = None
            interesting = []
        else:
            try:
                client = make_client()
                try:
                    if not args.skip_queue:
                        queued_ids = queue_missing_runs(client, args.queue_limit)
                        queue_counts = fetch_queue_counts(client)
                    if not args.skip_interesting:
                        interesting = select_interesting_completed(client, limit=20)
                finally:
                    client.close()
            except Exception as exc:
                notes.append(f"Turso query failed: {exc}")

    append_log(
        timestamp=timestamp,
        fleet_instances=fleet_instances,
        kept=kept,
        excess_action="terminated" if args.terminate_excess else "stopped",
        excess_ids=excess_ids,
        tmux_checks=tmux_checks,
        triage_cmd=triage_cmd,
        triage_rc=triage_rc,
        queued_ids=queued_ids,
        queue_counts=queue_counts,
        interesting=interesting,
        notes=notes,
    )

    if notes:
        for note in notes:
            print(f"[warn] {note}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
