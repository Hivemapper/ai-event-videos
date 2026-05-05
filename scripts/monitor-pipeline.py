#!/usr/bin/env python3
"""
Pipeline monitor — sends a push notification if no events complete for 1 hour.

Uses ntfy.sh (free push notifications, no account needed).

Setup:
  1. Install ntfy app on your phone (iOS/Android)
  2. Subscribe to topic: hivemapper-pipeline (or change NTFY_TOPIC below)
  3. Run: python3 scripts/monitor-pipeline.py

Usage:
    python3 scripts/monitor-pipeline.py
    python3 scripts/monitor-pipeline.py --check-interval 5   # check every 5 min
    python3 scripts/monitor-pipeline.py --stale-minutes 30    # alert after 30 min
"""

import argparse
import json
import time
import urllib.request
from datetime import datetime, timezone, timedelta

PIPELINE_API = "http://localhost:3000/api/pipeline?tab=completed&limit=1"
NTFY_TOPIC = "hivemapper-pipeline"
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

BOLD = "\033[1m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
DIM = "\033[2m"
RESET = "\033[0m"


def check_pipeline() -> dict:
    """Returns pipeline status: last_completed time, counts."""
    try:
        req = urllib.request.Request(PIPELINE_API)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())

        counts = data.get("counts", {})
        rows = data.get("rows", [])

        last_completed_at = None
        if rows and rows[0].get("completed_at"):
            last_completed_at = datetime.fromisoformat(
                rows[0]["completed_at"].replace("Z", "+00:00")
            )

        return {
            "ok": True,
            "last_completed": last_completed_at,
            "queued": counts.get("queued", 0),
            "completed": counts.get("completed", 0),
            "running": counts.get("running", 0),
            "failed": counts.get("failed", 0),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def send_notification(title: str, message: str, priority: str = "high"):
    """Send push notification via ntfy.sh."""
    try:
        data = message.encode("utf-8")
        req = urllib.request.Request(NTFY_URL, data=data, method="POST")
        req.add_header("Title", title)
        req.add_header("Priority", priority)
        req.add_header("Tags", "warning")
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"  {RED}Failed to send notification: {e}{RESET}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Pipeline monitor")
    parser.add_argument("--check-interval", type=int, default=5, help="Check interval in minutes (default: 5)")
    parser.add_argument("--stale-minutes", type=int, default=60, help="Alert after N minutes of no completions (default: 60)")
    parser.add_argument("--topic", type=str, default=NTFY_TOPIC, help=f"ntfy.sh topic (default: {NTFY_TOPIC})")
    args = parser.parse_args()

    global NTFY_URL
    NTFY_URL = f"https://ntfy.sh/{args.topic}"

    print(f"{BOLD}Pipeline Monitor{RESET}")
    print(f"  Check interval: {args.check_interval} min")
    print(f"  Alert threshold: {args.stale_minutes} min")
    print(f"  ntfy topic: {args.topic}")
    print(f"  Subscribe on phone: ntfy.sh app -> topic '{args.topic}'")
    print()

    alerted = False
    last_count = None

    while True:
        now = datetime.now(timezone.utc)
        status = check_pipeline()

        if not status["ok"]:
            ts = now.strftime("%H:%M")
            print(f"  {DIM}{ts}{RESET}  {RED}API unreachable: {status.get('error', '?')}{RESET}")
            time.sleep(args.check_interval * 60)
            continue

        last = status["last_completed"]
        gap_min = (now - last).total_seconds() / 60 if last else float("inf")
        ts = now.strftime("%H:%M")

        # Track progress
        current_count = status["completed"]
        delta = ""
        if last_count is not None:
            diff = current_count - last_count
            delta = f"  (+{diff})" if diff > 0 else "  (+0)"
        last_count = current_count

        if gap_min > args.stale_minutes:
            print(f"  {DIM}{ts}{RESET}  {RED}STALE — no completions for {gap_min:.0f} min{RESET}  q={status['queued']:,}  done={current_count:,}{delta}")
            if not alerted:
                sent = send_notification(
                    "Pipeline Stalled",
                    f"No events completed in {gap_min:.0f} min.\n"
                    f"Queued: {status['queued']:,} | Completed: {current_count:,} | Running: {status['running']}",
                )
                if sent:
                    print(f"  {YELLOW}Notification sent{RESET}")
                    alerted = True
        else:
            print(f"  {DIM}{ts}{RESET}  {GREEN}OK{RESET} — last completed {gap_min:.0f}m ago  q={status['queued']:,}  done={current_count:,}{delta}")
            if alerted:
                # Pipeline recovered, send recovery notification and reset
                send_notification(
                    "Pipeline Recovered",
                    f"Processing resumed. Queued: {status['queued']:,} | Completed: {current_count:,}",
                    priority="default",
                )
                print(f"  {GREEN}Recovery notification sent{RESET}")
            alerted = False

        time.sleep(args.check_interval * 60)


if __name__ == "__main__":
    main()
