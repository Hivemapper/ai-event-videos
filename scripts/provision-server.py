#!/usr/bin/env python3
"""
Provision detection servers via SSH.

Usage:
    python3 scripts/provision-server.py 34.220.49.228
    python3 scripts/provision-server.py 34.220.49.228 54.184.38.149 16.144.29.139
    python3 scripts/provision-server.py 34.220.49.228 --workers 3 --frames 300
"""

import argparse
import os
import subprocess
import sys
import time

KEY_PATH = "~/Downloads/vru.pem"
USER = "ec2-user"
PROJECT_DIR = "~/ai-event-videos"
HF_TOKEN = os.environ.get("HF_TOKEN", "")

BOLD = "\033[1m"
GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
DIM = "\033[2m"
RESET = "\033[0m"


def ssh_cmd(ip: str, command: str, timeout: int = 30) -> tuple[int, str]:
    """Run a command over SSH. Returns (returncode, output)."""
    full = [
        "ssh", "-i", KEY_PATH,
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        f"{USER}@{ip}",
        command,
    ]
    try:
        result = subprocess.run(
            full, capture_output=True, text=True, timeout=timeout
        )
        return result.returncode, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return 1, "SSH command timed out"


def provision(ip: str, workers: int, frames: int, frame_stride: int) -> bool:
    """Provision a single server. Returns True on success."""
    print(f"\n{BOLD}{'═' * 50}{RESET}")
    print(f"  {CYAN}Provisioning {ip}{RESET}")
    print(f"  Workers: {workers} | Frames: {frames} | Frame stride: {frame_stride}")
    print(f"{BOLD}{'═' * 50}{RESET}")

    # 1. Test connectivity
    print(f"  {DIM}Testing SSH connection...{RESET}", end=" ", flush=True)
    rc, out = ssh_cmd(ip, "echo ok")
    if rc != 0:
        print(f"{RED}FAILED{RESET}")
        print(f"  {RED}{out.strip()}{RESET}")
        return False
    print(f"{GREEN}OK{RESET}")

    # 2. Git pull
    print(f"  {DIM}Pulling latest code...{RESET}", end=" ", flush=True)
    rc, out = ssh_cmd(ip, f"cd {PROJECT_DIR} && git pull", timeout=60)
    if rc != 0:
        print(f"{RED}FAILED{RESET}")
        print(f"  {RED}{out.strip()}{RESET}")
        return False
    status = "up to date" if "Already up to date" in out else "updated"
    print(f"{GREEN}{status}{RESET}")

    # 3. Kill any existing detection server
    print(f"  {DIM}Stopping existing server...{RESET}", end=" ", flush=True)
    ssh_cmd(ip, "tmux kill-session -t detect 2>/dev/null; pkill -f detection-server.py 2>/dev/null")
    time.sleep(1)
    print(f"{GREEN}done{RESET}")

    # 4. Start detection server in tmux
    print(f"  {DIM}Starting detection server...{RESET}", end=" ", flush=True)
    start_cmd = (
        f"cd {PROJECT_DIR} && source .venv/bin/activate && "
        f"export HF_TOKEN={HF_TOKEN} && "
        f"tmux new-session -d -s detect "
        f"'export HF_TOKEN={HF_TOKEN} && python3 scripts/detection-server.py --workers {workers} --frames {frames} --frame-stride {frame_stride}'"
    )
    rc, out = ssh_cmd(ip, start_cmd, timeout=15)
    if rc != 0:
        print(f"{RED}FAILED{RESET}")
        print(f"  {RED}{out.strip()}{RESET}")
        return False
    print(f"{GREEN}started{RESET}")

    # 5. Verify it's running
    time.sleep(3)
    print(f"  {DIM}Verifying...{RESET}", end=" ", flush=True)
    rc, out = ssh_cmd(ip, "tmux has-session -t detect 2>&1 && echo RUNNING || echo STOPPED")
    if "RUNNING" in out:
        print(f"{GREEN}server is running{RESET}")
        return True
    else:
        print(f"{RED}server not running{RESET}")
        print(f"  {RED}{out.strip()}{RESET}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Provision detection servers")
    parser.add_argument("ips", nargs="+", help="IP addresses to provision")
    parser.add_argument("--workers", type=int, default=2, help="Workers per server (default: 2)")
    parser.add_argument("--frames", type=int, default=300, help="Maximum frames per video (default: 300)")
    parser.add_argument("--frame-stride", type=int, default=5, help="Sample every N source frames (default: 5)")
    args = parser.parse_args()

    results = {}
    for ip in args.ips:
        ok = provision(ip, args.workers, args.frames, args.frame_stride)
        results[ip] = ok

    # Summary
    print(f"\n{BOLD}{'═' * 50}{RESET}")
    print(f"  Summary")
    print(f"{BOLD}{'═' * 50}{RESET}")
    for ip, ok in results.items():
        status = f"{GREEN}OK{RESET}" if ok else f"{RED}FAILED{RESET}"
        print(f"  {ip}  {status}")

    failed = sum(1 for ok in results.values() if not ok)
    if failed:
        print(f"\n  {RED}{failed} server(s) failed{RESET}")
        sys.exit(1)
    else:
        print(f"\n  {GREEN}All {len(results)} server(s) provisioned{RESET}")


if __name__ == "__main__":
    main()
