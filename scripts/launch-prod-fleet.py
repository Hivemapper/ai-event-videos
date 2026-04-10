#!/usr/bin/env python3
"""
Launch and provision a fleet of g6e.xlarge EC2 instances for prod-pipeline.

Usage:
    python3 scripts/launch-prod-fleet.py --count 5
    python3 scripts/launch-prod-fleet.py --count 10 --spot
    python3 scripts/launch-prod-fleet.py --count 3 --dry-run
    python3 scripts/launch-prod-fleet.py --list          # show running fleet
    python3 scripts/launch-prod-fleet.py --terminate      # terminate all fleet instances

Requires:
    - AWS CLI configured (aws configure)
    - SSH key at KEY_PATH
    - AMI with CUDA, Python, and project dependencies pre-installed
"""

import argparse
import json
import subprocess
import sys
import time

# ── Config ───────────────────────────────────────────────────────────────────
INSTANCE_TYPE = "g6e.xlarge"
AMI_ID = "ami-XXXXXXXXXXXXXXXXX"  # TODO: set to your CUDA-ready AMI
KEY_NAME = "vru"
KEY_PATH = "~/Downloads/vru.pem"
SECURITY_GROUP = "sg-XXXXXXXXXXXXXXXXX"  # TODO: set to your security group
SUBNET_ID = ""  # leave empty for default VPC, or set specific subnet
REGION = "us-west-2"
USER = "ec2-user"
PROJECT_DIR = "~/ai-event-videos"
FLEET_TAG = "prod-pipeline-fleet"

BOLD = "\033[1m"
GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
YELLOW = "\033[33m"
DIM = "\033[2m"
RESET = "\033[0m"


# ── Helpers ──────────────────────────────────────────────────────────────────

def aws_cli(args: list[str], timeout: int = 60) -> dict | list | str:
    """Run an AWS CLI command and return parsed JSON output."""
    cmd = ["aws"] + args + ["--region", REGION, "--output", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(f"AWS CLI failed: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else {}


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
        result = subprocess.run(full, capture_output=True, text=True, timeout=timeout)
        return result.returncode, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return 1, "SSH command timed out"


def wait_for_ssh(ip: str, max_wait: int = 300) -> bool:
    """Wait until SSH is available on the instance."""
    start = time.time()
    while time.time() - start < max_wait:
        rc, _ = ssh_cmd(ip, "echo ok", timeout=10)
        if rc == 0:
            return True
        time.sleep(10)
    return False


# ── Launch ───────────────────────────────────────────────────────────────────

def launch_instances(count: int, spot: bool = False) -> list[dict]:
    """Launch EC2 instances. Returns list of {id, ip} dicts."""
    print(f"\n{BOLD}Launching {count} × {INSTANCE_TYPE} ({'spot' if spot else 'on-demand'})...{RESET}")

    launch_args = [
        "ec2", "run-instances",
        "--image-id", AMI_ID,
        "--instance-type", INSTANCE_TYPE,
        "--key-name", KEY_NAME,
        "--security-group-ids", SECURITY_GROUP,
        "--count", str(count),
        "--tag-specifications",
        f'ResourceType=instance,Tags=[{{Key=Name,Value={FLEET_TAG}}},{{Key=fleet,Value={FLEET_TAG}}}]',
    ]
    if SUBNET_ID:
        launch_args += ["--subnet-id", SUBNET_ID]
    if spot:
        launch_args += [
            "--instance-market-options",
            '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"persistent","InstanceInterruptionBehavior":"stop"}}',
        ]

    result = aws_cli(launch_args, timeout=120)
    instances = result.get("Instances", [])
    ids = [i["InstanceId"] for i in instances]
    print(f"  Launched: {', '.join(ids)}")

    # Wait for public IPs
    print(f"  {DIM}Waiting for instances to get public IPs...{RESET}", flush=True)
    time.sleep(15)

    for _ in range(30):
        desc = aws_cli(["ec2", "describe-instances", "--instance-ids"] + ids)
        reservations = desc.get("Reservations", [])
        all_instances = [i for r in reservations for i in r.get("Instances", [])]
        all_have_ip = all(i.get("PublicIpAddress") for i in all_instances)
        if all_have_ip:
            return [
                {"id": i["InstanceId"], "ip": i["PublicIpAddress"]}
                for i in all_instances
            ]
        time.sleep(10)

    # Return what we have
    desc = aws_cli(["ec2", "describe-instances", "--instance-ids"] + ids)
    all_instances = [i for r in desc.get("Reservations", []) for i in r.get("Instances", [])]
    return [
        {"id": i["InstanceId"], "ip": i.get("PublicIpAddress", "pending")}
        for i in all_instances
    ]


# ── Provision ────────────────────────────────────────────────────────────────

def provision_instance(ip: str) -> bool:
    """Set up and start prod-pipeline on a single instance."""
    print(f"\n  {CYAN}Provisioning {ip}...{RESET}")

    # Wait for SSH
    print(f"    {DIM}Waiting for SSH...{RESET}", end=" ", flush=True)
    if not wait_for_ssh(ip):
        print(f"{RED}TIMEOUT{RESET}")
        return False
    print(f"{GREEN}OK{RESET}")

    # Git pull
    print(f"    {DIM}Pulling latest code...{RESET}", end=" ", flush=True)
    rc, out = ssh_cmd(ip, f"cd {PROJECT_DIR} && git pull", timeout=60)
    if rc != 0:
        print(f"{RED}FAILED{RESET}: {out.strip()}")
        return False
    print(f"{GREEN}done{RESET}")

    # Kill any existing pipeline
    print(f"    {DIM}Stopping existing pipeline...{RESET}", end=" ", flush=True)
    ssh_cmd(ip, "tmux kill-session -t prod 2>/dev/null; pkill -f prod-pipeline.py 2>/dev/null")
    time.sleep(1)
    print(f"{GREEN}done{RESET}")

    # Start prod-pipeline in tmux
    print(f"    {DIM}Starting prod-pipeline...{RESET}", end=" ", flush=True)
    start_cmd = (
        f"cd {PROJECT_DIR} && source .venv/bin/activate && "
        f"tmux new-session -d -s prod "
        f"'python3 scripts/prod-pipeline.py'"
    )
    rc, out = ssh_cmd(ip, start_cmd, timeout=15)
    if rc != 0:
        print(f"{RED}FAILED{RESET}: {out.strip()}")
        return False
    print(f"{GREEN}started{RESET}")

    # Verify
    time.sleep(3)
    rc, out = ssh_cmd(ip, "tmux has-session -t prod 2>&1 && echo RUNNING || echo STOPPED")
    if "RUNNING" in out:
        print(f"    {GREEN}prod-pipeline is running{RESET}")
        return True
    else:
        print(f"    {RED}prod-pipeline not running{RESET}")
        return False


# ── List / Terminate ─────────────────────────────────────────────────────────

def list_fleet():
    """List all running fleet instances."""
    desc = aws_cli([
        "ec2", "describe-instances",
        "--filters",
        f"Name=tag:fleet,Values={FLEET_TAG}",
        "Name=instance-state-name,Values=running,pending,stopped",
    ])
    instances = [i for r in desc.get("Reservations", []) for i in r.get("Instances", [])]

    if not instances:
        print(f"\n  {DIM}No fleet instances found.{RESET}")
        return

    print(f"\n{BOLD}Fleet instances ({len(instances)}):{RESET}")
    print(f"  {'ID':<22} {'IP':<17} {'State':<12} {'Launch Time'}")
    print(f"  {'─'*22} {'─'*17} {'─'*12} {'─'*20}")
    for i in instances:
        iid = i["InstanceId"]
        ip = i.get("PublicIpAddress", "—")
        state = i["State"]["Name"]
        launch = i.get("LaunchTime", "—")
        color = GREEN if state == "running" else YELLOW if state == "pending" else RED
        print(f"  {iid:<22} {ip:<17} {color}{state:<12}{RESET} {launch}")


def terminate_fleet():
    """Terminate all fleet instances."""
    desc = aws_cli([
        "ec2", "describe-instances",
        "--filters",
        f"Name=tag:fleet,Values={FLEET_TAG}",
        "Name=instance-state-name,Values=running,pending,stopped",
    ])
    instances = [i for r in desc.get("Reservations", []) for i in r.get("Instances", [])]

    if not instances:
        print(f"\n  {DIM}No fleet instances to terminate.{RESET}")
        return

    ids = [i["InstanceId"] for i in instances]
    print(f"\n{YELLOW}Terminating {len(ids)} instances: {', '.join(ids)}{RESET}")

    confirm = input(f"  Type 'yes' to confirm: ")
    if confirm.strip().lower() != "yes":
        print(f"  {DIM}Cancelled.{RESET}")
        return

    aws_cli(["ec2", "terminate-instances", "--instance-ids"] + ids)
    print(f"  {GREEN}Terminated {len(ids)} instances.{RESET}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Launch and provision prod-pipeline fleet")
    parser.add_argument("--count", type=int, default=1, help="Number of instances to launch")
    parser.add_argument("--spot", action="store_true", help="Use spot instances")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done without launching")
    parser.add_argument("--list", action="store_true", help="List running fleet instances")
    parser.add_argument("--terminate", action="store_true", help="Terminate all fleet instances")
    args = parser.parse_args()

    if args.list:
        list_fleet()
        return

    if args.terminate:
        terminate_fleet()
        return

    if AMI_ID.startswith("ami-XXX"):
        print(f"{RED}ERROR: Set AMI_ID in the script before launching.{RESET}")
        print(f"  Create an AMI from your current working g6e.xlarge instance:")
        print(f"  aws ec2 create-image --instance-id <id> --name 'prod-pipeline-base' --no-reboot")
        sys.exit(1)

    if SECURITY_GROUP.startswith("sg-XXX"):
        print(f"{RED}ERROR: Set SECURITY_GROUP in the script before launching.{RESET}")
        sys.exit(1)

    if args.dry_run:
        price = "$0.96/hr" if not args.spot else "~$0.48/hr"
        total = f"${args.count * (0.96 if not args.spot else 0.48):.2f}/hr"
        print(f"\n{BOLD}Dry run:{RESET}")
        print(f"  Instances: {args.count} × {INSTANCE_TYPE}")
        print(f"  Pricing: {price} each = {total}")
        print(f"  AMI: {AMI_ID}")
        print(f"  Key: {KEY_NAME}")
        print(f"  Region: {REGION}")
        print(f"  Fleet tag: {FLEET_TAG}")
        return

    # Launch
    instances = launch_instances(args.count, args.spot)

    # Provision each
    results = {}
    for inst in instances:
        ip = inst["ip"]
        if ip == "pending":
            print(f"  {RED}Skipping {inst['id']} — no public IP yet{RESET}")
            results[inst["id"]] = False
            continue
        ok = provision_instance(ip)
        results[f"{inst['id']} ({ip})"] = ok

    # Summary
    print(f"\n{BOLD}{'═' * 60}{RESET}")
    print(f"  Fleet Summary")
    print(f"{BOLD}{'═' * 60}{RESET}")
    for label, ok in results.items():
        status = f"{GREEN}OK{RESET}" if ok else f"{RED}FAILED{RESET}"
        print(f"  {label}  {status}")

    failed = sum(1 for ok in results.values() if not ok)
    total_ok = len(results) - failed
    if failed:
        print(f"\n  {GREEN}{total_ok} launched{RESET}, {RED}{failed} failed{RESET}")
    else:
        print(f"\n  {GREEN}All {total_ok} instances launched and running prod-pipeline{RESET}")


if __name__ == "__main__":
    main()
