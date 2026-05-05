#!/usr/bin/env python3
"""
Provision an existing server to run the production pipeline.

Usage:
    python3 scripts/provision-prod-server.py 34.220.49.228
    python3 scripts/provision-prod-server.py 34.220.49.228 --env-file .env.local
    python3 scripts/provision-prod-server.py 34.220.49.228 --limit 50

The script verifies SSH, pulls the repo, optionally uploads an env file, checks
runtime prerequisites, and starts scripts/prod-pipeline.py in a tmux session.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path


BOLD = "\033[1m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
DIM = "\033[2m"
RESET = "\033[0m"


def quote_remote(value: str) -> str:
    """Quote a shell value while preserving a leading ~/ expansion."""
    if value == "~":
        return "$HOME"
    if value.startswith("~/"):
        return "$HOME/" + shlex.quote(value[2:])
    return shlex.quote(value)


def run_local(cmd: list[str], timeout: int = 60) -> tuple[int, str]:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result.returncode, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return 1, "local command timed out"


def ssh_cmd(
    ip: str,
    command: str,
    *,
    user: str,
    key_path: str,
    timeout: int = 30,
) -> tuple[int, str]:
    full = [
        "ssh",
        "-i",
        os.path.expanduser(key_path),
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "ConnectTimeout=10",
        f"{user}@{ip}",
        command,
    ]
    return run_local(full, timeout=timeout)


def scp_file(
    ip: str,
    local_path: Path,
    remote_path: str,
    *,
    user: str,
    key_path: str,
) -> tuple[int, str]:
    full = [
        "scp",
        "-i",
        os.path.expanduser(key_path),
        "-o",
        "StrictHostKeyChecking=no",
        str(local_path),
        f"{user}@{ip}:{remote_path}",
    ]
    return run_local(full, timeout=60)


def print_step(message: str) -> None:
    print(f"  {DIM}{message}{RESET}", end=" ", flush=True)


def fail(output: str) -> bool:
    print(f"{RED}FAILED{RESET}")
    if output.strip():
        print(f"  {RED}{output.strip()}{RESET}")
    return False


def check_remote_prereqs(
    ip: str,
    *,
    user: str,
    key_path: str,
    project_dir: str,
) -> bool:
    checks = f"""
set -e
cd {quote_remote(project_dir)}
command -v tmux >/dev/null
command -v ffmpeg >/dev/null
test -d .venv
source .venv/bin/activate
python3 - <<'PY'
import importlib.util
import os
from pathlib import Path

def read_env_file(path):
    values = {{}}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values

env_file = read_env_file(Path(".env.local"))

def has_env(name):
    return bool(os.environ.get(name) or env_file.get(name))

def has_instance_role():
    try:
        import urllib.request
        with urllib.request.urlopen(
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
            timeout=1,
        ) as response:
            return bool(response.read().strip())
    except Exception:
        return False

missing_env = [
    name
    for name in ("BEEMAPS_API_KEY", "TURSO_DATABASE_URL", "TURSO_AUTH_TOKEN")
    if not has_env(name)
]

has_aws = (
    bool(os.environ.get("AWS_ACCESS_KEY_ID"))
    or bool(os.environ.get("AWS_PROFILE"))
    or Path.home().joinpath(".aws", "credentials").exists()
    or has_instance_role()
)
if not has_aws:
    missing_env.append("AWS credentials or AWS_PROFILE")

missing_modules = [
    module
    for module in ("boto3", "cv2", "numpy", "requests", "ultralytics")
    if importlib.util.find_spec(module) is None
]

missing_files = []
if not Path("data/models/privacy_v8_all.pt").exists():
    missing_files.append("data/models/privacy_v8_all.pt")

if missing_env or missing_modules or missing_files:
    if missing_env:
        print("Missing env/credentials: " + ", ".join(missing_env))
    if missing_modules:
        print("Missing Python modules: " + ", ".join(missing_modules))
    if missing_files:
        print("Missing files: " + ", ".join(missing_files))
    raise SystemExit(1)

print("prereqs ok")
PY
"""
    rc, out = ssh_cmd(
        ip,
        checks,
        user=user,
        key_path=key_path,
        timeout=60,
    )
    if rc != 0:
        return fail(out)
    print(f"{GREEN}OK{RESET}")
    return True


def provision(ip: str, args: argparse.Namespace) -> bool:
    project_dir = args.project_dir
    remote_project_dir = quote_remote(project_dir)
    user = args.user
    key_path = args.key

    print(f"\n{BOLD}{'=' * 60}{RESET}")
    print(f"  {CYAN}Provisioning production pipeline on {ip}{RESET}")
    print(f"  User: {user} | Project: {project_dir} | Session: {args.session}")
    print(f"{BOLD}{'=' * 60}{RESET}")

    print_step("Testing SSH")
    rc, out = ssh_cmd(ip, "echo ok", user=user, key_path=key_path)
    if rc != 0:
        return fail(out)
    print(f"{GREEN}OK{RESET}")

    print_step("Pulling latest code")
    rc, out = ssh_cmd(
        ip,
        f"cd {remote_project_dir} && git pull",
        user=user,
        key_path=key_path,
        timeout=90,
    ) if not args.skip_pull else (0, "skipped")
    if args.skip_pull:
        print(f"{YELLOW}skipped{RESET}")
    else:
        if rc != 0:
            return fail(out)
        print(f"{GREEN}done{RESET}")

    if args.env_file:
        env_path = Path(args.env_file).expanduser()
        if not env_path.exists():
            print(f"  {RED}Env file not found: {env_path}{RESET}")
            return False
        remote_env = f"{project_dir.rstrip('/')}/.env.local"
        print_step(f"Uploading {env_path.name}")
        rc, out = scp_file(
            ip,
            env_path,
            remote_env,
            user=user,
            key_path=key_path,
        )
        if rc != 0:
            return fail(out)
        print(f"{GREEN}done{RESET}")

    print_step("Checking prerequisites")
    if not check_remote_prereqs(
        ip,
        user=user,
        key_path=key_path,
        project_dir=project_dir,
    ):
        return False

    if args.check_only:
        print(f"  {YELLOW}Check-only mode; not starting worker.{RESET}")
        return True

    print_step("Stopping existing production worker")
    ssh_cmd(
        ip,
        f"tmux kill-session -t {shlex.quote(args.session)} 2>/dev/null || true",
        user=user,
        key_path=key_path,
    )
    time.sleep(1)
    print(f"{GREEN}done{RESET}")

    prod_args = ["python3", "scripts/prod-pipeline.py", "--poll", str(args.poll)]
    if args.limit:
        prod_args.extend(["--limit", str(args.limit)])
    worker_cmd = " ".join(shlex.quote(part) for part in prod_args)
    start_cmd = (
        f"cd {remote_project_dir} && "
        "set -a && [ ! -f .env.local ] || source .env.local && set +a && "
        "source .venv/bin/activate && "
        f"tmux new-session -d -s {shlex.quote(args.session)} {shlex.quote(worker_cmd)}"
    )

    print_step("Starting production worker")
    rc, out = ssh_cmd(
        ip,
        start_cmd,
        user=user,
        key_path=key_path,
        timeout=20,
    )
    if rc != 0:
        return fail(out)
    print(f"{GREEN}started{RESET}")

    time.sleep(3)
    print_step("Verifying tmux session")
    rc, out = ssh_cmd(
        ip,
        f"tmux has-session -t {shlex.quote(args.session)} 2>&1 && echo RUNNING || echo STOPPED",
        user=user,
        key_path=key_path,
    )
    if rc == 0 and "RUNNING" in out:
        print(f"{GREEN}RUNNING{RESET}")
        print(f"  Logs: ssh -i {key_path} {user}@{ip} 'tmux attach -t {args.session}'")
        return True
    return fail(out)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Provision an existing server for scripts/prod-pipeline.py"
    )
    parser.add_argument("ip", help="Server public IP or hostname")
    parser.add_argument("--user", default="ec2-user", help="SSH user")
    parser.add_argument("--key", default="~/Downloads/vru.pem", help="SSH private key")
    parser.add_argument("--project-dir", default="~/ai-event-videos", help="Remote repo path")
    parser.add_argument("--env-file", help="Optional local env file to upload as remote .env.local")
    parser.add_argument("--session", default="prod", help="tmux session name")
    parser.add_argument("--poll", type=float, default=5, help="prod-pipeline poll interval")
    parser.add_argument("--limit", type=int, help="Optional run limit")
    parser.add_argument("--skip-pull", action="store_true", help="Do not git pull before starting")
    parser.add_argument("--check-only", action="store_true", help="Verify only; do not start")
    args = parser.parse_args()

    ok = provision(args.ip, args)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
