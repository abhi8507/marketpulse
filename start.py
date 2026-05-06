#!/usr/bin/env python3
"""
MarketPulse - Python Startup Script (Windows/Local Dev)
One-click script to start/stop/restart all MarketPulse services.

Usage:
    python start.py           # Start everything
    python start.py --stop    # Stop everything
    python start.py --restart # Restart everything
    python start.py --status  # Check status
"""

import argparse
import subprocess
import sys
import os
import time
import socket
import signal
from pathlib import Path

# ─── Config ───────────────────────────────────────────────────
ROOT     = Path(__file__).parent
LOG_DIR  = ROOT / "logs"
PID_FILE = LOG_DIR / "pids.txt"
VENV     = ROOT / "venv" / "Scripts" / "python.exe"
PYTHON   = str(VENV) if VENV.exists() else sys.executable

SERVICES = {
    "Producer": {
        "cmd":  [PYTHON, "ingestion/yahoo_producer.py"],
        "log":  "producer.log",
    },
    "ETL Pipeline": {
        "cmd":  [PYTHON, "processing/etl_pipeline.py"],
        "log":  "etl.log",
    },
    "FastAPI": {
        "cmd":  [PYTHON, "-m", "uvicorn", "api.main:app", "--reload", "--port", "8000", "--host", "0.0.0.0"],
        "log":  "api.log",
    },
}

PORTS = {
    "Zookeeper":  2181,
    "Kafka":      9092,
    "Kafka UI":   8080,
    "PostgreSQL": 5432,
    "Prometheus": 9090,
    "Grafana":    3000,
    "FastAPI":    8000,
}

# Python service ports to clean up on start
CLEANUP_PORTS = [8000]

# ─── Colors ───────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):      print(f"  {GREEN}✔{RESET}  {msg}")
def fail(msg):    print(f"  {RED}✘{RESET}  {msg}")
def waiting(msg): print(f"  {YELLOW}●{RESET}  {msg}")
def info(msg):    print(f"  {CYAN}ℹ{RESET}  {msg}")
def header(msg):  print(f"\n{CYAN}{BOLD}{'─'*55}{RESET}\n{BOLD} {msg}{RESET}\n{CYAN}{'─'*55}{RESET}")

# ─── Helpers ──────────────────────────────────────────────────

def ensure_log_dir():
    LOG_DIR.mkdir(exist_ok=True)

def is_port_open(port: int) -> bool:
    """Check if a port is accepting connections."""
    try:
        with socket.create_connection(("localhost", port), timeout=2):
            return True
    except (ConnectionRefusedError, TimeoutError, OSError):
        return False

def wait_for_port(name: str, port: int, retries: int = 15, delay: int = 3) -> bool:
    """Wait for a port to become available."""
    for i in range(1, retries + 1):
        if is_port_open(port):
            ok(f"{name} is ready on port {port}")
            return True
        waiting(f"Waiting for {name}... ({i}/{retries})")
        time.sleep(delay)
    fail(f"{name} did not become ready on port {port}")
    return False

def save_pids(pids: dict):
    """Save process PIDs to file."""
    ensure_log_dir()
    with open(PID_FILE, "w") as f:
        for name, pid in pids.items():
            f.write(f"{name}:{pid}\n")

def load_pids() -> dict:
    """Load saved PIDs from file."""
    if not PID_FILE.exists():
        return {}
    pids = {}
    with open(PID_FILE) as f:
        for line in f:
            line = line.strip()
            if ":" in line:
                name, pid = line.split(":", 1)
                pids[name] = int(pid)
    return pids

# ─── Port Cleanup ─────────────────────────────────────────────

def kill_port(port: int):
    """Kill any process currently using a specific port."""
    try:
        if sys.platform == "win32":
            result = subprocess.run(
                f"netstat -ano | findstr :{port}",
                shell=True, capture_output=True, text=True
            )
            pids_killed = set()
            for line in result.stdout.splitlines():
                parts = line.split()
                if parts and parts[-1].isdigit():
                    pid = parts[-1]
                    if pid not in pids_killed and pid != "0":
                        subprocess.run(
                            ["taskkill", "/F", "/PID", pid],
                            capture_output=True
                        )
                        pids_killed.add(pid)
        else:
            subprocess.run(["fuser", "-k", f"{port}/tcp"],
                           capture_output=True)
    except Exception:
        pass


def cleanup_ports():
    """Kill any lingering processes on Python service ports before starting."""
    waiting("Cleaning up ports...")
    for port in CLEANUP_PORTS:
        kill_port(port)
    time.sleep(1)
    ok("Ports cleared")

# ─── Docker ───────────────────────────────────────────────────

def docker_up():
    """Start Docker containers."""
    waiting("Starting Docker containers...")
    result = subprocess.run(
        ["docker-compose", "up", "-d"],
        cwd=ROOT, capture_output=True, text=True
    )
    if result.returncode == 0:
        ok("Docker containers started")
    else:
        fail(f"Docker error: {result.stderr}")
        sys.exit(1)

def docker_stop():
    """Stop Docker containers."""
    waiting("Stopping Docker containers...")
    subprocess.run(["docker-compose", "stop"], cwd=ROOT)
    ok("Docker containers stopped")

# ─── Python Services ──────────────────────────────────────────

def start_service(name: str, config: dict) -> int:
    """Start a Python service as a background process."""
    log_path = LOG_DIR / config["log"]
    log_file = open(log_path, "a")

    proc = subprocess.Popen(
        config["cmd"],
        cwd=ROOT,
        stdout=log_file,
        stderr=log_file,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
    )
    ok(f"{name} started (PID: {proc.pid}) → logs/{config['log']}")
    return proc.pid

def stop_python_services():
    """Stop all running Python services by PID."""
    pids = load_pids()
    if not pids:
        waiting("No saved PIDs found — killing by process name...")
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/F", "/IM", "python.exe"], capture_output=True)
        else:
            subprocess.run(["pkill", "-f", "yahoo_producer.py"], capture_output=True)
            subprocess.run(["pkill", "-f", "etl_pipeline.py"],   capture_output=True)
            subprocess.run(["pkill", "-f", "uvicorn"],            capture_output=True)
        ok("Python services stopped")
        return

    for name, pid in pids.items():
        try:
            if sys.platform == "win32":
                subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True)
            else:
                os.kill(pid, signal.SIGTERM)
            ok(f"{name} stopped (PID: {pid})")
        except Exception:
            waiting(f"{name} (PID: {pid}) already stopped")

    if PID_FILE.exists():
        PID_FILE.unlink()

    # Also clean up ports after stopping
    cleanup_ports()

# ─── Status ───────────────────────────────────────────────────

def show_status():
    header("MarketPulse Status")
    for name, port in PORTS.items():
        if is_port_open(port):
            ok(f"{name:<14} port {port}")
        else:
            fail(f"{name:<14} port {port} — not reachable")

    print()
    info("Kafka UI     → http://localhost:8080")
    info("FastAPI Docs → http://127.0.0.1:8000/docs")
    info("Grafana      → http://localhost:3000  (admin/admin)")
    info("Prometheus   → http://localhost:9090")
    print()

# ─── Start ────────────────────────────────────────────────────

def start_all():
    header("Starting MarketPulse")
    ensure_log_dir()

    # Step 1: Docker
    header("Step 1/4 — Docker Infrastructure")
    docker_up()
    wait_for_port("Kafka",      9092)
    wait_for_port("PostgreSQL", 5432)

    # Step 2: Port cleanup
    header("Step 2/4 — Port Cleanup")
    cleanup_ports()

    # Step 3: Python Services
    header("Step 3/4 — Python Services")
    pids = {}
    for name, config in SERVICES.items():
        pids[name] = start_service(name, config)
        time.sleep(3)  # stagger startup

    save_pids(pids)

    # Step 4: Status
    time.sleep(5)
    header("Step 4/4 — Status Check")
    show_status()

    header("MarketPulse is running!")
    info("Stop:    python start.py --stop")
    info("Status:  python start.py --status")
    info("Logs:    .\\logs\\")
    print()

# ─── Stop ─────────────────────────────────────────────────────

def stop_all():
    header("Stopping MarketPulse")
    stop_python_services()
    docker_stop()
    print(f"\n  {CYAN}{BOLD}MarketPulse stopped.{RESET}\n")

# ─── Entry Point ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="MarketPulse startup script")
    parser.add_argument("--stop",    action="store_true", help="Stop all services")
    parser.add_argument("--restart", action="store_true", help="Restart all services")
    parser.add_argument("--status",  action="store_true", help="Show status of all services")
    args = parser.parse_args()

    os.chdir(ROOT)

    if args.stop:
        stop_all()
    elif args.restart:
        stop_all()
        time.sleep(3)
        start_all()
    elif args.status:
        show_status()
    else:
        start_all()


if __name__ == "__main__":
    main()