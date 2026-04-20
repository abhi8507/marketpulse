#!/usr/bin/env python3
"""
SignalFlow - Setup Verification Script
Run this to check all required software is installed correctly.
Usage: python check_setup.py
"""

import subprocess
import sys
import importlib
from shutil import which

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"

passed = 0
failed = 0
warnings = 0

def check(label, ok, found="", required="", warn=False):
    global passed, failed, warnings
    if ok:
        print(f"  {GREEN}✔{RESET}  {label:<35} {CYAN}{found}{RESET}")
        passed += 1
    elif warn:
        print(f"  {YELLOW}⚠{RESET}  {label:<35} {YELLOW}Not found (optional){RESET}")
        warnings += 1
    else:
        detail = f"  →  need {required}" if required else ""
        print(f"  {RED}✘{RESET}  {label:<35} {RED}Not found{RESET}{detail}")
        failed += 1

def run(cmd):
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).decode().strip()
        return True, out
    except subprocess.CalledProcessError:
        return False, ""

def section(title):
    print(f"\n{BOLD}{CYAN}{'─'*50}{RESET}")
    print(f"{BOLD} {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─'*50}{RESET}")

# ─────────────────────────────────────────────
section("1. Core Runtimes")
# ─────────────────────────────────────────────

ok, ver = run("python --version")
if not ok:
    ok, ver = run("python3 --version")
py_ver = ver.replace("Python ", "") if ok else ""
py_ok = ok and int(py_ver.split(".")[1]) >= 11 if ok else False
check("Python 3.11+", py_ok, py_ver, "3.11 or higher")

ok, ver = run("node --version")
node_ver = ver.lstrip("v") if ok else ""
node_ok = ok and int(node_ver.split(".")[0]) >= 18 if ok else False
check("Node.js 18+", node_ok, ver if ok else "", "18 or higher")

ok, ver = run("git --version")
check("Git", ok, ver.replace("git version ", "") if ok else "")

ok, ver = run("docker --version")
check("Docker", ok, ver.split(",")[0].replace("Docker version ", "") if ok else "")

ok, _ = run("docker info")
check("Docker Desktop running", ok, "running" if ok else "")

# ─────────────────────────────────────────────
section("2. CLIs")
# ─────────────────────────────────────────────

ok, ver = run("ng version --skip-confirmation 2>&1 | grep 'Angular CLI'")
check("Angular CLI (ng)", ok, ver.strip() if ok else "")

ok, ver = run("aws --version")
check("AWS CLI", ok, ver.split(" ")[0] if ok else "", warn=True)

ok, ver = run("pip --version")
check("pip", ok, ver.split(" ")[1] if ok else "")

ok, _ = run("virtualenv --version")
check("virtualenv", ok, "installed" if ok else "")

# ─────────────────────────────────────────────
section("3. Python Packages")
# ─────────────────────────────────────────────

packages = [
    ("fastapi",       "fastapi"),
    ("uvicorn",       "uvicorn"),
    ("kafka-python",  "kafka"),
    ("pandas",        "pandas"),
    ("numpy",         "numpy"),
    ("yfinance",      "yfinance"),
    ("sqlalchemy",    "sqlalchemy"),
    ("requests",      "requests"),
    ("websockets",    "websockets"),
    ("openai",        "openai"),
]

for display, mod in packages:
    try:
        m = importlib.import_module(mod)
        ver = getattr(m, "__version__", "installed")
        check(display, True, ver)
    except ImportError:
        check(display, False, required=f"pip install {display}")

# ─────────────────────────────────────────────
section("4. Docker Services (docker-compose)")
# ─────────────────────────────────────────────

ok, out = run("docker ps --format '{{.Names}}'")
running = out.splitlines() if ok else []

services = {
    "kafka":      "Apache Kafka",
    "postgres":   "PostgreSQL",
    "zookeeper":  "Zookeeper",
}

for container, label in services.items():
    is_running = any(container in name for name in running)
    check(f"{label} container", is_running,
          "running" if is_running else "",
          warn=not is_running)

# ─────────────────────────────────────────────
section("5. Environment Variables (.env)")
# ─────────────────────────────────────────────

import os
env_vars = [
    ("ALPHA_VANTAGE_API_KEY", True),
    ("POLYGON_API_KEY",       True),
    ("OPENAI_API_KEY",        True),
    ("AWS_S3_BUCKET",         False),
    ("DATABASE_URL",          True),
    ("KAFKA_BOOTSTRAP_SERVERS", True),
]

# Try loading .env file if present
try:
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
except FileNotFoundError:
    print(f"  {YELLOW}⚠{RESET}  No .env file found in current directory")

for var, required in env_vars:
    val = os.environ.get(var, "")
    is_set = bool(val) and val not in ("your_key", "your_bucket", "")
    check(var, is_set,
          "set ✓" if is_set else "",
          warn=not required)

# ─────────────────────────────────────────────
section("Summary")
# ─────────────────────────────────────────────

total = passed + failed + warnings
print(f"\n  {GREEN}Passed:  {passed}{RESET}")
if warnings:
    print(f"  {YELLOW}Warnings: {warnings} (optional){RESET}")
if failed:
    print(f"  {RED}Failed:  {failed}{RESET}")

print()
if failed == 0:
    print(f"  {GREEN}{BOLD}✔ All required checks passed! You're ready to start.{RESET}")
    print(f"  Run: {CYAN}docker-compose up -d{RESET} to start Kafka + PostgreSQL")
else:
    print(f"  {RED}{BOLD}✘ Fix the failed checks above before starting.{RESET}")
    print(f"\n  Quick installs:")
    print(f"  {CYAN}pip install fastapi uvicorn kafka-python pandas numpy yfinance sqlalchemy requests websockets openai{RESET}")
    print(f"  {CYAN}npm install -g @angular/cli{RESET}")

print()