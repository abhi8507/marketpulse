#!/bin/bash
# ─────────────────────────────────────────────────────────────
# MarketPulse - Bash Startup Script (AWS/Linux Production)
# One-click script to start/stop/restart all services.
#
# Usage:
#   ./start.sh           # Start everything
#   ./start.sh --stop    # Stop everything
#   ./start.sh --restart # Restart everything
#   ./start.sh --status  # Check status
# ─────────────────────────────────────────────────────────────

set -e

# ─── Config ───────────────────────────────────────────────────
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$ROOT/logs"
PID_FILE="$LOG_DIR/pids.txt"
VENV="$ROOT/venv/bin/python"
PYTHON=$([ -f "$VENV" ] && echo "$VENV" || echo "python3")

# ─── Colors ───────────────────────────────────────────────────
GREEN="\033[92m"
RED="\033[91m"
YELLOW="\033[93m"
CYAN="\033[96m"
BOLD="\033[1m"
RESET="\033[0m"

ok()      { echo -e "  ${GREEN}✔${RESET}  $1"; }
fail()    { echo -e "  ${RED}✘${RESET}  $1"; }
waiting() { echo -e "  ${YELLOW}●${RESET}  $1"; }
info()    { echo -e "  ${CYAN}ℹ${RESET}  $1"; }
header()  { echo -e "\n${CYAN}${BOLD}$(printf '─%.0s' {1..55})${RESET}\n${BOLD} $1${RESET}\n${CYAN}$(printf '─%.0s' {1..55})${RESET}"; }

# ─── Helpers ──────────────────────────────────────────────────

ensure_log_dir() {
    mkdir -p "$LOG_DIR"
}

is_port_open() {
    local port=$1
    timeout 2 bash -c "echo >/dev/tcp/localhost/$port" 2>/dev/null
}

wait_for_port() {
    local name=$1
    local port=$2
    local retries=${3:-15}
    local delay=${4:-3}

    for ((i=1; i<=retries; i++)); do
        if is_port_open "$port"; then
            ok "$name is ready on port $port"
            return 0
        fi
        waiting "Waiting for $name... ($i/$retries)"
        sleep "$delay"
    done
    fail "$name did not become ready on port $port"
    return 1
}

# ─── Docker ───────────────────────────────────────────────────

docker_up() {
    waiting "Starting Docker containers..."
    cd "$ROOT"
    docker-compose up -d
    ok "Docker containers started"
}

docker_stop() {
    waiting "Stopping Docker containers..."
    cd "$ROOT"
    docker-compose stop
    ok "Docker containers stopped"
}

# ─── Python Services ──────────────────────────────────────────

start_service() {
    local name=$1
    local cmd=$2
    local log=$3

    cd "$ROOT"
    nohup $PYTHON $cmd >> "$LOG_DIR/$log" 2>&1 &
    local pid=$!
    echo "$name:$pid" >> "$PID_FILE"
    ok "$name started (PID: $pid) → logs/$log"
}

stop_python_services() {
    if [ -f "$PID_FILE" ]; then
        while IFS=: read -r name pid; do
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null && ok "$name stopped (PID: $pid)"
            else
                waiting "$name (PID: $pid) already stopped"
            fi
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    else
        waiting "No PID file found — killing by process name..."
        pkill -f "yahoo_producer.py" 2>/dev/null || true
        pkill -f "etl_pipeline.py"   2>/dev/null || true
        pkill -f "uvicorn"            2>/dev/null || true
        ok "Python services stopped"
    fi
}

# ─── Status ───────────────────────────────────────────────────

show_status() {
    header "MarketPulse Status"

    declare -A PORTS=(
        ["Zookeeper"]=2181
        ["Kafka"]=9092
        ["Kafka UI"]=8080
        ["PostgreSQL"]=5432
        ["Prometheus"]=9090
        ["Grafana"]=3000
        ["FastAPI"]=8000
    )

    for name in "Zookeeper" "Kafka" "Kafka UI" "PostgreSQL" "Prometheus" "Grafana" "FastAPI"; do
        port=${PORTS[$name]}
        if is_port_open "$port"; then
            ok "$(printf '%-14s' "$name") port $port"
        else
            fail "$(printf '%-14s' "$name") port $port — not reachable"
        fi
    done

    echo ""
    info "Kafka UI     → http://localhost:8080"
    info "FastAPI Docs → http://localhost:8000/docs"
    info "Grafana      → http://localhost:3000  (admin/admin)"
    info "Prometheus   → http://localhost:9090"
    echo ""
}

# ─── Start ────────────────────────────────────────────────────

start_all() {
    header "Starting MarketPulse"
    ensure_log_dir
    > "$PID_FILE"  # clear old pids

    # Step 1: Docker
    header "Step 1/3 — Docker Infrastructure"
    docker_up
    wait_for_port "Kafka"      9092
    wait_for_port "PostgreSQL" 5432

    # Step 2: Python Services
    header "Step 2/3 — Python Services"
    start_service "Producer"     "ingestion/yahoo_producer.py"  "producer.log"
    sleep 3
    start_service "ETL Pipeline" "processing/etl_pipeline.py"   "etl.log"
    sleep 3
    start_service "FastAPI"      "-m uvicorn api.main:app --reload --port 8000" "api.log"
    sleep 5

    # Step 3: Status
    header "Step 3/3 — Status Check"
    show_status

    header "MarketPulse is running!"
    info "Stop:    ./start.sh --stop"
    info "Status:  ./start.sh --status"
    info "Logs:    ./logs/"
    echo ""
}

# ─── Stop ─────────────────────────────────────────────────────

stop_all() {
    header "Stopping MarketPulse"
    stop_python_services
    docker_stop
    echo -e "\n  ${CYAN}${BOLD}MarketPulse stopped.${RESET}\n"
}

# ─── Entry Point ──────────────────────────────────────────────

case "${1:-}" in
    --stop)    stop_all    ;;
    --restart) stop_all; sleep 3; start_all ;;
    --status)  show_status ;;
    *)         start_all   ;;
esac