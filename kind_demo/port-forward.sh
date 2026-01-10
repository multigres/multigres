#!/usr/bin/env bash
# Copyright 2025 Supabase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

CONTEXT="kind-multidemo"
NAMESPACE="default"
PID_FILE="${TMPDIR:-/tmp}/multigres-port-forwards.pids"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" >&2
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" >&2
}

# Function to start a port forward and save its PID
start_port_forward() {
    local resource=$1
    local ports=$2
    local description=$3

    log_info "Starting port-forward: $description ($ports)"
    kubectl --context "$CONTEXT" -n "$NAMESPACE" port-forward "$resource" "$ports" >/dev/null 2>&1 &
    local pid=$!
    echo "$pid" >> "$PID_FILE"

    # Give it a moment to start
    sleep 0.5

    # Check if the process is still running
    if kill -0 "$pid" 2>/dev/null; then
        log_success "  ✓ Started (PID: $pid)"
    else
        log_error "  ✗ Failed to start"
        return 1
    fi
}

# Function to stop all port forwards
stop_all() {
    if [ ! -f "$PID_FILE" ]; then
        log_warn "No port-forwards running (PID file not found)"
        return 0
    fi

    log_info "Stopping all port-forwards..."

    while read -r pid; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            log_success "  ✓ Stopped PID $pid"
        fi
    done < "$PID_FILE"

    rm -f "$PID_FILE"
    log_success "All port-forwards stopped"
}

# Function to check status
check_status() {
    if [ ! -f "$PID_FILE" ]; then
        log_warn "No port-forwards running"
        return 0
    fi

    log_info "Port-forward status:"
    local running=0
    local dead=0

    while read -r pid; do
        if [ -n "$pid" ]; then
            if kill -0 "$pid" 2>/dev/null; then
                echo -e "  ${GREEN}✓${NC} PID $pid is running"
                ((running++))
            else
                echo -e "  ${RED}✗${NC} PID $pid is dead"
                ((dead++))
            fi
        fi
    done < "$PID_FILE"

    echo ""
    echo "Running: $running, Dead: $dead"

    if [ $dead -gt 0 ]; then
        log_warn "Some port-forwards have died. Run '$0 restart' to restart them."
    fi
}

# Main logic
case "${1:-start}" in
    start)
        # Check if already running
        if [ -f "$PID_FILE" ]; then
            log_error "Port-forwards may already be running. Use '$0 stop' first or '$0 restart' to restart."
            exit 1
        fi

        log_info "Starting all port-forwards for Multigres kind demo..."
        echo ""

        # Create empty PID file
        : > "$PID_FILE"

        # Start all port forwards
        start_port_forward "service/multigateway" "15432:15432" "PostgreSQL (multigateway)"
        start_port_forward "service/multiadmin-web" "18100:18100" "Multiadmin Web UI"
        start_port_forward "service/multiadmin" "18000:18000" "Multiadmin REST API"
        start_port_forward "service/multiadmin" "18070:18070" "Multiadmin gRPC API"
        start_port_forward "service/observability" "3000:3000" "Grafana"
        start_port_forward "service/observability" "9090:9090" "Prometheus"
        start_port_forward "pod/multipooler-zone1-0" "15433:5432" "Pooler zone1-0"
        start_port_forward "pod/multipooler-zone1-1" "15434:5432" "Pooler zone1-1"
        start_port_forward "pod/multipooler-zone1-2" "15435:5432" "Pooler zone1-2"

        echo ""
        log_success "All port-forwards started!"
        echo ""
        echo "========================================="
        echo "Access URLs:"
        echo "========================================="
        echo ""
        echo "PostgreSQL (via multigateway):"
        echo "  psql --host=localhost --port=15432 -U postgres -d postgres"
        echo ""
        echo "Multiadmin Web UI:"
        echo "  http://localhost:18100"
        echo ""
        echo "Multiadmin API:"
        echo "  REST API:   http://localhost:18000"
        echo "  gRPC API:   localhost:18070"
        echo ""
        echo "Observability:"
        echo "  Grafana:    http://localhost:3000/dashboards"
        echo "  Prometheus: http://localhost:9090"
        echo ""
        echo "Direct pooler access:"
        echo "  psql --host=localhost --port=15433 -U postgres -d postgres  (zone1-0)"
        echo "  psql --host=localhost --port=15434 -U postgres -d postgres  (zone1-1)"
        echo "  psql --host=localhost --port=15435 -U postgres -d postgres  (zone1-2)"
        echo ""
        echo "To stop: $0 stop"
        echo "To check status: $0 status"
        echo ""
        ;;

    stop)
        stop_all
        ;;

    restart)
        log_info "Restarting all port-forwards..."
        stop_all
        sleep 1
        exec "$0" start
        ;;

    status)
        check_status
        ;;

    *)
        echo "Usage: $0 {start|stop|restart|status}"
        echo ""
        echo "  start   - Start all port-forwards"
        echo "  stop    - Stop all port-forwards"
        echo "  restart - Restart all port-forwards"
        echo "  status  - Check status of port-forwards"
        exit 1
        ;;
esac
