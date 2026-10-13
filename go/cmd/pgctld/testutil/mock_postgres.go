// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// MockExecCommand mocks exec.Command for testing
type MockExecCommand struct {
	commands map[string]MockCommandResult
}

// MockCommandResult defines the expected result of a mocked command
type MockCommandResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Error    error
}

// NewMockExecCommand creates a new mock command executor
func NewMockExecCommand() *MockExecCommand {
	return &MockExecCommand{
		commands: make(map[string]MockCommandResult),
	}
}

// AddCommand adds a mock command with expected result
func (m *MockExecCommand) AddCommand(cmdLine string, result MockCommandResult) {
	m.commands[cmdLine] = result
}

// MockCommand simulates command execution for testing
func (m *MockExecCommand) MockCommand(name string, args ...string) *exec.Cmd {
	cmdLine := fmt.Sprintf("%s %s", name, strings.Join(args, " "))

	// Create a fake command that will be handled by the test helper
	cmd := exec.Command("echo", "mock")

	// Store the command line for verification
	if cmd.Env == nil {
		cmd.Env = os.Environ()
	}
	cmd.Env = append(cmd.Env, "MOCK_CMD="+cmdLine)

	return cmd
}

// VerifyCommand checks if a command was called with expected arguments
func (m *MockExecCommand) VerifyCommand(t *testing.T, expectedCmd string) {
	t.Helper()

	if _, exists := m.commands[expectedCmd]; !exists {
		t.Errorf("Expected command was not configured: %s", expectedCmd)
	}
}

// MockBinary creates a mock binary for testing
func MockBinary(t *testing.T, binDir, name, content string) string {
	t.Helper()

	binPath := fmt.Sprintf("%s/%s", binDir, name)

	script := fmt.Sprintf(`#!/bin/bash
# Mock %s binary for testing
%s
`, name, content)

	if err := os.WriteFile(binPath, []byte(script), 0o755); err != nil {
		t.Fatalf("Failed to create mock binary %s: %v", name, err)
	}

	return binPath
}

// CreateMockPostgreSQLBinaries creates mock PostgreSQL binaries for testing
func CreateMockPostgreSQLBinaries(t *testing.T, binDir string) {
	t.Helper()

	// Mock initdb
	MockBinary(t, binDir, "initdb", `
if [[ "$*" == *"--help"* ]]; then
    echo "initdb initializes a PostgreSQL database cluster."
    exit 0
fi
echo "Success. You can now start the database server using:"
mkdir -p "$2/base"
echo "15.0" > "$2/PG_VERSION"
touch "$2/postgresql.conf"
touch "$2/pg_hba.conf"
`)

	// Mock pg_controldata
	MockBinary(t, binDir, "pg_controldata", `
#!/bin/bash
echo "pg_control version number:            1300"
echo "Catalog version number:               202107181"
echo "Database system identifier:           7123456789012345678"
echo "Database cluster state:               shut down"
echo "pg_control last modified:             $(date)"
echo "Latest checkpoint location:           0/1234567"
echo "Latest checkpoint's REDO location:    0/1234567"
echo "Latest checkpoint's REDO WAL file:    000000010000000000000001"
echo "Latest checkpoint's TimeLineID:       1"
echo "Latest checkpoint's PrevTimeLineID:   1"
echo "Data page checksum version:           1"
echo "Mock pg_controldata for testing."
exit 0
`)

	// Mock postgres
	MockBinary(t, binDir, "postgres", `
if [[ "$*" == *"--help"* ]]; then
    echo "postgres is the PostgreSQL database server."
    exit 0
fi
# postgres -C <guc> prints the configured value and exits (used by pgctld's
# Status to report the effective max_connections).
if [[ "$1" == "-C" ]]; then
    case "$2" in
        max_connections) echo "100" ;;
        superuser_reserved_connections) echo "3" ;;
        reserved_connections) echo "0" ;;
        *) echo "unknown GUC: $2" >&2; exit 1 ;;
    esac
    exit 0
fi
echo "Mock PostgreSQL server starting..."
# For testing, create a fake PID file
DATADIR=""
for arg in "$@"; do
    case $arg in
        -D)
            NEXT_IS_DATADIR=true
            ;;
        -D*)
            DATADIR=${arg#-D}
            ;;
        *)
            if [ "$NEXT_IS_DATADIR" = true ]; then
                DATADIR=$arg
                NEXT_IS_DATADIR=false
            fi
            ;;
    esac
done

if [ -n "$DATADIR" ]; then
    # Use a PID above the Linux pid_max ceiling for this parse-only mock.
    echo "4194305" > "$DATADIR/postmaster.pid"
    echo "$DATADIR" >> "$DATADIR/postmaster.pid"
    echo "$(date +%s)" >> "$DATADIR/postmaster.pid"
    echo "5432" >> "$DATADIR/postmaster.pid"
    echo "/tmp" >> "$DATADIR/postmaster.pid"
    echo "localhost" >> "$DATADIR/postmaster.pid"
    echo "*" >> "$DATADIR/postmaster.pid"
    echo "ready" >> "$DATADIR/postmaster.pid"
fi
`)

	// Mock pg_ctl
	MockBinary(t, binDir, "pg_ctl", `
# Only the dedicated mock PID file establishes ownership of a process.
stop_mock_processes() {
    if [ -f "$DATADIR/mock-postgres.pids" ]; then
        while read -r PID; do
            if [[ "$PID" =~ ^[0-9]+$ ]] && [ "$PID" -gt 1 ] && [ "$PID" -ne "$$" ] && [ "$PID" -ne "$PPID" ]; then
                kill "$PID" 2>/dev/null || true
            fi
        done < "$DATADIR/mock-postgres.pids"
        rm -f "$DATADIR/mock-postgres.pids"
    fi
}

case "$1" in
    "init" | "initdb")
        mkdir -p "$3/base"
        echo "15.0" > "$3/PG_VERSION"
        touch "$3/postgresql.conf"
        touch "$3/pg_hba.conf"
        echo "Success. You can now start the database server using:"
        echo "    pg_ctl start -D $3"
        ;;
    "start")
        DATADIR=""
        # Parse -D argument
        while [[ $# -gt 0 ]]; do
            case $1 in
                -D)
                    DATADIR="$2"
                    shift 2
                    ;;
                -D*)
                    DATADIR="${1#-D}"
                    shift
                    ;;
                *)
                    shift
                    ;;
            esac
        done
        
        if [ -n "$DATADIR" ]; then
            # Background a real process so PID-liveness checks see something
            # running; its identity is irrelevant since the "is postgres
            # actually running" check is now a pg_isready probe.
            sleep 3600 >/dev/null 2>&1 &
            MOCK_PID=$!
            echo "$MOCK_PID" >> "$DATADIR/mock-postgres.pids"
            echo "$MOCK_PID" > "$DATADIR/postmaster.pid"
            echo "$DATADIR" >> "$DATADIR/postmaster.pid"
            echo "$(date +%s)" >> "$DATADIR/postmaster.pid"
            echo "5432" >> "$DATADIR/postmaster.pid"
            echo "/tmp" >> "$DATADIR/postmaster.pid"
            echo "localhost" >> "$DATADIR/postmaster.pid"
            echo "*" >> "$DATADIR/postmaster.pid"
            echo "ready" >> "$DATADIR/postmaster.pid"
        fi
        echo "waiting for server to start.... done"
        echo "server started"
        ;;
    "restart")
        DATADIR=""
        # Parse -D argument
        while [[ $# -gt 0 ]]; do
            case $1 in
                -D)
                    DATADIR="$2"
                    shift 2
                    ;;
                -D*)
                    DATADIR="${1#-D}"
                    shift
                    ;;
                *)
                    shift
                    ;;
            esac
        done
        
        if [ -n "$DATADIR" ]; then
            stop_mock_processes
            rm -f "$DATADIR/postmaster.pid"
            echo "waiting for server to shut down.... done"
            echo "server stopped"

            # Start a new background process; see the "start" case above.
            sleep 3600 >/dev/null 2>&1 &
            MOCK_PID=$!
            echo "$MOCK_PID" >> "$DATADIR/mock-postgres.pids"
            echo "$MOCK_PID" > "$DATADIR/postmaster.pid"
            echo "$DATADIR" >> "$DATADIR/postmaster.pid"
            echo "$(date +%s)" >> "$DATADIR/postmaster.pid"
            echo "5432" >> "$DATADIR/postmaster.pid"
            echo "/tmp" >> "$DATADIR/postmaster.pid"
            echo "localhost" >> "$DATADIR/postmaster.pid"
            echo "*" >> "$DATADIR/postmaster.pid"
            echo "ready" >> "$DATADIR/postmaster.pid"
        fi
        echo "waiting for server to start.... done"
        echo "server started"
        ;;
    "stop")
        DATADIR=""
        # Parse -D argument
        while [[ $# -gt 0 ]]; do
            case $1 in
                -D)
                    DATADIR="$2"
                    shift 2
                    ;;
                -D*)
                    DATADIR="${1#-D}"
                    shift
                    ;;
                *)
                    shift
                    ;;
            esac
        done
        
        if [ -n "$DATADIR" ]; then
            stop_mock_processes
            rm -f "$DATADIR/postmaster.pid"
        fi
        echo "waiting for server to shut down.... done"
        echo "server stopped"
        ;;
    "reload")
        echo "server signaled"
        ;;
    "status")
        if [ -f "$3/postmaster.pid" ]; then
            echo "pg_ctl: server is running"
        else
            echo "pg_ctl: no server running"
        fi
        ;;
    *)
        echo "Unknown pg_ctl command: $1"
        exit 1
        ;;
esac
`)

	// Mock pg_isready
	MockBinary(t, binDir, "pg_isready", `
# Always succeed for testing - works with both socket and TCP
if [[ "$*" == *"-h /tmp"* ]] || [[ "$*" == *"pg_sockets"* ]]; then
    echo "socket connection - accepting connections"
else
    echo "localhost:5432 - accepting connections"
fi
exit 0
`)

	// Mock pg_rewind
	MockBinary(t, binDir, "pg_rewind", `
# Parse flags
DRY_RUN=false
for arg in "$@"; do
    case $arg in
        --dry-run)
            DRY_RUN=true
            ;;
    esac
done

if [ "$DRY_RUN" = "true" ]; then
    echo "servers diverged at WAL location 0/5000000 on timeline 1"
else
    echo "pg_rewind: done"
fi
exit 0
`)

	// Mock psql
	MockBinary(t, binDir, "psql", `
if [[ "$*" == *"SELECT version()"* ]]; then
    echo " PostgreSQL 15.0 on x86_64-pc-linux-gnu, compiled by gcc"
else
    echo "Mock psql output"
fi
`)
}
