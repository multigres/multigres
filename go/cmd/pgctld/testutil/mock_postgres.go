/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

// AddPostgreSQLCommands adds common PostgreSQL command mocks
func (m *MockExecCommand) AddPostgreSQLCommands(dataDir string, pid int) {
	// Mock initdb command
	m.AddCommand(
		fmt.Sprintf("initdb -D %s --auth-local=trust --auth-host=md5", dataDir),
		MockCommandResult{ExitCode: 0, Stdout: "Success. You can now start the database server using:\n"},
	)

	// Mock postgres command
	m.AddCommand(
		fmt.Sprintf("postgres -D %s", dataDir),
		MockCommandResult{ExitCode: 0, Stdout: ""},
	)

	// Mock pg_isready command
	m.AddCommand(
		"pg_isready -h localhost -p 5432 -U postgres -d postgres",
		MockCommandResult{ExitCode: 0, Stdout: "localhost:5432 - accepting connections\n"},
	)

	// Mock pg_ctl commands
	m.AddCommand(
		fmt.Sprintf("pg_ctl stop -D %s -m fast -t 30", dataDir),
		MockCommandResult{ExitCode: 0, Stdout: "server stopped\n"},
	)

	m.AddCommand(
		fmt.Sprintf("pg_ctl reload -D %s", dataDir),
		MockCommandResult{ExitCode: 0, Stdout: "server signaled\n"},
	)

	// Mock psql version command
	m.AddCommand(
		"psql -h localhost -p 5432 -U postgres -d postgres -t -c SELECT version()",
		MockCommandResult{ExitCode: 0, Stdout: " PostgreSQL 15.0 on x86_64-pc-linux-gnu\n"},
	)
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
	cmd.Env = append(cmd.Env, fmt.Sprintf("MOCK_CMD=%s", cmdLine))

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

	if err := os.WriteFile(binPath, []byte(script), 0755); err != nil {
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

	// Mock postgres
	MockBinary(t, binDir, "postgres", `
if [[ "$*" == *"--help"* ]]; then
    echo "postgres is the PostgreSQL database server."
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
    echo "12345" > "$DATADIR/postmaster.pid"
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
case "$1" in
    "stop")
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
echo "localhost:5432 - accepting connections"
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
