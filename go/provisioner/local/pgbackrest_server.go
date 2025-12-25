// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/multigres/multigres/go/tools/telemetry"
)

// PgBackRestServerResult contains information about a started pgBackRest TLS server
type PgBackRestServerResult struct {
	PID     int
	Process *os.Process
	LogFile string
}

// StartPgBackRestServer starts a pgBackRest TLS server process.
// The server runs in the background and listens for TLS connections from other nodes.
func StartPgBackRestServer(ctx context.Context, configPath, logPath string) (*PgBackRestServerResult, error) {
	// Verify pgbackrest binary exists
	pgbackrestBinary, err := exec.LookPath("pgbackrest")
	if err != nil {
		return nil, fmt.Errorf("pgbackrest binary not found: %w", err)
	}

	// Build command arguments with built-in file logging
	args := []string{
		"--config=" + configPath,
		"--log-level-console=off",
		"--log-level-file=info",
		"--log-path=" + logPath,
		"server",
	}

	// Start pgbackrest server process
	cmd := exec.CommandContext(ctx, pgbackrestBinary, args...)

	if err := telemetry.StartCmd(ctx, cmd); err != nil {
		return nil, fmt.Errorf("failed to start pgbackrest server: %w", err)
	}

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Verify process is still running
	if cmd.Process == nil {
		return nil, fmt.Errorf("pgbackrest server process failed to start")
	}

	return &PgBackRestServerResult{
		PID:     cmd.Process.Pid,
		Process: cmd.Process,
		LogFile: logPath, // pgbackrest writes logs to this directory
	}, nil
}
