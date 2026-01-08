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

package command

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
)

type PgRewindResult struct {
	// Status message
	Message string
	Output  string
}

func PgRewindWithResult(ctx context.Context, logger *slog.Logger, poolerDir, sourceServer, password string, dryRun bool, extraArgs []string) (*PgRewindResult, error) {
	result := &PgRewindResult{}

	args := []string{
		"--source-server", sourceServer,
		"--target-pgdata", fmt.Sprintf("%s/pg_data", poolerDir),
	}
	if dryRun {
		args = append(args, "--dry-run")
	}
	args = append(args, extraArgs...)

	logger.Info("executing pg_rewind command",
		"command", "pg_rewind",
		"args", args,
		"source_server", sourceServer,
		"target_pgdata", fmt.Sprintf("%s/pg_data", poolerDir),
		"dry_run", dryRun)

	cmd := exec.CommandContext(ctx, "pg_rewind", args...)

	// Set PGPASSWORD environment variable for pg_rewind to use
	// pg_rewind doesn't reliably use passwords from connection strings
	if password != "" {
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", password))
	}

	// Capture both Stdout and Stderr
	output, err := cmd.CombinedOutput()
	result.Output = string(output)
	if err != nil {
		result.Message = "Rewind failed"
		logger.Error("pg_rewind command failed",
			"error", err,
			"output", string(output))
		return result, fmt.Errorf("pg_rewind failed: %w", err)
	}

	result.Message = "Rewind completed successfully"
	logger.Info("pg_rewind command completed successfully",
		"output", string(output))
	return result, nil
}
