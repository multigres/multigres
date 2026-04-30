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
	"path/filepath"
	"strconv"
	"time"

	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/executil"

	"github.com/spf13/cobra"
)

// PostgreSQL server status values
const (
	statusStopped = "STOPPED"
	statusRunning = "RUNNING"
)

const (
	// pgIsReadyDefaultTimeout is the connection timeout passed to pg_isready when
	// no context deadline is present. pg_isready's built-in default is also 3s,
	// but we pass it explicitly so the value is visible and intentional.
	pgIsReadyDefaultTimeout = 3 * time.Second

	// pgIsReadyDeadlineBuffer is subtracted from the remaining context deadline
	// before passing it to pg_isready via -t. This ensures pg_isready's own
	// timeout fires before the context cancels the subprocess mid-wait, avoiding
	// a race between libpq's connection timeout and executil's SIGTERM.
	// Must be >= 1s because -t only accepts whole seconds and the sub-second
	// remainder is truncated, so a smaller buffer may provide no margin at all.
	pgIsReadyDeadlineBuffer = 1 * time.Second
)

// StatusResult contains the result of checking PostgreSQL status
type StatusResult struct {
	Status        string // statusStopped, statusRunning
	PID           int
	Version       string
	UptimeSeconds int64
	DataDir       string
	Port          int
	Host          string
	Ready         bool
	Message       string
}

// PgCtlStatusCmd holds the status command configuration
type PgCtlStatusCmd struct {
	pgCtlCmd *PgCtlCommand
}

// AddStatusCommand adds the status subcommand to the root command
func AddStatusCommand(root *cobra.Command, pc *PgCtlCommand) {
	statusCmd := &PgCtlStatusCmd{
		pgCtlCmd: pc,
	}
	root.AddCommand(statusCmd.createCommand())
}

func (s *PgCtlStatusCmd) createCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Check PostgreSQL server status",
		Long: `Check the status of the PostgreSQL server instance and report health information.

The status command checks if PostgreSQL is running and reports detailed information
including PID, version, uptime, and connection readiness. Configuration can be
provided via config file, environment variables, or CLI flags.
CLI flags take precedence over config file and environment variable settings.

Examples:
  # Check status with default settings
  pgctld status --pooler-dir /var/lib/poolerdir/

  # Check status of PostgreSQL on custom port
  pgctld status --pooler-dir/var/lib/poolerdir/

  # Check status with specific connection parameters
  pgctld status -d /var/lib/poolerdir/

  # Check status of multiple instances
  pgctld status -d /var/lib/poolerdir/instance1
  pgctld status -d /var/lib/poolerdir/instance2`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return s.pgCtlCmd.validateInitialized(cmd, args)
		},
		RunE: s.runStatus,
	}
}

// GetStatusWithResult gets PostgreSQL status with the given configuration and returns detailed result information
func GetStatusWithResult(ctx context.Context, logger *slog.Logger, config *pgctld.PostgresCtlConfig) (*StatusResult, error) {
	result := &StatusResult{
		DataDir: config.PostgresDataDir,
		Port:    config.Port,
	}

	// Check if PostgreSQL is running
	if !isPostgreSQLRunning(config) {
		result.Status = statusStopped
		result.Message = "PostgreSQL server is stopped"
		return result, nil
	}

	// Process exists — verify it is actually accepting connections.
	// A process that exists but cannot respond (e.g. SIGSTOP, cgroup freeze)
	// is treated as not running so that multipooler and multiorch can detect
	// the failure and trigger recovery rather than waiting indefinitely.
	if result.Ready = isServerReadyWithConfig(ctx, config); !result.Ready {
		result.Status = statusStopped
		result.Message = "PostgreSQL process exists but is not accepting connections"
		return result, nil
	}

	// Server is running and accepting connections
	result.Status = statusRunning
	result.Message = "PostgreSQL server is running"

	// Get PID if running
	if pid, err := readPostmasterPID(config.PostgresDataDir); err == nil {
		result.PID = pid
	} else {
		logger.WarnContext(ctx, "Could not read postmaster PID", "error", err)
	}

	// Get server version if possible
	result.Version = getServerVersionWithConfig(ctx, config)

	// Get uptime (approximate based on pidfile mtime)
	pidFile := filepath.Join(config.PostgresDataDir, "postmaster.pid")
	if stat, err := os.Stat(pidFile); err == nil {
		result.UptimeSeconds = int64(time.Since(stat.ModTime()).Seconds())
	}

	return result, nil
}

func (s *PgCtlStatusCmd) runStatus(cmd *cobra.Command, args []string) error {
	config, err := NewPostgresCtlConfigFromDefaults(s.pgCtlCmd.GetPoolerDir(), s.pgCtlCmd.pgPort.Get(), s.pgCtlCmd.pgListenAddresses.Get(), s.pgCtlCmd.pgUser.Get(), s.pgCtlCmd.pgDatabase.Get(), s.pgCtlCmd.timeout.Get())
	if err != nil {
		return err
	}
	// No local flag overrides needed - all flags are global now

	result, err := GetStatusWithResult(cmd.Context(), s.pgCtlCmd.lg.GetLogger(), config)
	if err != nil {
		return err
	}

	// Display status for CLI users
	var statusDisplay string
	switch result.Status {
	case statusStopped:
		statusDisplay = "Stopped"
	case statusRunning:
		statusDisplay = "Running"
	default:
		statusDisplay = result.Status
	}

	fmt.Printf("Status: %s\n", statusDisplay)
	fmt.Printf("Data directory: %s", result.DataDir)

	switch result.Status {
	case statusStopped:
		fmt.Printf("\n")
	case statusRunning:
		fmt.Printf("\n")
		if result.PID > 0 {
			fmt.Printf("PID: %d\n", result.PID)
		}
		fmt.Printf("Port: %d\n", result.Port)
		fmt.Printf("Host: %s\n", result.Host)
		if result.Version != "" {
			fmt.Printf("Version: %s\n", result.Version)
		}
		if result.UptimeSeconds > 0 {
			fmt.Printf("Uptime: %s\n", formatUptime(result.UptimeSeconds))
		}
		if result.Ready {
			fmt.Printf("Ready: Yes\n")
		} else {
			fmt.Printf("Ready: No (server may be starting or in recovery)\n")
		}
	}

	return nil
}

// formatUptime formats uptime seconds into human-readable format
func formatUptime(seconds int64) string {
	duration := time.Duration(seconds) * time.Second
	days := int(duration.Hours()) / 24
	hours := int(duration.Hours()) % 24
	minutes := int(duration.Minutes()) % 60

	if days > 0 {
		return fmt.Sprintf("%d days, %d hours, %d minutes", days, hours, minutes)
	} else if hours > 0 {
		return fmt.Sprintf("%d hours, %d minutes", hours, minutes)
	} else {
		return fmt.Sprintf("%d minutes", minutes)
	}
}

// pgIsReadyTimeoutSecs returns the value to pass to pg_isready's -t flag.
//
// pg_isready -t only accepts whole seconds. The timeout is derived from the
// context deadline so pg_isready's own connection timeout fires before the
// context cancels the subprocess mid-wait. Without this, the subprocess relies
// on libpq's default (3 s), which may race with the gRPC deadline propagated
// down from multiorch (5 s total, shared across two hops).
//
// pgIsReadyDeadlineBuffer is subtracted from the remaining deadline before
// truncating to whole seconds, ensuring the truncation cannot accidentally push
// the timeout above the remaining deadline.
func pgIsReadyTimeoutSecs(ctx context.Context) int {
	timeout := pgIsReadyDefaultTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline) - pgIsReadyDeadlineBuffer; remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}
	return max(1, int(timeout.Seconds()))
}

func isServerReadyWithConfig(ctx context.Context, config *pgctld.PostgresCtlConfig) bool {
	// Use Unix socket connection for pg_isready
	socketDir := pgctld.PostgresSocketDir(config.PoolerDir)

	timeoutSecs := pgIsReadyTimeoutSecs(ctx)

	return executil.Command(ctx, "pg_isready",
		"-h", socketDir,
		"-p", strconv.Itoa(config.Port), // Need port even for socket connections
		"-U", config.User,
		"-d", config.Database,
		"-t", strconv.Itoa(timeoutSecs),
	).WithClientSpan().Run() == nil
}

func getServerVersionWithConfig(ctx context.Context, config *pgctld.PostgresCtlConfig) string {
	// Use Unix socket connection for psql
	socketDir := pgctld.PostgresSocketDir(config.PoolerDir)

	output, err := executil.Command(ctx, "psql",
		"-h", socketDir,
		"-p", strconv.Itoa(config.Port), // Need port even for socket connections
		"-U", config.User,
		"-d", config.Database,
		"-t", "-c", "SELECT version()",
	).WithClientSpan().Output()
	if err != nil {
		return ""
	}

	return string(output)
}

func getServerUptime(dataDir string) string {
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	stat, err := os.Stat(pidFile)
	if err != nil {
		return ""
	}

	startTime := stat.ModTime()
	uptime := time.Since(startTime)

	// Format uptime in human-readable format
	days := int(uptime.Hours()) / 24
	hours := int(uptime.Hours()) % 24
	minutes := int(uptime.Minutes()) % 60

	if days > 0 {
		return fmt.Sprintf("%d days, %d hours, %d minutes", days, hours, minutes)
	} else if hours > 0 {
		return fmt.Sprintf("%d hours, %d minutes", hours, minutes)
	} else {
		return fmt.Sprintf("%d minutes", minutes)
	}
}
