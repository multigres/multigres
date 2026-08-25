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

package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/retry"
)

// StartResult contains the result of starting PostgreSQL
type StartResult struct {
	PID            int
	AlreadyRunning bool
	Message        string
}

// NewPostgresCtlConfigFromDefaults creates a PostgresCtlConfig using
// command-line parameters. Port, listen_addresses, and
// unix_socket_directories come from CLI flags, not from the config file.
//
// Password is intentionally left unset — callers that need it (start, stop)
// resolve it via PgCtlCommand.GetPostgresPassword so the file/env precedence
// stays in one place.
func NewPostgresCtlConfigFromDefaults(poolerDir string, pgPort int, pgListenAddresses string, pgUser string, pgDatabase string, timeout int) (*pgctld.PostgresCtlConfig, error) {
	postgresConfigFile := pgctld.PostgresConfigFile()

	effectivePort := pgPort
	effectiveListenAddresses := pgListenAddresses
	effectiveUnixSocketDirectories := pgctld.PostgresSocketDir(poolerDir)

	config, err := pgctld.NewPostgresCtlConfig(effectivePort, pgUser, pgDatabase, timeout, pgctld.PostgresDataDir(), postgresConfigFile, poolerDir, effectiveListenAddresses, effectiveUnixSocketDirectories)
	if err != nil {
		return nil, fmt.Errorf("failed to create config: %w", err)
	}
	return config, nil
}

// AddStartCommand adds the start subcommand to the root command
func AddStartCommand(root *cobra.Command, pc *PgCtlCommand) {
	startCmd := &PgCtlStartCmd{
		pgCtlCmd: pc,
	}
	root.AddCommand(startCmd.createCommand())
}

// PgCtlStartCmd holds the start command configuration
type PgCtlStartCmd struct {
	pgCtlCmd *PgCtlCommand
}

func (s *PgCtlStartCmd) createCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start PostgreSQL server",
		Long: `Start a PostgreSQL server instance with the configured parameters.

The start command initializes the data directory if needed and starts PostgreSQL.
Configuration can be provided via config file, environment variables, or CLI flags.
CLI flags take precedence over config file and environment variable settings.

Examples:
  # Start with default settings
  pgctld start --pooler-dir /var/lib/postgresql/data

  # Start on custom port
  pgctld start --pooler-dir /var/lib/postgresql/data --port 5433

  # Start with custom socket directory and config file
  pgctld start --pooler-dir /var/lib/postgresql/data -s /var/run/postgresql -c /etc/postgresql/custom.conf`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return s.pgCtlCmd.validateInitialized(cmd, args)
		},
		RunE: s.runStart,
	}

	return cmd
}

func (s *PgCtlStartCmd) runStart(cmd *cobra.Command, args []string) error {
	config, err := NewPostgresCtlConfigFromDefaults(s.pgCtlCmd.GetPoolerDir(), s.pgCtlCmd.pgPort.Get(), s.pgCtlCmd.pgListenAddresses.Get(), s.pgCtlCmd.pgUser.Get(), s.pgCtlCmd.pgDatabase.Get(), s.pgCtlCmd.timeout.Get())
	if err != nil {
		return err
	}
	password, _, _, err := s.pgCtlCmd.GetPostgresPassword()
	if err != nil {
		return err
	}
	config.Password = password

	svc := &PgCtldService{logger: s.pgCtlCmd.lg.GetLogger(), pgConfig: config}
	result, err := svc.StartPostgreSQLWithResult(cmd.Context())
	if err != nil {
		return err
	}

	// Display appropriate message for CLI users
	if result.AlreadyRunning {
		fmt.Printf("PostgreSQL is already running (PID: %d)\n", result.PID)
	} else {
		fmt.Printf("PostgreSQL server started successfully (PID: %d)\n", result.PID)
	}

	return nil
}

// StartPostgreSQLWithResult starts PostgreSQL with the given configuration and returns detailed result information
func (s *PgCtldService) StartPostgreSQLWithResult(ctx context.Context) (*StartResult, error) {
	result := &StartResult{}
	logger := s.logger
	config := s.pgConfig

	// Check if PostgreSQL is already running
	if isPostgreSQLRunning(config.PostgresDataDir) {
		logger.Info("Postgres is already running") //nolint:sloglint // message intentionally starts with an operation name or proper noun
		result.AlreadyRunning = true
		result.Message = "PostgreSQL is already running"

		// Get PID of running instance
		if pid, err := readPostmasterPID(config.PostgresDataDir); err == nil {
			result.PID = pid
		}

		return result, nil
	}

	// Ensure Unix socket directory exists before starting PostgreSQL
	// This is necessary for restarts after restores, where pgBackRest only restores pg_data
	// but not external directories like pg_sockets
	if config.UnixSocketDirectories != "" {
		if err := os.MkdirAll(config.UnixSocketDirectories, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create Unix socket directory %s: %w", config.UnixSocketDirectories, err)
		}
		logger.InfoContext(ctx, "ensured Unix socket directory exists", "socket_dir", config.UnixSocketDirectories)
	}

	// Enforce PGDATA permission invariant before pg_ctl start
	if err := ensurePGDATAPermissions(logger, config.PostgresDataDir); err != nil {
		return nil, fmt.Errorf("PGDATA permission check failed: %w", err)
	}

	// Start PostgreSQL
	logger.InfoContext(ctx, "starting Postgres server", "data_dir", config.PostgresDataDir)
	if err := s.startPostgreSQLWithConfig(ctx); err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	// Wait for server to be ready
	logger.InfoContext(ctx, "waiting for Postgres to be ready")
	if err := waitForPostgreSQLWithConfig(logger, config); err != nil {
		return nil, fmt.Errorf("PostgreSQL failed to become ready: %w", err)
	}

	// Get PID of started instance
	if pid, err := readPostmasterPID(config.PostgresDataDir); err == nil {
		result.PID = pid
	}

	result.Message = "PostgreSQL server started successfully"
	logger.Info("Postgres server started successfully") //nolint:sloglint // message intentionally starts with an operation name or proper noun
	return result, nil
}

// StartPostgreSQLWithConfig starts PostgreSQL with the given configuration
func (s *PgCtldService) StartPostgreSQLWithConfig(ctx context.Context) error {
	logger := s.logger
	result, err := s.StartPostgreSQLWithResult(ctx)
	if err != nil {
		return err
	}

	// For backward compatibility, log the message if provided
	if result.Message != "" && !result.AlreadyRunning {
		logger.InfoContext(ctx, result.Message)
	}

	return nil
}

// ensurePGDATAPermissions ensures PGDATA is owned by the effective UID and set to 0700 before pg_ctl start.
// initdb sets this on bootstrap, but restore, rewind, or volume remounts may change it.
func ensurePGDATAPermissions(logger *slog.Logger, dataDir string) error {
	info, err := os.Stat(dataDir)
	if err != nil {
		return fmt.Errorf("failed to stat PGDATA %s: %w", dataDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("PGDATA %s is not a directory", dataDir)
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("failed to get syscall stat for PGDATA %s", dataDir)
	}

	currentUID := uint32(os.Geteuid())
	if stat.Uid != currentUID {
		return fmt.Errorf("PGDATA %s owned by UID %d, expected %d, refusing to start (ownership mismatch is a configuration error)",
			dataDir, stat.Uid, currentUID)
	}

	if info.Mode().Perm() == 0o700 && (info.Mode()&os.ModeSetgid) == 0 {
		return nil
	}

	oldMode := fmt.Sprintf("%04o", stat.Mode&0o7777)
	if err := os.Chmod(dataDir, 0o700); err != nil {
		return fmt.Errorf("failed to chmod PGDATA %s to 0700: %w", dataDir, err)
	}

	logger.Debug("normalized PGDATA permissions",
		"path", dataDir,
		"old_mode", oldMode,
		"new_mode", "0700",
	)

	return nil
}

func isPostgreSQLRunning(dataDir string) bool {
	// Check if postmaster.pid file exists and process is running
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	if _, err := os.Stat(pidFile); err != nil {
		return false
	}

	// Read PID from file and check if process is actually running
	pid, err := readPostmasterPID(dataDir)
	if err != nil {
		return false
	}

	return isProcessRunning(pid)
}

// runPgCtlStart runs `pg_ctl start` exactly once and returns its combined
// output alongside any error, so callers can both retry on a recognized
// transient failure and preserve the output for logging either way.
func (s *PgCtldService) runPgCtlStart(ctx context.Context) ([]byte, error) {
	config := s.pgConfig
	// Pass port, listen_addresses, and unix_socket_directories as command-line parameters for portability
	postgresOpts := fmt.Sprintf("-c config_file=%s -c port=%d -c listen_addresses=%s -c unix_socket_directories=%s",
		config.PostgresConfigFile, config.Port, config.ListenAddresses, config.UnixSocketDirectories)

	args := []string{
		"start",
		"-D", config.PostgresDataDir,
		"-o", postgresOpts,
		"-l", filepath.Join(config.PostgresDataDir, "postgresql.log"),
		"-W", // don't wait - we'll check readiness ourselves
	}

	cmd := executil.Command(ctx, "pg_ctl", args...)
	return cmd.CombinedOutput()
}

func (s *PgCtldService) startPostgreSQLWithConfig(ctx context.Context) error {
	logger, config := s.logger, s.pgConfig
	logger.InfoContext(ctx, "starting Postgres with configuration", "port", config.Port, "data_dir", config.PostgresDataDir, "config_file", config.PostgresConfigFile)

	// retryWhileLockHeld (crash_recovery.go) absorbs the same orphan-cleanup
	// lock window that single-user crash recovery already retries against;
	// a plain start has no special-case interpretation for it, so a lock
	// still held once the retry budget is exhausted is a genuine failure.
	r := retry.New(constants.OrphanCleanupRetryDelay, constants.OrphanCleanupRetryDelay)
	output, err := s.retryWhileLockHeld(ctx, s.runPgCtlStart, constants.OrphanCleanupMaxAttempts, r)

	// Preserve pg_ctl's output in the container's own log stream, matching the
	// previous behavior of streaming directly to os.Stdout/os.Stderr.
	_, _ = os.Stdout.Write(output)

	if err != nil {
		return fmt.Errorf("failed to start PostgreSQL with pg_ctl: %w", err)
	}

	// If orphan detection environment variables are set, spawn a watchdog process
	// that will stop postgres if the test parent dies or testdata dir is deleted
	if servenv.IsTestOrphanDetectionEnabled() {
		logger.InfoContext(ctx, "spawning watchdog process for orphan detection")
		watchdogCmd := exec.Command(
			"run_command_if_parent_dies.sh",
			"pg_ctl", "stop",
			"-D", config.PostgresDataDir,
			"-m", "fast",
		)
		// Put watchdog in its own process group so SIGINT/SIGTERM to parent doesn't kill it
		// The watchdog needs to survive the parent's death to perform cleanup
		watchdogCmd.SysProcAttr = &syscall.SysProcAttr{
			Setpgid: true,
			Pgid:    0,
		}
		// Environment variables automatically inherit
		if err := watchdogCmd.Start(); err != nil {
			logger.WarnContext(ctx, "failed to start watchdog process", "error", err)
			// Don't fail the start operation if watchdog fails to start
		} else {
			logger.InfoContext(ctx, "watchdog process started", "pid", watchdogCmd.Process.Pid)
		}
	}

	return nil
}

// readLogTail reads the last N lines from the PostgreSQL log file for diagnostics
func readLogTail(logPath string, lines int) string {
	content, err := os.ReadFile(logPath)
	if err != nil {
		return fmt.Sprintf("(failed to read log: %v)", err)
	}

	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		return "(empty log file)"
	}

	allLines := strings.Split(trimmed, "\n")
	if len(allLines) <= lines {
		return trimmed
	}

	return strings.Join(allLines[len(allLines)-lines:], "\n")
}

func waitForPostgreSQLWithConfig(logger *slog.Logger, config *pgctld.PostgresCtlConfig) error {
	socketDir := pgctld.PostgresSocketDir(config.PoolerDir)
	logPath := filepath.Join(config.PostgresDataDir, "postgresql.log")
	var lastOutput string

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeout := time.After(time.Duration(config.Timeout) * time.Second)
	attempt := 0

	for {
		select {
		case <-timeout:
			// On timeout, include diagnostic information
			logTail := readLogTail(logPath, 20)
			logger.Error("Postgres startup timeout", //nolint:sloglint // message intentionally starts with an operation name or proper noun
				"timeout_seconds", config.Timeout,
				"attempts", attempt,
				"last_pg_isready_output", lastOutput,
				"postgresql_log_tail", logTail,
			)
			return fmt.Errorf("PostgreSQL did not become ready within %d seconds (pg_isready: %s)",
				config.Timeout, lastOutput)

		case <-ticker.C:
			attempt++

			// Check if PostgreSQL process is still running (after first second)
			if attempt > 1 {
				pid, err := readPostmasterPID(config.PostgresDataDir)
				if err != nil {
					// No PID file means PostgreSQL never started or crashed immediately
					logTail := readLogTail(logPath, 20)
					logger.Error("Postgres process not running during startup", //nolint:sloglint // message intentionally starts with an operation name or proper noun
						"attempt", attempt,
						"error", err,
						"postgresql_log_tail", logTail,
					)
					return fmt.Errorf("PostgreSQL process not running: %w (check postgresql.log)", err)
				}

				if !isProcessRunning(pid) {
					// PID file exists but process is gone - crashed
					logTail := readLogTail(logPath, 20)
					logger.Error("Postgres process crashed during startup", //nolint:sloglint // message intentionally starts with an operation name or proper noun
						"pid", pid,
						"attempt", attempt,
						"postgresql_log_tail", logTail,
					)
					return fmt.Errorf("PostgreSQL process (PID %d) crashed during startup (check postgresql.log)", pid)
				}
			}

			cmd := exec.Command("pg_isready",
				"-h", socketDir,
				"-p", strconv.Itoa(config.Port),
				"-U", config.User,
				"-d", config.Database,
			)

			output, err := cmd.CombinedOutput()
			lastOutput = strings.TrimSpace(string(output))
			if err == nil {
				logger.Info("Postgres is ready", "attempts", attempt) //nolint:sloglint // message intentionally starts with an operation name or proper noun
				return nil
			}

			// Log progress every 5 seconds
			if attempt > 0 && attempt%5 == 0 {
				logger.Info("still waiting for Postgres to be ready",
					"attempt", attempt,
					"timeout", config.Timeout,
					"pg_isready_output", lastOutput,
				)
			}
		}
	}
}

func readPostmasterPID(dataDir string) (int, error) {
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	content, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, err
	}

	// First line contains the PID
	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return 0, errors.New("empty postmaster.pid file")
	}

	pid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil {
		return 0, fmt.Errorf("invalid PID in postmaster.pid: %s", lines[0])
	}

	return pid, nil
}

func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	// On Unix, sending signal 0 checks if process exists without actually sending a signal.
	// This is the standard way to check if a process is running.
	err = process.Signal(syscall.Signal(0))
	return err == nil
}
