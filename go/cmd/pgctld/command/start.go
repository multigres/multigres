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

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/services/pgctld"
)

// StartResult contains the result of starting PostgreSQL
type StartResult struct {
	PID            int
	AlreadyRunning bool
	Message        string
}

// NewPostgresCtlConfigFromDefaults creates a PostgresCtlConfig using command-line parameters
// Port, listen_addresses, and unix_socket_directories come from CLI flags, not from the config file
func NewPostgresCtlConfigFromDefaults(poolerDir string, pgPort int, pgListenAddresses string, pgUser string, pgDatabase string, timeout int) (*pgctld.PostgresCtlConfig, error) {
	postgresConfigFile := pgctld.PostgresConfigFile(poolerDir)

	effectivePort := pgPort
	effectiveListenAddresses := pgListenAddresses
	effectiveUnixSocketDirectories := pgctld.PostgresSocketDir(poolerDir)

	config, err := pgctld.NewPostgresCtlConfig(effectivePort, pgUser, pgDatabase, timeout, pgctld.PostgresDataDir(poolerDir), postgresConfigFile, poolerDir, effectiveListenAddresses, effectiveUnixSocketDirectories)
	if err != nil {
		return nil, fmt.Errorf("failed to create config: %w", err)
	}
	return config, nil
}

// resolvePassword handles password resolution from file or environment variable.
// It checks for a password file at the conventional location (poolerDir/pgpassword.txt).
// If the file exists, it reads the password from it. Otherwise, it falls back to PGPASSWORD env var.
// Returns error if both password file and PGPASSWORD are set.
func resolvePassword(poolerDir string) (string, error) {
	envPassword := os.Getenv("PGPASSWORD")
	var filePassword string
	var fileExists bool

	// Check for password file at conventional location
	pwfile := filepath.Join(poolerDir, "pgpassword.txt")
	if filePasswordBytes, readErr := os.ReadFile(pwfile); readErr == nil {
		// Remove trailing newline if present
		filePassword = strings.TrimRight(string(filePasswordBytes), "\n\r")
		fileExists = true
	}

	// Check if both file and environment variable are set
	if fileExists && envPassword != "" {
		return "", fmt.Errorf("both password file (%s) and PGPASSWORD environment variable are set, please use only one", pwfile)
	}

	// Use file password if set, otherwise use environment variable
	if fileExists {
		return filePassword, nil
	}

	return envPassword, nil
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

	result, err := StartPostgreSQLWithResult(s.pgCtlCmd.lg.GetLogger(), config, false)
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
func StartPostgreSQLWithResult(logger *slog.Logger, config *pgctld.PostgresCtlConfig, skipWait bool) (*StartResult, error) {
	result := &StartResult{}

	// Check if PostgreSQL is already running
	if isPostgreSQLRunning(config.PostgresDataDir) {
		logger.Info("PostgreSQL is already running")
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
		logger.Info("Ensured Unix socket directory exists", "socket_dir", config.UnixSocketDirectories)
	}

	// Start PostgreSQL
	logger.Info("Starting PostgreSQL server", "data_dir", config.PostgresDataDir)
	if err := startPostgreSQLWithConfig(logger, config); err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	// Wait for server to be ready (unless skip_wait is true)
	if skipWait {
		logger.Info("Skipping wait for PostgreSQL to be ready (skip_wait=true)")
	} else {
		logger.Info("Waiting for PostgreSQL to be ready")
		if err := waitForPostgreSQLWithConfig(logger, config); err != nil {
			return nil, fmt.Errorf("PostgreSQL failed to become ready: %w", err)
		}
	}

	// Get PID of started instance
	if pid, err := readPostmasterPID(config.PostgresDataDir); err == nil {
		result.PID = pid
	}

	result.Message = "PostgreSQL server started successfully"
	logger.Info("PostgreSQL server started successfully")
	return result, nil
}

// StartPostgreSQLWithConfig starts PostgreSQL with the given configuration
func StartPostgreSQLWithConfig(logger *slog.Logger, config *pgctld.PostgresCtlConfig) error {
	result, err := StartPostgreSQLWithResult(logger, config, false)
	if err != nil {
		return err
	}

	// For backward compatibility, log the message if provided
	if result.Message != "" && !result.AlreadyRunning {
		logger.Info(result.Message)
	}

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

func startPostgreSQLWithConfig(logger *slog.Logger, config *pgctld.PostgresCtlConfig) error {
	// Use pg_ctl to start PostgreSQL properly as a daemon
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

	logger.Info("Starting PostgreSQL with configuration", "port", config.Port, "dataDir", config.PostgresDataDir, "configFile", config.PostgresConfigFile)

	cmd := exec.Command("pg_ctl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start PostgreSQL with pg_ctl: %w", err)
	}

	// If orphan detection environment variables are set, spawn a watchdog process
	// that will stop postgres if the test parent dies or testdata dir is deleted
	if servenv.IsTestOrphanDetectionEnabled() {
		logger.Info("Spawning watchdog process for orphan detection")
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
			logger.Warn("Failed to start watchdog process", "error", err)
			// Don't fail the start operation if watchdog fails to start
		} else {
			logger.Info("Watchdog process started", "pid", watchdogCmd.Process.Pid)
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
			logger.Error("PostgreSQL startup timeout",
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
					logger.Error("PostgreSQL process not running during startup",
						"attempt", attempt,
						"error", err,
						"postgresql_log_tail", logTail,
					)
					return fmt.Errorf("PostgreSQL process not running: %w (check postgresql.log)", err)
				}

				if !isProcessRunning(pid) {
					// PID file exists but process is gone - crashed
					logTail := readLogTail(logPath, 20)
					logger.Error("PostgreSQL process crashed during startup",
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
				logger.Info("PostgreSQL is ready", "attempts", attempt)
				return nil
			}

			// Log progress every 5 seconds
			if attempt > 0 && attempt%5 == 0 {
				logger.Info("Still waiting for PostgreSQL to be ready",
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
