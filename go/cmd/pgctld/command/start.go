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

	"github.com/multigres/multigres/go/pgctld"
	"github.com/multigres/multigres/go/servenv"
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

// ResolvePassword handles password resolution from file or environment variable
// Returns error if both are set or if password file cannot be read
func resolvePassword(pgPwfile string) (string, error) {
	envPassword := os.Getenv("PGPASSWORD")
	var filePassword string

	// Read password from file if specified
	if pgPwfile != "" {
		filePasswordBytes, readErr := os.ReadFile(pgPwfile)
		if readErr != nil {
			return "", fmt.Errorf("failed to read password file %s: %w", pgPwfile, readErr)
		}
		// Remove trailing newline if present
		filePassword = strings.TrimRight(string(filePasswordBytes), "\n\r")
	}

	// Check if both file and environment variable are set
	if pgPwfile != "" && envPassword != "" {
		return "", fmt.Errorf("both --pg-pwfile flag and PGPASSWORD environment variable are set, please use only one")
	}

	// Use file password if set, otherwise use environment variable
	if pgPwfile != "" {
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

	result, err := StartPostgreSQLWithResult(s.pgCtlCmd.lg.GetLogger(), config)
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
func StartPostgreSQLWithResult(logger *slog.Logger, config *pgctld.PostgresCtlConfig) (*StartResult, error) {
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

	// Start PostgreSQL
	logger.Info("Starting PostgreSQL server", "data_dir", config.PostgresDataDir)
	if err := startPostgreSQLWithConfig(logger, config); err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	// Wait for server to be ready
	logger.Info("Waiting for PostgreSQL to be ready")
	if err := waitForPostgreSQLWithConfig(config); err != nil {
		return nil, fmt.Errorf("PostgreSQL failed to become ready: %w", err)
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
	result, err := StartPostgreSQLWithResult(logger, config)
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
		// Environment variables automatically inherit
		if err := watchdogCmd.Start(); err != nil {
			logger.Warn("Failed to start watchdog process", "error", err)
			// Don't fail the start operation if watchdog fails to start
		} else {
			logger.Info("Watchdog process started", "pid", watchdogCmd.Process.Pid)
		}
	}

	// Wait for PostgreSQL to be ready using pg_isready
	return waitForPostgreSQLWithConfig(config)
}

func waitForPostgreSQLWithConfig(config *pgctld.PostgresCtlConfig) error {
	// Try to connect using pg_isready
	socketDir := pgctld.PostgresSocketDir(config.PoolerDir)
	for i := 0; i < config.Timeout; i++ {
		cmd := exec.Command("pg_isready",
			"-h", socketDir,
			"-p", fmt.Sprintf("%d", config.Port), // Need port even for socket connections
			"-U", config.User,
			"-d", config.Database,
		)

		if err := cmd.Run(); err == nil {
			return nil
		}

		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("PostgreSQL did not become ready within %d seconds", config.Timeout)
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
		return 0, fmt.Errorf("empty postmaster.pid file")
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

	// On Unix, sending signal 0 checks if process exists
	err = process.Signal(syscall.Signal(0))
	return err == nil
}
