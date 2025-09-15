// Copyright 2025 The Supabase, Inc.
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
)

// StartResult contains the result of starting PostgreSQL
type StartResult struct {
	PID            int
	AlreadyRunning bool
	Message        string
}

// NewPostgresCtlConfigFromDefaults creates a PostgresCtlConfig by reading from existing postgresql.conf
func NewPostgresCtlConfigFromDefaults() (*pgctld.PostgresCtlConfig, error) {
	poolerDir := pgctld.GetPoolerDir()
	postgresConfigFile := pgctld.PostgresConfigFile(poolerDir)

	// Read existing port from postgresql.conf - file must exist
	existingConfig, err := pgctld.ReadPostgresServerConfig(&pgctld.PostgresServerConfig{
		Path: postgresConfigFile,
	}, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read existing postgresql.conf at %s: %w", postgresConfigFile, err)
	}

	effectivePort := existingConfig.Port

	// Handle password: use pgPassword flag or PGPASSWORD env var, error if both are set
	effectivePassword, err := resolvePassword()
	if err != nil {
		return nil, err
	}

	config, err := pgctld.NewPostgresCtlConfig(pgHost, effectivePort, pgUser, pgDatabase, effectivePassword, timeout, pgctld.PostgresDataDir(poolerDir), postgresConfigFile, poolerDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create config: %w", err)
	}
	return config, nil
}

// ResolvePassword handles password resolution from file or environment variable
// Returns error if both are set or if password file cannot be read
func resolvePassword() (string, error) {
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

func init() {
	Root.AddCommand(startCmd)
}

var startCmd = &cobra.Command{
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
	PreRunE: validateInitialized,
	RunE:    runStart,
}

func runStart(cmd *cobra.Command, args []string) error {
	config, err := NewPostgresCtlConfigFromDefaults()
	if err != nil {
		return err
	}

	result, err := StartPostgreSQLWithResult(config)
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
func StartPostgreSQLWithResult(config *pgctld.PostgresCtlConfig) (*StartResult, error) {
	logger := slog.Default()
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
	if err := startPostgreSQLWithConfig(config); err != nil {
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
func StartPostgreSQLWithConfig(config *pgctld.PostgresCtlConfig) error {
	result, err := StartPostgreSQLWithResult(config)
	if err != nil {
		return err
	}

	// For backward compatibility, log the message if provided
	if result.Message != "" && !result.AlreadyRunning {
		slog.Info(result.Message)
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

func startPostgreSQLWithConfig(config *pgctld.PostgresCtlConfig) error {
	// Use pg_ctl to start PostgreSQL properly as a daemon
	args := []string{
		"start",
		"-D", config.PostgresDataDir,
		"-o", fmt.Sprintf("-c config_file=%s", config.PostgresConfigFile),
		"-l", filepath.Join(config.PostgresDataDir, "postgresql.log"),
		"-W", // don't wait - we'll check readiness ourselves
	}

	slog.Info("Starting PostgreSQL with configuration", "port", config.Port, "dataDir", config.PostgresDataDir, "configFile", config.PostgresConfigFile)

	cmd := exec.Command("pg_ctl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start PostgreSQL with pg_ctl: %w", err)
	}

	// Wait for PostgreSQL to be ready using pg_isready
	return waitForPostgreSQLWithConfig(config)
}

func waitForPostgreSQLWithConfig(config *pgctld.PostgresCtlConfig) error {
	// Try to connect using pg_isready
	for i := 0; i < config.Timeout; i++ {
		cmd := exec.Command("pg_isready",
			"-h", config.Host,
			"-p", fmt.Sprintf("%d", config.Port),
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
