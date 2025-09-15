// Copyright 2025 The Multigres Authors.
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

	"github.com/multigres/multigres/go/pgctld"

	"github.com/spf13/cobra"
)

// StopResult contains the result of stopping PostgreSQL
type StopResult struct {
	WasRunning bool
	Message    string
}

func init() {
	Root.AddCommand(stopCmd)
	stopCmd.Flags().String("mode", "fast", "Shutdown mode: smart, fast, or immediate")
}

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop PostgreSQL server",
	Long: `Stop a running PostgreSQL server instance.

The stop command gracefully shuts down a running PostgreSQL server using pg_ctl.
Configuration can be provided via config file, environment variables, or CLI flags.
CLI flags take precedence over config file and environment variable settings.

Shutdown modes:
  smart:     Disallow new connections, wait for existing sessions to finish
  fast:      Disallow new connections, terminate existing sessions
  immediate: Force shutdown without cleanup (may cause recovery on restart)

Examples:
  # Stop with default settings (fast mode)
  pgctld stop --pg-data-dir /var/lib/postgresql/data

  # Stop with smart mode (wait for sessions)
  pgctld stop --pg-data-dir /var/lib/postgresql/data --mode smart

  # Stop with custom timeout
  pgctld stop --pg-data-dir /var/lib/postgresql/data --timeout 120

  # Force immediate stop with short timeout
  pgctld stop --pg-data-dir /var/lib/postgresql/data --mode immediate --timeout 10`,
	PreRunE: validateInitialized,
	RunE:    runStop,
}

func runStop(cmd *cobra.Command, args []string) error {
	config, err := NewPostgresCtlConfigFromDefaults()
	if err != nil {
		return err
	}

	mode, _ := cmd.Flags().GetString("mode")

	// No local flag overrides needed - all flags are global now

	result, err := StopPostgreSQLWithResult(config, mode)
	if err != nil {
		return err
	}

	// Display appropriate message for CLI users
	if result.WasRunning {
		fmt.Printf("PostgreSQL server stopped successfully (mode: %s)\n", mode)
	} else {
		fmt.Println("PostgreSQL is not running")
	}

	return nil
}

// StopPostgreSQLWithResult stops PostgreSQL with the given configuration and returns detailed result information
func StopPostgreSQLWithResult(config *pgctld.PostgresCtlConfig, mode string) (*StopResult, error) {
	logger := slog.Default()
	result := &StopResult{}

	// Default mode to "fast" if not specified
	if mode == "" {
		mode = "fast"
	}

	// Check if PostgreSQL is running
	if !isPostgreSQLRunning(config.PostgresDataDir) {
		logger.Info("PostgreSQL is not running")
		result.WasRunning = false
		result.Message = "PostgreSQL is not running"
		return result, nil
	}

	result.WasRunning = true
	logger.Info("Stopping PostgreSQL server", "data_dir", config.PostgresDataDir, "mode", mode)

	if err := stopPostgreSQLWithConfig(config, mode); err != nil {
		return nil, fmt.Errorf("failed to stop PostgreSQL: %w", err)
	}

	result.Message = "PostgreSQL server stopped successfully"
	logger.Info("PostgreSQL server stopped successfully")
	return result, nil
}

// StopPostgreSQLWithConfig stops PostgreSQL with the given configuration and mode
func StopPostgreSQLWithConfig(config *pgctld.PostgresCtlConfig, mode string) error {
	result, err := StopPostgreSQLWithResult(config, mode)
	if err != nil {
		return err
	}

	// For backward compatibility, log the message if PostgreSQL was actually stopped
	if result.WasRunning && result.Message != "" {
		slog.Info(result.Message)
	}

	return nil
}

func stopPostgreSQLWithConfig(config *pgctld.PostgresCtlConfig, mode string) error {
	// First try using pg_ctl
	if err := stopWithPgCtlWithConfig(config, mode); err != nil {
		slog.Error("pg_ctl stop failed,", "error", err)
		return err
	}
	return nil
}

func stopWithPgCtlWithConfig(config *pgctld.PostgresCtlConfig, mode string) error {
	// Take a checkpoint before stopping PostgreSQL for clean shutdown
	if err := takeCheckpoint(config); err != nil {
		slog.Warn("Failed to take checkpoint before stop", "error", err, "data_dir", config.PostgresDataDir)
		// Continue with stop even if checkpoint fails - it's not critical
	}

	args := []string{
		"stop",
		"-D", config.PostgresDataDir,
		"-m", mode,
		"-t", fmt.Sprintf("%d", config.Timeout),
	}

	cmd := exec.Command("pg_ctl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// takeCheckpoint executes a CHECKPOINT command to ensure all data is written to disk before shutdown
func takeCheckpoint(config *pgctld.PostgresCtlConfig) error {
	slog.Info("Taking checkpoint before stopping PostgreSQL", "data_dir", config.PostgresDataDir)

	// Use Unix socket connection for psql
	socketDir := pgctld.PostgresSocketDir(config.PoolerDir)
	args := []string{
		"-h", socketDir,
		"-p", fmt.Sprintf("%d", config.Port), // Need port even for socket connections
		"-U", config.User,
		"-d", config.Database,
		"-c", "CHECKPOINT;",
		"-q", // quiet mode - suppress messages
	}

	cmd := exec.Command("psql", args...)

	// Capture output to avoid cluttering the terminal
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("checkpoint command failed: %w, output: %s", err, string(output))
	}

	slog.Info("Checkpoint completed successfully", "data_dir", config.PostgresDataDir)
	return nil
}
