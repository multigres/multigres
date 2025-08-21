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

package command

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
)

func init() {
	Root.AddCommand(stopCmd)
	stopCmd.Flags().String("mode", "fast", "Shutdown mode: smart, fast, or immediate")
}

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop PostgreSQL server",
	Long: `Stop a running PostgreSQL server instance.

Shutdown modes:
  smart:     Disallow new connections, wait for existing sessions to finish
  fast:      Disallow new connections, terminate existing sessions
  immediate: Force shutdown without cleanup (may cause recovery on restart)`,
	RunE: runStop,
}

func runStop(cmd *cobra.Command, args []string) error {
	config := NewPostgresConfigFromViper()
	mode, _ := cmd.Flags().GetString("mode")
	return StopPostgreSQLWithConfig(config, mode)
}

// StopPostgreSQLWithConfig stops PostgreSQL with the given configuration and mode
func StopPostgreSQLWithConfig(config *PostgresConfig, mode string) error {
	logger := slog.Default()

	if config.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}

	// Check if PostgreSQL is running
	if !isPostgreSQLRunning(config.DataDir) {
		logger.Info("PostgreSQL is not running")
		return nil
	}

	logger.Info("Stopping PostgreSQL server", "data_dir", config.DataDir, "mode", mode)

	if err := stopPostgreSQLWithConfig(config, mode); err != nil {
		return fmt.Errorf("failed to stop PostgreSQL: %w", err)
	}

	logger.Info("PostgreSQL server stopped successfully")
	return nil
}

func stopPostgreSQL(dataDir, mode string, timeout int) error {
	// Legacy function - use defaults for other config
	config := NewPostgresConfigFromDefaults()
	config.DataDir = dataDir
	config.Timeout = timeout
	return stopPostgreSQLWithConfig(config, mode)
}

func stopPostgreSQLWithConfig(config *PostgresConfig, mode string) error {
	// First try using pg_ctl
	if err := stopWithPgCtlWithConfig(config, mode); err != nil {
		slog.Error("pg_ctl stop failed,", "error", err)
		return err
	}
	return nil
}

func stopWithPgCtl(dataDir, mode string, timeout int) error {
	// Legacy function - use defaults
	config := NewPostgresConfigFromDefaults()
	config.DataDir = dataDir
	config.Timeout = timeout
	return stopWithPgCtlWithConfig(config, mode)
}

func stopWithPgCtlWithConfig(config *PostgresConfig, mode string) error {
	args := []string{
		"stop",
		"-D", config.DataDir,
		"-m", mode,
		"-t", fmt.Sprintf("%d", config.Timeout),
	}

	cmd := exec.Command("pg_ctl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}
