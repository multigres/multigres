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

	"github.com/spf13/cobra"
)

// RestartResult contains the result of restarting PostgreSQL
type RestartResult struct {
	PID          int
	StoppedFirst bool
	Message      string
}

func init() {
	Root.AddCommand(restartCmd)
	restartCmd.Flags().String("mode", "fast", "Shutdown mode for stop phase: smart, fast, or immediate")
}

var restartCmd = &cobra.Command{
	Use:   "restart",
	Short: "Restart PostgreSQL server",
	Long:  `Restart the PostgreSQL server by stopping it and then starting it again.`,
	RunE:  runRestart,
}

// RestartPostgreSQLWithResult restarts PostgreSQL with the given configuration and returns detailed result information
func RestartPostgreSQLWithResult(config *PostgresConfig, mode string) (*RestartResult, error) {
	logger := slog.Default()
	result := &RestartResult{}

	if config.DataDir == "" {
		return nil, fmt.Errorf("data-dir is required")
	}

	logger.Info("Restarting PostgreSQL server", "data_dir", config.DataDir, "mode", mode)

	// Stop the server if it's running
	if isPostgreSQLRunning(config.DataDir) {
		logger.Info("Stopping PostgreSQL server")
		stopResult, err := StopPostgreSQLWithResult(config, mode)
		if err != nil {
			return nil, fmt.Errorf("failed to stop PostgreSQL during restart: %w", err)
		}
		result.StoppedFirst = stopResult.WasRunning
	} else {
		logger.Info("PostgreSQL is not running, proceeding with start")
		result.StoppedFirst = false
	}

	// Start the server
	logger.Info("Starting PostgreSQL server")
	startResult, err := StartPostgreSQLWithResult(config)
	if err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL during restart: %w", err)
	}

	result.PID = startResult.PID
	result.Message = "PostgreSQL server restarted successfully"

	logger.Info("PostgreSQL server restarted successfully")
	return result, nil
}

func runRestart(cmd *cobra.Command, args []string) error {
	config := NewPostgresConfigFromViper()
	mode, _ := cmd.Flags().GetString("mode")

	result, err := RestartPostgreSQLWithResult(config, mode)
	if err != nil {
		return err
	}

	// Display appropriate message for CLI users
	if result.StoppedFirst {
		fmt.Printf("PostgreSQL server restarted successfully (PID: %d, mode: %s)\n", result.PID, mode)
	} else {
		fmt.Printf("PostgreSQL server started successfully (PID: %d) - was not previously running\n", result.PID)
	}

	return nil
}
