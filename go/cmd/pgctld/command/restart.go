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
	"github.com/spf13/viper"
)

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

func runRestart(cmd *cobra.Command, args []string) error {
	logger := slog.Default()

	dataDir := viper.GetString("data-dir")
	if dataDir == "" {
		return fmt.Errorf("data-dir is required")
	}

	mode, _ := cmd.Flags().GetString("mode")
	timeout := viper.GetInt("timeout")

	logger.Info("Restarting PostgreSQL server", "data_dir", dataDir, "mode", mode)

	// Stop the server if it's running
	if isPostgreSQLRunning(dataDir) {
		logger.Info("Stopping PostgreSQL server")
		if err := stopPostgreSQL(dataDir, mode, timeout); err != nil {
			return fmt.Errorf("failed to stop PostgreSQL during restart: %w", err)
		}
	} else {
		logger.Info("PostgreSQL is not running, proceeding with start")
	}

	// Start the server
	logger.Info("Starting PostgreSQL server")
	if err := startPostgreSQL(dataDir); err != nil {
		return fmt.Errorf("failed to start PostgreSQL during restart: %w", err)
	}

	// Wait for server to be ready
	logger.Info("Waiting for PostgreSQL to be ready")
	if err := waitForPostgreSQL(); err != nil {
		return fmt.Errorf("PostgreSQL failed to become ready after restart: %w", err)
	}

	logger.Info("PostgreSQL server restarted successfully")
	return nil
}
