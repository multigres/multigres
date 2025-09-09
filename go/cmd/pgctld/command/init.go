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

// InitResult contains the result of initializing PostgreSQL data directory
type InitResult struct {
	AlreadyInitialized bool
	Message            string
}

func init() {
	Root.AddCommand(initCmd)

	// Add init-specific flags
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize PostgreSQL data directory",
	Long: `Initialize a PostgreSQL data directory with initdb.

The init command creates and initializes a new PostgreSQL data directory
using initdb. This command only initializes the data directory and does not
start the PostgreSQL server. Configuration can be provided via config file,
environment variables, or CLI flags. CLI flags take precedence over config
file and environment variable settings.

Examples:
  # Initialize data directory
  pgctld init --pg-data-dir /var/lib/postgresql/data

  # Initialize with existing configuration
  pgctld init -d /var/lib/postgresql/instance2/data

  # Initialize using config file settings
  pgctld init --config-file /etc/pgctld/config.yaml`,
	RunE: runInit,
}

// InitDataDirWithResult initializes PostgreSQL data directory and returns detailed result information
func InitDataDirWithResult(config *PostgresConfig) (*InitResult, error) {
	logger := slog.Default()
	result := &InitResult{}

	if config.DataDir == "" {
		return nil, fmt.Errorf("pg-data-dir is required")
	}

	// Check if data directory is already initialized
	if isDataDirInitialized(config.DataDir) {
		logger.Info("Data directory is already initialized", "data_dir", config.DataDir)
		result.AlreadyInitialized = true
		result.Message = "Data directory is already initialized"
		return result, nil
	}

	logger.Info("Initializing PostgreSQL data directory", "data_dir", config.DataDir)
	if err := initializeDataDir(config.DataDir); err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}

	result.AlreadyInitialized = false
	result.Message = "Data directory initialized successfully"
	logger.Info("PostgreSQL data directory initialized successfully")
	return result, nil
}

func runInit(cmd *cobra.Command, args []string) error {
	config := NewPostgresConfigFromDefaults()

	result, err := InitDataDirWithResult(config)
	if err != nil {
		return err
	}

	// Display appropriate message for CLI users
	if result.AlreadyInitialized {
		fmt.Printf("Data directory is already initialized: %s\n", config.DataDir)
	} else {
		fmt.Printf("Data directory initialized successfully: %s\n", config.DataDir)
	}

	return nil
}
