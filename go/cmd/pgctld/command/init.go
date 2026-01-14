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
	"fmt"
	"log/slog"
	"os"
	"os/exec"

	"github.com/multigres/multigres/go/services/pgctld"

	"github.com/spf13/cobra"
)

// InitResult contains the result of initializing PostgreSQL data directory
type InitResult struct {
	AlreadyInitialized bool
	Message            string
}

// PgCtldInitCmd holds the init command configuration
type PgCtldInitCmd struct {
	pgCtlCmd *PgCtlCommand
}

// AddInitCommand adds the init subcommand to the root command
func AddInitCommand(root *cobra.Command, pc *PgCtlCommand) {
	initCmd := &PgCtldInitCmd{
		pgCtlCmd: pc,
	}

	root.AddCommand(initCmd.createCommand())
}

func (i *PgCtldInitCmd) createCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize PostgreSQL data directory",
		Long: `Initialize a PostgreSQL data directory with initdb.

The init command creates and initializes a new PostgreSQL data directory
using initdb. This command only initializes the data directory and does not
start the PostgreSQL server. Configuration can be provided via config file,
environment variables, or CLI flags. CLI flags take precedence over config
file and environment variable settings.

Password must be provided in a .pgpass file at <pooler-dir>/.pgpass
using PostgreSQL standard format: *:*:*:postgres:<password>

Examples:
  # Initialize data directory
  pgctld init --pooler-dir /var/lib/pooler-dir

  # Initialize with existing configuration
  pgctld init -d /var/lib/pooler-dir

  # Initialize using config file settings
  pgctld init --config-file /etc/pgctld/config.yaml`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return i.pgCtlCmd.validateGlobalFlags(cmd, args)
		},
		RunE: i.runInit,
	}

	return cmd
}

// InitDataDirWithResult initializes PostgreSQL data directory and returns detailed result information
func InitDataDirWithResult(logger *slog.Logger, poolerDir string, pgPort int, pgUser string) (*InitResult, error) {
	result := &InitResult{}
	dataDir := pgctld.PostgresDataDir(poolerDir)

	// Check if data directory is already initialized
	if pgctld.IsDataDirInitialized(poolerDir) {
		logger.Info("Data directory is already initialized", "data_dir", dataDir)
		result.AlreadyInitialized = true
		result.Message = "Data directory is already initialized"
		return result, nil
	}

	logger.Info("Initializing PostgreSQL data directory", "data_dir", dataDir)
	if err := initializeDataDir(logger, poolerDir, pgUser); err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}
	// create server config using the pooler directory
	_, err := pgctld.GeneratePostgresServerConfig(poolerDir, pgPort, pgUser)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres config: %w", err)
	}

	result.AlreadyInitialized = false
	result.Message = "Data directory initialized successfully"
	logger.Info("PostgreSQL data directory initialized successfully")
	return result, nil
}

func (i *PgCtldInitCmd) runInit(cmd *cobra.Command, args []string) error {
	poolerDir := i.pgCtlCmd.GetPoolerDir()
	result, err := InitDataDirWithResult(i.pgCtlCmd.lg.GetLogger(), poolerDir, i.pgCtlCmd.pgPort.Get(), i.pgCtlCmd.pgUser.Get())
	if err != nil {
		return err
	}

	// Display appropriate message for CLI users
	if result.AlreadyInitialized {
		fmt.Printf("Data directory is already initialized: %s\n", pgctld.PostgresDataDir(poolerDir))
	} else {
		fmt.Printf("Data directory initialized successfully: %s\n", pgctld.PostgresDataDir(poolerDir))
	}

	return nil
}

func initializeDataDir(logger *slog.Logger, poolerDir string, pgUser string) error {
	// Derive dataDir from poolerDir using the standard convention
	dataDir := pgctld.PostgresDataDir(poolerDir)

	// Note: initdb will create the data directory itself if it doesn't exist.
	// We don't create it beforehand to avoid leaving empty directories if initdb fails.

	// Get the effective password from .pgpass file
	effectivePassword, pgpassFile, err := resolvePassword(poolerDir)
	if err != nil {
		return fmt.Errorf("failed to resolve password: %w", err)
	}

	logger.Info("Using password from .pgpass file", "user", pgUser, "pgpass_file", pgpassFile)

	// Build initdb command
	// It's generally a good idea to enable page data checksums. Furthermore,
	// pgBackRest will validate checksums for the Postgres cluster it's backing up.
	// However, pgBackRest merely logs checksum validation errors but does not fail
	// the backup.
	args := []string{"-D", dataDir, "--data-checksums", "--auth-local=trust", "--auth-host=scram-sha-256", "-U", pgUser}

	// If password is provided, create a temporary password file for initdb
	var tempPwFile string
	if effectivePassword != "" {
		// Create temporary password file
		tmpFile, err := os.CreateTemp("", "pgpassword-*.txt")
		if err != nil {
			return fmt.Errorf("failed to create temporary password file: %w", err)
		}
		tempPwFile = tmpFile.Name()
		defer os.Remove(tempPwFile)

		if _, err := tmpFile.WriteString(effectivePassword); err != nil {
			tmpFile.Close()
			return fmt.Errorf("failed to write password to temporary file: %w", err)
		}
		if err := tmpFile.Close(); err != nil {
			return fmt.Errorf("failed to close temporary password file: %w", err)
		}

		// Add pwfile argument to initdb
		args = append(args, "--pwfile="+tempPwFile)
		logger.Info("Setting password during initdb", "user", pgUser)
	} else {
		logger.Warn("No password provided - skipping password setup", "user", pgUser, "warning", "PostgreSQL user will not have password authentication enabled")
	}

	cmd := exec.Command("initdb", args...)

	// Capture both stdout and stderr to include in error messages
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("initdb failed: %w\nOutput: %s", err, string(output))
	}
	logger.Info("initdb completed successfully", "output", string(output))

	if effectivePassword != "" {
		logger.Info("User password set successfully", "user", pgUser)
	}

	return nil
}
