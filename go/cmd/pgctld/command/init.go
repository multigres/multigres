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
	"path/filepath"
	"strings"

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

Password can be set via PGPASSWORD environment variable or by placing a
password file at <pooler-dir>/pgpassword.txt.

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

	// Run initdb
	//
	// It's generally a good idea to enable page data checksums. Furthermore,
	// pgBackRest will validate checksums for the Postgres cluster it's backing up.
	// However, pgBackRest merely logs checksum validation errors but does not fail
	// the backup.
	cmd := exec.Command("initdb", "-D", dataDir, "--data-checksums", "--auth-local=trust", "--auth-host=scram-sha-256", "-U", pgUser)

	// Capture both stdout and stderr to include in error messages
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("initdb failed: %w\nOutput: %s", err, string(output))
	}
	logger.Info("initdb completed", "output", string(output))

	// Get the effective password and validate it
	effectivePassword, err := resolvePassword(poolerDir)
	if err != nil {
		return fmt.Errorf("failed to resolve password: %w", err)
	}

	// Skip password setup if password is empty and warn user
	if effectivePassword == "" {
		logger.Warn("No password provided - skipping password setup", "user", pgUser, "warning", "PostgreSQL user will not have password authentication enabled")
		logger.Info("PostgreSQL data directory initialized successfully")
		return nil
	}

	// Set up user password for authentication
	if err := setPostgresPassword(poolerDir, pgUser); err != nil {
		return fmt.Errorf("failed to set user password: %w", err)
	}

	// Determine password source for logging
	pwfile := filepath.Join(poolerDir, "pgpassword.txt")
	passwordSource := "default"
	if _, err := os.Stat(pwfile); err == nil {
		passwordSource = "password file"
	} else if os.Getenv("PGPASSWORD") != "" {
		passwordSource = "PGPASSWORD environment variable"
	}

	logger.Info("User password set successfully", "user", pgUser, "password_source", passwordSource)

	return nil
}

func setPostgresPassword(poolerDir string, pgUser string) error {
	// Derive dataDir from poolerDir using the standard convention
	dataDir := pgctld.PostgresDataDir(poolerDir)

	// Get the effective password
	effectivePassword, err := resolvePassword(poolerDir)
	if err != nil {
		return fmt.Errorf("failed to resolve password: %w", err)
	}
	// Start PostgreSQL temporarily in single-user mode to set password
	// Use the configured user in single-user mode with trust auth to set the password
	// Set password_encryption to scram-sha-256 to ensure SCRAM encoding
	cmd := exec.Command("postgres", "--single", "-D", dataDir, pgUser)
	sqlCommands := fmt.Sprintf("SET password_encryption = 'scram-sha-256';\nALTER USER %s WITH PASSWORD '%s';\n", pgUser, effectivePassword)
	cmd.Stdin = strings.NewReader(sqlCommands)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to set %s password: %w", pgUser, err)
	}

	return nil
}
