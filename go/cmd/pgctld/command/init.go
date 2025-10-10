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
	"strings"

	"github.com/multigres/multigres/go/pgctld"
	"github.com/multigres/multigres/go/viperutil"

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
	pgPwfile viperutil.Value[string]
}

// AddInitCommand adds the init subcommand to the root command
func AddInitCommand(root *cobra.Command, pc *PgCtlCommand) {
	initCmd := &PgCtldInitCmd{
		pgCtlCmd: pc,
		pgPwfile: viperutil.Configure("pg-pwfile", viperutil.Options[string]{
			Default:  "",
			FlagName: "pg-pwfile",
			Dynamic:  false,
		}),
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

Examples:
  # Initialize data directory
  pgctld init --pooler-dir /var/lib/pooler-dir

  # Initialize with existing configuration
  pgctld init -d /var/lib/pooler-dir

  # Initialize using config file settings
  pgctld init --config-file /etc/pgctld/config.yaml`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			i.pgCtlCmd.lg.SetupLogging()
			return i.pgCtlCmd.validateGlobalFlags(cmd, args)
		},
		RunE: i.runInit,
	}

	cmd.Flags().String("pg-pwfile", i.pgPwfile.Default(), "PostgreSQL password file path")
	viperutil.BindFlags(cmd.Flags(), i.pgPwfile)

	return cmd
}

// InitDataDirWithResult initializes PostgreSQL data directory and returns detailed result information
func InitDataDirWithResult(logger *slog.Logger, poolerDir string, pgPort int, pgUser string, pgPwfile string) (*InitResult, error) {
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
	if err := initializeDataDir(logger, dataDir, pgUser, pgPwfile); err != nil {
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
	result, err := InitDataDirWithResult(i.pgCtlCmd.lg.GetLogger(), poolerDir, i.pgCtlCmd.pgPort.Get(), i.pgCtlCmd.pgUser.Get(), i.pgPwfile.Get())
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

func initializeDataDir(logger *slog.Logger, dataDir string, pgUser string, pgPwfile string) error {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Run initdb
	cmd := exec.Command("initdb", "-D", dataDir, "--auth-local=trust", "--auth-host=md5", "-U", pgUser)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("initdb failed: %w", err)
	}

	// Get the effective password and validate it
	effectivePassword, err := resolvePassword(pgPwfile)
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
	if err := setPostgresPassword(dataDir, pgUser, pgPwfile); err != nil {
		return fmt.Errorf("failed to set user password: %w", err)
	}

	// Determine password source for logging
	passwordSource := "default"
	if pgPwfile != "" {
		passwordSource = "password file"
	} else if os.Getenv("PGPASSWORD") != "" {
		passwordSource = "PGPASSWORD environment variable"
	}

	logger.Info("User password set successfully", "user", pgUser, "password_source", passwordSource)

	return nil
}

func setPostgresPassword(dataDir string, pgUser string, pgPwfile string) error {
	// Get the effective password
	effectivePassword, err := resolvePassword(pgPwfile)
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
