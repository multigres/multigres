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

	"github.com/multigres/multigres/go/common/constants"
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

Password can be set via the POSTGRES_PASSWORD environment variable.

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

// InitDataDirWithResult initializes PostgreSQL data directory and returns detailed result information.
// When pgDatabase differs from the default "postgres" database, it starts PostgreSQL transiently
// and creates the target database — mirroring docker-library/postgres's docker_setup_db behaviour.
func InitDataDirWithResult(logger *slog.Logger, poolerDir string, pgPort int, pgUser string, pgPassword string, pgDatabase string) (*InitResult, error) {
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
	if err := initializeDataDir(logger, poolerDir, pgUser, pgPassword); err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}
	// create server config using the pooler directory
	_, err := pgctld.GeneratePostgresServerConfig(poolerDir, pgPort, pgUser)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres config: %w", err)
	}

	// If the target database is not the default "postgres" (always created by initdb),
	// start PostgreSQL transiently and create it — same as docker-library/postgres does.
	if pgDatabase != constants.DefaultPostgresDatabase {
		if err := setupDatabase(logger, poolerDir, pgPort, pgUser, pgDatabase); err != nil {
			return nil, fmt.Errorf("failed to create database %q: %w", pgDatabase, err)
		}
	}

	result.AlreadyInitialized = false
	result.Message = "Data directory initialized successfully"
	logger.Info("PostgreSQL data directory initialized successfully")
	return result, nil
}

func (i *PgCtldInitCmd) runInit(cmd *cobra.Command, args []string) error {
	poolerDir := i.pgCtlCmd.GetPoolerDir()
	result, err := InitDataDirWithResult(i.pgCtlCmd.lg.GetLogger(), poolerDir, i.pgCtlCmd.pgPort.Get(), i.pgCtlCmd.pgUser.Get(), i.pgCtlCmd.pgPassword.Get(), i.pgCtlCmd.pgDatabase.Get())
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

// setupDatabase starts a transient pgInstance and creates pgDatabase if it does
// not already exist, then stops the instance.  This mirrors what the official
// docker-library/postgres image does in its docker_setup_db() entrypoint function.
func setupDatabase(logger *slog.Logger, poolerDir string, pgPort int, pgUser, pgDatabase string) error {
	dataDir := pgctld.PostgresDataDir(poolerDir)
	configFile := pgctld.PostgresConfigFile(poolerDir)

	logger.Info("Starting PostgreSQL transiently to create database", "database", pgDatabase)
	pg, err := newPgInstance(logger, dataDir, configFile, pgPort, pgUser)
	if err != nil {
		return err
	}
	defer pg.stop()

	// Check whether the target database already exists.
	// Use Go string formatting to build the SQL — the database name comes from
	// operator config, not untrusted user input, so simple quoting is safe.
	// Single quotes in the name are escaped as '' per the SQL standard.
	checkOut, err := pg.psql(constants.DefaultPostgresDatabase,
		"-Atc", fmt.Sprintf("SELECT 1 FROM pg_database WHERE datname = '%s'",
			strings.ReplaceAll(pgDatabase, "'", "''")),
	)
	if err != nil {
		return fmt.Errorf("failed to query pg_database for %q: %w\nOutput: %s", pgDatabase, err, checkOut)
	}
	if strings.TrimSpace(string(checkOut)) == "1" {
		logger.Info("Database already exists, skipping creation", "database", pgDatabase)
		return nil
	}

	// CREATE DATABASE with a double-quoted identifier; double quotes in the name
	// are escaped as "" per the SQL standard.
	logger.Info("Creating database", "database", pgDatabase)
	if out, err := pg.psql(constants.DefaultPostgresDatabase,
		"-c", fmt.Sprintf(`CREATE DATABASE "%s"`, strings.ReplaceAll(pgDatabase, `"`, `""`)),
	); err != nil {
		return fmt.Errorf("failed to create database %q: %w\nOutput: %s", pgDatabase, err, out)
	}

	logger.Info("Database created successfully", "database", pgDatabase)
	return nil
}

func initializeDataDir(logger *slog.Logger, poolerDir string, pgUser string, pgPassword string) error {
	// Derive dataDir from poolerDir using the standard convention
	dataDir := pgctld.PostgresDataDir(poolerDir)

	// Note: initdb will create the data directory itself if it doesn't exist.
	// We don't create it beforehand to avoid leaving empty directories if initdb fails.

	// Build initdb command
	// It's generally a good idea to enable page data checksums. Furthermore,
	// pgBackRest will validate checksums for the Postgres cluster it's backing up.
	// However, pgBackRest merely logs checksum validation errors but does not fail
	// the backup.
	args := []string{"-D", dataDir, "--data-checksums", "--auth-local=trust", "--auth-host=scram-sha-256", "-U", pgUser}

	// If password is provided, create a temporary password file for initdb
	var tempPwFile string
	if pgPassword != "" {
		// Create temporary password file
		tmpFile, err := os.CreateTemp("", "pgpassword-*.txt")
		if err != nil {
			return fmt.Errorf("failed to create temporary password file: %w", err)
		}
		tempPwFile = tmpFile.Name()
		defer os.Remove(tempPwFile)

		if _, err := tmpFile.WriteString(pgPassword); err != nil {
			tmpFile.Close()
			return fmt.Errorf("failed to write password to temporary file: %w", err)
		}
		if err := tmpFile.Close(); err != nil {
			return fmt.Errorf("failed to close temporary password file: %w", err)
		}

		// Add pwfile argument to initdb
		args = append(args, "--pwfile="+tempPwFile)
		logger.Info("Setting password during initdb", "user", pgUser, "password_source", "POSTGRES_PASSWORD environment variable")
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

	if pgPassword != "" {
		logger.Info("User password set successfully", "user", pgUser, "password_source", "POSTGRES_PASSWORD environment variable")
	}

	return nil
}
