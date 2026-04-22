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

Extra initdb arguments can be set via the POSTGRES_INITDB_ARGS environment variable,
or overridden with the --pg-initdb-args flag.

Examples:
  # Initialize data directory
  pgctld init --pooler-dir /var/lib/pooler-dir

  # Initialize with existing configuration
  pgctld init -d /var/lib/pooler-dir

  # Initialize with ICU locale provider and specific locale en_US.UTF-8
  pgctld init --pg-initdb-args "--locale=icu --icu-locale=en_US.UTF-8" -d /var/lib/pooler-dir

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
func InitDataDirWithResult(logger *slog.Logger, poolerDir string, cfg PgCtldServiceConfig) (*InitResult, error) {
	result := &InitResult{}
	dataDir := pgctld.PostgresDataDir()

	// Check if data directory is already initialized
	if pgctld.IsDataDirInitialized() {
		logger.Info("Data directory is already initialized", "data_dir", dataDir)
		result.AlreadyInitialized = true
		result.Message = "Data directory is already initialized"
		return result, nil
	}

	logger.Info("Initializing PostgreSQL data directory", "data_dir", dataDir)
	if err := initializeDataDir(logger, cfg); err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}
	// create server config using the pooler directory
	_, err := pgctld.GeneratePostgresServerConfig(poolerDir, cfg.Port, cfg.User)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres config: %w", err)
	}

	// Post-initdb steps that need a running server (custom DB creation, init
	// SQL files) share a single transient PostgreSQL instance.
	if cfg.Database != constants.DefaultPostgresDatabase || len(cfg.InitDbSQLFiles) > 0 {
		if err := postInitdbSetup(logger, cfg); err != nil {
			return nil, err
		}
	}

	result.AlreadyInitialized = false
	result.Message = "Data directory initialized successfully"
	logger.Info("PostgreSQL data directory initialized successfully")
	return result, nil
}

// postInitdbSetup starts a transient PostgreSQL instance and performs any
// post-initdb setup steps: creating a custom target database (if requested)
// and running user-provided init SQL files against the target database.
func postInitdbSetup(logger *slog.Logger, cfg PgCtldServiceConfig) error {
	createDB := cfg.Database != constants.DefaultPostgresDatabase
	runSQL := len(cfg.InitDbSQLFiles) > 0

	logger.Info("Starting PostgreSQL transiently for post-initdb setup",
		"create_database", createDB, "run_init_sql", runSQL)
	pg, err := newPgInstance(logger, pgctld.PostgresDataDir(), pgctld.PostgresConfigFile(), cfg.Port, cfg.User)
	if err != nil {
		return err
	}
	defer pg.stop()

	if createDB {
		if err := createDatabaseOnInstance(logger, pg, cfg.Database); err != nil {
			return fmt.Errorf("failed to create database %q: %w", cfg.Database, err)
		}
	}

	if runSQL {
		if err := runInitDbSQLFiles(logger, pg, cfg.Database, cfg.InitDbSQLFiles); err != nil {
			return err
		}
	}

	return nil
}

// runInitDbSQLFiles executes each SQL file against database with
// ON_ERROR_STOP=1 so a failing statement aborts its script.
func runInitDbSQLFiles(logger *slog.Logger, pg *pgInstance, database string, files []string) error {
	for _, file := range files {
		if _, err := os.Stat(file); err != nil {
			return fmt.Errorf("init SQL file not accessible (%s): %w", file, err)
		}
		logger.Info("Running init SQL file", "file", file, "database", database)
		out, err := pg.psql(database, "-v", "ON_ERROR_STOP=1", "-f", file)
		if err != nil {
			return fmt.Errorf("init SQL file failed (%s): %w\nOutput: %s", file, err, out)
		}
		logger.Info("Init SQL file applied", "file", file)
	}
	return nil
}

func (i *PgCtldInitCmd) runInit(cmd *cobra.Command, args []string) error {
	poolerDir := i.pgCtlCmd.GetPoolerDir()
	cfg := PgCtldServiceConfig{
		Port:           i.pgCtlCmd.pgPort.Get(),
		User:           i.pgCtlCmd.pgUser.Get(),
		Database:       i.pgCtlCmd.pgDatabase.Get(),
		Password:       i.pgCtlCmd.pgPassword.Get(),
		InitDbSQLFiles: i.pgCtlCmd.initDbSQLFiles.Get(),
	}
	result, err := InitDataDirWithResult(i.pgCtlCmd.lg.GetLogger(), poolerDir, cfg)
	if err != nil {
		return err
	}

	// Display appropriate message for CLI users
	if result.AlreadyInitialized {
		fmt.Printf("Data directory is already initialized: %s\n", pgctld.PostgresDataDir())
	} else {
		fmt.Printf("Data directory initialized successfully: %s\n", pgctld.PostgresDataDir())
	}

	return nil
}

// createDatabaseOnInstance creates database on an already-running transient
// pgInstance if it does not already exist. This mirrors what the official
// docker-library/postgres image does in its docker_setup_db() entrypoint function.
func createDatabaseOnInstance(logger *slog.Logger, pg *pgInstance, database string) error {
	// Check whether the target database already exists.
	// Use Go string formatting to build the SQL — the database name comes from
	// operator config, not untrusted user input, so simple quoting is safe.
	// Single quotes in the name are escaped as '' per the SQL standard.
	checkOut, err := pg.psql(constants.DefaultPostgresDatabase,
		"-Atc", fmt.Sprintf("SELECT 1 FROM pg_database WHERE datname = '%s'",
			strings.ReplaceAll(database, "'", "''")),
	)
	if err != nil {
		return fmt.Errorf("failed to query pg_database for %q: %w\nOutput: %s", database, err, checkOut)
	}
	if strings.TrimSpace(string(checkOut)) == "1" {
		logger.Info("Database already exists, skipping creation", "database", database)
		return nil
	}

	// CREATE DATABASE with a double-quoted identifier; double quotes in the name
	// are escaped as "" per the SQL standard.
	logger.Info("Creating database", "database", database)
	if out, err := pg.psql(constants.DefaultPostgresDatabase,
		"-c", fmt.Sprintf(`CREATE DATABASE "%s"`, strings.ReplaceAll(database, `"`, `""`)),
	); err != nil {
		return fmt.Errorf("failed to create database %q: %w\nOutput: %s", database, err, out)
	}

	logger.Info("Database created successfully", "database", database)
	return nil
}

func initializeDataDir(logger *slog.Logger, cfg PgCtldServiceConfig) error {
	// Derive dataDir from poolerDir using the standard convention
	dataDir := pgctld.PostgresDataDir()

	// Note: initdb will create the data directory itself if it doesn't exist.
	// We don't create it beforehand to avoid leaving empty directories if initdb fails.

	// Build initdb command
	// It's generally a good idea to enable page data checksums. Furthermore,
	// pgBackRest will validate checksums for the Postgres cluster it's backing up.
	// However, pgBackRest merely logs checksum validation errors but does not fail
	// the backup.
	args := []string{"-D", dataDir, "--data-checksums", "--auth-local=trust", "--auth-host=scram-sha-256", "-U", cfg.User}

	// If password is provided, create a temporary password file for initdb
	var tempPwFile string
	if cfg.Password != "" {
		// Create temporary password file
		tmpFile, err := os.CreateTemp("", "pgpassword-*.txt")
		if err != nil {
			return fmt.Errorf("failed to create temporary password file: %w", err)
		}
		tempPwFile = tmpFile.Name()
		defer os.Remove(tempPwFile)

		if _, err := tmpFile.WriteString(cfg.Password); err != nil {
			tmpFile.Close()
			return fmt.Errorf("failed to write password to temporary file: %w", err)
		}
		if err := tmpFile.Close(); err != nil {
			return fmt.Errorf("failed to close temporary password file: %w", err)
		}

		// Add pwfile argument to initdb
		args = append(args, "--pwfile="+tempPwFile)
		logger.Info("Setting password during initdb", "user", cfg.User, "password_source", "POSTGRES_PASSWORD environment variable")
	} else {
		logger.Warn("No password provided - skipping password setup", "user", cfg.User, "warning", "PostgreSQL user will not have password authentication enabled")
	}

	if cfg.InitdbArgs != "" {
		args = append(args, strings.Fields(cfg.InitdbArgs)...)
		logger.Info("Appending extra initdb args", "args", cfg.InitdbArgs)
	}

	cmd := exec.Command("initdb", args...)

	// Capture both stdout and stderr to include in error messages
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("initdb failed: %w\nOutput: %s", err, string(output))
	}
	logger.Info("initdb completed successfully", "output", string(output))

	if cfg.Password != "" {
		logger.Info("User password set successfully", "user", cfg.User, "password_source", "POSTGRES_PASSWORD environment variable")
	}

	return nil
}
