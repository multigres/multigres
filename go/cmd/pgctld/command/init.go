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
	_, err := pgctld.GeneratePostgresServerConfig(poolerDir, cfg.User, cfg.InitdbExtraConfFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres config: %w", err)
	}

	// Post-initdb steps that need a running server (custom DB creation, init
	// SQL files) share a single transient PostgreSQL instance.
	if cfg.Database != constants.DefaultPostgresDatabase || len(cfg.InitdbSQLFiles) > 0 || len(cfg.InitdbSQLDirs) > 0 {
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
// and running user-provided init SQL against the target database.
//
// Execution order:
//  1. Create target database (if non-default).
//  2. Run --pg-initdb-sql-dirs: bulk schema/migration directories, each under
//     SET SESSION AUTHORIZATION <role>. Directories establish the base schema.
//  3. Run --pg-initdb-sql-files: individual files applied on top as targeted
//     overrides or patches.
func postInitdbSetup(logger *slog.Logger, cfg PgCtldServiceConfig) error {
	createDB := cfg.Database != constants.DefaultPostgresDatabase

	logger.Info("Starting PostgreSQL transiently for post-initdb setup",
		"create_database", createDB,
		"init_sql_files", len(cfg.InitdbSQLFiles),
		"init_sql_dirs", len(cfg.InitdbSQLDirs))
	pg, err := newPgInstance(logger, pgctld.PostgresDataDir(), pgctld.PostgresConfigFile(), cfg)
	if err != nil {
		return err
	}
	defer pg.stop()

	if createDB {
		if err := createDatabaseOnInstance(logger, pg, cfg.Database); err != nil {
			return fmt.Errorf("failed to create database %q: %w", cfg.Database, err)
		}
	}

	// Dirs first: establish bulk schema/migrations under the specified roles.
	if err := runInitdbSQLDirs(logger, pg, cfg.Database, cfg.InitdbSQLDirs); err != nil {
		return err
	}

	// Files second: apply targeted overrides or patches on top of the base schema.
	if err := runInitdbSQLFiles(logger, pg, cfg.Database, cfg.InitdbSQLFiles); err != nil {
		return err
	}

	return nil
}

// runInitdbSQLFiles executes each SQL file against database with
// ON_ERROR_STOP=1 so a failing statement aborts its script.
func runInitdbSQLFiles(logger *slog.Logger, pg *pgInstance, database string, files []string) error {
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

// runInitdbSQLDirs processes each role:path entry: reads all .sql files from the
// directory in lexicographic order and runs them in a single psql session under
// SET SESSION AUTHORIZATION "<role>" / RESET SESSION AUTHORIZATION.
func runInitdbSQLDirs(logger *slog.Logger, pg *pgInstance, database string, entries []string) error {
	for _, entry := range entries {
		role, dir, err := parseSQLDirEntry(entry)
		if err != nil {
			return err
		}

		files, err := sqlFilesInDir(dir)
		if err != nil {
			return err
		}
		if len(files) == 0 {
			logger.Info("Init SQL dir is empty, skipping", "dir", dir, "role", role)
			continue
		}

		logger.Info("Running init SQL dir", "dir", dir, "role", role, "files", len(files), "database", database)
		args := []string{
			"-v", "ON_ERROR_STOP=1",
			"-c", "SET SESSION AUTHORIZATION " + quoteIdentifier(role),
		}
		for _, f := range files {
			args = append(args, "-f", f)
		}
		args = append(args, "-c", "RESET SESSION AUTHORIZATION")

		if out, err := pg.psql(database, args...); err != nil {
			return fmt.Errorf("init SQL dir failed (%s as %s): %w\nOutput: %s", dir, role, err, out)
		}
		logger.Info("Init SQL dir applied", "dir", dir, "role", role)
	}
	return nil
}

// parseSQLDirEntry splits a "role:path" entry on the first colon.
func parseSQLDirEntry(entry string) (role, dir string, err error) {
	idx := strings.IndexByte(entry, ':')
	if idx <= 0 {
		return "", "", fmt.Errorf("invalid --pg-initdb-sql-dir entry %q: expected role:path format", entry)
	}
	return entry[:idx], entry[idx+1:], nil
}

// sqlFilesInDir returns .sql file paths from dir in lexicographic order,
// skipping subdirectories and non-.sql files.
// os.ReadDir already returns entries sorted by name.
func sqlFilesInDir(dir string) ([]string, error) {
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("cannot read init SQL dir %q: %w", dir, err)
	}
	var files []string
	for _, e := range dirEntries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	return files, nil
}

func (i *PgCtldInitCmd) runInit(cmd *cobra.Command, args []string) error {
	poolerDir := i.pgCtlCmd.GetPoolerDir()
	cfg, err := i.pgCtlCmd.buildServiceConfig()
	if err != nil {
		return err
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

	logger.Info("Creating database", "database", database)
	if out, err := pg.psql(constants.DefaultPostgresDatabase,
		"-c", "CREATE DATABASE "+quoteIdentifier(database),
	); err != nil {
		return fmt.Errorf("failed to create database %q: %w\nOutput: %s", database, err, out)
	}

	logger.Info("Database created successfully", "database", database)
	return nil
}

// resolveInitdbPwFile returns a filesystem path suitable for initdb's --pwfile
// flag along with a cleanup function the caller must defer.
//
// The primary path is the operator-supplied password file (POSTGRES_PASSWORD_FILE
// / --pg-password-file). In that case the returned path *is* that file
// and no plaintext is ever copied — initdb reads it directly. The cleanup is a
// no-op.
//
// The fallback path is the legacy POSTGRES_PASSWORD environment variable, which
// is being phased out. There is no file on disk yet, so we have to stage the
// password into a randomly named temp file long enough for initdb to read it.
// The cleanup unlinks that temp file. This branch exists only to keep operators
// who have not yet migrated to file-based secrets working; new deployments
// should always hit the primary path.
//
// Note on first-line semantics: initdb --pwfile reads only the first line of
// the file. pgsecret.ReadPasswordFile returns the whole content (minus
// trailing CR/LF). The two parsers agree for any single-line password (with
// or without a trailing newline) — which is every realistic Kubernetes Secret.
// A multi-line file would silently diverge: the cluster would be initialized
// with line 1, but the admin pool / replication would try to authenticate with
// the whole content. Password files MUST be single-line.
func resolveInitdbPwFile(cfg PgCtldServiceConfig) (path string, cleanup func(), err error) {
	if cfg.PasswordSource == PasswordSourceFile {
		return cfg.PasswordFile, func() {}, nil
	}
	// Fallback: POSTGRES_PASSWORD env. Stage the plaintext to a randomly named
	// file in /tmp. Removed unconditionally on return so the plaintext window
	// is bounded by the initdb exec. The filename has no prefix so its purpose
	// isn't advertised in /tmp listings.
	tmpFile, err := os.CreateTemp("", "*")
	if err != nil {
		return "", func() {}, fmt.Errorf("failed to create temporary password file: %w", err)
	}
	tempPwFile := tmpFile.Name()
	cleanup = func() { _ = os.Remove(tempPwFile) }

	if _, err := tmpFile.WriteString(cfg.Password); err != nil {
		tmpFile.Close()
		cleanup()
		return "", func() {}, fmt.Errorf("failed to write password to temporary file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("failed to close temporary password file: %w", err)
	}
	return tempPwFile, cleanup, nil
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
	args := []string{"-D", dataDir, "--data-checksums", "--auth-local=scram-sha-256", "--auth-host=scram-sha-256", "-U", cfg.User}

	// Invariant: production callers (runInit, the InitDataDir gRPC handler)
	// populate Password, PasswordSource (and PasswordFile when source==File)
	// via PgCtlCommand.GetPostgresPassword, which errors when no source is
	// configured. Reaching here with a missing source means a caller — almost
	// certainly a test — built PgCtldServiceConfig by hand without going
	// through the resolver. Panic rather than guess a source that would
	// mislabel the log line below.
	if cfg.PasswordSource == "" || cfg.PasswordSource == PasswordSourceNone {
		panic(fmt.Sprintf("initializeDataDir: PasswordSource=%q — callers must populate it via PgCtlCommand.GetPostgresPassword", cfg.PasswordSource))
	}
	if cfg.PasswordSource == PasswordSourceFile && cfg.PasswordFile == "" {
		panic("initializeDataDir: PasswordSource=POSTGRES_PASSWORD_FILE but PasswordFile is empty — callers must populate PasswordFile (use PgCtlCommand.GetPostgresPassword)")
	}

	pwFile, cleanup, err := resolveInitdbPwFile(cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	args = append(args, "--pwfile="+pwFile)
	logger.Info("Setting password during initdb", "user", cfg.User, "password_source", string(cfg.PasswordSource))

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
	logger.Info("User password set successfully", "user", cfg.User, "password_source", string(cfg.PasswordSource))

	return nil
}

// quoteIdentifier wraps name in double quotes and escapes any embedded double
// quotes as "" per the SQL standard, producing a safe PostgreSQL identifier.
// The value comes from operator config, not from untrusted user input, so a
// simple ReplaceAll is sufficient here. If that assumption ever changes, replace
// this with pq.QuoteIdentifier from github.com/lib/pq, which applies the same
// escaping but is a well-tested, purpose-built function.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
