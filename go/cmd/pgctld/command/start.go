// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/pgctld"
)

// NewPostgresCtlConfigFromDefaults creates a PostgresCtlConfig using command-line parameters
// Port, listen_addresses, and unix_socket_directories come from CLI flags, not from the config file
func NewPostgresCtlConfigFromDefaults(poolerDir string, pgPort int, pgListenAddresses string, pgUser string, pgDatabase string, timeout int) (*pgctld.PostgresCtlConfig, error) {
	postgresConfigFile := pgctld.PostgresConfigFile(poolerDir)

	effectivePort := pgPort
	effectiveListenAddresses := pgListenAddresses
	effectiveUnixSocketDirectories := pgctld.PostgresSocketDir(poolerDir)

	config, err := pgctld.NewPostgresCtlConfig(effectivePort, pgUser, pgDatabase, timeout, pgctld.PostgresDataDir(poolerDir), postgresConfigFile, poolerDir, effectiveListenAddresses, effectiveUnixSocketDirectories)
	if err != nil {
		return nil, fmt.Errorf("failed to create config: %w", err)
	}
	return config, nil
}

// ResolvePassword handles password resolution from file or environment variable
// Returns error if both are set or if password file cannot be read
func resolvePassword(pgPwfile string) (string, error) {
	envPassword := os.Getenv("PGPASSWORD")
	var filePassword string

	// Read password from file if specified
	if pgPwfile != "" {
		filePasswordBytes, readErr := os.ReadFile(pgPwfile)
		if readErr != nil {
			return "", fmt.Errorf("failed to read password file %s: %w", pgPwfile, readErr)
		}
		// Remove trailing newline if present
		filePassword = strings.TrimRight(string(filePasswordBytes), "\n\r")
	}

	// Check if both file and environment variable are set
	if pgPwfile != "" && envPassword != "" {
		return "", fmt.Errorf("both --pg-pwfile flag and PGPASSWORD environment variable are set, please use only one")
	}

	// Use file password if set, otherwise use environment variable
	if pgPwfile != "" {
		return filePassword, nil
	}

	return envPassword, nil
}

// AddStartCommand adds the start subcommand to the root command
func AddStartCommand(root *cobra.Command, pc *PgCtlCommand) {
	startCmd := &PgCtlStartCmd{
		pgCtlCmd: pc,
	}
	root.AddCommand(startCmd.createCommand())
}

// PgCtlStartCmd holds the start command configuration
type PgCtlStartCmd struct {
	pgCtlCmd *PgCtlCommand
}

func (s *PgCtlStartCmd) createCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start PostgreSQL server",
		Long: `Start a PostgreSQL server instance with the configured parameters.

The start command initializes the data directory if needed and starts PostgreSQL.
Configuration can be provided via config file, environment variables, or CLI flags.
CLI flags take precedence over config file and environment variable settings.

Examples:
  # Start with default settings
  pgctld start --pooler-dir /var/lib/postgresql/data

  # Start on custom port
  pgctld start --pooler-dir /var/lib/postgresql/data --port 5433

  # Start with custom socket directory and config file
  pgctld start --pooler-dir /var/lib/postgresql/data -s /var/run/postgresql -c /etc/postgresql/custom.conf`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return s.pgCtlCmd.validateInitialized(cmd, args)
		},
		RunE: s.runStart,
	}

	return cmd
}

func (s *PgCtlStartCmd) runStart(cmd *cobra.Command, args []string) error {
	config, err := NewPostgresCtlConfigFromDefaults(s.pgCtlCmd.GetPoolerDir(), s.pgCtlCmd.pgPort.Get(), s.pgCtlCmd.pgListenAddresses.Get(), s.pgCtlCmd.pgUser.Get(), s.pgCtlCmd.pgDatabase.Get(), s.pgCtlCmd.timeout.Get())
	if err != nil {
		return err
	}

	// Use daemon manager for CLI commands (production mode)
	manager := &DaemonPostgresManager{}
	result, err := StartPostgreSQLWithResult(s.pgCtlCmd.lg.GetLogger(), manager, config)
	if err != nil {
		return err
	}

	// Display appropriate message for CLI users
	if result.AlreadyRunning {
		fmt.Printf("PostgreSQL is already running (PID: %d)\n", result.PID)
	} else {
		fmt.Printf("PostgreSQL server started successfully (PID: %d)\n", result.PID)
	}

	return nil
}
