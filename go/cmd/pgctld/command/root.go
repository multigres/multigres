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

	"github.com/multigres/multigres/go/pgctld"
	"github.com/multigres/multigres/go/viperutil"

	"github.com/spf13/cobra"
)

// PgCtlCommand holds the configuration for pgctld commands
type PgCtlCommand struct {
	pgDatabase viperutil.Value[string]
	pgUser     viperutil.Value[string]
	timeout    viperutil.Value[int]
}

// GetRootCommand creates and returns the root command for pgctld with all subcommands
func GetRootCommand() *cobra.Command {
	pc := &PgCtlCommand{
		pgDatabase: viperutil.Configure("pg-database", viperutil.Options[string]{
			Default:  "postgres",
			FlagName: "pg-database",
			Dynamic:  false,
		}),
		pgUser: viperutil.Configure("pg-user", viperutil.Options[string]{
			Default:  "postgres",
			FlagName: "pg-user",
			Dynamic:  false,
		}),
		timeout: viperutil.Configure("timeout", viperutil.Options[int]{
			Default:  30,
			FlagName: "timeout",
			Dynamic:  false,
		}),
	}

	root := &cobra.Command{
		Use:   "pgctld",
		Short: "PostgreSQL control daemon for Multigres",
		Long: `pgctld manages PostgreSQL server instances within the Multigres cluster.
It provides lifecycle management including start, stop, restart, and configuration
management for PostgreSQL servers.`,
		Args: cobra.NoArgs,
	}

	root.PersistentFlags().StringP("pg-database", "D", pc.pgDatabase.Default(), "PostgreSQL database name")
	root.PersistentFlags().StringP("pg-user", "U", pc.pgUser.Default(), "PostgreSQL username")
	root.PersistentFlags().IntP("timeout", "t", pc.timeout.Default(), "Operation timeout in seconds")

	viperutil.BindFlags(root.PersistentFlags(),
		pc.pgDatabase,
		pc.pgUser,
		pc.timeout,
	)

	// Add all subcommands
	AddServerCommand(root, pc)
	AddInitCommand(root, pc)
	AddStartCommand(root, pc)
	AddStopCommand(root, pc)
	AddRestartCommand(root, pc)
	AddStatusCommand(root, pc)
	AddVersionCommand(root, pc)
	AddReloadCommand(root, pc)

	return root
}

// validateGlobalFlags validates required global flags for all pgctld commands
func validateGlobalFlags(cmd *cobra.Command, args []string) error {
	// Validate pooler-dir is required and non-empty for all commands
	poolerDir := pgctld.GetPoolerDir()
	if poolerDir == "" {
		return fmt.Errorf("pooler-dir needs to be set")
	}

	return nil
}

// validateInitialized validates that the PostgreSQL data directory has been initialized
// This should be called by all commands except 'init'
func validateInitialized(cmd *cobra.Command, args []string) error {
	// First run the standard global validation
	if err := validateGlobalFlags(cmd, args); err != nil {
		return err
	}

	// Check if data directory is initialized
	poolerDir := pgctld.GetPoolerDir()

	if !pgctld.IsDataDirInitialized(poolerDir) {
		dataDir := pgctld.PostgresDataDir(poolerDir)
		return fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	return nil
}
