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

	"github.com/spf13/cobra"
)

// Flag variables for root command (shared across all commands)
var (
	pgDatabase = "postgres"
	pgUser     = "postgres"
	timeout    = 30
)

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

// Root represents the base command when called without any subcommands
var Root = &cobra.Command{
	Use:   "pgctld",
	Short: "PostgreSQL control daemon for Multigres",
	Long: `pgctld manages PostgreSQL server instances within the Multigres cluster.
It provides lifecycle management including start, stop, restart, and configuration
management for PostgreSQL servers.`,
	Args: cobra.NoArgs,
}

func init() {
	Root.PersistentFlags().StringVarP(&pgDatabase, "pg-database", "D", pgDatabase, "PostgreSQL database name")
	Root.PersistentFlags().StringVarP(&pgUser, "pg-user", "U", pgUser, "PostgreSQL username")
	Root.PersistentFlags().IntVarP(&timeout, "timeout", "t", timeout, "Operation timeout in seconds")
}
