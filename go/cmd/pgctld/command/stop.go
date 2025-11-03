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
	"log/slog"
	"os/exec"

	"github.com/multigres/multigres/go/pgctld"
	"github.com/multigres/multigres/go/viperutil"

	"github.com/spf13/cobra"
)

// PgCtlStopCmd holds the stop command configuration
type PgCtlStopCmd struct {
	pgCtlCmd *PgCtlCommand
	mode     viperutil.Value[string]
}

// AddStopCommand adds the stop subcommand to the root command
func AddStopCommand(root *cobra.Command, pc *PgCtlCommand) {
	stopCmd := &PgCtlStopCmd{
		pgCtlCmd: pc,
		mode: viperutil.Configure("mode", viperutil.Options[string]{
			Default:  "fast",
			FlagName: "mode",
			Dynamic:  false,
		}),
	}
	root.AddCommand(stopCmd.createCommand())
}

func (s *PgCtlStopCmd) createCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop PostgreSQL server",
		Long: `Stop a running PostgreSQL server instance.

The stop command gracefully shuts down a running PostgreSQL server using pg_ctl.
Configuration can be provided via config file, environment variables, or CLI flags.
CLI flags take precedence over config file and environment variable settings.

Shutdown modes:
  smart:     Disallow new connections, wait for existing sessions to finish
  fast:      Disallow new connections, terminate existing sessions
  immediate: Force shutdown without cleanup (may cause recovery on restart)

Examples:
  # Stop with default settings (fast mode)
  pgctld stop --pg-data-dir /var/lib/postgresql/data

  # Stop with smart mode (wait for sessions)
  pgctld stop --pg-data-dir /var/lib/postgresql/data --mode smart

  # Stop with custom timeout
  pgctld stop --pg-data-dir /var/lib/postgresql/data --timeout 120

  # Force immediate stop with short timeout
  pgctld stop --pg-data-dir /var/lib/postgresql/data --mode immediate --timeout 10`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return s.pgCtlCmd.validateInitialized(cmd, args)
		},
		RunE: s.runStop,
	}

	cmd.Flags().String("mode", s.mode.Default(), "Shutdown mode: smart, fast, or immediate")
	viperutil.BindFlags(cmd.Flags(), s.mode)

	return cmd
}

func (s *PgCtlStopCmd) runStop(cmd *cobra.Command, args []string) error {
	config, err := NewPostgresCtlConfigFromDefaults(s.pgCtlCmd.GetPoolerDir(), s.pgCtlCmd.pgPort.Get(), s.pgCtlCmd.pgListenAddresses.Get(), s.pgCtlCmd.pgUser.Get(), s.pgCtlCmd.pgDatabase.Get(), s.pgCtlCmd.timeout.Get())
	if err != nil {
		return err
	}

	// Default mode to "fast" if not specified
	mode := s.mode.Get()
	if mode == "" {
		mode = "fast"
	}

	// Use daemon manager for CLI commands (production mode)
	manager := &DaemonPostgresManager{}
	result, err := StopPostgreSQLWithResult(s.pgCtlCmd.lg.GetLogger(), manager, config, mode)
	if err != nil {
		return err
	}

	// Display appropriate message for CLI users
	if result.WasRunning {
		fmt.Printf("PostgreSQL server stopped successfully (mode: %s)\n", mode)
	} else {
		fmt.Println("PostgreSQL is not running")
	}

	return nil
}

// takeCheckpoint executes a CHECKPOINT command to ensure all data is written to disk before shutdown
func takeCheckpoint(logger *slog.Logger, config *pgctld.PostgresCtlConfig) error {
	logger.Info("Taking checkpoint before stopping PostgreSQL", "data_dir", config.PostgresDataDir)

	// Use Unix socket connection for psql
	socketDir := pgctld.PostgresSocketDir(config.PoolerDir)
	args := []string{
		"-h", socketDir,
		"-p", fmt.Sprintf("%d", config.Port), // Need port even for socket connections
		"-U", config.User,
		"-d", config.Database,
		"-c", "CHECKPOINT;",
		"-q", // quiet mode - suppress messages
	}

	cmd := exec.Command("psql", args...)

	// Capture output to avoid cluttering the terminal
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("checkpoint command failed: %w, output: %s", err, string(output))
	}

	logger.Info("Checkpoint completed successfully", "data_dir", config.PostgresDataDir)
	return nil
}
