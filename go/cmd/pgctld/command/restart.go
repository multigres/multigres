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
	"os"
	"path/filepath"

	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/spf13/cobra"
)

// RestartResult contains the result of restarting PostgreSQL
type RestartResult struct {
	PID          int
	StoppedFirst bool
	Message      string
}

// PgCtlRestartCmd holds the restart command configuration
type PgCtlRestartCmd struct {
	pgCtlCmd *PgCtlCommand
	mode     viperutil.Value[string]
}

// AddRestartCommand adds the restart subcommand to the root command
func AddRestartCommand(root *cobra.Command, pc *PgCtlCommand) {
	restartCmd := &PgCtlRestartCmd{
		pgCtlCmd: pc,
		mode: viperutil.Configure(pc.reg, "restart-mode", viperutil.Options[string]{
			Default:  "fast",
			FlagName: "mode",
			Dynamic:  false,
		}),
	}
	root.AddCommand(restartCmd.createCommand())
}

func (r *PgCtlRestartCmd) createCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restart",
		Short: "Restart PostgreSQL server",
		Long: `Restart the PostgreSQL server by stopping it and then starting it again as a standby.

The restart command performs a stop followed by start operation in sequence.
PostgreSQL always starts in standby (recovery) mode, ensuring it never becomes
a primary unexpectedly. Configuration can be provided via config file, environment
variables, or CLI flags. CLI flags take precedence over config file and environment
variable settings.

Examples:
  # Restart with default settings
  pgctld restart --pg-data-dir /var/lib/postgresql/data

  # Restart on custom port with smart stop mode
  pgctld restart --pg-data-dir /var/lib/postgresql/data --port 5433 --mode smart

  # Restart with custom config and timeout
  pgctld restart -d /var/lib/postgresql/data -c /etc/multigres/pgctld.yaml --timeout 60 --pg-config-file /etc/postgresql/custom.conf`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return r.pgCtlCmd.validateInitialized(cmd, args)
		},
		RunE: r.runRestart,
	}

	cmd.Flags().String("mode", r.mode.Default(), "Shutdown mode for stop phase: smart, fast, or immediate")
	viperutil.BindFlags(cmd.Flags(), r.mode)

	return cmd
}

// RestartPostgreSQLWithResult restarts PostgreSQL and returns detailed result information.
// PostgreSQL always restarts in standby (recovery) mode — standby.signal is written
// before the new process starts, so it never enters primary mode unexpectedly.
func RestartPostgreSQLWithResult(logger *slog.Logger, config *pgctld.PostgresCtlConfig, mode string) (*RestartResult, error) {
	result := &RestartResult{}

	logger.Info("Restarting PostgreSQL server", "data_dir", config.PostgresDataDir, "mode", mode)

	// Stop the server if it's running
	if isPostgreSQLRunning(config.PostgresDataDir) {
		logger.Info("Stopping PostgreSQL server")
		stopResult, err := StopPostgreSQLWithResult(logger, config, mode)
		if err != nil {
			return nil, fmt.Errorf("failed to stop PostgreSQL during restart: %w", err)
		}
		result.StoppedFirst = stopResult.WasRunning
	} else {
		logger.Info("PostgreSQL is not running, proceeding with start")
	}

	// Always write standby.signal so postgres starts in recovery mode, never as primary.
	standbySignalPath := filepath.Join(config.PostgresDataDir, "standby.signal")
	if err := os.WriteFile(standbySignalPath, []byte(""), 0o644); err != nil {
		return nil, fmt.Errorf("failed to create standby.signal: %w", err)
	}

	startResult, err := StartPostgreSQLWithResult(logger, config)
	if err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL during restart: %w", err)
	}

	result.PID = startResult.PID
	result.Message = "PostgreSQL server restarted successfully"
	logger.Info("PostgreSQL server restarted successfully", "pid", result.PID)

	return result, nil
}

func (r *PgCtlRestartCmd) runRestart(cmd *cobra.Command, args []string) error {
	config, err := NewPostgresCtlConfigFromDefaults(r.pgCtlCmd.GetPoolerDir(), r.pgCtlCmd.pgPort.Get(), r.pgCtlCmd.pgListenAddresses.Get(), r.pgCtlCmd.pgUser.Get(), r.pgCtlCmd.pgDatabase.Get(), r.pgCtlCmd.timeout.Get())
	if err != nil {
		return err
	}
	result, err := RestartPostgreSQLWithResult(r.pgCtlCmd.lg.GetLogger(), config, r.mode.Get())
	if err != nil {
		return err
	}

	if result.StoppedFirst {
		fmt.Printf("PostgreSQL server restarted successfully (PID: %d, mode: %s)\n", result.PID, r.mode.Get())
	} else {
		fmt.Printf("PostgreSQL server started successfully (PID: %d) - was not previously running\n", result.PID)
	}

	return nil
}
