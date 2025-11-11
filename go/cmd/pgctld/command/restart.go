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

	"github.com/multigres/multigres/go/pgctld"
	"github.com/multigres/multigres/go/viperutil"

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
	pgCtlCmd  *PgCtlCommand
	mode      viperutil.Value[string]
	asStandby viperutil.Value[bool]
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
		asStandby: viperutil.Configure(pc.reg, "as-standby", viperutil.Options[bool]{
			Default:  false,
			FlagName: "as-standby",
			Dynamic:  false,
		}),
	}
	root.AddCommand(restartCmd.createCommand())
}

func (r *PgCtlRestartCmd) createCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restart",
		Short: "Restart PostgreSQL server",
		Long: `Restart the PostgreSQL server by stopping it and then starting it again.

The restart command performs a stop followed by start operation in sequence.
Configuration can be provided via config file, environment variables, or CLI flags.
CLI flags take precedence over config file and environment variable settings.

Examples:
  # Restart with default settings
  pgctld restart --pg-data-dir /var/lib/postgresql/data

  # Restart on custom port with smart stop mode
  pgctld restart --pg-data-dir /var/lib/postgresql/data --port 5433 --mode smart

  # Restart with custom config and timeout
  pgctld restart -d /var/lib/postgresql/data -c /etc/multigres/pgctld.yaml --timeout 60 --pg-config-file /etc/postgresql/custom.conf

  # Restart with immediate stop and custom socket directory
  pgctld restart -d /data --mode immediate -s /var/run/postgresql

  # Restart as standby (for demotion)
  pgctld restart --pg-data-dir /var/lib/postgresql/data --as-standby`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return r.pgCtlCmd.validateInitialized(cmd, args)
		},
		RunE: r.runRestart,
	}

	cmd.Flags().String("mode", r.mode.Default(), "Shutdown mode for stop phase: smart, fast, or immediate")
	cmd.Flags().Bool("as-standby", r.asStandby.Default(), "Create standby.signal to restart as standby (for demotion)")
	viperutil.BindFlags(cmd.Flags(), r.mode, r.asStandby)

	return cmd
}

// RestartPostgreSQLWithResult restarts PostgreSQL with the given configuration and returns detailed result information
func RestartPostgreSQLWithResult(logger *slog.Logger, config *pgctld.PostgresCtlConfig, mode string, asStandby bool) (*RestartResult, error) {
	result := &RestartResult{}

	if asStandby {
		logger.Info("Restarting PostgreSQL server as standby", "data_dir", config.PostgresDataDir, "mode", mode)
	} else {
		logger.Info("Restarting PostgreSQL server", "data_dir", config.PostgresDataDir, "mode", mode)
	}

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
		result.StoppedFirst = false
	}

	// Create standby.signal if restarting as standby
	if asStandby {
		standbySignalPath := filepath.Join(config.PostgresDataDir, "standby.signal")
		logger.Info("Creating standby.signal file", "path", standbySignalPath)
		if err := os.WriteFile(standbySignalPath, []byte(""), 0o644); err != nil {
			return nil, fmt.Errorf("failed to create standby.signal: %w", err)
		}
		logger.Info("standby.signal created successfully")
	}

	// Start the server
	if asStandby {
		logger.Info("Starting PostgreSQL server as standby")
	} else {
		logger.Info("Starting PostgreSQL server")
	}
	startResult, err := StartPostgreSQLWithResult(logger, config)
	if err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL during restart: %w", err)
	}

	result.PID = startResult.PID
	if asStandby {
		result.Message = "PostgreSQL server restarted as standby successfully"
		logger.Info("PostgreSQL server restarted as standby successfully")
	} else {
		result.Message = "PostgreSQL server restarted successfully"
		logger.Info("PostgreSQL server restarted successfully")
	}

	return result, nil
}

func (r *PgCtlRestartCmd) runRestart(cmd *cobra.Command, args []string) error {
	config, err := NewPostgresCtlConfigFromDefaults(r.pgCtlCmd.GetPoolerDir(), r.pgCtlCmd.pgPort.Get(), r.pgCtlCmd.pgListenAddresses.Get(), r.pgCtlCmd.pgUser.Get(), r.pgCtlCmd.pgDatabase.Get(), r.pgCtlCmd.timeout.Get())
	if err != nil {
		return err
	}
	result, err := RestartPostgreSQLWithResult(r.pgCtlCmd.lg.GetLogger(), config, r.mode.Get(), r.asStandby.Get())
	if err != nil {
		return err
	}

	// Display appropriate message for CLI users
	if r.asStandby.Get() {
		fmt.Printf("PostgreSQL server restarted as standby successfully (PID: %d, mode: %s)\n", result.PID, r.mode.Get())
	} else if result.StoppedFirst {
		fmt.Printf("PostgreSQL server restarted successfully (PID: %d, mode: %s)\n", result.PID, r.mode.Get())
	} else {
		fmt.Printf("PostgreSQL server started successfully (PID: %d) - was not previously running\n", result.PID)
	}

	return nil
}
