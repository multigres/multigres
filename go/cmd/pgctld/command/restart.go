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

	"github.com/multigres/multigres/go/viperutil"

	"github.com/spf13/cobra"
)

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
		mode: viperutil.Configure("restart-mode", viperutil.Options[string]{
			Default:  "fast",
			FlagName: "mode",
			Dynamic:  false,
		}),
		asStandby: viperutil.Configure("as-standby", viperutil.Options[bool]{
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

func (r *PgCtlRestartCmd) runRestart(cmd *cobra.Command, args []string) error {
	config, err := NewPostgresCtlConfigFromDefaults(r.pgCtlCmd.GetPoolerDir(), r.pgCtlCmd.pgPort.Get(), r.pgCtlCmd.pgListenAddresses.Get(), r.pgCtlCmd.pgUser.Get(), r.pgCtlCmd.pgDatabase.Get(), r.pgCtlCmd.timeout.Get())
	if err != nil {
		return err
	}

	// Use daemon manager for CLI commands (production mode)
	manager := &DaemonPostgresManager{}
	result, err := RestartPostgreSQL(r.pgCtlCmd.lg.GetLogger(), manager, config, r.mode.Get(), r.asStandby.Get())
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
