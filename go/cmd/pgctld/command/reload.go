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
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"syscall"

	"github.com/multigres/multigres/go/pgctld"

	"github.com/spf13/cobra"
)

// ReloadResult contains the result of reloading PostgreSQL configuration
type ReloadResult struct {
	WasRunning bool
	Message    string
}

// PgCtlReloadCmd holds the reload command configuration
type PgCtlReloadCmd struct {
	pgCtlCmd *PgCtlCommand
}

// AddReloadCommand adds the reload subcommand to the root command
func AddReloadCommand(root *cobra.Command, pc *PgCtlCommand) {
	reloadCmd := &PgCtlReloadCmd{
		pgCtlCmd: pc,
	}
	root.AddCommand(reloadCmd.createCommand())
}

func (r *PgCtlReloadCmd) createCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "reload-config",
		Short: "Reload PostgreSQL configuration",
		Long: `Reload the PostgreSQL server configuration without restarting.

This command sends a SIGHUP signal to the PostgreSQL process, causing it to re-read
its configuration files. This allows configuration changes to take effect without
stopping and starting the server. Configuration can be provided via config file,
environment variables, or CLI flags. CLI flags take precedence over config file
and environment variable settings.

Examples:
  # Reload configuration with default settings
  pgctld reload-config --data-dir /var/lib/postgresql/data

  # Reload configuration for specific instance
  pgctld reload-config -d /var/lib/postgresql/instance2/data`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return r.pgCtlCmd.validateInitialized(cmd, args)
		},
		RunE: r.runReload,
	}
}

// ReloadPostgreSQLConfigWithResult reloads PostgreSQL configuration and returns detailed result information
func ReloadPostgreSQLConfigWithResult(logger *slog.Logger, config *pgctld.PostgresCtlConfig) (*ReloadResult, error) {
	result := &ReloadResult{}

	// Check if PostgreSQL is running
	if !isPostgreSQLRunning(config.PostgresDataDir) {
		result.WasRunning = false
		result.Message = "PostgreSQL is not running"
		return result, fmt.Errorf("PostgreSQL is not running")
	}

	result.WasRunning = true
	logger.Info("Reloading PostgreSQL configuration", "data_dir", config.PostgresDataDir)

	if err := reloadPostgreSQLConfig(logger, config.PostgresDataDir); err != nil {
		return nil, fmt.Errorf("failed to reload PostgreSQL configuration: %w", err)
	}

	result.Message = "PostgreSQL configuration reloaded successfully"
	logger.Info("PostgreSQL configuration reloaded successfully")
	return result, nil
}

func (r *PgCtlReloadCmd) runReload(cmd *cobra.Command, args []string) error {
	config, err := NewPostgresCtlConfigFromDefaults(r.pgCtlCmd.GetPoolerDir(), r.pgCtlCmd.pgPort.Get(), r.pgCtlCmd.pgListenAddresses.Get(), r.pgCtlCmd.pgUser.Get(), r.pgCtlCmd.pgDatabase.Get(), r.pgCtlCmd.timeout.Get())
	if err != nil {
		return err
	}

	result, err := ReloadPostgreSQLConfigWithResult(r.pgCtlCmd.lg.GetLogger(), config)
	if err != nil {
		return err
	}

	// Display appropriate message for CLI users
	if result.WasRunning {
		fmt.Printf("PostgreSQL configuration reloaded successfully\n")
	} else {
		fmt.Printf("PostgreSQL is not running\n")
	}

	return nil
}

func reloadPostgreSQLConfig(logger *slog.Logger, dataDir string) error {
	// First try using pg_ctl
	if err := reloadWithPgCtl(dataDir); err != nil {
		logger.Warn("pg_ctl reload failed, trying direct signal approach", "error", err)
		return reloadWithSignal(dataDir)
	}
	return nil
}

func reloadWithPgCtl(dataDir string) error {
	args := []string{
		"reload",
		"-D", dataDir,
	}

	cmd := exec.CommandContext(context.TODO(), "pg_ctl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func reloadWithSignal(dataDir string) error {
	// Read PID from postmaster.pid file
	pid, err := readPostmasterPID(dataDir)
	if err != nil {
		return fmt.Errorf("failed to read postmaster PID: %w", err)
	}

	// Find the process
	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("failed to find process %d: %w", pid, err)
	}

	// Send SIGHUP signal to reload configuration
	if err := process.Signal(syscall.SIGHUP); err != nil {
		return fmt.Errorf("failed to send SIGHUP signal to process %d: %w", pid, err)
	}

	return nil
}
