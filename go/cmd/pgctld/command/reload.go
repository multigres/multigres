/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package command

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	Root.AddCommand(reloadCmd)
}

var reloadCmd = &cobra.Command{
	Use:   "reload-config",
	Short: "Reload PostgreSQL configuration",
	Long: `Reload the PostgreSQL server configuration without restarting.
This sends a SIGHUP signal to the PostgreSQL process, causing it to re-read
its configuration files.`,
	RunE: runReload,
}

func runReload(cmd *cobra.Command, args []string) error {
	logger := slog.Default()

	dataDir := viper.GetString("data-dir")
	if dataDir == "" {
		return fmt.Errorf("data-dir is required")
	}

	// Check if PostgreSQL is running
	if !isPostgreSQLRunning(dataDir) {
		return fmt.Errorf("PostgreSQL is not running")
	}

	logger.Info("Reloading PostgreSQL configuration", "data_dir", dataDir)

	if err := reloadPostgreSQLConfig(dataDir); err != nil {
		return fmt.Errorf("failed to reload PostgreSQL configuration: %w", err)
	}

	logger.Info("PostgreSQL configuration reloaded successfully")
	return nil
}

func reloadPostgreSQLConfig(dataDir string) error {
	// First try using pg_ctl
	if err := reloadWithPgCtl(dataDir); err != nil {
		slog.Warn("pg_ctl reload failed, trying direct signal approach", "error", err)
		return reloadWithSignal(dataDir)
	}
	return nil
}

func reloadWithPgCtl(dataDir string) error {
	args := []string{
		"reload",
		"-D", dataDir,
	}

	cmd := exec.Command("pg_ctl", args...)
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
