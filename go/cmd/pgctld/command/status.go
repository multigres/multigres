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
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	Root.AddCommand(statusCmd)
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check PostgreSQL server status",
	Long:  `Check the status of the PostgreSQL server instance and report health information.`,
	RunE:  runStatus,
}

func runStatus(cmd *cobra.Command, args []string) error {
	logger := slog.Default()

	dataDir := viper.GetString("data-dir")
	if dataDir == "" {
		return fmt.Errorf("data-dir is required")
	}

	// Check if data directory is initialized
	if !isDataDirInitialized(dataDir) {
		fmt.Printf("Status: Not initialized\n")
		fmt.Printf("Data directory: %s (not initialized)\n", dataDir)
		return nil
	}

	// Check if PostgreSQL is running
	if !isPostgreSQLRunning(dataDir) {
		fmt.Printf("Status: Stopped\n")
		fmt.Printf("Data directory: %s\n", dataDir)
		return nil
	}

	// Get PID if running
	pid, err := readPostmasterPID(dataDir)
	if err != nil {
		logger.Warn("Could not read postmaster PID", "error", err)
		pid = 0
	}

	// Check if server is accepting connections
	isReady := isServerReady()

	// Get server version if possible
	version := getServerVersion()

	// Get uptime
	uptime := getServerUptime(dataDir)

	// Display status
	fmt.Printf("Status: Running\n")
	fmt.Printf("Data directory: %s\n", dataDir)
	if pid > 0 {
		fmt.Printf("PID: %d\n", pid)
	}
	fmt.Printf("Port: %d\n", viper.GetInt("pg-port"))
	fmt.Printf("Host: %s\n", viper.GetString("pg-host"))
	if version != "" {
		fmt.Printf("Version: %s\n", version)
	}
	if uptime != "" {
		fmt.Printf("Uptime: %s\n", uptime)
	}
	if isReady {
		fmt.Printf("Ready: Yes\n")
	} else {
		fmt.Printf("Ready: No (server may be starting or in recovery)\n")
	}

	return nil
}

func isServerReady() bool {
	// Legacy function - use viper config
	config := NewPostgresConfigFromViper()
	return isServerReadyWithConfig(config)
}

func isServerReadyWithConfig(config *PostgresConfig) bool {
	cmd := exec.Command("pg_isready",
		"-h", config.Host,
		"-p", fmt.Sprintf("%d", config.Port),
		"-U", config.User,
		"-d", config.Database,
	)

	return cmd.Run() == nil
}

func getServerVersion() string {
	// Legacy function - use viper config
	config := NewPostgresConfigFromViper()
	return getServerVersionWithConfig(config)
}

func getServerVersionWithConfig(config *PostgresConfig) string {
	cmd := exec.Command("psql",
		"-h", config.Host,
		"-p", fmt.Sprintf("%d", config.Port),
		"-U", config.User,
		"-d", config.Database,
		"-t", "-c", "SELECT version()",
	)

	// Set environment to include PGPASSWORD if available
	cmd.Env = os.Environ()

	output, err := cmd.Output()
	if err != nil {
		return ""
	}

	return string(output)
}

func getServerUptime(dataDir string) string {
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	stat, err := os.Stat(pidFile)
	if err != nil {
		return ""
	}

	startTime := stat.ModTime()
	uptime := time.Since(startTime)

	// Format uptime in human-readable format
	days := int(uptime.Hours()) / 24
	hours := int(uptime.Hours()) % 24
	minutes := int(uptime.Minutes()) % 60

	if days > 0 {
		return fmt.Sprintf("%d days, %d hours, %d minutes", days, hours, minutes)
	} else if hours > 0 {
		return fmt.Sprintf("%d hours, %d minutes", hours, minutes)
	} else {
		return fmt.Sprintf("%d minutes", minutes)
	}
}
