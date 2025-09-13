// Copyright 2025 The Multigres Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"os/exec"
	"path/filepath"
	"time"

	"github.com/multigres/multigres/go/pgctld"

	"github.com/spf13/cobra"
)

// StatusResult contains the result of checking PostgreSQL status
type StatusResult struct {
	Status        string // "NOT_INITIALIZED", "STOPPED", "RUNNING"
	PID           int
	Version       string
	UptimeSeconds int64
	DataDir       string
	Port          int
	Host          string
	Ready         bool
	Message       string
}

func init() {
	Root.AddCommand(statusCmd)

	// Add status-specific flags
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check PostgreSQL server status",
	Long: `Check the status of the PostgreSQL server instance and report health information.

The status command checks if PostgreSQL is running and reports detailed information
including PID, version, uptime, and connection readiness. Configuration can be
provided via config file, environment variables, or CLI flags.
CLI flags take precedence over config file and environment variable settings.

Examples:
  # Check status with default settings
  pgctld status --pooler-dir /var/lib/poolerdir/

  # Check status of PostgreSQL on custom port
  pgctld status --pooler-dir/var/lib/poolerdir/ --port 5433

  # Check status with specific connection parameters
  pgctld status -d /var/lib/poolerdir/ -H remotehost -U admin --pg-database mydb

  # Check status of multiple instances
  pgctld status -d /var/lib/poolerdir/instance1 -p 5432
  pgctld status -d /var/lib/poolerdir/instance2 -p 5433`,
	PreRunE: validateInitialized,
	RunE:    runStatus,
}

// GetStatusWithResult gets PostgreSQL status with the given configuration and returns detailed result information
func GetStatusWithResult(config *pgctld.PostgresCtlConfig) (*StatusResult, error) {
	logger := slog.Default()
	result := &StatusResult{
		DataDir: config.PostgresDataDir,
		Port:    config.Port,
		Host:    config.Host,
	}

	// Check if PostgreSQL is running
	if !isPostgreSQLRunning(config.PostgresDataDir) {
		result.Status = "STOPPED"
		result.Message = "PostgreSQL server is stopped"
		return result, nil
	}

	// Server is running
	result.Status = "RUNNING"
	result.Message = "PostgreSQL server is running"

	// Get PID if running
	if pid, err := readPostmasterPID(config.PostgresDataDir); err == nil {
		result.PID = pid
	} else {
		logger.Warn("Could not read postmaster PID", "error", err)
	}

	// Check if server is accepting connections
	result.Ready = isServerReadyWithConfig(config)

	// Get server version if possible
	result.Version = getServerVersionWithConfig(config)

	// Get uptime (approximate based on pidfile mtime)
	pidFile := filepath.Join(config.PostgresDataDir, "postmaster.pid")
	if stat, err := os.Stat(pidFile); err == nil {
		result.UptimeSeconds = int64(time.Since(stat.ModTime()).Seconds())
	}

	return result, nil
}

func runStatus(cmd *cobra.Command, args []string) error {
	config, err := NewPostgresCtlConfigFromDefaults()
	if err != nil {
		return err
	}
	// No local flag overrides needed - all flags are global now

	result, err := GetStatusWithResult(config)
	if err != nil {
		return err
	}

	// Display status for CLI users
	var statusDisplay string
	switch result.Status {
	case "STOPPED":
		statusDisplay = "Stopped"
	case "RUNNING":
		statusDisplay = "Running"
	default:
		statusDisplay = result.Status
	}

	fmt.Printf("Status: %s\n", statusDisplay)
	fmt.Printf("Data directory: %s", result.DataDir)

	switch result.Status {
	case "STOPPED":
		fmt.Printf("\n")
	case "RUNNING":
		fmt.Printf("\n")
		if result.PID > 0 {
			fmt.Printf("PID: %d\n", result.PID)
		}
		fmt.Printf("Port: %d\n", result.Port)
		fmt.Printf("Host: %s\n", result.Host)
		if result.Version != "" {
			fmt.Printf("Version: %s\n", result.Version)
		}
		if result.UptimeSeconds > 0 {
			fmt.Printf("Uptime: %s\n", formatUptime(result.UptimeSeconds))
		}
		if result.Ready {
			fmt.Printf("Ready: Yes\n")
		} else {
			fmt.Printf("Ready: No (server may be starting or in recovery)\n")
		}
	}

	return nil
}

// formatUptime formats uptime seconds into human-readable format
func formatUptime(seconds int64) string {
	duration := time.Duration(seconds) * time.Second
	days := int(duration.Hours()) / 24
	hours := int(duration.Hours()) % 24
	minutes := int(duration.Minutes()) % 60

	if days > 0 {
		return fmt.Sprintf("%d days, %d hours, %d minutes", days, hours, minutes)
	} else if hours > 0 {
		return fmt.Sprintf("%d hours, %d minutes", hours, minutes)
	} else {
		return fmt.Sprintf("%d minutes", minutes)
	}
}

func isServerReadyWithConfig(config *pgctld.PostgresCtlConfig) bool {
	// Use Unix socket connection for pg_isready
	socketDir := pgctld.PostgresSocketDir(config.PoolerDir)
	cmd := exec.Command("pg_isready",
		"-h", socketDir,
		"-p", fmt.Sprintf("%d", config.Port), // Need port even for socket connections
		"-U", config.User,
		"-d", config.Database,
	)

	return cmd.Run() == nil
}

func getServerVersionWithConfig(config *pgctld.PostgresCtlConfig) string {
	// Use Unix socket connection for psql
	socketDir := pgctld.PostgresSocketDir(config.PoolerDir)
	cmd := exec.Command("psql",
		"-h", socketDir,
		"-p", fmt.Sprintf("%d", config.Port), // Need port even for socket connections
		"-U", config.User,
		"-d", config.Database,
		"-t", "-c", "SELECT version()",
	)

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
