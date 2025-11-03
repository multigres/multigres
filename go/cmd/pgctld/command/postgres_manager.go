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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/pgctld"
)

// PostgresManager defines the interface for managing PostgreSQL processes
type PostgresManager interface {
	// Start starts the PostgreSQL process
	Start(logger *slog.Logger, config *pgctld.PostgresCtlConfig) error
	// Stop stops the PostgreSQL process
	Stop(logger *slog.Logger, config *pgctld.PostgresCtlConfig, mode string) error
	// IsRunning checks if PostgreSQL is running
	IsRunning(config *pgctld.PostgresCtlConfig) bool
	// ReadPID reads the PostgreSQL PID
	ReadPID(config *pgctld.PostgresCtlConfig) (int, error)
}

// DaemonPostgresManager manages PostgreSQL using pg_ctl (production mode)
type DaemonPostgresManager struct{}

func (d *DaemonPostgresManager) Start(logger *slog.Logger, config *pgctld.PostgresCtlConfig) error {
	// Use pg_ctl to start PostgreSQL properly as a daemon
	postgresOpts := fmt.Sprintf("-c config_file=%s -c port=%d -c listen_addresses=%s -c unix_socket_directories=%s",
		config.PostgresConfigFile, config.Port, config.ListenAddresses, config.UnixSocketDirectories)

	args := []string{
		"start",
		"-D", config.PostgresDataDir,
		"-o", postgresOpts,
		"-l", filepath.Join(config.PostgresDataDir, "postgresql.log"),
		"-W", // don't wait - we'll check readiness ourselves
	}

	logger.Info("Starting PostgreSQL with pg_ctl", "port", config.Port, "dataDir", config.PostgresDataDir)

	cmd := exec.Command("pg_ctl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start PostgreSQL with pg_ctl: %w", err)
	}

	return nil
}

func (d *DaemonPostgresManager) Stop(logger *slog.Logger, config *pgctld.PostgresCtlConfig, mode string) error {
	// Use pg_ctl stop
	args := []string{
		"stop",
		"-D", config.PostgresDataDir,
		"-m", mode,
	}

	logger.Info("Stopping PostgreSQL with pg_ctl", "mode", mode, "dataDir", config.PostgresDataDir)

	cmd := exec.Command("pg_ctl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to stop PostgreSQL with pg_ctl: %w", err)
	}

	return nil
}

func (d *DaemonPostgresManager) IsRunning(config *pgctld.PostgresCtlConfig) bool {
	return isPostgreSQLRunning(config.PostgresDataDir)
}

func (d *DaemonPostgresManager) ReadPID(config *pgctld.PostgresCtlConfig) (int, error) {
	return readPostmasterPID(config.PostgresDataDir)
}

// SubprocessPostgresManager manages PostgreSQL as a subprocess with run_in_test (test mode)
// The purpose is to help defend against orphan processes that outlive a test run. If we start postgres
// as a daemon and a test run fails or test interrupted (a panic, for example) then the daemon process
// might keep running forever. By starting as a subprocess using the run_in_test.sh wrapper we can
// make sure the postgres process will kill itself after the parent test process dies.
type SubprocessPostgresManager struct {
	cmd     *exec.Cmd // Tracks the running postgres process
	dataDir string    // Tracks which data directory this process is managing
}

// matchesDataDir checks if the manager's tracked data directory matches the requested one
func (f *SubprocessPostgresManager) matchesDataDir(requestedDataDir string) bool {
	if f.cmd == nil || f.dataDir == "" {
		return false
	}
	// Normalize paths for comparison
	abs1, err1 := filepath.Abs(f.dataDir)
	abs2, err2 := filepath.Abs(requestedDataDir)
	if err1 != nil || err2 != nil {
		return f.dataDir == requestedDataDir
	}
	return abs1 == abs2
}

func (f *SubprocessPostgresManager) Start(logger *slog.Logger, config *pgctld.PostgresCtlConfig) error {
	// Check if we already have a process running for a different data directory
	if f.cmd != nil && !f.matchesDataDir(config.PostgresDataDir) {
		return fmt.Errorf("manager already managing a different postgres instance")
	}

	// Build postgres command arguments
	postgresArgs := []string{
		"-D", config.PostgresDataDir,
		"-c", fmt.Sprintf("config_file=%s", config.PostgresConfigFile),
		"-c", fmt.Sprintf("port=%d", config.Port),
		"-c", fmt.Sprintf("listen_addresses=%s", config.ListenAddresses),
		"-c", fmt.Sprintf("unix_socket_directories=%s", config.UnixSocketDirectories),
	}

	// Wrap with run_in_test.sh - expected to be in PATH
	wrapperPath := "run_in_test.sh"
	cmdArgs := append([]string{"postgres"}, postgresArgs...)

	logger.Info("Starting PostgreSQL in subprocess test mode", "wrapper", wrapperPath, "port", config.Port)

	f.cmd = exec.Command(wrapperPath, cmdArgs...)
	f.dataDir = config.PostgresDataDir

	// Redirect logs to file
	logFile, err := os.OpenFile(filepath.Join(config.PostgresDataDir, "postgresql.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	f.cmd.Stdout = logFile
	f.cmd.Stderr = logFile

	// Start the process
	if err := f.cmd.Start(); err != nil {
		logFile.Close()
		f.cmd = nil
		f.dataDir = ""
		return fmt.Errorf("failed to start PostgreSQL in subprocess mode: %w", err)
	}

	logger.Info("PostgreSQL started in subprocess mode", "pid", f.cmd.Process.Pid, "dataDir", f.dataDir)
	return nil
}

func (f *SubprocessPostgresManager) Stop(logger *slog.Logger, config *pgctld.PostgresCtlConfig, mode string) error {
	// Validate data directory matches
	if !f.matchesDataDir(config.PostgresDataDir) {
		// No matching process to stop
		return fmt.Errorf("no postgres process running for data directory: %s", config.PostgresDataDir)
	}

	if f.cmd == nil || f.cmd.Process == nil {
		return fmt.Errorf("no subprocess postgres process to stop")
	}

	logger.Info("Stopping subprocess PostgreSQL process", "pid", f.cmd.Process.Pid, "mode", mode, "dataDir", f.dataDir)

	// Send appropriate signal based on mode
	var sig syscall.Signal
	switch mode {
	case "smart":
		sig = syscall.SIGTERM
	case "fast":
		sig = syscall.SIGINT
	case "immediate":
		sig = syscall.SIGQUIT
	default:
		sig = syscall.SIGINT // default to fast
	}

	if err := f.cmd.Process.Signal(sig); err != nil {
		return fmt.Errorf("failed to send signal to postgres: %w", err)
	}

	// Wait for process to exit
	_ = f.cmd.Wait()
	f.cmd = nil
	f.dataDir = ""

	logger.Info("Subprocess PostgreSQL process stopped")
	return nil
}

func (f *SubprocessPostgresManager) IsRunning(config *pgctld.PostgresCtlConfig) bool {
	// Only report as running if data directory matches
	if !f.matchesDataDir(config.PostgresDataDir) {
		return false
	}

	// Check if our tracked process is still running
	if f.cmd != nil && f.cmd.Process != nil {
		if err := f.cmd.Process.Signal(syscall.Signal(0)); err == nil {
			return true
		}
	}

	// Process died, clean up
	f.cmd = nil
	f.dataDir = ""
	return false
}

func (f *SubprocessPostgresManager) ReadPID(config *pgctld.PostgresCtlConfig) (int, error) {
	// Only return PID if data directory matches
	if !f.matchesDataDir(config.PostgresDataDir) {
		return 0, fmt.Errorf("no postgres process running for data directory: %s", config.PostgresDataDir)
	}

	// If we have a tracked process, return its PID
	if f.cmd != nil && f.cmd.Process != nil {
		return f.cmd.Process.Pid, nil
	}

	return 0, fmt.Errorf("no subprocess postgres process")
}

// Helper functions used by both implementations

func isPostgreSQLRunning(dataDir string) bool {
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	if _, err := os.Stat(pidFile); err != nil {
		return false
	}

	pid, err := readPostmasterPID(dataDir)
	if err != nil {
		return false
	}

	return isProcessRunning(pid)
}

func readPostmasterPID(dataDir string) (int, error) {
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	content, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, err
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return 0, fmt.Errorf("empty postmaster.pid file")
	}

	pid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil {
		return 0, fmt.Errorf("invalid PID in postmaster.pid: %s", lines[0])
	}

	return pid, nil
}

func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// StartPostgreSQLResult contains the result of starting PostgreSQL
type StartPostgreSQLResult struct {
	PID            int
	AlreadyRunning bool
	Message        string
}

// StartPostgreSQLWithResult starts PostgreSQL if not already running, using the provided manager
func StartPostgreSQLWithResult(logger *slog.Logger, manager PostgresManager, config *pgctld.PostgresCtlConfig) (*StartPostgreSQLResult, error) {
	result := &StartPostgreSQLResult{}

	// Check if already running
	if manager.IsRunning(config) {
		logger.Info("PostgreSQL is already running")
		result.AlreadyRunning = true
		result.Message = "PostgreSQL is already running"

		if pid, err := manager.ReadPID(config); err == nil {
			result.PID = pid
		}

		return result, nil
	}

	// Start PostgreSQL
	logger.Info("Starting PostgreSQL server", "data_dir", config.PostgresDataDir)
	if err := manager.Start(logger, config); err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	// Wait for server to be ready
	logger.Info("Waiting for PostgreSQL to be ready")
	if err := waitForPostgreSQLWithConfig(config); err != nil {
		return nil, fmt.Errorf("PostgreSQL failed to become ready: %w", err)
	}

	// Get PID
	if pid, err := manager.ReadPID(config); err == nil {
		result.PID = pid
	}

	result.Message = "PostgreSQL server started successfully"
	logger.Info("PostgreSQL server started successfully")
	return result, nil
}

// waitForPostgreSQLWithConfig waits for PostgreSQL to be ready using pg_isready
func waitForPostgreSQLWithConfig(config *pgctld.PostgresCtlConfig) error {
	socketDir := pgctld.PostgresSocketDir(config.PoolerDir)
	for i := 0; i < config.Timeout; i++ {
		cmd := exec.Command("pg_isready",
			"-h", socketDir,
			"-p", fmt.Sprintf("%d", config.Port),
			"-U", config.User,
			"-d", config.Database,
		)

		if err := cmd.Run(); err == nil {
			return nil
		}

		// Sleep before next attempt
		if i < config.Timeout-1 {
			time.Sleep(1 * time.Second)
		}
	}

	return fmt.Errorf("PostgreSQL did not become ready within %d seconds", config.Timeout)
}

// StopResult contains the result of stopping PostgreSQL
type StopResult struct {
	WasRunning bool
	Message    string
}

// StopPostgreSQLWithResult stops PostgreSQL if it's running, using the provided manager
func StopPostgreSQLWithResult(logger *slog.Logger, manager PostgresManager, config *pgctld.PostgresCtlConfig, mode string) (*StopResult, error) {
	result := &StopResult{}

	// Check if PostgreSQL is running
	if !manager.IsRunning(config) {
		logger.Info("PostgreSQL is not running")
		result.WasRunning = false
		result.Message = "PostgreSQL is not running"
		return result, nil
	}

	// Stop PostgreSQL
	logger.Info("Stopping PostgreSQL server", "mode", mode)
	if err := manager.Stop(logger, config, mode); err != nil {
		return nil, fmt.Errorf("failed to stop PostgreSQL: %w", err)
	}

	result.WasRunning = true
	result.Message = "PostgreSQL server stopped successfully\n"
	logger.Info("PostgreSQL stopped successfully")
	return result, nil
}

// RestartPostgreSQLResult contains the result of restarting PostgreSQL
type RestartPostgreSQLResult struct {
	PID          int
	StoppedFirst bool
	Message      string
}

// RestartPostgreSQL restarts PostgreSQL using the provided manager
func RestartPostgreSQL(logger *slog.Logger, manager PostgresManager, config *pgctld.PostgresCtlConfig, mode string, asStandby bool) (*RestartPostgreSQLResult, error) {
	result := &RestartPostgreSQLResult{}

	if asStandby {
		logger.Info("Restarting PostgreSQL server as standby", "data_dir", config.PostgresDataDir, "mode", mode)
	} else {
		logger.Info("Restarting PostgreSQL server", "data_dir", config.PostgresDataDir, "mode", mode)
	}

	// Stop the server if it's running
	if manager.IsRunning(config) {
		logger.Info("Stopping PostgreSQL server")
		stopResult, err := StopPostgreSQLWithResult(logger, manager, config, mode)
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
	startResult, err := StartPostgreSQLWithResult(logger, manager, config)
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
