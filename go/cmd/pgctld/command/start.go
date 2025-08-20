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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	pb "github.com/multigres/multigres/go/pb"
)

// PostgresConfig holds all PostgreSQL configuration parameters
type PostgresConfig struct {
	DataDir    string
	Port       int
	Host       string
	User       string
	Database   string
	Password   string
	SocketDir  string
	ConfigFile string
	Timeout    int
}

// NewPostgresConfigFromViper creates a PostgresConfig from current viper settings
func NewPostgresConfigFromViper() *PostgresConfig {
	return &PostgresConfig{
		DataDir:    viper.GetString("data-dir"),
		Port:       viper.GetInt("pg-port"),
		Host:       viper.GetString("pg-host"),
		User:       viper.GetString("pg-user"),
		Database:   viper.GetString("pg-database"),
		Password:   viper.GetString("pg-password"),
		SocketDir:  viper.GetString("socket-dir"),
		ConfigFile: viper.GetString("config-file"),
		Timeout:    viper.GetInt("timeout"),
	}
}

// NewPostgresConfigFromDefaults creates a PostgresConfig with default values and viper fallbacks
func NewPostgresConfigFromDefaults() *PostgresConfig {
	return &PostgresConfig{
		DataDir:    viper.GetString("data-dir"),
		Port:       5432,
		Host:       "localhost",
		User:       "postgres",
		Database:   "postgres",
		Password:   "",
		SocketDir:  "/tmp",
		ConfigFile: "",
		Timeout:    30,
	}
}

// NewPostgresConfigFromStartRequest creates a PostgresConfig from a gRPC StartRequest
func NewPostgresConfigFromStartRequest(req *pb.StartRequest) *PostgresConfig {
	config := NewPostgresConfigFromDefaults()

	// Override with request parameters if provided
	if req.DataDir != "" {
		config.DataDir = req.DataDir
	}
	if req.Port > 0 {
		config.Port = int(req.Port)
	}
	if req.SocketDir != "" {
		config.SocketDir = req.SocketDir
	}
	if req.ConfigFile != "" {
		config.ConfigFile = req.ConfigFile
	}

	return config
}

// NewPostgresConfigFromStopRequest creates a PostgresConfig from a gRPC StopRequest
func NewPostgresConfigFromStopRequest(req *pb.StopRequest) *PostgresConfig {
	config := NewPostgresConfigFromDefaults()

	// Override with request parameters if provided
	if req.DataDir != "" {
		config.DataDir = req.DataDir
	}
	if req.Timeout > 0 {
		config.Timeout = int(req.Timeout)
	}

	return config
}

// NewPostgresConfigFromStatusRequest creates a PostgresConfig from a gRPC StatusRequest
func NewPostgresConfigFromStatusRequest(req *pb.StatusRequest) *PostgresConfig {
	config := NewPostgresConfigFromDefaults()

	// Override with request parameters if provided
	if req.DataDir != "" {
		config.DataDir = req.DataDir
	}

	return config
}

func init() {
	Root.AddCommand(startCmd)
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start PostgreSQL server",
	Long:  `Start a PostgreSQL server instance with the configured parameters.`,
	RunE:  runStart,
}

func runStart(cmd *cobra.Command, args []string) error {
	config := NewPostgresConfigFromViper()
	return StartPostgreSQLWithConfig(config)
}

// StartPostgreSQLWithConfig starts PostgreSQL with the given configuration
func StartPostgreSQLWithConfig(config *PostgresConfig) error {
	logger := slog.Default()

	if config.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}

	// Check if data directory exists and is initialized
	if !isDataDirInitialized(config.DataDir) {
		logger.Info("Data directory not initialized, running initdb", "data_dir", config.DataDir)
		if err := initializeDataDir(config.DataDir); err != nil {
			return fmt.Errorf("failed to initialize data directory: %w", err)
		}
	}

	// Check if PostgreSQL is already running
	if isPostgreSQLRunning(config.DataDir) {
		logger.Info("PostgreSQL is already running")
		return nil
	}

	// Start PostgreSQL
	logger.Info("Starting PostgreSQL server", "data_dir", config.DataDir)
	if err := startPostgreSQLWithConfig(config); err != nil {
		return fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	// Wait for server to be ready
	logger.Info("Waiting for PostgreSQL to be ready")
	if err := waitForPostgreSQLWithConfig(config); err != nil {
		return fmt.Errorf("PostgreSQL failed to become ready: %w", err)
	}

	logger.Info("PostgreSQL server started successfully")
	return nil
}

func isDataDirInitialized(dataDir string) bool {
	// Check if PG_VERSION file exists (indicates initialized data directory)
	pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
	_, err := os.Stat(pgVersionFile)
	return err == nil
}

func initializeDataDir(dataDir string) error {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Run initdb
	cmd := exec.Command("initdb", "-D", dataDir, "--auth-local=trust", "--auth-host=md5")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("initdb failed: %w", err)
	}

	return nil
}

func isPostgreSQLRunning(dataDir string) bool {
	// Check if postmaster.pid file exists and process is running
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	if _, err := os.Stat(pidFile); err != nil {
		return false
	}

	// Read PID from file and check if process is actually running
	pid, err := readPostmasterPID(dataDir)
	if err != nil {
		return false
	}

	return isProcessRunning(pid)
}

func startPostgreSQL(dataDir string) error {
	// Legacy function - use viper config
	config := NewPostgresConfigFromViper()
	config.DataDir = dataDir
	return startPostgreSQLWithConfig(config)
}

func startPostgreSQLWithConfig(config *PostgresConfig) error {
	// Use pg_ctl to start PostgreSQL properly as a daemon
	args := []string{
		"start",
		"-D", config.DataDir,
		"-l", filepath.Join(config.DataDir, "postgresql.log"),
		"-W", // don't wait - we'll check readiness ourselves
	}

	slog.Info("Starting PostgreSQL with configuration", "port", config.Port, "dataDir", config.DataDir)

	pgOptions := []string{
		fmt.Sprintf("-p %d", config.Port),
	}

	if config.SocketDir != "" {
		pgOptions = append(pgOptions, fmt.Sprintf("-k %s", config.SocketDir))
	}

	if config.ConfigFile != "" {
		pgOptions = append(pgOptions, fmt.Sprintf("-c config_file=%s", config.ConfigFile))
	}

	if len(pgOptions) > 0 {
		args = append(args, "-o", strings.Join(pgOptions, " "))
	}

	cmd := exec.Command("pg_ctl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start PostgreSQL with pg_ctl: %w", err)
	}

	// Wait for PostgreSQL to be ready by checking if the process is running
	// and the PID file is created with "ready" status
	for i := 0; i < config.Timeout; i++ {
		running := isPostgreSQLRunning(config.DataDir)
		ready := isPostgreSQLReady(config.DataDir)

		slog.Debug("Checking PostgreSQL readiness", "running", running, "ready", ready, "attempt", i+1)

		if running && ready {
			return nil
		}
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("PostgreSQL did not become ready within %d seconds", config.Timeout)
}

// isPostgreSQLReady checks if PostgreSQL is ready to accept connections
// by reading the postmaster.pid file and checking for "ready" status
func isPostgreSQLReady(dataDir string) bool {
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	content, err := os.ReadFile(pidFile)
	if err != nil {
		slog.Debug("Could not read postmaster.pid", "error", err)
		return false
	}

	// Check if any line contains "ready"
	lines := strings.Split(string(content), "\n")
	slog.Debug("postmaster.pid content", "lines", len(lines), "content", string(content))

	for _, line := range lines {
		if strings.Contains(strings.TrimSpace(line), "ready") {
			return true
		}
	}

	return false
}

func waitForPostgreSQL() error {
	// Legacy function - use viper config
	config := NewPostgresConfigFromViper()
	return waitForPostgreSQLWithConfig(config)
}

func waitForPostgreSQLWithConfig(config *PostgresConfig) error {
	// Try to connect using pg_isready
	for i := 0; i < config.Timeout; i++ {
		cmd := exec.Command("pg_isready",
			"-h", config.Host,
			"-p", fmt.Sprintf("%d", config.Port),
			"-U", config.User,
			"-d", config.Database,
		)

		if err := cmd.Run(); err == nil {
			return nil
		}

		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("PostgreSQL did not become ready within %d seconds", config.Timeout)
}

func readPostmasterPID(dataDir string) (int, error) {
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	content, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, err
	}

	// First line contains the PID
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

	// On Unix, sending signal 0 checks if process exists
	err = process.Signal(syscall.Signal(0))
	return err == nil
}
