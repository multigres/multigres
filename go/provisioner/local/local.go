// Copyright 2025 Supabase, Inc.
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

package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/tools/semver"
	"github.com/multigres/multigres/go/tools/stringutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"gopkg.in/yaml.v3"
)

// CellConfig holds the configuration for a single cell
type CellConfig struct {
	Name     string `yaml:"name"`
	RootPath string `yaml:"root-path"`
}

// TopologyConfig holds the configuration for cluster topology
type TopologyConfig struct {
	Backend        string       `yaml:"backend"`
	GlobalRootPath string       `yaml:"global-root-path"`
	Cells          []CellConfig `yaml:"cells"`
}

// CellServicesConfig holds the service configuration for a specific cell
type CellServicesConfig struct {
	Multigateway MultigatewayConfig `yaml:"multigateway"`
	Multipooler  MultipoolerConfig  `yaml:"multipooler"`
	Multiorch    MultiorchConfig    `yaml:"multiorch"`
	Pgctld       PgctldConfig       `yaml:"pgctld"`
}

// LocalProvisionerConfig represents the typed configuration for the local provisioner
type LocalProvisionerConfig struct {
	RootWorkingDir string                        `yaml:"root-working-dir"`
	DefaultDbName  string                        `yaml:"default-db-name"`
	Etcd           EtcdConfig                    `yaml:"etcd"`
	Topology       TopologyConfig                `yaml:"topology"`
	Multiadmin     MultiadminConfig              `yaml:"multiadmin"`
	Cells          map[string]CellServicesConfig `yaml:"cells,omitempty"`
}

// EtcdConfig holds etcd service configuration
type EtcdConfig struct {
	Version string `yaml:"version"`
	DataDir string `yaml:"data-dir"`
	Port    int    `yaml:"port"`
}

// MultigatewayConfig holds multigateway service configuration
type MultigatewayConfig struct {
	Path     string `yaml:"path"`
	HttpPort int    `yaml:"http-port"`
	GrpcPort int    `yaml:"grpc-port"`
	PgPort   int    `yaml:"pg-port"`
	LogLevel string `yaml:"log-level"`
}

// MultipoolerConfig holds multipooler service configuration
type MultipoolerConfig struct {
	Path       string `yaml:"path"`
	Database   string `yaml:"database"`
	TableGroup string `yaml:"table-group"`
	ServiceID  string `yaml:"service-id"`
	PoolerDir  string `yaml:"pooler-dir"` // Directory path for PostgreSQL socket files
	PgPort     int    `yaml:"pg-port"`    // PostgreSQL port number (same as pgctld)
	HttpPort   int    `yaml:"http-port"`
	GrpcPort   int    `yaml:"grpc-port"`
	LogLevel   string `yaml:"log-level"`
}

// MultiorchConfig holds multiorch service configuration
type MultiorchConfig struct {
	Path     string `yaml:"path"`
	HttpPort int    `yaml:"http-port"`
	GrpcPort int    `yaml:"grpc-port"`
	LogLevel string `yaml:"log-level"`
}

// MultiadminConfig holds multiadmin service configuration
type MultiadminConfig struct {
	Path     string `yaml:"path"`
	HttpPort int    `yaml:"http-port"`
	GrpcPort int    `yaml:"grpc-port"`
	LogLevel string `yaml:"log-level"`
}

// PgctldConfig holds pgctld service configuration
type PgctldConfig struct {
	Path       string `yaml:"path"`
	PoolerDir  string `yaml:"pooler-dir"`  // Base directory for this pgctld instance
	GrpcPort   int    `yaml:"grpc-port"`   // gRPC port for pgctld server
	PgPort     int    `yaml:"pg-port"`     // PostgreSQL port
	PgDatabase string `yaml:"pg-database"` // PostgreSQL database name
	PgUser     string `yaml:"pg-user"`     // PostgreSQL username
	PgPwfile   string `yaml:"pg-pwfile"`   // PostgreSQL password file path (optional)
	Timeout    int    `yaml:"timeout"`     // Operation timeout in seconds
	LogLevel   string `yaml:"log-level"`   // Log level
}

// localProvisioner implements the Provisioner interface for local binary-based provisioning
type localProvisioner struct {
	config  *LocalProvisionerConfig
	dataDir string // Base data directory for this provisioner instance
}

// Compile-time check to ensure localProvisioner implements Provisioner
var _ provisioner.Provisioner = (*localProvisioner)(nil)

// LocalProvisionedService represents a service instance that has been provisioned
type LocalProvisionedService struct {
	ID         string         `json:"id"`                    // Unique instance ID
	Service    string         `json:"service"`               // Service name (etcd, multigateway, etc.)
	PID        int            `json:"pid,omitempty"`         // For binary processes
	BinaryPath string         `json:"binary-path,omitempty"` // Path to the binary
	DataDir    string         `json:"data-dir,omitempty"`    // Data directory
	LogFile    string         `json:"log-file,omitempty"`    // Path to log file
	Ports      map[string]int `json:"ports"`                 // Port mappings
	FQDN       string         `json:"fqdn"`                  // Hostname/FQDN
	Runtime    string         `json:"runtime"`               // "binary"
	StartedAt  time.Time      `json:"started-at"`            // When it was started
	Metadata   map[string]any `json:"metadata,omitempty"`    // Additional metadata
}

const (
	// StateDir is the directory name where provision state files are stored
	StateDir = "state"
)

// Name returns the name of this provisioner
func (p *localProvisioner) Name() string {
	return "local"
}

// LoadConfig loads the provisioner-specific configuration from the given config paths
func (p *localProvisioner) LoadConfig(configPaths []string) error {
	// Try to find the config file in the provided paths
	for _, configPath := range configPaths {
		configFile := filepath.Join(configPath, "multigres.yaml")
		if _, err := os.Stat(configFile); err == nil {
			data, err := os.ReadFile(configFile)
			if err != nil {
				return fmt.Errorf("failed to read config file %s: %w", configFile, err)
			}

			// Parse the full config file
			var fullConfig struct {
				Provisioner       string         `yaml:"provisioner"`
				ProvisionerConfig map[string]any `yaml:"provisioner-config,omitempty"`
			}
			if err := yaml.Unmarshal(data, &fullConfig); err != nil {
				return fmt.Errorf("failed to parse config file %s: %w", configFile, err)
			}

			// Validate that this is for the local provisioner
			if fullConfig.Provisioner != "local" {
				return fmt.Errorf("config file %s is for provisioner '%s', not 'local'", configFile, fullConfig.Provisioner)
			}

			if err := p.ValidateConfig(fullConfig.ProvisionerConfig); err != nil {
				return fmt.Errorf("failed to validate config file %s: %w", configFile, err)
			}

			// Convert the provisioner-config section to our typed config
			yamlData, err := yaml.Marshal(fullConfig.ProvisionerConfig)
			if err != nil {
				return fmt.Errorf("failed to marshal provisioner config: %w", err)
			}

			p.config = &LocalProvisionerConfig{}
			if err := yaml.Unmarshal(yamlData, p.config); err != nil {
				return fmt.Errorf("failed to unmarshal provisioner config: %w", err)
			}

			return nil
		}
	}

	return fmt.Errorf("multigres.yaml not found in any of the provided paths: %v", configPaths)
}

// DefaultConfig returns the default configuration for the local provisioner
func (p *localProvisioner) DefaultConfig() map[string]any {
	// Use MTROOT environment variable if set, otherwise fall back to current directory
	mtroot := os.Getenv("MTROOT")
	baseDir := "."
	binDir := "bin"

	if mtroot != "" {
		baseDir = mtroot + "/multigres_local"
		binDir = filepath.Join(mtroot, "bin")
	} else {
		fmt.Println("Warning: MTROOT environment variable is not set, using relative paths for default binary configuration in local provisioner.")
	}

	// Generate service IDs for each cell using the same method as topo components
	serviceIDZone1 := stringutil.RandomString(8)
	serviceIDZone2 := stringutil.RandomString(8)
	tableGroup := "default"
	dbName := "postgres"

	// Create typed configuration with defaults
	localConfig := LocalProvisionerConfig{
		RootWorkingDir: baseDir,
		DefaultDbName:  dbName,
		Etcd: EtcdConfig{
			Version: "3.5.9",
			DataDir: filepath.Join(baseDir, "data", "etcd-data"),
			Port:    2379,
		},
		Topology: TopologyConfig{
			Backend:        "etcd2",
			GlobalRootPath: "/multigres/global",
			Cells: []CellConfig{
				{
					Name:     "zone1",
					RootPath: "/multigres/zone1",
				},
				{
					Name:     "zone2",
					RootPath: "/multigres/zone2",
				},
			},
		},
		Multiadmin: MultiadminConfig{
			Path:     filepath.Join(binDir, "multiadmin"),
			HttpPort: 15000,
			GrpcPort: 15990,
			LogLevel: "info",
		},
		Cells: map[string]CellServicesConfig{
			"zone1": {
				Multigateway: MultigatewayConfig{
					Path:     filepath.Join(binDir, "multigateway"),
					HttpPort: 15001,
					GrpcPort: 15991,
					PgPort:   15432,
					LogLevel: "info",
				},
				Multipooler: MultipoolerConfig{
					Path:       filepath.Join(binDir, "multipooler"),
					Database:   dbName,
					TableGroup: tableGroup,
					ServiceID:  serviceIDZone1,
					PoolerDir:  GeneratePoolerDir(baseDir, serviceIDZone1),
					PgPort:     5432, // Same as pgctld for this zone
					HttpPort:   15100,
					GrpcPort:   16001,
					LogLevel:   "info",
				},
				Multiorch: MultiorchConfig{
					Path:     filepath.Join(binDir, "multiorch"),
					HttpPort: 15300,
					GrpcPort: 16000,
					LogLevel: "info",
				},
				Pgctld: PgctldConfig{
					Path:       filepath.Join(binDir, "pgctld"),
					PoolerDir:  GeneratePoolerDir(baseDir, serviceIDZone1),
					GrpcPort:   17000,
					PgPort:     5432,
					PgDatabase: dbName,
					PgUser:     "postgres",
					PgPwfile:   filepath.Join(GeneratePoolerDir(baseDir, serviceIDZone1), "pgpassword.txt"),
					Timeout:    30,
					LogLevel:   "info",
				},
			},
			"zone2": {
				Multigateway: MultigatewayConfig{
					Path:     filepath.Join(binDir, "multigateway"),
					HttpPort: 15101, // zone1 + 100
					GrpcPort: 16091, // zone1 + 100
					PgPort:   15532, // zone1 + 100
					LogLevel: "info",
				},
				Multipooler: MultipoolerConfig{
					Path:       filepath.Join(binDir, "multipooler"),
					Database:   dbName,
					TableGroup: tableGroup,
					ServiceID:  serviceIDZone2,
					PoolerDir:  GeneratePoolerDir(baseDir, serviceIDZone2),
					PgPort:     5532,  // Same as pgctld for this zone (zone1 + 100)
					HttpPort:   15200, // zone1 + 100
					GrpcPort:   16101, // zone1 + 100
					LogLevel:   "info",
				},
				Multiorch: MultiorchConfig{
					Path:     filepath.Join(binDir, "multiorch"),
					HttpPort: 15400, // zone1 + 100
					GrpcPort: 16100, // zone1 + 100
					LogLevel: "info",
				},
				Pgctld: PgctldConfig{
					Path:       filepath.Join(binDir, "pgctld"),
					PoolerDir:  GeneratePoolerDir(baseDir, serviceIDZone2),
					GrpcPort:   17100, // zone1 + 100
					PgPort:     5532,  // zone1 + 100
					PgDatabase: dbName,
					PgUser:     "postgres",
					PgPwfile:   filepath.Join(GeneratePoolerDir(baseDir, serviceIDZone2), "pgpassword.txt"),
					Timeout:    30,
					LogLevel:   "info",
				},
			},
		},
	}

	// Convert to map[string]any via YAML marshaling to preserve struct ordering
	yamlData, err := yaml.Marshal(localConfig)
	if err != nil {
		// Fallback to empty config if marshaling fails
		fmt.Printf("Warning: failed to marshal default config: %v\n", err)
		return map[string]any{}
	}

	var configMap map[string]any
	if err := yaml.Unmarshal(yamlData, &configMap); err != nil {
		// Fallback to empty config if unmarshaling fails
		fmt.Printf("Warning: failed to unmarshal default config: %v\n", err)
		return map[string]any{}
	}

	return configMap
}

// createPasswordFileAndDirectories creates the pooler directory structure and password file
func createPasswordFileAndDirectories(poolerDir, passwordFilePath string) error {
	// Create the pooler directory structure
	if err := os.MkdirAll(poolerDir, 0o755); err != nil {
		return fmt.Errorf("failed to create pooler directory %s: %w", poolerDir, err)
	}

	// Create the password file with "postgres" password
	if err := os.WriteFile(passwordFilePath, []byte("postgres"), 0o600); err != nil {
		return fmt.Errorf("failed to create password file %s: %w", passwordFilePath, err)
	}

	return nil
}

// initializePgctldDirectories initializes all pgctld directories and password files based on the config
func (p *localProvisioner) initializePgctldDirectories() error {
	// Get the typed configuration
	config := p.config

	// Initialize directories for each cell's pgctld configuration
	for cellName, cellConfig := range config.Cells {
		fmt.Printf("Setting up pgctld directory for cell %s...\n", cellName)

		poolerDir := cellConfig.Pgctld.PoolerDir

		if poolerDir == "" {
			return fmt.Errorf("pooler-dir not found in config for pgtctld in cell %s", cellName)
		}

		passwordFile := cellConfig.Pgctld.PgPwfile

		if passwordFile == "" {
			return fmt.Errorf("pgctld password file not found in config for cell %s", cellName)
		}

		if err := createPasswordFileAndDirectories(poolerDir, passwordFile); err != nil {
			return fmt.Errorf("failed to initialize pgctld directory for cell %s: %w", cellName, err)
		}

		fmt.Printf("✓ Created pooler directory: %s\n", poolerDir)
		fmt.Printf("✓ Created password file: %s\n", passwordFile)
	}

	return nil
}

// provisionEtcd provisions etcd using local binary
func (p *localProvisioner) provisionEtcd(ctx context.Context, req *provisioner.ProvisionRequest) (*provisioner.ProvisionResult, error) {
	// Sanity check: ensure this method is called for etcd service
	if req.Service != "etcd" {
		return nil, fmt.Errorf("provisionEtcd called for wrong service type: %s", req.Service)
	}

	etcdConfig := p.getServiceConfig("etcd")

	// Check if etcd is already running by checking state
	existingService, err := p.findRunningEtcdService()
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing etcd service: %w", err)
	}

	if existingService != nil {
		fmt.Printf("etcd is already running (PID %d) ✓\n", existingService.PID)
		return &provisioner.ProvisionResult{
			ServiceName: "etcd",
			FQDN:        existingService.FQDN,
			Ports:       existingService.Ports,
			Metadata: map[string]any{
				"service_id": existingService.ID,
				"log_file":   existingService.LogFile,
			},
		}, nil
	}

	// Get port from config (default 2379)
	port := 2379
	if p, ok := etcdConfig["port"].(int); ok {
		port = p
	}

	// Find etcd binary (PATH or configured path)
	etcdBinary, err := p.findBinary("etcd", etcdConfig)
	if err != nil {
		return nil, fmt.Errorf("etcd binary not found: %w", err)
	}

	// Check etcd version
	expectedVersion, ok := etcdConfig["version"].(string)
	if ok && expectedVersion != "" {
		if err := p.checkEtcdVersion(etcdBinary, expectedVersion); err != nil {
			return nil, fmt.Errorf("etcd version check failed: %w", err)
		}
	}

	dir, ok := etcdConfig["data-dir"].(string)
	if !ok {
		return nil, fmt.Errorf("etcd data directory not found in config")
	}

	dataDir := dir

	// Create data directory
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create etcd data directory %s: %w", dataDir, err)
	}

	// Generate unique ID for this service instance (needed for log file)
	serviceID := stringutil.RandomString(8)

	// Create log file path
	logFile, err := p.createLogFile("etcd", serviceID, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	peerPort := port + 1

	args := []string{
		"--name", "default",
		"--data-dir", dataDir,
		"--listen-client-urls", fmt.Sprintf("http://0.0.0.0:%d", port),
		"--advertise-client-urls", fmt.Sprintf("http://localhost:%d", port),
		"--listen-peer-urls", fmt.Sprintf("http://0.0.0.0:%d", peerPort),
		"--initial-advertise-peer-urls", fmt.Sprintf("http://localhost:%d", peerPort),
		"--initial-cluster", fmt.Sprintf("default=http://localhost:%d", peerPort),
		"--initial-cluster-state", "new",
		"--log-outputs", logFile,
	}

	// Start etcd process
	etcdCmd := exec.CommandContext(ctx, etcdBinary, args...)

	fmt.Printf("▶️  - Launching etcd on port %d...", port)

	if err := etcdCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start etcd: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(etcdCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("etcd process validation failed: %w", err)
	}

	// Wait for etcd to be ready
	servicePorts := map[string]int{"etcd_port": port}
	if err := p.waitForServiceReady("etcd", "localhost", servicePorts, 10*time.Second); err != nil {
		logs := p.readServiceLogs(logFile, 20)
		return nil, fmt.Errorf("etcd readiness check failed: %w\n\nLast 20 lines from etcd logs:\n%s", err, logs)
	}
	fmt.Printf(" ready ✓\n")

	// Create provision state
	service := &LocalProvisionedService{
		ID:         serviceID,
		Service:    "etcd",
		PID:        etcdCmd.Process.Pid,
		BinaryPath: etcdBinary,
		DataDir:    dataDir,
		Ports:      map[string]int{"tcp": port},
		FQDN:       "localhost",
		LogFile:    logFile,
		StartedAt:  time.Now(),
	}

	// Save service state to disk
	if err := p.saveServiceState(service, ""); err != nil {
		fmt.Printf("Warning: failed to save service state: %v\n", err)
	}

	return &provisioner.ProvisionResult{
		ServiceName: "etcd",
		FQDN:        "localhost",
		Ports: map[string]int{
			"tcp": port,
		},
		Metadata: map[string]any{
			"runtime":     "binary",
			"pid":         etcdCmd.Process.Pid,
			"binary-path": etcdBinary,
			"data-dir":    dataDir,
			"service-id":  serviceID,
			"log-file":    logFile,
		},
	}, nil
}

// findBinary finds a binary by name, checking PATH first, then optional configured path
func (p *localProvisioner) findBinary(name string, serviceConfig map[string]any) (string, error) {
	// First try to find in PATH
	if binaryPath, err := exec.LookPath(name); err == nil {
		return binaryPath, nil
	}

	// Then try configured path if provided
	if pathConfig, ok := serviceConfig["path"].(string); ok && pathConfig != "" {
		// Check if it's an absolute path or relative path
		var fullPath string
		if filepath.IsAbs(pathConfig) {
			fullPath = pathConfig
		} else {
			// Make it relative to current directory
			fullPath = filepath.Join(".", pathConfig)
		}

		// Check if the binary exists and is executable
		if info, err := os.Stat(fullPath); err == nil && !info.IsDir() {
			return fullPath, nil
		}
	}

	return "", fmt.Errorf("binary '%s' not found in PATH or configured path", name)
}

// checkEtcdVersion verifies that the etcd binary major version matches expected version
func (p *localProvisioner) checkEtcdVersion(binaryPath, expectedVersion string) error {
	// Run etcd --version to get version info
	cmd := exec.Command(binaryPath, "--version")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to get etcd version: %w", err)
	}

	// Parse version from output - etcd version output format varies
	versionStr := string(output)

	// Try to extract version number from various etcd output formats
	versionRegex := regexp.MustCompile(`(?:etcd\s+Version:\s*|^|\s+)v?(\d+\.\d+\.\d+)`)
	matches := versionRegex.FindStringSubmatch(versionStr)
	if len(matches) < 2 {
		// If we can't parse version, just warn and continue
		fmt.Printf("Warning: could not parse etcd version from output: %s\n", strings.TrimSpace(versionStr))
		return nil
	}

	actualVersion := "v" + matches[1] // ensure v prefix for semver
	expectedVersionWithV := "v" + strings.TrimPrefix(expectedVersion, "v")

	// Use servenv semver to compare major versions
	actualMajor := semver.Major(actualVersion)
	expectedMajor := semver.Major(expectedVersionWithV)

	if actualMajor != expectedMajor {
		return fmt.Errorf("etcd major version mismatch: expected %s.x.x, found %s",
			strings.TrimPrefix(expectedMajor, "v"), strings.TrimPrefix(actualVersion, "v"))
	}

	fmt.Printf("🔍 - etcd %s found — version compatible ✓\n",
		strings.TrimPrefix(actualVersion, "v"))
	return nil
}

// readServiceLogs reads the last few lines from a service's log file for debugging
func (p *localProvisioner) readServiceLogs(logFile string, lines int) string {
	if logFile == "" {
		return "No log file available"
	}

	// Check if log file exists
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		return fmt.Sprintf("Log file not found: %s", logFile)
	}

	// Read the file
	data, err := os.ReadFile(logFile)
	if err != nil {
		return fmt.Sprintf("Failed to read log file %s: %v", logFile, err)
	}

	// Get the last N lines
	logLines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(logLines) == 0 {
		return "Log file is empty"
	}

	// Return last 'lines' lines or all lines if fewer exist
	start := max(len(logLines)-lines, 0)

	result := strings.Join(logLines[start:], "\n")
	if result == "" {
		return "Log file is empty"
	}

	return result
}

// validateProcessRunning checks if a process with the given PID is still running
func (p *localProvisioner) validateProcessRunning(pid int) error {
	if pid <= 0 {
		return fmt.Errorf("invalid PID: %d", pid)
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("process with PID %d not found: %w", pid, err)
	}

	// Send signal 0 to check if process exists without actually sending a signal
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		return fmt.Errorf("process with PID %d is not running: %w", pid, err)
	}

	return nil
}

// getRootWorkingDir returns the root working directory from config
func (p *localProvisioner) getRootWorkingDir() string {
	if p.config == nil {
		return "."
	}

	return p.config.RootWorkingDir
}

// GeneratePoolerDir generates a pooler directory path for a given base directory and service ID
func GeneratePoolerDir(baseDir, serviceID string) string {
	return filepath.Join(baseDir, "data", fmt.Sprintf("pooler_%s", serviceID))
}

// getStateDir returns the path to the state directory
func (p *localProvisioner) getStateDir() string {
	return filepath.Join(p.getRootWorkingDir(), "state")
}

// getLogsDir returns the path to the logs directory
func (p *localProvisioner) getLogsDir() string {
	return filepath.Join(p.getRootWorkingDir(), "logs")
}

func (p *localProvisioner) getDataDir() string {
	return filepath.Join(p.getRootWorkingDir(), "data")
}

// createLogFile creates a log file path and ensures the directory exists
func (p *localProvisioner) createLogFile(serviceName, serviceID, databaseName string) (string, error) {
	logsDir := p.getLogsDir()
	var serviceLogDir string

	if databaseName != "" {
		// For database services: logs/dbs/dbname/servicename
		serviceLogDir = filepath.Join(logsDir, "dbs", databaseName, serviceName)
	} else {
		// For non-database services (like etcd): logs/servicename
		serviceLogDir = filepath.Join(logsDir, serviceName)
	}

	// Create the service-specific log directory
	if err := os.MkdirAll(serviceLogDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create log directory %s: %w", serviceLogDir, err)
	}

	// Create the log file path
	logFile := filepath.Join(serviceLogDir, fmt.Sprintf("%s.log", serviceID))
	return logFile, nil
}

// cleanupLogFile removes a log file if it exists
func (p *localProvisioner) cleanupLogFile(logFilePath string) error {
	if logFilePath == "" {
		return nil
	}

	// Check if log file exists
	if _, err := os.Stat(logFilePath); os.IsNotExist(err) {
		return nil // File doesn't exist, nothing to clean up
	}

	// Remove the log file
	if err := os.Remove(logFilePath); err != nil {
		return fmt.Errorf("failed to remove log file %s: %w", logFilePath, err)
	}

	fmt.Printf("Cleaned up log file: %s\n", logFilePath)
	return nil
}

// saveServiceState saves the provisioned service state to disk
func (p *localProvisioner) saveServiceState(service *LocalProvisionedService, databaseName string) error {
	stateDir := p.getStateDir()
	var targetDir string

	if databaseName != "" {
		// For database services: state/dbs/dbname
		targetDir = filepath.Join(stateDir, "dbs", databaseName)
	} else {
		// For non-database services (like etcd): state/
		targetDir = stateDir
	}

	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("failed to create state directory %s: %w", targetDir, err)
	}

	// File name format: service_id.json (e.g., etcd_abc123.json)
	fileName := fmt.Sprintf("%s_%s.json", service.Service, service.ID)
	filePath := filepath.Join(targetDir, fileName)

	data, err := json.MarshalIndent(service, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal service state: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write service state file %s: %w", filePath, err)
	}

	return nil
}

// removeServiceState removes a service state file from disk
func (p *localProvisioner) removeServiceState(serviceID, serviceName, databaseName string) error {
	stateDir := p.getStateDir()
	var targetDir string

	if databaseName != "" {
		// For database services: state/dbs/dbname
		targetDir = filepath.Join(stateDir, "dbs", databaseName)
	} else {
		// For non-database services (like etcd): state/
		targetDir = stateDir
	}

	fileName := fmt.Sprintf("%s_%s.json", serviceName, serviceID)
	filePath := filepath.Join(targetDir, fileName)

	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove state file %s: %w", filePath, err)
	}

	return nil
}

// provisionMultigateway provisions multigateway using either binaries or Docker containers
func (p *localProvisioner) provisionMultigateway(ctx context.Context, req *provisioner.ProvisionRequest) (*provisioner.ProvisionResult, error) {
	// Sanity check: ensure this method is called for multigateway service
	if req.Service != "multigateway" {
		return nil, fmt.Errorf("provisionMultigateway called for wrong service type: %s", req.Service)
	}

	// Get cell parameter
	cell := req.Params["cell"].(string)

	// Check if multigateway is already running
	existingService, err := p.findRunningDbService("multigateway", req.DatabaseName, cell)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing multigateway service: %w", err)
	}

	if existingService != nil {
		fmt.Printf("multigateway is already running (PID %d) ✓\n", existingService.PID)
		return &provisioner.ProvisionResult{
			ServiceName: "multigateway",
			FQDN:        existingService.FQDN,
			Ports:       existingService.Ports,
			Metadata: map[string]any{
				"service_id": existingService.ID,
				"log_file":   existingService.LogFile,
			},
		}, nil
	}

	// Get parameters from request
	etcdAddress := req.Params["etcd_address"].(string)
	topoBackend := req.Params["topo_backend"].(string)
	topoGlobalRoot := req.Params["topo_global_root"].(string)

	// Get cell-specific multigateway config
	multigatewayConfig, err := p.getCellServiceConfig(cell, "multigateway")
	if err != nil {
		return nil, fmt.Errorf("failed to get multigateway config for cell %s: %w", cell, err)
	}

	// Get HTTP port from cell-specific config
	httpPort := 15001
	if p, ok := multigatewayConfig["http_port"].(int); ok {
		httpPort = p
	}

	// Get gRPC port from cell-specific config
	grpcPort := 15991
	if p, ok := multigatewayConfig["grpc_port"].(int); ok {
		grpcPort = p
	}

	// Get log level
	logLevel := "info"
	if level, ok := multigatewayConfig["log_level"].(string); ok {
		logLevel = level
	}

	// Find multigateway binary
	multigatewayBinary, err := p.findBinary("multigateway", multigatewayConfig)
	if err != nil {
		return nil, fmt.Errorf("multigateway binary not found: %w", err)
	}

	// Generate unique ID for this service instance (needed for log file)
	serviceID := stringutil.RandomString(8)

	// Create log file path
	logFile, err := p.createLogFile("multigateway", serviceID, req.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Build command arguments
	args := []string{
		"--http-port", fmt.Sprintf("%d", httpPort),
		"--grpc-port", fmt.Sprintf("%d", grpcPort),
		"--topo-global-server-addresses", etcdAddress,
		"--topo-global-root", topoGlobalRoot,
		"--topo-implementation", topoBackend,
		"--cell", cell,
		"--log-level", logLevel,
		"--log-output", logFile,
	}

	// Start multigateway process
	multigatewayCmd := exec.CommandContext(ctx, multigatewayBinary, args...)

	fmt.Printf("▶️  - Launching multigateway (HTTP:%d, gRPC:%d)...", httpPort, grpcPort)

	if err := multigatewayCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start multigateway: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(multigatewayCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("multigateway process validation failed: %w", err)
	}

	// Create provision state
	service := &LocalProvisionedService{
		ID:         serviceID,
		Service:    "multigateway",
		PID:        multigatewayCmd.Process.Pid,
		BinaryPath: multigatewayBinary,
		Ports:      map[string]int{"http_port": httpPort, "grpc_port": grpcPort},
		FQDN:       "localhost",
		LogFile:    logFile,
		StartedAt:  time.Now(),
		Metadata:   map[string]any{"cell": cell},
	}

	// Save service state to disk
	if err := p.saveServiceState(service, req.DatabaseName); err != nil {
		fmt.Printf("Warning: failed to save service state: %v\n", err)
	}

	// Wait for multigateway to be ready
	servicePorts := map[string]int{"http_port": httpPort, "grpc_port": grpcPort}
	if err := p.waitForServiceReady("multigateway", "localhost", servicePorts, 10*time.Second); err != nil {
		logs := p.readServiceLogs(logFile, 20)
		return nil, fmt.Errorf("multigateway readiness check failed: %w\n\nLast 20 lines from multigateway logs:\n%s", err, logs)
	}
	fmt.Printf(" ready ✓\n")

	return &provisioner.ProvisionResult{
		ServiceName: "multigateway",
		FQDN:        "localhost",
		Ports: map[string]int{
			"http_port": httpPort,
			"grpc_port": grpcPort,
		},
		Metadata: map[string]any{
			"service_id": serviceID,
			"log_file":   logFile,
		},
	}, nil
}

// provisionMultiadmin provisions multiadmin using local binary
func (p *localProvisioner) provisionMultiadmin(ctx context.Context, req *provisioner.ProvisionRequest) (*provisioner.ProvisionResult, error) {
	// Sanity check: ensure this method is called for multiadmin service
	if req.Service != "multiadmin" {
		return nil, fmt.Errorf("provisionMultiadmin called for wrong service type: %s", req.Service)
	}

	// Check if multiadmin is already running
	existingService, err := p.findRunningService("multiadmin")
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing multiadmin service: %w", err)
	}

	if existingService != nil {
		fmt.Printf("multiadmin is already running (PID %d) ✓\n", existingService.PID)
		return &provisioner.ProvisionResult{
			ServiceName: "multiadmin",
			FQDN:        existingService.FQDN,
			Ports:       existingService.Ports,
			Metadata: map[string]any{
				"service_id": existingService.ID,
				"log_file":   existingService.LogFile,
			},
		}, nil
	}

	// Get multiadmin config
	multiadminConfig := p.getServiceConfig("multiadmin")

	// Get HTTP port from config
	httpPort := 15000
	if p, ok := multiadminConfig["http_port"].(int); ok {
		httpPort = p
	}

	// Get gRPC port from config
	grpcPort := 15990
	if p, ok := multiadminConfig["grpc_port"].(int); ok {
		grpcPort = p
	}

	// Get parameters from request
	etcdAddress := req.Params["etcd_address"].(string)
	topoBackend := req.Params["topo_backend"].(string)
	topoGlobalRoot := req.Params["topo_global_root"].(string)

	// Get log level
	logLevel := "info"
	if level, ok := multiadminConfig["log_level"].(string); ok {
		logLevel = level
	}

	// Find multiadmin binary
	multiadminBinary, err := p.findBinary("multiadmin", multiadminConfig)
	if err != nil {
		return nil, fmt.Errorf("multiadmin binary not found: %w", err)
	}

	// Generate unique ID for this service instance (needed for log file)
	serviceID := stringutil.RandomString(8)

	// Create log file path
	logFile, err := p.createLogFile("multiadmin", serviceID, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Build command arguments
	args := []string{
		"--http-port", fmt.Sprintf("%d", httpPort),
		"--grpc-port", fmt.Sprintf("%d", grpcPort),
		"--topo-global-server-addresses", etcdAddress,
		"--topo-global-root", topoGlobalRoot,
		"--topo-implementation", topoBackend,
		"--log-level", logLevel,
		"--log-output", logFile,
		"--service-map", "grpc-multiadmin",
	}

	// Start multiadmin process
	multiadminCmd := exec.CommandContext(ctx, multiadminBinary, args...)

	fmt.Printf("▶️  - Launching multiadmin (HTTP:%d, gRPC:%d)...", httpPort, grpcPort)

	if err := multiadminCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start multiadmin: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(multiadminCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("multiadmin process validation failed: %w", err)
	}

	// Create provision state
	service := &LocalProvisionedService{
		ID:         serviceID,
		Service:    "multiadmin",
		PID:        multiadminCmd.Process.Pid,
		BinaryPath: multiadminBinary,
		Ports:      map[string]int{"http_port": httpPort, "grpc_port": grpcPort},
		FQDN:       "localhost",
		LogFile:    logFile,
		StartedAt:  time.Now(),
	}

	// Save service state to disk
	if err := p.saveServiceState(service, ""); err != nil {
		fmt.Printf("Warning: failed to save service state: %v\n", err)
	}

	// Wait for multiadmin to be ready (check HTTP port)
	servicePorts := map[string]int{"http_port": httpPort, "grpc_port": grpcPort}
	if err := p.waitForServiceReady("multiadmin", "localhost", servicePorts, 10*time.Second); err != nil {
		logs := p.readServiceLogs(logFile, 20)
		return nil, fmt.Errorf("multiadmin readiness check failed: %w\n\nLast 20 lines from multiadmin logs:\n%s", err, logs)
	}
	fmt.Printf(" ready ✓\n")

	return &provisioner.ProvisionResult{
		ServiceName: "multiadmin",
		FQDN:        "localhost",
		Ports: map[string]int{
			"http_port": httpPort,
			"grpc_port": grpcPort,
		},
		Metadata: map[string]any{
			"service_id": serviceID,
			"log_file":   logFile,
		},
	}, nil
}

// provisionMultipooler provisions multipooler using local binary
func (p *localProvisioner) provisionMultipooler(ctx context.Context, req *provisioner.ProvisionRequest) (*provisioner.ProvisionResult, error) {
	// Sanity check: ensure this method is called for multipooler service
	if req.Service != "multipooler" {
		return nil, fmt.Errorf("provisionMultipooler called for wrong service type: %s", req.Service)
	}

	// Get cell parameter
	cell := req.Params["cell"].(string)

	// Check if multipooler is already running
	existingService, err := p.findRunningDbService("multipooler", req.DatabaseName, cell)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing multipooler service: %w", err)
	}
	if existingService != nil {
		fmt.Printf("multipooler is already running (PID %d) ✓\n", existingService.PID)
		return &provisioner.ProvisionResult{
			ServiceName: "multipooler",
			FQDN:        existingService.FQDN,
			Ports:       existingService.Ports,
			Metadata: map[string]any{
				"service_id": existingService.ID,
				"log_file":   existingService.LogFile,
			},
		}, nil
	}

	// Get parameters from request
	etcdAddress := req.Params["etcd_address"].(string)
	topoBackend := req.Params["topo_backend"].(string)
	topoGlobalRoot := req.Params["topo_global_root"].(string)

	// Get cell-specific multipooler config
	multipoolerConfig, err := p.getCellServiceConfig(cell, "multipooler")
	if err != nil {
		return nil, fmt.Errorf("failed to get multipooler config for cell %s: %w", cell, err)
	}

	// Get HTTP port from cell-specific config
	httpPort := 15001
	if p, ok := multipoolerConfig["http_port"].(int); ok {
		httpPort = p
	}

	// Get grpc port from cell-specific config
	grpcPort := 16001
	if port, ok := multipoolerConfig["grpc_port"].(int); ok {
		grpcPort = port
	}

	// Get database from multipooler config, fall back to request if not set
	database := ""
	if dbFromConfig, ok := multipoolerConfig["database"].(string); ok && dbFromConfig != "" {
		database = dbFromConfig
	} else {
		database = req.DatabaseName
	}

	// Get table group from multipooler config, default to "default" if not set
	tableGroup := "default"
	if tgFromConfig, ok := multipoolerConfig["table_group"].(string); ok && tgFromConfig != "" {
		tableGroup = tgFromConfig
	}

	// Get log level
	logLevel := "info"
	if level, ok := multipoolerConfig["log_level"].(string); ok {
		logLevel = level
	}

	// Get pooler directory
	poolerDir := ""
	if val, ok := multipoolerConfig["pooler_dir"].(string); ok && val != "" {
		poolerDir = val
	}

	// Get PostgreSQL port from config or use default
	pgPort := 5432
	if port, ok := multipoolerConfig["pg_port"].(int); ok {
		pgPort = port
	}

	// Find multipooler binary
	multipoolerBinary, err := p.findBinary("multipooler", multipoolerConfig)
	if err != nil {
		return nil, fmt.Errorf("multipooler binary not found: %w", err)
	}

	// Get service ID from multipooler config - this should always be set
	serviceID := ""
	if id, ok := multipoolerConfig["service-id"].(string); ok && id != "" {
		serviceID = id
	} else {
		return nil, fmt.Errorf("service-id not found in multipooler config for cell %s", cell)
	}

	// Create log file path
	logFile, err := p.createLogFile("multipooler", serviceID, req.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Provision pgctld for this multipooler
	pgctldResult, err := p.provisionPgctld(ctx, database, tableGroup, serviceID, cell)
	if err != nil {
		return nil, fmt.Errorf("failed to provision pgctld for multipooler: %w", err)
	}

	// Build command arguments with pgctld-addr
	args := []string{
		"--http-port", fmt.Sprintf("%d", httpPort),
		"--grpc-port", fmt.Sprintf("%d", grpcPort),
		"--topo-global-server-addresses", etcdAddress,
		"--topo-global-root", topoGlobalRoot,
		"--topo-implementation", topoBackend,
		"--cell", cell,
		"--database", database,
		"--table-group", tableGroup,
		"--service-id", serviceID,
		"--pgctld-addr", pgctldResult.Address,
		"--log-level", logLevel,
		"--log-output", logFile,
		"--pooler-dir", poolerDir,
		"--pg-port", fmt.Sprintf("%d", pgPort),
	}

	// Add service map configuration to enable grpc-pooler service
	args = append(args, "--service-map", "grpc-pooler")

	// Start multipooler process
	multipoolerCmd := exec.CommandContext(ctx, multipoolerBinary, args...)

	fmt.Printf("▶️  - Launching multipooler (HTTP:%d, gRPC:%d)...", httpPort, grpcPort)

	if err := multipoolerCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start multipooler: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(multipoolerCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("multipooler process validation failed: %w", err)
	}

	// Wait for multipooler to be ready
	servicePorts := map[string]int{"http_port": httpPort, "grpc_port": grpcPort}
	if err := p.waitForServiceReady("multipooler", "localhost", servicePorts, 10*time.Second); err != nil {
		logs := p.readServiceLogs(logFile, 20)
		return nil, fmt.Errorf("multipooler readiness check failed: %w\n\nLast 20 lines from multipooler logs:\n%s", err, logs)
	}
	fmt.Printf(" ready ✓\n")

	// Create provision state
	service := &LocalProvisionedService{
		ID:         serviceID,
		Service:    "multipooler",
		PID:        multipoolerCmd.Process.Pid,
		BinaryPath: multipoolerBinary,
		Ports:      map[string]int{"http_port": httpPort, "grpc_port": grpcPort},
		FQDN:       "localhost",
		LogFile:    logFile,
		StartedAt:  time.Now(),
		Metadata:   map[string]any{"cell": cell},
	}

	// Save service state to disk
	if err := p.saveServiceState(service, req.DatabaseName); err != nil {
		fmt.Printf("Warning: failed to save service state: %v\n", err)
	}

	return &provisioner.ProvisionResult{
		ServiceName: "multipooler",
		FQDN:        "localhost",
		Ports: map[string]int{
			"http_port": httpPort,
			"grpc_port": grpcPort,
		},
		Metadata: map[string]any{
			"service_id": serviceID,
			"log_file":   logFile,
		},
	}, nil
}

// PgctldProvisionResult contains the result of provisioning pgctld
type PgctldProvisionResult struct {
	Address string
	Port    int
	LogFile string
}

// provisionMultiOrch provisions multi-orchestrator using local binary
func (p *localProvisioner) provisionMultiOrch(ctx context.Context, req *provisioner.ProvisionRequest) (*provisioner.ProvisionResult, error) {
	// Sanity check: ensure this method is called for multiorch service
	if req.Service != "multiorch" {
		return nil, fmt.Errorf("provisionMultiOrch called for wrong service type: %s", req.Service)
	}

	// Get cell parameter
	cell := req.Params["cell"].(string)

	// Check if multiorch is already running
	existingService, err := p.findRunningDbService("multiorch", req.DatabaseName, cell)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing multiorch service: %w", err)
	}
	if existingService != nil {
		fmt.Printf("multiorch is already running (PID %d) ✓\n", existingService.PID)
		return &provisioner.ProvisionResult{
			ServiceName: "multiorch",
			FQDN:        existingService.FQDN,
			Ports:       existingService.Ports,
			Metadata: map[string]any{
				"service_id": existingService.ID,
				"log_file":   existingService.LogFile,
			},
		}, nil
	}

	// Get parameters from request
	etcdAddress := req.Params["etcd_address"].(string)
	topoBackend := req.Params["topo_backend"].(string)
	topoGlobalRoot := req.Params["topo_global_root"].(string)
	cell = req.Params["cell"].(string)

	// Get cell-specific multiorch config
	multiorchConfig, err := p.getCellServiceConfig(cell, "multiorch")
	if err != nil {
		return nil, fmt.Errorf("failed to get multiorch config for cell %s: %w", cell, err)
	}

	// Get HTTP port from cell-specific config
	httpPort := 15301
	if p, ok := multiorchConfig["http_port"].(int); ok {
		httpPort = p
	}

	// Get grpc port from cell-specific config
	grpcPort := 16000
	if port, ok := multiorchConfig["grpc_port"].(int); ok {
		grpcPort = port
	}

	// Get log level
	logLevel := "info"
	if level, ok := multiorchConfig["log_level"].(string); ok {
		logLevel = level
	}

	// Find multiorch binary
	multiorchBinary, err := p.findBinary("multiorch", multiorchConfig)
	if err != nil {
		return nil, fmt.Errorf("multiorch binary not found: %w", err)
	}

	// Generate unique ID for this service instance (needed for log file)
	serviceID := stringutil.RandomString(8)

	// Create log file path
	logFile, err := p.createLogFile("multiorch", serviceID, req.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Build command arguments
	args := []string{
		"--http-port", fmt.Sprintf("%d", httpPort),
		"--grpc-port", fmt.Sprintf("%d", grpcPort),
		"--topo-global-server-addresses", etcdAddress,
		"--topo-global-root", topoGlobalRoot,
		"--topo-implementation", topoBackend,
		"--cell", cell,
		"--log-level", logLevel,
		"--log-output", logFile,
	}

	// Start multiorch process
	multiorchCmd := exec.CommandContext(ctx, multiorchBinary, args...)

	fmt.Printf("▶️  - Launching multiorch (HTTP:%d, gRPC:%d)...", httpPort, grpcPort)

	if err := multiorchCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start multiorch: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(multiorchCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("multiorch process validation failed: %w", err)
	}

	// Wait for multiorch to be ready
	servicePorts := map[string]int{"http_port": httpPort, "grpc_port": grpcPort}
	if err := p.waitForServiceReady("multiorch", "localhost", servicePorts, 10*time.Second); err != nil {
		logs := p.readServiceLogs(logFile, 20)
		return nil, fmt.Errorf("multiorch readiness check failed: %w\n\nLast 20 lines from multiorch logs:\n%s", err, logs)
	}
	fmt.Printf(" ready ✓\n")

	// Create provision state
	service := &LocalProvisionedService{
		ID:         serviceID,
		Service:    "multiorch",
		PID:        multiorchCmd.Process.Pid,
		BinaryPath: multiorchBinary,
		Ports:      map[string]int{"http_port": httpPort, "grpc_port": grpcPort},
		FQDN:       "localhost",
		LogFile:    logFile,
		StartedAt:  time.Now(),
		Metadata:   map[string]any{"cell": cell},
	}

	// Save service state to disk
	if err := p.saveServiceState(service, req.DatabaseName); err != nil {
		fmt.Printf("Warning: failed to save service state: %v\n", err)
	}

	return &provisioner.ProvisionResult{
		ServiceName: "multiorch",
		FQDN:        "localhost",
		Ports: map[string]int{
			"http_port": httpPort,
			"grpc_port": grpcPort,
		},
		Metadata: map[string]any{
			"service_id": serviceID,
			"log_file":   logFile,
		},
	}, nil
}

// Deprovision removes/stops a specific service
func (p *localProvisioner) Deprovision(ctx context.Context, req *provisioner.DeprovisionRequest) error {
	fmt.Printf("Deprovisioning %s service (ID: %s)...\n", req.Service, req.ServiceID)

	// Stop the service using the service-specific method
	if err := p.stopService(ctx, req); err != nil {
		return fmt.Errorf("failed to stop %s service: %w", req.Service, err)
	}

	// Remove state file on successful stop
	if err := p.removeServiceState(req.ServiceID, req.Service, req.DatabaseName); err != nil {
		fmt.Printf("Warning: failed to remove state file: %v\n", err)
	}

	fmt.Printf("%s service (ID: %s) deprovisioned successfully ✓\n", req.Service, req.ServiceID)
	return nil
}

// loadServiceState loads a specific service state from disk
func (p *localProvisioner) loadServiceState(req *provisioner.DeprovisionRequest) (*LocalProvisionedService, error) {
	stateDir := p.getStateDir()
	var targetDir string

	if req.DatabaseName != "" {
		// For database services: state/dbs/dbname
		targetDir = filepath.Join(stateDir, "dbs", req.DatabaseName)
	} else {
		// For non-database services (like etcd): state/
		targetDir = stateDir
	}

	fileName := fmt.Sprintf("%s_%s.json", req.Service, req.ServiceID)
	filePath := filepath.Join(targetDir, fileName)

	// Check if state file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil // Service not found
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read state file %s: %w", filePath, err)
	}

	var service LocalProvisionedService
	if err := json.Unmarshal(data, &service); err != nil {
		return nil, fmt.Errorf("failed to parse state file %s: %w", filePath, err)
	}

	// Sanity check: ensure this method is called for the expected service type
	if req.Service != service.Service {
		return nil, fmt.Errorf("deprovision%s called for wrong service type: %s", service.Service, req.Service)
	}

	return &service, nil
}

// loadDbProvisionedServices loads provisioned services for a specific database
func (p *localProvisioner) loadDbProvisionedServices(databaseName string) ([]*LocalProvisionedService, error) {
	if databaseName == "" {
		return nil, fmt.Errorf("database name is required")
	}

	stateDir := p.getStateDir()
	targetDir := filepath.Join(stateDir, "dbs", databaseName)

	// Check if target directory exists
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		return nil, nil // No services for this database
	}

	entries, err := os.ReadDir(targetDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", targetDir, err)
	}

	var services []*LocalProvisionedService
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			// Parse filename: service_id.json
			name := strings.TrimSuffix(entry.Name(), ".json")
			parts := strings.SplitN(name, "_", 2)
			if len(parts) != 2 {
				// Skip invalid state files
				continue
			}

			serviceName := parts[0]
			serviceID := parts[1]

			req := &provisioner.DeprovisionRequest{
				Service:      serviceName,
				ServiceID:    serviceID,
				DatabaseName: databaseName,
			}
			service, err := p.loadServiceState(req)
			if err != nil {
				// Log warning but continue with other services
				fmt.Printf("Warning: failed to load state for %s service %s: %v\n", serviceName, serviceID, err)
				continue
			}

			if service != nil {
				services = append(services, service)
			}
		}
	}

	return services, nil
}

// loadEtcdServices loads etcd services from the top-level state directory
func (p *localProvisioner) loadEtcdServices() ([]*LocalProvisionedService, error) {
	stateDir := p.getStateDir()

	// Check if state directory exists
	if _, err := os.Stat(stateDir); os.IsNotExist(err) {
		return nil, nil // No state directory, no services running
	}

	entries, err := os.ReadDir(stateDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", stateDir, err)
	}

	var services []*LocalProvisionedService
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") && strings.HasPrefix(entry.Name(), "etcd_") {
			// Parse filename: etcd_serviceID.json
			name := strings.TrimSuffix(entry.Name(), ".json")
			parts := strings.SplitN(name, "_", 2)
			if len(parts) != 2 || parts[0] != "etcd" {
				continue
			}

			serviceID := parts[1]

			req := &provisioner.DeprovisionRequest{
				Service:      "etcd",
				ServiceID:    serviceID,
				DatabaseName: "", // etcd is a global service
			}
			service, err := p.loadServiceState(req)
			if err != nil {
				// Log warning but continue with other services
				fmt.Printf("Warning: failed to load state for etcd service %s: %v\n", serviceID, err)
				continue
			}

			if service != nil {
				services = append(services, service)
			}
		}
	}

	return services, nil
}

// loadGlobalServices loads all global services (non-database services) from state files
func (p *localProvisioner) loadGlobalServices() ([]*LocalProvisionedService, error) {
	stateDir := p.getStateDir()

	// Check if state directory exists
	if _, err := os.Stat(stateDir); os.IsNotExist(err) {
		return nil, nil // No state directory, no services running
	}

	entries, err := os.ReadDir(stateDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", stateDir, err)
	}

	var services []*LocalProvisionedService
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			// Parse filename: serviceName_serviceID.json
			name := strings.TrimSuffix(entry.Name(), ".json")
			parts := strings.SplitN(name, "_", 2)
			if len(parts) != 2 {
				continue
			}

			serviceName := parts[0]
			serviceID := parts[1]

			// Load global services (non-etcd services can be included here)
			if serviceName == "multiadmin" || serviceName == "etcd" {
				req := &provisioner.DeprovisionRequest{
					Service:      serviceName,
					ServiceID:    serviceID,
					DatabaseName: "", // global services have no database name
				}

				service, err := p.loadServiceState(req)
				if err != nil {
					// Log warning but continue with other services
					fmt.Printf("Warning: failed to load state for global service %s: %v\n", serviceID, err)
					continue
				}

				if service != nil {
					services = append(services, service)
				}
			}
		}
	}

	return services, nil
}

// findRunningDbService finds a running service by service name within a specific database and cell
func (p *localProvisioner) findRunningDbService(serviceName, databaseName, cell string) (*LocalProvisionedService, error) {
	services, err := p.loadDbProvisionedServices(databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to load service states for database %s: %w", databaseName, err)
	}

	for _, service := range services {
		if service.Service == serviceName {
			// Check if the service matches the cell
			if serviceCell, ok := service.Metadata["cell"].(string); ok && serviceCell == cell {
				// Check if the service is actually still running
				if service.PID > 0 {
					if err := p.validateProcessRunning(service.PID); err == nil {
						return service, nil
					}
				}
			}
		}
	}

	// Check for port conflicts with other processes using cell-specific config
	expectedPorts := p.getExpectedPortsForDbService(serviceName, cell)
	for portName, port := range expectedPorts {
		if err := p.checkPortConflict(port, serviceName, portName); err != nil {
			return nil, err
		}
	}

	return nil, nil // No running service found
}

// getExpectedPortsForDbService returns expected ports for a DB-scoped service (per cell)
func (p *localProvisioner) getExpectedPortsForDbService(serviceName, cell string) map[string]int {
	ports := make(map[string]int)

	cellConfig, err := p.getCellServiceConfig(cell, serviceName)
	if err != nil {
		return ports
	}

	switch serviceName {
	case "multigateway":
		if httpPort, ok := cellConfig["http_port"].(int); ok {
			ports["http"] = httpPort
		}
		if grpcPort, ok := cellConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
	case "multipooler":
		if grpcPort, ok := cellConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
		if httpPort, ok := cellConfig["http_port"].(int); ok && httpPort > 0 {
			ports["http"] = httpPort
		}
	case "multiorch":
		if grpcPort, ok := cellConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
		if httpPort, ok := cellConfig["http_port"].(int); ok && httpPort > 0 {
			ports["http"] = httpPort
		}
	}

	return ports
}

// getExpectedPortsForService returns the expected ports for a service based on its configuration
func (p *localProvisioner) getExpectedPortsForService(serviceName string) map[string]int {
	serviceConfig := p.getServiceConfig(serviceName)
	ports := make(map[string]int)

	switch serviceName {
	case "multigateway":
		if httpPort, ok := serviceConfig["http_port"].(int); ok {
			ports["http"] = httpPort
		}
		if grpcPort, ok := serviceConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
	case "multipooler":
		if grpcPort, ok := serviceConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
		if httpPort, ok := serviceConfig["http_port"].(int); ok && httpPort > 0 {
			ports["http"] = httpPort
		}
	case "multiorch":
		if grpcPort, ok := serviceConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
		if httpPort, ok := serviceConfig["http_port"].(int); ok && httpPort > 0 {
			ports["http"] = httpPort
		}
	case "multiadmin":
		if httpPort, ok := serviceConfig["http_port"].(int); ok {
			ports["http"] = httpPort
		}
		if grpcPort, ok := serviceConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
	case "etcd":
		if port, ok := serviceConfig["port"].(int); ok {
			ports["tcp"] = port
		}
	}

	return ports
}

// checkPortConflict checks if a port is already in use by another process
func (p *localProvisioner) checkPortConflict(port int, serviceName, portName string) error {
	if port <= 0 {
		return nil // Skip invalid ports
	}

	address := fmt.Sprintf("localhost:%d", port)
	conn, err := net.DialTimeout("tcp", address, 1*time.Second)
	if err != nil {
		// Port is not in use, this is good
		return nil
	}
	conn.Close()

	// Port is in use by some process
	return fmt.Errorf("cannot start %s: port %d (%s) is already in use by another process. "+
		"There is no way to do a clean start. Please kill the process using port %d or change the configuration",
		serviceName, port, portName, port)
}

// findRunningEtcdService finds a running etcd service
func (p *localProvisioner) findRunningEtcdService() (*LocalProvisionedService, error) {
	services, err := p.loadEtcdServices()
	if err != nil {
		return nil, fmt.Errorf("failed to load etcd service states: %w", err)
	}

	for _, service := range services {
		if service.Service == "etcd" {
			// Check if the service is actually still running
			if service.PID > 0 {
				if err := p.validateProcessRunning(service.PID); err == nil {
					return service, nil
				}
			}
		}
	}

	// Check for port conflicts with other processes
	expectedPorts := p.getExpectedPortsForService("etcd")
	for portName, port := range expectedPorts {
		if err := p.checkPortConflict(port, "etcd", portName); err != nil {
			return nil, err
		}
	}

	return nil, nil // No running etcd service found
}

// findRunningService finds a running service by service name (for global services like multiadmin)
func (p *localProvisioner) findRunningService(serviceName string) (*LocalProvisionedService, error) {
	// Load global services (e.g., multiadmin, etcd)
	services, err := p.loadGlobalServices()
	if err != nil {
		return nil, fmt.Errorf("failed to load global service states: %w", err)
	}

	for _, service := range services {
		if service.Service == serviceName {
			// Check if the service is actually still running
			if service.PID > 0 {
				if err := p.validateProcessRunning(service.PID); err == nil {
					return service, nil
				}
			}
		}
	}

	// Check for port conflicts with other processes
	expectedPorts := p.getExpectedPortsForService(serviceName)
	for portName, port := range expectedPorts {
		if err := p.checkPortConflict(port, serviceName, portName); err != nil {
			return nil, err
		}
	}

	return nil, nil // No running service found
}

// stopService stops a specific service based on its type using the internal methods
func (p *localProvisioner) stopService(ctx context.Context, req *provisioner.DeprovisionRequest) error {
	switch req.Service {
	case "etcd":
		fallthrough
	case "multigateway":
		fallthrough
	case "multipooler":
		fallthrough
	case "multiorch":
		return p.deprovisionService(ctx, req)
	case "pgctld":
		// pgctld requires special handling to stop PostgreSQL first
		service, err := p.loadServiceState(req)
		if err != nil {
			return err
		}
		if service == nil {
			return fmt.Errorf("pgctld service not found")
		}
		return p.deprovisionPgctld(ctx, service)
	default:
		return fmt.Errorf("unknown service type: %s", req.Service)
	}
}

// deprovisionService(ctx stops a multiorch service instance
func (p *localProvisioner) deprovisionService(ctx context.Context, req *provisioner.DeprovisionRequest) error {
	// Load the specific service state
	service, err := p.loadServiceState(req)
	if err != nil {
		return err
	}

	if service == nil {
		return fmt.Errorf("service not found")
	}

	// Stop the process if it's running
	if service.PID > 0 {
		if err := p.stopProcessByPID(service.PID); err != nil {
			return fmt.Errorf("failed to stop multiorch process: %w", err)
		}
	}

	// Clean up log file if it exists
	if service.LogFile != "" {
		if err := p.cleanupLogFile(service.LogFile); err != nil {
			fmt.Printf("Warning: failed to clean up log file %s: %v\n", service.LogFile, err)
		}
	}

	// Remove state file
	if err := p.removeServiceState(req.ServiceID, req.Service, req.DatabaseName); err != nil {
		fmt.Printf("Warning: failed to remove etcd state file: %v\n", err)
	}

	// Clean up data directory if requested
	if req.Clean && service.DataDir != "" {
		fmt.Printf("Cleaning service data directory: %s\n", service.DataDir)
		if err := os.RemoveAll(service.DataDir); err != nil {
			return fmt.Errorf("failed to remove etcd data directory: %w", err)
		}
	}

	return nil
}

// stopProcessByPID stops a process by its PID
func (p *localProvisioner) stopProcessByPID(pid int) error {
	// Check if process exists
	process, err := os.FindProcess(pid)
	if err != nil {
		// Process not found, assume already cleaned up
		fmt.Printf("Process %d not found, assuming already stopped\n", pid)
		return nil
	}

	// Send SIGTERM to gracefully stop the process
	if err := process.Signal(syscall.SIGTERM); err != nil {
		// Process might already be dead, check errno
		if err.Error() == "no such process" || err.Error() == "process already finished" {
			fmt.Printf("Process %d already stopped\n", pid)
			return nil
		}

		// If SIGTERM fails for other reasons, try SIGKILL
		if err := process.Kill(); err != nil {
			// If kill also fails and it's because process doesn't exist, that's ok
			if err.Error() == "no such process" || err.Error() == "process already finished" {
				fmt.Printf("Process %d already stopped\n", pid)
				return nil
			}
			return fmt.Errorf("failed to kill process %d: %w", pid, err)
		}
	}

	// Wait a bit for the process to exit
	time.Sleep(2 * time.Second)

	fmt.Printf("Process %d stopped successfully\n", pid)
	return nil
}

// Bootstrap sets up etcd and creates the default database
func (p *localProvisioner) Bootstrap(ctx context.Context) ([]*provisioner.ProvisionResult, error) {
	fmt.Println("=== Bootstrapping Multigres cluster ===")
	fmt.Println("")

	var allResults []*provisioner.ProvisionResult

	// Provision etcd
	fmt.Println("=== Provisioning etcd ===")
	etcdResult, err := p.provisionEtcd(ctx, &provisioner.ProvisionRequest{Service: "etcd"})
	if err != nil {
		return nil, fmt.Errorf("failed to provision etcd: %w", err)
	}
	fmt.Println("")

	// Setup default cell using the configured cell name

	tcpPort := etcdResult.Ports["tcp"]
	fmt.Printf("🌐 - etcd available at: %s:%d\n", etcdResult.FQDN, tcpPort)
	allResults = append(allResults, etcdResult)

	etcdAddress := fmt.Sprintf("%s:%d", etcdResult.FQDN, tcpPort)

	// Initialize pgctld directories and password files
	fmt.Println("=== Setting up pgctld directories ===")
	if err := p.initializePgctldDirectories(); err != nil {
		return nil, fmt.Errorf("failed to initialize pgctld directories: %w", err)
	}
	fmt.Println("")

	topoConfig, err := p.getTopologyConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get topology config: %w", err)
	}

	// Get all cells and set them up
	cellNames, err := p.getCellNames()
	if err != nil {
		return nil, fmt.Errorf("failed to get cells: %w", err)
	}

	// Set up all cells
	for _, cellName := range cellNames {
		if err := p.setupDefaultCell(ctx, cellName, etcdAddress); err != nil {
			return nil, fmt.Errorf("failed to setup cell %s: %w", cellName, err)
		}
	}
	fmt.Println("")

	// Provision multiadmin (global admin service)
	fmt.Println("=== Starting MultiAdmin ===")
	multiadminReq := &provisioner.ProvisionRequest{
		Service: "multiadmin",
		Params: map[string]any{
			"etcd_address":     etcdAddress,
			"topo_backend":     topoConfig.Backend,
			"topo_global_root": topoConfig.GlobalRootPath,
		},
	}

	multiadminResult, err := p.provisionMultiadmin(ctx, multiadminReq)
	if err != nil {
		return nil, fmt.Errorf("failed to provision multiadmin: %w", err)
	}
	if httpPort, ok := multiadminResult.Ports["http_port"]; ok {
		fmt.Printf("🌐 - Available at: http://%s:%d\n", multiadminResult.FQDN, httpPort)
	}
	if grpcPort, ok := multiadminResult.Ports["grpc_port"]; ok {
		fmt.Printf("🌐 - gRPC available at: %s:%d\n", multiadminResult.FQDN, grpcPort)
	}
	allResults = append(allResults, multiadminResult)
	fmt.Println("")

	// Setup default database
	defaultDBName, err := p.getDefaultDatabaseName()
	if err != nil {
		return nil, fmt.Errorf("failed to get default database name: %w", err)
	}

	databaseResults, err := p.ProvisionDatabase(ctx, defaultDBName, etcdAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to provision default database: %w", err)
	}

	allResults = append(allResults, databaseResults...)

	return allResults, nil
}

// Teardown shuts down all services (reverse of Bootstrap)
func (p *localProvisioner) Teardown(ctx context.Context, clean bool) error {
	fmt.Println("=== Tearing down Multigres cluster ===")

	// Get the typed configuration
	config := p.config

	// Get etcd address (assuming etcd is running locally)
	etcdPort := config.Etcd.Port
	etcdAddress := fmt.Sprintf("localhost:%d", etcdPort)

	// 1. Deprovision database services first
	if err := p.DeprovisionDatabase(ctx, config.DefaultDbName, etcdAddress); err != nil {
		fmt.Printf("Warning: failed to deprovision database: %v\n", err)
	}

	// 2. Deprovision global services (multiadmin)
	fmt.Println("=== Deprovisioning global services ===")
	globalServices, err := p.loadGlobalServices()
	if err != nil {
		fmt.Printf("Warning: failed to load global service states: %v\n", err)
	} else {
		for _, service := range globalServices {
			if service.Service == "multiadmin" {
				req := &provisioner.DeprovisionRequest{
					Service:      "multiadmin",
					ServiceID:    service.ID,
					DatabaseName: "", // multiadmin is a global service
					Clean:        clean,
				}
				if err := p.deprovisionService(ctx, req); err != nil {
					fmt.Printf("Warning: failed to deprovision multiadmin: %v\n", err)
				}
			}
		}
	}

	// 3. Deprovision etcd last
	fmt.Println("=== Deprovisioning etcd ===")
	etcdServices, err := p.loadEtcdServices()
	if err != nil {
		return fmt.Errorf("failed to load etcd service states: %w", err)
	}

	for _, service := range etcdServices {
		if service.Service == "etcd" {
			req := &provisioner.DeprovisionRequest{
				Service:      "etcd",
				ServiceID:    service.ID,
				DatabaseName: "", // etcd is a global service
				Clean:        clean,
			}
			if err := p.deprovisionService(ctx, req); err != nil {
				fmt.Printf("Warning: failed to deprovision etcd: %v\n", err)
			}
			// There is a single etcd, we can break now
			break
		}
	}

	// 4. Clean up logs, state, and data directories if requested
	if clean {
		logsDir := p.getLogsDir()
		if err := p.cleanupLogsDirectory(logsDir); err != nil {
			fmt.Printf("Warning: failed to clean up logs directory: %v\n", err)
		}

		stateDir := p.getStateDir()
		if err := p.cleanupStateDirectory(stateDir); err != nil {
			fmt.Printf("Warning: failed to clean up state directory: %v\n", err)
		}

		dataDir := p.getDataDir()
		if err := p.cleanupDataDirectory(dataDir); err != nil {
			fmt.Printf("Warning: failed to clean up data directory: %v\n", err)
		}
	}

	fmt.Println("Teardown completed successfully")
	return nil
}

// cleanupLogsDirectory removes the entire logs directory and all its contents
func (p *localProvisioner) cleanupLogsDirectory(logsDir string) error {
	// Check if logs directory exists
	if _, err := os.Stat(logsDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, nothing to clean up
	}

	// Remove the entire logs directory
	if err := os.RemoveAll(logsDir); err != nil {
		return fmt.Errorf("failed to remove logs directory %s: %w", logsDir, err)
	}

	fmt.Printf("Cleaned up logs directory: %s\n", logsDir)
	return nil
}

// cleanupStateDirectory removes the entire state directory and all its contents
func (p *localProvisioner) cleanupStateDirectory(stateDir string) error {
	// Check if state directory exists
	if _, err := os.Stat(stateDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, nothing to clean up
	}

	// Remove the entire state directory
	if err := os.RemoveAll(stateDir); err != nil {
		return fmt.Errorf("failed to remove state directory %s: %w", stateDir, err)
	}

	fmt.Printf("Cleaned up state directory: %s\n", stateDir)
	return nil
}

// cleanupDataDirectory removes the entire data directory and all its contents
func (p *localProvisioner) cleanupDataDirectory(dataDir string) error {
	// Check if data directory exists
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, nothing to clean up
	}

	// Remove the entire data directory
	if err := os.RemoveAll(dataDir); err != nil {
		return fmt.Errorf("failed to remove data directory %s: %w", dataDir, err)
	}

	fmt.Printf("Cleaned up data directory: %s\n", dataDir)
	return nil
}

// getDefaultDatabaseName returns the default database name from config
func (p *localProvisioner) getDefaultDatabaseName() (string, error) {
	if p.config == nil {
		return "", fmt.Errorf("provisioner config not set")
	}

	if p.config.DefaultDbName == "" {
		return "", fmt.Errorf("default-dbname not specified in configuration")
	}

	return p.config.DefaultDbName, nil
}

// ProvisionDatabase provisions a complete database stack in all cells (assumes etcd is already running and cells are configured)
func (p *localProvisioner) ProvisionDatabase(ctx context.Context, databaseName string, etcdAddress string) ([]*provisioner.ProvisionResult, error) {
	fmt.Printf("=== Provisioning database: %s ===\n", databaseName)
	fmt.Println("")

	// Get topology configuration from provisioner config
	topoConfig := p.config.Topology

	// Get all cell information
	cellNames, err := p.getCellNames()
	if err != nil {
		return nil, fmt.Errorf("failed to get cells: %w", err)
	}

	// Register database in global topology store first
	fmt.Println("=== Registering database in topology ===")
	fmt.Printf("⚙️  - Registering database: %s\n", databaseName)

	ts, err := topo.OpenServer(topoConfig.Backend, topoConfig.GlobalRootPath, []string{etcdAddress})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to topology server: %w", err)
	}
	defer ts.Close()

	// Check if database already exists
	_, err = ts.GetDatabase(ctx, databaseName)
	if err == nil {
		fmt.Printf("⚙️  - Database \"%s\" detected — reusing existing database ✓\n", databaseName)
	} else if errors.Is(err, &topo.TopoError{Code: topo.NoNode}) {
		// Create the database if it doesn't exist
		fmt.Printf("⚙️  - Creating database \"%s\" with cells: [%s]...\n", databaseName, strings.Join(cellNames, ", "))

		databaseConfig := &clustermetadatapb.Database{
			Name:             databaseName,
			BackupLocation:   "",        // TODO: Configure backup location
			DurabilityPolicy: "none",    // Default durability policy
			Cells:            cellNames, // Register with all cells
		}

		if err := ts.CreateDatabase(ctx, databaseName, databaseConfig); err != nil {
			return nil, fmt.Errorf("failed to create database '%s' in topology: %w", databaseName, err)
		}

		fmt.Printf("⚙️  - Database \"%s\" registered successfully ✓\n", databaseName)
	} else {
		return nil, fmt.Errorf("failed to check database '%s': %w", databaseName, err)
	}
	fmt.Println("")

	var results []*provisioner.ProvisionResult

	// Provision services in each cell
	for _, cellName := range cellNames {
		fmt.Printf("=== Provisioning services in cell: %s ===\n", cellName)

		// Provision multigateway
		fmt.Printf("=== Starting Multigateway in %s ===\n", cellName)
		multigatewayReq := &provisioner.ProvisionRequest{
			Service:      "multigateway",
			DatabaseName: databaseName,
			Params: map[string]any{
				"etcd_address":     etcdAddress,
				"topo_backend":     topoConfig.Backend,
				"topo_global_root": topoConfig.GlobalRootPath,
				"cell":             cellName,
			},
		}

		multigatewayResult, err := p.provisionMultigateway(ctx, multigatewayReq)
		if err != nil {
			return nil, fmt.Errorf("failed to provision multigateway for database %s in cell %s: %w", databaseName, cellName, err)
		}
		if httpPort, ok := multigatewayResult.Ports["http_port"]; ok {
			fmt.Printf("🌐 - Available at: http://%s:%d\n", multigatewayResult.FQDN, httpPort)
		}
		results = append(results, multigatewayResult)

		// Provision multipooler
		fmt.Printf("\n=== Starting Multipooler in %s ===\n", cellName)
		multipoolerReq := &provisioner.ProvisionRequest{
			Service:      "multipooler",
			DatabaseName: databaseName,
			Params: map[string]any{
				"etcd_address":     etcdAddress,
				"topo_backend":     topoConfig.Backend,
				"topo_global_root": topoConfig.GlobalRootPath,
				"cell":             cellName,
			},
		}

		multipoolerResult, err := p.provisionMultipooler(ctx, multipoolerReq)
		if err != nil {
			return nil, fmt.Errorf("failed to provision multipooler for database %s in cell %s: %w", databaseName, cellName, err)
		}
		if grpcPort, ok := multipoolerResult.Ports["grpc_port"]; ok {
			fmt.Printf("🌐 - Available at: %s:%d\n", multipoolerResult.FQDN, grpcPort)
		}
		results = append(results, multipoolerResult)

		// Provision multiorch
		fmt.Printf("\n=== Starting MultiOrchestrator in %s ===\n", cellName)
		multiorchReq := &provisioner.ProvisionRequest{
			Service:      "multiorch",
			DatabaseName: databaseName,
			Params: map[string]any{
				"etcd_address":     etcdAddress,
				"topo_backend":     topoConfig.Backend,
				"topo_global_root": topoConfig.GlobalRootPath,
				"cell":             cellName,
			},
		}

		multiorchResult, err := p.provisionMultiOrch(ctx, multiorchReq)
		if err != nil {
			return nil, fmt.Errorf("failed to provision multiorch for database %s in cell %s: %w", databaseName, cellName, err)
		}
		if grpcPort, ok := multiorchResult.Ports["grpc_port"]; ok {
			fmt.Printf("🌐 - Available at: %s:%d\n", multiorchResult.FQDN, grpcPort)
		}
		results = append(results, multiorchResult)

		fmt.Printf("\n✓ Cell %s provisioned successfully\n\n", cellName)
	}

	fmt.Printf("Database %s provisioned successfully across %d cells with %d total services\n", databaseName, len(cellNames), len(results))
	return results, nil
}

// setupDefaultCell initializes the topology cell configuration for a database
func (p *localProvisioner) setupDefaultCell(ctx context.Context, cellName, etcdAddress string) error {
	fmt.Println("=== Configuring cell ===")
	fmt.Printf("⚙️  - Configuring cell: %s\n", cellName)
	fmt.Printf("⚙️  - Using etcd at: %s\n", etcdAddress)

	// Get topology configuration
	topoConfig := p.config.Topology

	// Create topology store using configured backend
	ts, err := topo.OpenServer(topoConfig.Backend, topoConfig.GlobalRootPath, []string{etcdAddress})
	if err != nil {
		return fmt.Errorf("failed to connect to topology server: %w", err)
	}
	defer ts.Close()

	// Check if cell already exists
	_, err = ts.GetCell(ctx, cellName)
	if err == nil {
		fmt.Printf("⚙️  - Cell \"%s\" detected — reusing existing cell ✓\n", cellName)
		return nil
	}

	// Create the cell if it doesn't exist
	if errors.Is(err, &topo.TopoError{Code: topo.NoNode}) {
		fmt.Printf("⚙️  - Creating cell \"%s\"...\n", cellName)

		// Get the specific cell config for this cell name
		cellConfigData, err := p.getCellByName(cellName)
		if err != nil {
			return fmt.Errorf("failed to get cell config for %s: %w", cellName, err)
		}

		cellConfig := &clustermetadatapb.Cell{
			Name:            cellName,
			ServerAddresses: []string{etcdAddress},
			Root:            cellConfigData.RootPath,
		}

		if err := ts.CreateCell(ctx, cellName, cellConfig); err != nil {
			return fmt.Errorf("failed to create cell '%s': %w", cellName, err)
		}

		fmt.Printf("⚙️  - Cell \"%s\" created successfully ✓\n", cellName)
		return nil
	}

	// Some other error occurred
	return fmt.Errorf("failed to check cell '%s': %w", cellName, err)
}

// DeprovisionDatabase deprovisions all services for a database
func (p *localProvisioner) DeprovisionDatabase(ctx context.Context, databaseName string, etcdAddress string) error {
	fmt.Printf("=== Deprovisioning database: %s ===\n", databaseName)

	// Find all running services related to this database
	services, err := p.loadDbProvisionedServices(databaseName)
	if err != nil {
		return fmt.Errorf("failed to load service states for database %s: %w", databaseName, err)
	}

	servicesStopped := 0

	for _, service := range services {
		fmt.Printf("Stopping %s service (ID: %s) for database %s...\n", service.Service, service.ID, databaseName)

		req := &provisioner.DeprovisionRequest{
			Service:      service.Service,
			ServiceID:    service.ID,
			DatabaseName: databaseName,
			Clean:        true, // Clean up data when deprovisioning database
		}

		if err := p.stopService(ctx, req); err != nil {
			fmt.Printf("Warning: failed to stop %s service: %v\n", service.Service, err)
			continue
		}
		// Remove state file
		if err := p.removeServiceState(service.ID, req.Service, req.DatabaseName); err != nil {
			fmt.Printf("Warning: failed to remove state file: %v\n", err)
		}

		servicesStopped++
	}

	fmt.Printf("Database %s deprovisioned successfully (%d services stopped)\n", databaseName, servicesStopped)
	return nil
}

// getTopologyConfig extracts topology configuration from provisioner config
func (p *localProvisioner) getTopologyConfig() (*TopologyConfig, error) {
	if p.config == nil {
		return nil, fmt.Errorf("provisioner config not set")
	}

	return &p.config.Topology, nil
}

// getAllCells returns all configured cells
func (p *localProvisioner) getAllCells() ([]CellConfig, error) {
	if p.config == nil {
		return nil, fmt.Errorf("provisioner config not set")
	}

	if len(p.config.Topology.Cells) == 0 {
		return nil, fmt.Errorf("no cells configured")
	}

	return p.config.Topology.Cells, nil
}

// getCellNames returns the names of all configured cells
func (p *localProvisioner) getCellNames() ([]string, error) {
	cells, err := p.getAllCells()
	if err != nil {
		return nil, err
	}

	var names []string
	for _, cell := range cells {
		names = append(names, cell.Name)
	}
	return names, nil
}

// getCellIndex returns the index of a cell in the list of cell names (for port calculation)
func (p *localProvisioner) getCellIndex(cellName string) (int, error) {
	cells, err := p.getAllCells()
	if err != nil {
		return -1, err
	}

	// Find the cell by name and return its index
	for i, cell := range cells {
		if cell.Name == cellName {
			return i, nil
		}
	}

	return -1, fmt.Errorf("cell %s not found", cellName)
}

// calculatePortOffset calculates the port offset for a given cell (100 per cell)
func (p *localProvisioner) calculatePortOffset(cellName string) (int, error) {
	cellIndex, err := p.getCellIndex(cellName)
	if err != nil {
		return 0, err
	}
	return cellIndex * 100, nil
}

// getDefaultCell returns the first cell configuration (for backward compatibility)
func (p *localProvisioner) getDefaultCell() (string, *CellConfig, error) {
	if p.config == nil {
		return "", nil, fmt.Errorf("provisioner config not set")
	}

	if len(p.config.Topology.Cells) == 0 {
		return "", nil, fmt.Errorf("no cells configured")
	}

	// Return the first cell (for backward compatibility with single-cell setup)
	for _, cell := range p.config.Topology.Cells {
		return cell.Name, &cell, nil
	}

	return "", nil, fmt.Errorf("no cells found")
}

// getCellByName returns the cell configuration for a specific cell name
func (p *localProvisioner) getCellByName(cellName string) (*CellConfig, error) {
	if p.config == nil {
		return nil, fmt.Errorf("provisioner config not set")
	}

	if len(p.config.Topology.Cells) == 0 {
		return nil, fmt.Errorf("no cells configured")
	}

	// Find the specific cell by name
	for _, cell := range p.config.Topology.Cells {
		if cell.Name == cellName {
			return &cell, nil
		}
	}

	return nil, fmt.Errorf("cell %s not found in configuration", cellName)
}

// ValidateConfig validates the local provisioner configuration
func (p *localProvisioner) ValidateConfig(config map[string]any) error {
	// Convert to typed configuration for validation
	typedConfig := &LocalProvisionerConfig{}
	yamlData, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	if err := yaml.Unmarshal(yamlData, typedConfig); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate topology backend
	availableBackends := topo.GetAvailableImplementations()
	validBackend := slices.Contains(availableBackends, typedConfig.Topology.Backend)
	if !validBackend {
		return fmt.Errorf("invalid topo backend: %s (available: %v)", typedConfig.Topology.Backend, availableBackends)
	}

	// Validate required topology fields
	if typedConfig.Topology.GlobalRootPath == "" {
		return fmt.Errorf("topology global-root-path is required")
	}
	if len(typedConfig.Topology.Cells) == 0 {
		return fmt.Errorf("topology must have at least one cell configured")
	}
	// Validate each cell
	for i, cell := range typedConfig.Topology.Cells {
		if cell.Name == "" {
			return fmt.Errorf("cell at index %d name is required", i)
		}
		if cell.RootPath == "" {
			return fmt.Errorf("cell %s root-path is required", cell.Name)
		}
	}

	return nil
}

// getServiceConfig gets the configuration for a specific service (global services only)
func (p *localProvisioner) getServiceConfig(service string) map[string]any {
	switch service {
	case "etcd":
		return map[string]any{
			"version":  p.config.Etcd.Version,
			"data-dir": p.config.Etcd.DataDir,
			"port":     p.config.Etcd.Port,
		}
	case "multiadmin":
		return map[string]any{
			"path":      p.config.Multiadmin.Path,
			"http_port": p.config.Multiadmin.HttpPort,
			"grpc_port": p.config.Multiadmin.GrpcPort,
			"log_level": p.config.Multiadmin.LogLevel,
		}
	default:
		// Return empty config if not found
		return map[string]any{}
	}
}

// getCellServiceConfig gets the configuration for a specific service in a specific cell
func (p *localProvisioner) getCellServiceConfig(cellName, service string) (map[string]any, error) {
	cellServices, exists := p.config.Cells[cellName]
	if !exists {
		return nil, fmt.Errorf("cell %s not found in configuration", cellName)
	}

	switch service {
	case "multigateway":
		return map[string]any{
			"path":      cellServices.Multigateway.Path,
			"http_port": cellServices.Multigateway.HttpPort,
			"grpc_port": cellServices.Multigateway.GrpcPort,
			"pg_port":   cellServices.Multigateway.PgPort,
			"log_level": cellServices.Multigateway.LogLevel,
		}, nil
	case "multipooler":
		return map[string]any{
			"path":        cellServices.Multipooler.Path,
			"database":    cellServices.Multipooler.Database,
			"table_group": cellServices.Multipooler.TableGroup,
			"service-id":  cellServices.Multipooler.ServiceID,
			"http_port":   cellServices.Multipooler.HttpPort,
			"grpc_port":   cellServices.Multipooler.GrpcPort,
			"log_level":   cellServices.Multipooler.LogLevel,
			"pooler_dir":  cellServices.Multipooler.PoolerDir,
			"pg_port":     cellServices.Multipooler.PgPort,
		}, nil
	case "multiorch":
		return map[string]any{
			"path":      cellServices.Multiorch.Path,
			"http_port": cellServices.Multiorch.HttpPort,
			"grpc_port": cellServices.Multiorch.GrpcPort,
			"log_level": cellServices.Multiorch.LogLevel,
		}, nil
	case "pgctld":
		return map[string]any{
			"path":        cellServices.Pgctld.Path,
			"pooler_dir":  cellServices.Pgctld.PoolerDir,
			"grpc_port":   cellServices.Pgctld.GrpcPort,
			"pg_port":     cellServices.Pgctld.PgPort,
			"pg_database": cellServices.Pgctld.PgDatabase,
			"pg_user":     cellServices.Pgctld.PgUser,
			"pg_pwfile":   cellServices.Pgctld.PgPwfile,
			"timeout":     cellServices.Pgctld.Timeout,
			"log_level":   cellServices.Pgctld.LogLevel,
		}, nil
	default:
		return nil, fmt.Errorf("unknown service %s", service)
	}
}

// NewLocalProvisioner creates a new local provisioner instance
func NewLocalProvisioner() (provisioner.Provisioner, error) {
	p := &localProvisioner{
		config: &LocalProvisionerConfig{},
	}

	return p, nil
}

// getMapKeys returns the keys of a map for debugging purposes
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func init() {
	// Register the local provisioner
	provisioner.RegisterProvisioner("local", NewLocalProvisioner)
}
