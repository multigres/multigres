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
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/provisioner/local/ports"
	"github.com/multigres/multigres/go/tools/pathutil"
	"github.com/multigres/multigres/go/tools/semver"
	"github.com/multigres/multigres/go/tools/stringutil"
	"github.com/multigres/multigres/go/tools/timertools"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"gopkg.in/yaml.v3"
)

// localProvisioner implements the Provisioner interface for local binary-based provisioning
type localProvisioner struct {
	config  *LocalProvisionerConfig
	dataDir string // Base data directory for this provisioner instance
}

// Compile-time check to ensure localProvisioner implements Provisioner
var _ provisioner.Provisioner = (*localProvisioner)(nil)

const (
	// StateDir is the directory name where provision state files are stored
	StateDir = "state"
)

// Name returns the name of this provisioner
func (p *localProvisioner) Name() string {
	return "local"
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

	// Get port from config or use default
	port := ports.DefaultEtcdPort
	if p, ok := etcdConfig["port"].(int); ok && p > 0 {
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

// findBinary finds a binary by name, checking PATH first, then the executable directory,
// and then the optional configured path
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
	httpPort := ports.DefaultMultigatewayHTTP
	if p, ok := multigatewayConfig["http_port"].(int); ok && p > 0 {
		httpPort = p
	}

	// Get gRPC port from cell-specific config
	grpcPort := ports.DefaultMultigatewayGRPC
	if p, ok := multigatewayConfig["grpc_port"].(int); ok && p > 0 {
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
		"--hostname", "localhost",
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
	httpPort := ports.DefaultMultiadminHTTP
	if p, ok := multiadminConfig["http_port"].(int); ok && p > 0 {
		httpPort = p
	}

	// Get gRPC port from config
	grpcPort := ports.DefaultMultiadminGRPC
	if p, ok := multiadminConfig["grpc_port"].(int); ok && p > 0 {
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
		"--hostname", "localhost",
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
	httpPort := ports.DefaultMultipoolerHTTP
	if p, ok := multipoolerConfig["http_port"].(int); ok && p > 0 {
		httpPort = p
	}

	// Get grpc port from cell-specific config
	grpcPort := ports.DefaultMultipoolerGRPC
	if port, ok := multipoolerConfig["grpc_port"].(int); ok && port > 0 {
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
	pgPort := ports.DefaultPostgresPort
	if port, ok := multipoolerConfig["pg_port"].(int); ok && port > 0 {
		pgPort = port
	}

	// Get gRPC socket file if configured
	socketFile, err := getGRPCSocketFile(multipoolerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure gRPC socket file: %w", err)
	}
	if socketFile != "" {
		fmt.Printf("▶️  - Configuring multipooler gRPC Unix socket: %s\n", socketFile)
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
		"--hostname", "localhost",
	}

	// Add socket file if configured
	if socketFile != "" {
		args = append(args, "--grpc-socket-file", socketFile)
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
	httpPort := ports.DefaultMultiorchHTTP
	if p, ok := multiorchConfig["http_port"].(int); ok && p > 0 {
		httpPort = p
	}

	// Get grpc port from cell-specific config
	grpcPort := ports.DefaultMultiorchGRPC
	if port, ok := multiorchConfig["grpc_port"].(int); ok && port > 0 {
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
		"--hostname", "localhost",
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
		fallthrough
	case "multiadmin":
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
			return fmt.Errorf("failed to stop process: %w", err)
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

	// Wait for the process to actually exit
	p.waitForProcessExit(process, 2*time.Second)

	return nil
}

// waitForProcessExit waits for a process to exit by polling with Signal(0)
func (p *localProvisioner) waitForProcessExit(process *os.Process, timeout time.Duration) {
	ticker := timertools.NewBackoffTicker(10*time.Millisecond, 1*time.Second)
	ticker.C <- time.Now()
	defer ticker.Stop()
	timeoutch := time.After(timeout)
	for {
		select {
		case <-ticker.C:
			// Send null signal to test if process exists
			err := process.Signal(syscall.Signal(0))
			if err != nil {
				fmt.Printf("Process %d stopped successfully\n", process.Pid)
				// Process has exited or doesn't exist
				return
			}
		case <-timeoutch:
			fmt.Printf("Process %d still running after SIGTERM\n", process.Pid)
			// No need to wait further
			return
		}
	}
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

		socketsDir := filepath.Join(p.config.RootWorkingDir, "sockets")
		if err := p.cleanupSocketsDirectory(socketsDir); err != nil {
			fmt.Printf("Warning: failed to clean up sockets directory: %v\n", err)
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

// cleanupSocketsDirectory removes the entire sockets directory and all its contents
func (p *localProvisioner) cleanupSocketsDirectory(socketsDir string) error {
	if _, err := os.Stat(socketsDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, nothing to clean up
	}

	if err := os.RemoveAll(socketsDir); err != nil {
		return fmt.Errorf("failed to remove sockets directory %s: %w", socketsDir, err)
	}

	fmt.Printf("Cleaned up sockets directory: %s\n", socketsDir)
	return nil
}

// getGRPCSocketFile extracts and prepares the gRPC socket file path from a service config.
// It returns the absolute path to the socket file and ensures the socket directory exists.
// Returns empty string if no socket file is configured.
func getGRPCSocketFile(serviceConfig map[string]any) (string, error) {
	sf, ok := serviceConfig["grpc_socket_file"].(string)
	if !ok || sf == "" {
		return "", nil // No socket file configured
	}

	// Convert to absolute path since the working directory may change
	socketFile, err := filepath.Abs(sf)
	if err != nil {
		return "", fmt.Errorf("failed to resolve socket file path: %w", err)
	}

	// Ensure socket directory exists
	socketDir := filepath.Dir(socketFile)
	if err := os.MkdirAll(socketDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create socket directory: %w", err)
	}

	return socketFile, nil
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

	var servicesStopped atomic.Int64

	var wg sync.WaitGroup
	for _, service := range services {
		fmt.Printf("Stopping %s service (ID: %s) for database %s...\n", service.Service, service.ID, databaseName)

		req := &provisioner.DeprovisionRequest{
			Service:      service.Service,
			ServiceID:    service.ID,
			DatabaseName: databaseName,
			Clean:        true, // Clean up data when deprovisioning database
		}

		wg.Go(func() {
			if err := p.stopService(ctx, req); err != nil {
				fmt.Printf("Warning: failed to stop %s service: %v\n", service.Service, err)
			}
			// Remove state file
			if err := p.removeServiceState(service.ID, req.Service, req.DatabaseName); err != nil {
				fmt.Printf("Warning: failed to remove state file: %v\n", err)
			}
			servicesStopped.Add(1)
		})
	}
	wg.Wait()

	fmt.Printf("Database %s deprovisioned successfully (%d services stopped)\n", databaseName, servicesStopped.Load())
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

// NewLocalProvisioner creates a new local provisioner instance
func NewLocalProvisioner() (provisioner.Provisioner, error) {
	p := &localProvisioner{
		config: &LocalProvisionerConfig{},
	}

	return p, nil
}

func getExecutablePath() (string, error) {
	executablePath, err := os.Executable()
	if err != nil {
		executablePath, err = os.Getwd()
	}
	return filepath.Dir(executablePath), err
}

func init() {
	// Register the local provisioner
	provisioner.RegisterProvisioner("local", NewLocalProvisioner)

	// Add the executable directory to the PATH. We're expecting
	// to find the other executables in the same directory.
	if binDir, err := getExecutablePath(); err == nil {
		pathutil.PrependPath(binDir)
	} else {
		slog.Error(fmt.Sprintf("Local Provisioner failed to get executable path: %v", err))
		os.Exit(1)
	}
}
