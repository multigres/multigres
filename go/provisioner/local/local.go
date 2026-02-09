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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/mod/semver"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/provisioner/local/ports"
	"github.com/multigres/multigres/go/tools/pathutil"
	"github.com/multigres/multigres/go/tools/retry"
	"github.com/multigres/multigres/go/tools/stringutil"
	"github.com/multigres/multigres/go/tools/telemetry"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"gopkg.in/yaml.v3"
)

var tracer = otel.Tracer("github.com/multigres/multigres/go/provisioner/local")

// localProvisioner implements the Provisioner interface for local binary-based provisioning
type localProvisioner struct {
	config              *LocalProvisionerConfig
	pgBackRestCertPaths *PgBackRestCertPaths
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

// createPoolerDirectoryWithPassword creates the pooler directory structure and password file
// at the conventional location (poolerDir/pgpassword.txt).
// If sourcePasswordFile is provided and exists, its content is copied; otherwise "postgres" is used.
func createPoolerDirectoryWithPassword(poolerDir, sourcePasswordFile string) error {
	// Create the pooler directory structure
	if err := os.MkdirAll(poolerDir, 0o755); err != nil {
		return fmt.Errorf("failed to create pooler directory %s: %w", poolerDir, err)
	}

	// Conventional password file location
	conventionalPwfile := filepath.Join(poolerDir, "pgpassword.txt")

	// Determine password content
	password := []byte("postgres")
	if sourcePasswordFile != "" {
		if content, err := os.ReadFile(sourcePasswordFile); err == nil {
			password = content
		}
		// If source file doesn't exist, fall back to default "postgres"
	}

	// Create the password file at the conventional location
	if err := os.WriteFile(conventionalPwfile, password, 0o600); err != nil {
		return fmt.Errorf("failed to create password file %s: %w", conventionalPwfile, err)
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

		if err := createPoolerDirectoryWithPassword(poolerDir, cellConfig.Pgctld.PgPwfile); err != nil {
			return fmt.Errorf("failed to initialize pgctld directory for cell %s: %w", cellName, err)
		}

		conventionalPwfile := filepath.Join(poolerDir, "pgpassword.txt")
		fmt.Printf("✓ Created pooler directory: %s\n", poolerDir)
		fmt.Printf("✓ Created password file: %s\n", conventionalPwfile)
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

	// Get peer port from config, or default to port + 1
	peerPort := port + 1
	if pp, ok := etcdConfig["peer-port"].(int); ok && pp > 0 {
		peerPort = pp
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
		return nil, errors.New("etcd data directory not found in config")
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

	if err := telemetry.StartCmd(ctx, etcdCmd); err != nil {
		return nil, fmt.Errorf("failed to start etcd: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(etcdCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("etcd process validation failed: %w", err)
	}

	// Wait for etcd to be ready
	servicePorts := map[string]int{"etcd_port": port}
	if err := p.waitForServiceReady(ctx, "etcd", "localhost", servicePorts, 10*time.Second); err != nil {
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
		return "Log file not found: " + logFile
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
	return filepath.Join(baseDir, "data", "pooler_"+serviceID)
}

// provisionMultigateway provisions multigateway using either binaries or Docker containers
func (p *localProvisioner) provisionMultigateway(ctx context.Context, req *provisioner.ProvisionRequest) (*provisioner.ProvisionResult, error) {
	// Sanity check: ensure this method is called for multigateway service
	if req.Service != constants.ServiceMultigateway {
		return nil, fmt.Errorf("provisionMultigateway called for wrong service type: %s", req.Service)
	}

	// Get cell parameter
	cell := req.Params["cell"].(string)

	// Check if multigateway is already running
	existingService, err := p.findRunningDbService(constants.ServiceMultigateway, req.DatabaseName, cell)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing multigateway service: %w", err)
	}

	if existingService != nil {
		fmt.Printf("multigateway is already running (PID %d) ✓\n", existingService.PID)
		return &provisioner.ProvisionResult{
			ServiceName: constants.ServiceMultigateway,
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
	topoGlobalRoot := req.Params["topo_global_root"].(string)

	// Get cell-specific multigateway config
	multigatewayConfig, err := p.getCellServiceConfig(cell, constants.ServiceMultigateway)
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

	// Get pg port from cell-specific config
	pgPort := ports.DefaultMultigatewayPG
	if p, ok := multigatewayConfig["pg_port"].(int); ok && p > 0 {
		pgPort = p
	}

	// Get log level
	logLevel := "info"
	if level, ok := multigatewayConfig["log_level"].(string); ok {
		logLevel = level
	}

	// Find multigateway binary
	multigatewayBinary, err := p.findBinary(constants.ServiceMultigateway, multigatewayConfig)
	if err != nil {
		return nil, fmt.Errorf("multigateway binary not found: %w", err)
	}

	// Generate unique ID for this service instance (needed for log file)
	serviceID := stringutil.RandomString(8)

	// Create log file path
	logFile, err := p.createLogFile(constants.ServiceMultigateway, serviceID, req.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Build command arguments
	args := []string{
		"--http-port", strconv.Itoa(httpPort),
		"--grpc-port", strconv.Itoa(grpcPort),
		"--pg-port", strconv.Itoa(pgPort),
		"--topo-global-server-addresses", etcdAddress,
		"--topo-global-root", topoGlobalRoot,
		"--cell", cell,
		"--log-level", logLevel,
		"--log-output", logFile,
		"--hostname", "localhost",
	}

	// Start multigateway process
	multigatewayCmd := exec.CommandContext(ctx, multigatewayBinary, args...)

	fmt.Printf("▶️  - Launching multigateway (HTTP:%d, gRPC:%d, pg:%d)...", httpPort, grpcPort, pgPort)

	if err := telemetry.StartCmd(ctx, multigatewayCmd); err != nil {
		return nil, fmt.Errorf("failed to start multigateway: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(multigatewayCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("multigateway process validation failed: %w", err)
	}

	// Create provision state
	service := &LocalProvisionedService{
		ID:         serviceID,
		Service:    constants.ServiceMultigateway,
		PID:        multigatewayCmd.Process.Pid,
		BinaryPath: multigatewayBinary,
		Ports:      map[string]int{"http_port": httpPort, "grpc_port": grpcPort, "pg_port": pgPort},
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
	servicePorts := map[string]int{"http_port": httpPort, "grpc_port": grpcPort, "pg_port": pgPort}
	if err := p.waitForServiceReady(ctx, constants.ServiceMultigateway, "localhost", servicePorts, 10*time.Second); err != nil {
		logs := p.readServiceLogs(logFile, 20)
		return nil, fmt.Errorf("multigateway readiness check failed: %w\n\nLast 20 lines from multigateway logs:\n%s", err, logs)
	}
	fmt.Printf(" ready ✓\n")

	return &provisioner.ProvisionResult{
		ServiceName: constants.ServiceMultigateway,
		FQDN:        "localhost",
		Ports: map[string]int{
			"http_port": httpPort,
			"grpc_port": grpcPort,
			"pg_port":   pgPort,
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
	if req.Service != constants.ServiceMultiadmin {
		return nil, fmt.Errorf("provisionMultiadmin called for wrong service type: %s", req.Service)
	}

	// Check if multiadmin is already running
	existingService, err := p.findRunningService(constants.ServiceMultiadmin)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing multiadmin service: %w", err)
	}

	if existingService != nil {
		fmt.Printf("multiadmin is already running (PID %d) ✓\n", existingService.PID)
		return &provisioner.ProvisionResult{
			ServiceName: constants.ServiceMultiadmin,
			FQDN:        existingService.FQDN,
			Ports:       existingService.Ports,
			Metadata: map[string]any{
				"service_id": existingService.ID,
				"log_file":   existingService.LogFile,
			},
		}, nil
	}

	// Get multiadmin config
	multiadminConfig := p.getServiceConfig(constants.ServiceMultiadmin)

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
	topoGlobalRoot := req.Params["topo_global_root"].(string)

	// Get log level
	logLevel := "info"
	if level, ok := multiadminConfig["log_level"].(string); ok {
		logLevel = level
	}

	// Find multiadmin binary
	multiadminBinary, err := p.findBinary(constants.ServiceMultiadmin, multiadminConfig)
	if err != nil {
		return nil, fmt.Errorf("multiadmin binary not found: %w", err)
	}

	// Generate unique ID for this service instance (needed for log file)
	serviceID := stringutil.RandomString(8)

	// Create log file path
	logFile, err := p.createLogFile(constants.ServiceMultiadmin, serviceID, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Build command arguments
	args := []string{
		"--http-port", strconv.Itoa(httpPort),
		"--grpc-port", strconv.Itoa(grpcPort),
		"--topo-global-server-addresses", etcdAddress,
		"--topo-global-root", topoGlobalRoot,
		"--log-level", logLevel,
		"--log-output", logFile,
		"--service-map", "grpc-multiadmin",
		"--hostname", "localhost",
	}

	// Start multiadmin process
	multiadminCmd := exec.CommandContext(ctx, multiadminBinary, args...)

	fmt.Printf("▶️  - Launching multiadmin (HTTP:%d, gRPC:%d)...", httpPort, grpcPort)

	if err := telemetry.StartCmd(ctx, multiadminCmd); err != nil {
		return nil, fmt.Errorf("failed to start multiadmin: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(multiadminCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("multiadmin process validation failed: %w", err)
	}

	// Create provision state
	service := &LocalProvisionedService{
		ID:         serviceID,
		Service:    constants.ServiceMultiadmin,
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
	if err := p.waitForServiceReady(ctx, constants.ServiceMultiadmin, "localhost", servicePorts, 10*time.Second); err != nil {
		logs := p.readServiceLogs(logFile, 20)
		return nil, fmt.Errorf("multiadmin readiness check failed: %w\n\nLast 20 lines from multiadmin logs:\n%s", err, logs)
	}
	fmt.Printf(" ready ✓\n")

	return &provisioner.ProvisionResult{
		ServiceName: constants.ServiceMultiadmin,
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
	if req.Service != constants.ServiceMultipooler {
		return nil, fmt.Errorf("provisionMultipooler called for wrong service type: %s", req.Service)
	}

	// Get cell parameter
	cell := req.Params["cell"].(string)

	// Check if multipooler is already running
	existingService, err := p.findRunningDbService(constants.ServiceMultipooler, req.DatabaseName, cell)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing multipooler service: %w", err)
	}
	if existingService != nil {
		fmt.Printf("multipooler is already running (PID %d) ✓\n", existingService.PID)
		return &provisioner.ProvisionResult{
			ServiceName: constants.ServiceMultipooler,
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
	topoGlobalRoot := req.Params["topo_global_root"].(string)

	// Get cell-specific multipooler config
	multipoolerConfig, err := p.getCellServiceConfig(cell, constants.ServiceMultipooler)
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

	// Get shard from multipooler config, default to "0-inf" if not set
	shard := "0-inf"
	if shardFromConfig, ok := multipoolerConfig["shard"].(string); ok && shardFromConfig != "" {
		shard = shardFromConfig
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
	pgPort := ports.DefaultLocalPostgresPort
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
	multipoolerBinary, err := p.findBinary(constants.ServiceMultipooler, multipoolerConfig)
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
	logFile, err := p.createLogFile(constants.ServiceMultipooler, serviceID, req.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Provision pgctld for this multipooler
	pgctldResult, err := p.provisionPgctld(ctx, database, tableGroup, serviceID, cell)
	if err != nil {
		return nil, fmt.Errorf("failed to provision pgctld for multipooler: %w", err)
	}

	// Construct the PostgreSQL socket file path: <poolerDir>/pg_sockets/.s.PGSQL.<port>
	pgSocketFile := filepath.Join(poolerDir, "pg_sockets", fmt.Sprintf(".s.PGSQL.%d", pgPort))

	// Build command arguments with pgctld-addr
	args := []string{
		"--http-port", strconv.Itoa(httpPort),
		"--grpc-port", strconv.Itoa(grpcPort),
		"--topo-global-server-addresses", etcdAddress,
		"--topo-global-root", topoGlobalRoot,
		"--cell", cell,
		"--database", database,
		"--table-group", tableGroup,
		"--shard", shard,
		"--service-id", serviceID,
		"--pgctld-addr", pgctldResult.Address,
		"--log-level", logLevel,
		"--log-output", logFile,
		"--pooler-dir", poolerDir,
		"--pg-port", strconv.Itoa(pgPort),
		"--hostname", "localhost",
		"--connpool-admin-password", "postgres", // Password created in initializePgctldDirectories
		"--socket-file", pgSocketFile, // PostgreSQL Unix socket for trust auth
	}

	// Add socket file if configured
	if socketFile != "" {
		args = append(args, "--grpc-socket-file", socketFile)
	}

	// Add service map configuration to enable grpc-pooler service
	args = append(args, "--service-map", "grpc-pooler")

	// Get pgbackrest port from config
	pgbackrestConfig, err := p.getCellServiceConfig(cell, constants.ServicePgbackrest)
	if err != nil {
		return nil, fmt.Errorf("failed to get pgbackrest config for cell %s: %w", cell, err)
	}
	pgbackrestPort := ports.DefaultPgbackRestPort
	if port, ok := pgbackrestConfig["port"].(int); ok && port > 0 {
		pgbackrestPort = port
	}

	// Add pgbackrest TLS certificate paths and port
	args = append(args,
		"--pgbackrest-cert-file", p.pgBackRestCertPaths.ServerCertFile,
		"--pgbackrest-key-file", p.pgBackRestCertPaths.ServerKeyFile,
		"--pgbackrest-ca-file", p.pgBackRestCertPaths.CACertFile,
		"--pgbackrest-port", strconv.Itoa(pgbackrestPort),
	)

	// Start multipooler process
	multipoolerCmd := exec.CommandContext(ctx, multipoolerBinary, args...)

	fmt.Printf("▶️  - Launching multipooler (HTTP:%d, gRPC:%d)...", httpPort, grpcPort)

	if err := telemetry.StartCmd(ctx, multipoolerCmd); err != nil {
		return nil, fmt.Errorf("failed to start multipooler: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(multipoolerCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("multipooler process validation failed: %w", err)
	}

	// Wait for multipooler to be ready
	servicePorts := map[string]int{"http_port": httpPort, "grpc_port": grpcPort}
	if err := p.waitForServiceReady(ctx, constants.ServiceMultipooler, "localhost", servicePorts, 10*time.Second); err != nil {
		logs := p.readServiceLogs(logFile, 20)
		return nil, fmt.Errorf("multipooler readiness check failed: %w\n\nLast 20 lines from multipooler logs:\n%s", err, logs)
	}
	fmt.Printf(" ready ✓\n")

	// Create provision state
	service := &LocalProvisionedService{
		ID:         serviceID,
		Service:    constants.ServiceMultipooler,
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
		ServiceName: constants.ServiceMultipooler,
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
	if req.Service != constants.ServiceMultiorch {
		return nil, fmt.Errorf("provisionMultiOrch called for wrong service type: %s", req.Service)
	}

	// Get cell parameter
	cell := req.Params["cell"].(string)

	// Check if multiorch is already running
	existingService, err := p.findRunningDbService(constants.ServiceMultiorch, req.DatabaseName, cell)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing multiorch service: %w", err)
	}
	if existingService != nil {
		fmt.Printf("multiorch is already running (PID %d) ✓\n", existingService.PID)
		return &provisioner.ProvisionResult{
			ServiceName: constants.ServiceMultiorch,
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
	topoGlobalRoot := req.Params["topo_global_root"].(string)
	cell = req.Params["cell"].(string)

	// Get cell-specific multiorch config
	multiorchConfig, err := p.getCellServiceConfig(cell, constants.ServiceMultiorch)
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
	multiorchBinary, err := p.findBinary(constants.ServiceMultiorch, multiorchConfig)
	if err != nil {
		return nil, fmt.Errorf("multiorch binary not found: %w", err)
	}

	// Generate unique ID for this service instance (needed for log file)
	serviceID := stringutil.RandomString(8)

	// Create log file path
	logFile, err := p.createLogFile(constants.ServiceMultiorch, serviceID, req.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Build command arguments
	args := []string{
		"--http-port", strconv.Itoa(httpPort),
		"--grpc-port", strconv.Itoa(grpcPort),
		"--topo-global-server-addresses", etcdAddress,
		"--topo-global-root", topoGlobalRoot,
		"--cell", cell,
		"--watch-targets", req.DatabaseName,
		"--log-level", logLevel,
		"--log-output", logFile,
		"--hostname", "localhost",
	}

	// Add optional interval configs if specified
	if interval, ok := multiorchConfig["cluster_metadata_refresh_interval"].(string); ok && interval != "" {
		args = append(args, "--cluster-metadata-refresh-interval", interval)
	}
	if interval, ok := multiorchConfig["pooler_health_check_interval"].(string); ok && interval != "" {
		args = append(args, "--pooler-health-check-interval", interval)
	}
	if interval, ok := multiorchConfig["recovery_cycle_interval"].(string); ok && interval != "" {
		args = append(args, "--recovery-cycle-interval", interval)
	}

	// Start multiorch process
	multiorchCmd := exec.CommandContext(ctx, multiorchBinary, args...)

	fmt.Printf("▶️  - Launching multiorch (HTTP:%d, gRPC:%d)...", httpPort, grpcPort)

	if err := telemetry.StartCmd(ctx, multiorchCmd); err != nil {
		return nil, fmt.Errorf("failed to start multiorch: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(multiorchCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("multiorch process validation failed: %w", err)
	}

	// Wait for multiorch to be ready
	servicePorts := map[string]int{"http_port": httpPort, "grpc_port": grpcPort}
	if err := p.waitForServiceReady(ctx, constants.ServiceMultiorch, "localhost", servicePorts, 10*time.Second); err != nil {
		logs := p.readServiceLogs(logFile, 20)
		return nil, fmt.Errorf("multiorch readiness check failed: %w\n\nLast 20 lines from multiorch logs:\n%s", err, logs)
	}
	fmt.Printf(" ready ✓\n")

	// Create provision state
	service := &LocalProvisionedService{
		ID:         serviceID,
		Service:    constants.ServiceMultiorch,
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
		ServiceName: constants.ServiceMultiorch,
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
	case constants.ServiceMultigateway:
		fallthrough
	case constants.ServiceMultiorch:
		fallthrough
	case constants.ServiceMultipooler:
		fallthrough
	case constants.ServiceMultiadmin:
		return p.deprovisionService(ctx, req)
	case constants.ServicePgbackrest:
		return p.deprovisionPgbackRestServer(ctx, req)
	case constants.ServicePgctld:
		// pgctld requires special handling to stop PostgreSQL first
		service, err := p.loadServiceState(req)
		if err != nil {
			return err
		}
		if service == nil {
			return errors.New("pgctld service not found")
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
		return errors.New("service not found")
	}

	// Stop the process if it's running
	if service.PID > 0 {
		if err := p.stopProcessByPID(ctx, service.Service, service.PID); err != nil {
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
func (p *localProvisioner) stopProcessByPID(ctx context.Context, name string, pid int) error {
	ctx, span := tracer.Start(ctx, "stopProcessByPID")
	span.SetAttributes(attribute.String("service", name))
	defer span.End()

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
		errMsg := err.Error()
		if strings.Contains(errMsg, "no such process") || strings.Contains(errMsg, "process already finished") {
			fmt.Printf("Process %d already stopped\n", pid)
			return nil
		}

		// If SIGTERM fails for other reasons, try SIGKILL
		if err := process.Kill(); err != nil {
			// If kill also fails and it's because process doesn't exist, that's ok
			errMsg := err.Error()
			if strings.Contains(errMsg, "no such process") || strings.Contains(errMsg, "process already finished") {
				fmt.Printf("Process %d already stopped\n", pid)
				return nil
			}
			return fmt.Errorf("failed to kill process %d: %w", pid, err)
		}
	}

	// Wait for the process to actually exit
	p.waitForProcessExit(ctx, process, 2*time.Second)

	return nil
}

// waitForProcessExit waits for a process to exit by polling with Signal(0).
// If the process doesn't exit within the timeout, it sends SIGKILL and waits again.
func (p *localProvisioner) waitForProcessExit(ctx context.Context, process *os.Process, timeout time.Duration) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	r := retry.New(10*time.Millisecond, 1*time.Second)
	for _, err := range r.Attempts(waitCtx) {
		if err != nil {
			// Timeout reached, escalate to SIGKILL
			fmt.Printf("Process %d still running after SIGTERM, sending SIGKILL\n", process.Pid)
			if killErr := process.Kill(); killErr != nil {
				errMsg := killErr.Error()
				if strings.Contains(errMsg, "no such process") || strings.Contains(errMsg, "process already finished") {
					fmt.Printf("Process %d already stopped\n", process.Pid)
					return
				}
				fmt.Printf("Failed to kill process %d: %v\n", process.Pid, killErr)
				return
			}
			// Wait a bit more for SIGKILL to take effect
			p.waitForProcessExitAfterKill(ctx, process, 5*time.Second)
			return
		}

		// Send null signal to test if process exists
		err := process.Signal(syscall.Signal(0))
		if err != nil {
			fmt.Printf("Process %d stopped successfully\n", process.Pid)
			// Process has exited or doesn't exist
			return
		}
	}
}

// waitForProcessExitAfterKill waits for a process to exit after SIGKILL.
func (p *localProvisioner) waitForProcessExitAfterKill(ctx context.Context, process *os.Process, timeout time.Duration) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	r := retry.New(50*time.Millisecond, 1*time.Second)
	for _, err := range r.Attempts(waitCtx) {
		if err != nil {
			fmt.Printf("Process %d still running after SIGKILL (timeout)\n", process.Pid)
			return
		}

		err := process.Signal(syscall.Signal(0))
		if err != nil {
			fmt.Printf("Process %d stopped after SIGKILL\n", process.Pid)
			return
		}
	}
}

// Bootstrap sets up etcd and creates the default database
func (p *localProvisioner) Bootstrap(ctx context.Context) ([]*provisioner.ProvisionResult, error) {
	// Validate binary paths before starting
	if err := p.validateBinaryPaths(p.config); err != nil {
		return nil, err
	}

	// Validate required system binaries before starting
	if err := p.validateSystemBinaries(); err != nil {
		return nil, err
	}

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
		Service: constants.ServiceMultiadmin,
		Params: map[string]any{
			"etcd_address":     etcdAddress,
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
			if service.Service == constants.ServiceMultiadmin {
				req := &provisioner.DeprovisionRequest{
					Service:      constants.ServiceMultiadmin,
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

		spoolDir := filepath.Join(p.config.RootWorkingDir, "spool")
		if err := p.cleanupSpoolDirectory(spoolDir); err != nil {
			fmt.Printf("Warning: failed to clean up spool directory: %v\n", err)
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

// cleanupSpoolDirectory removes the entire spool directory and all its contents
func (p *localProvisioner) cleanupSpoolDirectory(spoolDir string) error {
	if _, err := os.Stat(spoolDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, nothing to clean up
	}

	if err := os.RemoveAll(spoolDir); err != nil {
		return fmt.Errorf("failed to remove spool directory %s: %w", spoolDir, err)
	}

	fmt.Printf("Cleaned up spool directory: %s\n", spoolDir)
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
		return "", errors.New("provisioner config not set")
	}

	if p.config.DefaultDbName == "" {
		return "", errors.New("default-dbname not specified in configuration")
	}

	return p.config.DefaultDbName, nil
}

// ProvisionDatabase provisions a complete database stack in all cells (assumes etcd is already running and cells are configured)
func (p *localProvisioner) ProvisionDatabase(ctx context.Context, databaseName string, etcdAddress string) ([]*provisioner.ProvisionResult, error) {
	fmt.Printf("=== Provisioning database: %s ===\n", databaseName)
	fmt.Println("")

	// Create backup directory if using local backups
	if p.config.Backup.Local != nil {
		if p.config.Backup.Local.Path == "" {
			p.config.Backup.Local.Path = filepath.Join(p.config.RootWorkingDir, "data", "backups")
		}
		if err := os.MkdirAll(p.config.Backup.Local.Path, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create backup directory %s: %w", p.config.Backup.Local.Path, err)
		}
	}
	// S3 backups don't need local directory creation

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

	ts, err := topoclient.OpenServer(topoclient.DefaultTopoImplementation, topoConfig.GlobalRootPath, []string{etcdAddress}, topoclient.NewDefaultTopoConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to topology server: %w", err)
	}
	defer ts.Close()

	// Check if database already exists
	_, err = ts.GetDatabase(ctx, databaseName)
	if err == nil {
		fmt.Printf("⚙️  - Database \"%s\" detected — reusing existing database ✓\n", databaseName)
	} else if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
		// Create the database if it doesn't exist
		fmt.Printf("⚙️  - Creating database \"%s\" with cells: [%s]...\n", databaseName, strings.Join(cellNames, ", "))

		backupLocation, err := p.buildBackupLocation()
		if err != nil {
			return nil, fmt.Errorf("failed to build backup location: %w", err)
		}

		databaseConfig := &clustermetadatapb.Database{
			Name:             databaseName,
			BackupLocation:   backupLocation,
			DurabilityPolicy: "ANY_2",   // Default durability policy for bootstrap
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

	// Generate pgBackRest certificates before starting services
	if err := p.generatePgBackRestCertsOnce(ctx); err != nil {
		return nil, err
	}

	// Provision all services in parallel across all cells.
	// Multiorch's bootstrap action has a quorum check that will wait for enough
	// poolers to be available before attempting bootstrap, so strict ordering
	// is not required.
	fmt.Println("=== Starting all services in parallel ===")

	type provisionResult struct {
		result *provisioner.ProvisionResult
		err    error
	}

	// Calculate total number of services to provision
	numServices := len(cellNames) * 4 // multigateway + multipooler + multiorch + pgbackrest per cell
	resultsChan := make(chan provisionResult, numServices)

	// Start all services in parallel
	for _, cellName := range cellNames {
		cell := cellName // capture for goroutine

		// Start multigateway
		go func() {
			req := &provisioner.ProvisionRequest{
				Service:      constants.ServiceMultigateway,
				DatabaseName: databaseName,
				Params: map[string]any{
					"etcd_address":     etcdAddress,
					"topo_global_root": topoConfig.GlobalRootPath,
					"cell":             cell,
				},
			}
			result, err := p.provisionMultigateway(ctx, req)
			if err != nil {
				resultsChan <- provisionResult{err: fmt.Errorf("failed to provision multigateway in cell %s: %w", cell, err)}
				return
			}
			resultsChan <- provisionResult{result: result}
		}()

		// Start multipooler
		go func() {
			req := &provisioner.ProvisionRequest{
				Service:      constants.ServiceMultipooler,
				DatabaseName: databaseName,
				Params: map[string]any{
					"etcd_address":     etcdAddress,
					"topo_global_root": topoConfig.GlobalRootPath,
					"cell":             cell,
				},
			}
			result, err := p.provisionMultipooler(ctx, req)
			if err != nil {
				resultsChan <- provisionResult{err: fmt.Errorf("failed to provision multipooler in cell %s: %w", cell, err)}
				return
			}
			resultsChan <- provisionResult{result: result}
		}()

		// Start multiorch
		go func() {
			req := &provisioner.ProvisionRequest{
				Service:      constants.ServiceMultiorch,
				DatabaseName: databaseName,
				Params: map[string]any{
					"etcd_address":     etcdAddress,
					"topo_global_root": topoConfig.GlobalRootPath,
					"cell":             cell,
				},
			}
			result, err := p.provisionMultiOrch(ctx, req)
			if err != nil {
				resultsChan <- provisionResult{err: fmt.Errorf("failed to provision multiorch in cell %s: %w", cell, err)}
				return
			}
			resultsChan <- provisionResult{result: result}
		}()

		// Start pgbackrest
		go func() {
			// Provision pgbackrest server for this cell
			_, err := p.provisionPgbackRestServer(ctx, databaseName, cell)
			if err != nil {
				resultsChan <- provisionResult{err: fmt.Errorf("failed to provision pgbackrest server for cell %s: %w", cell, err)}
				return
			}
			// pgbackrest doesn't return a ProvisionResult in the same format, so we send a nil result
			resultsChan <- provisionResult{result: nil}
		}()
	}

	// Collect all results
	var results []*provisioner.ProvisionResult
	var provisionErrors []error
	for range numServices {
		res := <-resultsChan
		if res.err != nil {
			provisionErrors = append(provisionErrors, res.err)
		} else if res.result != nil {
			// Only append non-nil results (pgbackrest returns nil)
			results = append(results, res.result)
		}
	}

	// Report any errors
	if len(provisionErrors) > 0 {
		return nil, fmt.Errorf("failed to provision services: %v", provisionErrors)
	}

	fmt.Println("")
	fmt.Printf("✓ All cells provisioned successfully\n\n")

	fmt.Printf("Database %s provisioned successfully across %d cells with %d total services\n", databaseName, len(cellNames), len(results))
	return results, nil
}

// buildBackupLocation creates a BackupLocation proto from config
func (p *localProvisioner) buildBackupLocation() (*clustermetadatapb.BackupLocation, error) {
	switch p.config.Backup.Type {
	case "":
		// No backup type configured - use default filesystem backup location
		defaultPath := filepath.Join(p.config.RootWorkingDir, "data", "backups")
		if err := os.MkdirAll(defaultPath, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create default backup directory %s: %w", defaultPath, err)
		}
		return &clustermetadatapb.BackupLocation{
			Location: &clustermetadatapb.BackupLocation_Filesystem{
				Filesystem: &clustermetadatapb.FilesystemBackup{
					Path: defaultPath,
				},
			},
		}, nil

	case "local":
		if p.config.Backup.Local == nil || p.config.Backup.Local.Path == "" {
			return nil, errors.New("backup path not configured")
		}

		return &clustermetadatapb.BackupLocation{
			Location: &clustermetadatapb.BackupLocation_Filesystem{
				Filesystem: &clustermetadatapb.FilesystemBackup{
					Path: p.config.Backup.Local.Path,
				},
			},
		}, nil

	case "s3":
		if p.config.Backup.S3 == nil {
			return nil, errors.New("S3 backup not configured")
		}
		if p.config.Backup.S3.Bucket == "" {
			return nil, errors.New("S3 bucket not configured")
		}
		if p.config.Backup.S3.Region == "" {
			return nil, errors.New("S3 region not configured")
		}

		return &clustermetadatapb.BackupLocation{
			Location: &clustermetadatapb.BackupLocation_S3{
				S3: &clustermetadatapb.S3Backup{
					Bucket:            p.config.Backup.S3.Bucket,
					Region:            p.config.Backup.S3.Region,
					Endpoint:          p.config.Backup.S3.Endpoint,
					KeyPrefix:         p.config.Backup.S3.KeyPrefix,
					UseEnvCredentials: p.config.Backup.S3.UseEnvCredentials,
				},
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown backup type: %s", p.config.Backup.Type)
	}
}

// generatePgBackRestCertsOnce generates pgBackRest certificates once for all cells
func (p *localProvisioner) generatePgBackRestCertsOnce(ctx context.Context) error {
	fmt.Println("=== Generating pgBackRest certs ===")
	certDir := p.certDir()
	certPaths, err := GeneratePgBackRestCerts(certDir)
	if err != nil {
		return fmt.Errorf("failed to generate pgBackRest certs: %w", err)
	}
	// Store the cert paths for later use
	p.pgBackRestCertPaths = certPaths
	fmt.Println("")
	return nil
}

// setupDefaultCell initializes the topology cell configuration for a database
func (p *localProvisioner) setupDefaultCell(ctx context.Context, cellName, etcdAddress string) error {
	fmt.Println("=== Configuring cell ===")
	fmt.Printf("⚙️  - Configuring cell: %s\n", cellName)
	fmt.Printf("⚙️  - Using etcd at: %s\n", etcdAddress)

	// Get topology configuration
	topoConfig := p.config.Topology

	// Create topology store using configured backend
	ts, err := topoclient.OpenServer(topoclient.DefaultTopoImplementation, topoConfig.GlobalRootPath, []string{etcdAddress}, topoclient.NewDefaultTopoConfig())
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
	if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
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
		return nil, errors.New("provisioner config not set")
	}

	return &p.config.Topology, nil
}

// getAllCells returns all configured cells
func (p *localProvisioner) getAllCells() ([]CellConfig, error) {
	if p.config == nil {
		return nil, errors.New("provisioner config not set")
	}

	if len(p.config.Topology.Cells) == 0 {
		return nil, errors.New("no cells configured")
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

// getCellByName returns the cell configuration for a specific cell name
func (p *localProvisioner) getCellByName(cellName string) (*CellConfig, error) {
	if p.config == nil {
		return nil, errors.New("provisioner config not set")
	}

	if len(p.config.Topology.Cells) == 0 {
		return nil, errors.New("no cells configured")
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

	// Validate required topology fields
	if typedConfig.Topology.GlobalRootPath == "" {
		return errors.New("topology global-root-path is required")
	}
	if len(typedConfig.Topology.Cells) == 0 {
		return errors.New("topology must have at least one cell configured")
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

	// Validate Unix socket path length limits
	if err := p.validateUnixSocketPathLength(typedConfig); err != nil {
		return err
	}

	return nil
}

// UnixPathMax returns the maximum Unix socket path length for the current platform.
func UnixPathMax() int {
	var addr syscall.RawSockaddrUnix
	return len(addr.Path)
}

// validateUnixSocketPathLength validates that Unix socket paths won't exceed system limits
func (p *localProvisioner) validateUnixSocketPathLength(config *LocalProvisionerConfig) error {
	maxSocketPathLength := UnixPathMax()

	// Convert root working dir to absolute path for accurate length calculation
	absRootWorkingDir, err := filepath.Abs(config.RootWorkingDir)
	if err != nil {
		return fmt.Errorf("failed to convert root working dir to absolute path: %w", err)
	}

	// Calculate the maximum possible path length for Unix sockets
	// Path structure: <rootWorkingDir>/data/pooler_<serviceID>/pg_sockets/.s.PGSQL.5432
	// We use a worst-case service ID length (8 chars) to be safe
	maxServiceIDLength := 8
	worstCasePoolerSocketPath := []string{
		"data",
		"pooler_" + strings.Repeat("x", maxServiceIDLength),
		"pg_sockets",
		".s.PGSQL.5432",
	}
	worstCaseCurrentSocketPath := filepath.Join(append([]string{absRootWorkingDir}, worstCasePoolerSocketPath...)...)

	worstCaseProposedSocketPath := filepath.Join(append([]string{"/tmp/mt"}, worstCasePoolerSocketPath...)...)

	if len(worstCaseCurrentSocketPath) > maxSocketPathLength {
		return fmt.Errorf("unix socket path would exceed system limit (%d bytes): %s\n\n"+
			"To fix this issue:\n"+
			"1. Initialize multigres from a directory with a shorter path\n"+
			"2. Provide config-path to multigres (--config-path target_dir) that has a shorter length\n\n"+
			"Example:\n"+
			"  Current: multigres cluster init --config-path %s\n"+
			"  Better:  multigres cluster init --config-path /tmp/mt/\n\n"+
			"This will generate socket paths like:\n"+
			"  %s (%d bytes)\n\n"+
			"Current path length: %d bytes (limit: %d bytes)",
			maxSocketPathLength, worstCaseCurrentSocketPath, config.RootWorkingDir, worstCaseProposedSocketPath, len(worstCaseProposedSocketPath), len(worstCaseCurrentSocketPath), maxSocketPathLength)
	}

	return nil
}

// validateBinaryPaths validates that all configured binary paths exist and are executable
func (p *localProvisioner) validateBinaryPaths(config *LocalProvisionerConfig) error {
	var errors []string

	// Validate global service binaries
	if config.Multiadmin.Path != "" {
		if err := p.validateBinaryExists(config.Multiadmin.Path, "multiadmin"); err != nil {
			errors = append(errors, err.Error())
		}
	}

	// Validate cell service binaries
	for cellName, cellConfig := range config.Cells {
		// Validate multigateway
		if cellConfig.Multigateway.Path != "" {
			if err := p.validateBinaryExists(cellConfig.Multigateway.Path, fmt.Sprintf("multigateway (cell %s)", cellName)); err != nil {
				errors = append(errors, err.Error())
			}
		}

		// Validate multipooler
		if cellConfig.Multipooler.Path != "" {
			if err := p.validateBinaryExists(cellConfig.Multipooler.Path, fmt.Sprintf("multipooler (cell %s)", cellName)); err != nil {
				errors = append(errors, err.Error())
			}
		}

		// Validate multiorch
		if cellConfig.Multiorch.Path != "" {
			if err := p.validateBinaryExists(cellConfig.Multiorch.Path, fmt.Sprintf("multiorch (cell %s)", cellName)); err != nil {
				errors = append(errors, err.Error())
			}
		}

		// Validate pgctld
		if cellConfig.Pgctld.Path != "" {
			if err := p.validateBinaryExists(cellConfig.Pgctld.Path, fmt.Sprintf("pgctld (cell %s)", cellName)); err != nil {
				errors = append(errors, err.Error())
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("binary validation failed:\n%s", strings.Join(errors, "\n"))
	}

	return nil
}

// validateBinaryExists checks if a binary path exists and is executable
func (p *localProvisioner) validateBinaryExists(binaryPath, serviceName string) error {
	// Use exec.LookPath to find and validate the binary
	_, err := exec.LookPath(binaryPath)
	if err != nil {
		return fmt.Errorf("  %s binary not found: %s: %w", serviceName, binaryPath, err)
	}

	return nil
}

// validateSystemBinaries validates that required system binaries are available in PATH
func (p *localProvisioner) validateSystemBinaries() error {
	requiredBinaries := []string{
		"etcd",
		"pg_ctl",
		"postgres",
		"pg_isready",
	}

	var missingBinaries []string

	for _, binary := range requiredBinaries {
		if _, err := exec.LookPath(binary); err != nil {
			missingBinaries = append(missingBinaries, binary)
		}
	}

	if len(missingBinaries) > 0 {
		return fmt.Errorf("required system binaries not found in PATH: %s\n\n"+
			"Please ensure PostgreSQL and etcd are installed and available in your PATH.\n"+
			"For PostgreSQL: Install PostgreSQL client tools (pg_ctl, postgres, pg_isready)\n"+
			"For etcd: Install etcd client binary",
			strings.Join(missingBinaries, ", "))
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
		slog.Error("local provisioner failed to get executable path", "error", err)
		panic(fmt.Sprintf("local provisioner failed to get executable path: %v", err))
	}
}
