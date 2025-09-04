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

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/test/utils"

	_ "github.com/multigres/multigres/go/plugins/topo"
)

// Global variables for lazy binary building
var (
	multigresBinary string
	buildOnce       sync.Once
	buildError      error
)

// testPortConfig holds test-specific port configuration to avoid conflicts
type testPortConfig struct {
	EtcdPort             int
	MultigatewayHTTPPort int
	MultigatewayGRPCPort int
	MultipoolerGRPCPort  int
	MultiorchGRPCPort    int
}

// getTestPortConfig returns a port configuration for tests that avoids conflicts
func getTestPortConfig() *testPortConfig {
	return &testPortConfig{
		EtcdPort:             utils.GetNextEtcd2Port(),
		MultigatewayHTTPPort: utils.GetNextPort(),
		MultigatewayGRPCPort: utils.GetNextPort(),
		MultipoolerGRPCPort:  utils.GetNextPort(),
		MultiorchGRPCPort:    utils.GetNextPort(),
	}
}

// checkPortAvailable checks if a port is available for binding
func checkPortAvailable(port int) error {
	address := fmt.Sprintf("localhost:%d", port)
	conn, err := net.DialTimeout("tcp", address, 1*time.Second)
	if err != nil {
		// Port is not in use, which is good
		return nil
	}
	defer conn.Close()
	return fmt.Errorf("port %d is already in use", port)
}

// checkAllPortsAvailable ensures all test ports are available before starting
func checkAllPortsAvailable(config *testPortConfig) error {
	ports := []int{
		config.EtcdPort,
		config.MultigatewayHTTPPort,
		config.MultigatewayGRPCPort,
		config.MultipoolerGRPCPort,
		config.MultiorchGRPCPort,
	}

	for _, port := range ports {
		if err := checkPortAvailable(port); err != nil {
			return fmt.Errorf("port availability check failed: %w", err)
		}
	}
	return nil
}

// killProcessByPID kills a process by PID using kill -9
func killProcessByPID(pid int) error {
	if pid <= 0 {
		return fmt.Errorf("invalid PID: %d", pid)
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("failed to find process %d: %w", pid, err)
	}

	// Use kill -9 (SIGKILL) to forcefully terminate
	err = process.Signal(syscall.SIGKILL)
	if err != nil {
		return fmt.Errorf("failed to kill process %d: %w", pid, err)
	}

	return nil
}

// cleanupTestProcesses kills all processes that were started during the test
func cleanupTestProcesses(tempDir string) error {
	serviceStates, err := getServiceStates(tempDir)
	if err != nil {
		// If we can't read service states, that's okay - maybe nothing was started
		return nil
	}

	var errors []string
	for serviceName, state := range serviceStates {
		if state.PID > 0 {
			fmt.Printf("Cleaning up %s process (PID: %d)...\n", serviceName, state.PID)
			if err := killProcessByPID(state.PID); err != nil {
				errors = append(errors, fmt.Sprintf("failed to kill %s (PID %d): %v", serviceName, state.PID, err))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("cleanup errors: %s", strings.Join(errors, "; "))
	}

	return nil
}

// createTestConfigWithPorts creates a test configuration file with custom ports
func createTestConfigWithPorts(tempDir string, portConfig *testPortConfig) (string, error) {
	// Create a typed configuration using LocalProvisionerConfig
	binPath := filepath.Join(tempDir, "bin")
	localConfig := &local.LocalProvisionerConfig{
		RootWorkingDir: tempDir,
		DefaultDbName:  "default",
		Topology: local.TopologyConfig{
			Backend:             "etcd2",
			GlobalRootPath:      "/multigres/global",
			DefaultCellName:     "zone1",
			DefaultCellRootPath: "/multigres/zone1",
		},
		Etcd: local.EtcdConfig{
			Version: "3.5.9",
			DataDir: filepath.Join(tempDir, "data", "etcd-data"),
			Port:    portConfig.EtcdPort,
		},
		Multigateway: local.MultigatewayConfig{
			Path:     filepath.Join(binPath, "multigateway"),
			HttpPort: portConfig.MultigatewayHTTPPort,
			GrpcPort: portConfig.MultigatewayGRPCPort,
			PgPort:   15432, // Use default PG port
			LogLevel: "info",
		},
		Multipooler: local.MultipoolerConfig{
			Path:     filepath.Join(binPath, "multipooler"),
			GrpcPort: portConfig.MultipoolerGRPCPort,
			LogLevel: "info",
		},
		Multiorch: local.MultiorchConfig{
			Path:     filepath.Join(binPath, "multiorch"),
			GrpcPort: portConfig.MultiorchGRPCPort,
			LogLevel: "info",
		},
	}

	// Convert the typed config to map[string]interface{} via YAML marshaling
	yamlData, err := yaml.Marshal(localConfig)
	if err != nil {
		return "", fmt.Errorf("failed to marshal local config to YAML: %w", err)
	}

	var configMap map[string]interface{}
	if err := yaml.Unmarshal(yamlData, &configMap); err != nil {
		return "", fmt.Errorf("failed to unmarshal local config to map: %w", err)
	}

	// Create the full configuration
	config := &MultigresConfig{
		Provisioner:       "local",
		ProvisionerConfig: configMap,
	}

	// Marshal to YAML
	yamlData, err = yaml.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("failed to marshal config to YAML: %w", err)
	}

	// Write config file
	configFile := filepath.Join(tempDir, "multigres.yaml")
	if err := os.WriteFile(configFile, yamlData, 0644); err != nil {
		return "", fmt.Errorf("failed to write config file %s: %w", configFile, err)
	}

	return configFile, nil
}

// checkCellExistsInTopology checks if a cell exists in the topology server
func checkCellExistsInTopology(etcdAddress, globalRootPath, cellName string) error {
	// Create topology store connection
	ts, err := topo.OpenServer("etcd2", globalRootPath, []string{etcdAddress})
	if err != nil {
		return fmt.Errorf("failed to connect to topology server: %w", err)
	}
	defer ts.Close()

	// Try to get the cell
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cell, err := ts.GetCell(ctx, cellName)
	if err != nil {
		return fmt.Errorf("failed to get cell '%s' from topology: %w", cellName, err)
	}

	// Verify cell has expected properties
	if cell.Name != cellName {
		return fmt.Errorf("cell name mismatch: expected %s, got %s", cellName, cell.Name)
	}
	if len(cell.ServerAddresses) == 0 {
		return fmt.Errorf("cell '%s' has no server addresses", cellName)
	}
	if cell.Root == "" {
		return fmt.Errorf("cell '%s' has no root path", cellName)
	}

	return nil
}

// getServiceStates reads all service state files from the state directory
func getServiceStates(configDir string) (map[string]local.LocalProvisionedService, error) {
	stateDir := filepath.Join(configDir, "state")

	// Check if state directory exists
	if _, err := os.Stat(stateDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("state directory not found: %s", stateDir)
	}

	states := make(map[string]local.LocalProvisionedService)

	// Helper function to load services from a directory
	loadServicesFromDir := func(dir string) error {
		files, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("failed to read directory %s: %w", dir, err)
		}

		for _, file := range files {
			if !file.IsDir() && filepath.Ext(file.Name()) == ".json" {
				filePath := filepath.Join(dir, file.Name())
				data, err := os.ReadFile(filePath)
				if err != nil {
					continue // Skip files we can't read
				}

				var state local.LocalProvisionedService
				if err := json.Unmarshal(data, &state); err != nil {
					continue // Skip files we can't parse
				}

				states[state.Service] = state
			}
		}
		return nil
	}

	// Load services from the root state directory (global services like etcd)
	if err := loadServicesFromDir(stateDir); err != nil {
		return nil, err
	}

	// Load services from database directories
	dbsDir := filepath.Join(stateDir, "dbs")
	if _, err := os.Stat(dbsDir); err == nil {
		dbEntries, err := os.ReadDir(dbsDir)
		if err != nil {
			return nil, fmt.Errorf("failed to read dbs directory %s: %w", dbsDir, err)
		}

		for _, dbEntry := range dbEntries {
			if dbEntry.IsDir() {
				dbDir := filepath.Join(dbsDir, dbEntry.Name())
				if err := loadServicesFromDir(dbDir); err != nil {
					return nil, fmt.Errorf("failed to load services from database %s: %w", dbEntry.Name(), err)
				}
			}
		}
	}

	return states, nil
}

// checkServiceConnectivity checks if a service is reachable on its configured ports
func checkServiceConnectivity(service string, state local.LocalProvisionedService) error {
	for portName, port := range state.Ports {
		address := net.JoinHostPort(state.FQDN, fmt.Sprintf("%d", port))
		conn, err := net.DialTimeout("tcp", address, 5*time.Second)
		if err != nil {
			return fmt.Errorf("failed to connect to %s %s port at %s: %w", service, portName, address, err)
		}
		conn.Close()
	}
	return nil
}

// buildMultigresBinary builds the multigres binary and returns its path
func buildMultigresBinary() (string, error) {
	// Create a temporary directory for the multigres binary
	tempDir, err := os.MkdirTemp("", "multigres_binary_")
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory for multigres binary: %v", err)
	}

	// Require MTROOT environment variable
	projectRoot := os.Getenv("MTROOT")
	if projectRoot == "" {
		return "", fmt.Errorf("MTROOT environment variable must be set. Please run: source ./build.env")
	}

	// Build multigres binary
	binaryPath := filepath.Join(tempDir, "multigres")
	sourceDir := filepath.Join(projectRoot, "go/cmd", "multigres")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, sourceDir)
	buildCmd.Dir = projectRoot

	buildOutput, err := buildCmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to build multigres: %v\nOutput: %s", err, string(buildOutput))
	}

	return binaryPath, nil
}

// buildServiceBinaries builds service binaries (not multigres) in the specified directory
func buildServiceBinaries(tempDir string) error {
	// Create bin directory inside temp directory
	binDir := filepath.Join(tempDir, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		return fmt.Errorf("failed to create bin directory: %v", err)
	}

	// Require MTROOT environment variable
	projectRoot := os.Getenv("MTROOT")
	if projectRoot == "" {
		return fmt.Errorf("MTROOT environment variable must be set. Please run: source ./build.env")
	}

	// Build service binaries (excluding multigres which is built separately)
	binaries := []string{
		"multigateway",
		"multiorch",
		"multipooler",
		"pgctld",
	}

	for _, binaryName := range binaries {
		// Define binary paths in the bin directory
		binaryPath := filepath.Join(binDir, binaryName)
		sourceDir := filepath.Join(projectRoot, "go/cmd", binaryName)
		buildCmd := exec.Command("go", "build", "-o", binaryPath, sourceDir)
		buildCmd.Dir = projectRoot

		buildOutput, err := buildCmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to build %s: %v\nOutput: %s", binaryName, err, string(buildOutput))
		}
	}

	return nil
}

// ensureBinaryBuilt ensures the multigres binary is built exactly once
// It should be called at the start of each test function
func ensureBinaryBuilt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	buildOnce.Do(func() {
		var err error
		multigresBinary, err = buildMultigresBinary()
		if err != nil {
			buildError = fmt.Errorf("failed to build multigres binary: %w", err)
		}
	})

	if buildError != nil {
		t.Fatalf("Binary build failed: %v", buildError)
	}
}

// TestMain runs before all tests
func TestMain(m *testing.M) {
	// Run all tests
	exitCode := m.Run()

	// Clean up multigres binary after all tests if it was built
	if multigresBinary != "" {
		os.RemoveAll(filepath.Dir(multigresBinary))
	}

	// Exit with the test result code
	os.Exit(exitCode)
}

// executeInitCommand runs the actual multigres binary with "cluster init" command
func executeInitCommand(t *testing.T, args []string) (string, error) {
	// Prepare the full command: "multigres cluster init <args>"
	cmdArgs := append([]string{"cluster", "init"}, args...)
	cmd := exec.Command(multigresBinary, cmdArgs...)

	output, err := cmd.CombinedOutput()
	return string(output), err
}

func TestInitCommand(t *testing.T) {
	ensureBinaryBuilt(t)

	tests := []struct {
		name           string
		setupDirs      func(*testing.T) ([]string, func()) // returns config paths and cleanup
		expectError    bool
		errorContains  string
		outputContains []string
	}{
		{
			name: "successful init with current directory",
			setupDirs: func(t *testing.T) ([]string, func()) {
				tempDir, err := os.MkdirTemp("", "multigres_init_test")
				require.NoError(t, err)
				return []string{tempDir}, func() { os.RemoveAll(tempDir) }
			},
			expectError:    false,
			outputContains: []string{"Initializing Multigres cluster configuration", "successfully"},
		},
		{
			name: "error with non-existent config path",
			setupDirs: func(t *testing.T) ([]string, func()) {
				return []string{"/nonexistent/path/that/should/not/exist"}, func() {}
			},
			expectError:   true,
			errorContains: "config path does not exist",
		},
		{
			name: "error with file instead of directory",
			setupDirs: func(t *testing.T) ([]string, func()) {
				tempFile, err := os.CreateTemp("", "multigres_init_test_file")
				require.NoError(t, err)
				tempFile.Close()
				return []string{tempFile.Name()}, func() { os.Remove(tempFile.Name()) }
			},
			expectError:   true,
			errorContains: "config path is not a directory",
		},
		{
			name: "successful init with multiple valid paths",
			setupDirs: func(t *testing.T) ([]string, func()) {
				tempDir1, err := os.MkdirTemp("", "multigres_init_test1")
				require.NoError(t, err)
				tempDir2, err := os.MkdirTemp("", "multigres_init_test2")
				require.NoError(t, err)
				return []string{tempDir1, tempDir2}, func() {
					os.RemoveAll(tempDir1)
					os.RemoveAll(tempDir2)
				}
			},
			expectError:    false,
			outputContains: []string{"Initializing Multigres cluster configuration", "successfully"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test directories
			configPaths, cleanup := tt.setupDirs(t)
			defer cleanup()

			// Build command arguments
			args := []string{}
			for _, path := range configPaths {
				args = append(args, "--config-path", path)
			}

			// Execute command using the actual binary
			output, err := executeInitCommand(t, args)

			// Check results
			if tt.expectError {
				require.Error(t, err)
				// Error message should be in stderr, but exec.CombinedOutput captures both
				errorOutput := output
				if err != nil {
					errorOutput = err.Error() + "\n" + output
				}
				assert.Contains(t, strings.ToLower(errorOutput), strings.ToLower(tt.errorContains))
			} else {
				require.NoError(t, err, "Command failed with output: %s", output)
				for _, expectedOutput := range tt.outputContains {
					assert.Contains(t, output, expectedOutput)
				}
			}
		})
	}
}

func TestInitCommandConfigFileCreation(t *testing.T) {
	ensureBinaryBuilt(t)

	// Setup test directory
	tempDir, err := os.MkdirTemp("", "multigres_init_config_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Execute command using the actual binary
	output, err := executeInitCommand(t, []string{"--config-path", tempDir})

	// Command should succeed
	require.NoError(t, err, "Command failed with output: %s", output)

	// Check output contains expected messages
	assert.Contains(t, output, "Initializing Multigres cluster configuration")

	// Check config file was created
	configFile := filepath.Join(tempDir, "multigres.yaml")
	_, err = os.Stat(configFile)
	require.NoError(t, err, "Config file should exist")

	// Read and validate config content
	configData, err := os.ReadFile(configFile)
	require.NoError(t, err)

	var config MultigresConfig
	err = yaml.Unmarshal(configData, &config)
	require.NoError(t, err)

	// Verify config values
	assert.Equal(t, "local", config.Provisioner)

	// Extract topology config from provisioner config
	topoConfig, ok := config.ProvisionerConfig["topology"].(map[string]interface{})
	require.True(t, ok, "topology config should be present")

	assert.Equal(t, "etcd2", topoConfig["backend"])
	assert.Equal(t, "/multigres/global", topoConfig["global-root-path"])
	assert.Equal(t, "zone1", topoConfig["default-cell-name"])
	assert.Equal(t, "/multigres/zone1", topoConfig["default-cell-root-path"])
}

func TestInitCommandConfigFileAlreadyExists(t *testing.T) {
	ensureBinaryBuilt(t)

	// Setup test directory
	tempDir, err := os.MkdirTemp("", "multigres_init_exists_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create existing config file
	existingConfig := filepath.Join(tempDir, "multigres.yaml")
	err = os.WriteFile(existingConfig, []byte("existing: config"), 0644)
	require.NoError(t, err)

	// Execute command using the actual binary
	output, err := executeInitCommand(t, []string{"--config-path", tempDir})

	// Should fail with appropriate error
	require.Error(t, err)
	errorOutput := err.Error() + "\n" + output
	assert.Contains(t, errorOutput, "config file already exists")
	assert.Contains(t, errorOutput, existingConfig)
}

// executeStartCommand runs the actual multigres binary with "cluster up" command
func executeStartCommand(t *testing.T, args []string) (string, error) {
	// Prepare the full command: "multigres cluster up <args>"
	cmdArgs := append([]string{"cluster", "start"}, args...)
	cmd := exec.Command(multigresBinary, cmdArgs...)

	output, err := cmd.CombinedOutput()
	return string(output), err
}

// executeStopCommand runs the actual multigres binary with "cluster down" command
func executeStopCommand(t *testing.T, args []string) (string, error) {
	// Prepare the full command: "multigres cluster down <args>"
	cmdArgs := append([]string{"cluster", "stop"}, args...)
	cmd := exec.Command(multigresBinary, cmdArgs...)

	output, err := cmd.CombinedOutput()
	return string(output), err
}

func TestClusterLifecycle(t *testing.T) {
	ensureBinaryBuilt(t)

	// Require etcd binary to be available (required for local provisioner)
	_, err := exec.LookPath("etcd")
	require.NoError(t, err, "etcd binary must be available in PATH for cluster lifecycle tests")

	// Binaries are built in TestMain, no need for MTROOT environment variable

	t.Run("cluster init and basic connectivity test", func(t *testing.T) {
		// Setup test directory
		tempDir, err := os.MkdirTemp("", "multigres_lifecycle_test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Always cleanup processes, even if test fails
		defer func() {
			if cleanupErr := cleanupTestProcesses(tempDir); cleanupErr != nil {
				t.Logf("Warning: cleanup failed: %v", cleanupErr)
			}
		}()

		t.Logf("Testing cluster lifecycle in directory: %s", tempDir)

		// Build service binaries in the test directory
		t.Log("Building service binaries...")
		require.NoError(t, buildServiceBinaries(tempDir), "Failed to build service binaries")

		// Setup test ports and sanity checks
		t.Log("Setting up test ports and performing sanity checks...")
		testPorts := getTestPortConfig()
		require.NoError(t, checkAllPortsAvailable(testPorts),
			"Test ports should be available before starting cluster")

		t.Logf("Using test ports - etcd:%d, multigateway-http:%d, multigateway-grpc:%d, multipooler:%d, multiorch:%d",
			testPorts.EtcdPort, testPorts.MultigatewayHTTPPort, testPorts.MultigatewayGRPCPort,
			testPorts.MultipoolerGRPCPort, testPorts.MultiorchGRPCPort)

		// Create cluster configuration with test ports
		t.Log("Creating cluster configuration with test ports...")
		configFile, err := createTestConfigWithPorts(tempDir, testPorts)
		require.NoError(t, err, "Failed to create test configuration")
		t.Logf("Created test configuration: %s", configFile)
		// Print the actual config file contents
		configContents, _ := os.ReadFile(configFile)
		t.Logf("Config file contents:\n%s", string(configContents))

		// Start cluster (up)
		t.Log("Starting cluster...")
		upOutput, err := executeStartCommand(t, []string{"--config-path", tempDir})
		require.NoError(t, err, "Up command should succeed and start the cluster: %v", upOutput)

		// Verify we got expected output
		assert.Contains(t, upOutput, "Multigres — Distributed Postgres made easy")

		// Verify all services connectivity using state files
		t.Log("Verifying all services connectivity...")

		// Read all service states from the state files
		serviceStates, err := getServiceStates(tempDir)
		require.NoError(t, err, "should be able to read service states")
		require.NotEmpty(t, serviceStates, "should have at least one service running")

		// Check connectivity for each service
		expectedServices := []string{"etcd", "multigateway", "multipooler", "multiorch"}
		for _, serviceName := range expectedServices {
			state, exists := serviceStates[serviceName]
			require.True(t, exists, "service %s should have a state file", serviceName)

			t.Logf("Checking %s connectivity at %s with ports %v", serviceName, state.FQDN, state.Ports)
			require.NoError(t, checkServiceConnectivity(serviceName, state),
				"%s should be reachable on its configured ports", serviceName)

			// If service has a datadir defined, verify it exists
			if state.DataDir != "" {
				assert.DirExists(t, state.DataDir, "service %s datadir should exist at %s", serviceName, state.DataDir)
			}
		}

		// Verify cell exists in topology using etcd from state
		t.Log("Verifying cell exists in topology...")

		// Get etcd connection details from state
		etcdState, exists := serviceStates["etcd"]
		require.True(t, exists, "etcd service state should exist")
		etcdPort, exists := etcdState.Ports["tcp"]
		require.True(t, exists, "etcd should have tcp port defined")
		etcdAddress := fmt.Sprintf("%s:%d", etcdState.FQDN, etcdPort)

		// Read the config to get topology settings
		configData, err := os.ReadFile(configFile)
		require.NoError(t, err)
		var config MultigresConfig
		err = yaml.Unmarshal(configData, &config)
		require.NoError(t, err)

		// Extract topology config from provisioner config
		topoConfig, ok := config.ProvisionerConfig["topology"].(map[string]interface{})
		require.True(t, ok, "topology config should be present")

		cellName := topoConfig["default-cell-name"].(string)
		globalRootPath := topoConfig["global-root-path"].(string)

		t.Logf("Checking cell '%s' exists in topology at %s with root path %s",
			cellName, etcdAddress, globalRootPath)
		require.NoError(t, checkCellExistsInTopology(etcdAddress, globalRootPath, cellName),
			"cell should exist in topology after cluster up command")

		// Stop cluster (down)
		t.Log("Stopping cluster...")
		downOutput, err := executeStopCommand(t, []string{"--config-path", tempDir})
		require.NoError(t, err, "Down command failed with output: %s", downOutput)
		assert.Contains(t, downOutput, "Stopping Multigres cluster")
		assert.Contains(t, downOutput, "Multigres cluster stopped successfully")

		// Verify data directories still exist after normal stop but are empty
		t.Log("Verifying data directories exist but are empty after normal stop...")

		assert.DirExists(t, filepath.Join(tempDir, "data"))
		assert.DirExists(t, filepath.Join(tempDir, "data", "etcd-data"))
		assert.DirExists(t, filepath.Join(tempDir, "logs"))
		assert.DirExists(t, filepath.Join(tempDir, "state"))

		// Verify logs directory tree contains no files (subdirectories are ok, but they should be empty)
		assert.NoError(t, assertDirectoryTreeEmpty(filepath.Join(tempDir, "logs")),
			"logs directory tree should contain no files after normal stop")

		// Verify state directory is empty (no state files)
		assert.Empty(t, assertDirectoryTreeEmpty(filepath.Join(tempDir, "state")), "state directory should be empty after normal stop")

		// Start and stop with --clean flag
		t.Log("Testing clean stop behavior...")
		_, err = executeStartCommand(t, []string{"--config-path", tempDir})
		require.NoError(t, err, "Second start should succeed")

		// Stop with --clean flag
		downCleanOutput, err := executeStopCommand(t, []string{"--config-path", tempDir, "--clean"})
		require.NoError(t, err, "Clean stop should succeed")
		assert.Contains(t, downCleanOutput, "clean mode, all data for this local cluster will be deleted")
		assert.Contains(t, downCleanOutput, "Cleaned up data directory")
		assert.Contains(t, downCleanOutput, "Cleaned up state directory")
		assert.Contains(t, downCleanOutput, "Cleaned up logs directory")

		// Verify all data directories are completely removed
		t.Log("Verifying all data directories are removed after clean stop...")
		assert.NoFileExists(t, filepath.Join(tempDir, "data"))
		assert.NoFileExists(t, filepath.Join(tempDir, "state"))
		assert.NoFileExists(t, filepath.Join(tempDir, "logs"))

		// Only config file and bin directory should remain
		entries, err := os.ReadDir(tempDir)
		require.NoError(t, err)

		var remainingDirs []string
		for _, entry := range entries {
			if entry.IsDir() {
				remainingDirs = append(remainingDirs, entry.Name())
			}
		}
		assert.ElementsMatch(t, []string{"bin"}, remainingDirs, "Only bin directory should remain after clean")

		t.Log("Cluster lifecycle test completed successfully")
	})
}

// assertDirectoryTreeEmpty recursively checks that a directory tree contains no files,
// only empty directories. Returns an error if any files are found.
func assertDirectoryTreeEmpty(rootPath string) error {
	return filepath.WalkDir(rootPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip the root directory itself
		if path == rootPath {
			return nil
		}

		// If it's a file, that's an error - no files should exist
		if !d.IsDir() {
			return fmt.Errorf("found file in directory tree: %s", path)
		}

		// It's a directory, which is fine - continue walking
		return nil
	})
}
