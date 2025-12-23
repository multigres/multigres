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

package endtoend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"

	"github.com/multigres/multigres/go/cmd/multigres/command/cluster"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/retry"
	"github.com/multigres/multigres/go/tools/stringutil"

	_ "github.com/multigres/multigres/go/common/plugins/topo"
)

// lastTestClusterTempDir tracks the temp directory of the last test cluster setup.
// Used by TestMain to dump service logs on test failure.
var lastTestClusterTempDir string

// getProjectRoot finds the project root directory by traversing up from the current file.
func getProjectRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("cannot get current file path: %w", err)
	}
	// The current file is in go/test/endtoend, so we go up three levels.
	projectRoot := filepath.Join(wd, "..", "..", "..")
	return filepath.Abs(projectRoot)
}

// zonePortConfig holds port configuration for per-zone services
// TODO: In the future, may need to support multiple instances of a service within a zone
type zonePortConfig struct {
	MultigatewayHTTPPort int
	MultigatewayGRPCPort int
	MultigatewayPGPort   int
	MultipoolerHTTPPort  int
	MultipoolerGRPCPort  int
	MultiorchHTTPPort    int
	MultiorchGRPCPort    int
	PgctldGRPCPort       int
	PgctldPGPort         int
}

// testPortConfig holds test-specific port configuration to avoid conflicts
type testPortConfig struct {
	// Global services (shared across zones)
	EtcdClientPort     int
	EtcdPeerPort       int
	MultiadminHTTPPort int
	MultiadminGRPCPort int

	// Per-zone services (one of each per zone)
	Zones []zonePortConfig
}

// getTestPortConfig returns a port configuration for tests that avoids conflicts
func getTestPortConfig(t *testing.T, numZones int) *testPortConfig {
	config := &testPortConfig{
		EtcdClientPort:     utils.GetFreePort(t),
		EtcdPeerPort:       utils.GetFreePort(t),
		MultiadminHTTPPort: utils.GetFreePort(t),
		MultiadminGRPCPort: utils.GetFreePort(t),
		Zones:              make([]zonePortConfig, numZones),
	}

	for i := range numZones {
		config.Zones[i] = zonePortConfig{
			MultigatewayHTTPPort: utils.GetFreePort(t),
			MultigatewayGRPCPort: utils.GetFreePort(t),
			MultigatewayPGPort:   utils.GetFreePort(t),
			MultipoolerHTTPPort:  utils.GetFreePort(t),
			MultipoolerGRPCPort:  utils.GetFreePort(t),
			MultiorchHTTPPort:    utils.GetFreePort(t),
			MultiorchGRPCPort:    utils.GetFreePort(t),
			PgctldGRPCPort:       utils.GetFreePort(t),
			PgctldPGPort:         utils.GetFreePort(t),
		}
	}

	return config
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
		return nil //nolint:nilerr // Cleanup should continue even if state file is missing
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
// The number of zones is determined by len(portConfig.Zones)
func createTestConfigWithPorts(tempDir string, portConfig *testPortConfig) (string, error) {
	numZones := len(portConfig.Zones)
	if numZones == 0 {
		return "", fmt.Errorf("portConfig must have at least one zone")
	}

	// Build cell configs dynamically
	cellConfigs := make([]local.CellConfig, numZones)
	for i := range numZones {
		cellConfigs[i] = local.CellConfig{
			Name:     fmt.Sprintf("zone%d", i+1),
			RootPath: fmt.Sprintf("/multigres/zone%d", i+1),
		}
	}

	localConfig := &local.LocalProvisionerConfig{
		RootWorkingDir: tempDir,
		DefaultDbName:  "postgres",
		Etcd: local.EtcdConfig{
			Version:  "3.5.9",
			DataDir:  filepath.Join(tempDir, "data", "etcd-data"),
			Port:     portConfig.EtcdClientPort,
			PeerPort: portConfig.EtcdPeerPort,
		},
		Topology: local.TopologyConfig{
			Backend:        "etcd2",
			GlobalRootPath: "/multigres/global",
			Cells:          cellConfigs,
		},
		Multiadmin: local.MultiadminConfig{
			Path:     "multiadmin",
			HttpPort: portConfig.MultiadminHTTPPort,
			GrpcPort: portConfig.MultiadminGRPCPort,
			LogLevel: "info",
		},
	}

	// Build cell services dynamically for each zone
	localConfig.Cells = make(map[string]local.CellServicesConfig)
	for i := range numZones {
		zoneName := fmt.Sprintf("zone%d", i+1)
		serviceID := stringutil.RandomString(8)
		zonePort := &portConfig.Zones[i]

		localConfig.Cells[zoneName] = local.CellServicesConfig{
			Multigateway: local.MultigatewayConfig{
				Path:     "multigateway",
				HttpPort: zonePort.MultigatewayHTTPPort,
				GrpcPort: zonePort.MultigatewayGRPCPort,
				PgPort:   zonePort.MultigatewayPGPort,
				LogLevel: "info",
			},
			Multipooler: local.MultipoolerConfig{
				Path:           "multipooler",
				Database:       "postgres",
				TableGroup:     constants.DefaultTableGroup,
				Shard:          constants.DefaultShard,
				ServiceID:      serviceID,
				PoolerDir:      local.GeneratePoolerDir(tempDir, serviceID),
				PgPort:         zonePort.PgctldPGPort, // Same as pgctld for this zone
				HttpPort:       zonePort.MultipoolerHTTPPort,
				GrpcPort:       zonePort.MultipoolerGRPCPort,
				GRPCSocketFile: filepath.Join(tempDir, "sockets", fmt.Sprintf("multipooler-%s.sock", zoneName)),
				LogLevel:       "info",
			},
			Multiorch: local.MultiorchConfig{
				Path:                           "multiorch",
				HttpPort:                       zonePort.MultiorchHTTPPort,
				GrpcPort:                       zonePort.MultiorchGRPCPort,
				LogLevel:                       "info",
				ClusterMetadataRefreshInterval: "500ms",
				PoolerHealthCheckInterval:      "500ms",
				RecoveryCycleInterval:          "500ms",
			},
			Pgctld: local.PgctldConfig{
				Path:           "pgctld",
				GrpcPort:       zonePort.PgctldGRPCPort,
				GRPCSocketFile: filepath.Join(tempDir, "sockets", fmt.Sprintf("pgctld-%s.sock", zoneName)),
				PgPort:         zonePort.PgctldPGPort,
				PgDatabase:     "postgres",
				PgUser:         "postgres",
				Timeout:        30,
				LogLevel:       "info",
				PoolerDir:      local.GeneratePoolerDir(tempDir, serviceID),
				// PgPwfile not set - provisioner will create pgpassword.txt with default "postgres" password
			},
		}
	}

	// Convert the typed config to map[string]any via YAML marshaling
	yamlData, err := yaml.Marshal(localConfig)
	if err != nil {
		return "", fmt.Errorf("failed to marshal local config to YAML: %w", err)
	}

	var configMap map[string]any
	if err := yaml.Unmarshal(yamlData, &configMap); err != nil {
		return "", fmt.Errorf("failed to unmarshal local config to map: %w", err)
	}

	// Create the full configuration
	config := &cluster.MultigresConfig{
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
	if err := os.WriteFile(configFile, yamlData, 0o644); err != nil {
		return "", fmt.Errorf("failed to write config file %s: %w", configFile, err)
	}

	return configFile, nil
}

// checkCellExistsInTopology checks if a cell exists in the topology server
func checkCellExistsInTopology(etcdAddress, globalRootPath, cellName string) error {
	// Create topology store connection
	ts, err := topoclient.OpenServer("etcd2", globalRootPath, []string{etcdAddress}, topoclient.NewDefaultTopoConfig())
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

// checkMultipoolerTopoRegistration checks if multipooler is registered with correct database, tablegroup, and shard in topology
func checkMultipoolerTopoRegistration(etcdAddress, globalRootPath, cellName, expectedDatabase, expectedTableGroup, expectedShard string) error {
	// Create topology store connection
	ts, err := topoclient.OpenServer("etcd2", globalRootPath, []string{etcdAddress}, topoclient.NewDefaultTopoConfig())
	if err != nil {
		return fmt.Errorf("failed to connect to topology server: %w", err)
	}
	defer ts.Close()

	// Get all multipooler IDs in the cell
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	multipoolerInfos, err := ts.GetMultiPoolersByCell(ctx, cellName, nil)
	if err != nil {
		return fmt.Errorf("failed to get multipoolers from topology for cell '%s': %w", cellName, err)
	}

	if len(multipoolerInfos) == 0 {
		return fmt.Errorf("no multipoolers found in cell '%s'", cellName)
	}

	// Check that at least one multipooler has the correct database, tablegroup, and shard
	for _, info := range multipoolerInfos {
		if info.Database == expectedDatabase &&
			info.TableGroup == expectedTableGroup &&
			info.Shard == expectedShard {
			// Found a multipooler with the expected registration
			return nil
		}
	}

	// If we get here, no multipooler had the expected values
	var found []string
	for _, info := range multipoolerInfos {
		found = append(found, fmt.Sprintf("{database: '%s', tablegroup: '%s', shard: '%s'}",
			info.Database, info.TableGroup, info.Shard))
	}

	return fmt.Errorf("expected to find multipooler with database='%s', tablegroup='%s', shard='%s' but found: [%s]",
		expectedDatabase, expectedTableGroup, expectedShard, strings.Join(found, ", "))
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

// checkHeartbeatsWritten checks if at least one heartbeat was written to the heartbeat table.
// This function checks immediately without waiting. Callers that need to wait for heartbeats
// should use require.Eventually with this function.
func checkHeartbeatsWritten(multipoolerAddr string) (bool, error) {
	count, err := queryHeartbeatCount(multipoolerAddr)
	if err != nil {
		return false, fmt.Errorf("failed to query heartbeat table: %w", err)
	}

	return count > 0, nil
}

// waitForMultigatewayReady waits for a multigateway to be ready to execute queries.
// This is necessary because PoolerDiscovery is async and may not have discovered
// poolers yet, and lib/pq Ping uses an empty query (";") that bypasses pooler discovery.
// With multi-cell discovery, any multigateway can find the primary in any zone,
// so we only need to wait on the first port.
func waitForMultigatewayReady(t *testing.T, ctx context.Context, pgPort int) error {
	t.Helper()

	r := retry.New(1*time.Millisecond, 500*time.Millisecond)
	for attempt, err := range r.Attempts(ctx) {
		if err != nil {
			return fmt.Errorf("timeout waiting for multigateway to be ready after %d attempts: %w", attempt, err)
		}

		connStr := fmt.Sprintf("host=localhost port=%d user=postgres dbname=postgres sslmode=disable connect_timeout=2", pgPort)
		db, err := sql.Open("postgres", connStr)
		if err != nil {
			continue
		}

		queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err = db.ExecContext(queryCtx, "SELECT 1")
		cancel()
		db.Close()

		if err == nil {
			t.Logf("Multigateway on port %d is ready to execute queries", pgPort)
			return nil
		}

		if attempt == 1 || attempt%10 == 0 {
			t.Logf("Waiting for multigateway to be ready (attempt %d)...", attempt)
		}
	}

	return fmt.Errorf("timeout waiting for multigateway to be ready")
}

// queryHeartbeatCount queries the number of heartbeats in the heartbeat table via
// the multipooler gRPC service
func queryHeartbeatCount(addr string) (int, error) {
	// Create gRPC client
	client, err := NewMultiPoolerTestClient(addr)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to multipooler gRPC at %s: %w", addr, err)
	}
	defer client.Close()

	// Execute query to count heartbeats
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := client.ExecuteQuery(ctx, "SELECT COUNT(*) FROM multigres.heartbeat", 1)
	if err != nil {
		return 0, fmt.Errorf("failed to execute heartbeat count query: %w", err)
	}

	// Parse the result
	if len(result.Rows) != 1 {
		return 0, fmt.Errorf("expected 1 row, got %d", len(result.Rows))
	}

	if len(result.Rows[0].Values) != 1 {
		return 0, fmt.Errorf("expected 1 column, got %d", len(result.Rows[0].Values))
	}

	// Convert the count value from bytes to int
	countStr := string(result.Rows[0].Values[0])
	var count int
	_, err = fmt.Sscanf(countStr, "%d", &count)
	if err != nil {
		return 0, fmt.Errorf("failed to parse count value '%s': %w", countStr, err)
	}

	return count, nil
}

// executeInitCommand runs the actual multigres binary with "cluster init" command
func executeInitCommand(t *testing.T, args []string) (string, error) {
	// Prepare the full command: "multigres cluster init <args>"
	cmdArgs := append([]string{"cluster", "init"}, args...)
	cmd := exec.Command("multigres", cmdArgs...)

	output, err := cmd.CombinedOutput()
	return string(output), err
}

func TestInitCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping InitCommandtest in short mode")
	}
	tests := []struct {
		name           string
		setupDirs      func(*testing.T) ([]string, func()) // returns config paths and cleanup
		expectError    bool
		errorContains  string
		outputContains []string
	}{
		{
			name: "basic successful init",
			setupDirs: func(t *testing.T) ([]string, func()) {
				tempDir, err := os.MkdirTemp("/tmp", "mlt")
				require.NoError(t, err)
				return []string{tempDir}, func() { os.RemoveAll(tempDir) }
			},
			expectError:    false,
			outputContains: []string{"Initializing Multigres cluster configuration", "successfully"},
		},
		{
			name: "successful init with multiple valid paths",
			setupDirs: func(t *testing.T) ([]string, func()) {
				tempDir1, err := os.MkdirTemp("/tmp", "mlt")
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
		{
			name: "init fails in long path (it will exceed Unix socket limit)",
			setupDirs: func(t *testing.T) ([]string, func()) {
				// Create a very long path that will exceed Unix socket limit
				tempDir, err := os.MkdirTemp("/tmp/", "very_long_path_that_will_exceed_unix_socket_path_length_limit_for_postgresql_sockets")
				require.NoError(t, err)
				return []string{tempDir}, func() { os.RemoveAll(tempDir) }
			},
			expectError:   true,
			errorContains: "Unix socket path would exceed system limit",
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
	if testing.Short() {
		t.Skip("skipping InitCommandConfigFileCreation test in short mode")
	}
	// Setup test directory
	tempDir, err := os.MkdirTemp("/tmp/", "multigres_init_config_test")
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

	var config cluster.MultigresConfig
	err = yaml.Unmarshal(configData, &config)
	require.NoError(t, err)

	// Verify config values
	assert.Equal(t, "local", config.Provisioner)

	// Extract topology config from provisioner config
	topoConfig, ok := config.ProvisionerConfig["topology"].(map[string]any)
	require.True(t, ok, "topology config should be present")

	assert.Equal(t, "etcd2", topoConfig["backend"])
	assert.Equal(t, "/multigres/global", topoConfig["global-root-path"])

	// Check cells structure in topology (now a slice)
	cellsRaw, ok := topoConfig["cells"]
	require.True(t, ok, "cells config should be present in topology")

	cells, ok := cellsRaw.([]any)
	require.True(t, ok, "cells config should be a slice")
	require.Len(t, cells, 3, "should have exactly 3 cells")

	// Check first cell (zone1)
	cell1, ok := cells[0].(map[string]any)
	require.True(t, ok, "first cell config should be a map")
	assert.Equal(t, "zone1", cell1["name"])
	assert.Equal(t, "/multigres/zone1", cell1["root-path"])

	// Check second cell (zone2)
	cell2, ok := cells[1].(map[string]any)
	require.True(t, ok, "second cell config should be a map")
	assert.Equal(t, "zone2", cell2["name"])
	assert.Equal(t, "/multigres/zone2", cell2["root-path"])

	// Check third cell (zone3)
	cell3, ok := cells[2].(map[string]any)
	require.True(t, ok, "third cell config should be a map")
	assert.Equal(t, "zone3", cell3["name"])
	assert.Equal(t, "/multigres/zone3", cell3["root-path"])

	// Check that cell services are configured
	cellServices, ok := config.ProvisionerConfig["cells"].(map[string]any)
	require.True(t, ok, "cell services config should be present")

	zone1Services, ok := cellServices["zone1"].(map[string]any)
	require.True(t, ok, "zone1 services should be present")

	// Verify services exist in zone1
	_, ok = zone1Services["multigateway"]
	assert.True(t, ok, "multigateway should be configured in zone1")
	_, ok = zone1Services["multipooler"]
	assert.True(t, ok, "multipooler should be configured in zone1")
	_, ok = zone1Services["multiorch"]
	assert.True(t, ok, "multiorch should be configured in zone1")

	zone2Services, ok := cellServices["zone2"].(map[string]any)
	require.True(t, ok, "zone2 services should be present")

	// Verify services exist in zone2
	_, ok = zone2Services["multigateway"]
	assert.True(t, ok, "multigateway should be configured in zone2")
	_, ok = zone2Services["multipooler"]
	assert.True(t, ok, "multipooler should be configured in zone2")
	_, ok = zone2Services["multiorch"]
	assert.True(t, ok, "multiorch should be configured in zone2")
}

func TestInitCommandConfigFileAlreadyExists(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping InitCommandConfigFileAlreadyExists test in short mode")
	}
	// Setup test directory
	tempDir, err := os.MkdirTemp("/tmp", "mlt")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create existing config file
	existingConfig := filepath.Join(tempDir, "multigres.yaml")
	err = os.WriteFile(existingConfig, []byte("existing: config"), 0o644)
	require.NoError(t, err)

	// Execute command using the actual binary
	output, err := executeInitCommand(t, []string{"--config-path", tempDir})

	// Should fail with appropriate error
	require.Error(t, err)
	errorOutput := err.Error() + "\n" + output
	assert.Contains(t, errorOutput, "config file already exists")
	assert.Contains(t, errorOutput, existingConfig)
}

// executeStartCommand runs the actual multigres binary with "cluster start" command
func executeStartCommand(t *testing.T, args []string, tempDir string) (string, error) {
	// Prepare the full command: "multigres cluster start <args>"
	cmdArgs := append([]string{"cluster", "start"}, args...)
	cmd := exec.Command("multigres", cmdArgs...)

	// Set MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup
	// LC_ALL is required to avoid "postmaster became multithreaded during startup" on macOS
	cmd.Env = append(os.Environ(),
		"MULTIGRES_TESTDATA_DIR="+tempDir,
		"LC_ALL=en_US.UTF-8",
	)

	// On macOS, PostgreSQL 17 requires proper locale settings to avoid
	// "postmaster became multithreaded during startup" errors.
	if runtime.GOOS == "darwin" {
		cmd.Env = append(cmd.Env,
			"LC_ALL=en_US.UTF-8",
			"LANG=en_US.UTF-8",
		)
	}

	output, err := cmd.CombinedOutput()
	return string(output), err
}

// executeStopCommand runs the actual multigres binary with "cluster down" command
func executeStopCommand(t *testing.T, args []string) (string, error) {
	// Prepare the full command: "multigres cluster down <args>"
	cmdArgs := append([]string{"cluster", "stop"}, args...)
	cmd := exec.Command("multigres", cmdArgs...)

	output, err := cmd.CombinedOutput()
	return string(output), err
}

// testPostgreSQLConnection tests PostgreSQL connectivity using Unix socket
func testPostgreSQLConnection(t *testing.T, tempDir string, port int, zone string) {
	t.Helper()

	t.Logf("Testing PostgreSQL connection on port %d (Zone %s)...", port, zone)

	// Find the socket directory for this port
	// The socket is at <tempDir>/data/pooler_*/pg_sockets/.s.PGSQL.<port>
	pattern := filepath.Join(tempDir, "data", "pooler_*", "pg_sockets", fmt.Sprintf(".s.PGSQL.%d", port))
	matches, err := filepath.Glob(pattern)
	require.NoError(t, err, "Failed to glob for socket file")
	require.Len(t, matches, 1, "Expected exactly one socket file matching pattern %s, got %v", pattern, matches)

	// Get the directory containing the socket
	socketDir := filepath.Dir(matches[0])
	t.Logf("Using Unix socket in directory: %s", socketDir)

	// Execute psql command to test connectivity via Unix socket (no password needed)
	cmd := exec.Command("psql", "-h", socketDir, "-p", fmt.Sprintf("%d", port), "-U", "postgres", "-d", "postgres", "-c", fmt.Sprintf("SELECT 'Zone %s PostgreSQL is working!' as status, version();", zone))

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "PostgreSQL connection failed on port %d (Zone %s): %s", port, zone, string(output))

	t.Logf("Zone %s PostgreSQL (port %d) is responding correctly", zone, port)

	// Also test TCP connection with password to validate password was set correctly
	// The default password is "postgres" (set by the local provisioner at pgpassword.txt)
	testPostgreSQLTCPConnection(t, port, zone)
}

// testPostgreSQLTCPConnection tests TCP connection with password authentication.
// This validates that the password file convention is working correctly.
func testPostgreSQLTCPConnection(t *testing.T, port int, zone string) {
	t.Helper()

	t.Logf("Testing PostgreSQL TCP connection with password on port %d (Zone %s)...", port, zone)

	// Connect via TCP using the default password "postgres" (from pgpassword.txt)
	cmd := exec.Command("psql", "-h", "127.0.0.1", "-p", fmt.Sprintf("%d", port), "-U", "postgres", "-d", "postgres", "-c", fmt.Sprintf("SELECT 'Zone %s TCP auth works!' as status;", zone))
	cmd.Env = append(os.Environ(), "PGPASSWORD=postgres")

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "PostgreSQL TCP connection with password failed on port %d (Zone %s): %s", port, zone, string(output))
	assert.Contains(t, string(output), "TCP auth works!", "Should see successful TCP connection message")

	t.Logf("Zone %s PostgreSQL TCP auth (port %d) is working correctly", zone, port)
}

func TestClusterLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster lifecycle test for short tests")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping PostgreSQL test for short tests with no postgres binaries")
		return
	}

	// Require etcd binary to be available (required for local provisioner)
	_, err := exec.LookPath("etcd")
	require.NoError(t, err, "etcd binary must be available in PATH for cluster lifecycle tests")

	t.Run("cluster init and basic connectivity test", func(t *testing.T) {
		// Setup test directory
		clusterSetup, cleanup := setupTestCluster(t)
		t.Cleanup(cleanup)
		tempDir := clusterSetup.TempDir
		configFile := clusterSetup.ConfigFile
		testPorts := clusterSetup.PortConfig
		t.Logf("Testing cluster lifecycle in directory: %s", tempDir)
		// Print the actual config file contents
		configContents, _ := os.ReadFile(configFile)
		t.Logf("Config file contents:\n%s", string(configContents))

		// Verify all services connectivity using state files
		t.Log("Verifying all services connectivity...")

		// Read all service states from the state files
		serviceStates, err := getServiceStates(tempDir)
		require.NoError(t, err, "should be able to read service states")
		require.NotEmpty(t, serviceStates, "should have at least one service running")

		// Check connectivity for each service
		expectedServices := []string{"etcd", "multiadmin", "multigateway", "multipooler", "multiorch"}
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
		var config cluster.MultigresConfig
		err = yaml.Unmarshal(configData, &config)
		require.NoError(t, err)

		// Extract topology config from provisioner config
		topoConfig, ok := config.ProvisionerConfig["topology"].(map[string]any)
		require.True(t, ok, "topology config should be present")

		// Get cell name from the new structure (now a slice)
		cellsRaw, ok := topoConfig["cells"]
		require.True(t, ok, "cells config should be present")

		cells, ok := cellsRaw.([]any)
		require.True(t, ok, "cells config should be a slice")
		require.Len(t, cells, 2, "should have exactly 2 cells")

		// Get the first cell (for backward compatibility)
		cell1, ok := cells[0].(map[string]any)
		require.True(t, ok, "first cell config should be a map")

		cellName := cell1["name"].(string)
		globalRootPath := topoConfig["global-root-path"].(string)

		// Get database from cell-specific multipooler config
		cellServices, ok := config.ProvisionerConfig["cells"].(map[string]any)
		require.True(t, ok, "cell services config should be present")

		zone1Services, ok := cellServices["zone1"].(map[string]any)
		require.True(t, ok, "zone1 services should be present")

		multipoolerConfig, ok := zone1Services["multipooler"].(map[string]any)
		require.True(t, ok, "multipooler config should be present in zone1")
		expectedDatabase, ok := multipoolerConfig["database"].(string)
		require.True(t, ok, "multipooler database config should be present")
		require.NotEmpty(t, expectedDatabase, "multipooler database should not be empty")

		t.Logf("Checking cell '%s' exists in topology at %s with root path %s",
			cellName, etcdAddress, globalRootPath)
		require.NoError(t, checkCellExistsInTopology(etcdAddress, globalRootPath, cellName),
			"cell should exist in topology after cluster start command")

		// Verify multipooler is registered with database, tablegroup, and shard in topology
		t.Log("Verifying multipooler registration in topology...")
		require.NoError(t, checkMultipoolerTopoRegistration(etcdAddress, globalRootPath, cellName, expectedDatabase, constants.DefaultTableGroup, constants.DefaultShard),
			"multipooler should be registered with correct database, tablegroup, and shard in topology")

		// Wait for multiorch to bootstrap both zones (create multigres schema and heartbeat table)
		// PostgreSQL is initialized and started as part of the bootstrap process
		multipoolerAddr := fmt.Sprintf("localhost:%d", testPorts.Zones[0].MultipoolerGRPCPort)

		// Test PostgreSQL connectivity for both zones (after bootstrap)
		t.Log("Testing PostgreSQL connectivity for both zones...")
		testPostgreSQLConnection(t, tempDir, testPorts.Zones[0].PgctldPGPort, "1")
		testPostgreSQLConnection(t, tempDir, testPorts.Zones[1].PgctldPGPort, "2")
		t.Log("Both PostgreSQL instances are working correctly!")

		// Detect which zone is primary (multiorch can elect either)
		zone1Addr := fmt.Sprintf("localhost:%d", testPorts.Zones[0].MultipoolerGRPCPort)
		zone2Addr := fmt.Sprintf("localhost:%d", testPorts.Zones[1].MultipoolerGRPCPort)
		zone1IsPrimary, err := IsPrimary(zone1Addr)
		require.NoError(t, err, "should be able to check zone1 primary status")
		t.Logf("Zone1 is primary: %v", zone1IsPrimary)

		// Test multipooler gRPC functionality via TCP
		// Run write tests on primary, read-only tests on replica
		t.Log("Testing multipooler gRPC ExecuteQuery functionality via TCP...")
		if zone1IsPrimary {
			testMultipoolerGRPC(t, zone1Addr)
			testMultipoolerGRPCReadOnly(t, zone2Addr)
		} else {
			testMultipoolerGRPC(t, zone2Addr)
			testMultipoolerGRPCReadOnly(t, zone1Addr)
		}
		t.Log("Both multipooler gRPC instances are working correctly via TCP!")

		// Test multipooler gRPC functionality via Unix socket
		t.Log("Testing multipooler gRPC ExecuteQuery functionality via Unix socket...")
		zone1Socket := "unix://" + filepath.Join(tempDir, "sockets", "multipooler-zone1.sock")
		zone2Socket := "unix://" + filepath.Join(tempDir, "sockets", "multipooler-zone2.sock")
		if zone1IsPrimary {
			testMultipoolerGRPC(t, zone1Socket)
			testMultipoolerGRPCReadOnly(t, zone2Socket)
		} else {
			testMultipoolerGRPC(t, zone2Socket)
			testMultipoolerGRPCReadOnly(t, zone1Socket)
		}
		t.Log("Both multipooler gRPC instances are working correctly via Unix socket!")

		// Test pgctld gRPC functionality via Unix socket
		t.Log("Testing pgctld gRPC Status functionality via Unix socket...")
		testPgctldGRPC(t, "unix://"+filepath.Join(tempDir, "sockets", "pgctld-zone1.sock"))
		testPgctldGRPC(t, "unix://"+filepath.Join(tempDir, "sockets", "pgctld-zone2.sock"))
		t.Log("Both pgctld gRPC instances are working correctly via Unix socket!")

		// Start cluster is idempotent
		t.Log("Attempting to start running cluster...")
		upOutput, err := executeStartCommand(t, []string{"--config-path", tempDir}, tempDir)
		require.NoError(t, err, "Start command failed with output: %s", upOutput)
		assert.Contains(t, upOutput, "Multigres — Distributed Postgres made easy")
		assert.Contains(t, upOutput, "is already running")

		// Wait for heartbeats to be written
		t.Log("Waiting for heartbeats...")
		require.Eventually(t, func() bool {
			written, err := checkHeartbeatsWritten(multipoolerAddr)
			return err == nil && written
		}, 10*time.Second, 500*time.Millisecond, "heartbeats should be written after bootstrap")
		t.Log("Heartbeats detected")

		// Stop cluster (down)
		t.Log("Stopping cluster...")
		downOutput, err := executeStopCommand(t, []string{"--config-path", tempDir})
		require.NoError(t, err, "Stop command failed with output: %s", downOutput)
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

		// Start cluster again and verify state is preserved after restart
		t.Log("Starting cluster again to verify state is preserved after restart...")
		_, err = executeStartCommand(t, []string{"--config-path", tempDir}, tempDir)
		require.NoError(t, err, "Second start should succeed")

		// Wait for both zones to be ready after restart (same as initial bootstrap)
		t.Log("Waiting for zone1 to be ready after restart...")
		require.NoError(t, WaitForBootstrap(t, multipoolerAddr, 60*time.Second, tempDir, expectedDatabase),
			"zone1 should be ready after restart")
		t.Log("Waiting for zone2 to be ready after restart...")
		multipoolerAddr2 := fmt.Sprintf("localhost:%d", testPorts.Zones[1].MultipoolerGRPCPort)
		require.NoError(t, WaitForBootstrap(t, multipoolerAddr2, 60*time.Second, tempDir, expectedDatabase),
			"zone2 should be ready after restart")

		// Verify PostgreSQL connectivity for both zones after restart
		t.Log("Testing PostgreSQL connectivity for both zones after restart...")
		testPostgreSQLConnection(t, tempDir, testPorts.Zones[0].PgctldPGPort, "1")
		testPostgreSQLConnection(t, tempDir, testPorts.Zones[1].PgctldPGPort, "2")
		t.Log("Both PostgreSQL instances are working correctly after restart!")

		// Verify primary/replica roles are preserved after restart
		t.Log("Verifying primary/replica roles are preserved after restart...")
		zone1IsPrimaryAfterRestart, err := IsPrimary(zone1Addr)
		require.NoError(t, err, "should be able to check zone1 primary status after restart")
		require.Equal(t, zone1IsPrimary, zone1IsPrimaryAfterRestart,
			"primary/replica roles must be preserved after restart")
		t.Logf("Zone1 is primary after restart: %v (preserved from before)", zone1IsPrimaryAfterRestart)

		// Verify multipooler registration is preserved after restart
		// This tests that immutable fields (database, tablegroup, shard) are not overwritten
		t.Log("Verifying multipooler registration is preserved after restart...")
		require.NoError(t, checkMultipoolerTopoRegistration(etcdAddress, globalRootPath, cellName, expectedDatabase, constants.DefaultTableGroup, constants.DefaultShard),
			"multipooler registration (database, tablegroup, shard) should be preserved after restart")

		// Test write/read operations work correctly after restart
		t.Log("Testing write/read operations after restart...")
		if zone1IsPrimary {
			testMultipoolerGRPC(t, zone1Addr)
			testMultipoolerGRPCReadOnly(t, zone2Addr)
		} else {
			testMultipoolerGRPC(t, zone2Addr)
			testMultipoolerGRPCReadOnly(t, zone1Addr)
		}
		t.Log("Write/read operations work correctly after restart!")

		// Stop cluster to test immutable field validation
		t.Log("Stopping cluster to test immutable field validation...")
		downOutput, err = executeStopCommand(t, []string{"--config-path", tempDir})
		require.NoError(t, err, "Stop command failed: %s", downOutput)

		// Read the original config for restoration
		originalConfig, err := os.ReadFile(configFile)
		require.NoError(t, err)

		// Test 1: Changing database should fail
		t.Log("Testing that changing database fails...")
		modifiedConfig := strings.ReplaceAll(string(originalConfig), "database: postgres", "database: different_db")
		require.NotEqual(t, string(originalConfig), modifiedConfig, "config should have been modified for database test")
		err = os.WriteFile(configFile, []byte(modifiedConfig), 0o644)
		require.NoError(t, err)

		upOutput, err = executeStartCommand(t, []string{"--config-path", tempDir}, tempDir)
		require.Error(t, err, "Start should fail when multipooler database is changed. Output: %s", upOutput)
		combined := strings.ToLower(err.Error() + "\n" + upOutput)
		assert.Contains(t, combined, "database mismatch", "error should mention database mismatch")
		assert.Contains(t, combined, "immutable", "error should mention immutability")
		t.Log("Correctly rejected start with changed database")

		// Restore original config for clean stop
		// Note: We only test database changes here. Tablegroup and shard changes
		// are blocked by MVP validation before the immutable field check runs.
		// The immutable field validation for those fields is still in place.
		err = os.WriteFile(configFile, originalConfig, 0o644)
		require.NoError(t, err)

		// Start cluster again for clean stop test
		t.Log("Starting cluster again for clean stop test...")
		_, err = executeStartCommand(t, []string{"--config-path", tempDir}, tempDir)
		require.NoError(t, err, "Start should succeed with original config")

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

		// After clean stop, only config file should remain (no directories)
		entries, err := os.ReadDir(tempDir)
		require.NoError(t, err)

		var remainingDirs []string
		var remainingFiles []string
		for _, entry := range entries {
			if entry.IsDir() {
				remainingDirs = append(remainingDirs, entry.Name())
			} else {
				remainingFiles = append(remainingFiles, entry.Name())
			}
		}

		t.Logf("Remaining directories after clean: %v", remainingDirs)
		t.Logf("Remaining files after clean: %v", remainingFiles)

		assert.Empty(t, remainingDirs, "No directories should remain after clean stop")

		t.Log("Cluster lifecycle test completed successfully")
	})

	t.Run("multipooler requires database flag", func(t *testing.T) {
		// This test verifies that multipooler binary requires --database flag
		// We'll test this by trying to run the provisioned multipooler directly
		// without the --database flag and expecting it to fail

		tempDir, err := os.MkdirTemp("/tmp", "mlt")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		projectRoot, err := getProjectRoot()
		require.NoError(t, err)
		require.NotEmpty(t, projectRoot, "projectRoot should not be empty")

		// Try to run multipooler without --database flag (should fail)
		t.Log("Testing multipooler without --database flag (should fail)...")
		cmd := exec.Command("multipooler",
			"--topo-global-server-addresses", "fake-address",
			"--topo-global-root", "fake-root",
			"--topo-implementation", "etcd2",
		)
		output, err := cmd.CombinedOutput()

		// Should fail with database flag required error
		require.Error(t, err, "multipooler should fail when --database flag is missing")
		outputStr := string(output)
		assert.Contains(t, outputStr, "database is required",
			"Error message should mention database is required. Got: %s", outputStr)

		// Try to run multipooler with --database flag (should succeed with setup)
		t.Log("Testing multipooler with --database flag (should not show database error)...")
		cmd = exec.Command("multipooler", "--cell", "testcell", "--database", "testdb", "--help")
		output, err = cmd.CombinedOutput()
		require.NoError(t, err)

		// Should not fail due to database flag (may fail for other reasons like missing topo)
		outputStr = string(output)
		assert.NotContains(t, outputStr, "--database flag is required",
			"Should not show database flag error when flag is provided. Got: %s", outputStr)
	})

	// Verifies that if a required service port is already in use by another process,
	// cluster start fails with a helpful error mentioning the conflict.
	t.Run("cluster start fails when a service port is already in use", func(t *testing.T) {
		// Setup test directory
		tempDir, err := os.MkdirTemp("/tmp", "mlt")
		require.NoError(t, err)

		// Always cleanup processes, even if test fails
		defer func() {
			if cleanupErr := cleanupTestProcesses(tempDir); cleanupErr != nil {
				t.Logf("Warning: cleanup failed: %v", cleanupErr)
			}
			os.RemoveAll(tempDir)
		}()

		// Setup test ports (only need 1 zone for port conflict test)
		testPorts := getTestPortConfig(t, 1)

		// Intentionally occupy the multipooler gRPC port to create a conflict
		conflictPort := testPorts.Zones[0].MultipoolerGRPCPort
		ln, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", conflictPort))
		require.NoError(t, err, "failed to bind conflict port %d", conflictPort)
		defer ln.Close()

		// Create cluster configuration with these ports
		t.Log("Creating cluster configuration with conflicting port...")
		configFile, err := createTestConfigWithPorts(tempDir, testPorts)
		require.NoError(t, err, "Failed to create test configuration")
		t.Logf("Created test configuration: %s", configFile)

		// Attempt to start cluster — should fail due to port conflict
		t.Log("Starting cluster (expected to fail due to port conflict)...")
		upOutput, err := executeStartCommand(t, []string{"--config-path", tempDir}, tempDir)
		require.Error(t, err, "Start should fail when a configured port is already in use. Output: %s", upOutput)

		combined := err.Error() + "\n" + upOutput
		assert.Contains(t, combined, "already in use", "error/output should mention port already in use. Got: %s", combined)
		assert.Contains(t, combined, fmt.Sprintf("%d", conflictPort), "error/output should mention the conflicting port. Got: %s", combined)
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

// testMultipoolerGRPC tests the multipooler gRPC ExecuteQuery functionality (writes require primary)
func testMultipoolerGRPC(t *testing.T, addr string) {
	t.Helper()

	// Connect to multipooler gRPC service
	client, err := NewMultiPoolerTestClient(addr)
	require.NoError(t, err, "Failed to connect to multipooler gRPC at %s", addr)
	defer client.Close()

	// Test basic SELECT query
	TestBasicSelect(t, client)

	// Test data types
	TestDataTypes(t, client)

	// Test a simple table lifecycle (without affecting other tests)
	// Use a simple hash of the address to create unique table names
	tableName := fmt.Sprintf("test_table_%d", stringHash(addr))
	TestCreateTable(t, client, tableName)

	// Insert some test data
	testData := []map[string]any{
		{"name": "test1", "value": 100},
		{"name": "test2", "value": 200},
	}
	TestInsertData(t, client, tableName, testData)

	// Verify the data
	TestSelectData(t, client, tableName, len(testData))

	// Clean up
	TestDropTable(t, client, tableName)

	// Test that the multigres schema exists
	TestMultigresSchemaExists(t, client)

	// Test that the heartbeat table exists with expected columns
	TestHeartbeatTableExists(t, client)

	// Test primary detection
	TestPrimaryDetection(t, client)

	t.Logf("Multipooler gRPC test completed successfully for %s", addr)
}

// testMultipoolerGRPCReadOnly tests read-only operations (for replicas)
func testMultipoolerGRPCReadOnly(t *testing.T, addr string) {
	t.Helper()

	// Connect to multipooler gRPC service
	client, err := NewMultiPoolerTestClient(addr)
	require.NoError(t, err, "Failed to connect to multipooler gRPC at %s", addr)
	defer client.Close()

	// Test basic SELECT query
	TestBasicSelect(t, client)

	// Test data types
	TestDataTypes(t, client)

	// Test that the multigres schema exists (replicated from primary)
	TestMultigresSchemaExists(t, client)

	// Test that the heartbeat table exists with expected columns
	TestHeartbeatTableExists(t, client)

	t.Logf("Multipooler gRPC read-only test completed successfully for %s", addr)
}

// testPgctldGRPC tests the pgctld gRPC Status functionality
func testPgctldGRPC(t *testing.T, addr string) {
	t.Helper()

	// Connect to pgctld gRPC service
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "Failed to connect to pgctld gRPC at %s", addr)
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)
	ctx := context.Background()

	// Test Status call to verify connectivity
	statusResp, err := client.Status(ctx, &pb.StatusRequest{})
	require.NoError(t, err, "Status call failed")
	assert.Equal(t, pb.ServerStatus_RUNNING, statusResp.GetStatus(), "PostgreSQL should be running")
	assert.NotZero(t, statusResp.GetPid(), "PID should be non-zero")

	t.Logf("Pgctld gRPC test completed successfully for %s", addr)
}

// stringHash generates a simple hash from a string for creating unique identifiers
func stringHash(s string) int {
	h := 0
	for _, c := range s {
		h = 31*h + int(c)
	}
	if h < 0 {
		h = -h
	}
	return h
}

// testClusterSetup holds the resources for a test cluster
type testClusterSetup struct {
	TempDir     string
	PortConfig  *testPortConfig
	ConfigFile  string
	Database    string
	ReadyPGPort int // Multigateway PG port that has access to the PRIMARY pooler
}

// setupTestCluster sets up a complete test cluster with all services running.
// This includes building binaries, creating configuration, starting the cluster,
// and verifying all services are up and responding. Returns a testClusterSetup
// with resources and a cleanup function that must be called when done (typically
// via t.Cleanup).
func setupTestCluster(t *testing.T) (*testClusterSetup, func()) {
	t.Helper()

	// Setup test directory
	tempDir, err := os.MkdirTemp("/tmp", "mlt")
	require.NoError(t, err)

	// Track for log dumping on test failure
	lastTestClusterTempDir = tempDir

	// Create cleanup function
	cleanup := func() {
		if cleanupErr := cleanupTestProcesses(tempDir); cleanupErr != nil {
			t.Logf("Warning: cleanup failed: %v", cleanupErr)
		}
		if os.Getenv("KEEP_TEMP_DIRS") != "" {
			t.Logf("Keeping test directory for debugging: %s", tempDir)
		} else {
			os.RemoveAll(tempDir)
		}
	}

	t.Logf("Testing cluster lifecycle in directory: %s", tempDir)

	// Setup test ports
	t.Log("Setting up test ports...")
	testPorts := getTestPortConfig(t, 2)

	t.Logf("Using test ports - etcd-client:%d, etcd-peer:%d, multiadmin-http:%d, multiadmin-grpc:%d",
		testPorts.EtcdClientPort, testPorts.EtcdPeerPort, testPorts.MultiadminHTTPPort, testPorts.MultiadminGRPCPort)
	for i, zone := range testPorts.Zones {
		t.Logf("Zone %d ports - multigateway-http:%d, multigateway-grpc:%d, multigateway-pg:%d, multipooler-http:%d, multipooler-grpc:%d, multiorch-http:%d, multiorch-grpc:%d",
			i+1, zone.MultigatewayHTTPPort, zone.MultigatewayGRPCPort, zone.MultigatewayPGPort,
			zone.MultipoolerHTTPPort, zone.MultipoolerGRPCPort, zone.MultiorchHTTPPort, zone.MultiorchGRPCPort)
	}

	// Create cluster configuration with test ports
	t.Log("Creating cluster configuration with test ports...")
	configFile, err := createTestConfigWithPorts(tempDir, testPorts)
	require.NoError(t, err, "Failed to create test configuration")
	t.Logf("Created test configuration: %s", configFile)

	// Start cluster (up)
	t.Log("Starting cluster...")
	upOutput, err := executeStartCommand(t, []string{"--config-path", tempDir}, tempDir)
	require.NoError(t, err, "Start command should succeed and start the cluster: %v", upOutput)

	// Verify we got expected output
	assert.Contains(t, upOutput, "Multigres — Distributed Postgres made easy")

	// Verify all services connectivity using state files
	t.Log("Verifying all services connectivity...")

	// Read all service states from the state files
	serviceStates, err := getServiceStates(tempDir)
	require.NoError(t, err, "should be able to read service states")
	require.NotEmpty(t, serviceStates, "should have at least one service running")

	// Check connectivity for each service
	expectedServices := []string{"etcd", "multiadmin", "multigateway", "multipooler", "multiorch"}
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

	// Wait for multiorch to bootstrap both zones (create multigres schema and heartbeat table)
	// PostgreSQL is initialized and started as part of the bootstrap process
	database := "postgres" // Matches DefaultDbName in createTestConfigWithPorts
	t.Log("Waiting for multiorch to bootstrap zone1...")
	multipoolerAddr := fmt.Sprintf("localhost:%d", testPorts.Zones[0].MultipoolerGRPCPort)
	require.NoError(t, WaitForBootstrap(t, multipoolerAddr, 60*time.Second, tempDir, database),
		"multiorch should bootstrap zone1 within timeout")

	t.Log("Waiting for multiorch to bootstrap zone2...")
	multipoolerAddr2 := fmt.Sprintf("localhost:%d", testPorts.Zones[1].MultipoolerGRPCPort)
	require.NoError(t, WaitForBootstrap(t, multipoolerAddr2, 60*time.Second, tempDir, database),
		"multiorch should bootstrap zone2 within timeout")

	t.Log("Test cluster setup completed successfully")

	// Find a multigateway that has access to the PRIMARY pooler.
	// We need to wait for discovery to complete.
	readyPort := testPorts.Zones[0].MultigatewayPGPort
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer waitCancel()
	err = waitForMultigatewayReady(t, waitCtx, readyPort)
	require.NoError(t, err, "multigateway should be ready after bootstrap")

	return &testClusterSetup{
		TempDir:     tempDir,
		PortConfig:  testPorts,
		ConfigFile:  configFile,
		Database:    database,
		ReadyPGPort: readyPort,
	}, cleanup
}
