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
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/etcdtopo"
	"github.com/multigres/multigres/go/provisioner/local/pgbackrest"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// nodeInstance represents a multipooler node for bootstrap testing
type nodeInstance struct {
	name           string
	cell           string
	grpcPort       int
	pgPort         int
	pgctldGrpcPort int
	dataDir        string
	pgctldProcess  *exec.Cmd
	multipoolerCmd *exec.Cmd
}

// testEnvConfig holds configuration options for test environment setup
type testEnvConfig struct {
	tempDirPrefix    string
	cellName         string
	database         string
	shardID          string
	tableGroup       string
	durabilityPolicy string
	stanzaName       string
}

// testEnv manages the test environment including etcd, topology server, and nodes
type testEnv struct {
	t              *testing.T
	config         testEnvConfig
	tempDir        string
	etcdClientAddr string
	ts             topoclient.Store
	backupLocation string
	nodes          []*nodeInstance
	multiOrchCmd   *exec.Cmd
}

// setupMultiOrchTestEnv creates a test environment with etcd and topology server
// for multiorch integration tests.
func setupMultiOrchTestEnv(t *testing.T, config testEnvConfig) *testEnv {
	t.Helper()

	ctx := t.Context()

	// Setup test directory - use /tmp to avoid long paths that exceed Unix socket limits (103 bytes on macOS)
	tempDir, err := os.MkdirTemp("/tmp", config.tempDirPrefix)
	require.NoError(t, err, "Failed to create temp directory")
	t.Logf("Test directory: %s", tempDir)

	// Use t.Cleanup to ensure directory cleanup happens even on test failure
	t.Cleanup(func() {
		if os.Getenv("KEEP_TEMP_DIRS") != "" {
			t.Logf("Keeping test directory for debugging: %s", tempDir)
			return
		}
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("Warning: failed to remove temp directory %s: %v", tempDir, err)
		} else {
			t.Logf("Cleaned up test directory: %s", tempDir)
		}
	})

	// Start etcd
	etcdDataDir := filepath.Join(tempDir, "etcd_data")
	require.NoError(t, os.MkdirAll(etcdDataDir, 0o755))

	etcdClientAddr, _ := etcdtopo.StartEtcdWithOptions(t, etcdtopo.EtcdOptions{
		ClientPort: utils.GetFreePort(t),
		PeerPort:   utils.GetFreePort(t),
		DataDir:    etcdDataDir,
	})
	t.Logf("Started etcd at %s", etcdClientAddr)

	// Create topology server and cell
	testRoot := "/multigres"
	globalRoot := filepath.Join(testRoot, "global")
	cellRoot := filepath.Join(testRoot, config.cellName)

	ts, err := topoclient.OpenServer("etcd2", globalRoot, []string{etcdClientAddr}, topoclient.NewDefaultTopoConfig())
	require.NoError(t, err, "Failed to open topology server")
	t.Cleanup(func() { ts.Close() })

	err = ts.CreateCell(ctx, config.cellName, &clustermetadatapb.Cell{
		ServerAddresses: []string{etcdClientAddr},
		Root:            cellRoot,
	})
	require.NoError(t, err, "Failed to create cell")

	// Create database
	backupLocation := filepath.Join(tempDir, "pgbackrest-repo")
	err = ts.CreateDatabase(ctx, config.database, &clustermetadatapb.Database{
		Name:             config.database,
		BackupLocation:   backupLocation,
		DurabilityPolicy: config.durabilityPolicy,
	})
	require.NoError(t, err, "Failed to create database in topology")
	t.Logf("Created database '%s' with policy '%s' and backup_location=%s",
		config.database, config.durabilityPolicy, backupLocation)

	return &testEnv{
		t:              t,
		config:         config,
		tempDir:        tempDir,
		etcdClientAddr: etcdClientAddr,
		ts:             ts,
		backupLocation: backupLocation,
		nodes:          make([]*nodeInstance, 0),
	}
}

// createNodes creates the specified number of empty nodes
func (env *testEnv) createNodes(count int) []*nodeInstance {
	env.t.Helper()

	nodes := make([]*nodeInstance, count)
	for i := range count {
		node := createEmptyNode(env.t, env.tempDir, env.config.cellName,
			env.config.shardID, env.config.database, i, env.etcdClientAddr, env.config.stanzaName)
		nodes[i] = node
		env.t.Cleanup(func() { cleanupNode(env.t, node) })
	}
	env.nodes = nodes
	env.t.Logf("Created %d nodes with pgctld running", count)
	return nodes
}

// setupPgBackRest sets up pgbackrest configuration for all nodes in the environment
func (env *testEnv) setupPgBackRest() {
	env.t.Helper()
	setupPgBackRestForBootstrap(env.t, env.tempDir, env.nodes, env.config.stanzaName)
}

// registerNodes registers all nodes in the topology
func (env *testEnv) registerNodes() {
	env.t.Helper()
	ctx := env.t.Context()

	for _, node := range env.nodes {
		pooler := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      node.cell,
				Name:      node.name,
			},
			Hostname: "localhost",
			PortMap: map[string]int32{
				"grpc": int32(node.grpcPort),
			},
			Shard:      env.config.shardID,
			Database:   env.config.database,
			TableGroup: env.config.tableGroup,
			Type:       clustermetadatapb.PoolerType_UNKNOWN, // All start with unknown type until bootstrap determines role
		}
		err := env.ts.RegisterMultiPooler(ctx, pooler, true /* overwrite */)
		require.NoError(env.t, err, "Failed to register pooler %s in topology", node.name)
		env.t.Logf("Registered pooler %s in topology", node.name)
	}
}

// startMultiOrch starts multiorch with the configured watch targets
func (env *testEnv) startMultiOrch() *exec.Cmd {
	env.t.Helper()

	watchTarget := fmt.Sprintf("%s/%s/%s", env.config.database, env.config.tableGroup, env.config.shardID)
	cmd := startMultiOrch(env.t, env.tempDir, env.config.cellName, env.etcdClientAddr, []string{watchTarget})
	env.multiOrchCmd = cmd
	env.t.Cleanup(func() { terminateProcess(env.t, cmd, "multiorch", 5*time.Second) })
	return cmd
}

// createEmptyNode creates a new empty multipooler node with pgctld and multipooler processes
func createEmptyNode(t *testing.T, baseDir, cell, shard, database string, index int, etcdAddr, pgBackRestStanza string) *nodeInstance {
	t.Helper()

	name := fmt.Sprintf("node%d", index)
	dataDir := filepath.Join(baseDir, name)
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	// Allocate ports
	pgctldGrpcPort := utils.GetFreePort(t)
	pgPort := utils.GetFreePort(t)
	grpcPort := utils.GetFreePort(t)

	// Start pgctld server
	logFile := filepath.Join(dataDir, "pgctld.log")
	pgctldCmd := exec.Command("pgctld", "server",
		"--pooler-dir", dataDir,
		"--grpc-port", fmt.Sprintf("%d", pgctldGrpcPort),
		"--pg-port", fmt.Sprintf("%d", pgPort),
		"--log-output", logFile)

	// Set MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup
	pgctldCmd.Env = append(os.Environ(),
		"MULTIGRES_TESTDATA_DIR="+baseDir,
	)

	require.NoError(t, pgctldCmd.Start())
	t.Logf("Started pgctld for %s (pid: %d, grpc: %d, pg: %d)", name, pgctldCmd.Process.Pid, pgctldGrpcPort, pgPort)

	// Wait for pgctld to be ready
	waitForProcessReady(t, "pgctld", pgctldGrpcPort, 10*time.Second)

	// Start multipooler
	serviceID := fmt.Sprintf("%s/%s", cell, name)
	multipoolerCmd := exec.Command("multipooler",
		"--grpc-port", fmt.Sprintf("%d", grpcPort),
		"--database", database,
		"--table-group", "test", // table group is required
		"--pgctld-addr", fmt.Sprintf("localhost:%d", pgctldGrpcPort),
		"--pooler-dir", dataDir,
		"--pg-port", fmt.Sprintf("%d", pgPort),
		"--service-map", "grpc-pooler,grpc-poolermanager,grpc-consensus,grpc-backup",
		"--topo-global-server-addresses", etcdAddr,
		"--topo-global-root", "/multigres/global",
		"--topo-implementation", "etcd2",
		"--cell", cell,
		"--service-id", serviceID,
		"--pgbackrest-stanza", pgBackRestStanza,
	)
	multipoolerCmd.Dir = dataDir
	mpLogFile := filepath.Join(dataDir, "multipooler.log")
	mpLogF, err := os.Create(mpLogFile)
	require.NoError(t, err)
	multipoolerCmd.Stdout = mpLogF
	multipoolerCmd.Stderr = mpLogF

	require.NoError(t, multipoolerCmd.Start())
	t.Logf("Started multipooler for %s (pid: %d, grpc: %d)", name, multipoolerCmd.Process.Pid, grpcPort)

	// Wait for multipooler to be ready by polling its status
	waitForMultipoolerReady(t, grpcPort, 30*time.Second)
	t.Logf("Multipooler %s is ready", name)

	return &nodeInstance{
		name:           name,
		cell:           cell,
		grpcPort:       grpcPort,
		pgPort:         pgPort,
		pgctldGrpcPort: pgctldGrpcPort,
		dataDir:        dataDir,
		pgctldProcess:  pgctldCmd,
		multipoolerCmd: multipoolerCmd,
	}
}

// createMultiPoolerProto creates a MultiPooler proto for the given gRPC port
func createMultiPoolerProto(grpcPort int) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Hostname: "localhost",
		PortMap: map[string]int32{
			"grpc": int32(grpcPort),
		},
	}
}

// waitForMultipoolerReady polls the multipooler gRPC endpoint until it's ready
func waitForMultipoolerReady(t *testing.T, grpcPort int, timeout time.Duration) {
	t.Helper()

	client := rpcclient.NewMultiPoolerClient(10) // Small cache for test connections
	defer client.Close()

	pooler := createMultiPoolerProto(grpcPort)

	// Use require.Eventually to poll the RPC call with connection caching
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		_, err := client.Status(ctx, pooler, &multipoolermanagerdatapb.StatusRequest{})
		return err == nil
	}, timeout, 200*time.Millisecond, "Multipooler at port %d did not become ready", grpcPort)
}

// terminateProcess gracefully terminates a process by first sending SIGTERM,
// waiting for graceful shutdown, and only using SIGKILL if necessary.
func terminateProcess(t *testing.T, cmd *exec.Cmd, name string, timeout time.Duration) {
	t.Helper()
	if cmd == nil || cmd.Process == nil {
		return
	}

	// Try graceful shutdown with SIGTERM first
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Logf("Failed to send SIGTERM to %s: %v, forcing kill", name, err)
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return
	}

	// Wait for graceful shutdown with timeout
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-time.After(timeout):
		t.Logf("%s did not terminate gracefully within %v, forcing kill", name, timeout)
		_ = cmd.Process.Kill()
		<-done // Wait for process to actually die
	case err := <-done:
		if err != nil {
			t.Logf("%s terminated with error: %v", name, err)
		} else {
			t.Logf("%s terminated gracefully", name)
		}
	}
}

// cleanupNode stops pgctld and multipooler processes
func cleanupNode(t *testing.T, node *nodeInstance) {
	t.Helper()
	if node.multipoolerCmd != nil && node.multipoolerCmd.Process != nil {
		terminateProcess(t, node.multipoolerCmd, "multipooler", 2*time.Second)
	}
	if node.pgctldProcess != nil && node.pgctldProcess.Process != nil {
		terminateProcess(t, node.pgctldProcess, "pgctld", 2*time.Second)
	}
}

// checkInitializationStatus checks the status of a node (which includes initialization info)
func checkInitializationStatus(t *testing.T, node *nodeInstance) *multipoolermanagerdatapb.Status {
	t.Helper()

	client := rpcclient.NewMultiPoolerClient(10) // Small cache for test connections
	defer client.Close()

	pooler := createMultiPoolerProto(node.grpcPort)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Status(ctx, pooler, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)

	return resp.Status
}

// connectToPostgres establishes a connection to PostgreSQL using Unix socket
func connectToPostgres(t *testing.T, socketDir string, port int) *sql.DB {
	t.Helper()

	connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable", socketDir, port)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "Failed to open database connection")

	err = db.Ping()
	require.NoError(t, err, "Failed to ping database")

	return db
}

// verifyMultigresTablesExist checks that the multigres internal tables exist on a node
func verifyMultigresTablesExist(t *testing.T, node *nodeInstance) {
	t.Helper()

	socketDir := filepath.Join(node.dataDir, "pg_sockets")
	db := connectToPostgres(t, socketDir, node.pgPort)
	defer db.Close()

	// Check that heartbeat table exists
	var heartbeatExists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'multigres'
			AND table_name = 'heartbeat'
		)
	`).Scan(&heartbeatExists)
	require.NoError(t, err, "Should query heartbeat table existence on %s", node.name)
	assert.True(t, heartbeatExists, "Heartbeat table should exist on %s", node.name)

	// Check that durability_policy table exists
	var durabilityPolicyExists bool
	err = db.QueryRow(`
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'multigres'
			AND table_name = 'durability_policy'
		)
	`).Scan(&durabilityPolicyExists)
	require.NoError(t, err, "Should query durability_policy table existence on %s", node.name)
	assert.True(t, durabilityPolicyExists, "Durability policy table should exist on %s", node.name)
}

// waitForProcessReady waits for a process to be ready by checking its gRPC port
func waitForProcessReady(t *testing.T, name string, grpcPort int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	connectAttempts := 0
	for time.Now().Before(deadline) {
		connectAttempts++
		// Test gRPC connectivity
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", grpcPort), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			t.Logf("%s ready on gRPC port %d (after %d attempts)", name, grpcPort, connectAttempts)
			return
		}
		if connectAttempts%10 == 0 {
			t.Logf("Still waiting for %s to start (attempt %d)...", name, connectAttempts)
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("Timeout: %s failed to start listening on port %d after %d attempts", name, grpcPort, connectAttempts)
}

// setupPgBackRestForBootstrap sets up pgbackrest configuration and stanzas for all nodes
// This follows the same pattern as localProvisioner.GeneratePgBackRestConfigs and InitializePgBackRestStanzas
func setupPgBackRestForBootstrap(t *testing.T, baseDir string, nodes []*nodeInstance, sharedStanzaName string) {
	t.Helper()

	// Create shared pgbackrest directories
	repoPath := filepath.Join(baseDir, "pgbackrest-repo")
	logPath := filepath.Join(baseDir, "pgbackrest-logs")
	spoolPath := filepath.Join(baseDir, "pgbackrest-spool")

	require.NoError(t, os.MkdirAll(repoPath, 0o755), "Failed to create pgbackrest repo dir")
	require.NoError(t, os.MkdirAll(logPath, 0o755), "Failed to create pgbackrest log dir")
	require.NoError(t, os.MkdirAll(spoolPath, 0o755), "Failed to create pgbackrest spool dir")

	t.Logf("Created pgbackrest directories (repo: %s, log: %s, spool: %s)", repoPath, logPath, spoolPath)

	// Setup pgbackrest for each node (following localProvisioner pattern)
	for _, node := range nodes {
		// Build list of other nodes for multi-host configuration
		var additionalHosts []pgbackrest.PgHost
		for _, otherNode := range nodes {
			if otherNode.name != node.name {
				additionalHosts = append(additionalHosts, pgbackrest.PgHost{
					DataPath:  filepath.Join(otherNode.dataDir, "pg_data"),
					SocketDir: filepath.Join(otherNode.dataDir, "pg_sockets"),
					Port:      otherNode.pgPort,
					User:      "postgres",
					Database:  "postgres",
				})
			}
		}

		// Create pgbackrest configuration
		configPath := filepath.Join(node.dataDir, "pgbackrest.conf")
		lockPath := filepath.Join(node.dataDir, "pgbackrest-lock")
		require.NoError(t, os.MkdirAll(lockPath, 0o755), "Failed to create pgbackrest lock dir for %s", node.name)

		backupCfg := pgbackrest.Config{
			StanzaName:      sharedStanzaName,
			PgDataPath:      filepath.Join(node.dataDir, "pg_data"),
			PgPort:          node.pgPort,
			PgSocketDir:     filepath.Join(node.dataDir, "pg_sockets"),
			PgUser:          "postgres",
			PgDatabase:      "postgres",
			AdditionalHosts: additionalHosts,
			LogPath:         logPath,
			SpoolPath:       spoolPath,
			LockPath:        lockPath,
			RetentionFull:   2,
		}

		// Write the pgBackRest config file (reusing pgbackrest.WriteConfigFile from provisioner)
		require.NoError(t, pgbackrest.WriteConfigFile(configPath, backupCfg),
			"Failed to write pgbackrest config for %s", node.name)
		t.Logf("Created pgbackrest config for %s at %s (stanza: %s)", node.name, configPath, sharedStanzaName)
	}

	t.Logf("pgbackrest configuration completed for all nodes")
}

// startMultiOrch starts a multiorch process with the given configuration
func startMultiOrch(t *testing.T, baseDir, cell string, etcdAddr string, watchTargets []string) *exec.Cmd {
	t.Helper()

	orchDataDir := filepath.Join(baseDir, "multiorch")
	require.NoError(t, os.MkdirAll(orchDataDir, 0o755), "Failed to create multiorch data dir")

	grpcPort := utils.GetFreePort(t)
	httpPort := utils.GetFreePort(t)

	args := []string{
		"--cell", cell,
		"--watch-targets", strings.Join(watchTargets, ","),
		"--topo-global-server-addresses", etcdAddr,
		"--topo-global-root", "/multigres/global",
		"--topo-implementation", "etcd2",
		"--grpc-port", fmt.Sprintf("%d", grpcPort),
		"--http-port", fmt.Sprintf("%d", httpPort),
		"--bookkeeping-interval", "2s",
		"--cluster-metadata-refresh-interval", "2s",
	}

	multiOrchCmd := exec.Command("multiorch", args...)
	multiOrchCmd.Dir = orchDataDir

	logFile := filepath.Join(orchDataDir, "multiorch.log")
	logF, err := os.Create(logFile)
	require.NoError(t, err, "Failed to create multiorch log file")
	multiOrchCmd.Stdout = logF
	multiOrchCmd.Stderr = logF

	require.NoError(t, multiOrchCmd.Start(), "Failed to start multiorch")
	t.Logf("Started multiorch (pid: %d, grpc: %d, http: %d, log: %s)",
		multiOrchCmd.Process.Pid, grpcPort, httpPort, logFile)

	// Wait for multiorch to be ready
	waitForProcessReady(t, "multiorch", grpcPort, 15*time.Second)
	t.Logf("MultiOrch is ready")

	return multiOrchCmd
}

// waitForShardPrimary polls multipooler nodes until at least one is initialized as primary.
// Uses PoolerType from topology (set by ChangeType RPC) rather than postgres-level role from pg_is_in_recovery().
// This ensures the test waits until multiorch has completed the full bootstrap sequence including ChangeType.
func waitForShardPrimary(t *testing.T, nodes []*nodeInstance, timeout time.Duration) *nodeInstance {
	t.Helper()

	deadline := time.Now().Add(timeout)
	checkInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		for _, node := range nodes {
			status := checkInitializationStatus(t, node)
			// Check PoolerType (from topology) instead of Role (from pg_is_in_recovery)
			if status.IsInitialized && status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
				t.Logf("Shard bootstrapped: primary is %s (pooler_type=%s)", node.name, status.PoolerType)
				return node
			}
		}
		t.Logf("Waiting for shard bootstrap... (sleeping %v)", checkInterval)
		time.Sleep(checkInterval)
	}

	t.Fatalf("Timeout: shard did not bootstrap within %v", timeout)
	return nil
}

// waitForStandbysInitialized polls multipooler nodes until all non-primary nodes are initialized as replicas.
// This ensures multiorch has completed standby initialization before proceeding with verification.
func waitForStandbysInitialized(t *testing.T, nodes []*nodeInstance, primaryName string, expectedCount int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	checkInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		standbyCount := 0
		for _, node := range nodes {
			if node.name == primaryName {
				continue // Skip the primary
			}
			status := checkInitializationStatus(t, node)
			if status.IsInitialized && status.PoolerType == clustermetadatapb.PoolerType_REPLICA && status.PostgresRunning {
				standbyCount++
			}
		}
		if standbyCount >= expectedCount {
			t.Logf("All %d standbys initialized successfully", standbyCount)
			return
		}
		t.Logf("Waiting for standbys to initialize... (have %d/%d, sleeping %v)", standbyCount, expectedCount, checkInterval)
		time.Sleep(checkInterval)
	}

	t.Fatalf("Timeout: standbys did not initialize within %v", timeout)
}

// getPostgresPid reads the postgres PID from postmaster.pid file
func getPostgresPid(t *testing.T, node *nodeInstance) int {
	t.Helper()

	pidFile := filepath.Join(node.dataDir, "pg_data", "postmaster.pid")
	data, err := os.ReadFile(pidFile)
	require.NoError(t, err, "Failed to read postgres PID file for %s", node.name)

	lines := strings.Split(string(data), "\n")
	require.Greater(t, len(lines), 0, "PID file should have at least one line")

	pid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	require.NoError(t, err, "Failed to parse PID from postmaster.pid")

	return pid
}

// killPostgres terminates the postgres process for a node (simulates database crash)
func killPostgres(t *testing.T, node *nodeInstance) {
	t.Helper()

	pgPid := getPostgresPid(t, node)
	t.Logf("Killing postgres (PID %d) on node %s", pgPid, node.name)

	err := syscall.Kill(pgPid, syscall.SIGKILL)
	require.NoError(t, err, "Failed to kill postgres process")

	t.Logf("Postgres killed on %s - multipooler should detect failure", node.name)
}

// waitForNewPrimaryElected polls nodes until a new primary (different from oldPrimaryName) is elected.
// Uses PoolerType from topology (set by ChangeType RPC) rather than postgres-level role.
func waitForNewPrimaryElected(t *testing.T, nodes []*nodeInstance, oldPrimaryName string, timeout time.Duration) *nodeInstance {
	t.Helper()

	deadline := time.Now().Add(timeout)
	checkInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.name == oldPrimaryName {
				continue // Skip the old primary
			}
			status := checkInitializationStatus(t, node)
			// Check PoolerType (from topology) instead of Role (from pg_is_in_recovery)
			if status.IsInitialized && status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
				t.Logf("New primary elected: %s (pooler_type=%s)", node.name, status.PoolerType)
				return node
			}
		}
		t.Logf("Waiting for new primary election... (sleeping %v)", checkInterval)
		time.Sleep(checkInterval)
	}

	t.Fatalf("Timeout: new primary not elected within %v", timeout)
	return nil
}
