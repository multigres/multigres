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
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/etcdtopo"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/multiorch/actions"
	"github.com/multigres/multigres/go/multiorch/store"
	"github.com/multigres/multigres/go/provisioner/local/pgbackrest"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	// Register topo plugins
	_ "github.com/multigres/multigres/go/common/plugins/topo"
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

func TestBootstrapInitialization(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end bootstrap test (short mode or no postgres binaries)")
	}

	// Require etcd binary (PATH already configured by TestMain in cluster_test.go)
	_, err := exec.LookPath("etcd")
	require.NoError(t, err, "etcd binary must be available in PATH")

	ctx := t.Context()

	// Setup test directory - use /tmp to avoid long paths that exceed Unix socket limits (103 bytes on macOS)
	tempDir, err := os.MkdirTemp("/tmp", "btst*")
	require.NoError(t, err, "Failed to create temp directory")
	t.Logf("Bootstrap test directory: %s", tempDir)

	// Use t.Cleanup to ensure directory cleanup happens even on test failure.
	// This prevents orphaned processes from previous test runs interfering with new tests.
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

	// Start etcd using shared helper
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
	cellName := "test-cell"
	cellRoot := filepath.Join(testRoot, cellName)

	ts, err := topo.OpenServer("etcd2", globalRoot, []string{etcdClientAddr})
	require.NoError(t, err, "Failed to open topology server")
	defer ts.Close()

	err = ts.CreateCell(ctx, cellName, &clustermetadatapb.Cell{
		ServerAddresses: []string{etcdClientAddr},
		Root:            cellRoot,
	})
	require.NoError(t, err, "Failed to create cell")

	// Use postgres database (multigres always uses postgres database with table_group for isolation)
	database := "postgres"
	backupLocation := filepath.Join(tempDir, "pgbackrest-repo")
	err = ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
		Name:             database,
		BackupLocation:   backupLocation,
		DurabilityPolicy: "ANY_2",
	})
	require.NoError(t, err, "Failed to create database in topology")

	t.Logf("Created database '%s' with policy 'ANY_2' and backup_location=%s", database, backupLocation)

	// Create 3 empty nodes
	shardID := "test-shard-01"
	// Use a shared stanza name for all nodes
	pgBackRestStanza := "bootstrap-test"

	nodes := make([]*nodeInstance, 3)
	for i := range 3 {
		nodes[i] = createEmptyNode(t, tempDir, cellName, shardID, database, i, etcdClientAddr, pgBackRestStanza)
		defer cleanupNode(t, nodes[i])
	}

	t.Logf("Created 3 empty nodes")

	// Verify all nodes are uninitialized
	for i, node := range nodes {
		status := checkInitializationStatus(t, node)
		require.False(t, status.IsInitialized, "Node %d should be uninitialized", i)
		t.Logf("Node %d (%s) confirmed uninitialized", i, node.name)
	}

	// Setup pgbackrest configuration for all nodes before bootstrap
	setupPgBackRestForBootstrap(t, tempDir, nodes, pgBackRestStanza)

	// Create coordinator nodes for bootstrap action
	rpcClient := rpcclient.NewClient(10) // connection pool capacity
	coordNodes := make([]*store.PoolerHealth, 3)
	for i, node := range nodes {
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
			Shard:    shardID,
			Database: database,
		}
		coordNodes[i] = store.NewPoolerHealthFromMultiPooler(pooler)
	}

	// Execute bootstrap action
	t.Logf("Executing bootstrap action...")
	logger := slog.Default()
	bootstrapAction := actions.NewBootstrapShardAction(rpcClient, ts, logger)

	err = bootstrapAction.Execute(ctx, shardID, database, coordNodes)
	require.NoError(t, err, "Bootstrap action should succeed")

	t.Logf("Bootstrap action completed successfully")

	// Wait for initialization to complete
	time.Sleep(2 * time.Second)

	// Verify bootstrap results
	t.Run("verify primary initialized", func(t *testing.T) {
		// Check at least one node is initialized as primary
		var primaryNode *nodeInstance
		for _, node := range nodes {
			status := checkInitializationStatus(t, node)
			if status.IsInitialized && status.Role == "primary" {
				primaryNode = node
				break
			}
		}
		require.NotNil(t, primaryNode, "Should have one primary node")
		t.Logf("Primary node: %s", primaryNode.name)

		// Connect to primary and verify durability policy
		socketDir := filepath.Join(primaryNode.dataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, primaryNode.pgPort)
		defer db.Close()

		// Verify multigres schema exists
		var schemaExists bool
		err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'multigres')").Scan(&schemaExists)
		require.NoError(t, err)
		assert.True(t, schemaExists, "multigres schema should exist")

		// Verify durability_policy table exists
		var tableExists bool
		err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = 'multigres' AND tablename = 'durability_policy')").Scan(&tableExists)
		require.NoError(t, err)
		assert.True(t, tableExists, "durability_policy table should exist")

		// Query the durability policy
		var policyName string
		var policyVersion int64
		var quorumRuleJSON string
		var isActive bool
		err = db.QueryRow(`
			SELECT policy_name, policy_version, quorum_rule::text, is_active
			FROM multigres.durability_policy
			WHERE policy_name = $1
		`, "ANY_2").Scan(&policyName, &policyVersion, &quorumRuleJSON, &isActive)
		require.NoError(t, err, "Should find ANY_2 policy")

		// Verify policy fields
		assert.Equal(t, "ANY_2", policyName)
		assert.Equal(t, int64(1), policyVersion)
		assert.True(t, isActive)

		// Parse and verify JSONB structure
		var quorumRule map[string]any
		err = json.Unmarshal([]byte(quorumRuleJSON), &quorumRule)
		require.NoError(t, err, "Should parse quorum_rule JSON")

		// Verify QuorumType (protojson uses camelCase field names)
		quorumType, ok := quorumRule["quorumType"].(float64)
		require.True(t, ok, "quorumType should be a number")
		assert.Equal(t, float64(clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N), quorumType)

		// Verify RequiredCount
		requiredCount, ok := quorumRule["requiredCount"].(float64)
		require.True(t, ok, "requiredCount should be a number")
		assert.Equal(t, float64(2), requiredCount)

		// Verify Description
		description, ok := quorumRule["description"].(string)
		require.True(t, ok, "description should be a string")
		assert.Equal(t, "Any 2 nodes must acknowledge", description)

		t.Logf("Verified durability policy in database:")
		t.Logf("  policy_name: %s", policyName)
		t.Logf("  policy_version: %d", policyVersion)
		t.Logf("  quorum_type: ANY_N (%d)", int(quorumType))
		t.Logf("  required_count: %d", int(requiredCount))
		t.Logf("  is_active: %t", isActive)
	})
	t.Run("verify standbys initialized", func(t *testing.T) {
		// Count standbys
		standbyCount := 0
		for _, node := range nodes {
			status := checkInitializationStatus(t, node)
			if status.IsInitialized && status.Role == "standby" {
				standbyCount++
				t.Logf("Standby node: %s", node.name)
			}
		}
		// Should have at least 1 standby (might have issues with some)
		assert.GreaterOrEqual(t, standbyCount, 1, "Should have at least one standby")
	})

	t.Run("verify multigres internal tables exist", func(t *testing.T) {
		// Verify tables exist on all initialized nodes (both primary and standbys)
		for _, node := range nodes {
			status := checkInitializationStatus(t, node)
			if status.IsInitialized {
				verifyMultigresTablesExist(t, node)
				t.Logf("Verified multigres tables exist on %s (%s)", node.name, status.Role)
			}
		}
	})

	t.Run("verify consensus term", func(t *testing.T) {
		// All initialized nodes should have consensus term = 1
		for _, node := range nodes {
			status := checkInitializationStatus(t, node)
			if status.IsInitialized {
				assert.Equal(t, int64(1), status.ConsensusTerm, "Node %s should have consensus term 1", node.name)
			}
		}
	})
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

		_, err := client.InitializationStatus(ctx, pooler, &multipoolermanagerdatapb.InitializationStatusRequest{})
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

// checkInitializationStatus checks the initialization status of a node
func checkInitializationStatus(t *testing.T, node *nodeInstance) *multipoolermanagerdatapb.InitializationStatusResponse {
	t.Helper()

	client := rpcclient.NewMultiPoolerClient(10) // Small cache for test connections
	defer client.Close()

	pooler := createMultiPoolerProto(node.grpcPort)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.InitializationStatus(ctx, pooler, &multipoolermanagerdatapb.InitializationStatusRequest{})
	require.NoError(t, err)

	return resp
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
