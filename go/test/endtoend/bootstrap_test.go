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

// Package endtoend contains integration tests for multigres components.
//
// Bootstrap test:
//   - TestBootstrapInitialization: Verifies multiorch automatically detects and bootstraps
//     uninitialized shards without manual intervention.
package endtoend

import (
	"encoding/json"
	"fmt"
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
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	// Register topo plugins
	_ "github.com/multigres/multigres/go/common/plugins/topo"
)

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

	// Create 3 empty nodes with PostgreSQL running but uninitialized
	// This ensures multipooler can connect and return "uninitialized" status
	shardID := "test-shard-01"
	// Use a shared stanza name for all nodes
	pgBackRestStanza := "bootstrap-test"

	nodes := make([]*nodeInstance, 3)
	for i := range 3 {
		// Create node with pgctld and multipooler, but start PostgreSQL in between
		node := &nodeInstance{
			name:           fmt.Sprintf("node%d", i),
			cell:           cellName,
			grpcPort:       utils.GetFreePort(t),
			pgPort:         utils.GetFreePort(t),
			pgctldGrpcPort: utils.GetFreePort(t),
			dataDir:        filepath.Join(tempDir, fmt.Sprintf("node%d", i)),
		}
		require.NoError(t, os.MkdirAll(node.dataDir, 0o755))

		// 1. Start pgctld server
		logFile := filepath.Join(node.dataDir, "pgctld.log")
		pgctldCmd := exec.Command("pgctld", "server",
			"--pooler-dir", node.dataDir,
			"--grpc-port", fmt.Sprintf("%d", node.pgctldGrpcPort),
			"--pg-port", fmt.Sprintf("%d", node.pgPort),
			"--log-output", logFile)
		pgctldCmd.Env = append(os.Environ(), "MULTIGRES_TESTDATA_DIR="+tempDir)
		require.NoError(t, pgctldCmd.Start())
		node.pgctldProcess = pgctldCmd
		t.Logf("Started pgctld for %s (pid: %d, grpc: %d, pg: %d)", node.name, pgctldCmd.Process.Pid, node.pgctldGrpcPort, node.pgPort)

		// Wait for pgctld to be ready
		waitForProcessReady(t, "pgctld", node.pgctldGrpcPort, 10*time.Second)

		// 2. DO NOT initialize PostgreSQL data directory - let multiorch bootstrap do it
		// InitializeEmptyPrimary will call InitDataDir, configure archive_mode, and start postgres
		// This tests the proper bootstrap flow from completely empty nodes
		t.Logf("Node %s: pgctld ready (postgres data directory NOT initialized yet)", node.name)

		// 4. Start multipooler (without postgres running, it will wait for bootstrap)
		serviceID := fmt.Sprintf("%s/%s", cellName, node.name)
		multipoolerCmd := exec.Command("multipooler",
			"--grpc-port", fmt.Sprintf("%d", node.grpcPort),
			"--database", database,
			"--table-group", "test",
			"--pgctld-addr", fmt.Sprintf("localhost:%d", node.pgctldGrpcPort),
			"--pooler-dir", node.dataDir,
			"--pg-port", fmt.Sprintf("%d", node.pgPort),
			"--service-map", "grpc-pooler,grpc-poolermanager,grpc-consensus,grpc-backup",
			"--topo-global-server-addresses", etcdClientAddr,
			"--topo-global-root", "/multigres/global",
			"--topo-implementation", "etcd2",
			"--cell", cellName,
			"--service-id", serviceID,
			"--pgbackrest-stanza", pgBackRestStanza,
		)
		multipoolerCmd.Dir = node.dataDir
		mpLogFile := filepath.Join(node.dataDir, "multipooler.log")
		mpLogF, err := os.Create(mpLogFile)
		require.NoError(t, err)
		multipoolerCmd.Stdout = mpLogF
		multipoolerCmd.Stderr = mpLogF
		require.NoError(t, multipoolerCmd.Start())
		node.multipoolerCmd = multipoolerCmd
		t.Logf("Started multipooler for %s (pid: %d, grpc: %d)", node.name, multipoolerCmd.Process.Pid, node.grpcPort)

		// Wait for multipooler to be ready
		waitForMultipoolerReady(t, node.grpcPort, 30*time.Second)
		t.Logf("Multipooler %s is ready", node.name)

		nodes[i] = node
		defer cleanupNode(t, node)
	}

	t.Logf("Created 3 nodes with pgctld running but no PostgreSQL data directory yet")

	// Verify nodes are completely uninitialized (no data directory, no postgres running)
	// Multiorch will detect these as needing bootstrap and call InitializeEmptyPrimary
	// which will create the data directory, configure archive mode, and start postgres
	for i, node := range nodes {
		status := checkInitializationStatus(t, node)
		t.Logf("Node %d (%s) InitializationStatus: IsInitialized=%v, HasDataDirectory=%v, PostgresRunning=%v, Role=%s",
			i, node.name, status.IsInitialized, status.HasDataDirectory, status.PostgresRunning, status.Role)
		// Nodes should be completely uninitialized (no data directory at all)
		require.False(t, status.IsInitialized, "Node %d should not be initialized yet", i)
		require.False(t, status.HasDataDirectory, "Node %d should not have data directory yet", i)
		require.False(t, status.PostgresRunning, "Node %d should not have postgres running yet", i)
		t.Logf("Node %d (%s) ready for bootstrap (no data directory, postgres not running)", i, node.name)
	}

	// Setup pgbackrest configuration for all nodes before bootstrap
	setupPgBackRestForBootstrap(t, tempDir, nodes, pgBackRestStanza)

	// Register nodes in topology so multiorch can discover them
	for _, node := range nodes {
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
			Shard:      shardID,
			Database:   database,
			TableGroup: "test",
			Type:       clustermetadatapb.PoolerType_UNKNOWN, // All start with unknown type until bootstrap determines role
		}
		err = ts.RegisterMultiPooler(ctx, pooler, true /* overwrite */)
		require.NoError(t, err, "Failed to register pooler %s in topology", node.name)
		t.Logf("Registered pooler %s in topology", node.name)
	}

	// Start multiorch to watch this shard
	watchTarget := fmt.Sprintf("%s/test/%s", database, shardID)
	multiOrchCmd := startMultiOrch(t, tempDir, cellName, etcdClientAddr, []string{watchTarget})
	defer terminateProcess(t, multiOrchCmd, "multiorch", 5*time.Second)

	// Wait for multiorch to detect uninitialized shard and bootstrap it automatically
	t.Logf("Waiting for multiorch to detect and bootstrap the shard...")
	primaryNode := waitForShardBootstrapped(t, nodes, 60*time.Second)
	require.NotNil(t, primaryNode, "Expected multiorch to bootstrap shard automatically")

	// Verify bootstrap results
	t.Run("verify primary initialized", func(t *testing.T) {
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
