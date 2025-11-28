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
// Leader reelection and recovery tests:
//   - TestMultiOrchLeaderReelection: Verifies multiorch detects primary failure and
//     automatically elects a new leader from remaining standbys.
//   - TestMultiOrchMixedInitializationRepair: Verifies multiorch handles mixed scenarios
//     where some nodes are initialized and others are not, preferring to promote
//     initialized standbys over bootstrapping new nodes.
package endtoend

import (
	"log/slog"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/multiorch/recovery/actions"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// TestMultiOrchLeaderReelection tests multiorch's ability to detect a primary failure
// and elect a new primary from the standbys.
//
// This test previously failed due to a multiorch bootstrap coordination race condition.
// Multiple nodes detected "ShardNeedsBootstrap" simultaneously and attempted to bootstrap concurrently.
// The second bootstrap attempt restarted postgres on the primary while the first was still completing,
// creating a second postgres instance that masked the test's intentional SIGKILL.
// This prevented multiorch from detecting the primary failure and electing a new leader.
// The race typically occurred ~7 seconds after initial bootstrap completion.
// Fixed: Distributed locking now prevents concurrent bootstrap attempts.
func TestMultiOrchLeaderReelection(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end leader reelection test (short mode or no postgres binaries)")
	}

	_, err := exec.LookPath("etcd")
	require.NoError(t, err, "etcd binary must be available in PATH")

	// Setup test environment
	env := setupMultiOrchTestEnv(t, testEnvConfig{
		tempDirPrefix:    "lrtest*",
		cellName:         "test-cell",
		database:         "postgres",
		shardID:          "test-shard-reelect",
		tableGroup:       "test",
		durabilityPolicy: "ANY_2",
		stanzaName:       "reelect-test",
	})

	// Create 3 nodes
	nodes := env.createNodes(3)
	t.Logf("Created 3 empty nodes")

	// Setup pgbackrest
	env.setupPgBackRest()

	// Register all nodes in topology
	env.registerNodes()

	// Start multiorch
	env.startMultiOrch()

	// Wait for initial bootstrap
	t.Logf("Waiting for initial bootstrap...")
	primaryNode := waitForShardBootstrapped(t, nodes, 60*time.Second)
	require.NotNil(t, primaryNode)
	t.Logf("Initial primary: %s", primaryNode.name)

	// Wait for standbys to catch up
	time.Sleep(5 * time.Second)

	// Kill postgres on the primary (multipooler stays running to report unhealthy status)
	t.Logf("Killing postgres on primary node %s to simulate database crash", primaryNode.name)
	killPostgres(t, primaryNode)

	// Wait for multiorch to detect failure and elect new primary
	t.Logf("Waiting for multiorch to detect primary failure and elect new leader...")

	newPrimaryNode := waitForNewPrimaryElected(t, nodes, primaryNode.name, 60*time.Second)
	require.NotNil(t, newPrimaryNode, "Expected multiorch to elect new primary automatically")
	t.Logf("New primary elected: %s", newPrimaryNode.name)

	// Verify new primary is functional
	t.Run("verify new primary is functional", func(t *testing.T) {
		status := checkInitializationStatus(t, newPrimaryNode)
		require.True(t, status.IsInitialized, "New primary should be initialized")
		require.Equal(t, "primary", status.Role, "New leader should have primary role")

		// Verify we can connect and query
		socketDir := filepath.Join(newPrimaryNode.dataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, newPrimaryNode.pgPort)
		defer db.Close()

		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		require.NoError(t, err, "Should be able to query new primary")
		assert.Equal(t, 1, result)
	})
}

// TestMultiOrchMixedInitializationRepair tests multiorch's ability to repair a mixed initialization
// scenario where some nodes are initialized and others are not.
//
// The test creates: node0 (initialized primary - killed), node1 (initialized standby), node2 (uninitialized).
// Expected: multiorch should elect node1 (initialized standby) as the new primary.
// The leader election logic now prefers initialized nodes over uninitialized nodes.
func TestMultiOrchMixedInitializationRepair(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end mixed initialization test (short mode or no postgres binaries)")
	}

	_, err := exec.LookPath("etcd")
	require.NoError(t, err, "etcd binary must be available in PATH")

	ctx := t.Context()

	// Setup test environment
	env := setupMultiOrchTestEnv(t, testEnvConfig{
		tempDirPrefix:    "mixtest*",
		cellName:         "test-cell",
		database:         "postgres",
		shardID:          "test-shard-mixed",
		tableGroup:       "test",
		durabilityPolicy: "ANY_2",
		stanzaName:       "mixed-test",
	})

	// Create 3 nodes
	nodes := env.createNodes(3)

	// Setup pgbackrest
	env.setupPgBackRest()

	// Bootstrap first 2 nodes manually (simulate partial initialization)
	t.Logf("Manually bootstrapping first 2 nodes to create mixed scenario...")

	rpcClient := rpcclient.NewMultiPoolerClient(10)
	defer rpcClient.Close()

	coordNodes := make([]*multiorchdatapb.PoolerHealthState, 2)
	for i := range 2 {
		node := nodes[i]
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
			Shard:    env.config.shardID,
			Database: env.config.database,
		}
		coordNodes[i] = &multiorchdatapb.PoolerHealthState{
			MultiPooler: pooler,
		}
	}

	logger := slog.Default()
	bootstrapAction := actions.NewBootstrapShardAction(rpcClient, env.ts, logger)
	err = bootstrapAction.Execute(ctx, env.config.shardID, env.config.database, coordNodes)
	require.NoError(t, err, "Manual bootstrap of first 2 nodes should succeed")

	time.Sleep(3 * time.Second) // Wait for bootstrap to complete

	// Verify first 2 nodes are initialized
	for i := range 2 {
		status := checkInitializationStatus(t, nodes[i])
		require.True(t, status.IsInitialized, "Node %d should be initialized", i)
		t.Logf("Node %d initialized as %s", i, status.Role)
	}

	// Kill the primary to create a mixed scenario: 1 initialized standby + 1 uninitialized
	var initialPrimary *nodeInstance
	var initialStandby *nodeInstance
	for i := range 2 {
		status := checkInitializationStatus(t, nodes[i])
		if status.Role == "primary" {
			initialPrimary = nodes[i]
		} else {
			initialStandby = nodes[i]
		}
	}
	require.NotNil(t, initialPrimary, "Should have identified initial primary")
	require.NotNil(t, initialStandby, "Should have identified initial standby")

	t.Logf("Killing postgres on initial primary %s to create repair scenario", initialPrimary.name)
	killPostgres(t, initialPrimary)

	// Register all nodes in topology (including the uninitialized node3)
	for _, node := range nodes {
		if node.name == initialPrimary.name {
			continue // Don't register the killed primary
		}
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
		err = env.ts.RegisterMultiPooler(ctx, pooler, true)
		require.NoError(t, err, "Failed to register pooler %s", node.name)
		t.Logf("Registered %s (initialized: %v)", node.name, node.name != nodes[2].name)
	}

	// Start multiorch to watch and repair
	env.startMultiOrch()

	// Wait for multiorch to detect mixed state and appoint the initialized standby as primary
	t.Logf("Waiting for multiorch to detect mixed initialization and appoint new leader...")

	newPrimary := waitForShardBootstrapped(t, []*nodeInstance{initialStandby, nodes[2]}, 60*time.Second)
	require.NotNil(t, newPrimary, "Expected multiorch to appoint new primary")

	// Should prefer the initialized standby over uninitialized node
	assert.Equal(t, initialStandby.name, newPrimary.name,
		"MultiOrch should elect the initialized standby as primary")

	t.Logf("MultiOrch successfully repaired mixed initialization scenario: %s is new primary", newPrimary.name)
}
