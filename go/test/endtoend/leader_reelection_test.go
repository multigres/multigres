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
// Leader reelection tests:
//   - TestMultiOrchLeaderReelection: Verifies multiorch detects primary failure and
//     automatically elects a new leader from remaining standbys.
package endtoend

import (
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/utils"
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
		require.Equal(t, clustermetadatapb.PoolerType_PRIMARY, status.PoolerType, "New leader should have PRIMARY pooler type")

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
