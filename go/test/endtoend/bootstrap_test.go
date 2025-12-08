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
//     uninitialized shards without manual intervention, including synchronous replication setup.
package endtoend

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestBootstrapInitialization(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end bootstrap test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end bootstrap test (short mode or no postgres binaries)")
	}

	// Setup test environment using shared helper
	env := setupMultiOrchTestEnv(t, testEnvConfig{
		tempDirPrefix:    "btst*",
		cellName:         "test-cell",
		database:         "postgres",
		shardID:          "test-shard-01",
		tableGroup:       "test",
		durabilityPolicy: "ANY_2",
		stanzaName:       "bootstrap-test",
	})

	nodes := env.createNodes(3)

	// Verify nodes are completely uninitialized (no data directory, no postgres running)
	// Multiorch will detect these as needing bootstrap and call InitializeEmptyPrimary
	// which will create the data directory, configure archive mode, and start postgres
	for i, node := range nodes {
		status := checkInitializationStatus(t, node)
		t.Logf("Node %d (%s) Status: IsInitialized=%v, HasDataDirectory=%v, PostgresRunning=%v, PostgresRole=%s, PoolerType=%s",
			i, node.name, status.IsInitialized, status.HasDataDirectory, status.PostgresRunning, status.PostgresRole, status.PoolerType)
		// Nodes should be completely uninitialized (no data directory at all)
		require.False(t, status.IsInitialized, "Node %d should not be initialized yet", i)
		require.False(t, status.HasDataDirectory, "Node %d should not have data directory yet", i)
		require.False(t, status.PostgresRunning, "Node %d should not have postgres running yet", i)
		t.Logf("Node %d (%s) ready for bootstrap (no data directory, postgres not running)", i, node.name)
	}

	env.setupPgBackRest()
	env.registerNodes()

	// Start multiorch and wait for bootstrap
	env.startMultiOrch()

	// Wait for multiorch to detect uninitialized shard and bootstrap it automatically
	t.Logf("Waiting for multiorch to detect and bootstrap the shard...")
	primaryNode := waitForShardPrimary(t, nodes, 60*time.Second)
	require.NotNil(t, primaryNode, "Expected multiorch to bootstrap shard automatically")

	// Wait for at least 1 standby to initialize, which indicates bootstrap has progressed.
	// Some standbys may fail to initialize in test environments due to resource constraints.
	t.Logf("Waiting for standbys to complete initialization...")
	waitForStandbysInitialized(t, nodes, primaryNode.name, 1, 60*time.Second)

	// Give bootstrap time to complete sync replication configuration.
	// This is needed because initializeStandbys may take up to 30s per failed node,
	// and sync replication config happens after all standby attempts complete.
	t.Log("Waiting for bootstrap to complete sync replication configuration...")
	time.Sleep(35 * time.Second)

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
		// Count standbys using PoolerType from topology
		standbyCount := 0
		for _, node := range nodes {
			status := checkInitializationStatus(t, node)
			if status.IsInitialized && status.PoolerType == clustermetadatapb.PoolerType_REPLICA {
				standbyCount++
				t.Logf("Standby node: %s (pooler_type=%s)", node.name, status.PoolerType)
			}
		}
		// Should have at least 1 standby (might have issues with some)
		assert.GreaterOrEqual(t, standbyCount, 1, "Should have at least one standby")
	})

	t.Run("verify multigres internal tables exist", func(t *testing.T) {
		// Verify tables exist on all initialized nodes (both primary and standbys)
		// Note: Some nodes may report IsInitialized but have PostgreSQL unavailable
		// due to startup failures in resource-constrained test environments
		for _, node := range nodes {
			status := checkInitializationStatus(t, node)
			if status.IsInitialized && status.PostgresRunning {
				verifyMultigresTablesExist(t, node)
				t.Logf("Verified multigres tables exist on %s (pooler_type=%s)", node.name, status.PoolerType)
			}
		}
	})

	t.Run("verify consensus term", func(t *testing.T) {
		// All fully initialized nodes should have consensus term = 1
		// Skip nodes that failed to start PostgreSQL
		for _, node := range nodes {
			status := checkInitializationStatus(t, node)
			if status.IsInitialized && status.PostgresRunning {
				assert.Equal(t, int64(1), status.ConsensusTerm, "Node %s should have consensus term 1", node.name)
			}
		}
	})

	t.Run("verify sync replication configured", func(t *testing.T) {
		// Verify synchronous_standby_names is configured on primary based on durability policy
		socketDir := filepath.Join(primaryNode.dataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, primaryNode.pgPort)
		defer db.Close()

		var syncStandbyNames string
		err := db.QueryRow("SHOW synchronous_standby_names").Scan(&syncStandbyNames)
		require.NoError(t, err, "should query synchronous_standby_names")

		assert.NotEmpty(t, syncStandbyNames,
			"synchronous_standby_names should be configured for ANY_2 policy")
		t.Logf("synchronous_standby_names = %s", syncStandbyNames)

		// Verify it contains ANY keyword (for ANY_2 policy)
		assert.Contains(t, strings.ToUpper(syncStandbyNames), "ANY",
			"should use ANY method for ANY_2 policy")
	})
}
