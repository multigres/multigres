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
// BeginTerm auto-demotion test:
//   - TestBeginTermAutoDemote: Verifies that primaries auto-demote when
//     accepting a higher term via BeginTerm, preventing split-brain.
package endtoend

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	"github.com/multigres/multigres/go/test/utils"
)

// TestBeginTermDemotesPrimary verifies that when a primary accepts a BeginTerm
// for a higher term, it automatically demotes itself to prevent split-brain.
// The response includes the demote_lsn (final LSN before demotion).
func TestBeginTermDemotesPrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestBeginTermAutoDemote test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping BeginTerm auto-demote test (short mode or no postgres binaries)")
	}

	_, err := exec.LookPath("etcd")
	require.NoError(t, err, "etcd binary must be available in PATH")

	// Setup test environment using existing helpers
	env := setupMultiOrchTestEnv(t, testEnvConfig{
		tempDirPrefix:    "btad*",
		cellName:         "test-cell",
		database:         "postgres",
		shardID:          "auto-demote-shard",
		tableGroup:       "test",
		durabilityPolicy: "ANY_2",
		stanzaName:       "auto-demote-test",
	})

	// Create 3 nodes
	nodes := env.createNodes(3)
	t.Logf("Created 3 empty nodes")

	// Setup pgbackrest
	env.setupPgBackRest()

	// Register all nodes in topology
	env.registerNodes()

	// Start multiorch for initial bootstrap only
	multiOrchCmd := env.startMultiOrch()

	// Wait for initial bootstrap
	t.Logf("Waiting for initial bootstrap...")
	primaryNode := waitForShardPrimary(t, nodes, 90*time.Second)
	require.NotNil(t, primaryNode)
	t.Logf("Initial primary: %s", primaryNode.name)

	// Wait for standbys to complete initialization
	t.Logf("Waiting for standbys to complete initialization...")
	waitForStandbysInitialized(t, nodes, primaryNode.name, len(nodes)-1, 60*time.Second)

	// CRITICAL: Stop multiorch so it doesn't interfere with our manual BeginTerm test
	t.Logf("Stopping multiorch to prevent automatic recovery...")
	terminateProcess(t, multiOrchCmd, "multiorch", 5*time.Second)

	// Continue with BeginTerm test...
	testBeginTermWithAutoDemotion(t, nodes, primaryNode)
}

// getCurrentTerm queries the consensus status of a node and returns its current term.
func getCurrentTerm(t *testing.T, node *nodeInstance) int64 {
	t.Helper()

	client := rpcclient.NewMultiPoolerClient(10)
	defer client.Close()

	pooler := createMultiPoolerProto(node.grpcPort)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ConsensusStatus(ctx, pooler, &consensusdatapb.StatusRequest{})
	require.NoError(t, err, "Failed to get consensus status from %s", node.name)

	return resp.CurrentTerm
}

// sendBeginTerm sends a BeginTerm RPC to a node and returns the full response.
func sendBeginTerm(t *testing.T, node *nodeInstance, term int64, candidateID *clustermetadatapb.ID) *consensusdatapb.BeginTermResponse {
	t.Helper()

	client := rpcclient.NewMultiPoolerClient(10)
	defer client.Close()

	pooler := createMultiPoolerProto(node.grpcPort)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &consensusdatapb.BeginTermRequest{
		Term:        term,
		CandidateId: candidateID,
	}

	resp, err := client.BeginTerm(ctx, pooler, req)
	require.NoError(t, err, "Failed to send BeginTerm to %s", node.name)

	t.Logf("BeginTerm response from %s: accepted=%v, term=%d, demote_lsn=%s",
		node.name, resp.Accepted, resp.Term, resp.DemoteLsn)
	return resp
}

// isPostgresPrimary checks if the PostgreSQL instance on a node is running as primary.
// Returns true if pg_is_in_recovery() returns false (meaning it's a primary).
func isPostgresPrimary(t *testing.T, node *nodeInstance) bool {
	t.Helper()

	socketDir := filepath.Join(node.dataDir, "pg_sockets")
	db := connectToPostgres(t, socketDir, node.pgPort)
	defer db.Close()

	var isInRecovery bool
	err := db.QueryRow("SELECT pg_is_in_recovery()").Scan(&isInRecovery)
	require.NoError(t, err, "Failed to check recovery status on %s", node.name)

	return !isInRecovery // Primary means NOT in recovery
}

// testBeginTermWithAutoDemotion sends BeginTerm to all nodes and verifies
// that the primary auto-demotes when accepting a higher term.
func testBeginTermWithAutoDemotion(t *testing.T, nodes []*nodeInstance, primaryNode *nodeInstance) {
	t.Helper()

	// Step 1: Get current term from primary
	currentTerm := getCurrentTerm(t, primaryNode)
	t.Logf("Current term from primary %s: %d", primaryNode.name, currentTerm)

	// Step 2: Verify primary is actually running as postgres primary
	require.True(t, isPostgresPrimary(t, primaryNode),
		"Primary node %s should be running postgres as primary before test", primaryNode.name)
	t.Logf("Confirmed %s is running postgres as primary", primaryNode.name)

	// Step 3: Create a fake coordinator ID for our BeginTerm request
	fakeCoordinatorID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "fake-coordinator",
	}

	// Step 4: Send BeginTerm with term+1 to all nodes (simulating coordinator behavior)
	newTerm := currentTerm + 1
	t.Logf("Sending BeginTerm with term %d to all nodes...", newTerm)

	acceptedCount := 0
	var primaryResponse *consensusdatapb.BeginTermResponse
	for _, node := range nodes {
		resp := sendBeginTerm(t, node, newTerm, fakeCoordinatorID)
		if resp.Accepted {
			acceptedCount++
			t.Logf("Node %s accepted term %d", node.name, newTerm)
			if node.name == primaryNode.name {
				primaryResponse = resp
			}
		} else {
			t.Logf("Node %s rejected term %d", node.name, newTerm)
		}
	}

	// All healthy nodes should accept the new term
	require.GreaterOrEqual(t, acceptedCount, 2,
		"At least 2 nodes should accept the new term for quorum")

	// Step 5: Verify primary's response includes demote_lsn
	t.Run("verifies_primary_response_fields", func(t *testing.T) {
		require.NotNil(t, primaryResponse, "Primary should have responded")
		assert.NotEmpty(t, primaryResponse.DemoteLsn,
			"Primary's response should include demote_lsn after auto-demotion")
		t.Logf("Primary response: demote_lsn=%s", primaryResponse.DemoteLsn)
	})

	// Step 6: Wait a moment for postgres to restart as standby
	time.Sleep(3 * time.Second)

	// Step 7: Verify primary has demoted (no longer running as primary)
	t.Run("verifies_primary_demoted", func(t *testing.T) {
		stillPrimary := isPostgresPrimary(t, primaryNode)
		assert.False(t, stillPrimary,
			"Primary %s should have demoted after accepting term %d", primaryNode.name, newTerm)

		if !stillPrimary {
			t.Logf("SUCCESS: Primary %s demoted after accepting BeginTerm with term %d", primaryNode.name, newTerm)
		}
	})

	// Step 8: Verify old primary rejects writes (is now read-only)
	t.Run("verifies_writes_rejected", func(t *testing.T) {
		socketDir := filepath.Join(primaryNode.dataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, primaryNode.pgPort)
		defer db.Close()

		// Try to create a test table - should fail because node is now read-only
		_, err := db.Exec("CREATE TABLE IF NOT EXISTS split_brain_test (id serial PRIMARY KEY, value text)")
		if err != nil {
			t.Logf("SUCCESS: Old primary correctly rejects DDL after demotion: %v", err)
			assert.Contains(t, err.Error(), "read-only",
				"Error should indicate read-only mode")
		} else {
			// DDL might succeed if table already exists, try INSERT
			_, err = db.Exec("INSERT INTO split_brain_test (value) VALUES ('written after term change')")
			require.Error(t, err, "Old primary should reject writes after demotion")
			t.Logf("SUCCESS: Old primary correctly rejects INSERT after demotion: %v", err)
			assert.Contains(t, err.Error(), "read-only",
				"Error should indicate read-only mode")
		}
	})
}
