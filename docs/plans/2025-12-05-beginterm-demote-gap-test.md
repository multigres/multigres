# BeginTerm Auto-Demotion Gap Test Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create an end-to-end test that demonstrates the gap where BeginTerm does not trigger automatic demotion on primaries.

**Architecture:** This test bootstraps a 3-node cluster, manually sends BeginTerm RPCs to simulate a coordinator starting a new term, and verifies that the primary does NOT demote itself (demonstrating the current gap). It also demonstrates the split-brain risk by showing that the old primary can still accept writes after accepting a higher term.

**Tech Stack:** Go testing, gRPC rpcclient, existing multiorch_helpers.go test utilities

---

## Background

The BeginTerm protocol currently has a gap:
- When a coordinator sends BeginTerm to nodes, primaries accept the term but do NOT demote themselves
- This can lead to split-brain: old primary still accepts writes while new primary is being elected
- Expected behavior: A primary that accepts BeginTerm for a higher term should automatically demote

## Test Overview

The test will:
1. Bootstrap a healthy 3-node cluster via multiorch
2. Stop multiorch (to prevent automatic recovery interfering)
3. Manually send BeginTerm to all nodes with term+1
4. Verify the primary accepted the term but is still running as primary (the bug)
5. Demonstrate split-brain potential by writing to both old primary and attempting promotion of a standby

---

### Task 1: Create test file scaffold

**Files:**
- Create: `go/test/endtoend/beginterm_demote_test.go`

**Step 1: Create the test file with imports and constants**

```go
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
// BeginTerm demotion gap test:
//   - TestBeginTermDemoteGap: Demonstrates that primaries do NOT auto-demote
//     when accepting a higher term via BeginTerm, creating split-brain risk.
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
```

**Step 2: Run go build to verify imports compile**

Run: `go build ./go/test/endtoend/...`
Expected: SUCCESS (no errors)

**Step 3: Commit**

```bash
git add go/test/endtoend/beginterm_demote_test.go
git commit -m "test(endtoend): add scaffold for BeginTerm demote gap test"
```

---

### Task 2: Add test setup using existing helpers

**Files:**
- Modify: `go/test/endtoend/beginterm_demote_test.go`

**Step 1: Add the main test function with setup**

Add after the imports:

```go
// TestBeginTermDemoteGap demonstrates the current gap in BeginTerm handling:
// when a primary accepts a BeginTerm for a higher term, it does NOT demote itself.
// This creates a split-brain risk where the old primary continues accepting writes.
//
// This test is marked with a TODO comment indicating it should PASS once the
// auto-demotion logic is implemented. Currently it demonstrates the bug.
func TestBeginTermDemoteGap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestBeginTermDemoteGap test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping BeginTerm demote gap test (short mode or no postgres binaries)")
	}

	_, err := exec.LookPath("etcd")
	require.NoError(t, err, "etcd binary must be available in PATH")

	// Setup test environment using existing helpers
	env := setupMultiOrchTestEnv(t, testEnvConfig{
		tempDirPrefix:    "btdg*",
		cellName:         "test-cell",
		database:         "postgres",
		shardID:          "demote-gap-shard",
		tableGroup:       "test",
		durabilityPolicy: "ANY_2",
		stanzaName:       "demote-gap-test",
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
	testBeginTermWithoutDemotion(t, nodes, primaryNode)
}
```

**Step 2: Run go build to verify syntax**

Run: `go build ./go/test/endtoend/...`
Expected: FAIL with "testBeginTermWithoutDemotion not defined"

**Step 3: Add stub for testBeginTermWithoutDemotion**

Add after TestBeginTermDemoteGap:

```go
// testBeginTermWithoutDemotion sends BeginTerm to all nodes and verifies
// that the primary does NOT demote itself (demonstrating the bug).
func testBeginTermWithoutDemotion(t *testing.T, nodes []*nodeInstance, primaryNode *nodeInstance) {
	t.Helper()
	// Implementation in next task
	t.Log("TODO: Implement BeginTerm test")
}
```

**Step 4: Run go build again**

Run: `go build ./go/test/endtoend/...`
Expected: SUCCESS

**Step 5: Commit**

```bash
git add go/test/endtoend/beginterm_demote_test.go
git commit -m "test(endtoend): add TestBeginTermDemoteGap setup and helpers"
```

---

### Task 3: Implement getCurrentTerm helper

**Files:**
- Modify: `go/test/endtoend/beginterm_demote_test.go`

**Step 1: Add helper to get current consensus term from a node**

Add before testBeginTermWithoutDemotion:

```go
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
```

**Step 2: Run go build**

Run: `go build ./go/test/endtoend/...`
Expected: SUCCESS

**Step 3: Commit**

```bash
git add go/test/endtoend/beginterm_demote_test.go
git commit -m "test(endtoend): add getCurrentTerm helper"
```

---

### Task 4: Implement sendBeginTerm helper

**Files:**
- Modify: `go/test/endtoend/beginterm_demote_test.go`

**Step 1: Add helper to send BeginTerm RPC to a node**

Add after getCurrentTerm:

```go
// sendBeginTerm sends a BeginTerm RPC to a node and returns whether it was accepted.
func sendBeginTerm(t *testing.T, node *nodeInstance, term int64, candidateID *clustermetadatapb.ID) (accepted bool, responseTerm int64) {
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

	t.Logf("BeginTerm response from %s: accepted=%v, term=%d", node.name, resp.Accepted, resp.Term)
	return resp.Accepted, resp.Term
}
```

**Step 2: Run go build**

Run: `go build ./go/test/endtoend/...`
Expected: SUCCESS

**Step 3: Commit**

```bash
git add go/test/endtoend/beginterm_demote_test.go
git commit -m "test(endtoend): add sendBeginTerm helper"
```

---

### Task 5: Implement isPostgresPrimary helper

**Files:**
- Modify: `go/test/endtoend/beginterm_demote_test.go`

**Step 1: Add helper to check if a node's postgres is running as primary**

Add after sendBeginTerm:

```go
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
```

**Step 2: Run go build**

Run: `go build ./go/test/endtoend/...`
Expected: SUCCESS

**Step 3: Commit**

```bash
git add go/test/endtoend/beginterm_demote_test.go
git commit -m "test(endtoend): add isPostgresPrimary helper"
```

---

### Task 6: Implement the core test logic

**Files:**
- Modify: `go/test/endtoend/beginterm_demote_test.go`

**Step 1: Replace the stub testBeginTermWithoutDemotion with full implementation**

Replace the existing stub with:

```go
// testBeginTermWithoutDemotion sends BeginTerm to all nodes and verifies
// that the primary does NOT demote itself (demonstrating the bug).
func testBeginTermWithoutDemotion(t *testing.T, nodes []*nodeInstance, primaryNode *nodeInstance) {
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
	for _, node := range nodes {
		accepted, _ := sendBeginTerm(t, node, newTerm, fakeCoordinatorID)
		if accepted {
			acceptedCount++
			t.Logf("Node %s accepted term %d", node.name, newTerm)
		} else {
			t.Logf("Node %s rejected term %d", node.name, newTerm)
		}
	}

	// All healthy nodes should accept the new term
	require.GreaterOrEqual(t, acceptedCount, 2,
		"At least 2 nodes should accept the new term for quorum")

	// Step 5: Wait a moment for any async operations
	time.Sleep(2 * time.Second)

	// Step 6: THE BUG - Primary should have demoted but doesn't!
	// This assertion documents the current (buggy) behavior.
	// TODO: Once auto-demotion is implemented, change this to:
	//   assert.False(t, isPostgresPrimary(t, primaryNode), "Primary should have demoted after accepting higher term")
	t.Run("demonstrates_bug_primary_not_demoted", func(t *testing.T) {
		stillPrimary := isPostgresPrimary(t, primaryNode)

		// Current behavior (BUG): Primary does NOT demote itself
		assert.True(t, stillPrimary,
			"BUG DEMONSTRATED: Primary %s is still running as primary after accepting term %d (should have demoted)",
			primaryNode.name, newTerm)

		if stillPrimary {
			t.Logf("BUG CONFIRMED: Primary %s did NOT demote after accepting BeginTerm with term %d", primaryNode.name, newTerm)
			t.Logf("This creates split-brain risk: old primary can still accept writes")
		}
	})

	// Step 7: Demonstrate split-brain potential - write to old primary
	t.Run("demonstrates_split_brain_risk", func(t *testing.T) {
		socketDir := filepath.Join(primaryNode.dataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, primaryNode.pgPort)
		defer db.Close()

		// Create a test table and insert data AFTER accepting higher term
		_, err := db.Exec("CREATE TABLE IF NOT EXISTS split_brain_test (id serial PRIMARY KEY, value text)")
		require.NoError(t, err, "Should be able to create table on old primary (BUG: should be demoted)")

		_, err = db.Exec("INSERT INTO split_brain_test (value) VALUES ('written after term change')")
		require.NoError(t, err, "Should be able to insert on old primary (BUG: should be demoted)")

		t.Logf("SPLIT-BRAIN RISK DEMONSTRATED: Successfully wrote to old primary after it accepted term %d", newTerm)
		t.Logf("In a real scenario, a new primary could be promoted and accept conflicting writes")
	})
}
```

**Step 2: Run go build**

Run: `go build ./go/test/endtoend/...`
Expected: SUCCESS

**Step 3: Commit**

```bash
git add go/test/endtoend/beginterm_demote_test.go
git commit -m "test(endtoend): implement core BeginTerm demote gap test logic"
```

---

### Task 7: Run the test and verify it demonstrates the bug

**Files:**
- Test: `go/test/endtoend/beginterm_demote_test.go`

**Step 1: Run the test**

Run: `PATH=/Users/deepthi/.gvm/gos/go1.25.0/bin:/Applications/Postgres.app/Contents/Versions/18/bin:/Users/deepthi/github/multigres/bin:$PATH MTROOT=/Users/deepthi/github/multigres KEEP_TEMP_DIRS=1 go test -v -run TestBeginTermDemoteGap -timeout 5m ./go/test/endtoend/...`

Expected output should include:
```
BUG CONFIRMED: Primary node0 did NOT demote after accepting BeginTerm with term 2
SPLIT-BRAIN RISK DEMONSTRATED: Successfully wrote to old primary after it accepted term 2
--- PASS: TestBeginTermDemoteGap
```

**Step 2: Verify test artifacts (if KEEP_TEMP_DIRS=1)**

Check the temp directory for logs:
- `node0/multipooler.log` - should show BeginTerm accepted
- `node0/pgctld.log` - should NOT show any demote activity

**Step 3: Final commit**

```bash
git add go/test/endtoend/beginterm_demote_test.go
git commit -m "test(endtoend): verify BeginTerm demote gap test demonstrates the bug

The test confirms:
1. Primary accepts BeginTerm for higher term
2. Primary does NOT demote itself (the bug)
3. Old primary can still accept writes (split-brain risk)

Once auto-demotion is implemented, the test assertions should be
inverted to verify the fix."
```

---

## Summary

This test demonstrates the current gap in the consensus protocol:

1. **Setup**: Bootstrap a 3-node cluster with multiorch, then stop multiorch
2. **Action**: Manually send BeginTerm with term+1 to all nodes (simulating coordinator)
3. **Expected behavior (once fixed)**: Primary should demote itself
4. **Current behavior (bug)**: Primary accepts the term but continues running as primary
5. **Risk demonstrated**: Old primary can still accept writes, creating split-brain potential

When the auto-demotion logic is implemented, the test's assertions in `demonstrates_bug_primary_not_demoted` should be inverted to verify the fix works correctly.
