# MultiOrch Recovery System Improvements

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Clean up TODOs, centralize shard locking, add lock timeout, and add missing unit tests.

**Architecture:** Centralize shard lock path construction in topo package to prevent future mistakes, add configurable lock acquisition timeout to RecoveryMetadata, and address code review findings.

**Tech Stack:** Go, Protocol Buffers, etcd (topo), gRPC

---

## Task 1: Add LockShardForRecovery Helper to Topo Package

**Context:** Both `AppointLeaderRecoveryAction` and `BootstrapRecoveryAction` duplicate the lock path construction (`recovery/{db}/{tg}/{shard}`). Centralizing this in topo prevents future actions from making mistakes in lock name construction.

**Files:**
- Create: `go/clustermetadata/topo/recovery_lock.go`
- Create: `go/clustermetadata/topo/recovery_lock_test.go`

**Step 1: Write failing test for the helper**

Create `go/clustermetadata/topo/recovery_lock_test.go`:

```go
// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package topo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
)

func TestLockShardForRecovery(t *testing.T) {
	ctx := context.Background()
	ts, err := memorytopo.NewServer(ctx, "global")
	require.NoError(t, err)
	defer ts.Close()

	// Acquire lock
	lockDesc, err := ts.LockShardForRecovery(ctx, "db1", "tg1", "shard1", "test operation", 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, lockDesc)

	// Verify lock blocks second acquisition (with short timeout)
	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = ts.LockShardForRecovery(shortCtx, "db1", "tg1", "shard1", "second operation", 100*time.Millisecond)
	assert.Error(t, err, "second lock acquisition should fail")

	// Release first lock
	err = lockDesc.Unlock(ctx)
	require.NoError(t, err)

	// Now should be able to acquire again
	lockDesc2, err := ts.LockShardForRecovery(ctx, "db1", "tg1", "shard1", "third operation", 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, lockDesc2)
	defer lockDesc2.Unlock(ctx)
}

func TestLockShardForRecovery_DifferentShards(t *testing.T) {
	ctx := context.Background()
	ts, err := memorytopo.NewServer(ctx, "global")
	require.NoError(t, err)
	defer ts.Close()

	// Acquire lock on shard1
	lock1, err := ts.LockShardForRecovery(ctx, "db1", "tg1", "shard1", "op1", 5*time.Second)
	require.NoError(t, err)
	defer lock1.Unlock(ctx)

	// Should be able to acquire lock on shard2 (different shard)
	lock2, err := ts.LockShardForRecovery(ctx, "db1", "tg1", "shard2", "op2", 5*time.Second)
	require.NoError(t, err)
	defer lock2.Unlock(ctx)
}

func TestLockShardForRecovery_Timeout(t *testing.T) {
	ctx := context.Background()
	ts, err := memorytopo.NewServer(ctx, "global")
	require.NoError(t, err)
	defer ts.Close()

	// Acquire lock
	lock1, err := ts.LockShardForRecovery(ctx, "db1", "tg1", "shard1", "op1", 5*time.Second)
	require.NoError(t, err)
	defer lock1.Unlock(ctx)

	// Try to acquire with very short timeout - should fail with timeout error
	start := time.Now()
	_, err = ts.LockShardForRecovery(ctx, "db1", "tg1", "shard1", "op2", 50*time.Millisecond)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Less(t, elapsed, 200*time.Millisecond, "should timeout quickly")
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./go/clustermetadata/topo/... -v -run TestLockShardForRecovery`
Expected: FAIL (function doesn't exist)

**Step 3: Create the helper implementation**

Create `go/clustermetadata/topo/recovery_lock.go`:

```go
// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package topo

import (
	"context"
	"fmt"
	"time"
)

// RecoveryLockPath returns the canonical lock path for shard-level recovery operations.
// This centralizes the lock path format to ensure all recovery actions use consistent naming.
func RecoveryLockPath(database, tableGroup, shard string) string {
	return fmt.Sprintf("recovery/%s/%s/%s", database, tableGroup, shard)
}

// LockShardForRecovery acquires a distributed lock for recovery operations on a shard.
// It uses LockName which doesn't require the path to exist and has a 24-hour TTL safety net.
//
// Parameters:
//   - ctx: Parent context (used for cancellation, but lock acquisition has its own timeout)
//   - database, tableGroup, shard: Identify the shard to lock
//   - purpose: Description of the recovery operation (for debugging)
//   - lockTimeout: Maximum time to wait for lock acquisition (should be < total operation timeout)
//
// Returns a LockDescriptor that MUST be unlocked when done, even if the operation fails.
// Use a defer with background context for unlock to ensure cleanup:
//
//	lockDesc, err := ts.LockShardForRecovery(ctx, db, tg, shard, "bootstrap", 15*time.Second)
//	if err != nil { return err }
//	defer func() {
//	    unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	    defer cancel()
//	    lockDesc.Unlock(unlockCtx)
//	}()
func (ts *store) LockShardForRecovery(ctx context.Context, database, tableGroup, shard, purpose string, lockTimeout time.Duration) (LockDescriptor, error) {
	lockPath := RecoveryLockPath(database, tableGroup, shard)
	lockContents := fmt.Sprintf("%s for shard %s/%s/%s", purpose, database, tableGroup, shard)

	// Create a timeout context for lock acquisition
	lockCtx, cancel := context.WithTimeout(ctx, lockTimeout)
	defer cancel()

	conn, err := ts.ConnForCell(lockCtx, GlobalCell)
	if err != nil {
		return nil, fmt.Errorf("failed to get topo connection for recovery lock: %w", err)
	}

	lockDesc, err := conn.LockName(lockCtx, lockPath, lockContents)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire recovery lock on %s: %w", lockPath, err)
	}

	return lockDesc, nil
}
```

**Step 4: Add method to Store interface**

In `go/clustermetadata/topo/store.go`, add to the `Store` interface:

```go
	// LockShardForRecovery acquires a distributed lock for recovery operations on a shard.
	// See recovery_lock.go for full documentation.
	LockShardForRecovery(ctx context.Context, database, tableGroup, shard, purpose string, lockTimeout time.Duration) (LockDescriptor, error)
```

**Step 5: Run tests to verify they pass**

Run: `go test ./go/clustermetadata/topo/... -v -run TestLockShardForRecovery`
Expected: PASS

**Step 6: Commit**

```bash
git add go/clustermetadata/topo/recovery_lock.go go/clustermetadata/topo/recovery_lock_test.go go/clustermetadata/topo/store.go
git commit -m "feat(topo): add LockShardForRecovery helper to centralize recovery lock path"
```

---

## Task 2: Add LockTimeout to RecoveryMetadata and Update Recovery Actions

**Context:** Lock acquisition should have a shorter timeout than the total action timeout. Add `LockTimeout` field to `RecoveryMetadata` with 15-second default, and update recovery actions to use the new helper.

**Files:**
- Modify: `go/multiorch/recovery/analysis/recovery.go`
- Modify: `go/multiorch/recovery/analysis/appoint_leader_recovery_action.go`
- Modify: `go/multiorch/recovery/analysis/bootstrap_recovery_action.go`

**Step 1: Add LockTimeout to RecoveryMetadata**

In `go/multiorch/recovery/analysis/recovery.go`, update RecoveryMetadata:

```go
// RecoveryMetadata describes the recovery action.
type RecoveryMetadata struct {
	Name        string
	Description string
	Timeout     time.Duration
	// LockTimeout is the maximum time to wait for lock acquisition.
	// Should be shorter than Timeout to leave time for the actual operation.
	// Defaults to 15 seconds if zero.
	LockTimeout time.Duration
	Retryable   bool
}

// GetLockTimeout returns the lock timeout, defaulting to 15 seconds if not set.
func (m RecoveryMetadata) GetLockTimeout() time.Duration {
	if m.LockTimeout == 0 {
		return 15 * time.Second
	}
	return m.LockTimeout
}
```

**Step 2: Update AppointLeaderRecoveryAction**

In `go/multiorch/recovery/analysis/appoint_leader_recovery_action.go`:

1. Remove the manual lock path construction
2. Use `topo.LockShardForRecovery`
3. Use background context for unlock
4. Add LockTimeout to Metadata

Replace the lock acquisition code (lines 44-69):

```go
func (a *AppointLeaderRecoveryAction) Execute(ctx context.Context, problem Problem) error {
	a.logger.InfoContext(ctx, "executing appoint leader recovery action",
		"database", problem.Database,
		"tablegroup", problem.TableGroup,
		"shard", problem.Shard)

	// Step 1: Acquire distributed lock for this shard
	metadata := a.Metadata()
	lockPath := topo.RecoveryLockPath(problem.Database, problem.TableGroup, problem.Shard)
	a.logger.InfoContext(ctx, "acquiring recovery lock", "lock_path", lockPath)

	lockDesc, err := a.topoStore.LockShardForRecovery(
		ctx,
		problem.Database,
		problem.TableGroup,
		problem.Shard,
		"appoint leader recovery",
		metadata.GetLockTimeout(),
	)
	if err != nil {
		a.logger.InfoContext(ctx, "failed to acquire lock, another recovery may be in progress",
			"lock_path", lockPath,
			"error", err)
		return err
	}
	defer func() {
		// Use background context for unlock to ensure lock release even if ctx is cancelled
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if unlockErr := lockDesc.Unlock(unlockCtx); unlockErr != nil {
			a.logger.WarnContext(ctx, "failed to release recovery lock",
				"lock_path", lockPath,
				"error", unlockErr)
		}
	}()

	a.logger.InfoContext(ctx, "acquired recovery lock", "lock_path", lockPath)

	// ... rest of the method stays the same
```

Update Metadata():

```go
func (a *AppointLeaderRecoveryAction) Metadata() RecoveryMetadata {
	return RecoveryMetadata{
		Name:        "AppointLeader",
		Description: "Elect a new primary for the shard using consensus",
		Timeout:     60 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}
```

**Step 3: Update BootstrapRecoveryAction with the same pattern**

Apply the same changes to `bootstrap_recovery_action.go`.

**Step 4: Run tests**

Run: `go test ./go/multiorch/recovery/analysis/... -v`
Expected: PASS

**Step 5: Commit**

```bash
git add go/multiorch/recovery/analysis/recovery.go go/multiorch/recovery/analysis/appoint_leader_recovery_action.go go/multiorch/recovery/analysis/bootstrap_recovery_action.go
git commit -m "feat: add LockTimeout to RecoveryMetadata and use centralized LockShardForRecovery"
```

---

## Task 3: Remove Confusing Shard Locking TODO

**Context:** The recovery_loop.go has a TODO about implementing shard locking, but locking is now centralized in topo and used by recovery actions.

**Files:**
- Modify: `go/multiorch/recovery/recovery_loop.go:213-218`

**Step 1: Read the current code**

Read lines 210-225 of recovery_loop.go to understand the context.

**Step 2: Remove the confusing TODO block**

Replace:
```go
	// Acquire lock if needed
	if problem.RecoveryAction.RequiresLock() {
		// TODO: Implement shard locking
		re.logger.DebugContext(re.ctx, "recovery action requires lock", "problem_code", problem.Code)
	}
```

With:
```go
	// Note: Locking is handled by the recovery actions themselves using
	// topo.LockShardForRecovery(). Actions that require locking (RequiresLock() == true)
	// acquire distributed locks internally with configurable timeout from Metadata().LockTimeout.
	if problem.RecoveryAction.RequiresLock() {
		re.logger.DebugContext(re.ctx, "recovery action will acquire shard lock", "problem_code", problem.Code)
	}
```

**Step 3: Run existing tests**

Run: `go test ./go/multiorch/recovery/... -v`
Expected: PASS

**Step 4: Commit**

```bash
git add go/multiorch/recovery/recovery_loop.go
git commit -m "docs: clarify that locking is handled via topo.LockShardForRecovery"
```

---

## Task 4: Add Port Comparison in Primary Detection

**Context:** The primary detection in generator.go only compares hostnames, not ports. This could cause issues if multiple multipooler instances run on the same host.

**Files:**
- Modify: `go/multiorch/recovery/analysis/generator.go:287-293`
- Test: `go/multiorch/recovery/analysis/generator_test.go` (create if needed)

**Step 1: Write a failing test for port matching**

Create or add to `go/multiorch/recovery/analysis/generator_test.go`:

```go
func TestAggregateReplicaStats_MatchesByHostAndPort(t *testing.T) {
	// Create a store with primary and replica on same host but different ports
	store := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	primaryID := "db1-tg1-shard1-node1"
	replicaID := "db1-tg1-shard1-node2"

	// Primary on host1:5432
	store.Set(primaryID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         &clustermetadatapb.ID{Uid: "node1"},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "shard1",
			Hostname:   "host1",
			PortMap:    map[string]int32{"postgres": 5432},
		},
		PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
		IsLastCheckValid: true,
		PrimaryStatus:    &multipoolermanagerdata.PrimaryStatus{Lsn: "0/1234"},
	})

	// Replica pointing to host1:5433 (wrong port - different primary)
	store.Set(replicaID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         &clustermetadatapb.ID{Uid: "node2"},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "shard1",
			Hostname:   "host2",
		},
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		IsLastCheckValid: true,
		ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
			LastReplayLsn: "0/1234",
			PrimaryConnInfo: &multipoolermanagerdata.PrimaryConnInfo{
				Host: "host1",
				Port: 5433, // Different port!
			},
		},
	})

	gen := NewAnalysisGenerator(store)
	analysis, err := gen.GenerateAnalysisForPooler(primaryID)
	require.NoError(t, err)

	// Should NOT count this replica since port doesn't match
	assert.Equal(t, uint(0), analysis.CountReplicas, "replica with wrong port should not be counted")
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./go/multiorch/recovery/analysis/... -v -run TestAggregateReplicaStats_MatchesByHostAndPort`
Expected: FAIL

**Step 3: Implement port comparison**

In `go/multiorch/recovery/analysis/generator.go`, replace lines 287-293:

```go
			// Also check via primary_conninfo if we didn't find it in connected followers
			if !isPointingToPrimary && pooler.ReplicationStatus != nil && pooler.ReplicationStatus.PrimaryConnInfo != nil {
				connInfo := pooler.ReplicationStatus.PrimaryConnInfo
				primaryPort := primary.MultiPooler.PortMap["postgres"]
				if connInfo.Host == primary.MultiPooler.Hostname && connInfo.Port == primaryPort {
					isPointingToPrimary = true
				}
			}
```

**Step 4: Run test to verify it passes**

Run: `go test ./go/multiorch/recovery/analysis/... -v -run TestAggregateReplicaStats_MatchesByHostAndPort`
Expected: PASS

**Step 5: Commit**

```bash
git add go/multiorch/recovery/analysis/generator.go go/multiorch/recovery/analysis/generator_test.go
git commit -m "fix: add port comparison in primary detection"
```

---

## Task 5: Add Unit Tests for IsInitialized Helper

**Context:** The IsInitialized helper needs comprehensive unit tests.

**Files:**
- Create: `go/multiorch/store/pooler_health_state_test.go`

**Step 1: Create test file**

Create `go/multiorch/store/pooler_health_state_test.go`:

```go
// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"testing"

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdata "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestIsInitialized(t *testing.T) {
	tests := []struct {
		name     string
		pooler   *multiorchdata.PoolerHealthState
		expected bool
	}{
		{
			name: "unreachable node is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: false,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
			},
			expected: false,
		},
		{
			name: "nil MultiPooler is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      nil,
			},
			expected: false,
		},
		{
			name: "primary with LSN is initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
				PrimaryStatus:    &multipoolermanagerdata.PrimaryStatus{Lsn: "0/1234"},
			},
			expected: true,
		},
		{
			name: "primary without LSN is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
				PrimaryStatus:    &multipoolermanagerdata.PrimaryStatus{Lsn: ""},
			},
			expected: false,
		},
		{
			name: "primary with nil PrimaryStatus is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
				PrimaryStatus:    nil,
			},
			expected: false,
		},
		{
			name: "replica with LastReplayLsn is initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
					LastReplayLsn: "0/1234",
				},
			},
			expected: true,
		},
		{
			name: "replica with LastReceiveLsn is initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
					LastReceiveLsn: "0/5678",
				},
			},
			expected: true,
		},
		{
			name: "replica with empty LSNs is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
					LastReplayLsn:  "",
					LastReceiveLsn: "",
				},
			},
			expected: false,
		},
		{
			name: "replica with nil ReplicationStatus is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid:  true,
				MultiPooler:       &clustermetadatapb.MultiPooler{},
				PoolerType:        clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: nil,
			},
			expected: false,
		},
		{
			name: "UNKNOWN type with ReplicationStatus is initialized (fallback to replica check)",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_UNKNOWN,
				ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
					LastReplayLsn: "0/1234",
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsInitialized(tt.pooler)
			assert.Equal(t, tt.expected, result)
		})
	}
}
```

**Step 2: Run the tests**

Run: `go test ./go/multiorch/store/... -v -run TestIsInitialized`
Expected: PASS

**Step 3: Commit**

```bash
git add go/multiorch/store/pooler_health_state_test.go
git commit -m "test: add comprehensive unit tests for IsInitialized helper"
```

---

## Task 6: Add Unit Tests for Generator Edge Cases

**Context:** Test empty store, nil entries, all replicas lagging.

**Files:**
- Create or modify: `go/multiorch/recovery/analysis/generator_test.go`

**Step 1: Add tests**

```go
func TestGenerateAnalyses_EmptyStore(t *testing.T) {
	s := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	gen := NewAnalysisGenerator(s)
	analyses := gen.GenerateAnalyses()
	assert.Empty(t, analyses)
}

func TestGenerateAnalyses_SkipsNilEntries(t *testing.T) {
	s := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	s.Set("nil-pooler", nil)
	s.Set("valid-pooler", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         &clustermetadatapb.ID{Uid: "valid"},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "shard1",
		},
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		IsLastCheckValid: true,
	})
	gen := NewAnalysisGenerator(s)
	analyses := gen.GenerateAnalyses()
	assert.Len(t, analyses, 1)
}
```

**Step 2: Run tests**

Run: `go test ./go/multiorch/recovery/analysis/... -v -run TestGenerateAnalyses`
Expected: PASS

**Step 3: Commit**

```bash
git add go/multiorch/recovery/analysis/generator_test.go
git commit -m "test: add unit tests for generator edge cases"
```

---

## Task 7: Add Unit Tests for populatePrimaryInfo Edge Cases

**Context:** Test no primary exists, primary postgres down.

**Files:**
- Modify: `go/multiorch/recovery/analysis/generator_test.go`

**Step 1: Add tests**

```go
func TestPopulatePrimaryInfo_NoPrimaryInShard(t *testing.T) {
	s := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	replicaID := "db1-tg1-shard1-replica"
	s.Set(replicaID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         &clustermetadatapb.ID{Uid: "replica"},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "shard1",
		},
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		IsLastCheckValid: true,
		ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
			LastReplayLsn: "0/1234",
		},
	})

	gen := NewAnalysisGenerator(s)
	analysis, err := gen.GenerateAnalysisForPooler(replicaID)
	require.NoError(t, err)
	assert.Nil(t, analysis.PrimaryPoolerID)
	assert.False(t, analysis.PrimaryReachable)
}

func TestPopulatePrimaryInfo_PrimaryPostgresDown(t *testing.T) {
	s := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	primaryID := "db1-tg1-shard1-primary"
	replicaID := "db1-tg1-shard1-replica"

	s.Set(primaryID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       &clustermetadatapb.ID{Uid: "primary"},
			Database: "db1", TableGroup: "tg1", Shard: "shard1",
		},
		PoolerType:        clustermetadatapb.PoolerType_PRIMARY,
		IsLastCheckValid:  true,
		IsPostgresRunning: false, // Postgres is down!
		PrimaryStatus:     &multipoolermanagerdata.PrimaryStatus{Lsn: "0/1234"},
	})
	s.Set(replicaID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       &clustermetadatapb.ID{Uid: "replica"},
			Database: "db1", TableGroup: "tg1", Shard: "shard1",
		},
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		IsLastCheckValid: true,
		ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
			LastReplayLsn: "0/1234",
		},
	})

	gen := NewAnalysisGenerator(s)
	analysis, err := gen.GenerateAnalysisForPooler(replicaID)
	require.NoError(t, err)
	assert.NotNil(t, analysis.PrimaryPoolerID)
	assert.False(t, analysis.PrimaryReachable, "primary should NOT be reachable when postgres is down")
}
```

**Step 2: Run the tests**

Run: `go test ./go/multiorch/recovery/analysis/... -v -run TestPopulatePrimaryInfo`
Expected: PASS

**Step 3: Commit**

```bash
git add go/multiorch/recovery/analysis/generator_test.go
git commit -m "test: add unit tests for populatePrimaryInfo edge cases"
```

---

## Task 8: Clean Up Unused finalLSN Variable

**Context:** In rpc_initialization.go, finalLSN is extracted but never used.

**Files:**
- Modify: `go/multipooler/manager/rpc_initialization.go:190-198`

**Step 1: Simplify the code**

Replace:
```go
	finalLSN := ""
	if len(backups) > 0 && backups[0].FinalLsn != "" {
		finalLSN = backups[0].FinalLsn
		pm.logger.InfoContext(ctx, "Backup LSN captured", "backup_id", backups[0].BackupId, "final_lsn", finalLSN)
	} else {
		pm.logger.WarnContext(ctx, "No LSN available from backup metadata")
	}
	// TODO: do something with finalLSN?
```

With:
```go
	if len(backups) > 0 {
		pm.logger.InfoContext(ctx, "Using backup for restore",
			"backup_id", backups[0].BackupId,
			"final_lsn", backups[0].FinalLsn)
	}
```

**Step 2: Run tests**

Run: `go test ./go/multipooler/manager/... -v`
Expected: PASS

**Step 3: Commit**

```bash
git add go/multipooler/manager/rpc_initialization.go
git commit -m "cleanup: remove unused finalLSN variable and TODO"
```

---

## Summary

| Task | Description | Priority |
|------|-------------|----------|
| 1 | Add LockShardForRecovery helper to topo | High |
| 2 | Add LockTimeout to RecoveryMetadata, update actions | High |
| 3 | Remove confusing shard locking TODO | High |
| 4 | Add port comparison in primary detection | High |
| 5 | Add unit tests for IsInitialized | Medium |
| 6 | Add unit tests for generator edge cases | Medium |
| 7 | Add unit tests for populatePrimaryInfo | Medium |
| 8 | Clean up unused finalLSN variable | Low |

**Estimated total:** 8 tasks
