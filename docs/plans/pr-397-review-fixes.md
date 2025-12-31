# PR #397 Review Fixes Implementation Plan

## Summary

Address review comments from @rafael on the leadership_history PR.

---

## Task 1: Add ClusterID.String() helper to topoclient

**File:** `go/common/topoclient/id.go`

Add a helper method to generate string representation of cluster IDs:

```go
// String returns a string representation of the cluster ID.
// Format: "cell_name" for multipooler IDs.
func (id *clustermetadatapb.ID) String() string {
    if id == nil {
        return ""
    }
    return fmt.Sprintf("%s_%s", id.Cell, id.Name)
}
```

Note: Since `clustermetadatapb.ID` is a generated protobuf type, we can't add methods directly. Instead, add a helper function:

```go
// ClusterIDString returns a string representation of the cluster ID.
func ClusterIDString(id *clustermetadatapb.ID) string {
    if id == nil {
        return ""
    }
    return fmt.Sprintf("%s_%s", id.Cell, id.Name)
}
```

---

## Task 2: Validate coordinator in NewRecoveryActionFactory constructor

**File:** `go/multiorch/recovery/analysis/action_factory.go`

Change the constructor to require a non-nil coordinator and validate upfront:

```go
func NewRecoveryActionFactory(
    poolerStore *store.PoolerHealthStore,
    rpcClient rpcclient.MultiPoolerClient,
    topoStore topoclient.Store,
    coordinator *coordinator.Coordinator,
    logger *slog.Logger,
) *RecoveryActionFactory {
    if coordinator == nil {
        panic("coordinator cannot be nil")
    }
    return &RecoveryActionFactory{
        // ...
    }
}
```

Then update `NewBootstrapShardAction` to use `GetCoordinatorID()` directly without nil checks.

---

## Task 3: Pass Coordinator to BootstrapShardAction (like AppointLeaderAction)

**Files:**
- `go/multiorch/recovery/actions/bootstrap_shard.go`
- `go/multiorch/recovery/analysis/action_factory.go`

Change `NewBootstrapShardAction` signature to accept `*coordinator.Coordinator` instead of `coordinatorID string`:

```go
func NewBootstrapShardAction(
    rpcClient rpcclient.MultiPoolerClient,
    poolerStore *store.PoolerHealthStore,
    topoStore topoclient.Store,
    coordinator *coordinator.Coordinator,  // Changed from coordinatorID string
    logger *slog.Logger,
) *BootstrapShardAction
```

Update `Execute` to get the coordinator ID when needed using `GetCoordinatorID()`.

---

## Task 4: Add comment explaining cohort vs recruited difference

**File:** `go/multiorch/coordinator/coordinator.go`

Add a comment at line ~98 explaining when cohort differs from recruited:

```go
// Reconstruct the recruited list (nodes that accepted the term).
// This is candidate + standbys.
//
// The recruited list may differ from the original cohort in these scenarios:
// - Some nodes in the cohort were unreachable during BeginTerm
// - Some nodes rejected the term (e.g., had a higher term already)
// - Some nodes failed validation (e.g., insufficient LSN)
recruited := make([]*multiorchdatapb.PoolerHealthState, 0, len(standbys)+1)
```

---

## Task 5: Always log leadership history (even with nil values)

**File:** `go/multipooler/manager/rpc_manager.go`

Change the condition at line ~1008 to always insert leadership history, using "unknown" for missing values:

```go
// Write leadership history record - always record for audit trail
if reason == "" {
    reason = "unknown"
}
if coordinatorID == "" {
    coordinatorID = "unknown"
}
if err := pm.insertLeadershipHistory(ctx, consensusTerm, leaderID, coordinatorID, finalLSN, reason, cohortMembers, acceptedMembers); err != nil {
    // Log but don't fail the promotion - history is for audit, not correctness
    pm.logger.WarnContext(ctx, "Failed to insert leadership history",
        "term", consensusTerm,
        "error", err)
}
```

---

## Task 6: Add unit test for insertLeadershipHistory error path

**File:** `go/multipooler/manager/rpc_initialization_test.go` (or new file)

Add a test that verifies `insertLeadershipHistory` failures don't fail the parent operation:

```go
func TestPromote_LeadershipHistoryErrorDoesNotFailPromotion(t *testing.T) {
    // Setup: create manager with a mock that fails insertLeadershipHistory
    // Execute: call Promote with valid parameters
    // Verify: Promote succeeds despite history insertion failure
}
```

---

## Task 7: Reduce timeouts and consolidate wait helpers

**Files:**
- `go/test/endtoend/multiorch/bootstrap_test.go`
- `go/test/endtoend/multiorch/multiorch_helpers.go`

### 7a. Reduce timeouts
Change from 60s/30s to more reasonable values (e.g., 30s/15s):

```go
waitForStandbysInitialized(t, setup, 2, 30*time.Second)
waitForSyncReplicationConfigured(t, setup, 15*time.Second)
```

### 7b. Consolidate waitForSyncReplicationConfigured into waitForStandbysInitialized

Modify `waitForStandbysInitialized` to also check that sync replication is configured before returning:

```go
func waitForStandbysInitialized(t *testing.T, setup *shardsetup.ShardSetup, expectedCount int, timeout time.Duration) {
    // ... existing logic to wait for standbys ...

    // Also wait for sync replication to be configured on primary
    // (initialization includes wiring up replication)
    waitForSyncReplicationConfigured(t, setup, timeout/2)
}
```

Or inline the sync replication check at the end of `waitForStandbysInitialized`.

---

## Task Order

1. Task 1: Add ClusterIDString helper (foundation for other tasks)
2. Task 2: Validate coordinator in factory constructor
3. Task 3: Pass Coordinator to BootstrapShardAction
4. Task 4: Add cohort vs recruited comment
5. Task 5: Always log leadership history
6. Task 6: Add unit test for error path
7. Task 7: Reduce timeouts and consolidate helpers

---

## Files to Modify

| File | Tasks |
|------|-------|
| `go/common/topoclient/id.go` | Task 1 |
| `go/multiorch/recovery/analysis/action_factory.go` | Tasks 2, 3 |
| `go/multiorch/recovery/actions/bootstrap_shard.go` | Task 3 |
| `go/multiorch/coordinator/coordinator.go` | Task 4 |
| `go/multiorch/coordinator/leader_appointment.go` | Task 1 (use new helper) |
| `go/multipooler/manager/rpc_manager.go` | Task 5 |
| `go/multipooler/manager/rpc_initialization_test.go` | Task 6 |
| `go/test/endtoend/multiorch/bootstrap_test.go` | Task 7 |
| `go/test/endtoend/multiorch/multiorch_helpers.go` | Task 7 |
