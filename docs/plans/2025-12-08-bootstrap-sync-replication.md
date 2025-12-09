# Bootstrap Synchronous Replication Fix

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Configure `synchronous_standby_names` during shard bootstrap to match the durability policy requirements.

**Architecture:** Extract `buildSyncReplicationConfig` from Coordinator to a shared function, then call it from BootstrapShardAction after standbys are initialized to configure synchronous replication on the primary.

**Tech Stack:** Go, gRPC, PostgreSQL synchronous replication

---

## Background

Currently, `BootstrapShardAction` creates a durability policy (e.g., ANY_2) in the database but does NOT configure `synchronous_standby_names`. This means writes succeed without standby acknowledgment until the first leader re-election, violating the durability guarantee.

`AppointLeaderAction` correctly configures sync replication via `Coordinator.Propagate -> Promote` with `SyncReplicationConfig`.

## Tasks

### Task 1: Export BuildSyncReplicationConfig from Coordinator

**Files:**
- Modify: `go/multiorch/coordinator/leader_appointment.go:316-434`

**Step 1: Rename buildSyncReplicationConfig to BuildSyncReplicationConfig**

Change the function signature from private to exported:

```go
// BuildSyncReplicationConfig creates synchronous replication configuration based on the quorum policy.
// Returns nil if synchronous replication should not be configured (required_count=1 or no standbys).
// For MULTI_CELL_ANY_N policies, excludes standbys in the same cell as the candidate (primary).
func BuildSyncReplicationConfig(logger *slog.Logger, quorumRule *clustermetadatapb.QuorumRule, standbys []*multiorchdatapb.PoolerHealthState, candidate *multiorchdatapb.PoolerHealthState) (*multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest, error) {
```

**Step 2: Update the function body to use the passed logger**

Replace all `c.logger` calls with `logger` parameter calls.

**Step 3: Inline filterStandbysByCell logic**

The helper `filterStandbysByCell` is simple enough to inline. Replace the call to `c.filterStandbysByCell(candidate, standbys)` with inline logic:

```go
// For MULTI_CELL_ANY_N, filter out standbys in the same cell as the primary
eligibleStandbys := standbys
if quorumRule.QuorumType == clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N {
    candidateCell := candidate.MultiPooler.Id.Cell
    filtered := make([]*multiorchdatapb.PoolerHealthState, 0, len(standbys))
    for _, standby := range standbys {
        if standby.MultiPooler.Id.Cell != candidateCell {
            filtered = append(filtered, standby)
        }
    }
    eligibleStandbys = filtered
    // ... rest of logging and validation
}
```

**Step 4: Update Coordinator.Propagate to call the exported function**

In `Propagate`, change:
```go
syncConfig, err := c.buildSyncReplicationConfig(quorumRule, standbys, candidate)
```
to:
```go
syncConfig, err := BuildSyncReplicationConfig(c.logger, quorumRule, standbys, candidate)
```

**Step 5: Run tests to verify refactor didn't break anything**

Run: `go test -v -run "TestBuildSyncReplicationConfig" ./go/multiorch/coordinator/...`
Expected: All tests pass

**Step 6: Commit**

```bash
git add go/multiorch/coordinator/leader_appointment.go
git commit -m "refactor(coordinator): export BuildSyncReplicationConfig for reuse"
```

---

### Task 2: Delete the now-unused filterStandbysByCell method

**Files:**
- Modify: `go/multiorch/coordinator/leader_appointment.go:421-434`

**Step 1: Remove the filterStandbysByCell method**

Delete the entire method since logic is now inlined:

```go
// DELETE THIS ENTIRE FUNCTION:
// filterStandbysByCell returns standbys that are NOT in the same cell as the candidate.
// Used for MULTI_CELL_ANY_N to ensure synchronous replication spans multiple cells.
func (c *Coordinator) filterStandbysByCell(candidate *multiorchdatapb.PoolerHealthState, standbys []*multiorchdatapb.PoolerHealthState) []*multiorchdatapb.PoolerHealthState {
    // ...
}
```

**Step 2: Run tests**

Run: `go test -v ./go/multiorch/coordinator/...`
Expected: All tests pass

**Step 3: Commit**

```bash
git add go/multiorch/coordinator/leader_appointment.go
git commit -m "refactor(coordinator): remove unused filterStandbysByCell method"
```

---

### Task 3: Add synchronous replication configuration to BootstrapShardAction

**Files:**
- Modify: `go/multiorch/recovery/actions/bootstrap_shard.go`

**Step 1: Add import for coordinator package**

Add to imports:
```go
"github.com/multigres/multigres/go/multiorch/coordinator"
```

**Step 2: Add configureSynchronousReplication method**

Add after the `initializeSingleStandby` method:

```go
// configureSynchronousReplication configures synchronous replication on the primary
// based on the durability policy and available standbys.
func (a *BootstrapShardAction) configureSynchronousReplication(
    ctx context.Context,
    primary *multiorchdatapb.PoolerHealthState,
    standbys []*multiorchdatapb.PoolerHealthState,
    quorumRule *clustermetadatapb.QuorumRule,
) error {
    // Build sync replication config using shared function
    syncConfig, err := coordinator.BuildSyncReplicationConfig(a.logger, quorumRule, standbys, primary)
    if err != nil {
        return mterrors.Wrap(err, "failed to build sync replication config")
    }

    // If syncConfig is nil, sync replication is not needed (e.g., required_count=1)
    if syncConfig == nil {
        a.logger.InfoContext(ctx, "Skipping synchronous replication configuration",
            "reason", "not required by policy")
        return nil
    }

    // Configure synchronous replication on the primary
    _, err = a.rpcClient.ConfigureSynchronousReplication(ctx, primary.MultiPooler, syncConfig)
    if err != nil {
        return mterrors.Wrap(err, "failed to configure synchronous replication")
    }

    a.logger.InfoContext(ctx, "Configured synchronous replication",
        "primary", primary.MultiPooler.Id.Name,
        "num_sync", syncConfig.NumSync,
        "standby_count", len(syncConfig.StandbyIds))

    return nil
}
```

**Step 3: Call configureSynchronousReplication after standbys are initialized**

In the `Execute` method, after the `initializeStandbys` call (around line 208-213), add:

```go
// Initialize remaining nodes as standbys
standbys := make([]*multiorchdatapb.PoolerHealthState, 0, len(cohort)-1)
for _, pooler := range cohort {
    if pooler.MultiPooler.Id.Name != candidate.MultiPooler.Id.Name {
        standbys = append(standbys, pooler)
    }
}

if err := a.initializeStandbys(ctx, problem.ShardKey, candidate, standbys, resp.BackupId); err != nil {
    // Log but don't fail - we have a primary at least
    a.logger.WarnContext(ctx, "failed to initialize some standbys",
        "shard_key", problem.ShardKey.String(),
        "error", err)
}

// Configure synchronous replication on primary based on durability policy
// This must be done AFTER standbys are initialized so they can receive WAL
if err := a.configureSynchronousReplication(ctx, candidate, standbys, quorumRule); err != nil {
    // Log but don't fail - standbys can catch up async, and next healthcheck will fix this
    a.logger.WarnContext(ctx, "failed to configure synchronous replication",
        "shard_key", problem.ShardKey.String(),
        "error", err)
}
```

**Step 4: Build to verify compilation**

Run: `go build ./go/multiorch/recovery/actions/...`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add go/multiorch/recovery/actions/bootstrap_shard.go
git commit -m "feat(bootstrap): configure synchronous replication after standby init"
```

---

### Task 4: Write unit test for BootstrapShardAction sync replication

**Files:**
- Modify: `go/multiorch/recovery/actions/bootstrap_shard_test.go`

**Step 1: Add test for configureSynchronousReplication being called**

Add a new test case that verifies sync replication is configured after bootstrap:

```go
func TestBootstrapShardAction_ConfiguresSyncReplication(t *testing.T) {
    // Setup mock RPC client
    mockClient := &rpcclient.FakeClient{}

    // Configure mock to track ConfigureSynchronousReplication calls
    var syncReplicationCalled bool
    var syncReplicationRequest *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest
    mockClient.ConfigureSynchronousReplicationFunc = func(
        ctx context.Context,
        pooler *clustermetadatapb.MultiPooler,
        req *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest,
    ) (*multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse, error) {
        syncReplicationCalled = true
        syncReplicationRequest = req
        return &multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse{}, nil
    }

    // ... setup rest of test (pooler store, topo store, etc.)

    // Execute bootstrap action
    // ...

    // Verify sync replication was configured
    assert.True(t, syncReplicationCalled, "ConfigureSynchronousReplication should be called")
    assert.NotNil(t, syncReplicationRequest)
    assert.Equal(t, int32(1), syncReplicationRequest.NumSync, "ANY_2 policy requires 1 sync standby")
}
```

**Step 2: Run tests**

Run: `go test -v -run "TestBootstrapShardAction" ./go/multiorch/recovery/actions/...`
Expected: Tests pass

**Step 3: Commit**

```bash
git add go/multiorch/recovery/actions/bootstrap_shard_test.go
git commit -m "test(bootstrap): verify sync replication configured during bootstrap"
```

---

### Task 5: Add e2e test for bootstrap sync replication

**Files:**
- Create: `go/test/endtoend/bootstrap_sync_replication_test.go`

**Step 1: Write e2e test that bootstraps a shard and verifies sync replication**

```go
// TestBootstrapConfiguresSyncReplication verifies that after bootstrap completes,
// the primary has synchronous_standby_names configured according to the durability policy.
func TestBootstrapConfiguresSyncReplication(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping e2e test in short mode")
    }
    if utils.ShouldSkipRealPostgres() {
        t.Skip("skipping test without postgres")
    }

    // Setup 3-node cluster with ANY_2 durability policy
    env := setupMultiOrchTestEnv(t, testEnvConfig{
        tempDirPrefix:    "bsrt*",
        cellName:         "test-cell",
        database:         "postgres",
        shardID:          "sync-repl-test",
        tableGroup:       "test",
        durabilityPolicy: "ANY_2",
        stanzaName:       "sync-repl-test",
    })

    nodes := env.createNodes(3)
    env.setupPgBackRest()
    env.registerNodes()

    // Start multiorch and wait for bootstrap
    multiOrchCmd := env.startMultiOrch()
    defer terminateProcess(t, multiOrchCmd, "multiorch", 5*time.Second)

    primaryNode := waitForShardPrimary(t, nodes, 90*time.Second)
    require.NotNil(t, primaryNode, "should have a primary after bootstrap")

    // Wait for standbys
    waitForStandbysInitialized(t, nodes, primaryNode.name, 2, 60*time.Second)

    // Verify synchronous_standby_names is configured on primary
    t.Run("verifies_sync_standby_names_configured", func(t *testing.T) {
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
```

**Step 2: Run test**

Run: `go test -v -run "TestBootstrapConfiguresSyncReplication" ./go/test/endtoend/...`
Expected: Test passes (may need to iterate on timing)

**Step 3: Commit**

```bash
git add go/test/endtoend/bootstrap_sync_replication_test.go
git commit -m "test(e2e): verify bootstrap configures synchronous replication"
```

---

### Task 6: Run full test suite and fix any issues

**Step 1: Run multiorch tests**

Run: `go test -v ./go/multiorch/...`
Expected: All pass

**Step 2: Run coordinator tests**

Run: `go test -v ./go/multiorch/coordinator/...`
Expected: All pass

**Step 3: Run bootstrap action tests**

Run: `go test -v ./go/multiorch/recovery/actions/...`
Expected: All pass

**Step 4: Final commit if any fixes needed**

```bash
git add -A
git commit -m "fix: address test failures from bootstrap sync replication changes"
```

---

## Verification

After implementation, verify:

1. **Unit tests pass**: `go test ./go/multiorch/...`
2. **Bootstrap configures sync replication**: New e2e test passes
3. **AppointLeader still works**: Existing leader appointment tests pass
4. **No regression**: Full test suite passes

## Notes

- The `configureSynchronousReplication` call is best-effort during bootstrap. If it fails, standbys still initialize and the next healthcheck cycle will detect the misconfiguration.
- For policies like `ANY_1` (if supported), sync replication is skipped since only the primary is needed.
- The `BuildSyncReplicationConfig` function handles MULTI_CELL_ANY_N by filtering standbys in the same cell as primary.
