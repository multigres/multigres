# BeginTerm and Bootstrap Improvements Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Improve BeginTerm correctness (reject lagging standbys, single file write, reset replication) and reduce RPC calls in bootstrap.

**Architecture:** Four independent improvements: (1) Standbys reject BeginTerm if too far behind, (2) Combine term update and candidate acceptance into single file write, (3) Standbys reset replication when accepting new term, (4) Push ChangeType and CreateDurabilityPolicy into InitializeEmptyPrimary to reduce RPC overhead.

**Tech Stack:** Go, protobuf, sqlmock for testing

---

## Background

### Issue 1: Standby replication lag check is too lenient
Current code at `rpc_consensus.go:154-157`:
```go
if err != nil {
    // No WAL receiver (could be disconnected standby)
    // Don't reject the acceptance - let it proceed
}
```
This allows disconnected standbys to accept terms, which is incorrect.

### Issue 2: Two file writes in BeginTerm
Currently BeginTerm writes to the consensus file twice:
1. `pm.validateAndUpdateTerm()` → calls `cs.UpdateTermAndSave()`
2. `cs.AcceptCandidateAndSave()`

This is inefficient and non-atomic.

### Issue 3: Standbys don't reset replication
The TODO at line 35-38 notes standbys should break replication when accepting a new term.

### Issue 4: Bootstrap makes 3 RPC calls
`BootstrapShardAction.Execute()` makes 3 separate RPCs to the same multipooler:
1. `InitializeEmptyPrimary`
2. `ChangeType`
3. `CreateDurabilityPolicy`

These can be consolidated.

---

## Task 1: Reject BeginTerm on standbys that are too far behind

**Files:**
- Modify: `go/multipooler/manager/rpc_consensus.go:152-165`
- Test: `go/multipooler/manager/rpc_consensus_test.go`

**Step 1: Write failing test for disconnected standby rejection**

Add test case to `TestBeginTerm`:

```go
{
    name: "StandbyRejectsTermWhenNoWALReceiver",
    initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
        TermNumber: 5,
    },
    requestTerm: 10,
    requestCandidate: &clustermetadatapb.ID{
        Component: clustermetadatapb.ID_MULTIPOOLER,
        Cell:      "zone1",
        Name:      "new-candidate",
    },
    setupMocks: func(mock sqlmock.Sqlmock) {
        mock.ExpectPing()
        // isPrimary check - returns true (in recovery = standby)
        mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
            WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
        // WAL receiver query returns no rows (disconnected standby)
        mock.ExpectQuery("SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").
            WillReturnError(sql.ErrNoRows)
    },
    expectedAccepted:                    false,
    expectedTerm:                        10, // Term is updated
    expectedAcceptedTermFromCoordinator: "",
    description:                         "Standby should reject term when no WAL receiver (disconnected)",
},
```

**Step 2: Run test to verify it fails**

Run: `go test -v -run "TestBeginTerm/StandbyRejectsTermWhenNoWALReceiver" ./go/multipooler/manager/...`

Expected: FAIL (current code proceeds when no WAL receiver)

**Step 3: Update BeginTerm to reject disconnected standbys**

Modify lines 152-165 in `rpc_consensus.go`:

```go
// Check if we're caught up with replication (within 30 seconds)
// Only relevant for standbys - primaries don't have WAL receivers
if !wasPrimary {
    var lastMsgReceiptTime *time.Time
    err = pm.db.QueryRowContext(ctx, "SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").Scan(&lastMsgReceiptTime)
    if err != nil {
        // No WAL receiver (disconnected standby) - reject the term
        pm.logger.WarnContext(ctx, "Rejecting term acceptance: standby has no WAL receiver",
            "term", req.Term,
            "error", err)
        return response, nil
    }
    if lastMsgReceiptTime != nil {
        timeSinceLastMessage := time.Since(*lastMsgReceiptTime)
        if timeSinceLastMessage > 30*time.Second {
            // We're too far behind in replication, don't accept
            pm.logger.WarnContext(ctx, "Rejecting term acceptance: standby too far behind",
                "term", req.Term,
                "last_msg_receipt_time", lastMsgReceiptTime,
                "time_since_last_message", timeSinceLastMessage)
            return response, nil
        }
    }
}
```

**Step 4: Run test to verify it passes**

Run: `go test -v -run "TestBeginTerm/StandbyRejectsTermWhenNoWALReceiver" ./go/multipooler/manager/...`

Expected: PASS

**Step 5: Run all BeginTerm tests**

Run: `go test -v -run "TestBeginTerm" ./go/multipooler/manager/...`

Expected: All PASS

**Step 6: Commit**

```bash
git add go/multipooler/manager/rpc_consensus.go go/multipooler/manager/rpc_consensus_test.go
git commit -m "fix(consensus): reject BeginTerm on standbys with no WAL receiver"
```

---

## Task 2: Combine term update and candidate acceptance into single file write

**Files:**
- Modify: `go/multipooler/manager/consensus_state.go`
- Modify: `go/multipooler/manager/rpc_consensus.go`
- Test: `go/multipooler/manager/rpc_consensus_test.go`

**Step 1: Add UpdateTermAndAcceptCandidate method to ConsensusState**

Add to `consensus_state.go` after `AcceptCandidateAndSave`:

```go
// UpdateTermAndAcceptCandidate atomically updates the term and accepts a candidate in one file write.
// This is used by BeginTerm to avoid two separate file writes.
// If newTerm > currentTerm, updates term and resets acceptance, then sets the candidate.
// If newTerm == currentTerm, just accepts the candidate (same as AcceptCandidateAndSave).
// Returns error if newTerm < currentTerm.
func (cs *ConsensusState) UpdateTermAndAcceptCandidate(ctx context.Context, newTerm int64, candidateID *clustermetadatapb.ID) error {
    if err := AssertActionLockHeld(ctx); err != nil {
        return err
    }
    cs.mu.Lock()
    defer cs.mu.Unlock()

    if candidateID == nil {
        return fmt.Errorf("candidate ID cannot be nil")
    }

    currentTerm := int64(0)
    if cs.term != nil {
        currentTerm = cs.term.GetTermNumber()
    }

    if newTerm < currentTerm {
        return fmt.Errorf("cannot update to older term: current=%d, new=%d", currentTerm, newTerm)
    }

    var newTermProto *multipoolermanagerdatapb.ConsensusTerm

    if newTerm > currentTerm {
        // Higher term: create new term with the candidate already set
        newTermProto = &multipoolermanagerdatapb.ConsensusTerm{
            TermNumber:                    newTerm,
            AcceptedTermFromCoordinatorId: proto.Clone(candidateID).(*clustermetadatapb.ID),
            LastAcceptanceTime:            timestamppb.New(time.Now()),
            LeaderId:                      nil,
        }
    } else {
        // Same term: just update acceptance (idempotent check first)
        if cs.term == nil {
            return fmt.Errorf("consensus term not initialized")
        }

        // If already accepted from this coordinator, idempotent success
        if cs.term.AcceptedTermFromCoordinatorId != nil && proto.Equal(cs.term.AcceptedTermFromCoordinatorId, candidateID) {
            return nil
        }

        // Check if already accepted from someone else in this term
        if cs.term.AcceptedTermFromCoordinatorId != nil {
            return fmt.Errorf("already accepted term from %s in term %d",
                cs.term.AcceptedTermFromCoordinatorId.GetName(), cs.term.TermNumber)
        }

        // Prepare acceptance
        newTermProto = cloneTerm(cs.term)
        newTermProto.AcceptedTermFromCoordinatorId = proto.Clone(candidateID).(*clustermetadatapb.ID)
        newTermProto.LastAcceptanceTime = timestamppb.New(time.Now())
    }

    // Single file write
    return cs.saveAndUpdateLocked(newTermProto)
}
```

**Step 2: Write test for UpdateTermAndAcceptCandidate**

Add to `rpc_consensus_test.go`:

```go
func TestUpdateTermAndAcceptCandidate(t *testing.T) {
    tests := []struct {
        name           string
        initialTerm    int64
        initialAccept  *clustermetadatapb.ID
        newTerm        int64
        candidateID    *clustermetadatapb.ID
        expectError    bool
        expectedTerm   int64
        expectedAccept string
    }{
        {
            name:        "higher term updates and accepts atomically",
            initialTerm: 5,
            newTerm:     10,
            candidateID: &clustermetadatapb.ID{
                Component: clustermetadatapb.ID_MULTIPOOLER,
                Cell:      "zone1",
                Name:      "candidate-a",
            },
            expectError:    false,
            expectedTerm:   10,
            expectedAccept: "candidate-a",
        },
        {
            name:        "same term accepts candidate",
            initialTerm: 5,
            newTerm:     5,
            candidateID: &clustermetadatapb.ID{
                Component: clustermetadatapb.ID_MULTIPOOLER,
                Cell:      "zone1",
                Name:      "candidate-b",
            },
            expectError:    false,
            expectedTerm:   5,
            expectedAccept: "candidate-b",
        },
        {
            name:        "lower term rejected",
            initialTerm: 10,
            newTerm:     5,
            candidateID: &clustermetadatapb.ID{
                Component: clustermetadatapb.ID_MULTIPOOLER,
                Cell:      "zone1",
                Name:      "candidate-c",
            },
            expectError: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            poolerDir := t.TempDir()
            serviceID := &clustermetadatapb.ID{
                Component: clustermetadatapb.ID_MULTIPOOLER,
                Cell:      "test-cell",
                Name:      "test-pooler",
            }

            cs := NewConsensusState(poolerDir, serviceID)
            _, err := cs.Load()
            require.NoError(t, err)

            // Set initial term
            ctx := context.Background()
            ctx = setActionLockHeld(ctx)
            if tt.initialTerm > 0 {
                err = cs.UpdateTermAndSave(ctx, tt.initialTerm)
                require.NoError(t, err)
            }

            // Call UpdateTermAndAcceptCandidate
            err = cs.UpdateTermAndAcceptCandidate(ctx, tt.newTerm, tt.candidateID)

            if tt.expectError {
                require.Error(t, err)
                return
            }

            require.NoError(t, err)
            term, err := cs.GetTerm(ctx)
            require.NoError(t, err)
            assert.Equal(t, tt.expectedTerm, term.TermNumber)
            assert.Equal(t, tt.expectedAccept, term.AcceptedTermFromCoordinatorId.GetName())
        })
    }
}
```

**Step 3: Run test**

Run: `go test -v -run "TestUpdateTermAndAcceptCandidate" ./go/multipooler/manager/...`

Expected: PASS

**Step 4: Update BeginTerm to use single atomic write**

Replace the two separate writes in `rpc_consensus.go` with a single call:

In the primary demotion branch (lines 129-133), replace:
```go
// Demotion succeeded - now update term and accept
if err := pm.validateAndUpdateTerm(ctx, req.Term, false); err != nil {
    return nil, fmt.Errorf("failed to update term after demotion: %w", err)
}
response.Term = req.Term
response.DemoteLsn = demoteLSN.LsnPosition
```

With:
```go
// Demotion succeeded - update term atomically with acceptance below
response.Term = req.Term
response.DemoteLsn = demoteLSN.LsnPosition
```

In the else branch (lines 143-148), replace:
```go
// Update term if needed (only if req.Term > currentTerm)
if req.Term > currentTerm {
    if err := pm.validateAndUpdateTerm(ctx, req.Term, false); err != nil {
        return nil, fmt.Errorf("failed to update term: %w", err)
    }
    response.Term = req.Term
}
```

With:
```go
// Update response term if higher (actual update happens atomically below)
if req.Term > currentTerm {
    response.Term = req.Term
}
```

Replace the final acceptance block (lines 168-171):
```go
// Accept term from coordinator atomically (checks, saves to disk, updates memory)
if err := cs.AcceptCandidateAndSave(ctx, req.CandidateId); err != nil {
    return nil, fmt.Errorf("failed to accept term from coordinator: %w", err)
}
```

With:
```go
// Atomically update term AND accept candidate in single file write
if err := cs.UpdateTermAndAcceptCandidate(ctx, req.Term, req.CandidateId); err != nil {
    return nil, fmt.Errorf("failed to update term and accept candidate: %w", err)
}
```

**Step 5: Run all BeginTerm tests**

Run: `go test -v -run "TestBeginTerm" ./go/multipooler/manager/...`

Expected: All PASS

**Step 6: Commit**

```bash
git add go/multipooler/manager/consensus_state.go go/multipooler/manager/rpc_consensus.go go/multipooler/manager/rpc_consensus_test.go
git commit -m "refactor(consensus): combine term update and acceptance into single file write"
```

---

## Task 3: Standbys reset replication when accepting new term

**Files:**
- Modify: `go/multipooler/manager/rpc_consensus.go`
- Test: `go/multipooler/manager/rpc_consensus_test.go`

**Step 1: Add replication pause when standby accepts new term**

Add logic after the standby replication lag check and before acceptance. Modify the else branch in `rpc_consensus.go`:

After the replication lag check (before the acceptance), add:

```go
// Standby accepting new term: pause replication to break connection with old primary
// This ensures we don't continue replicating from a demoted primary
if req.Term > currentTerm {
    pm.logger.InfoContext(ctx, "Standby accepting new term, pausing replication",
        "term", req.Term,
        "current_term", currentTerm)

    // Pause replication - this will stop the WAL receiver
    _, pauseErr := pm.pauseReplication(ctx, multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_FULLY, false)
    if pauseErr != nil {
        // Log but don't fail - the new primary will reconnect us
        pm.logger.WarnContext(ctx, "Failed to pause replication during term acceptance",
            "error", pauseErr)
    }
}
```

**Step 2: Update the BeginTerm documentation**

Replace lines 28-38 in `rpc_consensus.go`:

```go
// BeginTerm handles coordinator requests during leader appointments.
// Accepting a new term means this node will not accept any new requests from
// the current term. This is the revocation step of consensus, which applies
// to both primaries and standbys:
//
//   - If this node is a primary and receives a higher term, it MUST demote
//     itself before accepting. If demotion fails, the term is rejected.
//   - If this node is a standby and receives a higher term, it pauses
//     replication to break the connection with the old primary. The new
//     primary will reconfigure replication via SetPrimaryConnInfo.
```

**Step 3: Write test for standby replication pause**

Add test case to `TestBeginTerm`:

```go
{
    name: "StandbyPausesReplicationWhenAcceptingNewTerm",
    initialTerm: &multipoolermanagerdatapb.ConsensusTerm{
        TermNumber: 5,
    },
    requestTerm: 10,
    requestCandidate: &clustermetadatapb.ID{
        Component: clustermetadatapb.ID_MULTIPOOLER,
        Cell:      "zone1",
        Name:      "new-candidate",
    },
    setupMocks: func(mock sqlmock.Sqlmock) {
        mock.ExpectPing()
        // isPrimary check - standby
        mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
            WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
        // WAL receiver check - connected and caught up
        mock.ExpectQuery("SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").
            WillReturnRows(sqlmock.NewRows([]string{"last_msg_receipt_time"}).AddRow(time.Now().Add(-5 * time.Second)))
        // pauseReplication check - verify we're still in recovery
        mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
            WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))
        // pauseReplication - ALTER SYSTEM
        mock.ExpectExec("ALTER SYSTEM SET primary_conninfo").
            WillReturnResult(sqlmock.NewResult(0, 0))
        // pauseReplication - pg_reload_conf
        mock.ExpectQuery("SELECT pg_reload_conf\\(\\)").
            WillReturnRows(sqlmock.NewRows([]string{"pg_reload_conf"}).AddRow(true))
    },
    expectedAccepted:                    true,
    expectedTerm:                        10,
    expectedAcceptedTermFromCoordinator: "new-candidate",
    description:                         "Standby should pause replication when accepting new term",
},
```

**Step 4: Run test**

Run: `go test -v -run "TestBeginTerm/StandbyPausesReplicationWhenAcceptingNewTerm" ./go/multipooler/manager/...`

Expected: PASS (after implementation)

**Step 5: Run all BeginTerm tests**

Run: `go test -v -run "TestBeginTerm" ./go/multipooler/manager/...`

Expected: All PASS

**Step 6: Commit**

```bash
git add go/multipooler/manager/rpc_consensus.go go/multipooler/manager/rpc_consensus_test.go
git commit -m "feat(consensus): standby pauses replication when accepting new term"
```

---

## Task 4: Consolidate RPC calls in BootstrapShardAction

**Files:**
- Modify: `go/multipooler/manager/rpc_initialization.go`
- Modify: `go/multiorch/recovery/actions/bootstrap_shard.go`
- Test: `go/multiorch/recovery/actions/bootstrap_shard_test.go`
- Proto: `proto/multipoolermanagerdata.proto` (if needed)

**Step 1: Add internal methods for ChangeType and CreateDurabilityPolicy**

Add to `rpc_manager.go` (after ChangeType):

```go
// changeTypeLocked updates the pooler type without acquiring the action lock.
// The caller MUST already hold the action lock.
func (pm *MultiPoolerManager) changeTypeLocked(ctx context.Context, poolerType clustermetadatapb.PoolerType) error {
    if err := AssertActionLockHeld(ctx); err != nil {
        return err
    }

    pm.logger.InfoContext(ctx, "changeTypeLocked called", "pooler_type", poolerType.String(), "service_id", pm.serviceID.String())

    // Update the multipooler record in topology
    updatedMultipooler, err := pm.topoClient.UpdateMultiPoolerFields(ctx, pm.serviceID, func(mp *clustermetadatapb.MultiPooler) error {
        mp.Type = poolerType
        return nil
    })
    if err != nil {
        pm.logger.ErrorContext(ctx, "Failed to update pooler type in topology", "error", err, "service_id", pm.serviceID.String())
        return mterrors.Wrap(err, "failed to update pooler type in topology")
    }

    pm.mu.Lock()
    pm.multipooler.MultiPooler = updatedMultipooler
    pm.updateCachedMultipooler()
    pm.mu.Unlock()

    // Update heartbeat tracker based on new type
    if pm.replTracker != nil {
        if poolerType == clustermetadatapb.PoolerType_PRIMARY {
            pm.logger.InfoContext(ctx, "Starting heartbeat writer for new primary")
            pm.replTracker.MakePrimary()
        } else {
            pm.logger.InfoContext(ctx, "Stopping heartbeat writer for replica")
            pm.replTracker.MakeNonPrimary()
        }
    }

    pm.logger.InfoContext(ctx, "Pooler type updated successfully", "new_type", poolerType.String(), "service_id", pm.serviceID.String())
    return nil
}

// createDurabilityPolicyLocked creates a durability policy without acquiring the action lock.
// The caller MUST already hold the action lock.
func (pm *MultiPoolerManager) createDurabilityPolicyLocked(ctx context.Context, policyName string, quorumRule *clustermetadatapb.QuorumRule) error {
    if err := AssertActionLockHeld(ctx); err != nil {
        return err
    }

    pm.logger.InfoContext(ctx, "createDurabilityPolicyLocked called", "policy_name", policyName)

    // Validate inputs
    if policyName == "" {
        return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "policy_name is required")
    }

    if quorumRule == nil {
        return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "quorum_rule is required")
    }

    // Check that we have a database connection
    if pm.db == nil {
        return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "database connection not available")
    }

    // Marshal the quorum rule to JSON using protojson
    marshaler := protojson.MarshalOptions{
        UseEnumNumbers: true,
    }
    quorumRuleJSON, err := marshaler.Marshal(quorumRule)
    if err != nil {
        return mterrors.Wrapf(err, "failed to marshal quorum rule")
    }

    // Insert or update the policy in the database
    query := `
        INSERT INTO multigres.durability_policy (policy_name, quorum_rule)
        VALUES ($1, $2)
        ON CONFLICT (policy_name) DO UPDATE SET quorum_rule = EXCLUDED.quorum_rule
    `
    _, err = pm.db.ExecContext(ctx, query, policyName, string(quorumRuleJSON))
    if err != nil {
        return mterrors.Wrapf(err, "failed to insert durability policy")
    }

    pm.logger.InfoContext(ctx, "Durability policy created/updated successfully", "policy_name", policyName)
    return nil
}
```

**Step 2: Update InitializeEmptyPrimary proto to include new fields**

Add to `proto/multipoolermanagerdata.proto` in `InitializeEmptyPrimaryRequest`:

```protobuf
message InitializeEmptyPrimaryRequest {
  int64 consensus_term = 1;
  // Optional: Set pooler type after initialization (defaults to PRIMARY if not specified)
  clustermetadata.PoolerType pooler_type = 2;
  // Optional: Create durability policy after initialization
  string durability_policy_name = 3;
  clustermetadata.QuorumRule durability_quorum_rule = 4;
}
```

Run: `make proto`

**Step 3: Update InitializeEmptyPrimary to handle ChangeType and CreateDurabilityPolicy**

Add at the end of `InitializeEmptyPrimary` in `rpc_initialization.go`, before the success return:

```go
// Set pooler type if requested (defaults to PRIMARY)
poolerType := req.PoolerType
if poolerType == clustermetadatapb.PoolerType_UNKNOWN {
    poolerType = clustermetadatapb.PoolerType_PRIMARY
}
if err := pm.changeTypeLocked(ctx, poolerType); err != nil {
    return nil, mterrors.Wrap(err, "failed to set pooler type")
}

// Create durability policy if requested
if req.DurabilityPolicyName != "" && req.DurabilityQuorumRule != nil {
    if err := pm.createDurabilityPolicyLocked(ctx, req.DurabilityPolicyName, req.DurabilityQuorumRule); err != nil {
        return nil, mterrors.Wrap(err, "failed to create durability policy")
    }
    pm.logger.InfoContext(ctx, "Created durability policy", "policy_name", req.DurabilityPolicyName)
}
```

**Step 4: Update BootstrapShardAction to use consolidated RPC**

Modify `bootstrap_shard.go` Execute function. Replace lines 147-199:

```go
// Initialize the candidate as an empty primary with term=1
// This now also sets the pooler type and creates the durability policy
req := &multipoolermanagerdatapb.InitializeEmptyPrimaryRequest{
    ConsensusTerm:         1,
    PoolerType:            clustermetadatapb.PoolerType_PRIMARY,
    DurabilityPolicyName:  policyName,
    DurabilityQuorumRule:  quorumRule,
}
resp, err := a.rpcClient.InitializeEmptyPrimary(ctx, candidate.MultiPooler, req)
if err != nil {
    return mterrors.Wrap(err, "failed to initialize empty primary")
}

if !resp.Success {
    return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
        "failed to initialize empty primary on node %s: %s",
        candidate.MultiPooler.Id.Name, resp.ErrorMessage)
}

a.logger.InfoContext(ctx, "successfully initialized primary with durability policy",
    "shard_key", problem.ShardKey.String(),
    "primary", candidate.MultiPooler.Id.Name,
    "backup_id", resp.BackupId,
    "policy_name", policyName)
```

Remove the separate ChangeType and CreateDurabilityPolicy RPC calls (the old lines 167-199).

**Step 5: Run bootstrap tests**

Run: `go test -v ./go/multiorch/recovery/actions/... -run "Bootstrap"`

Expected: All PASS

**Step 6: Run end-to-end tests**

Run: `go test -v ./go/test/endtoend/... -run "Bootstrap"`

Expected: All PASS

**Step 7: Commit**

```bash
git add proto/multipoolermanagerdata.proto go/multipooler/manager/rpc_initialization.go go/multipooler/manager/rpc_manager.go go/multiorch/recovery/actions/bootstrap_shard.go
git commit -m "refactor(bootstrap): consolidate ChangeType and CreateDurabilityPolicy into InitializeEmptyPrimary"
```

---

## Task 5: Run full test suite and verify no regressions

**Step 1: Run multipooler manager tests**

Run: `go test -v ./go/multipooler/manager/...`

Expected: All PASS

**Step 2: Run multipooler package tests**

Run: `go test -v ./go/multipooler/...`

Expected: All PASS

**Step 3: Run multiorch tests**

Run: `go test -v ./go/multiorch/...`

Expected: All PASS

**Step 4: Build to verify compilation**

Run: `make build`

Expected: Success, no errors

---

## Summary

After completing all tasks:

1. **Standbys reject BeginTerm if disconnected** - No WAL receiver = rejection
2. **Single file write in BeginTerm** - `UpdateTermAndAcceptCandidate` combines both operations atomically
3. **Standbys reset replication** - Pause replication when accepting new term to break connection with old primary
4. **Reduced RPC overhead** - Bootstrap makes 1 RPC instead of 3 by consolidating ChangeType and CreateDurabilityPolicy into InitializeEmptyPrimary
