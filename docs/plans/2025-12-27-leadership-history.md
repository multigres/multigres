# Leadership History Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Track leadership history in a database table for audit/compliance, operational queries, and debugging purposes.

**Architecture:** Add `leadership_history` table to the multigres schema, extend `PromoteRequest` proto with election metadata fields, and write the history record in the multipooler's `Promote` method after successful promotion. The coordinator passes election context (reason, cohort, accepted members) to the promote call.

**Tech Stack:** Go, Protocol Buffers, PostgreSQL, gRPC

---

## Task 1: Add leadership_history table creation

**Files:**
- Modify: `go/multipooler/manager/pg_multischema.go`

**Step 1: Add createLeadershipHistoryTable method**

Add after `createDurabilityPolicyTable` (around line 174):

```go
// createLeadershipHistoryTable creates the leadership_history table and its indexes
func (pm *MultiPoolerManager) createLeadershipHistoryTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.exec(execCtx, `CREATE TABLE IF NOT EXISTS multigres.leadership_history (
		id BIGSERIAL PRIMARY KEY,
		term_number BIGINT NOT NULL,
		leader_id TEXT NOT NULL,
		coordinator_id TEXT NOT NULL,
		wal_position TEXT NOT NULL,
		reason TEXT NOT NULL,
		cohort_members JSONB NOT NULL,
		accepted_members JSONB NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create leadership_history table")
	}

	execCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.exec(execCtx, `CREATE INDEX IF NOT EXISTS idx_leadership_history_term
		ON multigres.leadership_history(term_number DESC)`); err != nil {
		return mterrors.Wrap(err, "failed to create leadership_history index")
	}

	return nil
}
```

**Step 2: Call createLeadershipHistoryTable in createSidecarSchema**

Modify `createSidecarSchema` to add the call after `createDurabilityPolicyTable`:

```go
func (pm *MultiPoolerManager) createSidecarSchema(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "Creating multigres sidecar schema")

	if err := pm.createSchema(ctx); err != nil {
		return err
	}

	if err := pm.createHeartbeatTable(ctx); err != nil {
		return err
	}

	if err := pm.createDurabilityPolicyTable(ctx); err != nil {
		return err
	}

	if err := pm.createLeadershipHistoryTable(ctx); err != nil {
		return err
	}

	// Create multischema global tables for the default tablegroup
	// ... rest of function unchanged
```

**Step 3: Run build to verify compilation**

Run: `go build ./go/multipooler/...`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add go/multipooler/manager/pg_multischema.go
git commit -m "feat(multipooler): add leadership_history table creation"
```

---

## Task 2: Add unit tests for leadership_history table creation

**Files:**
- Modify: `go/multipooler/manager/pg_multischema_test.go`

**Step 1: Update TestCreateSidecarSchema to include leadership_history**

Update the "successful schema creation for default tablegroup" test case to include the new table:

```go
{
	name:       "successful schema creation for default tablegroup",
	tableGroup: constants.DefaultTableGroup,
	setupMock: func(m *mock.QueryService) {
		m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
		m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
		m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.durability_policy", mock.MakeQueryResult(nil, nil))
		m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS idx_durability_policy_active", mock.MakeQueryResult(nil, nil))
		m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.leadership_history", mock.MakeQueryResult(nil, nil))
		m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS idx_leadership_history_term", mock.MakeQueryResult(nil, nil))
		m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup", mock.MakeQueryResult(nil, nil))
		m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup_table", mock.MakeQueryResult(nil, nil))
		m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.shard", mock.MakeQueryResult(nil, nil))
	},
	expectError: false,
},
```

**Step 2: Update all other test cases that mock schema creation**

Update the other test cases in TestCreateSidecarSchema to include leadership_history mocks where appropriate (for tests that get past durability_policy).

**Step 3: Add test case for leadership_history table creation failure**

Add new test case:

```go
{
	name:       "leadership_history table creation fails",
	tableGroup: constants.DefaultTableGroup,
	setupMock: func(m *mock.QueryService) {
		m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
		m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
		m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.durability_policy", mock.MakeQueryResult(nil, nil))
		m.AddQueryPatternOnce("CREATE INDEX IF NOT EXISTS idx_durability_policy_active", mock.MakeQueryResult(nil, nil))
		m.AddQueryPatternOnceWithError("CREATE TABLE IF NOT EXISTS multigres.leadership_history", fmt.Errorf("table creation failed"))
	},
	expectError:   true,
	errorContains: "failed to create leadership_history table",
},
```

**Step 4: Run tests to verify they pass**

Run: `go test -v -run TestCreateSidecarSchema ./go/multipooler/manager/...`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add go/multipooler/manager/pg_multischema_test.go
git commit -m "test(multipooler): add leadership_history table creation tests"
```

---

## Task 3: Extend PromoteRequest proto with election metadata

**Files:**
- Modify: `proto/multipoolermanagerdata.proto`

**Step 1: Add new fields to PromoteRequest**

Find `message PromoteRequest` (around line 362) and add new fields:

```protobuf
message PromoteRequest {
  // Consensus term for this promotion operation
  // Used to ensure this promotion corresponds to the correct term
  int64 consensus_term = 1;

  // Expected LSN position before promotion (optional, for validation)
  // By the Propagate stage, replication should already be stopped and the LSN frozen.
  // This is an assertion to verify the pooler has the expected durable state.
  // If the actual LSN doesn't match, this indicates an error in an earlier consensus stage.
  // If empty, skip LSN validation.
  string expected_lsn = 2;

  // Synchronous replication configuration to apply after promotion
  // This rewires the cohort for the new topology
  ConfigureSynchronousReplicationRequest sync_replication_config = 3;

  // Force the operation even if term validation fails
  // Should only be used in recovery scenarios
  bool force = 4;

  // Reason for the election (e.g., "dead_primary", "manual_failover", "bootstrap")
  string reason = 5;

  // Coordinator ID that ran this election
  string coordinator_id = 6;

  // List of pooler names that were in the cohort
  repeated string cohort_members = 7;

  // List of pooler names that accepted the term during BeginTerm
  repeated string accepted_members = 8;
}
```

**Step 2: Regenerate Go code**

Run: `make proto`
Expected: Proto generation succeeds

**Step 3: Verify generated code**

Run: `go build ./go/pb/...`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add proto/multipoolermanagerdata.proto go/pb/multipoolermanagerdata/multipoolermanagerdata.pb.go
git commit -m "feat(proto): add election metadata fields to PromoteRequest"
```

---

## Task 4: Add insertLeadershipHistory method

**Files:**
- Modify: `go/multipooler/manager/pg_multischema.go`

**Step 1: Add insertLeadershipHistory method**

Add after `insertDurabilityPolicy` (around line 294):

```go
// insertLeadershipHistory inserts a leadership history record into the leadership_history table.
func (pm *MultiPoolerManager) insertLeadershipHistory(ctx context.Context, termNumber int64, leaderID, coordinatorID, walPosition, reason string, cohortMembers, acceptedMembers []string) error {
	pm.logger.InfoContext(ctx, "Inserting leadership history",
		"term", termNumber,
		"leader", leaderID,
		"coordinator", coordinatorID,
		"reason", reason)

	cohortJSON, err := json.Marshal(cohortMembers)
	if err != nil {
		return mterrors.Wrap(err, "failed to marshal cohort_members")
	}

	acceptedJSON, err := json.Marshal(acceptedMembers)
	if err != nil {
		return mterrors.Wrap(err, "failed to marshal accepted_members")
	}

	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err = pm.execArgs(execCtx, `INSERT INTO multigres.leadership_history
		(term_number, leader_id, coordinator_id, wal_position, reason, cohort_members, accepted_members)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb)`,
		termNumber, leaderID, coordinatorID, walPosition, reason, cohortJSON, acceptedJSON)
	if err != nil {
		return mterrors.Wrap(err, "failed to insert leadership history")
	}

	pm.logger.InfoContext(ctx, "Successfully inserted leadership history", "term", termNumber)
	return nil
}
```

**Step 2: Add json import if not present**

Add to imports at top of file:

```go
import (
	"context"
	"encoding/json"
	"time"
	// ... existing imports
)
```

**Step 3: Run build to verify compilation**

Run: `go build ./go/multipooler/...`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add go/multipooler/manager/pg_multischema.go
git commit -m "feat(multipooler): add insertLeadershipHistory method"
```

---

## Task 5: Add unit tests for insertLeadershipHistory

**Files:**
- Modify: `go/multipooler/manager/pg_multischema_test.go`

**Step 1: Add TestInsertLeadershipHistory**

Add new test function after `TestInsertDurabilityPolicy`:

```go
func TestInsertLeadershipHistory(t *testing.T) {
	tests := []struct {
		name            string
		termNumber      int64
		leaderID        string
		coordinatorID   string
		walPosition     string
		reason          string
		cohortMembers   []string
		acceptedMembers []string
		setupMock       func(m *mock.QueryService)
		expectError     bool
		errorContains   string
	}{
		{
			name:            "successful insert",
			termNumber:      5,
			leaderID:        "pooler-1",
			coordinatorID:   "multiorch-1",
			walPosition:     "0/1234567",
			reason:          "dead_primary",
			cohortMembers:   []string{"pooler-1", "pooler-2", "pooler-3"},
			acceptedMembers: []string{"pooler-1", "pooler-2"},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.leadership_history", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:            "insert fails with db error",
			termNumber:      10,
			leaderID:        "pooler-2",
			coordinatorID:   "multiorch-2",
			walPosition:     "0/ABCDEF0",
			reason:          "bootstrap",
			cohortMembers:   []string{"pooler-2"},
			acceptedMembers: []string{"pooler-2"},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.leadership_history", fmt.Errorf("connection refused"))
			},
			expectError:   true,
			errorContains: "failed to insert leadership history",
		},
		{
			name:            "insert with empty cohort",
			termNumber:      1,
			leaderID:        "pooler-1",
			coordinatorID:   "multiorch-1",
			walPosition:     "0/0",
			reason:          "manual_failover",
			cohortMembers:   []string{},
			acceptedMembers: []string{},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.leadership_history", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := pm.insertLeadershipHistory(ctx, tt.termNumber, tt.leaderID, tt.coordinatorID, tt.walPosition, tt.reason, tt.cohortMembers, tt.acceptedMembers)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}
```

**Step 2: Run tests to verify they pass**

Run: `go test -v -run TestInsertLeadershipHistory ./go/multipooler/manager/...`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add go/multipooler/manager/pg_multischema_test.go
git commit -m "test(multipooler): add insertLeadershipHistory tests"
```

---

## Task 6: Update Promote method to write leadership history

**Files:**
- Modify: `go/multipooler/manager/rpc_manager.go`

**Step 1: Update Promote method signature**

Find the `Promote` method (around line 916) and update to accept additional parameters:

```go
func (pm *MultiPoolerManager) Promote(ctx context.Context, consensusTerm int64, expectedLSN string, syncReplicationConfig *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest, force bool, reason string, coordinatorID string, cohortMembers []string, acceptedMembers []string) (*multipoolermanagerdatapb.PromoteResponse, error) {
```

**Step 2: Add leadership history write before returning success**

Find the comment `// TODO: Populate consensus metadata tables.` (around line 999) and replace with:

```go
	// Write leadership history record
	leaderID := pm.getServiceIDString()
	if reason != "" && coordinatorID != "" {
		if err := pm.insertLeadershipHistory(ctx, consensusTerm, leaderID, coordinatorID, finalLSN, reason, cohortMembers, acceptedMembers); err != nil {
			// Log but don't fail the promotion - history is for audit, not correctness
			pm.logger.WarnContext(ctx, "Failed to insert leadership history",
				"term", consensusTerm,
				"error", err)
		}
	}
```

**Step 3: Add getServiceIDString helper method if needed**

Check if `getServiceIDString` exists. If not, add it:

```go
// getServiceIDString returns the service ID as a string in the format "cell_name"
func (pm *MultiPoolerManager) getServiceIDString() string {
	if pm.config.ServiceID == nil {
		return "unknown"
	}
	return fmt.Sprintf("%s_%s", pm.config.ServiceID.Cell, pm.config.ServiceID.Name)
}
```

**Step 4: Run build to verify compilation**

Run: `go build ./go/multipooler/...`
Expected: Build succeeds (will have compile errors in callers that need updating)

**Step 5: Commit**

```bash
git add go/multipooler/manager/rpc_manager.go
git commit -m "feat(multipooler): write leadership history in Promote"
```

---

## Task 7: Update grpcmanagerservice to pass new fields

**Files:**
- Modify: `go/multipooler/grpcmanagerservice/service.go`

**Step 1: Update Promote handler to pass new fields**

Find the `Promote` method (around line 226) and update:

```go
func (s *managerService) Promote(ctx context.Context, req *multipoolermanagerdatapb.PromoteRequest) (*multipoolermanagerdatapb.PromoteResponse, error) {
	resp, err := s.manager.Promote(ctx,
		req.ConsensusTerm,
		req.ExpectedLsn,
		req.SyncReplicationConfig,
		req.Force,
		req.Reason,
		req.CoordinatorId,
		req.CohortMembers,
		req.AcceptedMembers)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}
```

**Step 2: Run build to verify compilation**

Run: `go build ./go/multipooler/...`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add go/multipooler/grpcmanagerservice/service.go
git commit -m "feat(grpcservice): pass election metadata to Promote"
```

---

## Task 8: Update coordinator to populate new PromoteRequest fields

**Files:**
- Modify: `go/multiorch/coordinator/leader_appointment.go`

**Step 1: Update Propagate method to track and pass cohort/accepted info**

The Propagate method needs to receive the recruited nodes and pass them to Promote. Update the method signature and implementation:

First, update the `Propagate` signature to receive the full cohort:

```go
func (c *Coordinator) Propagate(ctx context.Context, candidate *multiorchdatapb.PoolerHealthState, standbys []*multiorchdatapb.PoolerHealthState, recruited []*multiorchdatapb.PoolerHealthState, term int64, quorumRule *clustermetadatapb.QuorumRule, reason string) error {
```

**Step 2: Update the PromoteRequest construction**

Find where `promoteReq` is constructed (around line 363) and update:

```go
	// Build cohort and accepted member lists
	cohortMembers := make([]string, 0, len(standbys)+1)
	cohortMembers = append(cohortMembers, candidate.MultiPooler.Id.Name)
	for _, s := range standbys {
		cohortMembers = append(cohortMembers, s.MultiPooler.Id.Name)
	}

	acceptedMembers := make([]string, 0, len(recruited))
	for _, r := range recruited {
		acceptedMembers = append(acceptedMembers, r.MultiPooler.Id.Name)
	}

	promoteReq := &multipoolermanagerdatapb.PromoteRequest{
		ConsensusTerm:         term,
		ExpectedLsn:           expectedLSN,
		SyncReplicationConfig: syncConfig,
		Force:                 false,
		Reason:                reason,
		CoordinatorId:         c.getCoordinatorIDString(),
		CohortMembers:         cohortMembers,
		AcceptedMembers:       acceptedMembers,
	}
```

**Step 3: Add getCoordinatorIDString helper method**

Add to coordinator.go:

```go
// getCoordinatorIDString returns the coordinator ID as a string
func (c *Coordinator) getCoordinatorIDString() string {
	if c.coordinatorID == nil {
		return "unknown"
	}
	return fmt.Sprintf("%s_%s", c.coordinatorID.Cell, c.coordinatorID.Name)
}
```

**Step 4: Update AppointLeader to pass reason to Propagate**

Update the call to Propagate in coordinator.go:

```go
	// Stage 6: Propagate (setup replication within shard)
	c.logger.InfoContext(ctx, "Stage 6: Propagating replication", "shard", shardID)
	if err := c.Propagate(ctx, candidate, standbys, recruited, term, quorumRule, "dead_primary"); err != nil {
		return mterrors.Wrap(err, "Propagate failed")
	}
```

Note: For now we hardcode "dead_primary" as the reason. This will be parameterized in a future task.

**Step 5: Update BeginTerm to return recruited nodes**

The BeginTerm already returns recruited nodes as part of the standbys calculation. Update the return to include full recruited list.

**Step 6: Run build to verify compilation**

Run: `go build ./go/multiorch/...`
Expected: Build succeeds

**Step 7: Commit**

```bash
git add go/multiorch/coordinator/leader_appointment.go go/multiorch/coordinator/coordinator.go
git commit -m "feat(coordinator): pass election metadata to Promote"
```

---

## Task 9: Update FakeClient and RPC client interface

**Files:**
- Modify: `go/common/rpcclient/fake_client.go`
- Modify: `go/common/rpcclient/client.go` (if interface changed)

**Step 1: Verify FakeClient.Promote accepts the new proto fields**

The FakeClient already accepts the full `PromoteRequest` proto, so it should automatically get the new fields. Verify this by checking the signature.

**Step 2: Run build to verify**

Run: `go build ./go/common/rpcclient/...`
Expected: Build succeeds

**Step 3: Commit (if changes needed)**

```bash
git add go/common/rpcclient/fake_client.go
git commit -m "chore(rpcclient): update FakeClient for new PromoteRequest fields"
```

---

## Task 10: Update existing Promote tests

**Files:**
- Modify: `go/multipooler/manager/rpc_manager_test.go`

**Step 1: Update test calls to Promote with new parameters**

Find all calls to `pm.Promote(ctx, ...)` and add the new parameters:

```go
// Before:
resp, err := pm.Promote(ctx, 10, "0/ABCDEF0", nil, false /* force */)

// After:
resp, err := pm.Promote(ctx, 10, "0/ABCDEF0", nil, false /* force */, "", "", nil, nil)
```

**Step 2: Run tests to verify they pass**

Run: `go test -v -run TestPromote ./go/multipooler/manager/...`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add go/multipooler/manager/rpc_manager_test.go
git commit -m "test(multipooler): update Promote tests for new parameters"
```

---

## Task 11: Update coordinator tests

**Files:**
- Modify: `go/multiorch/coordinator/leader_appointment_test.go`

**Step 1: Update test calls to Propagate with new parameters**

Find all calls to `c.Propagate(...)` and update the signature.

**Step 2: Run tests to verify they pass**

Run: `go test -v ./go/multiorch/coordinator/...`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add go/multiorch/coordinator/leader_appointment_test.go
git commit -m "test(coordinator): update Propagate tests for new parameters"
```

---

## Task 12: Run full test suite

**Step 1: Run unit tests**

Run: `go test ./go/multipooler/manager/...`
Expected: All PASS

**Step 2: Run coordinator tests**

Run: `go test ./go/multiorch/...`
Expected: All PASS

**Step 3: Run short tests**

Run: `make test-short`
Expected: All PASS

**Step 4: Run e2e bootstrap test**

Run: `PATH=/bin:/usr/bin:/Users/deepthi/.gvm/gos/go1.25.0/bin:/Applications/Postgres.app/Contents/Versions/18/bin:/Users/deepthi/github/multigres/bin:$PATH MTROOT=/Users/deepthi/github/multigres go test -v -run TestBootstrapInitialization ./go/test/endtoend/multiorch/... -timeout 5m`
Expected: PASS

**Step 5: Commit any remaining fixes**

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Add leadership_history table creation | `go/multipooler/manager/pg_multischema.go` |
| 2 | Add unit tests for table creation | `go/multipooler/manager/pg_multischema_test.go` |
| 3 | Extend PromoteRequest proto | `proto/multipoolermanagerdata.proto` |
| 4 | Add insertLeadershipHistory method | `go/multipooler/manager/pg_multischema.go` |
| 5 | Add unit tests for insert method | `go/multipooler/manager/pg_multischema_test.go` |
| 6 | Update Promote to write history | `go/multipooler/manager/rpc_manager.go` |
| 7 | Update grpcmanagerservice | `go/multipooler/grpcmanagerservice/service.go` |
| 8 | Update coordinator to pass metadata | `go/multiorch/coordinator/leader_appointment.go` |
| 9 | Update FakeClient | `go/common/rpcclient/fake_client.go` |
| 10 | Update Promote tests | `go/multipooler/manager/rpc_manager_test.go` |
| 11 | Update coordinator tests | `go/multiorch/coordinator/leader_appointment_test.go` |
| 12 | Run full test suite | - |
