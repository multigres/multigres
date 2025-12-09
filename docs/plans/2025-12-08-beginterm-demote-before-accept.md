# BeginTerm: Demote Before Accept Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ensure BeginTerm only accepts a new term if demotion succeeds when the node is a primary.

**Architecture:** Move demotion logic before `AcceptCandidateAndSave` call. If node is primary and receiving a higher term, attempt demotion first. Only accept the term if demotion succeeds. This prevents split-brain by ensuring a primary cannot accept a term it cannot safely demote from.

**Tech Stack:** Go, protobuf, sqlmock for testing

---

## Background

Current flow in `BeginTerm` (lines 130-162 of `rpc_consensus.go`):
1. Accept term via `AcceptCandidateAndSave`
2. If was primary and higher term, attempt demotion
3. If demotion fails, log error but return `accepted=true`

**Problem:** If demotion fails, we've already accepted the term and can't undo it. This could lead to split-brain if the old primary continues accepting writes.

**Fix:** Demote FIRST, then accept only if demotion succeeds.

---

### Task 1: Add test for demotion failure rejecting term acceptance

**Files:**
- Modify: `go/multipooler/manager/rpc_consensus_test.go`

**Step 1: Write the failing test**

Add a new test case to `TestBeginTerm` that verifies when a primary fails to demote, it rejects the term:

```go
{
    name: "PrimaryRejectTermWhenDemotionFails",
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
        // isPrimary check - returns true (not in recovery = primary)
        mock.ExpectQuery("SELECT pg_is_in_recovery()").
            WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
        // checkPrimaryGuardrails - verifies still primary
        mock.ExpectQuery("SELECT pg_is_in_recovery()").
            WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
        // demoteLocked fails at setServingReadOnly or another early step
        // Simulate failure by not setting up expected queries for demotion steps
    },
    expectedAccepted:                    false,
    expectedTerm:                        5, // Term should NOT be updated
    expectedAcceptedTermFromCoordinator: "",
    description:                         "Primary should reject term when demotion fails",
},
```

**Step 2: Run test to verify it fails**

Run: `go test -v -run "TestBeginTerm/PrimaryRejectTermWhenDemotionFails" ./go/multipooler/manager/...`

Expected: FAIL (current code accepts term even when demotion fails)

**Step 3: Commit test**

```bash
git add go/multipooler/manager/rpc_consensus_test.go
git commit -m "test(consensus): add test for rejecting term when demotion fails"
```

---

### Task 2: Refactor BeginTerm to demote before accepting

**Files:**
- Modify: `go/multipooler/manager/rpc_consensus.go:38-165`

**Step 1: Restructure BeginTerm logic**

Replace the current implementation (lines 104-162) with:

```go
// If we're a primary accepting a higher term, we must demote FIRST.
// Only accept the term if demotion succeeds. This prevents split-brain
// by ensuring the old primary stops accepting writes before acknowledging
// the new term.
if wasPrimary && req.Term > currentTerm {
    pm.logger.InfoContext(ctx, "Primary receiving higher term, attempting demotion before acceptance",
        "current_term", currentTerm,
        "new_term", req.Term,
        "candidate_id", req.CandidateId.GetName())

    // Use a reasonable drain timeout for demotion
    drainTimeout := 5 * time.Second
    demoteLSN, demoteErr := pm.demoteLocked(ctx, req.Term, drainTimeout)
    if demoteErr != nil {
        // Demotion failed - do NOT accept the term
        // Return accepted=false so coordinator knows this node couldn't safely step down
        pm.logger.ErrorContext(ctx, "Demotion failed, rejecting term acceptance",
            "error", demoteErr,
            "term", req.Term)
        return response, nil
    }

    // Demotion succeeded - now update term and accept
    if err := pm.validateAndUpdateTerm(ctx, req.Term, false); err != nil {
        return nil, fmt.Errorf("failed to update term after demotion: %w", err)
    }
    response.Term = req.Term
    response.DemoteLsn = demoteLSN.LsnPosition

    pm.logger.InfoContext(ctx, "Demotion completed, accepting term",
        "demote_lsn", demoteLSN.LsnPosition,
        "term", req.Term)
} else {
    // Not a primary or same/lower term - handle normally

    // Update term if needed (only if req.Term > currentTerm)
    if req.Term > currentTerm {
        if err := pm.validateAndUpdateTerm(ctx, req.Term, false); err != nil {
            return nil, fmt.Errorf("failed to update term: %w", err)
        }
        response.Term = req.Term
    }

    // Check if we're caught up with replication (within 30 seconds)
    // Only relevant for standbys - primaries don't have WAL receivers
    if !wasPrimary {
        var lastMsgReceiptTime *time.Time
        err = pm.db.QueryRowContext(ctx, "SELECT last_msg_receipt_time FROM pg_stat_wal_receiver").Scan(&lastMsgReceiptTime)
        if err != nil {
            // No WAL receiver (could be disconnected standby)
            // Don't reject the acceptance - let it proceed
        } else if lastMsgReceiptTime != nil {
            timeSinceLastMessage := time.Since(*lastMsgReceiptTime)
            if timeSinceLastMessage > 30*time.Second {
                // We're too far behind in replication, don't accept
                return response, nil
            }
        }
    }
}

// Accept term from coordinator atomically (checks, saves to disk, updates memory)
if err := cs.AcceptCandidateAndSave(ctx, req.CandidateId); err != nil {
    return nil, fmt.Errorf("failed to accept term from coordinator: %w", err)
}

response.Accepted = true
return response, nil
```

**Step 2: Run the test to verify it passes**

Run: `go test -v -run "TestBeginTerm/PrimaryRejectTermWhenDemotionFails" ./go/multipooler/manager/...`

Expected: PASS

**Step 3: Run all BeginTerm tests**

Run: `go test -v -run "TestBeginTerm" ./go/multipooler/manager/...`

Expected: All tests PASS

**Step 4: Commit implementation**

```bash
git add go/multipooler/manager/rpc_consensus.go
git commit -m "fix(consensus): demote before accepting term in BeginTerm"
```

---

### Task 3: Update test for successful demotion case

**Files:**
- Modify: `go/multipooler/manager/rpc_consensus_test.go`

**Step 1: Add test for successful primary demotion**

Add test case that verifies a primary successfully demotes and accepts term:

```go
{
    name: "PrimaryAcceptsTermAfterSuccessfulDemotion",
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
        // isPrimary check
        mock.ExpectQuery("SELECT pg_is_in_recovery()").
            WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))
        // Setup all demotion mocks (checkPrimaryGuardrails, checkDemotionState, etc.)
        // This requires understanding the full demotion mock sequence
        setupDemotionMocks(mock)
    },
    expectedAccepted:                    true,
    expectedTerm:                        10,
    expectedAcceptedTermFromCoordinator: "new-candidate",
    description:                         "Primary should accept term after successful demotion",
},
```

Note: You'll need to create a helper `setupDemotionMocks(mock sqlmock.Sqlmock)` function that sets up all the SQL expectations for a successful demotion. Reference the existing demotion tests in `rpc_manager_test.go` for the required mock sequence.

**Step 2: Run test**

Run: `go test -v -run "TestBeginTerm/PrimaryAcceptsTermAfterSuccessfulDemotion" ./go/multipooler/manager/...`

Expected: PASS

**Step 3: Commit**

```bash
git add go/multipooler/manager/rpc_consensus_test.go
git commit -m "test(consensus): add test for successful primary demotion in BeginTerm"
```

---

### Task 4: Remove TODO comment and update function documentation

**Files:**
- Modify: `go/multipooler/manager/rpc_consensus.go`

**Step 1: Update the BeginTerm documentation**

Update the function comment (lines 28-37) to reflect the new behavior:

```go
// BeginTerm handles coordinator requests during leader appointments.
// Accepting a new term means this node will not accept any new requests from
// the current term. This is the revocation step of consensus, which applies
// to both primaries and standbys:
//
//   - If this node is a primary and receives a higher term, it MUST demote
//     itself before accepting. If demotion fails, the term is rejected.
//   - If this node is a standby, it should break replication as part of
//     revocation. However, breaking replication on all standbys before the primary
//     is demoted will have the effect of blocking writes to the primary,
//     so this has to be done at the proper time (TODO: not implemented yet).
```

**Step 2: Run all consensus tests**

Run: `go test -v ./go/multipooler/manager/... -run "Consensus|BeginTerm"`

Expected: All PASS

**Step 3: Commit**

```bash
git add go/multipooler/manager/rpc_consensus.go
git commit -m "docs(consensus): update BeginTerm documentation for demote-first behavior"
```

---

### Task 5: Run full test suite and verify no regressions

**Step 1: Run multipooler manager tests**

Run: `go test -v ./go/multipooler/manager/...`

Expected: All PASS

**Step 2: Run multipooler package tests**

Run: `go test -v ./go/multipooler/...`

Expected: All PASS

**Step 3: Build to verify compilation**

Run: `go build ./go/multipooler/...`

Expected: Success, no errors

---

## Summary

After completing all tasks:

1. **BeginTerm** now demotes a primary BEFORE accepting a higher term
2. If demotion fails, the term is rejected (`accepted=false`)
3. If demotion succeeds, the term is accepted with `demote_lsn` populated
4. Tests cover both success and failure scenarios
5. Documentation reflects the new behavior
