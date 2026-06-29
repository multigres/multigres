// Copyright 2026 Supabase, Inc.
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

package manager

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus/consensustest"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// newLeaderAddress builds a PoolerAddress suitable for use as the leader in a
// SetPrimaryRequest.
func newLeaderAddress(name, host string, port int32) *clustermetadatapb.PoolerAddress {
	return &clustermetadatapb.PoolerAddress{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      name,
		},
		Host:         host,
		PostgresPort: port,
	}
}

// ruleAtTermForLeader builds a ShardRule with the given coordinator_term and
// the leader_id matching the supplied leader. Used to satisfy SetPrimary's
// leader.id == rule.leader_id validation in tests.
func ruleAtTermForLeader(leader *clustermetadatapb.PoolerAddress, term int64) *clustermetadatapb.ShardRule {
	return &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
		LeaderId:   leader.GetId(),
	}
}

func TestSetPrimary_ValidationErrors(t *testing.T) {
	validLeader := newLeaderAddress("p1", "host", 5432)
	tests := []struct {
		name           string
		req            *consensusdatapb.SetPrimaryRequest
		expectErrMatch string
	}{
		{
			name:           "NilLeader",
			req:            &consensusdatapb.SetPrimaryRequest{ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{Rule: ruleAtTermForLeader(validLeader, 5)}},
			expectErrMatch: "replication_primary.primary is required",
		},
		{
			name:           "NilRule",
			req:            &consensusdatapb.SetPrimaryRequest{ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{Primary: validLeader}},
			expectErrMatch: "replication_primary.rule is required",
		},
		{
			name: "RuleMissingLeaderId",
			req: &consensusdatapb.SetPrimaryRequest{
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
					},
					Primary: validLeader,
				},
			},
			expectErrMatch: "rule.leader_id is required",
		},
		{
			name: "LeaderIdMismatchesRule",
			req: &consensusdatapb.SetPrimaryRequest{
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
						LeaderId: &clustermetadatapb.ID{
							Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "different",
						},
					},
					Primary: validLeader,
				},
			},
			expectErrMatch: "does not match rule.leader_id",
		},
		{
			name: "MissingHost",
			req: &consensusdatapb.SetPrimaryRequest{
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule:    ruleAtTermForLeader(validLeader, 5),
					Primary: &clustermetadatapb.PoolerAddress{Id: validLeader.GetId(), PostgresPort: 5432},
				},
			},
			expectErrMatch: "leader host is required",
		},
		{
			name: "MissingPostgresPort",
			req: &consensusdatapb.SetPrimaryRequest{
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule:    ruleAtTermForLeader(validLeader, 5),
					Primary: &clustermetadatapb.PoolerAddress{Id: validLeader.GetId(), Host: "host"},
				},
			},
			expectErrMatch: "has no postgres port configured",
		},
		{
			// SetPrimary is the follower-side RPC. If the coordinator routes
			// this call to the designated leader itself, reject with INVALID_ARGUMENT
			// and direct the caller to Promote, which carries the full
			// CoordinatorProposal a leader needs to safely promote.
			//
			// setupManagerWithMockDB creates a pooler whose serviceID names
			// "test-pooler" in cell "zone1", so building leader with the same ID
			// triggers the self-leader guard.
			name: "LeaderIsSelf",
			req: func() *consensusdatapb.SetPrimaryRequest {
				selfLeader := newLeaderAddress("test-pooler", "host", 5432)
				return &consensusdatapb.SetPrimaryRequest{
					ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
						Rule:    ruleAtTermForLeader(selfLeader, 5),
						Primary: selfLeader,
					},
				}
			}(),
			expectErrMatch: "leader is self; designated leaders are appointed via Promote",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueryService := mock.NewQueryService()
			pm, _ := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: makeRulePosition(5)})

			resp, err := pm.SetPrimary(t.Context(), tt.req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectErrMatch)
			assert.Nil(t, resp)
		})
	}
}

// TestSetPrimary_NoOpWhenPositionNotHigher verifies that SetPrimary returns success
// without touching primary_conninfo when the supplied position is equal to or
// lower than this pooler's own observed position. This is the staleness gate
// that keeps out-of-order recovery rounds from clobbering fresh state.
func TestSetPrimary_NoOpWhenPositionNotHigher(t *testing.T) {
	tests := []struct {
		name        string
		selfTerm    int64
		incomingPos *clustermetadatapb.PoolerPosition
	}{
		{
			name:        "EqualPosition",
			selfTerm:    7,
			incomingPos: makeRulePosition(7),
		},
		{
			name:        "LowerPosition",
			selfTerm:    7,
			incomingPos: makeRulePosition(3),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueryService := mock.NewQueryService()
			// The only postgres call SetPrimary must make in the no-op path is
			// ObservePosition, which is handled by the fakeRuleStore — no SQL
			// at all should hit the mock query service.
			pm, _ := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: makeRulePosition(tt.selfTerm)})

			leader := newLeaderAddress("new-primary", "primary-host", 5432)
			incomingRule := tt.incomingPos.GetRule()
			incomingRule.LeaderId = leader.GetId()
			req := &consensusdatapb.SetPrimaryRequest{
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule:    incomingRule,
					Primary: leader,
				},
			}
			resp, err := pm.SetPrimary(t.Context(), req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.ConsensusStatus)

			// (rule, primary) MUST be recorded even on no-op paths — that's
			// the substrate multiorch reads from the health stream to skip
			// future redundant SetPrimary calls. The proof that the apply
			// branch wasn't reached is that no apply-path postgres queries
			// were issued (ExpectationsWereMet below).
			highest := pm.consensusMgr.GetReplicationPrimary()
			require.NotNil(t, highest, "SetPrimary should record the rule even on no-op")
			assert.Equal(t, tt.incomingPos.GetRule().GetRuleNumber().GetCoordinatorTerm(),
				highest.GetRule().GetRuleNumber().GetCoordinatorTerm())
			require.NotNil(t, highest.GetPrimary(), "SetPrimary should record the primary even on no-op")
			assert.Equal(t, "new-primary", highest.GetPrimary().Id.Name)
			assert.Equal(t, "primary-host", highest.GetPrimary().GetHost())

			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

// TestSetPrimary_StandbyAppliesNewPrimary verifies the standby branch: when the
// receiver is in recovery and the incoming position is higher, SetPrimary issues
// the same setPrimaryConnInfo work (stop replication -> ALTER SYSTEM SET
// primary_conninfo -> reload -> start replication).
func TestSetPrimary_StandbyAppliesNewPrimary(t *testing.T) {
	mockQueryService := mock.NewQueryService()

	// SetPrimary's isPrimary check: returns true for "in recovery" -> standby.
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))

	// setPrimaryConnInfoLocked's own isPrimary guardrail.
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))

	// pauseReplication(REPLAY_ONLY): pg_wal_replay_pause + verify paused.
	mockQueryService.AddQueryPatternOnce("SELECT pg_wal_replay_pause",
		mock.MakeQueryResult(nil, nil))
	replayStateCols := []string{"replay_lsn", "is_paused"}
	mockQueryService.AddQueryPattern("^SELECT pg_last_wal_replay_lsn",
		mock.MakeQueryResult(replayStateCols, [][]any{{"0/100", true}}))

	// Capture the rendered primary_conninfo so we can assert host=primary-host.
	var capturedConnInfoSQL string
	mockQueryService.AddQueryPatternWithCallback(
		"ALTER SYSTEM SET primary_conninfo",
		mock.MakeQueryResult(nil, nil),
		func(sql string) { capturedConnInfoSQL = sql },
	)
	expectReloadConfig(mockQueryService)

	// startReplicationAfter=true: waitForDatabaseConnection + pg_wal_replay_resume.
	mockQueryService.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult(nil, nil))
	mockQueryService.AddQueryPatternOnce("SELECT pg_wal_replay_resume",
		mock.MakeQueryResult(nil, nil))

	pm, _ := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: makeRulePosition(3)})

	leader := newLeaderAddress("new-primary", "primary-host", 5432)
	req := &consensusdatapb.SetPrimaryRequest{
		ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
			Rule:    ruleAtTermForLeader(leader, 10),
			Primary: leader,
			// Leader is rewind-ready, so a stale-primary demote proceeds rather than
			// deferring the pg_rewind (no-op for the standby-update path).
			RewindReady: true,
		},
	}
	resp, err := pm.SetPrimary(t.Context(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ConsensusStatus)

	assert.Contains(t, capturedConnInfoSQL, "host=primary-host",
		"rendered primary_conninfo should reference the new primary host")

	recorded := pm.consensusMgr.GetReplicationPrimary().GetPrimary()
	require.NotNil(t, recorded, "primary should be recorded after standby update")
	assert.Equal(t, "new-primary", recorded.GetId().GetName())
	assert.Equal(t, "primary-host", recorded.GetHost())
}

// TestSetPrimary_StalePrimaryDemotes verifies the stale-primary branch end to end:
// when the receiver is acting as primary (pg_is_in_recovery=false) and the
// supplied position is higher, SetPrimary must drive a full demote — pg_rewind,
// restart as standby, set primary_conninfo at the new primary, flip topology
// to REPLICA, and update the gateway's leader observation. The revocation is
// intentionally NOT touched (SetPrimary is a notification, not a recruit;
// revocations are authored by coordinators via Recruit).
func TestSetPrimary_StalePrimaryDemotes(t *testing.T) {
	mockQueryService := mock.NewQueryService()

	// 1. SetPrimary's own isPrimary check: not in recovery -> take the demote branch.
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	// 2. After restart, every subsequent pg_is_in_recovery must report standby.
	// Covers isInRecovery (verify after restart) and setPrimaryConnInfoLocked's
	// guardrail. The post-demotion sync clear goes through the (fake) rule store's
	// ClearSyncStandby, asserted below, so it issues no SQL here.
	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))

	// 3. waitForDatabaseConnection polls SELECT 1 after the standby restart.
	mockQueryService.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult(nil, nil))

	// 4. setPrimaryConnInfoLocked(false, false): rewrite primary_conninfo + reload.
	var capturedConnInfoSQL string
	mockQueryService.AddQueryPatternWithCallback(
		"ALTER SYSTEM SET primary_conninfo",
		mock.MakeQueryResult(nil, nil),
		func(sql string) { capturedConnInfoSQL = sql },
	)
	expectReloadConfig(mockQueryService)

	// 6. getStandbyReplayLSN — best-effort LSN read after demote.
	mockQueryService.AddQueryPattern("SELECT pg_last_wal_replay_lsn",
		mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn"}, [][]any{{"0/2000"}}))

	ruleStore := &fakeRuleStore{pos: makeRulePosition(3)}
	pm, tmpDir := setupManagerWithMockDB(t, mockQueryService, ruleStore)

	// Seed an initial revocation so we can verify SetPrimary leaves it
	// untouched even when the incoming rule's coordinator_term is higher.
	consensustest.SeedTerm(t, tmpDir, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3})
	_, err := pm.consensusMgr.Promises().Load()
	require.NoError(t, err)

	leader := newLeaderAddress("new-primary", "primary-host", 5432)
	req := &consensusdatapb.SetPrimaryRequest{
		ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
			Rule:    ruleAtTermForLeader(leader, 10),
			Primary: leader,
			// Leader is rewind-ready, so a stale-primary demote proceeds rather than
			// deferring the pg_rewind (no-op for the standby-update path).
			RewindReady: true,
		},
	}
	resp, err := pm.SetPrimary(t.Context(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ConsensusStatus)

	// primary_conninfo points at the new primary.
	assert.Contains(t, capturedConnInfoSQL, "host=primary-host",
		"rendered primary_conninfo should reference the new primary host")

	// Manager state recorded the new primary.
	recorded := pm.consensusMgr.GetReplicationPrimary().GetPrimary()
	require.NotNil(t, recorded)
	assert.Equal(t, "new-primary", recorded.GetId().GetName())
	assert.Equal(t, "primary-host", recorded.GetHost())

	// SetPrimary must NOT touch term_revocation. The revocation seeded above
	// (revoked_below_term=3) is preserved verbatim. Revocations are authored
	// by coordinators via Recruit, not by side effects of SetPrimary.
	rev := pm.consensusMgr.Promises().GetInconsistentRevocation()
	assert.Equal(t, int64(3), rev.GetRevokedBelowTerm(),
		"SetPrimary must not bump revoked_below_term — that's a coordinator responsibility")

	// Demoting a stale primary clears synchronous_standby_names through the rule
	// store's SyncStandbyManager (keeping its cache coherent), not via a direct
	// ALTER SYSTEM that would bypass the cache.
	assert.True(t, ruleStore.clearSyncCalled,
		"stale-primary demotion should clear sync standby names via ClearSyncStandby")

	// Gateway leader observation should reflect the new primary.
	healthState := pm.healthStreamer.getState()
	require.NotNil(t, healthState.LeaderObservation)
	assert.Equal(t, "new-primary", healthState.LeaderObservation.LeaderID.Name)
	assert.Equal(t, int64(10), healthState.LeaderObservation.LeaderTerm)
}

// TestSetPrimary_IgnoresRevokedRule verifies that when the incoming rule is
// revoked by the pooler's recorded revocation (i.e. coordinator_term below
// revoked_below_term and the outgoing-rule override does not fire), SetPrimary
// returns success without touching postgres and without recording the rule.
// The override scenarios are unit-tested at the IsRuleRevoked predicate; this
// pins the RPC-level behavior.
func TestSetPrimary_IgnoresRevokedRule(t *testing.T) {
	tests := []struct {
		name         string
		revokedBelow int64
		outgoing     *clustermetadatapb.RuleNumber
		incomingTerm int64
	}{
		{
			name:         "BelowRevoked_NoOutgoing",
			revokedBelow: 5,
			outgoing:     nil,
			incomingTerm: 3,
		},
		{
			name:         "BelowRevoked_BelowOutgoing",
			revokedBelow: 5,
			outgoing:     &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
			incomingTerm: 2,
		},
		{
			name:         "BelowRevoked_EqualOutgoing",
			revokedBelow: 5,
			outgoing:     &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
			incomingTerm: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueryService := mock.NewQueryService()
			pm, tmpDir := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: makeRulePosition(0)})

			consensustest.SeedTerm(t, tmpDir, &clustermetadatapb.TermRevocation{
				RevokedBelowTerm: tt.revokedBelow,
				OutgoingRule:     tt.outgoing,
			})
			_, err := pm.consensusMgr.Promises().Load()
			require.NoError(t, err)

			leader := newLeaderAddress("new-primary", "primary-host", 5432)
			req := &consensusdatapb.SetPrimaryRequest{
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule:    ruleAtTermForLeader(leader, tt.incomingTerm),
					Primary: leader,
				},
			}
			resp, err := pm.SetPrimary(t.Context(), req)
			require.NoError(t, err, "SetPrimary should silently ignore revoked rules")
			require.NotNil(t, resp)
			require.NotNil(t, resp.ConsensusStatus)

			// RecordTermPrimary must not have run: the (rule, leader) tuple
			// is not the substrate multiorch should read from a refused FYI.
			// This is also the proof the apply branch wasn't reached.
			highest := pm.consensusMgr.GetReplicationPrimary()
			assert.Nil(t, highest, "revoked SetPrimary should not be recorded")

			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

// TestSetPrimary_AppliesViaOutgoingRuleOverride verifies that an SetPrimary whose
// rule term is below revoked_below_term is accepted when the rule strictly
// exceeds the revocation's recorded outgoing_rule — the runaway-recruit
// self-heal path. To avoid wiring full postgres mocks for the apply branch,
// the test sets self's rule term above the incoming rule so SetPrimary takes
// its "not higher, no-op" branch after the revocation check passes; the
// (rule, leader) tuple is still recorded by RecordTermPrimary, which is the
// observable proving the override fired.
func TestSetPrimary_AppliesViaOutgoingRuleOverride(t *testing.T) {
	mockQueryService := mock.NewQueryService()

	const selfRuleTerm = 10
	pm, tmpDir := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: makeRulePosition(selfRuleTerm)})

	// Revocation at term 5 with outgoing_rule at term 1; an incoming rule
	// at term 3 is below revoked_below_term but strictly above outgoing_rule.
	consensustest.SeedTerm(t, tmpDir, &clustermetadatapb.TermRevocation{
		RevokedBelowTerm: 5,
		OutgoingRule:     &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
	})
	_, err := pm.consensusMgr.Promises().Load()
	require.NoError(t, err)

	leader := newLeaderAddress("new-primary", "primary-host", 5432)
	req := &consensusdatapb.SetPrimaryRequest{
		ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
			Rule:    ruleAtTermForLeader(leader, 3),
			Primary: leader,
		},
	}
	resp, err := pm.SetPrimary(t.Context(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ConsensusStatus)

	// Override fired → RecordTermPrimary ran → the rule + primary are observable.
	highest := pm.consensusMgr.GetReplicationPrimary()
	require.NotNil(t, highest, "override should let RecordTermPrimary persist the rule")
	require.NotNil(t, highest.GetPrimary())
	assert.Equal(t, "new-primary", highest.GetPrimary().Id.Name)
	assert.Equal(t, int64(3), highest.GetRule().GetRuleNumber().GetCoordinatorTerm())

	// Self's rule is higher, so the apply branch is skipped — proof is that
	// no apply-path postgres queries were issued (ExpectationsWereMet below).
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// TestSetPrimary_ApplyPathErrors covers the post-revocation-check error
// paths in SetPrimary: ObservePosition, isPrimary, and the standby branch's
// setPrimaryConnInfoLocked. Each case drives the handler past validation and
// the revocation gate, then injects a failure at a single downstream step
// and asserts the error is propagated (wrapped) to the caller.
//
// The stale-primary branch's demote error path is already exercised end-to-end
// by TestDemoteStalePrimary_UpdatesConsensusTerm in rpc_manager_test.go, so
// it's not re-tested here.
func TestSetPrimary_ApplyPathErrors(t *testing.T) {
	tests := []struct {
		name string
		// ruleStore overrides the default fakeRuleStore. Used to inject
		// ObservePosition failures.
		ruleStore *fakeRuleStore
		// setupMocks runs after the default fakeRuleStore (if used) but before
		// the SetPrimary call, to inject postgres-query failures.
		setupMocks     func(*mock.QueryService)
		expectErrMatch string
	}{
		{
			// ObservePosition fails: SetPrimary cannot decide whether the
			// incoming rule is higher than the recorded one. Caller learns about
			// the failure rather than silently dropping the SetPrimary.
			name: "ObservePositionFails",
			ruleStore: &fakeRuleStore{
				pos:        makeRulePosition(3),
				observeErr: errors.New("rule store offline"),
			},
			setupMocks:     func(_ *mock.QueryService) {},
			expectErrMatch: "failed to observe local position",
		},
		{
			// isPrimary check (pg_is_in_recovery) fails: SetPrimary cannot
			// route between the standby and stale-primary branches.
			name:      "IsPrimaryQueryFails",
			ruleStore: &fakeRuleStore{pos: makeRulePosition(3)},
			setupMocks: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("SELECT pg_is_in_recovery",
					errors.New("postgres unreachable"))
			},
			expectErrMatch: "failed to check recovery status",
		},
		{
			// setPrimaryConnInfoLocked fails in the standby branch: postgres
			// reload errors are surfaced rather than swallowed.
			name:      "SetPrimaryConnInfoFails",
			ruleStore: &fakeRuleStore{pos: makeRulePosition(3)},
			setupMocks: func(m *mock.QueryService) {
				// Route to the standby branch: isPrimary -> "t" (in recovery).
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery",
					mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				// setPrimaryConnInfoLocked's own isPrimary guardrail.
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery",
					mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				// pauseReplication: pg_wal_replay_pause + verify paused.
				m.AddQueryPatternOnce("SELECT pg_wal_replay_pause",
					mock.MakeQueryResult(nil, nil))
				m.AddQueryPattern("^SELECT pg_last_wal_replay_lsn",
					mock.MakeQueryResult([]string{"replay_lsn", "is_paused"}, [][]any{{"0/100", true}}))
				// ALTER SYSTEM SET primary_conninfo fails.
				m.AddQueryPatternOnceWithError("ALTER SYSTEM SET primary_conninfo",
					errors.New("disk full"))
			},
			expectErrMatch: "disk full",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueryService := mock.NewQueryService()
			tt.setupMocks(mockQueryService)
			pm, _ := setupManagerWithMockDB(t, mockQueryService, tt.ruleStore)

			leader := newLeaderAddress("new-primary", "primary-host", 5432)
			req := &consensusdatapb.SetPrimaryRequest{
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule:    ruleAtTermForLeader(leader, 10),
					Primary: leader,
				},
			}
			resp, err := pm.SetPrimary(t.Context(), req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectErrMatch)
			assert.Nil(t, resp)
		})
	}
}
