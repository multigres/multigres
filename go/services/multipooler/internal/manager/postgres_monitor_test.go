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
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus/consensustest"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/tools/viperutil"
)

func TestDiscoverPostgresState_PgctldUnavailable(t *testing.T) {
	ctx := t.Context()
	pm := &MultipoolerManager{
		pgctldClient: nil, // pgctld unavailable
	}

	state, err := pm.discoverPostgresState(ctx)
	require.NoError(t, err)

	assert.False(t, state.pgctldAvailable)
	assert.False(t, state.dirInitialized)
	assert.False(t, state.postgresRunning)
	assert.False(t, state.backupsAvailable)
}

func TestDiscoverPostgresState_NotInitialized(t *testing.T) {
	ctx := t.Context()

	// Create mock pgctld client
	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_NOT_INITIALIZED,
		},
	}

	pm := &MultipoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
		actionLock:   actionlock.NewActionLock(),
		config:       &Config{},
		record:       newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: t.TempDir()}),
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

	state, err := pm.discoverPostgresState(ctx)
	// The data dir is not initialized, so discoverPostgresState checks whether
	// a backup exists to restore from. With no usable pgbackrest config the
	// repository cannot be read — unknown state, surfaced as an error so the
	// monitor skips the tick rather than bootstrapping over an unreadable repo.
	// The pgctld-derived fields are populated before that check.
	require.ErrorContains(t, err, "check for complete backups")
	assert.True(t, state.pgctldAvailable)
	assert.False(t, state.dirInitialized)
	assert.False(t, state.postgresRunning)
}

func TestDiscoverPostgresState_InitializedNotRunning(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_STOPPED,
		},
	}

	pm := NewTestMultipoolerManager(t)
	pm.pgctldClient = mockPgctld

	state, err := pm.discoverPostgresState(ctx)
	require.NoError(t, err)

	assert.True(t, state.pgctldAvailable)
	assert.True(t, state.dirInitialized)
	assert.False(t, state.postgresRunning)
	// backupsAvailable should NOT be checked when dirInitialized is true
	assert.False(t, state.bootstrapSentinelPresent)
}

// newRunningStandbyManagerForTest builds a manager whose pgctld reports RUNNING
// and pg_is_in_recovery=t (standby), backed by the given rule store, for
// discoverPostgresState tests. The rule store controls whether the per-iteration
// ObservePosition refresh succeeds.
func newRunningStandbyManagerForTest(t *testing.T, rs consensus.RuleStorer) *MultipoolerManager {
	t.Helper()
	mp := &clustermetadatapb.Multipooler{
		Id:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "test-pooler"},
		ShardKey:  &clustermetadatapb.ShardKey{TableGroup: "default", Shard: "0-inf"},
		PoolerDir: t.TempDir(),
	}
	pm, err := NewMultipoolerManagerForTesting(t, slog.Default(), mp, &Config{}, withFakeRules(rs))
	require.NoError(t, err)
	pm.pgctldClient = &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_RUNNING},
	}
	mockQueryService := mock.NewQueryService()
	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	pm.qsc = &mockPoolerController{queryService: mockQueryService}
	return pm
}

func TestDiscoverPostgresState_Running(t *testing.T) {
	ctx := t.Context()
	pm := newRunningStandbyManagerForTest(t, &fakeRuleStore{pos: makeRulePosition(1)})

	state, err := pm.discoverPostgresState(ctx)
	require.NoError(t, err)

	assert.True(t, state.pgctldAvailable)
	assert.True(t, state.dirInitialized)
	assert.True(t, state.postgresRunning)
	assert.Equal(t, pgmode.InRecovery, state.pgMode, "pg_is_in_recovery=t means standby")
	assert.False(t, state.bootstrapSentinelPresent)
}

// TestDiscoverPostgresState_RefreshesConsensusPosition is a regression test:
// even when the monitor takes no remedial action, a running postgres must
// refresh the cached consensus position each iteration (via ObservePosition) so
// the recovery loop converges on current state instead of lagging the periodic
// Status RPC that otherwise refreshes the cache.
func TestDiscoverPostgresState_RefreshesConsensusPosition(t *testing.T) {
	ctx := t.Context()
	rs := &fakeRuleStore{pos: makeRulePosition(1)}
	pm := newRunningStandbyManagerForTest(t, rs)

	before := rs.observePositionCallCount()
	_, err := pm.discoverPostgresState(ctx)
	require.NoError(t, err)
	assert.Greater(t, rs.observePositionCallCount(), before,
		"running postgres must refresh the cached consensus position each iteration")
}

// TestDiscoverPostgresState_ConsensusPositionUnreadable is a regression test:
// when postgres is running but the consensus position cannot be read (postgres
// or the multischema is unreadable — readCurrentRule errors when current_rule is
// missing), discovery must surface an error so the monitor skips remediation
// this tick rather than acting on stale/absent consensus state.
func TestDiscoverPostgresState_ConsensusPositionUnreadable(t *testing.T) {
	ctx := t.Context()
	rs := &fakeRuleStore{observeErr: errors.New("current_rule initial row missing: tables may not be initialized")}
	pm := newRunningStandbyManagerForTest(t, rs)

	_, err := pm.discoverPostgresState(ctx)
	require.Error(t, err)
	assert.ErrorContains(t, err, "refresh consensus position")
}

// TestDiscoverPostgresState_RunningRoleProbeFails is a regression test: when
// postgres is running but pg_is_in_recovery cannot be read, the role is
// genuinely ambiguous and discovery must surface an error so the monitor skips
// remediation. Acting on the guessed value is dangerous because the recovery-mode probe
// defaults to true on probe failure, which would make a healthy but
// momentarily-unqueryable replica look like a stale primary and trigger a
// destructive demote.
func TestDiscoverPostgresState_RunningRoleProbeFails(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_RUNNING,
		},
	}

	pm := NewTestMultipoolerManager(t)
	pm.pgctldClient = mockPgctld

	mockQueryService := mock.NewQueryService()
	mockQueryService.AddQueryPatternWithError("SELECT pg_is_in_recovery", errors.New("connection refused"))
	pm.qsc = &mockPoolerController{queryService: mockQueryService}

	state, err := pm.discoverPostgresState(ctx)
	require.Error(t, err)
	assert.ErrorContains(t, err, "determine recovery mode")
	// The process-up fact is still observed; only the role is ambiguous.
	assert.True(t, state.postgresRunning)
}

func TestDiscoverPostgresState_BootstrapSentinelPresent(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_STOPPED,
		},
	}

	pm := NewTestMultipoolerManager(t)
	pm.pgctldClient = mockPgctld

	// Plant sentinel to simulate a crashed prior first-backup attempt.
	sentinelPath := filepath.Join(pm.record.PoolerDir(), constants.BootstrapSentinelFile)
	require.NoError(t, os.WriteFile(sentinelPath, []byte("prior attempt\n"), 0o644))

	state, err := pm.discoverPostgresState(ctx)
	require.NoError(t, err)

	assert.True(t, state.bootstrapSentinelPresent)
}

func TestDiscoverPostgresState_StatusError(t *testing.T) {
	ctx := t.Context()

	// Create mock pgctld client that returns error
	mockPgctld := &mockPgctldClient{
		statusError: assert.AnError,
	}

	pm := &MultipoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
	}

	state, err := pm.discoverPostgresState(ctx)
	// Status() failure returns both the error (wrapping the cause) and a state
	// with pgctldAvailable=false so the caller can distinguish this from other
	// discover failures.
	require.Error(t, err)
	assert.False(t, state.pgctldAvailable)
	assert.False(t, state.dirInitialized)
	assert.False(t, state.postgresRunning)
	assert.False(t, state.backupsAvailable)
}

// TestDetermineRemedialAction tests the decision logic that maps discovered state to remedial actions.
// This is a table-driven test covering all decision paths in the monitor loop.
func TestDetermineRemedialAction(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "self"}
	otherID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "other"}
	otherAddr := &clustermetadatapb.PoolerAddress{Id: otherID, Host: "other-host", PostgresPort: 5432}

	// selfPos builds a cached position whose rule names the given leader at the
	// given term.
	selfPos := func(term int64, leader *clustermetadatapb.ID) *clustermetadatapb.PoolerPosition {
		return &clustermetadatapb.PoolerPosition{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   leader,
			}},
		}
	}
	recordedPrimary := func(term int64, leader *clustermetadatapb.ID, addr *clustermetadatapb.PoolerAddress) *clustermetadatapb.ReplicationPrimary {
		return &clustermetadatapb.ReplicationPrimary{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   leader,
			}},
			Primary: addr,
			// The recorded leader has advertised rewind-readiness, so the
			// stale-primary demote is allowed to proceed.
			RewindReady: true,
		}
	}

	tests := []struct {
		name               string
		state              postgresState
		poolerType         clustermetadatapb.PoolerType
		seedPrimary        *clustermetadatapb.ReplicationPrimary
		cachedPos          *clustermetadatapb.PoolerPosition
		resignedLeaderTerm int64
		inconsistentGUC    bool
		expectedAction     remedialAction
	}{
		{
			name:           "pgctld_unavailable",
			state:          postgresState{pgctldAvailable: false},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			cachedPos:      selfPos(5, selfID),
			expectedAction: remedialActionNone,
		},
		{
			// No rule known yet: intendedRole is UNKNOWN, so the monitor waits
			// for a rule-bearing backup rather than acting on observed state.
			name: "no_rule_waits",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				pgMode:          pgmode.Primary,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			cachedPos:      nil,
			expectedAction: remedialActionNone,
		},
		{
			// Cold boot: record type still UNKNOWN, postgres running, rule names
			// self -> publish the derived role (PRIMARY) and transition to SERVING.
			name: "unknown_type_with_rule_publishes_role",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				pgMode:          pgmode.Primary,
			},
			poolerType:     clustermetadatapb.PoolerType_UNKNOWN,
			cachedPos:      selfPos(5, selfID),
			expectedAction: remedialActionReconcileState,
		},
		{
			// Rule names self (intended PRIMARY) and postgres is a primary, but
			// the published label still says REPLICA: publish the role.
			name: "label_drift_replica_to_primary_publishes_role",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				pgMode:          pgmode.Primary,
			},
			poolerType:     clustermetadatapb.PoolerType_REPLICA,
			cachedPos:      selfPos(5, selfID),
			expectedAction: remedialActionReconcileState,
		},
		{
			// Intended REPLICA (consensus recorded a higher rule naming another
			// leader with contact info) but postgres is still a primary: demote.
			name: "intended_replica_but_primary_demotes",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				pgMode:          pgmode.Primary,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			seedPrimary:    recordedPrimary(5, otherID, otherAddr),
			cachedPos:      selfPos(4, selfID),
			expectedAction: remedialActionDemoteStalePrimary,
		},
		{
			// Intended PRIMARY (rule names self) but postgres is a standby and we
			// have not resigned yet: resign so the coordinator re-elects.
			name: "intended_primary_but_standby_resigns",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				pgMode:          pgmode.InRecovery,
			},
			poolerType:         clustermetadatapb.PoolerType_PRIMARY,
			cachedPos:          selfPos(5, selfID),
			resignedLeaderTerm: 0,
			expectedAction:     remedialActionResignLeadership,
		},
		{
			// Same as above but resignation already published: no further action.
			name: "intended_primary_but_standby_already_resigned",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				pgMode:          pgmode.InRecovery,
			},
			poolerType:         clustermetadatapb.PoolerType_PRIMARY,
			cachedPos:          selfPos(5, selfID),
			resignedLeaderTerm: 5,
			expectedAction:     remedialActionNone,
		},
		{
			// Rule names us leader but postgres is a standby and the label still
			// says REPLICA (e.g. a former leader rebooting after its postgres came
			// back as a standby). We already resigned. We must NOT ReconcileRole to
			// publish a SERVING PRIMARY label on a read-only standby — wait for the
			// rule to move to the new leader.
			name: "intended_primary_but_standby_resigned_replica_label_does_not_relabel",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				pgMode:          pgmode.InRecovery,
			},
			poolerType:         clustermetadatapb.PoolerType_REPLICA,
			cachedPos:          selfPos(5, selfID),
			resignedLeaderTerm: 5,
			expectedAction:     remedialActionNone,
		},
		{
			// Intended PRIMARY, postgres primary, label already PRIMARY, but the
			// GUC drifted: reconcile it.
			name: "intended_primary_with_stale_guc",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				pgMode:          pgmode.Primary,
			},
			poolerType:      clustermetadatapb.PoolerType_PRIMARY,
			cachedPos:       selfPos(5, selfID),
			inconsistentGUC: true,
			expectedAction:  remedialActionReconcileGUC,
		},
		{
			// Intended REPLICA, postgres standby, label already REPLICA, in sync:
			// nothing to do.
			name: "intended_replica_in_sync",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				pgMode:          pgmode.InRecovery,
			},
			poolerType:     clustermetadatapb.PoolerType_REPLICA,
			cachedPos:      selfPos(5, otherID),
			expectedAction: remedialActionNone,
		},
		{
			// Rule names self, postgres is a primary, label already PRIMARY, GUC in
			// sync: the happy path — nothing to do.
			name: "intended_primary_in_sync",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				pgMode:          pgmode.Primary,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			cachedPos:      selfPos(5, selfID),
			expectedAction: remedialActionNone,
		},
		{
			// Former primary now running as a standby while the rule has moved to
			// another leader, but the published label still says PRIMARY: republish
			// the rule-derived REPLICA role. (The reverse-direction relabel of the
			// old observed-role AdjustTypeToReplica path.)
			name: "label_drift_primary_to_replica",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: true,
				pgMode:          pgmode.InRecovery,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			cachedPos:      selfPos(5, otherID),
			expectedAction: remedialActionReconcileState,
		},
		{
			name: "postgres_stopped_start",
			state: postgresState{
				pgctldAvailable: true,
				postgresRunning: false,
				dirInitialized:  true,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			expectedAction: remedialActionStartPostgres,
		},
		{
			name: "postgres_stopped_restore",
			state: postgresState{
				pgctldAvailable:  true,
				postgresRunning:  false,
				dirInitialized:   false,
				backupsAvailable: true,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			expectedAction: remedialActionRestoreFromBackup,
		},
		{
			name: "postgres_stopped_no_backup_creates_first",
			state: postgresState{
				pgctldAvailable:  true,
				postgresRunning:  false,
				dirInitialized:   false,
				backupsAvailable: false,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			expectedAction: remedialActionCreateFirstBackup,
		},
		{
			// Sentinel from a prior crashed first-backup attempt must override
			// the dirInitialized=true signal, so cleanup+retry runs instead of
			// a doomed start on a stub data directory.
			name: "bootstrap_sentinel_present_forces_first_backup_path",
			state: postgresState{
				pgctldAvailable:          true,
				postgresRunning:          false,
				dirInitialized:           true,
				bootstrapSentinelPresent: true,
			},
			poolerType:     clustermetadatapb.PoolerType_PRIMARY,
			expectedAction: remedialActionCreateFirstBackup,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seed := &clustermetadatapb.Multipooler{Id: selfID, Type: tt.poolerType}
			if tt.poolerType == clustermetadatapb.PoolerType_PRIMARY {
				// Record invariant: a PRIMARY record must carry a self-leadership
				// observation that names itself.
				seed.RoutingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY}
			}
			pm := newTestManager(t,
				withServiceID(selfID),
				withRecord(newRecordFromProto(seed)),
				withReplicationPrimary(tt.seedPrimary),
				withResignedLeaderAtTerm(tt.resignedLeaderTerm),
				withRuleStore(&fakeRuleStore{pos: tt.cachedPos, inconsistentGUC: tt.inconsistentGUC}),
			)

			// This table exercises role, demote, resign and GUC decisions, not
			// physical-primary drift (that moved to StateManager.hasDrift/fixDrift and
			// is covered separately).
			got := pm.determineRemedialAction(t.Context(), tt.state)
			require.Equal(t, tt.expectedAction, got)
		})
	}
}

// TestDetermineRemedialAction_StalePrimaryDemote covers the consensus-authoritative
// "rogue primary" path: postgres is running as a primary, but consensus has
// recorded a higher-numbered rule naming a different leader. The monitor must
// retry the SetTermPrimary-ordered demote — but only when it has a recorded
// primary to rewind against and stream from. Without one, it waits.
func TestDetermineRemedialAction_StalePrimaryDemote(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "self"}
	otherID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "other"}
	otherAddr := &clustermetadatapb.PoolerAddress{Id: otherID, Host: "other-host", PostgresPort: 5432}

	// Postgres is up and running as a primary; the published label is PRIMARY,
	// so the only thing that should pull us off remedialActionNone is a demote.
	runningPrimary := postgresState{pgctldAvailable: true, postgresRunning: true, pgMode: pgmode.Primary}

	selfPos := func(term int64) *clustermetadatapb.PoolerPosition {
		return &clustermetadatapb.PoolerPosition{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   selfID,
			}},
		}
	}
	recordedPrimary := func(term int64, leader *clustermetadatapb.ID, addr *clustermetadatapb.PoolerAddress) *clustermetadatapb.ReplicationPrimary {
		return &clustermetadatapb.ReplicationPrimary{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   leader,
			}},
			Primary: addr,
			// The recorded leader has advertised rewind-readiness, so the
			// stale-primary demote is allowed to proceed.
			RewindReady: true,
		}
	}

	tests := []struct {
		name           string
		seedPrimary    *clustermetadatapb.ReplicationPrimary
		cachedPos      *clustermetadatapb.PoolerPosition
		expectedAction remedialAction
	}{
		{
			// Recorded rule outranks our applied position and names another
			// leader with usable contact info: retry the demote.
			name:           "higher_rule_names_other_leader_demotes",
			seedPrimary:    recordedPrimary(5, otherID, otherAddr),
			cachedPos:      selfPos(4),
			expectedAction: remedialActionDemoteStalePrimary,
		},
		{
			// No recorded primary: we have nothing to rewind against or stream
			// from, so wait rather than restart blind.
			name:           "no_recorded_primary_waits",
			seedPrimary:    nil,
			cachedPos:      selfPos(4),
			expectedAction: remedialActionNone,
		},
		{
			// Recorded rule advanced but carries no contact info: still no
			// source to connect to, so wait.
			name:           "recorded_primary_without_address_waits",
			seedPrimary:    recordedPrimary(5, otherID, nil),
			cachedPos:      selfPos(4),
			expectedAction: remedialActionNone,
		},
		{
			// Recorded rule is not higher than our applied position: it is stale
			// relative to us and must not trigger a demote.
			name:           "recorded_rule_not_higher_waits",
			seedPrimary:    recordedPrimary(4, otherID, otherAddr),
			cachedPos:      selfPos(4),
			expectedAction: remedialActionNone,
		},
		{
			// Recorded rule names us as the leader: not a "superseded" case.
			name:           "recorded_rule_names_self_waits",
			seedPrimary:    recordedPrimary(5, selfID, &clustermetadatapb.PoolerAddress{Id: selfID, Host: "self-host", PostgresPort: 5432}),
			cachedPos:      selfPos(4),
			expectedAction: remedialActionNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm := newTestManager(t,
				withServiceID(selfID),
				withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{
					Id:           selfID,
					Type:         clustermetadatapb.PoolerType_PRIMARY,
					RoutingState: &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
				})),
				withReplicationPrimary(tt.seedPrimary),
				withRuleStore(&fakeRuleStore{pos: tt.cachedPos}),
			)

			got := pm.determineRemedialAction(t.Context(), runningPrimary)
			require.Equal(t, tt.expectedAction, got)
		})
	}
}

// TestDeterminePostgresNotRunningAction_DivergedStartsHeld verifies recover-and-
// hold: a down, diverged standby is brought up with a plain start
// (remedialActionStartPostgres — startPostgres clears primary_conninfo when
// divergence is suspected), never a rewind. Rewinding needs the live database (to
// read the recruit-position floor), which a down node can't serve; the rewind
// waits until postgres is up (see TestShouldRewindForDivergence).
func TestDeterminePostgresNotRunningAction_DivergedStartsHeld(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "self"}
	otherID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "other"}
	otherAddr := &clustermetadatapb.PoolerAddress{Id: otherID, Host: "other-host", PostgresPort: 5432}
	notRunning := postgresState{pgctldAvailable: true, dirInitialized: true, postgresRunning: false}

	// Fully diverged, with a different, rewind-ready leader known — the strongest
	// case for a rewind. A down node must still just start.
	pm := newTestManager(t, withServiceID(selfID), withReplicationPrimary(&clustermetadatapb.ReplicationPrimary{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
			LeaderId:   otherID,
		}},
		Primary:     otherAddr,
		RewindReady: true,
	}))
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test-seed")
	require.NoError(t, err)
	_, err = pm.consensusMgr.SetSuspectedDivergence(lockCtx, true)
	require.NoError(t, err)
	pm.actionLock.Release(lockCtx)

	require.Equal(t, remedialActionStartPostgres, pm.determineRemedialAction(t.Context(), notRunning))
}

// TestShouldRewindForDivergence verifies the up-path rewind gate: suspected
// divergence, a different non-revoked leader known to rewind toward, that leader
// rewind-ready, and the backoff elapsed. Any missing condition holds instead.
func TestShouldRewindForDivergence(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "self"}
	otherID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "other"}
	otherAddr := &clustermetadatapb.PoolerAddress{Id: otherID, Host: "other-host", PostgresPort: 5432}
	selfAddr := &clustermetadatapb.PoolerAddress{Id: selfID, Host: "self-host", PostgresPort: 5432}

	// recordedPrimary builds the ReplicationPrimary a test seeds via
	// withReplicationPrimary; a nil addr means "no recorded leader".
	recordedPrimary := func(leader *clustermetadatapb.ID, addr *clustermetadatapb.PoolerAddress, rewindReady bool) *clustermetadatapb.ReplicationPrimary {
		if addr == nil {
			return nil
		}
		return &clustermetadatapb.ReplicationPrimary{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
				LeaderId:   leader,
			}},
			Primary:     addr,
			RewindReady: rewindReady,
		}
	}

	tests := []struct {
		name               string
		replicationPrimary *clustermetadatapb.ReplicationPrimary
		suspectDivergence  bool
		backoffPending     bool // record a rewind attempt first, so the backoff has not elapsed
		want               bool
	}{
		{
			// All conditions met: rewind toward the recorded, rewind-ready leader.
			name:               "all_conditions_met",
			replicationPrimary: recordedPrimary(otherID, otherAddr, true),
			suspectDivergence:  true,
			want:               true,
		},
		{
			// No divergence suspected: nothing to rewind.
			name:               "not_suspected",
			replicationPrimary: recordedPrimary(otherID, otherAddr, true),
			suspectDivergence:  false,
			want:               false,
		},
		{
			// Leader has not checkpointed onto its new timeline yet: hold.
			name:               "leader_not_rewind_ready",
			replicationPrimary: recordedPrimary(otherID, otherAddr, false),
			suspectDivergence:  true,
			want:               false,
		},
		{
			// The recorded leader is us: nothing to diverge from.
			name:               "recorded_leader_is_self",
			replicationPrimary: recordedPrimary(selfID, selfAddr, true),
			suspectDivergence:  true,
			want:               false,
		},
		{
			// No recorded leader to rewind toward: hold.
			name:               "no_recorded_leader",
			replicationPrimary: recordedPrimary(otherID, nil, true),
			suspectDivergence:  true,
			want:               false,
		},
		{
			// Backoff has not elapsed: don't thrash.
			name:               "backoff_not_elapsed",
			replicationPrimary: recordedPrimary(otherID, otherAddr, true),
			suspectDivergence:  true,
			backoffPending:     true,
			want:               false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := []testManagerOption{withServiceID(selfID)}
			if tt.replicationPrimary != nil {
				opts = append(opts, withReplicationPrimary(tt.replicationPrimary))
			}
			pm := newTestManager(t, opts...)
			if tt.suspectDivergence {
				lockCtx, err := pm.actionLock.Acquire(t.Context(), "test-seed")
				require.NoError(t, err)
				_, err = pm.consensusMgr.SetSuspectedDivergence(lockCtx, true)
				require.NoError(t, err)
				pm.actionLock.Release(lockCtx)
			}
			if tt.backoffPending {
				// Record an attempt so the backoff window has not yet elapsed.
				pm.consensusMgr.RecordRewindAttempt()
			}
			require.Equal(t, tt.want, pm.shouldRewindForDivergence())
		})
	}
}

// TestStaleStandbyDemoteTarget verifies the gating for monitor-driven
// self-demotion: a stale primary restarts as a standby only when consensus has
// genuinely, non-revocably named another leader at a rule that outranks our
// applied position. Every other case returns nil ("wait rather than restart
// blind"), so a healthy primary is never demoted on partial/stale information.
func TestStaleStandbyDemoteTarget(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "z", Name: "self"}
	otherID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "z", Name: "other"}
	otherAddr := &clustermetadatapb.PoolerAddress{Id: otherID, Host: "other-host", PostgresPort: 5432}
	rule := func(term int64, leader *clustermetadatapb.ID) *clustermetadatapb.ShardRule {
		return &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term}, LeaderId: leader}
	}
	// newPM builds a manager whose applied position is at selfTerm (naming self)
	// and whose recorded replication primary is recordedRule/addr (nil = unset).
	newPM := func(t *testing.T, recordedRule *clustermetadatapb.ShardRule, addr *clustermetadatapb.PoolerAddress, selfTerm int64) *MultipoolerManager {
		opts := []testManagerOption{
			withServiceID(selfID),
			withRuleStore(&fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{Position: &clustermetadatapb.RulePosition{Decision: rule(selfTerm, selfID)}}}),
		}
		if recordedRule != nil {
			// RewindReady so the rewind-ready gate is satisfied; cases that expect
			// nil do so for their own reason (not-ready is covered separately below).
			opts = append(opts, withReplicationPrimary(&clustermetadatapb.ReplicationPrimary{Position: &clustermetadatapb.RulePosition{Decision: recordedRule}, Primary: addr, RewindReady: true}))
		}
		return newTestManager(t, opts...)
	}

	t.Run("no recorded replication primary -> nil", func(t *testing.T) {
		require.Nil(t, newPM(t, nil, nil, 4).staleStandbyDemoteTarget())
	})

	t.Run("recorded primary missing host/port -> nil", func(t *testing.T) {
		pm := newPM(t, rule(5, otherID), &clustermetadatapb.PoolerAddress{Id: otherID}, 4)
		require.Nil(t, pm.staleStandbyDemoteTarget())
	})

	t.Run("recorded leader is us -> nil (nothing to demote toward)", func(t *testing.T) {
		pm := newPM(t, rule(5, selfID), &clustermetadatapb.PoolerAddress{Id: selfID, Host: "self-host", PostgresPort: 5432}, 4)
		require.Nil(t, pm.staleStandbyDemoteTarget())
	})

	t.Run("recorded rule does not outrank our position -> nil", func(t *testing.T) {
		pm := newPM(t, rule(4, otherID), otherAddr, 4) // equal rule number
		require.Nil(t, pm.staleStandbyDemoteTarget())
	})

	t.Run("recorded rule is revoked -> nil", func(t *testing.T) {
		dir := t.TempDir()
		// Seed a revocation of everything below term 6, which revokes the recorded
		// rule at term 5.
		consensustest.SeedTerm(t, dir, &clustermetadatapb.TermRevocation{
			RevokedBelowTerm: 6,
			OutgoingRule:     rule(5, otherID).GetRuleNumber(),
		})
		cs := consensus.NewConsensusPromises(dir, selfID)
		_, err := cs.Load()
		require.NoError(t, err)
		pm := newTestManager(t,
			withServiceID(selfID),
			withPromises(cs),
			withReplicationPrimary(&clustermetadatapb.ReplicationPrimary{Position: &clustermetadatapb.RulePosition{Decision: rule(5, otherID)}, Primary: otherAddr, RewindReady: true}),
			withRuleStore(&fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{Position: &clustermetadatapb.RulePosition{Decision: rule(4, selfID)}}}),
		)
		require.Nil(t, pm.staleStandbyDemoteTarget())
	})

	t.Run("superseded by another leader at a higher rule -> returns target", func(t *testing.T) {
		pm := newPM(t, rule(5, otherID), otherAddr, 4)
		got := pm.staleStandbyDemoteTarget()
		require.NotNil(t, got)
		assert.Equal(t, "other-host", got.GetHost())
		assert.Equal(t, int32(5432), got.GetPostgresPort())
	})

	t.Run("recorded leader not yet rewind-ready -> nil (defer demote)", func(t *testing.T) {
		// Same as the returns-target case, but the recorded leader has not advertised
		// rewind-readiness, so we defer rather than restart into a rewind that would
		// FATAL against a not-yet-checkpointed source.
		pm := newTestManager(t,
			withServiceID(selfID),
			withReplicationPrimary(&clustermetadatapb.ReplicationPrimary{Position: &clustermetadatapb.RulePosition{Decision: rule(5, otherID)}, Primary: otherAddr, RewindReady: false}),
			withRuleStore(&fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{Position: &clustermetadatapb.RulePosition{Decision: rule(4, selfID)}}}),
		)
		require.Nil(t, pm.staleStandbyDemoteTarget())
	})
}

func TestShouldMarkRewindReady(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "self"}
	// newMgr builds a manager whose consensus state controls the two inputs
	// shouldMarkRewindReady reads beyond (rewindSourceReady, role): the resigned
	// term and the recorded ReplicationPrimary's rewind-ready flag.
	newMgr := func(resignedTerm int64, rp *clustermetadatapb.ReplicationPrimary) *MultipoolerManager {
		return newTestManager(t,
			withServiceID(selfID),
			withResignedLeaderAtTerm(resignedTerm),
			withReplicationPrimary(rp),
		)
	}
	rewindReadyState := postgresState{rewindSourceReady: true}

	t.Run("not a rewind source yet", func(t *testing.T) {
		assert.False(t, newMgr(0, nil).shouldMarkRewindReady(postgresState{}, commonconsensus.ConsensusRoleLeader))
	})
	t.Run("not the consensus leader", func(t *testing.T) {
		assert.False(t, newMgr(0, nil).shouldMarkRewindReady(rewindReadyState, commonconsensus.ConsensusRoleFollower))
	})
	t.Run("leadership resigned", func(t *testing.T) {
		assert.False(t, newMgr(5, nil).shouldMarkRewindReady(rewindReadyState, commonconsensus.ConsensusRoleLeader))
	})
	t.Run("already advertised as rewind-ready", func(t *testing.T) {
		// RecordTermPrimary only records an rp that carries a rule, so give it one.
		rp := &clustermetadatapb.ReplicationPrimary{
			Position:    &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}}},
			RewindReady: true,
		}
		assert.False(t, newMgr(0, rp).shouldMarkRewindReady(rewindReadyState, commonconsensus.ConsensusRoleLeader))
	})
	t.Run("leader, checkpointed, not yet advertised", func(t *testing.T) {
		assert.True(t, newMgr(0, nil).shouldMarkRewindReady(rewindReadyState, commonconsensus.ConsensusRoleLeader))
	})
}

func TestTakeRemedialAction_PgctldUnavailable(t *testing.T) {
	ctx := t.Context()

	pm := &MultipoolerManager{
		logger:     slog.Default(),
		actionLock: actionlock.NewActionLock(),
	}

	// Acquire lock before calling takeRemedialAction
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Should log error and take no action
	pm.takeRemedialAction(lockCtx, remedialActionNone, postgresState{})

	// Note: takeRemedialAction with remedialActionNone doesn't log
	assert.Equal(t, "", pm.pgMonitorLastLoggedReason)
}

func TestTakeRemedialAction_PostgresReady(t *testing.T) {
	ctx := t.Context()

	pm := &MultipoolerManager{
		logger:     slog.Default(),
		actionLock: actionlock.NewActionLock(),
		record: newRecordFromProto(&clustermetadatapb.Multipooler{
			Type: clustermetadatapb.PoolerType_REPLICA,
		}),
	}

	// Acquire lock before calling takeRemedialAction
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Should log info and take no action (no type mismatch)
	pm.takeRemedialAction(lockCtx, remedialActionNone, postgresState{})

	// Note: takeRemedialAction with remedialActionNone doesn't log
	assert.Equal(t, "", pm.pgMonitorLastLoggedReason)
}

func TestTakeRemedialAction_StartPostgres(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{}

	pm := newTestManager(t)
	pm.pgctldClient = mockPgctld

	// Acquire lock before calling takeRemedialAction
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Should attempt to start postgres
	pm.takeRemedialAction(lockCtx, remedialActionStartPostgres, postgresState{})

	assert.Equal(t, "starting_postgres", pm.pgMonitorLastLoggedReason)
	assert.True(t, mockPgctld.startCalled, "Should have called Start()")
}

func TestTakeRemedialAction_StartPostgresFails(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{
		startError: assert.AnError,
	}

	pm := newTestManager(t)
	pm.pgctldClient = mockPgctld
	pm.pgMonitorLastLoggedReason = "starting_postgres"

	// Acquire lock before calling takeRemedialAction
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Should handle error gracefully
	pm.takeRemedialAction(lockCtx, remedialActionStartPostgres, postgresState{})

	assert.True(t, mockPgctld.startCalled, "Should have attempted to call Start()")
	// Reason stays the same since we're retrying
}

func TestTakeRemedialAction_WaitingForBackup(t *testing.T) {
	ctx := t.Context()

	pm := &MultipoolerManager{
		logger:     slog.Default(),
		actionLock: actionlock.NewActionLock(),
	}

	// Acquire lock before calling takeRemedialAction
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// With no backups and uninitialized dir, action is None - doesn't do anything
	pm.takeRemedialAction(lockCtx, remedialActionNone, postgresState{})

	// takeRemedialAction with None action doesn't modify last logged reason
	assert.Equal(t, "", pm.pgMonitorLastLoggedReason)
}

func TestTakeRemedialAction_LogDeduplication(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{}

	// newTestManager defaults to a REPLICA record, which is what this test needs.
	pm := newTestManager(t)
	pm.pgctldClient = mockPgctld
	pm.pgMonitorLastLoggedReason = "starting_postgres"

	// Acquire lock before calling takeRemedialAction
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Call multiple times with same action - reason should stay the same (log deduplication)
	pm.takeRemedialAction(lockCtx, remedialActionStartPostgres, postgresState{})
	assert.Equal(t, "starting_postgres", pm.pgMonitorLastLoggedReason)

	pm.takeRemedialAction(lockCtx, remedialActionStartPostgres, postgresState{})
	assert.Equal(t, "starting_postgres", pm.pgMonitorLastLoggedReason)

	pm.takeRemedialAction(lockCtx, remedialActionStartPostgres, postgresState{})
	assert.Equal(t, "starting_postgres", pm.pgMonitorLastLoggedReason)

	// Change action type - reason should change
	pm.takeRemedialAction(lockCtx, remedialActionRestoreFromBackup, postgresState{})
	assert.Equal(t, "restoring_from_backup", pm.pgMonitorLastLoggedReason)
}

func TestTakeRemedialAction_ResignationSignal(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"}
	otherID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "other-pooler"}
	selfPos := func(term int64) *clustermetadatapb.PoolerPosition {
		return &clustermetadatapb.PoolerPosition{Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
			LeaderId:   selfID,
		}}}
	}
	tests := []struct {
		name           string
		action         remedialAction
		poolerType     clustermetadatapb.PoolerType
		resignedBefore int64                             // set resignedLeaderAtTerm before action (0 = don't set)
		cachedPos      *clustermetadatapb.PoolerPosition // rule the monitor reads; the resign term comes from it
		wantAvStatus   *clustermetadatapb.AvailabilityStatus
	}{
		{
			// The resign term is the highest-known rule's term; seed a rule naming
			// self at term 5.
			name:       "ResignLeadership sets resignation at the highest-known term",
			action:     remedialActionResignLeadership,
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			cachedPos:  selfPos(5),
			wantAvStatus: &clustermetadatapb.AvailabilityStatus{
				LeadershipStatus: &clustermetadatapb.LeadershipStatus{
					LeaderTerm: 5,
					Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
				},
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
				},
			},
		},
		{
			// No known rule (nil position) -> term 0 -> no resignation signal.
			name:       "ResignLeadership sets no resignation when no known term",
			action:     remedialActionResignLeadership,
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			wantAvStatus: &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
				},
			},
		},
		{
			name:           "ReconcileRole does not clear existing resignation signal",
			action:         remedialActionReconcileState,
			poolerType:     clustermetadatapb.PoolerType_REPLICA,
			resignedBefore: 7,
			// Rule names another leader, so the rule-derived role is REPLICA;
			// ReconcileRole republishes REPLICA and must leave resignation intact.
			cachedPos: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 8},
					LeaderId:   otherID,
				}},
			},
			wantAvStatus: &clustermetadatapb.AvailabilityStatus{
				LeadershipStatus: &clustermetadatapb.LeadershipStatus{
					LeaderTerm: 7,
					Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
				},
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			multipooler := &clustermetadatapb.Multipooler{
				Id:   selfID,
				Type: tc.poolerType,
			}
			if tc.poolerType == clustermetadatapb.PoolerType_PRIMARY {
				// Record invariant: a PRIMARY record must carry a self-leadership obs.
				multipooler.RoutingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY}
			}
			dir := t.TempDir()
			// Seed a revocation of everything below term 1 so the manager has a term.
			consensustest.SeedTerm(t, dir, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1})
			cs := consensus.NewConsensusPromises(dir, nil)
			_, err := cs.Load()
			require.NoError(t, err)

			pm := newRemedialActionTestManager(t, multipooler,
				withRuleStore(&fakeRuleStore{pos: tc.cachedPos}),
				withPromises(cs),
			)

			lockCtx, err := pm.actionLock.Acquire(ctx, "test")
			require.NoError(t, err)
			defer pm.actionLock.Release(lockCtx)

			if tc.resignedBefore != 0 {
				require.NoError(t, pm.consensusMgr.SetResignedLeaderAtTerm(lockCtx, &clustermetadatapb.RulePosition{
					Decision: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: tc.resignedBefore},
					},
				}))
			}

			pm.takeRemedialAction(lockCtx, tc.action, postgresState{})

			assert.Equal(t, tc.wantAvStatus, pm.buildAvailabilityStatus())
		})
	}
}

func TestTakeRemedialAction_ReconcileGUC(t *testing.T) {
	ctx := t.Context()

	frs := &fakeRuleStore{}
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"}
	pm := newRemedialActionTestManager(t, &clustermetadatapb.Multipooler{
		Id:   selfID,
		Type: clustermetadatapb.PoolerType_PRIMARY,
		// A PRIMARY record must name itself as leader (the record invariant).
		RoutingState: &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
	}, withRuleStore(frs))

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	pm.takeRemedialAction(lockCtx, remedialActionReconcileGUC, postgresState{pgMode: pgmode.Primary})

	assert.True(t, frs.reconcileGUCCalled, "ReconcileGUC should have been called")
	assert.Equal(t, "postgres_running", pm.pgMonitorLastLoggedReason)
}

// setupManagerWithMockDBAndPgctld is setupManagerWithMockDB, but also returns
// the mock pgctld service so a test can assert on calls made through it (e.g.
// StopRestoreCommandCalls) — setupManagerWithMockDB discards that reference.
func setupManagerWithMockDBAndPgctld(t *testing.T, mockQueryService *mock.QueryService, rules consensus.RuleStorer) (*MultipoolerManager, *testutil.MockPgCtldService) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })

	mockPgctld := &testutil.MockPgCtldService{}
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, mockPgctld)
	t.Cleanup(cleanupPgctld)

	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	serviceID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"}
	multipooler := &clustermetadatapb.Multipooler{
		Id:            serviceID,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_REPLICA,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   database,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}
	require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

	tmpDir := t.TempDir()
	multipooler.PoolerDir = tmpDir
	config := &Config{
		TopoClient: ts,
		PgctldAddr: pgctldAddr,
	}
	pm, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
		withMockController(&mockPoolerController{queryService: mockQueryService}),
		withFakeRules(rules),
	)
	require.NoError(t, err)
	t.Cleanup(func() { pm.ShutdownForTest(context.Background()) })

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	pm.Start(senv)

	require.Eventually(t, func() bool {
		return pm.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	return pm, mockPgctld
}

// TestShouldDisableRestoreCommand exercises the pure decision logic (no
// mutation) that gates remedialActionDisableRestoreCommand: a cohort member
// must never trust archive-sourced WAL, and an observer that's already
// streaming successfully has no remaining need for archive catch-up either.
func TestShouldDisableRestoreCommand(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"}
	cohortRule := &clustermetadatapb.ShardRule{
		CohortMembers: []*clustermetadatapb.ID{selfID},
	}

	tests := []struct {
		name           string
		cohortMember   bool
		restoreCommand string
		restoreCmdErr  error
		walReceiver    string
		replStatusErr  error
		want           bool
	}{
		{name: "cohort member with restore_command set", cohortMember: true, restoreCommand: "pgctld restore-wrapper ...", want: true},
		{name: "observer streaming with restore_command set", restoreCommand: "pgctld restore-wrapper ...", walReceiver: "streaming", want: true},
		{name: "observer not streaming leaves restore_command alone", restoreCommand: "pgctld restore-wrapper ...", walReceiver: "waiting", want: false},
		{name: "restore_command already unset", restoreCommand: "", want: false},
		{name: "cohort member but restore_command already unset", cohortMember: true, restoreCommand: "", want: false},
		{name: "fails closed on read error", restoreCommand: "pgctld restore-wrapper ...", restoreCmdErr: errors.New("boom"), want: false},
		{name: "fails closed on replication status error", restoreCommand: "pgctld restore-wrapper ...", replStatusErr: errors.New("boom"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := mock.NewQueryService()
			if tt.restoreCmdErr != nil {
				m.AddQueryPatternOnceWithError("current_setting.*restore_command", tt.restoreCmdErr)
			} else {
				m.AddQueryPatternOnce("current_setting.*restore_command", mock.MakeQueryResult([]string{"current_setting"}, [][]any{{tt.restoreCommand}}))
			}
			if tt.restoreCmdErr == nil && tt.restoreCommand != "" && !tt.cohortMember {
				// Only queried when a cohort-member short-circuit didn't already decide it.
				cols := []string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "last_xact_replay_ts", "primary_conninfo", "status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"}
				if tt.replStatusErr != nil {
					m.AddQueryPatternOnceWithError("pg_last_wal_replay_lsn", tt.replStatusErr)
				} else {
					m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(cols, [][]any{{nil, nil, false, "not paused", nil, "", tt.walReceiver, nil, nil, nil}}))
				}
			}

			var frs *fakeRuleStore
			if tt.cohortMember {
				frs = &fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{Position: &clustermetadatapb.RulePosition{Decision: cohortRule}}}
			} else {
				frs = &fakeRuleStore{}
			}

			pm, _ := setupManagerWithMockDBAndPgctld(t, m, frs)

			assert.Equal(t, tt.want, pm.shouldDisableRestoreCommand(t.Context()))
		})
	}
}

// TestTakeRemedialAction_DisableRestoreCommand verifies the monitor's backstop
// clears restore_command AND stops any in-flight invocation — a config change
// alone only affects the next fetch decision, so a backstop that only reset
// the GUC would leave exactly the gap it exists to close (see Recruit and
// SetPrimary, which both pair resetRestoreCommand with stopRestoreCommand).
func TestTakeRemedialAction_DisableRestoreCommand(t *testing.T) {
	m := mock.NewQueryService()
	m.AddQueryPatternOnce("ALTER SYSTEM RESET restore_command", mock.MakeQueryResult(nil, nil))
	expectReloadConfig(m)

	pm, mockPgctld := setupManagerWithMockDBAndPgctld(t, m, &fakeRuleStore{})

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	pm.takeRemedialAction(lockCtx, remedialActionDisableRestoreCommand, postgresState{pgMode: pgmode.InRecovery})

	assert.NoError(t, m.ExpectationsWereMet(), "resetRestoreCommand's queries should have run")
	assert.Len(t, mockPgctld.StopRestoreCommandCalls, 1, "stopRestoreCommand should have called the pgctld RPC")
}

// TestTakeRemedialAction_ReconcileRole_AppliesRuleDerivedRole verifies that
// ReconcileRole applies the role the committed rule implies — transitioning the
// pooler to PRIMARY and recording the self-leadership observation built from the
// rule — even when the record's label still says REPLICA.
func TestTakeRemedialAction_ReconcileRole_AppliesRuleDerivedRole(t *testing.T) {
	multipooler := &clustermetadatapb.Multipooler{
		Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"},
		Type: clustermetadatapb.PoolerType_REPLICA,
	}
	committed := &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}
	// The committed rule names this pooler leader, so the rule-derived role is
	// PRIMARY — ReconcileRole must publish PRIMARY plus the self-leadership obs
	// regardless of the stale REPLICA label on the record.
	pm := newRemedialActionTestManager(t, multipooler, withRuleStore(&fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: committed, LeaderId: multipooler.Id}},
	}}))

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	pm.takeRemedialAction(lockCtx, remedialActionReconcileState,
		postgresState{pgctldAvailable: true, postgresRunning: true, pgMode: pgmode.Primary})

	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.record.Type())
	obs := pm.record.RoutingState()
	require.NotNil(t, obs, "ReconcileRole must persist a PRIMARY routing_state when the rule names this pooler leader")
	assert.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, obs.GetRole())
	assert.Equal(t, committed, obs.GetRule())
}

func TestHasCompleteBackups_UnreadableRepoSurfacesError(t *testing.T) {
	ctx := t.Context()
	poolerDir := t.TempDir()

	pm := &MultipoolerManager{
		logger:     slog.Default(),
		actionLock: actionlock.NewActionLock(),
		config:     &Config{},
		record:     newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: poolerDir}),
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

	// With no usable pgbackrest config the repository cannot be inspected.
	// That is unknown state, not an absence of backups: hasCompleteBackups
	// must surface the error (false, err) rather than masking it as "no
	// backups", so the monitor refuses to bootstrap over a repo it can't read.
	result, err := pm.hasCompleteBackups(ctx)

	require.Error(t, err)
	assert.False(t, result)
}

func TestHasCompleteBackups_ActionLockTimeout(t *testing.T) {
	poolerDir := t.TempDir()

	pm := &MultipoolerManager{
		logger:     slog.Default(),
		actionLock: actionlock.NewActionLock(),
		config:     &Config{},
		record:     newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: poolerDir}),
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

	// Acquire the action lock to block hasCompleteBackups
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test-holder")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Create a context with timeout for hasCompleteBackups call
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	// Can't acquire the lock — an unknown-state error, surfaced not masked.
	result, err := pm.hasCompleteBackups(ctx)

	require.Error(t, err)
	assert.False(t, result)
}

func TestStartPostgres_Success(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{}

	pm := newTestManager(t)
	pm.pgctldClient = mockPgctld

	err := pm.startPostgres(ctx)

	require.NoError(t, err)
	assert.True(t, mockPgctld.startCalled)
}

func TestStartPostgres_PgctldUnavailable(t *testing.T) {
	ctx := t.Context()

	pm := &MultipoolerManager{
		pgctldClient: nil,
		logger:       slog.Default(),
	}

	err := pm.startPostgres(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgctld client not available")
}

func TestStartPostgres_StartFails(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{
		startError: assert.AnError,
	}

	pm := newTestManager(t)
	pm.pgctldClient = mockPgctld

	err := pm.startPostgres(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start PostgreSQL")
	assert.True(t, mockPgctld.startCalled)
}

func TestMonitorPostgres_WaitsForReady(t *testing.T) {
	ctx := t.Context()

	readyChan := make(chan struct{})

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_STOPPED,
		},
	}

	pm := NewTestMultipoolerManager(t)
	pm.readyChan = readyChan
	pm.pgctldClient = mockPgctld
	pm.state = ManagerStateStarting

	// Call iteration when not ready - should return early without calling pgctld
	pm.monitorPostgresIteration(ctx) //nolint:errcheck
	assert.False(t, mockPgctld.startCalled, "Should not attempt to start when not ready")

	// Set state to ready
	pm.mu.Lock()
	pm.state = ManagerStateReady
	pm.mu.Unlock()
	close(readyChan)

	// Call iteration again when ready - should proceed and attempt to start
	pm.monitorPostgresIteration(ctx) //nolint:errcheck
	assert.True(t, mockPgctld.startCalled, "Should attempt to start when ready")
}

func TestMonitorPostgres_HandlesRunningPostgres(t *testing.T) {
	ctx := t.Context()

	readyChan := make(chan struct{})
	close(readyChan)

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_RUNNING,
		},
	}

	pm := NewTestMultipoolerManager(t)
	pm.readyChan = readyChan
	pm.pgctldClient = mockPgctld
	pm.state = ManagerStateReady
	setPoolerTypeForTest(t, pm, clustermetadatapb.PoolerType_PRIMARY)

	// Call iteration - should discover running state and not call Start
	pm.monitorPostgresIteration(ctx) //nolint:errcheck

	// Should not have called Start (postgres already running)
	assert.False(t, mockPgctld.startCalled, "Should not call Start when postgres is already running")
}

func TestMonitorPostgres_StartsStoppedPostgres(t *testing.T) {
	ctx := t.Context()

	readyChan := make(chan struct{})
	close(readyChan)

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_STOPPED,
		},
	}

	pm := NewTestMultipoolerManager(t)
	pm.readyChan = readyChan
	pm.pgctldClient = mockPgctld
	pm.state = ManagerStateReady

	// Call iteration - should discover stopped state and attempt to start
	pm.monitorPostgresIteration(ctx) //nolint:errcheck

	// Should have attempted to start postgres
	assert.True(t, mockPgctld.startCalled, "Should attempt to start stopped postgres")
}

func TestMonitorPostgres_RetriesOnStartFailure(t *testing.T) {
	ctx := t.Context()

	readyChan := make(chan struct{})
	close(readyChan)

	mockPgctld := &mockPgctldClientWithCounter{
		mockPgctldClient: mockPgctldClient{
			statusResponse: &pgctldpb.StatusResponse{
				Status: pgctldpb.ServerStatus_STOPPED,
			},
			startError: assert.AnError,
		},
	}

	pm := NewTestMultipoolerManager(t)
	pm.readyChan = readyChan
	pm.pgctldClient = mockPgctld
	pm.state = ManagerStateReady

	// Call iteration multiple times to simulate retry behavior
	for range 5 {
		pm.monitorPostgresIteration(ctx) //nolint:errcheck
	}

	// Should have retried multiple times
	assert.Equal(t, 5, mockPgctld.startCallCount, "Should attempt to start on each iteration")
}

func TestPostgresStateEqual(t *testing.T) {
	base := postgresState{
		pgctldAvailable:          true,
		dirInitialized:           true,
		postgresRunning:          true,
		backupsAvailable:         true,
		pgMode:                   pgmode.Primary,
		bootstrapSentinelPresent: true,
	}

	t.Run("equal states", func(t *testing.T) {
		assert.True(t, postgresStateEqual(base, base))
	})

	tests := []struct {
		name  string
		other postgresState
	}{
		{"pgctldAvailable", func() postgresState { s := base; s.pgctldAvailable = false; return s }()},
		{"dirInitialized", func() postgresState { s := base; s.dirInitialized = false; return s }()},
		{"postgresRunning", func() postgresState { s := base; s.postgresRunning = false; return s }()},
		{"backupsAvailable", func() postgresState { s := base; s.backupsAvailable = false; return s }()},
		{"pgMode", func() postgresState { s := base; s.pgMode = pgmode.InRecovery; return s }()},
		{"bootstrapSentinelPresent", func() postgresState { s := base; s.bootstrapSentinelPresent = false; return s }()},
	}
	for _, tc := range tests {
		t.Run("differs in "+tc.name, func(t *testing.T) {
			assert.False(t, postgresStateEqual(base, tc.other))
		})
	}
}

// TestPrimaryConnInfoDiffersFromRecorded exercises the MonitorPostgres
// self-heal predicate. The function decides whether the monitor should issue
// remedialActionFixPrimaryConnInfo on a given tick by comparing the live
// primary_conninfo (read from postgres) against the (rule, primary) tuple
// recorded by SetPrimary/Promote. The function is intentionally
// conservative: when in doubt, return false so the monitor takes no action.
//
// The cases below cover each early-exit path plus the live-vs-recorded
// comparison branches. setupManagerWithMockDB seeds serviceID with cell
// "zone1" and name "test-pooler"; the SelfIsLeader case relies on that.
func TestPrimaryConnInfoDiffersFromRecorded(t *testing.T) {
	const (
		recordedHost = "primary.example.com"
		recordedPort = int32(5432)
	)
	recordedID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "primary-pooler",
	}
	selfID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	mkRule := func(term int64, leader *clustermetadatapb.ID) *clustermetadatapb.ShardRule {
		return &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
			LeaderId:   leader,
		}
	}
	mkAddress := func(host string, port int32) *clustermetadatapb.PoolerAddress {
		return &clustermetadatapb.PoolerAddress{Id: recordedID, Host: host, PostgresPort: port}
	}

	tests := []struct {
		name string
		// seedRP, when non-nil, is recorded on consensusPromises before the call.
		seedRP *clustermetadatapb.ReplicationPrimary
		// seedRevocation, when non-nil, is written to consensusPromises before the call.
		seedRevocation *clustermetadatapb.TermRevocation
		// seedManualStop, when true, sets the walReceiverManuallyStopped flag
		// before the call to simulate a prior StopReplication.
		seedManualStop bool
		// mockConnInfo controls what readPrimaryConnInfo returns. Empty string
		// means NULL; mockReadError takes precedence and triggers a query error.
		mockConnInfo  string
		mockReadError bool
		// expectQuery is true when readPrimaryConnInfo is expected to run; the
		// early-exit branches don't issue the SQL.
		expectQuery bool
		want        bool
	}{
		{
			name: "NoReplicationPrimaryRecorded",
			// rp == nil -> nothing to compare against
			want: false,
		},
		{
			// StopReplication-set manual-stop flag preempts drift detection
			// so the monitor doesn't fire a reconciliation action that
			// setPrimaryConnInfoLocked would refuse with FAILED_PRECONDITION
			// (once per 5s tick, noisily).
			name: "ManualStopFlagSet",
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: mkRule(5, recordedID)},
				Primary:  mkAddress(recordedHost, recordedPort),
			},
			seedManualStop: true,
			want:           false,
		},
		{
			name: "PrimaryFieldMissing",
			// rule recorded but primary contact info absent -> nothing to compare
			seedRP: &clustermetadatapb.ReplicationPrimary{Position: &clustermetadatapb.RulePosition{Decision: mkRule(5, recordedID)}},
			want:   false,
		},
		{
			name: "SelfIsLeader",
			// recorded rule names this pooler — primary-side case, not replica
			// reconciliation
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: mkRule(5, selfID)},
				Primary:  mkAddress(recordedHost, recordedPort),
			},
			want: false,
		},
		{
			name: "RecordedRuleRevoked",
			// revocation at term 9 outranks rule at term 5 -> reconcile would
			// race the in-flight Recruit/Promote
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: mkRule(5, recordedID)},
				Primary:  mkAddress(recordedHost, recordedPort),
			},
			seedRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm: 9,
				OutgoingRule:     &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
			},
			want: false,
		},
		{
			name: "RecordedHostEmpty",
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: mkRule(5, recordedID)},
				Primary:  mkAddress("", recordedPort),
			},
			want: false,
		},
		{
			name: "RecordedPortZero",
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: mkRule(5, recordedID)},
				Primary:  mkAddress(recordedHost, 0),
			},
			want: false,
		},
		{
			name: "ReadPrimaryConnInfoErrors",
			// Conservative: don't trigger reconciliation we can't verify
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: mkRule(5, recordedID)},
				Primary:  mkAddress(recordedHost, recordedPort),
			},
			expectQuery:   true,
			mockReadError: true,
			want:          false,
		},
		{
			name: "LiveConnInfoEmpty_DriftsFromRecorded",
			// Recorded says we should be following a primary; postgres has
			// nothing -> drift
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: mkRule(5, recordedID)},
				Primary:  mkAddress(recordedHost, recordedPort),
			},
			expectQuery:  true,
			mockConnInfo: "",
			want:         true,
		},
		{
			name: "LiveConnInfoUnparseable_TreatedAsDrift",
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: mkRule(5, recordedID)},
				Primary:  mkAddress(recordedHost, recordedPort),
			},
			expectQuery:  true,
			mockConnInfo: "host=primary.example.com port=invalid user=replicator",
			want:         true,
		},
		{
			name: "LiveConnInfoMatchesRecorded",
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: mkRule(5, recordedID)},
				Primary:  mkAddress(recordedHost, recordedPort),
			},
			expectQuery:  true,
			mockConnInfo: "host=primary.example.com port=5432 user=replicator",
			want:         false,
		},
		{
			name: "LiveConnInfoHostMismatch",
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: mkRule(5, recordedID)},
				Primary:  mkAddress(recordedHost, recordedPort),
			},
			expectQuery:  true,
			mockConnInfo: "host=other.example.com port=5432 user=replicator",
			want:         true,
		},
		{
			name: "LiveConnInfoPortMismatch",
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: mkRule(5, recordedID)},
				Primary:  mkAddress(recordedHost, recordedPort),
			},
			expectQuery:  true,
			mockConnInfo: "host=primary.example.com port=9999 user=replicator",
			want:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueryService := mock.NewQueryService()
			if tt.expectQuery {
				if tt.mockReadError {
					mockQueryService.AddQueryPatternOnceWithError(
						"current_setting.*primary_conninfo", assert.AnError)
				} else {
					var row [][]any
					if tt.mockConnInfo == "" {
						row = [][]any{{nil}}
					} else {
						row = [][]any{{tt.mockConnInfo}}
					}
					mockQueryService.AddQueryPatternOnce(
						"current_setting.*primary_conninfo",
						mock.MakeQueryResult([]string{"current_setting"}, row))
				}
			}

			pm, tmpDir := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{pos: makeRulePosition(0)})

			if tt.seedRP != nil {
				lockCtx, err := pm.actionLock.Acquire(t.Context(), "test-seed")
				require.NoError(t, err)
				require.NoError(t, pm.consensusMgr.RecordTermPrimary(lockCtx, tt.seedRP))
				pm.actionLock.Release(lockCtx)
			}
			if tt.seedRevocation != nil {
				consensustest.SeedTerm(t, tmpDir, tt.seedRevocation)
				_, err := pm.consensusMgr.Promises().Load()
				require.NoError(t, err)
			}
			if tt.seedManualStop {
				pm.walReceiverManuallyStopped.Store(true)
			}

			got := pm.primaryConnInfoDiffersFromRecorded(t.Context())
			assert.Equal(t, tt.want, got)
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

// TestStartPostgres_MarksSuspectedDivergenceAfterCrashRecovery covers the block
// in startPostgres that marks suspected divergence when pgctld had to force
// crash recovery AND a *different* node is the known consensus leader — the
// recovered local WAL may have diverged from that leader's timeline. When we are
// (or no one is) the leader, or no crash recovery ran, the flag stays clear.
func TestStartPostgres_MarksSuspectedDivergenceAfterCrashRecovery(t *testing.T) {
	self := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "self"}
	other := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "other"}

	recordedPrimary := func(leader *clustermetadatapb.ID) *clustermetadatapb.ReplicationPrimary {
		if leader == nil {
			return nil
		}
		return &clustermetadatapb.ReplicationPrimary{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
				LeaderId:   leader,
			}},
			Primary: &clustermetadatapb.PoolerAddress{Id: leader, Host: "host", PostgresPort: 5432},
		}
	}

	tests := []struct {
		name             string
		crashRecoveryRan bool
		recordedLeader   *clustermetadatapb.ID // nil = no leader recorded
		wantSuspected    bool
	}{
		{
			// Crash recovery ran and a different leader is known: mark divergence.
			name:             "crash_recovery_different_leader_marks",
			crashRecoveryRan: true,
			recordedLeader:   other,
			wantSuspected:    true,
		},
		{
			// The known leader is us: our WAL is authoritative, nothing to diverge from.
			name:             "crash_recovery_self_leader_no_mark",
			crashRecoveryRan: true,
			recordedLeader:   self,
			wantSuspected:    false,
		},
		{
			// No leader known: nothing to diverge from.
			name:             "crash_recovery_no_leader_no_mark",
			crashRecoveryRan: true,
			recordedLeader:   nil,
			wantSuspected:    false,
		},
		{
			// Postgres started cleanly (no crash recovery): no divergence suspected.
			name:             "no_crash_recovery_no_mark",
			crashRecoveryRan: false,
			recordedLeader:   other,
			wantSuspected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := []testManagerOption{withServiceID(self)}
			if rp := recordedPrimary(tt.recordedLeader); rp != nil {
				opts = append(opts, withReplicationPrimary(rp))
			}
			pm := newTestManager(t, opts...)
			pm.pgctldClient = &mockPgctldClient{
				startResponse: &pgctldpb.StartResponse{CrashRecoveryRan: tt.crashRecoveryRan},
			}

			lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
			require.NoError(t, err)
			defer pm.actionLock.Release(lockCtx)

			require.NoError(t, pm.startPostgres(lockCtx))
			assert.Equal(t, tt.wantSuspected, pm.consensusMgr.SuspectedDivergence())
		})
	}
}
