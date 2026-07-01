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

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus/consensustest"
)

func TestDiscoverPostgresState_PgctldUnavailable(t *testing.T) {
	ctx := t.Context()
	pm := &MultiPoolerManager{
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

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
		actionLock:   actionlock.NewActionLock(),
		config:       &Config{},
		record:       newRecordFromProto(&clustermetadatapb.MultiPooler{PoolerDir: t.TempDir()}),
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

	state, err := pm.discoverPostgresState(ctx)
	require.NoError(t, err)

	assert.True(t, state.pgctldAvailable)
	assert.False(t, state.dirInitialized)
	assert.False(t, state.postgresRunning)
	// backupsAvailable will be false since no pgbackrest setup
	assert.False(t, state.backupsAvailable)
}

func TestDiscoverPostgresState_InitializedNotRunning(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_STOPPED,
		},
	}

	pm := NewTestMultiPoolerManager(t)
	pm.pgctldClient = mockPgctld

	state, err := pm.discoverPostgresState(ctx)
	require.NoError(t, err)

	assert.True(t, state.pgctldAvailable)
	assert.True(t, state.dirInitialized)
	assert.False(t, state.postgresRunning)
	// backupsAvailable should NOT be checked when dirInitialized is true
	assert.False(t, state.bootstrapSentinelPresent)
}

func TestDiscoverPostgresState_Running(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_RUNNING,
		},
	}

	pm := NewTestMultiPoolerManager(t)
	pm.pgctldClient = mockPgctld

	// Running postgres triggers a pg_is_in_recovery probe; stub it as a standby
	// so discovery determines the role rather than failing on an unreadable one.
	mockQueryService := mock.NewQueryService()
	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	pm.qsc = &mockPoolerController{queryService: mockQueryService}

	state, err := pm.discoverPostgresState(ctx)
	require.NoError(t, err)

	assert.True(t, state.pgctldAvailable)
	assert.True(t, state.dirInitialized)
	assert.True(t, state.postgresRunning)
	assert.False(t, state.isPrimary, "pg_is_in_recovery=t means standby")
	assert.False(t, state.bootstrapSentinelPresent)
}

// TestDiscoverPostgresState_RunningRoleProbeFails is a regression test: when
// postgres is running but pg_is_in_recovery cannot be read, the role is
// genuinely ambiguous and discovery must surface an error so the monitor skips
// remediation. Acting on the guessed value is dangerous because isPrimary
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

	pm := NewTestMultiPoolerManager(t)
	pm.pgctldClient = mockPgctld

	mockQueryService := mock.NewQueryService()
	mockQueryService.AddQueryPatternWithError("SELECT pg_is_in_recovery", errors.New("connection refused"))
	pm.qsc = &mockPoolerController{queryService: mockQueryService}

	state, err := pm.discoverPostgresState(ctx)
	require.Error(t, err)
	assert.ErrorContains(t, err, "determine primary status")
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

	pm := NewTestMultiPoolerManager(t)
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

	pm := &MultiPoolerManager{
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
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   leader,
			},
		}
	}
	recordedPrimary := func(term int64, leader *clustermetadatapb.ID, addr *clustermetadatapb.PoolerAddress) *clustermetadatapb.ReplicationPrimary {
		return &clustermetadatapb.ReplicationPrimary{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   leader,
			},
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
				isPrimary:       true,
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
				isPrimary:       true,
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
				isPrimary:       true,
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
				isPrimary:       true,
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
				isPrimary:       false,
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
				isPrimary:       false,
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
				isPrimary:       false,
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
				isPrimary:       true,
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
				isPrimary:       false,
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
				isPrimary:       true,
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
				isPrimary:       false,
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
			seed := &clustermetadatapb.MultiPooler{Id: selfID, Type: tt.poolerType}
			if tt.poolerType == clustermetadatapb.PoolerType_PRIMARY {
				// Record invariant: a PRIMARY record must carry a self-leadership
				// observation that names itself.
				seed.SelfLeadership = &clustermetadatapb.LeaderObservation{LeaderId: selfID}
			}
			pm := newTestManager(t,
				withServiceID(selfID),
				withRecord(newRecordFromProto(seed)),
				withReplicationPrimary(tt.seedPrimary),
				withResignedLeaderAtTerm(tt.resignedLeaderTerm),
				withRuleStore(&fakeRuleStore{pos: tt.cachedPos, inconsistentGUC: tt.inconsistentGUC}),
			)

			// lastAppliedPrimary == state.isPrimary: this table exercises role,
			// demote, resign and GUC decisions, not physical-primary drift (covered
			// separately), so pass no drift here.
			got := pm.determineRemedialAction(t.Context(), tt.state, tt.state.isPrimary)
			require.Equal(t, tt.expectedAction, got)
		})
	}
}

// TestDetermineRemedialAction_PrimaryDrift covers the physical-primary-drift path:
// the role is already aligned (rule names self, record says PRIMARY), but the
// StateManager's last-applied primary-ness differs from what postgres reports.
// That drift alone must trigger remedialActionReconcileState so the writable-only
// components (heartbeat writer, LISTEN) re-evaluate — e.g. recovery clearing again
// after a resign.
func TestDetermineRemedialAction_PrimaryDrift(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "self"}
	newAlignedPrimaryManager := func(serving clustermetadatapb.PoolerServingStatus) *MultiPoolerManager {
		// Rule names self, so intendedRole is PRIMARY and the record already agrees:
		// no role drift, isolating the physical-primary / serving decisions.
		return newTestManager(t,
			withServiceID(selfID),
			withRecord(newRecordFromProto(&clustermetadatapb.MultiPooler{
				Id:             selfID,
				Type:           clustermetadatapb.PoolerType_PRIMARY,
				SelfLeadership: &clustermetadatapb.LeaderObservation{LeaderId: selfID},
				ServingStatus:  serving,
			})),
			withRuleStore(&fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
					LeaderId:   selfID,
				},
			}}),
		)
	}

	runningPrimary := postgresState{pgctldAvailable: true, postgresRunning: true, isPrimary: true}

	t.Run("primary drift reconciles", func(t *testing.T) {
		pm := newAlignedPrimaryManager(clustermetadatapb.PoolerServingStatus_SERVING)
		// Components last saw a standby (false) but postgres is now a primary (true).
		got := pm.determineRemedialAction(t.Context(), runningPrimary, false /* lastAppliedPrimary */)
		require.Equal(t, remedialActionReconcileState, got)
	})

	t.Run("draining reconciles", func(t *testing.T) {
		// Role and primary aligned, but serving was left DRAINING (e.g. a demotion
		// that errored before re-serving). The monitor re-enables it.
		pm := newAlignedPrimaryManager(clustermetadatapb.PoolerServingStatus_DRAINING)
		got := pm.determineRemedialAction(t.Context(), runningPrimary, true /* lastAppliedPrimary */)
		require.Equal(t, remedialActionReconcileState, got)
	})

	t.Run("disabled is left alone", func(t *testing.T) {
		// DISABLED is a deliberate non-serving state (stopping/paused/operator);
		// the monitor must NOT auto-re-enable it.
		pm := newAlignedPrimaryManager(clustermetadatapb.PoolerServingStatus_DISABLED)
		got := pm.determineRemedialAction(t.Context(), runningPrimary, true /* lastAppliedPrimary */)
		require.Equal(t, remedialActionNone, got)
	})

	t.Run("no drift is a no-op", func(t *testing.T) {
		pm := newAlignedPrimaryManager(clustermetadatapb.PoolerServingStatus_SERVING)
		got := pm.determineRemedialAction(t.Context(), runningPrimary, true /* lastAppliedPrimary */)
		require.Equal(t, remedialActionNone, got)
	})
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
	runningPrimary := postgresState{pgctldAvailable: true, postgresRunning: true, isPrimary: true}

	selfPos := func(term int64) *clustermetadatapb.PoolerPosition {
		return &clustermetadatapb.PoolerPosition{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   selfID,
			},
		}
	}
	recordedPrimary := func(term int64, leader *clustermetadatapb.ID, addr *clustermetadatapb.PoolerAddress) *clustermetadatapb.ReplicationPrimary {
		return &clustermetadatapb.ReplicationPrimary{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   leader,
			},
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
				withRecord(newRecordFromProto(&clustermetadatapb.MultiPooler{
					Id:             selfID,
					Type:           clustermetadatapb.PoolerType_PRIMARY,
					SelfLeadership: &clustermetadatapb.LeaderObservation{LeaderId: selfID},
				})),
				withReplicationPrimary(tt.seedPrimary),
				withRuleStore(&fakeRuleStore{pos: tt.cachedPos}),
			)

			got := pm.determineRemedialAction(t.Context(), runningPrimary, runningPrimary.isPrimary)
			require.Equal(t, tt.expectedAction, got)
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
	newPM := func(t *testing.T, recordedRule *clustermetadatapb.ShardRule, addr *clustermetadatapb.PoolerAddress, selfTerm int64) *MultiPoolerManager {
		opts := []testManagerOption{
			withServiceID(selfID),
			withRuleStore(&fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{Rule: rule(selfTerm, selfID)}}),
		}
		if recordedRule != nil {
			// RewindReady so the rewind-ready gate is satisfied; cases that expect
			// nil do so for their own reason (not-ready is covered separately below).
			opts = append(opts, withReplicationPrimary(&clustermetadatapb.ReplicationPrimary{Rule: recordedRule, Primary: addr, RewindReady: true}))
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
		consensustest.SeedTerm(t, dir, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 6})
		cs := consensus.NewConsensusPromises(dir, selfID)
		_, err := cs.Load()
		require.NoError(t, err)
		pm := newTestManager(t,
			withServiceID(selfID),
			withPromises(cs),
			withReplicationPrimary(&clustermetadatapb.ReplicationPrimary{Rule: rule(5, otherID), Primary: otherAddr, RewindReady: true}),
			withRuleStore(&fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{Rule: rule(4, selfID)}}),
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
			withReplicationPrimary(&clustermetadatapb.ReplicationPrimary{Rule: rule(5, otherID), Primary: otherAddr, RewindReady: false}),
			withRuleStore(&fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{Rule: rule(4, selfID)}}),
		)
		require.Nil(t, pm.staleStandbyDemoteTarget())
	})
}

func TestShouldMarkRewindReady(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "self"}
	// newMgr builds a manager whose consensus state controls the two inputs
	// shouldMarkRewindReady reads beyond (rewindSourceReady, role): the resigned
	// term and the recorded ReplicationPrimary's rewind-ready flag.
	newMgr := func(resignedTerm int64, rp *clustermetadatapb.ReplicationPrimary) *MultiPoolerManager {
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
			Rule:        &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}},
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

	pm := &MultiPoolerManager{
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

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: actionlock.NewActionLock(),
		record: newRecordFromProto(&clustermetadatapb.MultiPooler{
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

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
		actionLock:   actionlock.NewActionLock(),
	}

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

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
		actionLock:   actionlock.NewActionLock(),
	}
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

	pm := &MultiPoolerManager{
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

	pm := &MultiPoolerManager{
		logger:       slog.Default(),
		actionLock:   actionlock.NewActionLock(),
		pgctldClient: mockPgctld,
		record: newRecordFromProto(&clustermetadatapb.MultiPooler{
			Type: clustermetadatapb.PoolerType_REPLICA,
		}),
	}

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
		return &clustermetadatapb.PoolerPosition{Rule: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
			LeaderId:   selfID,
		}}
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
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 8},
					LeaderId:   otherID,
				},
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

			multipooler := &clustermetadatapb.MultiPooler{
				Id:   selfID,
				Type: tc.poolerType,
			}
			if tc.poolerType == clustermetadatapb.PoolerType_PRIMARY {
				// Record invariant: a PRIMARY record must carry a self-leadership obs.
				multipooler.SelfLeadership = &clustermetadatapb.LeaderObservation{LeaderId: multipooler.Id}
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
				require.NoError(t, pm.consensusMgr.SetResignedLeaderAtTerm(lockCtx, tc.resignedBefore))
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
	pm := newRemedialActionTestManager(t, &clustermetadatapb.MultiPooler{
		Id:   selfID,
		Type: clustermetadatapb.PoolerType_PRIMARY,
		// A PRIMARY record must name itself as leader (the record invariant).
		SelfLeadership: &clustermetadatapb.LeaderObservation{LeaderId: selfID},
	}, withRuleStore(frs))

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	pm.takeRemedialAction(lockCtx, remedialActionReconcileGUC, postgresState{isPrimary: true})

	assert.True(t, frs.reconcileGUCCalled, "ReconcileGUC should have been called")
	assert.Equal(t, "postgres_running", pm.pgMonitorLastLoggedReason)
}

// TestTakeRemedialAction_ReconcileRole_AppliesRuleDerivedRole verifies that
// ReconcileRole applies the role the committed rule implies — transitioning the
// pooler to PRIMARY and recording the self-leadership observation built from the
// rule — even when the record's label still says REPLICA.
func TestTakeRemedialAction_ReconcileRole_AppliesRuleDerivedRole(t *testing.T) {
	multipooler := &clustermetadatapb.MultiPooler{
		Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"},
		Type: clustermetadatapb.PoolerType_REPLICA,
	}
	committed := &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}
	// The committed rule names this pooler leader, so the rule-derived role is
	// PRIMARY — ReconcileRole must publish PRIMARY plus the self-leadership obs
	// regardless of the stale REPLICA label on the record.
	pm := newRemedialActionTestManager(t, multipooler, withRuleStore(&fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{
		Rule: &clustermetadatapb.ShardRule{RuleNumber: committed, LeaderId: multipooler.Id},
	}}))

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	pm.takeRemedialAction(lockCtx, remedialActionReconcileState,
		postgresState{pgctldAvailable: true, postgresRunning: true, isPrimary: true})

	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.record.Type())
	obs := pm.record.SelfLeadership()
	require.NotNil(t, obs, "ReconcileRole must apply the rule-derived role with its self-leadership observation when the rule names this pooler leader")
	assert.Equal(t, multipooler.Id, obs.GetLeaderId())
	assert.Equal(t, committed, obs.GetLeaderRuleNumber())
}

func TestHasCompleteBackups_WithCompleteBackup(t *testing.T) {
	ctx := t.Context()
	poolerDir := t.TempDir()

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: actionlock.NewActionLock(),
		config:     &Config{},
		record:     newRecordFromProto(&clustermetadatapb.MultiPooler{PoolerDir: poolerDir}),
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

	// Mock ListBackups to return a complete backup
	// This is tested via the actual implementation
	// For unit test, we verify hasCompleteBackups returns false when no backups
	result := pm.hasCompleteBackups(ctx)

	// Without proper pgbackrest setup, should return false
	assert.False(t, result)
}

func TestHasCompleteBackups_NoBackups(t *testing.T) {
	ctx := t.Context()
	poolerDir := t.TempDir()

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: actionlock.NewActionLock(),
		config:     &Config{},
		record:     newRecordFromProto(&clustermetadatapb.MultiPooler{PoolerDir: poolerDir}),
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

	result := pm.hasCompleteBackups(ctx)

	assert.False(t, result)
}

func TestHasCompleteBackups_ActionLockTimeout(t *testing.T) {
	poolerDir := t.TempDir()

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: actionlock.NewActionLock(),
		config:     &Config{},
		record:     newRecordFromProto(&clustermetadatapb.MultiPooler{PoolerDir: poolerDir}),
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

	// Acquire the action lock to block hasCompleteBackups
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test-holder")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Create a context with timeout for hasCompleteBackups call
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	// hasCompleteBackups should return false when it can't acquire lock
	result := pm.hasCompleteBackups(ctx)

	assert.False(t, result)
}

func TestStartPostgres_Success(t *testing.T) {
	ctx := t.Context()

	mockPgctld := &mockPgctldClient{}

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
	}

	err := pm.startPostgres(ctx)

	require.NoError(t, err)
	assert.True(t, mockPgctld.startCalled)
}

func TestStartPostgres_PgctldUnavailable(t *testing.T) {
	ctx := t.Context()

	pm := &MultiPoolerManager{
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

	pm := &MultiPoolerManager{
		pgctldClient: mockPgctld,
		logger:       slog.Default(),
	}

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

	pm := NewTestMultiPoolerManager(t)
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

	pm := NewTestMultiPoolerManager(t)
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

	pm := NewTestMultiPoolerManager(t)
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

	pm := NewTestMultiPoolerManager(t)
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
		isPrimary:                true,
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
		{"isPrimary", func() postgresState { s := base; s.isPrimary = false; return s }()},
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
				Rule:    mkRule(5, recordedID),
				Primary: mkAddress(recordedHost, recordedPort),
			},
			seedManualStop: true,
			want:           false,
		},
		{
			name: "PrimaryFieldMissing",
			// rule recorded but primary contact info absent -> nothing to compare
			seedRP: &clustermetadatapb.ReplicationPrimary{Rule: mkRule(5, recordedID)},
			want:   false,
		},
		{
			name: "SelfIsLeader",
			// recorded rule names this pooler — primary-side case, not replica
			// reconciliation
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Rule:    mkRule(5, selfID),
				Primary: mkAddress(recordedHost, recordedPort),
			},
			want: false,
		},
		{
			name: "RecordedRuleRevoked",
			// revocation at term 9 outranks rule at term 5 -> reconcile would
			// race the in-flight Recruit/Promote
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Rule:    mkRule(5, recordedID),
				Primary: mkAddress(recordedHost, recordedPort),
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
				Rule:    mkRule(5, recordedID),
				Primary: mkAddress("", recordedPort),
			},
			want: false,
		},
		{
			name: "RecordedPortZero",
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Rule:    mkRule(5, recordedID),
				Primary: mkAddress(recordedHost, 0),
			},
			want: false,
		},
		{
			name: "ReadPrimaryConnInfoErrors",
			// Conservative: don't trigger reconciliation we can't verify
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Rule:    mkRule(5, recordedID),
				Primary: mkAddress(recordedHost, recordedPort),
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
				Rule:    mkRule(5, recordedID),
				Primary: mkAddress(recordedHost, recordedPort),
			},
			expectQuery:  true,
			mockConnInfo: "",
			want:         true,
		},
		{
			name: "LiveConnInfoUnparseable_TreatedAsDrift",
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Rule:    mkRule(5, recordedID),
				Primary: mkAddress(recordedHost, recordedPort),
			},
			expectQuery:  true,
			mockConnInfo: "host=primary.example.com port=invalid user=replicator",
			want:         true,
		},
		{
			name: "LiveConnInfoMatchesRecorded",
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Rule:    mkRule(5, recordedID),
				Primary: mkAddress(recordedHost, recordedPort),
			},
			expectQuery:  true,
			mockConnInfo: "host=primary.example.com port=5432 user=replicator",
			want:         false,
		},
		{
			name: "LiveConnInfoHostMismatch",
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Rule:    mkRule(5, recordedID),
				Primary: mkAddress(recordedHost, recordedPort),
			},
			expectQuery:  true,
			mockConnInfo: "host=other.example.com port=5432 user=replicator",
			want:         true,
		},
		{
			name: "LiveConnInfoPortMismatch",
			seedRP: &clustermetadatapb.ReplicationPrimary{
				Rule:    mkRule(5, recordedID),
				Primary: mkAddress(recordedHost, recordedPort),
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
