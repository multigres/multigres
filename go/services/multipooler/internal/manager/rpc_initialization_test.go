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

package manager

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// NewTestMultiPoolerManager builds a MultiPoolerManager for tests with a
// minimal MultiPooler (MVP table_group/shard, a temp PoolerDir, and a valid
// service ID). Tests override pm.record fields, pm.pgctldClient, etc.
// as needed.
func NewTestMultiPoolerManager(t *testing.T) *MultiPoolerManager {
	t.Helper()
	mp := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "test-cell",
			Name:      "test-pooler",
		},
		ShardKey: &clustermetadatapb.ShardKey{
			TableGroup: "default",
			Shard:      "0-inf",
		},
		PoolerDir: t.TempDir(),
	}
	pm, err := NewMultiPoolerManager(slog.Default(), mp, &Config{})
	require.NoError(t, err)
	// Swap in a fake rule store so tests that exercise ObservePosition /
	// CachedPosition don't crash on the real store's nil query service.
	pm.rules = &fakeRuleStore{}
	return pm
}

// TestIsInitialized verifies that isInitialized requires both the marker file and
// multigres schema to be present. The marker file (MULTIGRES_INITIALIZED) is written
// by setInitialized() only after the full bootstrap sequence completes:
//   - Primary: initdb + multigres schema + pgBackRest stanza-create + backup
//   - Replica:  restore from canonical backup + postgres started
//
// The marker file prevents false positives on crash-restart between schema creation
// and backup completion. The schema check is a safety invariant: schema existence is
// necessary (though not sufficient) for a node to be initialized.
func TestIsInitialized(t *testing.T) {
	t.Run("returns false when no data directory exists", func(t *testing.T) {
		ctx := t.Context()
		poolerDir := t.TempDir()
		pm := &MultiPoolerManager{
			config: &Config{},
			record: newRecordFromProto(&clustermetadatapb.MultiPooler{PoolerDir: poolerDir}),
		}

		assert.False(t, pm.isInitialized(ctx))
	})

	t.Run("returns false when data directory exists but marker file is absent", func(t *testing.T) {
		ctx := t.Context()
		poolerDir := t.TempDir()
		dataDir := filepath.Join(poolerDir, "pg_data")
		require.NoError(t, os.MkdirAll(dataDir, 0o755))
		// Simulate postgres having run initdb and created the multigres schema,
		// but the full bootstrap sequence (backup) did not complete: no marker file.
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16"), 0o644))

		pm := &MultiPoolerManager{
			config: &Config{},
			record: newRecordFromProto(&clustermetadatapb.MultiPooler{PoolerDir: poolerDir}),
		}

		assert.False(t, pm.isInitialized(ctx))
		// Cached state must not be poisoned.
		assert.False(t, pm.initialized)
	})

	t.Run("returns true when marker file is present but postgres is unreachable; cache not set", func(t *testing.T) {
		ctx := t.Context()
		poolerDir := t.TempDir()
		dataDir := filepath.Join(poolerDir, "pg_data")
		markerDir := filepath.Join(dataDir, constants.MultigresMarkerDirectory)
		require.NoError(t, os.MkdirAll(markerDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(markerDir, multigresInitMarker), []byte("initialized\n"), 0o644))
		t.Setenv(constants.PgDataDirEnvVar, dataDir)

		pm := &MultiPoolerManager{
			config: &Config{},
			record: newRecordFromProto(&clustermetadatapb.MultiPooler{PoolerDir: poolerDir}),
		}

		// Marker present, postgres unreachable → trust marker, return true.
		assert.True(t, pm.isInitialized(ctx))
		// Cache is NOT set since we could not confirm the schema; re-check on next call.
		assert.False(t, pm.initialized)
	})

	t.Run("fast path: returns true when initialized is already cached", func(t *testing.T) {
		poolerDir := t.TempDir()
		// No data directory at all, but in-memory cache is true.
		pm := &MultiPoolerManager{
			config:      &Config{},
			record:      newRecordFromProto(&clustermetadatapb.MultiPooler{PoolerDir: poolerDir}),
			initialized: true,
		}

		assert.True(t, pm.isInitialized(t.Context()))
	})
}

func TestHelperMethods(t *testing.T) {
	t.Run("hasDataDirectory", func(t *testing.T) {
		poolerDir := t.TempDir()
		dataDir := filepath.Join(poolerDir, "pg_data")
		t.Setenv(constants.PgDataDirEnvVar, dataDir)

		multiPooler := &clustermetadatapb.MultiPooler{PoolerDir: poolerDir}
		pm := &MultiPoolerManager{config: &Config{}, record: newRecordFromProto(multiPooler)}

		// Initially no data directory
		assert.False(t, pm.hasDataDirectory())

		// Create data directory with PG_VERSION file (simulating initialized postgres)
		require.NoError(t, os.MkdirAll(dataDir, 0o755))
		pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
		require.NoError(t, os.WriteFile(pgVersionFile, []byte("16"), 0o644))

		// Now should return true
		assert.True(t, pm.hasDataDirectory())
	})

	t.Run("getShardID", func(t *testing.T) {
		serviceID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "test-cell",
			Name:      "test-pooler",
		}

		multipooler := &clustermetadatapb.MultiPooler{
			Id: serviceID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testgroup",
				Shard:      "shard-123",
			},
		}

		pm := &MultiPoolerManager{
			record: newRecordFromProto(multipooler),
		}

		assert.Equal(t, "shard-123", pm.getShardID())
	})

	t.Run("removeDataDirectory safety checks", func(t *testing.T) {
		poolerDir := t.TempDir()
		multiPooler := &clustermetadatapb.MultiPooler{PoolerDir: poolerDir}
		pm := &MultiPoolerManager{config: &Config{}, record: newRecordFromProto(multiPooler), logger: slog.Default()}

		// Create data directory
		dataDir := filepath.Join(poolerDir, "pg_data")
		require.NoError(t, os.MkdirAll(dataDir, 0o755))
		t.Setenv(constants.PgDataDirEnvVar, dataDir)

		// Should succeed with valid directory
		err := pm.removeDataDirectory()
		require.NoError(t, err)

		// Verify directory was removed
		_, err = os.Stat(dataDir)
		assert.True(t, os.IsNotExist(err))
	})

	// Load-bearing for the crashed-bootstrap recovery path: the retry calls
	// removeDataDirectory when the sentinel is present, and a prior crash may
	// have already removed the directory. Idempotence lets us treat that nil
	// return as "already clean" instead of a distinguishable special case.
	t.Run("removeDataDirectory is idempotent on an already-deleted dir", func(t *testing.T) {
		poolerDir := t.TempDir()
		pm := &MultiPoolerManager{
			config: &Config{},
			record: newRecordFromProto(&clustermetadatapb.MultiPooler{PoolerDir: poolerDir}),
			logger: slog.Default(),
		}

		dataDir := filepath.Join(poolerDir, "pg_data")
		t.Setenv(constants.PgDataDirEnvVar, dataDir)
		// dataDir was never created — removeDataDirectory should still return nil.

		require.NoError(t, pm.removeDataDirectory())
		// Calling again is also a no-op.
		require.NoError(t, pm.removeDataDirectory())
	})

	// Covers the "real removal failure" branch in the sentinel-recovery path:
	// on a non-nil error, the caller must surface it rather than silently
	// proceed — otherwise a permission regression would go unnoticed.
	t.Run("removeDataDirectory surfaces a permission-denied error", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("filesystem permissions do not apply to root")
		}
		poolerDir := t.TempDir()
		pm := &MultiPoolerManager{
			config: &Config{},
			record: newRecordFromProto(&clustermetadatapb.MultiPooler{PoolerDir: poolerDir}),
			logger: slog.Default(),
		}

		// Put pg_data under a read-only parent so unlinking pg_data requires
		// write permission on readonlyParent, which we have denied.
		readonlyParent := filepath.Join(poolerDir, "readonly")
		dataDir := filepath.Join(readonlyParent, "pg_data")
		require.NoError(t, os.MkdirAll(dataDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16"), 0o644))
		require.NoError(t, os.Chmod(readonlyParent, 0o500))
		t.Cleanup(func() {
			// Restore perms so t.TempDir's cleanup can remove everything.
			_ = os.Chmod(readonlyParent, 0o755)
		})
		t.Setenv(constants.PgDataDirEnvVar, dataDir)

		err := pm.removeDataDirectory()
		require.Error(t, err)
		assert.True(t, errors.Is(err, fs.ErrPermission), "expected permission error, got: %v", err)
	})
}

// MonitorPostgres Tests

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

	state, err := pm.discoverPostgresState(ctx)
	require.NoError(t, err)

	assert.True(t, state.pgctldAvailable)
	assert.True(t, state.dirInitialized)
	assert.True(t, state.postgresRunning)
	assert.False(t, state.bootstrapSentinelPresent)
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

// TODO: move TestDetermineRemedialAction and TestTakeRemedialAction_* to a dedicated postgres_monitor_test.go

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
	recordedPrimary := func(term int64, leader *clustermetadatapb.ID, addr *clustermetadatapb.PoolerAddress) *consensus.ConsensusState {
		cs := consensus.NewConsensusState("", selfID)
		cs.RecordTermPrimary(&clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
			LeaderId:   leader,
		}, addr)
		return cs
	}

	tests := []struct {
		name               string
		state              postgresState
		poolerType         clustermetadatapb.PoolerType
		consensusState     *consensus.ConsensusState
		cachedPos          *clustermetadatapb.PoolerPosition
		primaryTerm        int64
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
			expectedAction: remedialActionReconcileRole,
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
			expectedAction: remedialActionReconcileRole,
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
			consensusState: recordedPrimary(5, otherID, otherAddr),
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
			primaryTerm:        5,
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
			primaryTerm:        5,
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
			primaryTerm:        5,
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
			expectedAction: remedialActionReconcileRole,
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
			pm := &MultiPoolerManager{
				serviceID: selfID,
				record:    newRecordFromProto(seed),
			}
			if tt.consensusState != nil {
				pm.consensusState = tt.consensusState
			} else {
				pm.consensusState = consensus.NewConsensusState("", selfID)
			}
			pm.resignedLeaderAtTerm = tt.resignedLeaderTerm
			pm.rules = &fakeRuleStore{pos: tt.cachedPos, inconsistentGUC: tt.inconsistentGUC}
			tt.state.primaryTerm = tt.primaryTerm

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
	runningPrimary := postgresState{pgctldAvailable: true, postgresRunning: true, isPrimary: true}

	selfPos := func(term int64) *clustermetadatapb.PoolerPosition {
		return &clustermetadatapb.PoolerPosition{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   selfID,
			},
		}
	}
	recordedPrimary := func(term int64, leader *clustermetadatapb.ID, addr *clustermetadatapb.PoolerAddress) *consensus.ConsensusState {
		cs := consensus.NewConsensusState("", selfID)
		cs.RecordTermPrimary(&clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
			LeaderId:   leader,
		}, addr)
		return cs
	}

	tests := []struct {
		name           string
		consensusState *consensus.ConsensusState
		cachedPos      *clustermetadatapb.PoolerPosition
		expectedAction remedialAction
	}{
		{
			// Recorded rule outranks our applied position and names another
			// leader with usable contact info: retry the demote.
			name:           "higher_rule_names_other_leader_demotes",
			consensusState: recordedPrimary(5, otherID, otherAddr),
			cachedPos:      selfPos(4),
			expectedAction: remedialActionDemoteStalePrimary,
		},
		{
			// No recorded primary: we have nothing to rewind against or stream
			// from, so wait rather than restart blind.
			name:           "no_recorded_primary_waits",
			consensusState: consensus.NewConsensusState("", selfID),
			cachedPos:      selfPos(4),
			expectedAction: remedialActionNone,
		},
		{
			// Recorded rule advanced but carries no contact info: still no
			// source to connect to, so wait.
			name:           "recorded_primary_without_address_waits",
			consensusState: recordedPrimary(5, otherID, nil),
			cachedPos:      selfPos(4),
			expectedAction: remedialActionNone,
		},
		{
			// Recorded rule is not higher than our applied position: it is stale
			// relative to us and must not trigger a demote.
			name:           "recorded_rule_not_higher_waits",
			consensusState: recordedPrimary(4, otherID, otherAddr),
			cachedPos:      selfPos(4),
			expectedAction: remedialActionNone,
		},
		{
			// Recorded rule names us as the leader: not a "superseded" case.
			name:           "recorded_rule_names_self_waits",
			consensusState: recordedPrimary(5, selfID, &clustermetadatapb.PoolerAddress{Id: selfID, Host: "self-host", PostgresPort: 5432}),
			cachedPos:      selfPos(4),
			expectedAction: remedialActionNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm := &MultiPoolerManager{
				serviceID: selfID,
				record: newRecordFromProto(&clustermetadatapb.MultiPooler{
					Id:             selfID,
					Type:           clustermetadatapb.PoolerType_PRIMARY,
					SelfLeadership: &clustermetadatapb.LeaderObservation{LeaderId: selfID},
				}),
			}
			pm.consensusState = tt.consensusState
			pm.rules = &fakeRuleStore{pos: tt.cachedPos}

			got := pm.determineRemedialAction(t.Context(), runningPrimary)
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
		cs := consensus.NewConsensusState(t.TempDir(), selfID)
		if recordedRule != nil {
			cs.RecordTermPrimary(recordedRule, addr)
		}
		return &MultiPoolerManager{
			serviceID:      selfID,
			consensusState: cs,
			rules:          &fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{Rule: rule(selfTerm, selfID)}},
		}
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
		pm := newPM(t, rule(5, otherID), otherAddr, 4)
		lockCtx, err := actionlock.NewActionLock().Acquire(t.Context(), "test")
		require.NoError(t, err)
		// Revoke everything below term 6, which revokes the recorded rule at term 5.
		require.NoError(t, pm.consensusState.UpdateTermAndSave(lockCtx, 6))
		require.Nil(t, pm.staleStandbyDemoteTarget())
	})

	t.Run("superseded by another leader at a higher rule -> returns target", func(t *testing.T) {
		pm := newPM(t, rule(5, otherID), otherAddr, 4)
		got := pm.staleStandbyDemoteTarget()
		require.NotNil(t, got)
		assert.Equal(t, "other-host", got.GetHost())
		assert.Equal(t, int32(5432), got.GetPostgresPort())
	})
}

// TestIntendedRole verifies that intendedRole() derives the PoolerType from
// the freshest rule this pooler knows about (latestRule, which merges the
// consensus-recorded replication primary with the rule store's cached
// position): UNKNOWN when no rule exists, PRIMARY when the freshest rule names
// this pooler as leader, REPLICA when it names another pooler. The
// consensus-recorded rule can outrank our cached position — that is the
// stale-primary signal, where intendedRole() reports REPLICA even though our
// cached rule still names self.
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

// Note: Type adjustment action execution (AdjustTypeToPrimary, AdjustTypeToReplica) is tested in
// integration tests because it requires topoClient and full infrastructure.
// The decision logic for type adjustment is tested in TestDetermineRemedialAction above.
// The resignation signal behavior is tested below without full infrastructure.

func newRemedialActionTestManager(t *testing.T, multipooler *clustermetadatapb.MultiPooler) *MultiPoolerManager {
	t.Helper()
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))
	record, err := newPoolerRecord(slog.Default(), ts, multipooler)
	require.NoError(t, err)
	return &MultiPoolerManager{
		logger:            slog.Default(),
		actionLock:        actionlock.NewActionLock(),
		record:            record,
		serviceID:         multipooler.Id,
		topoClient:        ts,
		servingState:      NewStateManager(slog.Default(), record),
		cohortEligibility: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
	}
}

func TestTakeRemedialAction_ResignationSignal(t *testing.T) {
	otherID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "other-pooler"}
	tests := []struct {
		name           string
		action         remedialAction
		poolerType     clustermetadatapb.PoolerType
		primaryTerm    int64                             // set in consensus state before action
		resignedBefore int64                             // set resignedLeaderAtTerm before action (0 = don't set)
		cachedPos      *clustermetadatapb.PoolerPosition // rule the monitor reads for ReconcileRole
		wantAvStatus   *clustermetadatapb.AvailabilityStatus
	}{
		{
			name:        "ResignLeadership sets resignation at primary_term",
			action:      remedialActionResignLeadership,
			poolerType:  clustermetadatapb.PoolerType_PRIMARY,
			primaryTerm: 5,
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
			name:        "ResignLeadership sets no resignation when primary_term is zero",
			action:      remedialActionResignLeadership,
			poolerType:  clustermetadatapb.PoolerType_PRIMARY,
			primaryTerm: 0,
			wantAvStatus: &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
				},
			},
		},
		{
			name:           "ReconcileRole does not clear existing resignation signal",
			action:         remedialActionReconcileRole,
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
				Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"},
				Type: tc.poolerType,
			}
			if tc.poolerType == clustermetadatapb.PoolerType_PRIMARY {
				// Record invariant: a PRIMARY record must carry a self-leadership obs.
				multipooler.SelfLeadership = &clustermetadatapb.LeaderObservation{LeaderId: multipooler.Id}
			}
			pm := newRemedialActionTestManager(t, multipooler)
			pm.rules = &fakeRuleStore{pos: tc.cachedPos}

			cs := consensus.NewConsensusState(t.TempDir(), nil)
			pm.consensusState = cs

			lockCtx, err := pm.actionLock.Acquire(ctx, "test")
			require.NoError(t, err)
			defer pm.actionLock.Release(lockCtx)

			require.NoError(t, cs.UpdateTermAndSave(lockCtx, 1))

			if tc.resignedBefore != 0 {
				require.NoError(t, pm.setResignedLeaderAtTerm(lockCtx, tc.resignedBefore))
			}

			pm.takeRemedialAction(lockCtx, tc.action, postgresState{primaryTerm: tc.primaryTerm})

			assert.Equal(t, tc.wantAvStatus, pm.buildAvailabilityStatus())
		})
	}
}

func TestTakeRemedialAction_ReconcileGUC(t *testing.T) {
	ctx := t.Context()

	frs := &fakeRuleStore{}
	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		actionLock: actionlock.NewActionLock(),
		rules:      frs,
		record: newRecordFromProto(&clustermetadatapb.MultiPooler{
			Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"},
			Type: clustermetadatapb.PoolerType_PRIMARY,
			// A PRIMARY record must name itself as leader (the record invariant).
			SelfLeadership: &clustermetadatapb.LeaderObservation{
				LeaderId: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"},
			},
		}),
	}
	pm.consensusState = consensus.NewConsensusState("", nil)

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	pm.takeRemedialAction(lockCtx, remedialActionReconcileGUC, postgresState{isPrimary: true})

	assert.True(t, frs.reconcileGUCCalled, "ReconcileGUC should have been called")
	assert.Equal(t, "postgres_running", pm.pgMonitorLastLoggedReason)
}

// TestUpdateTopologyAfterPromotion_PublishesSelfLeadership verifies the
// promotion path records a self-leadership observation naming this pooler under
// the rule it was promoted under, alongside Type=PRIMARY + SERVING.
func TestUpdateTopologyAfterPromotion_PublishesSelfLeadership(t *testing.T) {
	multipooler := &clustermetadatapb.MultiPooler{
		Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test-pooler"},
		Type: clustermetadatapb.PoolerType_REPLICA,
	}
	pm := newRemedialActionTestManager(t, multipooler)

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// The pooler is promoted under this rule, which names it as leader.
	rule := &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 7, LeaderSubterm: 2},
		LeaderId:   multipooler.Id,
	}
	require.NoError(t, pm.updateTopologyAfterPromotion(lockCtx, &promotionState{}, rule))

	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, pm.record.Type())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, pm.record.ServingStatus())
	obs := pm.record.SelfLeadership()
	require.NotNil(t, obs, "promotion must publish a self-leadership observation")
	assert.Equal(t, multipooler.Id, obs.GetLeaderId())
	assert.Equal(t, rule.GetRuleNumber(), obs.GetLeaderRuleNumber())
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
	pm := newRemedialActionTestManager(t, multipooler)
	pm.consensusState = consensus.NewConsensusState("", multipooler.Id)
	committed := &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}
	// The committed rule names this pooler leader, so the rule-derived role is
	// PRIMARY — ReconcileRole must publish PRIMARY plus the self-leadership obs
	// regardless of the stale REPLICA label on the record.
	pm.rules = &fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{
		Rule: &clustermetadatapb.ShardRule{RuleNumber: committed, LeaderId: multipooler.Id},
	}}

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	pm.takeRemedialAction(lockCtx, remedialActionReconcileRole,
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

// Integration Tests for MonitorPostgres

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
		primaryTerm:              42,
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
		{"primaryTerm", func() postgresState { s := base; s.primaryTerm = 99; return s }()},
	}
	for _, tc := range tests {
		t.Run("differs in "+tc.name, func(t *testing.T) {
			assert.False(t, postgresStateEqual(base, tc.other))
		})
	}
}

// Mock pgctld client for testing
type mockPgctldClient struct {
	statusResponse *pgctldpb.StatusResponse
	statusError    error
	startCalled    bool
	startError     error
	restartCalled  bool
	restartError   error
}

func (m *mockPgctldClient) Status(ctx context.Context, req *pgctldpb.StatusRequest, opts ...grpc.CallOption) (*pgctldpb.StatusResponse, error) {
	if m.statusError != nil {
		return nil, m.statusError
	}
	if m.statusResponse != nil {
		return m.statusResponse, nil
	}
	return &pgctldpb.StatusResponse{
		Status: pgctldpb.ServerStatus_RUNNING,
	}, nil
}

func (m *mockPgctldClient) Start(ctx context.Context, req *pgctldpb.StartRequest, opts ...grpc.CallOption) (*pgctldpb.StartResponse, error) {
	m.startCalled = true
	if m.startError != nil {
		return nil, m.startError
	}
	return &pgctldpb.StartResponse{}, nil
}

func (m *mockPgctldClient) Stop(ctx context.Context, req *pgctldpb.StopRequest, opts ...grpc.CallOption) (*pgctldpb.StopResponse, error) {
	return &pgctldpb.StopResponse{}, nil
}

func (m *mockPgctldClient) Restart(ctx context.Context, req *pgctldpb.RestartRequest, opts ...grpc.CallOption) (*pgctldpb.RestartResponse, error) {
	m.restartCalled = true
	if m.restartError != nil {
		return nil, m.restartError
	}
	return &pgctldpb.RestartResponse{}, nil
}

func (m *mockPgctldClient) InitDataDir(ctx context.Context, req *pgctldpb.InitDataDirRequest, opts ...grpc.CallOption) (*pgctldpb.InitDataDirResponse, error) {
	return &pgctldpb.InitDataDirResponse{}, nil
}

func (m *mockPgctldClient) ReloadConfig(ctx context.Context, req *pgctldpb.ReloadConfigRequest, opts ...grpc.CallOption) (*pgctldpb.ReloadConfigResponse, error) {
	return &pgctldpb.ReloadConfigResponse{}, nil
}

func (m *mockPgctldClient) Version(ctx context.Context, req *pgctldpb.VersionRequest, opts ...grpc.CallOption) (*pgctldpb.VersionResponse, error) {
	return &pgctldpb.VersionResponse{}, nil
}

func (m *mockPgctldClient) PgRewind(ctx context.Context, req *pgctldpb.PgRewindRequest, opts ...grpc.CallOption) (*pgctldpb.PgRewindResponse, error) {
	return &pgctldpb.PgRewindResponse{}, nil
}

// mockPgctldClientWithCounter extends mockPgctldClient with call counters
type mockPgctldClientWithCounter struct {
	mockPgctldClient
	startCallCount int
}

func (m *mockPgctldClientWithCounter) Start(ctx context.Context, req *pgctldpb.StartRequest, opts ...grpc.CallOption) (*pgctldpb.StartResponse, error) {
	m.startCallCount++
	return m.mockPgctldClient.Start(ctx, req, opts...)
}
