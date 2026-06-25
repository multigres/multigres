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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"

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
	setTestRuleStore(pm, &fakeRuleStore{})
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

// TODO: move TestDetermineRemedialAction and TestTakeRemedialAction_* to a dedicated postgres_monitor_test.go

// Note: Type adjustment action execution (AdjustTypeToPrimary, AdjustTypeToReplica) is tested in
// integration tests because it requires topoClient and full infrastructure.
// The decision logic for type adjustment is tested in TestDetermineRemedialAction above.
// The resignation signal behavior is tested below without full infrastructure.

func newRemedialActionTestManager(t *testing.T, multipooler *clustermetadatapb.MultiPooler, opts ...testManagerOption) *MultiPoolerManager {
	t.Helper()
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))
	record, err := newPoolerRecord(slog.Default(), ts, multipooler)
	require.NoError(t, err)
	// Default the recorded service identity to this pooler unless an option
	// overrode it, so the promises default is rooted at the right ID.
	cfg := resolveTestManagerConfig(t, append([]testManagerOption{withServiceID(multipooler.Id)}, opts...)...)
	pm := &MultiPoolerManager{
		logger:       slog.Default(),
		actionLock:   actionlock.NewActionLock(),
		record:       record,
		serviceID:    multipooler.Id,
		topoClient:   ts,
		servingState: NewStateManager(slog.Default(), record),
		consensusMgr: cfg.consensusManager(),
	}
	cfg.seedLockedState(t, pm)
	return pm
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

// Integration Tests for MonitorPostgres

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
