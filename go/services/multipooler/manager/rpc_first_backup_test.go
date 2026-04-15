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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	commontypes "github.com/multigres/multigres/go/common/types"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// stubPgctldClient is a minimal pgctld stub that returns errors for all calls.
// It satisfies pgctldpb.PgCtldClient without a real gRPC connection.
type stubPgctldClient struct{}

func (s *stubPgctldClient) Start(_ context.Context, _ *pgctldpb.StartRequest, _ ...grpc.CallOption) (*pgctldpb.StartResponse, error) {
	return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: not available")
}

func (s *stubPgctldClient) Stop(_ context.Context, _ *pgctldpb.StopRequest, _ ...grpc.CallOption) (*pgctldpb.StopResponse, error) {
	return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: not available")
}

func (s *stubPgctldClient) Restart(_ context.Context, _ *pgctldpb.RestartRequest, _ ...grpc.CallOption) (*pgctldpb.RestartResponse, error) {
	return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: not available")
}

func (s *stubPgctldClient) ReloadConfig(_ context.Context, _ *pgctldpb.ReloadConfigRequest, _ ...grpc.CallOption) (*pgctldpb.ReloadConfigResponse, error) {
	return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: not available")
}

func (s *stubPgctldClient) Status(_ context.Context, _ *pgctldpb.StatusRequest, _ ...grpc.CallOption) (*pgctldpb.StatusResponse, error) {
	return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: not available")
}

func (s *stubPgctldClient) Version(_ context.Context, _ *pgctldpb.VersionRequest, _ ...grpc.CallOption) (*pgctldpb.VersionResponse, error) {
	return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: not available")
}

func (s *stubPgctldClient) InitDataDir(_ context.Context, _ *pgctldpb.InitDataDirRequest, _ ...grpc.CallOption) (*pgctldpb.InitDataDirResponse, error) {
	return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: not available")
}

func (s *stubPgctldClient) PgRewind(_ context.Context, _ *pgctldpb.PgRewindRequest, _ ...grpc.CallOption) (*pgctldpb.PgRewindResponse, error) {
	return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: not available")
}

var _ pgctldpb.PgCtldClient = (*stubPgctldClient)(nil)

// successStubPgctldClient is a pgctld stub that succeeds for all calls.
// InitDataDir creates the pg_data directory to simulate what real pgctld does.
type successStubPgctldClient struct {
	pgDataDir string // set by test; InitDataDir creates this directory
}

func (s *successStubPgctldClient) Start(context.Context, *pgctldpb.StartRequest, ...grpc.CallOption) (*pgctldpb.StartResponse, error) {
	return &pgctldpb.StartResponse{}, nil
}

func (s *successStubPgctldClient) Stop(context.Context, *pgctldpb.StopRequest, ...grpc.CallOption) (*pgctldpb.StopResponse, error) {
	return &pgctldpb.StopResponse{}, nil
}

func (s *successStubPgctldClient) Restart(context.Context, *pgctldpb.RestartRequest, ...grpc.CallOption) (*pgctldpb.RestartResponse, error) {
	return &pgctldpb.RestartResponse{}, nil
}

func (s *successStubPgctldClient) ReloadConfig(context.Context, *pgctldpb.ReloadConfigRequest, ...grpc.CallOption) (*pgctldpb.ReloadConfigResponse, error) {
	return &pgctldpb.ReloadConfigResponse{}, nil
}

func (s *successStubPgctldClient) Status(context.Context, *pgctldpb.StatusRequest, ...grpc.CallOption) (*pgctldpb.StatusResponse, error) {
	return &pgctldpb.StatusResponse{}, nil
}

func (s *successStubPgctldClient) Version(context.Context, *pgctldpb.VersionRequest, ...grpc.CallOption) (*pgctldpb.VersionResponse, error) {
	return &pgctldpb.VersionResponse{}, nil
}

func (s *successStubPgctldClient) InitDataDir(context.Context, *pgctldpb.InitDataDirRequest, ...grpc.CallOption) (*pgctldpb.InitDataDirResponse, error) {
	if s.pgDataDir != "" {
		_ = os.MkdirAll(s.pgDataDir, 0o755)
	}
	return &pgctldpb.InitDataDirResponse{}, nil
}

func (s *successStubPgctldClient) PgRewind(context.Context, *pgctldpb.PgRewindRequest, ...grpc.CallOption) (*pgctldpb.PgRewindResponse, error) {
	return &pgctldpb.PgRewindResponse{}, nil
}

var _ pgctldpb.PgCtldClient = (*successStubPgctldClient)(nil)

// TestLoadDurabilityPolicy verifies that loadDurabilityPolicy returns the
// bootstrap_durability_policy from the topology database record.
func TestLoadDurabilityPolicy(t *testing.T) {
	ctx := t.Context()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()

	const dbName = "testdb"
	require.NoError(t, store.CreateDatabase(ctx, dbName, &clustermetadatapb.Database{
		Name:                      dbName,
		BootstrapDurabilityPolicy: topoclient.AtLeastN(2),
	}))

	pm := &MultiPoolerManager{
		topoClient:  store,
		multipooler: &clustermetadatapb.MultiPooler{Database: dbName},
	}

	got, err := pm.loadDurabilityPolicy(ctx)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N, got.QuorumType)
	assert.Equal(t, int32(2), got.RequiredCount)
}

// TestLoadDurabilityPolicy_NoPolicyConfigured verifies that a database without a
// durability_policy field returns FAILED_PRECONDITION.
func TestLoadDurabilityPolicy_NoPolicyConfigured(t *testing.T) {
	ctx := t.Context()

	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()

	const dbName = "testdb"
	err := store.CreateDatabase(ctx, dbName, &clustermetadatapb.Database{
		Name: dbName,
		// DurabilityPolicy intentionally left empty
	})
	require.NoError(t, err)

	pm := &MultiPoolerManager{
		topoClient:  store,
		multipooler: &clustermetadatapb.MultiPooler{Database: dbName},
	}

	_, err = pm.loadDurabilityPolicy(ctx)
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	assert.Contains(t, err.Error(), "no durability_policy configured")
}

// TestCreateFirstBackupAndInitialize_NoDurabilityPolicy verifies that a missing
// durability_policy fails before any expensive work (initdb). The stub pgctld client
// returns UNAVAILABLE for InitDataDir; if the ordering were wrong we would see
// UNAVAILABLE rather than FAILED_PRECONDITION.
func TestCreateFirstBackupAndInitialize_NoDurabilityPolicy(t *testing.T) {
	ctx := t.Context()

	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()

	const dbName = "testdb"
	err := store.CreateDatabase(ctx, dbName, &clustermetadatapb.Database{
		Name: dbName,
		// DurabilityPolicy intentionally left empty
	})
	require.NoError(t, err)

	poolerDir := t.TempDir()
	// No PG_VERSION written — hasDataDirectory() returns false.

	pm := &MultiPoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   NewActionLock(),
		pgctldClient: &stubPgctldClient{},
		multipooler: &clustermetadatapb.MultiPooler{
			Database:   dbName,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
			PoolerDir:  poolerDir,
		},
		config: &Config{},
	}

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	busy, backupFound, err := pm.createFirstBackupAndInitializeLocked(lockCtx)
	require.Error(t, err)
	assert.False(t, busy)
	assert.False(t, backupFound)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	assert.Contains(t, err.Error(), "no durability_policy configured")
}

// TestCreateFirstBackupAndInitialize_DataDirExists verifies that the function refuses
// to proceed when the data directory already exists.
func TestCreateFirstBackupAndInitialize_DataDirExists(t *testing.T) {
	ctx := t.Context()

	poolerDir := t.TempDir()
	dataDir := filepath.Join(poolerDir, "pg_data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	// Write PG_VERSION so hasDataDirectory() returns true
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16"), 0o644))
	t.Setenv(constants.PgDataDirEnvVar, dataDir)

	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()

	pm := &MultiPoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   NewActionLock(),
		pgctldClient: &stubPgctldClient{},
		multipooler: &clustermetadatapb.MultiPooler{
			Database:   "testdb",
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
			PoolerDir:  poolerDir,
		},
		config: &Config{},
	}

	// Acquire the action lock as the monitor would do before calling this function
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	busy, backupFound, err := pm.createFirstBackupAndInitializeLocked(lockCtx)
	require.Error(t, err)
	assert.False(t, busy)
	assert.False(t, backupFound)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	assert.Contains(t, err.Error(), "data directory already exists")
}

// TestCreateFirstBackupAndInitialize_InitDataDirFails verifies that an initdb failure
// is returned to the caller. The cleanup defer (scheduled before InitDataDir) runs
// but has nothing to clean up because the data directory was never created.
func TestCreateFirstBackupAndInitialize_InitDataDirFails(t *testing.T) {
	ctx := t.Context()

	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()

	const dbName = "testdb"
	require.NoError(t, store.CreateDatabase(ctx, dbName, &clustermetadatapb.Database{
		Name:                      dbName,
		BootstrapDurabilityPolicy: topoclient.AtLeastN(2),
	}))

	poolerDir := t.TempDir()
	dataDir := filepath.Join(poolerDir, "pg_data")
	t.Setenv(constants.PgDataDirEnvVar, dataDir)
	// No PG_VERSION — hasDataDirectory() returns false.

	pm := &MultiPoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   NewActionLock(),
		pgctldClient: &stubPgctldClient{}, // InitDataDir returns UNAVAILABLE
		multipooler: &clustermetadatapb.MultiPooler{
			Database:   dbName,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
			PoolerDir:  poolerDir,
		},
		config: &Config{},
	}

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	busy, backupFound, err := pm.createFirstBackupAndInitializeLocked(lockCtx)
	require.Error(t, err)
	assert.False(t, busy)
	assert.False(t, backupFound)
	assert.Contains(t, err.Error(), "failed to initialize data directory")
}

// TestCreateFirstBackupAndInitialize_CleansUpAfterLaterFailure verifies that
// when InitDataDir succeeds but a later step fails, the data directory is removed.
func TestCreateFirstBackupAndInitialize_CleansUpAfterLaterFailure(t *testing.T) {
	ctx := t.Context()

	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()

	const dbName = "testdb"
	require.NoError(t, store.CreateDatabase(ctx, dbName, &clustermetadatapb.Database{
		Name:                      dbName,
		BootstrapDurabilityPolicy: topoclient.AtLeastN(2),
	}))

	poolerDir := t.TempDir()
	dataDir := filepath.Join(poolerDir, "pg_data")
	t.Setenv(constants.PgDataDirEnvVar, dataDir)

	pgctld := &successStubPgctldClient{pgDataDir: dataDir}

	pm := &MultiPoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   NewActionLock(),
		pgctldClient: pgctld,
		multipooler: &clustermetadatapb.MultiPooler{
			Database:   dbName,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
			PoolerDir:  poolerDir,
		},
		config: &Config{},
		// pgBackRestConfigPath deliberately empty → configureArchiveMode will fail,
		// triggering the cleanup defer after InitDataDir succeeded.
	}

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	_, _, err = pm.createFirstBackupAndInitializeLocked(lockCtx)
	require.Error(t, err)

	// The data directory should have been cleaned up by the defer.
	assert.NoDirExists(t, dataDir, "data directory should be removed after failure")
}

// TestWithBackupLease_ReturnsNodeExistsWhenHeld verifies that WithBackupLease returns
// a NodeExists error when the lease is already held by another pooler. This is the
// error that createFirstBackupAndInitializeLocked maps to busy=true.
// The full first-backup flow (prep + lease contention) is covered by integration tests.
func TestWithBackupLease_ReturnsNodeExistsWhenHeld(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer ts.Close()

	shardKey := commontypes.ShardKey{
		Database:   "testdb",
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	// Another pooler holds the lease.
	_, otherUnlock, err := ts.TryLockBackup(ctx, shardKey, "other-pooler-backup")
	require.NoError(t, err)
	var otherErr error
	defer otherUnlock(&otherErr)

	// WithBackupLease should fail with NodeExists.
	err = ts.WithBackupLease(ctx, shardKey, "our-pooler", "create-first-backup", slog.Default(), func(context.Context) error {
		t.Fatal("fn must not be called when lease is held")
		return nil
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}),
		"expected NodeExists when lease is held, got: %v", err)
}
