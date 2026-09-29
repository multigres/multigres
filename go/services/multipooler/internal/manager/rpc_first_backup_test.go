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

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/test/utils"

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

func (s *stubPgctldClient) StopRestoreCommand(_ context.Context, _ *pgctldpb.StopRestoreCommandRequest, _ ...grpc.CallOption) (*pgctldpb.StopRestoreCommandResponse, error) {
	return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: not available")
}

var _ pgctldpb.PgCtldClient = (*stubPgctldClient)(nil)

// successStubPgctldClient is a pgctld stub that succeeds for all calls.
// InitDataDir creates the pg_data directory to simulate what real pgctld does.
type successStubPgctldClient struct {
	pgDataDir   string // set by test; InitDataDir creates this directory
	startErr    error  // when set, Start returns this error (InitDataDir still succeeds)
	lastStopReq *pgctldpb.StopRequest
}

func (s *successStubPgctldClient) Start(context.Context, *pgctldpb.StartRequest, ...grpc.CallOption) (*pgctldpb.StartResponse, error) {
	if s.startErr != nil {
		return nil, s.startErr
	}
	return &pgctldpb.StartResponse{}, nil
}

func (s *successStubPgctldClient) Stop(_ context.Context, req *pgctldpb.StopRequest, _ ...grpc.CallOption) (*pgctldpb.StopResponse, error) {
	s.lastStopReq = req
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

func (s *successStubPgctldClient) StopRestoreCommand(context.Context, *pgctldpb.StopRestoreCommandRequest, ...grpc.CallOption) (*pgctldpb.StopRestoreCommandResponse, error) {
	return &pgctldpb.StopRestoreCommandResponse{}, nil
}

var _ pgctldpb.PgCtldClient = (*successStubPgctldClient)(nil)

// stopFailsPgctldClient behaves like successStubPgctldClient but Stop always
// fails - used to verify that a failed Stop blocks data-directory removal
// during first-backup cleanup, rather than being logged and ignored.
type stopFailsPgctldClient struct {
	successStubPgctldClient
}

func (s *stopFailsPgctldClient) Stop(context.Context, *pgctldpb.StopRequest, ...grpc.CallOption) (*pgctldpb.StopResponse, error) {
	return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: stop failed")
}

var _ pgctldpb.PgCtldClient = (*stopFailsPgctldClient)(nil)

// ctxCapturingPgctldClient behaves like successStubPgctldClient but records
// the ctx.Err() of whatever context Stop was called with, and optionally
// calls cancelOuter (simulating the caller's own context expiring, e.g. a
// monitor tick timeout) right after Start returns - before cleanup runs.
type ctxCapturingPgctldClient struct {
	successStubPgctldClient
	cancelOuter context.CancelFunc
	stopCtxErr  error
}

func (s *ctxCapturingPgctldClient) Start(ctx context.Context, req *pgctldpb.StartRequest, opts ...grpc.CallOption) (*pgctldpb.StartResponse, error) {
	resp, err := s.successStubPgctldClient.Start(ctx, req, opts...)
	if s.cancelOuter != nil {
		s.cancelOuter()
	}
	return resp, err
}

func (s *ctxCapturingPgctldClient) Stop(ctx context.Context, req *pgctldpb.StopRequest, opts ...grpc.CallOption) (*pgctldpb.StopResponse, error) {
	s.stopCtxErr = ctx.Err()
	return s.successStubPgctldClient.Stop(ctx, req, opts...)
}

var _ pgctldpb.PgCtldClient = (*ctxCapturingPgctldClient)(nil)

// lockCheckingPgctldClient behaves like successStubPgctldClient but its Stop
// independently verifies the action lock is present in ctx, mirroring
// protectedPgctldClient.Stop's real requirement (production wraps every
// pgctld client with it) - this is the check a context that merely looks
// "fresh" can still fail if it doesn't carry the lock value forward.
type lockCheckingPgctldClient struct {
	successStubPgctldClient
	stopLockErr error
}

func (s *lockCheckingPgctldClient) Stop(ctx context.Context, req *pgctldpb.StopRequest, opts ...grpc.CallOption) (*pgctldpb.StopResponse, error) {
	s.stopLockErr = actionlock.AssertActionLockHeld(ctx)
	return s.successStubPgctldClient.Stop(ctx, req, opts...)
}

var _ pgctldpb.PgCtldClient = (*lockCheckingPgctldClient)(nil)

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

	pm := &MultipoolerManager{
		topoClient: store,
		record:     newRecordFromProto(&clustermetadatapb.Multipooler{ShardKey: &clustermetadatapb.ShardKey{Database: dbName}}),
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

	pm := &MultipoolerManager{
		topoClient: store,
		record:     newRecordFromProto(&clustermetadatapb.Multipooler{ShardKey: &clustermetadatapb.ShardKey{Database: dbName}}),
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

	pm := &MultipoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   actionlock.NewActionLock(),
		pgctldClient: &stubPgctldClient{},
		record: newRecordFromProto(&clustermetadatapb.Multipooler{
			PoolerDir: poolerDir,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   dbName,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}),
		config: &Config{},
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

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

	pm := &MultipoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   actionlock.NewActionLock(),
		pgctldClient: &stubPgctldClient{},
		record: newRecordFromProto(&clustermetadatapb.Multipooler{
			PoolerDir: poolerDir,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}),
		config: &Config{},
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

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

	pm := &MultipoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   actionlock.NewActionLock(),
		pgctldClient: &stubPgctldClient{}, // InitDataDir returns UNAVAILABLE
		record: newRecordFromProto(&clustermetadatapb.Multipooler{
			PoolerDir: poolerDir,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   dbName,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}),
		config: &Config{},
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

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
// when InitDataDir succeeds but a later step fails, the data directory is removed
// and the bootstrap sentinel is cleared (because cleanup returned to a clean state).
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

	pm := &MultipoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   actionlock.NewActionLock(),
		pgctldClient: pgctld,
		record: newRecordFromProto(&clustermetadatapb.Multipooler{
			PoolerDir: poolerDir,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   dbName,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}),
		config: &Config{},
	}
	// SetConfigPath is deliberately not called → the engine has no config path,
	// so ConfigureArchiveMode will fail, triggering the cleanup defer after
	// InitDataDir succeeded.
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{PgDataDir: dataDir})

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	_, _, err = pm.createFirstBackupAndInitializeLocked(lockCtx)
	require.Error(t, err)

	// The data directory should have been cleaned up by the defer.
	assert.NoDirExists(t, dataDir, "data directory should be removed after failure")
	// The sentinel should also be cleared since data-dir cleanup succeeded.
	assert.NoFileExists(t, filepath.Join(poolerDir, constants.BootstrapSentinelFile),
		"sentinel should be removed after successful defer cleanup")
	// Cleanup should stop postgres with Mode "immediate" rather than the
	// escalation ladder used elsewhere: this bootstrap instance has no real
	// client traffic to drain and its data is about to be deleted regardless,
	// so there's nothing a graceful stop buys here worth waiting for.
	require.NotNil(t, pgctld.lastStopReq, "cleanup must call Stop")
	assert.Equal(t, "immediate", pgctld.lastStopReq.Mode)
}

// writeMockPgBackRestConfig writes a minimal pgbackrest.conf so that
// ConfigureArchiveMode's config-path existence check passes. Only the file's
// presence matters here; its contents are not parsed by ConfigureArchiveMode.
func writeMockPgBackRestConfig(t *testing.T, poolerDir string) string {
	t.Helper()
	pgbackrestDir := filepath.Join(poolerDir, "pgbackrest")
	require.NoError(t, os.MkdirAll(pgbackrestDir, 0o755))
	configPath := filepath.Join(pgbackrestDir, "pgbackrest.conf")
	require.NoError(t, os.WriteFile(configPath, []byte("[global]\n[multigres]\n"), 0o600))
	return configPath
}

// TestCreateFirstBackupAndInitialize_StartFails verifies that a failure to start
// PostgreSQL during bootstrap is returned to the caller. InitDataDir and
// ConfigureArchiveMode succeed so execution reaches the writable
// Start(as_primary=true) call; the stub then fails it, exercising that
// error-return branch, and the cleanup defer removes the data directory.
func TestCreateFirstBackupAndInitialize_StartFails(t *testing.T) {
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
	configPath := writeMockPgBackRestConfig(t, poolerDir)

	// InitDataDir succeeds (creates dataDir); Start fails.
	pgctld := &successStubPgctldClient{
		pgDataDir: dataDir,
		startErr:  mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: start failed"),
	}

	pm := &MultipoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   actionlock.NewActionLock(),
		pgctldClient: pgctld,
		record: newRecordFromProto(&clustermetadatapb.Multipooler{
			PoolerDir: poolerDir,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   dbName,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}),
		config: &Config{},
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{PgDataDir: dataDir})
	// Configure the engine so ConfigureArchiveMode succeeds and execution reaches Start.
	pm.backup.SetConfigPath(configPath)
	backupCfg, err := commonbackup.NewConfig(utils.FilesystemBackupLocation(filepath.Join(poolerDir, "backups")))
	require.NoError(t, err)
	pm.backup.SetBackupConfig(backupCfg)

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	busy, backupFound, err := pm.createFirstBackupAndInitializeLocked(lockCtx)
	require.Error(t, err)
	assert.False(t, busy)
	assert.False(t, backupFound)
	assert.Contains(t, err.Error(), "failed to start PostgreSQL")
	// The cleanup defer should have removed the data dir InitDataDir created.
	assert.NoDirExists(t, dataDir, "data directory should be removed after Start failure")
}

// TestCreateFirstBackupAndInitialize_StopFailureStillRemovesDataDir verifies
// that a failed Stop does not block data-directory removal during cleanup.
// This is safe specifically because the whole call runs under pm.actionLock,
// and Recruit/Promote - the only ways this pooler could become a real,
// externally-recognized leader - both acquire that same lock first, so
// nothing outside this call can be relying on this bootstrap postgres.
func TestCreateFirstBackupAndInitialize_StopFailureStillRemovesDataDir(t *testing.T) {
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
	configPath := writeMockPgBackRestConfig(t, poolerDir)

	// InitDataDir succeeds (creates dataDir); Start fails, triggering cleanup;
	// Stop (called by cleanup) also fails.
	pgctld := &stopFailsPgctldClient{successStubPgctldClient: successStubPgctldClient{
		pgDataDir: dataDir,
		startErr:  mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: start failed"),
	}}

	pm := &MultipoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   actionlock.NewActionLock(),
		pgctldClient: pgctld,
		record: newRecordFromProto(&clustermetadatapb.Multipooler{
			PoolerDir: poolerDir,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   dbName,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}),
		config: &Config{},
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{PgDataDir: dataDir})
	pm.backup.SetConfigPath(configPath)
	backupCfg, err := commonbackup.NewConfig(utils.FilesystemBackupLocation(filepath.Join(poolerDir, "backups")))
	require.NoError(t, err)
	pm.backup.SetBackupConfig(backupCfg)

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	_, _, err = pm.createFirstBackupAndInitializeLocked(lockCtx)
	require.Error(t, err)

	assert.NoDirExists(t, dataDir,
		"cleanup must still remove the data directory even when Stop fails")
}

// TestCreateFirstBackupAndInitialize_CleanupUsesFreshContextForStop is a
// regression test: the caller's ctx may already be canceled/expired by the
// time cleanup runs (e.g. the monitor tick's own timeout), but Stop must
// still be given a fresh, non-canceled context so it has a fair chance at a
// graceful shutdown rather than failing immediately for an unrelated reason.
func TestCreateFirstBackupAndInitialize_CleanupUsesFreshContextForStop(t *testing.T) {
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
	configPath := writeMockPgBackRestConfig(t, poolerDir)

	pgctld := &ctxCapturingPgctldClient{successStubPgctldClient: successStubPgctldClient{pgDataDir: dataDir}}

	pm := &MultipoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   actionlock.NewActionLock(),
		pgctldClient: pgctld,
		record: newRecordFromProto(&clustermetadatapb.Multipooler{
			PoolerDir: poolerDir,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   dbName,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}),
		config: &Config{},
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{PgDataDir: dataDir})
	pm.backup.SetConfigPath(configPath)
	backupCfg, err := commonbackup.NewConfig(utils.FilesystemBackupLocation(filepath.Join(poolerDir, "backups")))
	require.NoError(t, err)
	pm.backup.SetBackupConfig(backupCfg)

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// actionCtx mirrors the monitor's own per-action bound (context.WithTimeout
	// derived from lockCtx) - separate from lockCtx itself, which stays valid
	// for Release regardless of what happens to the action's own bound.
	actionCtx, cancelAction := context.WithCancel(lockCtx)
	defer cancelAction()
	pgctld.cancelOuter = cancelAction // simulates the tick's own timeout firing right after Start returns

	_, _, err = pm.createFirstBackupAndInitializeLocked(actionCtx)
	require.Error(t, err, "waitForDatabaseConnection must fail once actionCtx is canceled")

	require.NoError(t, pgctld.stopCtxErr,
		"Stop must receive a fresh context, not the already-canceled actionCtx")
	assert.NoDirExists(t, dataDir, "cleanup must still complete even though actionCtx had expired")
}

// TestCreateFirstBackupAndInitialize_CleanupStopCarriesActionLock is a
// regression test: production wraps every pgctld client in
// protectedPgctldClient, whose Stop requires actionlock.AssertActionLockHeld
// to pass. A cleanup context that merely detaches from cancellation but
// forgets to carry the lock value forward would make every real Stop call
// during cleanup fail with "requires action lock to be held".
func TestCreateFirstBackupAndInitialize_CleanupStopCarriesActionLock(t *testing.T) {
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
	configPath := writeMockPgBackRestConfig(t, poolerDir)

	pgctld := &lockCheckingPgctldClient{successStubPgctldClient: successStubPgctldClient{
		pgDataDir: dataDir,
		startErr:  mterrors.New(mtrpcpb.Code_UNAVAILABLE, "stub: start failed"),
	}}

	pm := &MultipoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   actionlock.NewActionLock(),
		pgctldClient: pgctld,
		record: newRecordFromProto(&clustermetadatapb.Multipooler{
			PoolerDir: poolerDir,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   dbName,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}),
		config: &Config{},
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{PgDataDir: dataDir})
	pm.backup.SetConfigPath(configPath)
	backupCfg, err := commonbackup.NewConfig(utils.FilesystemBackupLocation(filepath.Join(poolerDir, "backups")))
	require.NoError(t, err)
	pm.backup.SetBackupConfig(backupCfg)

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	_, _, err = pm.createFirstBackupAndInitializeLocked(lockCtx)
	require.Error(t, err)

	require.NoError(t, pgctld.stopLockErr,
		"cleanup's Stop call must carry the action lock forward, or protectedPgctldClient.Stop would reject it in production")
}

// TestCreateFirstBackupAndInitialize_StaleSentinelCleansUpDataDir verifies the
// recovery path after an incomplete prior attempt. The test sets up the on-disk
// state that such an attempt would leave behind — a sentinel file alongside a
// populated data directory — and asserts the next call cleans up the data
// directory before proceeding. The sentinel itself remains until bootstrap
// completes successfully.
func TestCreateFirstBackupAndInitialize_StaleSentinelCleansUpDataDir(t *testing.T) {
	ctx := t.Context()

	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()

	const dbName = "testdb"
	// Database with no durability policy: the function will fail at loadDurabilityPolicy,
	// which runs AFTER the stale-sentinel cleanup but BEFORE any write. That lets us
	// observe that the stale data directory was removed without needing to run initdb.
	require.NoError(t, store.CreateDatabase(ctx, dbName, &clustermetadatapb.Database{
		Name: dbName,
	}))

	poolerDir := t.TempDir()
	dataDir := filepath.Join(poolerDir, "pg_data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16"), 0o644))
	t.Setenv(constants.PgDataDirEnvVar, dataDir)

	// Pre-existing sentinel from a prior crashed attempt.
	sentinelPath := filepath.Join(poolerDir, constants.BootstrapSentinelFile)
	require.NoError(t, os.WriteFile(sentinelPath, []byte("stale\n"), 0o644))

	pm := &MultipoolerManager{
		logger:       slog.Default(),
		topoClient:   store,
		actionLock:   actionlock.NewActionLock(),
		pgctldClient: &stubPgctldClient{},
		record: newRecordFromProto(&clustermetadatapb.Multipooler{
			PoolerDir: poolerDir,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   dbName,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}),
		config: &Config{},
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	_, _, err = pm.createFirstBackupAndInitializeLocked(lockCtx)
	require.Error(t, err)
	// The error must come from loadDurabilityPolicy (after the sentinel cleanup),
	// NOT from hasDataDirectory (which would indicate cleanup did not run).
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	assert.Contains(t, err.Error(), "no durability_policy configured")
	// Stale data directory must be gone.
	assert.NoDirExists(t, dataDir, "stale data directory should be removed when sentinel detected")
	// Sentinel stays because bootstrap has not completed successfully.
	assert.FileExists(t, sentinelPath, "sentinel should remain until bootstrap completes")
}

// TestBootstrapSentinelPlacement guards the load-bearing invariant that the
// sentinel lives in pooler_dir and never under PGDATA — pgBackRest backs up
// PGDATA only, so placement here keeps the sentinel out of backups.
func TestBootstrapSentinelPlacement(t *testing.T) {
	poolerDir := t.TempDir()
	dataDir := filepath.Join(poolerDir, "pg_data")
	t.Setenv(constants.PgDataDirEnvVar, dataDir)

	pm := &MultipoolerManager{
		record: newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: poolerDir}),
	}

	require.NoError(t, pm.writeBootstrapSentinel())
	assert.FileExists(t, filepath.Join(poolerDir, constants.BootstrapSentinelFile))
	assert.NoFileExists(t, filepath.Join(dataDir, constants.BootstrapSentinelFile),
		"sentinel must NOT live under PGDATA — it would be backed up")
}

// TestWithBackupLease_ReturnsNodeExistsWhenHeld verifies that WithBackupLease returns
// a NodeExists error when the lease is already held by another pooler. This is the
// error that createFirstBackupAndInitializeLocked maps to busy=true.
// The full first-backup flow (prep + lease contention) is covered by integration tests.
func TestWithBackupLease_ReturnsNodeExistsWhenHeld(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer ts.Close()

	shardKey := &clustermetadatapb.ShardKey{
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
