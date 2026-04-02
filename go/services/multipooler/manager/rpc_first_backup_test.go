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
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
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

// TestParseDurabilityPolicyName tests all valid policy names and the error case.
func TestParseDurabilityPolicyName(t *testing.T) {
	tests := []struct {
		name          string
		policyName    string
		expectedType  clustermetadatapb.QuorumType
		expectedCount int32
		expectError   bool
	}{
		{
			name:          "AT_LEAST_2",
			policyName:    "AT_LEAST_2",
			expectedType:  clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			expectedCount: 2,
		},
		{
			name:          "MULTI_CELL_AT_LEAST_2",
			policyName:    "MULTI_CELL_AT_LEAST_2",
			expectedType:  clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N,
			expectedCount: 2,
		},
		{
			name:          "ANY_2 (deprecated)",
			policyName:    "ANY_2",
			expectedType:  clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N, //nolint:staticcheck // deprecated
			expectedCount: 2,
		},
		{
			name:          "MULTI_CELL_ANY_2 (deprecated)",
			policyName:    "MULTI_CELL_ANY_2",
			expectedType:  clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N, //nolint:staticcheck // deprecated
			expectedCount: 2,
		},
		{
			name:        "unknown policy returns INVALID_ARGUMENT",
			policyName:  "UNKNOWN_POLICY",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule, err := parseDurabilityPolicyName(tt.policyName)
			if tt.expectError {
				require.Error(t, err)
				assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
				assert.Nil(t, rule)
			} else {
				require.NoError(t, err)
				require.NotNil(t, rule)
				assert.Equal(t, tt.expectedType, rule.QuorumType)
				assert.Equal(t, tt.expectedCount, rule.RequiredCount)
			}
		})
	}
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

	_, _, err = pm.loadDurabilityPolicy(ctx)
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

	busy, backupFound, err := pm.createFirstBackupAndInitialize(lockCtx)
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

	busy, backupFound, err := pm.createFirstBackupAndInitialize(lockCtx)
	require.Error(t, err)
	assert.False(t, busy)
	assert.False(t, backupFound)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	assert.Contains(t, err.Error(), "data directory already exists")
}

// TestCreateFirstBackupAndInitialize_Busy verifies that when another pooler already
// holds the backup lease, the function returns (busy=true, nil).
func TestCreateFirstBackupAndInitialize_Busy(t *testing.T) {
	ctx := t.Context()

	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()

	pm := &MultiPoolerManager{
		logger:     slog.Default(),
		topoClient: store,
		actionLock: NewActionLock(),
		multipooler: &clustermetadatapb.MultiPooler{
			Database:   "testdb",
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
		config: &Config{},
	}

	// Simulate another pooler holding the backup lease
	_, otherUnlock, err := store.TryLockBackup(ctx, pm.shardKey(), "other-pooler-backup")
	require.NoError(t, err)
	var otherErr error
	defer otherUnlock(&otherErr)

	// Acquire the action lock as the monitor would do
	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	busy, backupFound, err := pm.createFirstBackupAndInitialize(lockCtx)
	assert.True(t, busy)
	assert.False(t, backupFound)
	assert.NoError(t, err)
}
