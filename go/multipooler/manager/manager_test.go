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
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/servenv"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestManagerState_InitialState(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	config := &Config{
		TopoClient: ts,
		ServiceID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-service",
		},
	}

	manager := NewMultiPoolerManager(logger, config)
	defer manager.Close()

	// Initial state should be Starting
	assert.Equal(t, ManagerStateStarting, manager.GetState())

	mp, state, err := manager.GetMultiPooler()
	assert.Nil(t, mp)
	assert.Equal(t, ManagerStateStarting, state)
	assert.Nil(t, err)
}

func TestManagerState_SuccessfulLoad(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create temp directory for pooler-dir
	poolerDir := t.TempDir()

	// Create the multipooler in topology
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Database:      "testdb",
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PoolerDir:  poolerDir,
	}

	manager := NewMultiPoolerManager(logger, config)
	defer manager.Close()

	// Start both async loaders (topo and consensus term)
	go manager.loadMultiPoolerFromTopo()
	go manager.loadConsensusTermFromDisk()

	// Wait for the state to become Ready
	require.Eventually(t, func() bool {
		return manager.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	// Verify the loaded multipooler
	mp, state, err := manager.GetMultiPooler()
	assert.NotNil(t, mp)
	assert.Equal(t, ManagerStateReady, state)
	assert.Nil(t, err)
	assert.Equal(t, "test-service", mp.Id.Name)
	assert.Equal(t, "testdb", mp.Database)
}

func TestManagerState_LoadFailureTimeout(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Inject error for all Get operations on multipooler
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	poolerPath := "/poolers/" + topo.MultiPoolerIDString(serviceID) + "/Pooler"
	factory.AddOperationError(memorytopo.Get, poolerPath, assert.AnError)

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
	}

	// Create manager with a short timeout for testing
	manager := NewMultiPoolerManagerWithTimeout(logger, config, 1*time.Second)
	defer manager.Close()

	// Start the async loader
	go manager.loadMultiPoolerFromTopo()

	// Wait for the state to become Error
	require.Eventually(t, func() bool {
		return manager.GetState() == ManagerStateError
	}, 3*time.Second, 100*time.Millisecond, "Manager should reach Error state")

	// Verify the error state
	mp, state, err := manager.GetMultiPooler()
	assert.Nil(t, mp)
	assert.Equal(t, ManagerStateError, state)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

func TestManagerState_CancellationDuringLoad(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Inject error to keep it retrying
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	poolerPath := "/poolers/" + topo.MultiPoolerIDString(serviceID) + "/Pooler"
	factory.AddOperationError(memorytopo.Get, poolerPath, assert.AnError)

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
	}

	manager := NewMultiPoolerManager(logger, config)

	// Start the async loader
	go manager.loadMultiPoolerFromTopo()

	// Give it a moment to start
	time.Sleep(200 * time.Millisecond)

	// Cancel the manager
	manager.Close()

	// Wait a bit for the cancellation to propagate
	time.Sleep(100 * time.Millisecond)

	// State should be Error due to context cancellation
	assert.Equal(t, ManagerStateError, manager.GetState())

	// Verify the error contains "cancelled"
	_, _, err := manager.GetMultiPooler()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "cancelled")
}

func TestManagerState_RetryUntilSuccess(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create temp directory for pooler-dir
	poolerDir := t.TempDir()

	// Create the multipooler in topology
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Database:      "testdb",
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Inject 2 one-time errors to simulate transient failures
	poolerPath := "/poolers/" + topo.MultiPoolerIDString(serviceID) + "/Pooler"
	factory.AddOneTimeOperationError(memorytopo.Get, poolerPath, assert.AnError)
	factory.AddOneTimeOperationError(memorytopo.Get, poolerPath, assert.AnError)

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PoolerDir:  poolerDir,
	}

	manager := NewMultiPoolerManager(logger, config)
	defer manager.Close()

	// Start both async loaders (topo and consensus term)
	go manager.loadMultiPoolerFromTopo()
	go manager.loadConsensusTermFromDisk()

	// Wait for the state to become Ready after retries
	require.Eventually(t, func() bool {
		return manager.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state after retries")

	// Verify the loaded multipooler
	mp, state, err := manager.GetMultiPooler()
	assert.NotNil(t, mp)
	assert.Equal(t, ManagerStateReady, state)
	assert.Nil(t, err)
	assert.Equal(t, "testdb", mp.Database)
}

func TestManagerState_NilServiceID(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	config := &Config{
		TopoClient: ts,
		ServiceID:  nil, // Nil ServiceID
	}

	manager := NewMultiPoolerManager(logger, config)
	defer manager.Close()

	// Start the async loader
	go manager.loadMultiPoolerFromTopo()

	// Wait for the state to become Error
	require.Eventually(t, func() bool {
		return manager.GetState() == ManagerStateError
	}, 1*time.Second, 50*time.Millisecond, "Manager should reach Error state with nil ServiceID")

	// Verify the error state
	mp, state, err := manager.GetMultiPooler()
	assert.Nil(t, mp)
	assert.Equal(t, ManagerStateError, state)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "ServiceID cannot be nil")
}

func TestValidateAndUpdateTerm(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	tests := []struct {
		name          string
		currentTerm   int64
		requestTerm   int64
		force         bool
		expectError   bool
		expectedCode  mtrpcpb.Code
		errorContains string
	}{
		{
			name:        "Equal term should accept",
			currentTerm: 5,
			requestTerm: 5,
			force:       false,
			expectError: false,
		},
		{
			name:        "Higher term should update and accept",
			currentTerm: 5,
			requestTerm: 10,
			force:       false,
			expectError: false,
		},
		{
			name:          "Lower term should reject",
			currentTerm:   10,
			requestTerm:   5,
			force:         false,
			expectError:   true,
			expectedCode:  mtrpcpb.Code_FAILED_PRECONDITION,
			errorContains: "consensus term too old",
		},
		{
			name:        "Force flag bypasses validation",
			currentTerm: 10,
			requestTerm: 5,
			force:       true,
			expectError: false,
		},
		{
			name:          "Zero cached term rejects (uninitialized)",
			currentTerm:   0,
			requestTerm:   5,
			force:         false,
			expectError:   true,
			expectedCode:  mtrpcpb.Code_FAILED_PRECONDITION,
			errorContains: "not initialized",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
			defer ts.Close()

			// Create temp directory for pooler-dir
			poolerDir := t.TempDir()

			// Create a minimal data directory structure to satisfy IsDataDirInitialized check
			dataDir := postgresDataDir(poolerDir)
			require.NoError(t, os.MkdirAll(dataDir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("15\n"), 0o644))

			// Set initial consensus term on disk if currentTerm > 0
			if tt.currentTerm > 0 {
				initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
					CurrentTerm: tt.currentTerm,
				}
				require.NoError(t, SetTerm(poolerDir, initialTerm))
			}

			multipooler := &clustermetadatapb.MultiPooler{
				Id:            serviceID,
				Database:      "testdb",
				Hostname:      "localhost",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          clustermetadatapb.PoolerType_PRIMARY,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			}
			require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

			config := &Config{
				TopoClient: ts,
				ServiceID:  serviceID,
				PoolerDir:  poolerDir,
			}
			manager := NewMultiPoolerManager(logger, config)
			defer manager.Close()

			// Start and wait for ready
			senv := servenv.NewServEnv()
			go manager.Start(senv)
			require.Eventually(t, func() bool {
				return manager.GetState() == ManagerStateReady
			}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

			// Call validateAndUpdateTerm
			err := manager.validateAndUpdateTerm(ctx, tt.requestTerm, tt.force)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)

				if tt.expectedCode != 0 {
					code := mterrors.Code(err)
					assert.Equal(t, tt.expectedCode, code)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateSyncReplicationParams(t *testing.T) {
	tests := []struct {
		name        string
		numSync     int32
		standbyIDs  []*clustermetadatapb.ID
		expectError bool
		errorMsg    string
	}{
		{
			name:    "Valid single standby",
			numSync: 1,
			standbyIDs: []*clustermetadatapb.ID{
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "standby1",
				},
			},
			expectError: false,
		},
		{
			name:    "Valid multiple standbys",
			numSync: 2,
			standbyIDs: []*clustermetadatapb.ID{
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "standby1",
				},
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "standby2",
				},
			},
			expectError: false,
		},
		{
			name:        "Valid empty standbys",
			numSync:     0,
			standbyIDs:  []*clustermetadatapb.ID{},
			expectError: false,
		},
		{
			name:        "Valid numSync zero with nil standbys",
			numSync:     0,
			standbyIDs:  nil,
			expectError: false,
		},
		{
			name:        "Invalid negative numSync",
			numSync:     -1,
			standbyIDs:  []*clustermetadatapb.ID{},
			expectError: true,
			errorMsg:    "num_sync must be non-negative, got: -1",
		},
		{
			name:    "Invalid numSync exceeds standby count",
			numSync: 3,
			standbyIDs: []*clustermetadatapb.ID{
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "standby1",
				},
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "standby2",
				},
			},
			expectError: true,
			errorMsg:    "num_sync (3) cannot exceed number of standby_ids (2)",
		},
		{
			name:    "Invalid nil standby ID",
			numSync: 1,
			standbyIDs: []*clustermetadatapb.ID{
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "standby1",
				},
				nil,
			},
			expectError: true,
			errorMsg:    "standby_ids[1] is nil",
		},
		{
			name:    "Invalid empty cell",
			numSync: 1,
			standbyIDs: []*clustermetadatapb.ID{
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "",
					Name:      "standby1",
				},
			},
			expectError: true,
			errorMsg:    "standby_ids[0] has empty cell",
		},
		{
			name:    "Invalid empty name",
			numSync: 1,
			standbyIDs: []*clustermetadatapb.ID{
				{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "",
				},
			},
			expectError: true,
			errorMsg:    "standby_ids[0] has empty name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSyncReplicationParams(tt.numSync, tt.standbyIDs)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				code := mterrors.Code(err)
				assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, code)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestQueryReplicationStatus(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tests := []struct {
		name           string
		setupMock      func(mock sqlmock.Sqlmock)
		expectError    bool
		validateResult func(t *testing.T, status *multipoolermanagerdatapb.ReplicationStatus)
	}{
		{
			name: "All fields with valid values",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{
					"pg_last_wal_replay_lsn",
					"pg_last_wal_receive_lsn",
					"pg_is_wal_replay_paused",
					"pg_get_wal_replay_pause_state",
					"pg_last_xact_replay_timestamp",
					"current_setting",
				}).AddRow(
					"0/3000000",              // replay_lsn
					"0/3000100",              // receive_lsn
					false,                    // is_paused
					"not paused",             // pause_state
					"2025-01-15 10:00:00+00", // last_xact_time
					"host=primary port=5432", // primary_conninfo
				)
				mock.ExpectQuery("SELECT").WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.ReplicationStatus) {
				assert.Equal(t, "0/3000000", status.LastReplayLsn)
				assert.Equal(t, "0/3000100", status.LastReceiveLsn)
				assert.False(t, status.IsWalReplayPaused)
				assert.Equal(t, "not paused", status.WalReplayPauseState)
				assert.Equal(t, "2025-01-15 10:00:00+00", status.LastXactReplayTimestamp)
				assert.NotNil(t, status.PrimaryConnInfo)
				assert.Equal(t, "primary", status.PrimaryConnInfo.Host)
			},
		},
		{
			name: "NULL LSN values (primary server case)",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{
					"pg_last_wal_replay_lsn",
					"pg_last_wal_receive_lsn",
					"pg_is_wal_replay_paused",
					"pg_get_wal_replay_pause_state",
					"pg_last_xact_replay_timestamp",
					"current_setting",
				}).AddRow(
					nil,          // replay_lsn is NULL on primary
					nil,          // receive_lsn is NULL on primary
					false,        // is_paused
					"not paused", // pause_state
					nil,          // last_xact_time is NULL on primary
					"",           // empty primary_conninfo on primary
				)
				mock.ExpectQuery("SELECT").WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.ReplicationStatus) {
				assert.Empty(t, status.LastReplayLsn, "LastReplayLsn should be empty when NULL")
				assert.Empty(t, status.LastReceiveLsn, "LastReceiveLsn should be empty when NULL")
				assert.False(t, status.IsWalReplayPaused)
				assert.Equal(t, "not paused", status.WalReplayPauseState)
				assert.Empty(t, status.LastXactReplayTimestamp, "LastXactReplayTimestamp should be empty when NULL")
			},
		},
		{
			name: "Paused replication with valid LSNs",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{
					"pg_last_wal_replay_lsn",
					"pg_last_wal_receive_lsn",
					"pg_is_wal_replay_paused",
					"pg_get_wal_replay_pause_state",
					"pg_last_xact_replay_timestamp",
					"current_setting",
				}).AddRow(
					"0/4000000",              // replay_lsn
					"0/4000200",              // receive_lsn
					true,                     // is_paused
					"paused",                 // pause_state
					"2025-01-15 11:00:00+00", // last_xact_time
					"host=primary port=5432 user=replicator application_name=standby1",
				)
				mock.ExpectQuery("SELECT").WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.ReplicationStatus) {
				assert.Equal(t, "0/4000000", status.LastReplayLsn)
				assert.Equal(t, "0/4000200", status.LastReceiveLsn)
				assert.True(t, status.IsWalReplayPaused)
				assert.Equal(t, "paused", status.WalReplayPauseState)
				assert.Equal(t, "2025-01-15 11:00:00+00", status.LastXactReplayTimestamp)
				assert.NotNil(t, status.PrimaryConnInfo)
				assert.Equal(t, "primary", status.PrimaryConnInfo.Host)
				assert.Equal(t, int32(5432), status.PrimaryConnInfo.Port)
				assert.Equal(t, "replicator", status.PrimaryConnInfo.User)
				assert.Equal(t, "standby1", status.PrimaryConnInfo.ApplicationName)
			},
		},
		{
			name: "Mixed NULL and valid values",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{
					"pg_last_wal_replay_lsn",
					"pg_last_wal_receive_lsn",
					"pg_is_wal_replay_paused",
					"pg_get_wal_replay_pause_state",
					"pg_last_xact_replay_timestamp",
					"current_setting",
				}).AddRow(
					"0/5000000",              // replay_lsn is valid
					nil,                      // receive_lsn is NULL (e.g., no active connection)
					false,                    // is_paused
					"not paused",             // pause_state
					nil,                      // last_xact_time is NULL
					"host=primary port=5432", // primary_conninfo
				)
				mock.ExpectQuery("SELECT").WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.ReplicationStatus) {
				assert.Equal(t, "0/5000000", status.LastReplayLsn, "LastReplayLsn should be populated")
				assert.Empty(t, status.LastReceiveLsn, "LastReceiveLsn should be empty when NULL")
				assert.False(t, status.IsWalReplayPaused)
				assert.Empty(t, status.LastXactReplayTimestamp, "LastXactReplayTimestamp should be empty when NULL")
			},
		},
		{
			name: "Query error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT").WillReturnError(sql.ErrConnDone)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock database
			mockDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer mockDB.Close()

			// Setup mock expectations
			tt.setupMock(mock)

			// Create minimal manager with mock DB
			pm := &MultiPoolerManager{
				logger: logger,
				db:     mockDB,
			}

			// Call the method
			ctx := context.Background()
			status, err := pm.queryReplicationStatus(ctx)

			// Validate results
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, status)
			} else {
				require.NoError(t, err)
				require.NotNil(t, status)
				if tt.validateResult != nil {
					tt.validateResult(t, status)
				}
			}

			// Ensure all expectations were met
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
