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
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/multipooler/executor/mock"
	"github.com/multigres/multigres/go/tools/viperutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestManagerState_InitialState(t *testing.T) {
	ctx := t.Context()
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
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)
	defer manager.Close()

	// Initial state should be Starting
	assert.Equal(t, ManagerStateStarting, manager.GetState())

	mp, state, err := manager.GetMultiPooler()
	assert.Nil(t, mp)
	assert.Equal(t, ManagerStateStarting, state)
	assert.Nil(t, err)
}

func TestManagerState_SuccessfulLoad(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create temp directory for pooler-dir
	poolerDir := t.TempDir()

	// Create the database in topology with backup location
	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	// Create the multipooler in topology
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Database:      database,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PoolerDir:  poolerDir,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)
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
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Inject error for all Get operations on multipooler
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	poolerPath := "/poolers/" + topoclient.MultiPoolerIDString(serviceID) + "/Pooler"
	factory.AddOperationError(memorytopo.Get, poolerPath, assert.AnError)

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	// Create manager with a short timeout for testing
	manager, err := NewMultiPoolerManagerWithTimeout(logger, config, 1*time.Second)
	require.NoError(t, err)
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
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Inject error to keep it retrying
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	poolerPath := "/poolers/" + topoclient.MultiPoolerIDString(serviceID) + "/Pooler"
	factory.AddOperationError(memorytopo.Get, poolerPath, assert.AnError)

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	manager, err := NewMultiPoolerManager(logger, config)
	// If we don't open, Close is a noop.
	manager.isOpen = true
	require.NoError(t, err)

	// Start the async loader
	go manager.loadMultiPoolerFromTopo()

	// Give it a moment to start retrying
	time.Sleep(200 * time.Millisecond)

	// Cancel the manager
	manager.Close()

	// Wait for the state to become Error due to context cancellation
	require.Eventually(t, func() bool {
		return manager.GetState() == ManagerStateError
	}, 3*time.Second, 100*time.Millisecond, "Manager should reach Error state after cancellation")

	// Verify the error contains "cancelled"
	_, _, err = manager.GetMultiPooler()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "cancelled")
}

func TestManagerState_RetryUntilSuccess(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create temp directory for pooler-dir
	poolerDir := t.TempDir()

	// Create the database in topology with backup location
	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	// Create the multipooler in topology
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Database:      database,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Inject 2 one-time errors to simulate transient failures
	poolerPath := "/poolers/" + topoclient.MultiPoolerIDString(serviceID) + "/Pooler"
	factory.AddOneTimeOperationError(memorytopo.Get, poolerPath, assert.AnError)
	factory.AddOneTimeOperationError(memorytopo.Get, poolerPath, assert.AnError)

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PoolerDir:  poolerDir,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)
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
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	config := &Config{
		TopoClient: ts,
		ServiceID:  nil, // Nil ServiceID
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)
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
	ctx := t.Context()
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
					TermNumber: tt.currentTerm,
				}
				require.NoError(t, setConsensusTerm(poolerDir, initialTerm))
			}

			// Create the database in topology with backup location
			database := "testdb"
			addDatabaseToTopo(t, ts, database)

			multipooler := &clustermetadatapb.MultiPooler{
				Id:            serviceID,
				Database:      database,
				Hostname:      "localhost",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          clustermetadatapb.PoolerType_PRIMARY,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				TableGroup:    constants.DefaultTableGroup,
				Shard:         constants.DefaultShard,
			}
			require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

			config := &Config{
				TopoClient:       ts,
				ServiceID:        serviceID,
				PoolerDir:        poolerDir,
				ConsensusEnabled: true,
				TableGroup:       constants.DefaultTableGroup,
				Shard:            constants.DefaultShard,
			}
			manager, err := NewMultiPoolerManager(logger, config)
			require.NoError(t, err)
			defer manager.Close()

			// Set up mock query service for isInRecovery check during startup
			mockQueryService := mock.NewQueryService()
			mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
			manager.qsc = &mockPoolerController{queryService: mockQueryService}

			// Start and wait for ready
			senv := servenv.NewServEnv(viperutil.NewRegistry())
			go manager.Start(senv)
			require.Eventually(t, func() bool {
				return manager.GetState() == ManagerStateReady
			}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

			// Acquire action lock before calling validateAndUpdateTerm
			ctx, err := manager.actionLock.Acquire(ctx, "test")
			require.NoError(t, err)
			defer manager.actionLock.Release(ctx)

			// Call validateAndUpdateTerm
			err = manager.validateAndUpdateTerm(ctx, tt.requestTerm, tt.force)

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

func TestGetBackupLocation(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	// Create test topology store
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create test database with backup_location
	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	// Create manager config
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PoolerDir:  filepath.Join(tmpDir, "pooler"),
		PgctldAddr: "",
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)

	// Set the multipooler to have the database
	multipoolerInfo := &topoclient.MultiPoolerInfo{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Database: database,
		},
	}
	manager.multipooler = multipoolerInfo
	manager.cachedMultipooler.multipooler = topoclient.NewMultiPoolerInfo(
		proto.Clone(multipoolerInfo.MultiPooler).(*clustermetadatapb.MultiPooler),
		multipoolerInfo.Version(),
	)
	// backupLocation is now the full path: base + database/tablegroup/shard
	expectedShardBackupLocation := filepath.Join("/var/backups/pgbackrest", database, constants.DefaultTableGroup, constants.DefaultShard)
	manager.backupLocation = expectedShardBackupLocation

	// Test accessing backup location field
	assert.Equal(t, expectedShardBackupLocation, manager.backupLocation)
}

// TestWaitUntilReady_Success verifies that WaitUntilReady returns immediately
// when the manager is already in Ready state
func TestWaitUntilReady_Success(t *testing.T) {
	logger := slog.Default()
	config := &Config{
		ConsensusEnabled: false,
		TableGroup:       constants.DefaultTableGroup,
		Shard:            constants.DefaultShard,
	}

	pm, err := NewMultiPoolerManagerWithTimeout(logger, config, 100*time.Millisecond)
	require.NoError(t, err)

	// Simulate immediate ready state
	pm.mu.Lock()
	pm.state = ManagerStateReady
	pm.topoLoaded = true
	close(pm.readyChan) // Signal that state has changed
	pm.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	err = pm.WaitUntilReady(ctx)
	require.NoError(t, err)
}

// TestWaitUntilReady_Error verifies that WaitUntilReady returns an error
// when the manager is in Error state
func TestWaitUntilReady_Error(t *testing.T) {
	logger := slog.Default()
	config := &Config{
		ConsensusEnabled: false,
		TableGroup:       constants.DefaultTableGroup,
		Shard:            constants.DefaultShard,
	}

	pm, err := NewMultiPoolerManagerWithTimeout(logger, config, 100*time.Millisecond)
	require.NoError(t, err)

	// Simulate error state
	pm.mu.Lock()
	pm.state = ManagerStateError
	pm.stateError = assert.AnError
	close(pm.readyChan) // Signal that state has changed
	pm.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	err = pm.WaitUntilReady(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manager is in error state")
}

// TestWaitUntilReady_Timeout verifies that WaitUntilReady returns a context error
// when the manager stays in Starting state and the context times out
func TestWaitUntilReady_Timeout(t *testing.T) {
	logger := slog.Default()
	config := &Config{
		ConsensusEnabled: false,
		TableGroup:       constants.DefaultTableGroup,
		Shard:            constants.DefaultShard,
	}

	pm, err := NewMultiPoolerManagerWithTimeout(logger, config, 100*time.Millisecond)
	require.NoError(t, err)

	// Leave in Starting state - will timeout
	// Don't close readyChan to test context timeout

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err = pm.WaitUntilReady(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context")
}

// TestWaitUntilReady_ConcurrentCalls verifies that multiple goroutines can
// safely call WaitUntilReady concurrently without data races
func TestWaitUntilReady_ConcurrentCalls(t *testing.T) {
	logger := slog.Default()
	config := &Config{
		ConsensusEnabled: false,
		TableGroup:       constants.DefaultTableGroup,
		Shard:            constants.DefaultShard,
	}

	pm, err := NewMultiPoolerManagerWithTimeout(logger, config, 100*time.Millisecond)
	require.NoError(t, err)

	// Start multiple goroutines calling WaitUntilReady
	const numGoroutines = 10
	errChan := make(chan error, numGoroutines)

	ctx := t.Context()

	for range numGoroutines {
		go func() {
			err := pm.WaitUntilReady(ctx)
			errChan <- err
		}()
	}

	// Simulate state transition to Ready after a delay
	time.Sleep(50 * time.Millisecond)
	pm.mu.Lock()
	pm.state = ManagerStateReady
	pm.topoLoaded = true
	close(pm.readyChan) // Signal that state has changed
	pm.mu.Unlock()

	// Collect results
	for range numGoroutines {
		err := <-errChan
		require.NoError(t, err)
	}
}

func TestNewMultiPoolerManager_MVPValidation(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	tests := []struct {
		name        string
		tableGroup  string
		shard       string
		wantErr     bool
		errContains string
	}{
		{
			name:       "valid default tablegroup and shard",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			wantErr:    false,
		},
		{
			name:        "empty tablegroup fails",
			tableGroup:  "",
			shard:       constants.DefaultShard,
			wantErr:     true,
			errContains: "TableGroup is required",
		},
		{
			name:        "empty shard fails",
			tableGroup:  constants.DefaultTableGroup,
			shard:       "",
			wantErr:     true,
			errContains: "Shard is required",
		},
		{
			name:        "invalid tablegroup fails",
			tableGroup:  "custom",
			shard:       constants.DefaultShard,
			wantErr:     true,
			errContains: "only default tablegroup is supported",
		},
		{
			name:        "invalid shard fails",
			tableGroup:  constants.DefaultTableGroup,
			shard:       "0-100",
			wantErr:     true,
			errContains: "only shard " + constants.DefaultShard + " is supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				TopoClient: ts,
				ServiceID:  serviceID,
				TableGroup: tt.tableGroup,
				Shard:      tt.shard,
			}

			manager, err := NewMultiPoolerManager(logger, config)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Nil(t, manager)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, manager)
				manager.Close()
			}
		})
	}
}

func TestMultiPoolerManager_backupLocationPath(t *testing.T) {
	tests := []struct {
		name               string
		baseBackupLocation string
		database           string
		tableGroup         string
		shard              string
		wantPath           string
		wantErr            bool
		wantErrContains    string
	}{
		{
			name:               "simple valid path",
			baseBackupLocation: "/backups",
			database:           "mydb",
			tableGroup:         "tg1",
			shard:              "shard0",
			wantPath:           "/backups/mydb/tg1/shard0",
			wantErr:            false,
		},
		{
			name:               "with dots in identifiers",
			baseBackupLocation: "/backups",
			database:           "my.db",
			tableGroup:         "tg.1",
			shard:              "shard.0",
			wantPath:           "/backups/my.db/tg.1/shard.0",
			wantErr:            false,
		},
		{
			name:               "empty database",
			baseBackupLocation: "/backups",
			database:           "",
			tableGroup:         "tg1",
			shard:              "shard0",
			wantErr:            true,
			wantErrContains:    "database cannot be empty",
		},
		{
			name:               "empty table group",
			baseBackupLocation: "/backups",
			database:           "mydb",
			tableGroup:         "",
			shard:              "shard0",
			wantErr:            true,
			wantErrContains:    "table group cannot be empty",
		},
		{
			name:               "empty shard",
			baseBackupLocation: "/backups",
			database:           "mydb",
			tableGroup:         "tg1",
			shard:              "",
			wantErr:            true,
			wantErrContains:    "shard cannot be empty",
		},
		{
			name:               "double dot encoded",
			baseBackupLocation: "/backups",
			database:           "..",
			tableGroup:         "tg1",
			shard:              "shard0",
			wantPath:           "/backups/%2E%2E/tg1/shard0",
			wantErr:            false,
		},
		{
			name:               "slash in component encoded",
			baseBackupLocation: "/backups",
			database:           "db/etc",
			tableGroup:         "tg1",
			shard:              "shard0",
			wantPath:           "/backups/db%2Fetc/tg1/shard0",
			wantErr:            false,
		},
		{
			name:               "backslash in component encoded",
			baseBackupLocation: "/backups",
			database:           "db\\windows",
			tableGroup:         "tg1",
			shard:              "shard0",
			wantPath:           "/backups/db%5Cwindows/tg1/shard0",
			wantErr:            false,
		},
		{
			name:               "unicode identifiers",
			baseBackupLocation: "/backups",
			database:           "データベース",
			tableGroup:         "グループ",
			shard:              "シャード",
			wantPath:           "/backups/%E3%83%87%E3%83%BC%E3%82%BF%E3%83%99%E3%83%BC%E3%82%B9/%E3%82%B0%E3%83%AB%E3%83%BC%E3%83%97/%E3%82%B7%E3%83%A3%E3%83%BC%E3%83%89",
			wantErr:            false,
		},
		{
			name:               "colon in identifier",
			baseBackupLocation: "/backups",
			database:           "db:backup",
			tableGroup:         "tg1",
			shard:              "shard0",
			wantPath:           "/backups/db%3Abackup/tg1/shard0",
			wantErr:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm := &MultiPoolerManager{}
			gotPath, err := pm.backupLocationPath(tt.baseBackupLocation, tt.database, tt.tableGroup, tt.shard)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantPath, gotPath)
			}
		})
	}
}

func TestEnableMonitor(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)

	// Initialize the manager context (simulating Open without connecting to DB)
	manager.mu.Lock()
	manager.ctx, manager.cancel = context.WithCancel(context.TODO())
	manager.isOpen = true
	manager.mu.Unlock()
	defer manager.cancel()

	// Verify monitor is not running initially
	manager.mu.Lock()
	assert.Nil(t, manager.monitorCancel, "Monitor should not be running initially")
	manager.mu.Unlock()

	// Enable the monitor
	err = manager.enableMonitorInternal()
	require.NoError(t, err)

	// Verify monitor is enabled
	manager.mu.Lock()
	assert.NotNil(t, manager.monitorCancel, "Monitor should be enabled")
	manager.mu.Unlock()

	// Clean up: disable the monitor
	manager.disableMonitorInternal()
}

func TestEnableMonitor_Idempotent(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)

	// Initialize the manager context (simulating Open without connecting to DB)
	manager.mu.Lock()
	manager.ctx, manager.cancel = context.WithCancel(context.TODO())
	manager.isOpen = true
	manager.mu.Unlock()
	defer manager.cancel()

	// Enable the monitor
	err = manager.enableMonitorInternal()
	require.NoError(t, err)

	// Verify monitor is enabled
	manager.mu.Lock()
	assert.NotNil(t, manager.monitorCancel, "Monitor should be enabled")
	manager.mu.Unlock()

	// Enable again - should be idempotent (no error, monitor still enabled)
	err = manager.enableMonitorInternal()
	require.NoError(t, err)

	// Verify monitor is still enabled (idempotency means calling again has no effect)
	manager.mu.Lock()
	assert.NotNil(t, manager.monitorCancel, "Monitor should still be enabled after second call")
	manager.mu.Unlock()

	// Clean up: disable the monitor
	manager.disableMonitorInternal()
}

func TestDisableMonitor(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)

	// Initialize the manager context (simulating Open without connecting to DB)
	manager.mu.Lock()
	manager.ctx, manager.cancel = context.WithCancel(context.TODO())
	manager.isOpen = true
	manager.mu.Unlock()
	defer manager.cancel()

	// Enable the monitor first
	err = manager.enableMonitorInternal()
	require.NoError(t, err)

	// Verify monitor is running
	manager.mu.Lock()
	assert.NotNil(t, manager.monitorCancel, "Monitor should be running after enableMonitorInternal")
	manager.mu.Unlock()

	// Disable the monitor
	manager.disableMonitorInternal()

	// Verify monitor is disabled
	manager.mu.Lock()
	assert.Nil(t, manager.monitorCancel, "Monitor should be disabled")
	manager.mu.Unlock()
}

func TestDisableMonitor_Idempotent(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)

	// Initialize the manager context (simulating Open without connecting to DB)
	manager.mu.Lock()
	manager.ctx, manager.cancel = context.WithCancel(context.TODO())
	manager.isOpen = true
	manager.mu.Unlock()
	defer manager.cancel()

	// Enable the monitor first
	err = manager.enableMonitorInternal()
	require.NoError(t, err)

	// Disable the monitor
	manager.disableMonitorInternal()

	// Verify monitor is disabled
	manager.mu.Lock()
	assert.Nil(t, manager.monitorCancel, "Monitor should be disabled")
	manager.mu.Unlock()

	// Disable again - should be idempotent (no panic)
	manager.disableMonitorInternal()

	// Verify monitor is still disabled
	manager.mu.Lock()
	assert.Nil(t, manager.monitorCancel, "Monitor should still be disabled")
	manager.mu.Unlock()
}

func TestEnableDisableMonitor_Cycle(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)

	// Initialize the manager context (simulating Open without connecting to DB)
	manager.mu.Lock()
	manager.ctx, manager.cancel = context.WithCancel(context.TODO())
	manager.isOpen = true
	manager.mu.Unlock()
	defer manager.cancel()

	// Test multiple enable/disable cycles
	for i := range 3 {
		// Enable
		err = manager.enableMonitorInternal()
		require.NoError(t, err)
		manager.mu.Lock()
		assert.NotNil(t, manager.monitorCancel, "Monitor should be enabled at iteration %d", i)
		manager.mu.Unlock()

		// Give the goroutine a moment to start
		time.Sleep(10 * time.Millisecond)

		// Disable
		manager.disableMonitorInternal()
		manager.mu.Lock()
		assert.Nil(t, manager.monitorCancel, "Monitor should be disabled at iteration %d", i)
		manager.mu.Unlock()
	}
}

func TestEnableMonitor_WhenNotOpen(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)

	// Initialize the manager context but don't set isOpen
	manager.mu.Lock()
	manager.ctx, manager.cancel = context.WithCancel(context.TODO())
	// isOpen is false by default
	manager.mu.Unlock()
	defer manager.cancel()

	// Try to enable monitor when manager is not open
	err = manager.enableMonitorInternal()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manager is not open")
}

func TestPausePostgresMonitor_RequiresActionLock(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	config := &Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
	}

	manager, err := NewMultiPoolerManager(logger, config)
	require.NoError(t, err)

	// Initialize the manager context
	manager.mu.Lock()
	manager.ctx, manager.cancel = context.WithCancel(context.TODO())
	manager.isOpen = true
	manager.mu.Unlock()
	defer manager.cancel()

	t.Run("WithoutActionLock", func(t *testing.T) {
		// Try to call PausePostgresMonitor without holding the action lock
		resumeMonitor, err := manager.PausePostgresMonitor(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "action lock")
		assert.Nil(t, resumeMonitor)
	})

	t.Run("WithActionLock", func(t *testing.T) {
		// Acquire the action lock
		lockCtx, err := manager.actionLock.Acquire(ctx, "test")
		require.NoError(t, err)
		defer manager.actionLock.Release(lockCtx)

		// Should succeed with action lock held
		resumeMonitor, err := manager.PausePostgresMonitor(lockCtx)
		require.NoError(t, err)
		assert.NotNil(t, resumeMonitor)

		// Clean up - resume also requires action lock
		resumeMonitor(lockCtx)
	})

	t.Run("ResumeWithoutActionLock", func(t *testing.T) {
		// Acquire the action lock
		lockCtx, err := manager.actionLock.Acquire(ctx, "test")
		require.NoError(t, err)

		// Should succeed with action lock held
		resumeMonitor, err := manager.PausePostgresMonitor(lockCtx)
		require.NoError(t, err)
		assert.NotNil(t, resumeMonitor)

		// Release the lock before calling resume
		manager.actionLock.Release(lockCtx)

		// Resume should detect missing action lock and log error (but not panic)
		// This is a graceful degradation - logs error but doesn't crash
		resumeMonitor(ctx)
	})
}
