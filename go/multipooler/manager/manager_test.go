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

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/multipooler/executor/mock"
	"github.com/multigres/multigres/go/test/utils"
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

	serviceID := &clustermetadatapb.ID{
		Cell: "zone1",
		Name: "test-service",
	}

	multiPooler := topoclient.NewMultiPooler(serviceID.Name, serviceID.Cell, "localhost", constants.DefaultTableGroup)
	multiPooler.Shard = constants.DefaultShard
	multiPooler.PoolerDir = "/tmp/test"

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
	require.NoError(t, err)
	defer manager.Close()

	// Initial state should be Starting
	assert.Equal(t, ManagerStateStarting, manager.GetState())

	state, err := manager.GetStateAndError()
	assert.Equal(t, ManagerStateStarting, state)
	assert.Nil(t, err)
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

	multiPooler := topoclient.NewMultiPooler(serviceID.Name, serviceID.Cell, "localhost", constants.DefaultTableGroup)
	multiPooler.Shard = constants.DefaultShard
	multiPooler.PoolerDir = "/tmp/test"

	config := &Config{
		TopoClient: ts,
	}

	// Create manager with a short timeout for testing
	manager, err := NewMultiPoolerManagerWithTimeout(logger, multiPooler, config, 1*time.Second)
	require.NoError(t, err)
	defer manager.Close()

	// Start the async loader
	go manager.loadMultiPoolerFromTopo()

	// Wait for the state to become Error
	require.Eventually(t, func() bool {
		return manager.GetState() == ManagerStateError
	}, 3*time.Second, 100*time.Millisecond, "Manager should reach Error state")

	// Verify the error state
	state, err := manager.GetStateAndError()
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

	multiPooler := topoclient.NewMultiPooler(serviceID.Name, serviceID.Cell, "localhost", constants.DefaultTableGroup)
	multiPooler.Shard = constants.DefaultShard
	multiPooler.PoolerDir = "/tmp/test"

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
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
	_, err = manager.GetStateAndError()
	assert.Contains(t, err.Error(), "cancelled")
}

func TestManagerState_RetryUntilSuccess(t *testing.T) {
	t.Skip("TODO(sougou): Need to change to test new retry behavior after implementation")
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

	multiPoolerObj := topoclient.NewMultiPooler(serviceID.Name, serviceID.Cell, "localhost", constants.DefaultTableGroup)
	multiPoolerObj.Shard = constants.DefaultShard
	multiPoolerObj.PoolerDir = poolerDir

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPoolerObj, config)
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
	state, err := manager.GetStateAndError()
	assert.Equal(t, ManagerStateReady, state)
	assert.Nil(t, err)
}

func TestManagerState_NilServiceID(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create MultiPooler with nil Id to test validation
	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         nil, // Nil ID for testing
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerDir:  "/tmp/test",
	}

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPooler, config)

	// Now that MultiPooler.Id is validated in constructor, we expect an error immediately
	require.Error(t, err)
	require.Nil(t, manager)
	assert.Contains(t, err.Error(), "MultiPooler.Id is required")
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
				PoolerDir:     poolerDir,
			}
			require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

			config := &Config{
				TopoClient:       ts,
				ConsensusEnabled: true,
			}
			manager, err := NewMultiPoolerManager(logger, multipooler, config)
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
	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         serviceID,
		Database:   database,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerDir:  filepath.Join(tmpDir, "pooler"),
	}
	config := &Config{
		TopoClient: ts,
		PgctldAddr: "",
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
	require.NoError(t, err)

	// Set backup config
	backupConfig, err := backup.NewConfig(
		utils.FilesystemBackupLocation("/var/backups/pgbackrest"),
	)
	require.NoError(t, err)
	manager.backupConfig = backupConfig

	// Test backup config
	assert.Equal(t, "filesystem", manager.backupConfig.Type())
	expectedShardBackupLocation := filepath.Join("/var/backups/pgbackrest", database, constants.DefaultTableGroup, constants.DefaultShard)
	shardPath, err := manager.backupConfig.FullPath(database, constants.DefaultTableGroup, constants.DefaultShard)
	require.NoError(t, err)
	assert.Equal(t, expectedShardBackupLocation, shardPath)
}

func TestGetBackupLocation_S3(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	// Create test topology store
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create test database with S3 backup location
	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	// Create manager config
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         serviceID,
		Database:   database,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerDir:  filepath.Join(tmpDir, "pooler"),
	}
	config := &Config{
		TopoClient: ts,
		PgctldAddr: "",
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
	require.NoError(t, err)

	// Set S3 backup config
	backupConfig, err := backup.NewConfig(
		utils.S3BackupLocation("my-backup-bucket", "us-west-2",
			utils.WithS3KeyPrefix("prod/backups/")),
	)
	require.NoError(t, err)
	manager.backupConfig = backupConfig

	// Test S3 backup config
	assert.Equal(t, "s3", manager.backupConfig.Type())

	// Verify full path includes S3 bucket, prefix, and path components
	expectedPath := "s3://my-backup-bucket/prod/backups/testdb/default/0-inf"
	shardPath, err := manager.backupConfig.FullPath(database, constants.DefaultTableGroup, constants.DefaultShard)
	require.NoError(t, err)
	assert.Equal(t, expectedPath, shardPath)

	// Verify PgBackRestConfig returns correct S3 settings
	pgbrConfig, err := manager.backupConfig.PgBackRestConfig("multigres")
	require.NoError(t, err)
	assert.Equal(t, "s3", pgbrConfig["repo1-type"])
	assert.Equal(t, "my-backup-bucket", pgbrConfig["repo1-s3-bucket"])
	assert.Equal(t, "us-west-2", pgbrConfig["repo1-s3-region"])
	assert.Equal(t, "auto", pgbrConfig["repo1-s3-key-type"])
	assert.Equal(t, "/prod/backups/multigres", pgbrConfig["repo1-path"])
}

// TestWaitUntilReady_Success verifies that WaitUntilReady returns immediately
// when the manager is already in Ready state
func TestWaitUntilReady_Success(t *testing.T) {
	logger := slog.Default()

	serviceID := &clustermetadatapb.ID{
		Cell: "zone1",
		Name: "test-service",
	}
	multiPooler := topoclient.NewMultiPooler(serviceID.Name, serviceID.Cell, "localhost", constants.DefaultTableGroup)
	multiPooler.Shard = constants.DefaultShard
	multiPooler.PoolerDir = "/tmp/test"

	config := &Config{
		ConsensusEnabled: false,
	}

	pm, err := NewMultiPoolerManagerWithTimeout(logger, multiPooler, config, 100*time.Millisecond)
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

	serviceID := &clustermetadatapb.ID{
		Cell: "zone1",
		Name: "test-service",
	}
	multiPooler := topoclient.NewMultiPooler(serviceID.Name, serviceID.Cell, "localhost", constants.DefaultTableGroup)
	multiPooler.Shard = constants.DefaultShard
	multiPooler.PoolerDir = "/tmp/test"

	config := &Config{
		ConsensusEnabled: false,
	}

	pm, err := NewMultiPoolerManagerWithTimeout(logger, multiPooler, config, 100*time.Millisecond)
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

	serviceID := &clustermetadatapb.ID{
		Cell: "zone1",
		Name: "test-service",
	}
	multiPooler := topoclient.NewMultiPooler(serviceID.Name, serviceID.Cell, "localhost", constants.DefaultTableGroup)
	multiPooler.Shard = constants.DefaultShard
	multiPooler.PoolerDir = "/tmp/test"

	config := &Config{
		ConsensusEnabled: false,
	}

	pm, err := NewMultiPoolerManagerWithTimeout(logger, multiPooler, config, 100*time.Millisecond)
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

	serviceID := &clustermetadatapb.ID{
		Cell: "zone1",
		Name: "test-service",
	}
	multiPooler := topoclient.NewMultiPooler(serviceID.Name, serviceID.Cell, "localhost", constants.DefaultTableGroup)
	multiPooler.Shard = constants.DefaultShard
	multiPooler.PoolerDir = "/tmp/test"

	config := &Config{
		ConsensusEnabled: false,
	}

	pm, err := NewMultiPoolerManagerWithTimeout(logger, multiPooler, config, 100*time.Millisecond)
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
			multiPooler := &clustermetadatapb.MultiPooler{
				Id:         serviceID,
				Database:   "testdb",
				Hostname:   "localhost",
				PortMap:    map[string]int32{"grpc": 8080},
				TableGroup: tt.tableGroup,
				Shard:      tt.shard,
				PoolerDir:  "/tmp/test",
			}

			config := &Config{
				TopoClient: ts,
			}

			manager, err := NewMultiPoolerManager(logger, multiPooler, config)
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

	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         serviceID,
		Database:   "testdb",
		Hostname:   "localhost",
		PortMap:    map[string]int32{"grpc": 8080},
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerDir:  "/tmp/test",
	}

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
	require.NoError(t, err)

	// Initialize the manager context (simulating Open without connecting to DB)
	manager.mu.Lock()
	manager.ctx, manager.cancel = context.WithCancel(context.TODO())
	manager.isOpen = true
	manager.mu.Unlock()
	defer manager.cancel()

	// Verify monitor is not running initially
	assert.False(t, manager.pgMonitor.Running(), "Monitor should not be running initially")

	// Enable the monitor
	err = manager.enableMonitorInternal()
	require.NoError(t, err)

	// Verify monitor is enabled
	assert.True(t, manager.pgMonitor.Running(), "Monitor should be enabled")

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

	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         serviceID,
		Database:   "testdb",
		Hostname:   "localhost",
		PortMap:    map[string]int32{"grpc": 8080},
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerDir:  "/tmp/test",
	}

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
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
	assert.True(t, manager.pgMonitor.Running(), "Monitor should be enabled")

	// Enable again - should be idempotent (no error, monitor still enabled)
	err = manager.enableMonitorInternal()
	require.NoError(t, err)

	// Verify monitor is still enabled (idempotency means calling again has no effect)
	assert.True(t, manager.pgMonitor.Running(), "Monitor should still be enabled after second call")

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

	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         serviceID,
		Database:   "testdb",
		Hostname:   "localhost",
		PortMap:    map[string]int32{"grpc": 8080},
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerDir:  "/tmp/test",
	}

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
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
	assert.True(t, manager.pgMonitor.Running(), "Monitor should be running after enableMonitorInternal")

	// Disable the monitor
	manager.disableMonitorInternal()

	// Verify monitor is disabled
	assert.False(t, manager.pgMonitor.Running(), "Monitor should be disabled")
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

	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         serviceID,
		Database:   "testdb",
		Hostname:   "localhost",
		PortMap:    map[string]int32{"grpc": 8080},
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerDir:  "/tmp/test",
	}

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
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
	assert.False(t, manager.pgMonitor.Running(), "Monitor should be disabled")

	// Disable again - should be idempotent (no panic)
	manager.disableMonitorInternal()

	// Verify monitor is still disabled
	assert.False(t, manager.pgMonitor.Running(), "Monitor should still be disabled")
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

	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         serviceID,
		Database:   "testdb",
		Hostname:   "localhost",
		PortMap:    map[string]int32{"grpc": 8080},
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerDir:  "/tmp/test",
	}

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
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
		assert.True(t, manager.pgMonitor.Running(), "Monitor should be enabled at iteration %d", i)

		// Give the goroutine a moment to start
		time.Sleep(10 * time.Millisecond)

		// Disable
		manager.disableMonitorInternal()
		assert.False(t, manager.pgMonitor.Running(), "Monitor should be disabled at iteration %d", i)
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

	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         serviceID,
		Database:   "testdb",
		Hostname:   "localhost",
		PortMap:    map[string]int32{"grpc": 8080},
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerDir:  "/tmp/test",
	}

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
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

	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         serviceID,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		Database:   "test-db",
	}

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
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
