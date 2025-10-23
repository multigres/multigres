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

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/servenv"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
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
				initialTerm := &pgctldpb.ConsensusTerm{
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

func TestPrimaryPosition(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	tests := []struct {
		name          string
		poolerType    clustermetadatapb.PoolerType
		expectError   bool
		expectedCode  mtrpcpb.Code
		errorContains string
	}{
		{
			name:          "REPLICA pooler returns FAILED_PRECONDITION",
			poolerType:    clustermetadatapb.PoolerType_REPLICA,
			expectError:   true,
			expectedCode:  mtrpcpb.Code_FAILED_PRECONDITION,
			errorContains: "pooler type is REPLICA",
		},
		{
			name:          "PRIMARY pooler passes type check",
			poolerType:    clustermetadatapb.PoolerType_PRIMARY,
			expectError:   true,
			errorContains: "database connection failed", // Will fail on DB connection, not type check
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
			defer ts.Close()

			// Create temp directory for pooler-dir
			poolerDir := t.TempDir()

			multipooler := &clustermetadatapb.MultiPooler{
				Id:            serviceID,
				Database:      "testdb",
				Hostname:      "localhost",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          tt.poolerType,
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

			// Call PrimaryPosition
			_, err := manager.PrimaryPosition(ctx)

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

func TestParseAndRedactPrimaryConnInfo(t *testing.T) {
	tests := []struct {
		name        string
		connInfo    string
		expected    *multipoolermanagerdata.PrimaryConnInfo
		expectError bool
	}{
		{
			name:     "Complete connection string",
			connInfo: "host=localhost port=5432 user=postgres application_name=test_cell_standby1",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "test_cell_standby1",
				Raw:             "host=localhost port=5432 user=postgres application_name=test_cell_standby1",
			},
		},
		{
			name:     "Missing application_name",
			connInfo: "host=primary.example.com port=5433 user=replicator",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "primary.example.com",
				Port:            5433,
				User:            "replicator",
				ApplicationName: "",
				Raw:             "host=primary.example.com port=5433 user=replicator",
			},
		},
		{
			name:     "Missing port",
			connInfo: "host=localhost user=postgres application_name=test_app",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            0,
				User:            "postgres",
				ApplicationName: "test_app",
				Raw:             "host=localhost user=postgres application_name=test_app",
			},
		},
		{
			name:     "Empty string",
			connInfo: "",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "",
				Port:            0,
				User:            "",
				ApplicationName: "",
				Raw:             "",
			},
		},
		{
			name:     "Extra parameters ignored",
			connInfo: "host=localhost port=5432 user=postgres application_name=test keepalives_idle=30 keepalives_interval=10",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "test",
				Raw:             "host=localhost port=5432 user=postgres application_name=test keepalives_idle=30 keepalives_interval=10",
			},
		},
		{
			name:     "Invalid port ignored",
			connInfo: "host=localhost port=invalid user=postgres",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            0,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=invalid user=postgres",
			},
		},
		{
			name:     "Connection with sslmode",
			connInfo: "host=localhost port=5432 user=postgres sslmode=require",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres sslmode=require",
			},
		},
		{
			name:     "Connection with password (redacted)",
			connInfo: "host=localhost port=5432 user=postgres password=secret123",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres password=[REDACTED]",
			},
		},
		{
			name:     "Connection with passfile",
			connInfo: "host=localhost port=5432 user=postgres passfile=/home/user/.pgpass",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres passfile=/home/user/.pgpass",
			},
		},
		{
			name:     "Connection with multiple SSL parameters",
			connInfo: "host=localhost port=5432 user=postgres sslmode=verify-full sslcert=/path/to/cert.pem sslkey=/path/to/key.pem sslrootcert=/path/to/ca.pem",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres sslmode=verify-full sslcert=/path/to/cert.pem sslkey=/path/to/key.pem sslrootcert=/path/to/ca.pem",
			},
		},
		{
			name:     "Connection with keepalive and timeout parameters",
			connInfo: "host=localhost port=5432 user=postgres keepalives_idle=30 keepalives_interval=10 keepalives_count=5 connect_timeout=10",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres keepalives_idle=30 keepalives_interval=10 keepalives_count=5 connect_timeout=10",
			},
		},
		{
			name:     "Connection with channel_binding",
			connInfo: "host=localhost port=5432 user=postgres channel_binding=require",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres channel_binding=require",
			},
		},
		{
			name:     "Connection with gssencmode",
			connInfo: "host=localhost port=5432 user=postgres gssencmode=prefer",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres gssencmode=prefer",
			},
		},
		{
			name:     "Complex connection with all parsed and unparsed fields",
			connInfo: "host=primary.db.local port=5433 user=replicator application_name=zone1_standby2 sslmode=require keepalives_idle=60 connect_timeout=30",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "primary.db.local",
				Port:            5433,
				User:            "replicator",
				ApplicationName: "zone1_standby2",
				Raw:             "host=primary.db.local port=5433 user=replicator application_name=zone1_standby2 sslmode=require keepalives_idle=60 connect_timeout=30",
			},
		},
		{
			name:     "Connection with hostaddr",
			connInfo: "hostaddr=172.28.40.9 port=5432 user=postgres",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "hostaddr=172.28.40.9 port=5432 user=postgres",
			},
		},
		{
			name:     "Connection with dbname",
			connInfo: "host=localhost port=5432 dbname=mydb user=postgres",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 dbname=mydb user=postgres",
			},
		},
		{
			name:     "Connection with client_encoding",
			connInfo: "host=localhost port=5432 user=postgres client_encoding=UTF8",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres client_encoding=UTF8",
			},
		},
		{
			name:     "Connection with options parameter",
			connInfo: "host=localhost port=5432 user=postgres options=-c\\ geqo=off",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres options=-c\\ geqo=off",
			},
		},
		{
			name:     "Connection with replication mode",
			connInfo: "host=localhost port=5432 user=postgres replication=database",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres replication=database",
			},
		},
		{
			name:     "Connection with target_session_attrs",
			connInfo: "host=localhost port=5432 user=postgres target_session_attrs=read-write",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres target_session_attrs=read-write",
			},
		},
		{
			name:     "Connection with sslcrl and sslcompression",
			connInfo: "host=localhost port=5432 user=postgres sslmode=verify-full sslcrl=/path/to/crl.pem sslcompression=1",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres sslmode=verify-full sslcrl=/path/to/crl.pem sslcompression=1",
			},
		},
		{
			name:     "Connection with requirepeer",
			connInfo: "host=localhost port=5432 user=postgres requirepeer=postgres",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres requirepeer=postgres",
			},
		},
		{
			name:     "Connection with krbsrvname and gsslib",
			connInfo: "host=localhost port=5432 user=postgres krbsrvname=postgres gsslib=gssapi",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres krbsrvname=postgres gsslib=gssapi",
			},
		},
		{
			name:     "Connection with service",
			connInfo: "service=myservice",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "",
				Port:            0,
				User:            "",
				ApplicationName: "",
				Raw:             "service=myservice",
			},
		},
		{
			name:     "Comprehensive connection with password redaction",
			connInfo: "host=prod.db.com port=5433 user=repl_user password=SuperSecret123 application_name=standby1 sslmode=verify-full sslcert=/certs/client.pem sslkey=/certs/client.key keepalives_idle=30 connect_timeout=10",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "prod.db.com",
				Port:            5433,
				User:            "repl_user",
				ApplicationName: "standby1",
				Raw:             "host=prod.db.com port=5433 user=repl_user password=[REDACTED] application_name=standby1 sslmode=verify-full sslcert=/certs/client.pem sslkey=/certs/client.key keepalives_idle=30 connect_timeout=10",
			},
		},
		{
			name:        "Invalid format - space-separated without equals",
			connInfo:    "host localhost port 5432",
			expectError: true,
		},
		{
			name:        "Invalid format - missing equals sign in one parameter",
			connInfo:    "host=localhost port 5432 user=postgres",
			expectError: true,
		},
		{
			name:        "Invalid format - empty key",
			connInfo:    "host=localhost =5432 user=postgres",
			expectError: true,
		},
		{
			name:     "Connection with multiple spaces between parameters",
			connInfo: "host=localhost   port=5432  user=postgres",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres", // Spaces normalized due to split/join
			},
		},
		{
			name:     "Connection with leading and trailing spaces",
			connInfo: "  host=localhost port=5432 user=postgres  ",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres", // Leading/trailing spaces trimmed
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseAndRedactPrimaryConnInfo(tt.connInfo)

			if tt.expectError {
				// Expect parsing to fail for invalid formats
				require.Error(t, err, "Should return error for invalid format")
				assert.Nil(t, result, "Result should be nil when parsing fails")
			} else {
				// Expect parsing to succeed
				require.NoError(t, err, "Should not return error for valid format")
				require.NotNil(t, result, "Result should not be nil")
				assert.Equal(t, tt.expected.Host, result.Host, "Host should match")
				assert.Equal(t, tt.expected.Port, result.Port, "Port should match")
				assert.Equal(t, tt.expected.User, result.User, "User should match")
				assert.Equal(t, tt.expected.ApplicationName, result.ApplicationName, "ApplicationName should match")
				assert.Equal(t, tt.expected.Raw, result.Raw, "Raw should match")
			}
		})
	}
}
