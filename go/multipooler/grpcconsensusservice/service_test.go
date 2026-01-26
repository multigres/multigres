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

package grpcconsensusservice

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/multipooler/connpoolmanager"
	"github.com/multigres/multigres/go/multipooler/manager"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdata "github.com/multigres/multigres/go/pb/consensusdata"
)

func addDatabaseToTopo(t *testing.T, ts topoclient.Store, database string) {
	t.Helper()
	ctx := context.Background()
	err := ts.CreateDatabase(ctx, database, &clustermetadata.Database{
		Name:             database,
		BackupLocation:   utils.FilesystemBackupLocation("/var/backups/pgbackrest"),
		DurabilityPolicy: "ANY_2",
	})
	require.NoError(t, err)
}

func TestConsensusService_BeginTerm(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	t.Cleanup(cleanupPgctld)

	// Create database in topology
	addDatabaseToTopo(t, ts, "testdb")

	// Create the multipooler in topology so manager can reach ready state
	serviceID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multipooler := &clustermetadata.MultiPooler{
		Id:            serviceID,
		Database:      "testdb",
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadata.PoolerType_REPLICA,
		ServingStatus: clustermetadata.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Create temporary directory for pooler
	tmpDir := t.TempDir()

	multipooler.PoolerDir = tmpDir

	config := &manager.Config{
		TopoClient:       ts,
		PgctldAddr:       pgctldAddr,
		ConsensusEnabled: true,
		ConnPoolConfig:   connpoolmanager.NewConfig(viperutil.NewRegistry()),
	}
	pm, err := manager.NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	// Mark as initialized to skip auto-restore (not testing backup functionality)
	// Create both PG_VERSION and the marker file since setInitialized() is not exported
	pgDataDir := tmpDir + "/pg_data"
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(pgDataDir+"/PG_VERSION", []byte("16\n"), 0o644))
	require.NoError(t, os.WriteFile(pgDataDir+"/MULTIGRES_INITIALIZED", []byte("initialized\n"), 0o644))
	defer pm.Close()

	// Start the async loader
	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go pm.Start(senv)

	// Wait for the manager to become ready
	require.Eventually(t, func() bool {
		return pm.GetState() == manager.ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	svc := &consensusService{
		manager: pm,
	}

	t.Run("BeginTerm with REVOKE action without database connection should reject", func(t *testing.T) {
		req := &consensusdata.BeginTermRequest{
			Term: 5,
			CandidateId: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "candidate-1",
			},
			ShardId: "shard-1",
			Action:  consensusdata.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
		}

		resp, err := svc.BeginTerm(ctx, req)

		// TODO: This behavior is temporary. Once we separate voting term from primary term,
		// unhealthy nodes should accept the term (for voting) even if they can't execute revoke.
		// Current behavior: reject term when postgres is down with REVOKE action
		// Future behavior: accept term (voting term) but keep old primary term
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.False(t, resp.Accepted, "Term should be rejected when postgres is unhealthy with REVOKE action")
	})
}

func TestConsensusService_Status(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	t.Cleanup(cleanupPgctld)

	// Create database in topology
	addDatabaseToTopo(t, ts, "testdb")

	// Create the multipooler in topology
	serviceID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multipooler := &clustermetadata.MultiPooler{
		Id:            serviceID,
		Database:      "testdb",
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadata.PoolerType_REPLICA,
		ServingStatus: clustermetadata.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Create temporary directory for pooler
	tmpDir := t.TempDir()

	multipooler.PoolerDir = tmpDir

	config := &manager.Config{
		TopoClient:       ts,
		PgctldAddr:       pgctldAddr,
		ConsensusEnabled: true,
		ConnPoolConfig:   connpoolmanager.NewConfig(viperutil.NewRegistry()),
	}
	pm, err := manager.NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	// Mark as initialized to skip auto-restore (not testing backup functionality)
	// Create both PG_VERSION and the marker file since setInitialized() is not exported
	pgDataDir := tmpDir + "/pg_data"
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(pgDataDir+"/PG_VERSION", []byte("16\n"), 0o644))
	require.NoError(t, os.WriteFile(pgDataDir+"/MULTIGRES_INITIALIZED", []byte("initialized\n"), 0o644))
	defer pm.Close()

	// Start the async loader
	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go pm.Start(senv)

	// Wait for the manager to become ready
	require.Eventually(t, func() bool {
		return pm.GetState() == manager.ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	svc := &consensusService{
		manager: pm,
	}

	t.Run("Status returns node information", func(t *testing.T) {
		req := &consensusdata.StatusRequest{
			ShardId: "shard-1",
		}

		resp, err := svc.Status(ctx, req)

		// Should succeed
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "test-service", resp.PoolerId)
		assert.Equal(t, "zone1", resp.Cell)
		// Without database, should not be healthy
		assert.False(t, resp.IsHealthy)
		// But should still be eligible
		assert.True(t, resp.IsEligible)
	})
}

func TestConsensusService_GetLeadershipView(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	t.Cleanup(cleanupPgctld)

	// Create database in topology
	addDatabaseToTopo(t, ts, "testdb")

	// Create the multipooler in topology
	serviceID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multipooler := &clustermetadata.MultiPooler{
		Id:            serviceID,
		Database:      "testdb",
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadata.PoolerType_REPLICA,
		ServingStatus: clustermetadata.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Create temporary directory for pooler
	tmpDir := t.TempDir()

	multipooler.PoolerDir = tmpDir

	config := &manager.Config{
		TopoClient:       ts,
		PgctldAddr:       pgctldAddr,
		ConsensusEnabled: true,
		ConnPoolConfig:   connpoolmanager.NewConfig(viperutil.NewRegistry()),
	}
	pm, err := manager.NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	// Mark as initialized to skip auto-restore (not testing backup functionality)
	// Create both PG_VERSION and the marker file since setInitialized() is not exported
	pgDataDir := tmpDir + "/pg_data"
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(pgDataDir+"/PG_VERSION", []byte("16\n"), 0o644))
	require.NoError(t, os.WriteFile(pgDataDir+"/MULTIGRES_INITIALIZED", []byte("initialized\n"), 0o644))
	defer pm.Close()

	// Start the async loader
	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go pm.Start(senv)

	// Wait for the manager to become ready
	require.Eventually(t, func() bool {
		return pm.GetState() == manager.ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	svc := &consensusService{
		manager: pm,
	}

	t.Run("GetLeadershipView without a valid database connection should fail", func(t *testing.T) {
		req := &consensusdata.LeadershipViewRequest{
			ShardId: "shard-1",
		}

		resp, err := svc.GetLeadershipView(ctx, req)

		// Should fail because no database to query
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "failed to connect to")
	})
}

func TestConsensusService_CanReachPrimary(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	t.Cleanup(cleanupPgctld)

	// Create database in topology
	addDatabaseToTopo(t, ts, "testdb")

	// Create the multipooler in topology
	serviceID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multipooler := &clustermetadata.MultiPooler{
		Id:            serviceID,
		Database:      "testdb",
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadata.PoolerType_REPLICA,
		ServingStatus: clustermetadata.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Create temporary directory for pooler
	tmpDir := t.TempDir()

	multipooler.PoolerDir = tmpDir

	config := &manager.Config{
		TopoClient:       ts,
		PgctldAddr:       pgctldAddr,
		ConsensusEnabled: true,
		ConnPoolConfig:   connpoolmanager.NewConfig(viperutil.NewRegistry()),
	}
	pm, err := manager.NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	// Mark as initialized to skip auto-restore (not testing backup functionality)
	// Create both PG_VERSION and the marker file since setInitialized() is not exported
	pgDataDir := tmpDir + "/pg_data"
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(pgDataDir+"/PG_VERSION", []byte("16\n"), 0o644))
	require.NoError(t, os.WriteFile(pgDataDir+"/MULTIGRES_INITIALIZED", []byte("initialized\n"), 0o644))
	defer pm.Close()

	// Start the async loader
	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go pm.Start(senv)

	// Wait for the manager to become ready
	require.Eventually(t, func() bool {
		return pm.GetState() == manager.ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	svc := &consensusService{
		manager: pm,
	}

	t.Run("CanReachPrimary without database connection", func(t *testing.T) {
		req := &consensusdata.CanReachPrimaryRequest{
			PrimaryHost: "primary.example.com",
			PrimaryPort: 5432,
		}

		resp, err := svc.CanReachPrimary(ctx, req)

		// Should succeed but indicate not reachable due to no database connection
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.False(t, resp.Reachable) // No database connection
		assert.Equal(t, "database connection not available", resp.ErrorMessage)
	})
}

func TestConsensusService_AllMethods(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	t.Cleanup(cleanupPgctld)

	// Create database in topology
	addDatabaseToTopo(t, ts, "testdb")

	// Create the multipooler in topology
	serviceID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multipooler := &clustermetadata.MultiPooler{
		Id:            serviceID,
		Database:      "testdb",
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadata.PoolerType_REPLICA,
		ServingStatus: clustermetadata.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Create temporary directory for pooler
	tmpDir := t.TempDir()

	multipooler.PoolerDir = tmpDir

	config := &manager.Config{
		TopoClient:       ts,
		PgctldAddr:       pgctldAddr,
		ConsensusEnabled: true,
		ConnPoolConfig:   connpoolmanager.NewConfig(viperutil.NewRegistry()),
	}
	pm, err := manager.NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	// Mark as initialized to skip auto-restore (not testing backup functionality)
	// Create both PG_VERSION and the marker file since setInitialized() is not exported
	pgDataDir := tmpDir + "/pg_data"
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(pgDataDir+"/PG_VERSION", []byte("16\n"), 0o644))
	require.NoError(t, os.WriteFile(pgDataDir+"/MULTIGRES_INITIALIZED", []byte("initialized\n"), 0o644))
	defer pm.Close()

	// Start the async loader
	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go pm.Start(senv)

	// Wait for the manager to become ready
	require.Eventually(t, func() bool {
		return pm.GetState() == manager.ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	svc := &consensusService{
		manager: pm,
	}

	tests := []struct {
		name          string
		method        func() error
		shouldSucceed bool
	}{
		{
			name: "BeginTerm",
			method: func() error {
				req := &consensusdata.BeginTermRequest{
					Term: 5,
					CandidateId: &clustermetadata.ID{
						Component: clustermetadata.ID_MULTIPOOLER,
						Cell:      "zone1",
						Name:      "candidate-1",
					},
					ShardId: "shard-1",
					Action:  consensusdata.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
				}
				_, err := svc.BeginTerm(ctx, req)
				return err
			},
			// No database connection, revoke action short-circuits and rejects the call
			shouldSucceed: true,
		},
		{
			name: "Status",
			method: func() error {
				req := &consensusdata.StatusRequest{
					ShardId: "shard-1",
				}
				_, err := svc.Status(ctx, req)
				return err
			},
			shouldSucceed: true, // Works without database
		},
		{
			name: "GetLeadershipView",
			method: func() error {
				req := &consensusdata.LeadershipViewRequest{
					ShardId: "shard-1",
				}
				_, err := svc.GetLeadershipView(ctx, req)
				return err
			},
			shouldSucceed: false, // No replication tracker
		},
		{
			name: "CanReachPrimary",
			method: func() error {
				req := &consensusdata.CanReachPrimaryRequest{
					PrimaryHost: "primary.example.com",
					PrimaryPort: 5432,
				}
				_, err := svc.CanReachPrimary(ctx, req)
				return err
			},
			shouldSucceed: true, // Returns non-error response even if not reachable
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.method()

			if tt.shouldSucceed {
				assert.NoError(t, err, "Method %s should succeed", tt.name)
			} else {
				assert.Error(t, err, "Method %s should fail without database/replication tracker", tt.name)
			}
		})
	}
}
