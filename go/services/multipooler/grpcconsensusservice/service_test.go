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
	"path/filepath"
	"testing"
	"time"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/manager"
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
		Name:           database,
		BackupLocation: utils.FilesystemBackupLocation("/var/backups/pgbackrest"),
	})
	require.NoError(t, err)
}

func TestConsensusService_Status(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
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
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadata.PoolerType_REPLICA,
		ServingStatus: clustermetadata.PoolerServingStatus_SERVING,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "testdb",
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Create temporary directory for pooler
	tmpDir := t.TempDir()

	multipooler.PoolerDir = tmpDir

	// ConnPoolConfig now requires ResolvePgPassword to run before any pool is
	// opened; seed POSTGRES_PASSWORD so the env path resolves cleanly. The
	// value is irrelevant — fake postgres uses trust auth.
	t.Setenv(constants.PgPasswordEnvVar, "test-password")
	connPoolConfig := connpoolmanager.NewConfig(viperutil.NewRegistry())
	require.NoError(t, connPoolConfig.ResolvePgPassword())
	config := &manager.Config{
		TopoClient:       ts,
		PgctldAddr:       pgctldAddr,
		ConsensusEnabled: true,
		ConnPoolConfig:   connPoolConfig,
	}
	pm, err := manager.NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	// Mark as initialized to skip auto-restore (not testing backup functionality)
	// Create both PG_VERSION and the marker file since setInitialized() is not exported
	pgDataDir := tmpDir + "/pg_data"
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(pgDataDir+"/PG_VERSION", []byte("16\n"), 0o644))
	t.Setenv(constants.PgDataDirEnvVar, pgDataDir)
	require.NoError(t, os.MkdirAll(filepath.Join(pgDataDir, constants.MultigresMarkerDirectory), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pgDataDir, constants.MultigresMarkerDirectory, "MULTIGRES_INITIALIZED"), []byte("initialized\n"), 0o644))
	defer pm.Shutdown()

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
		assert.Equal(t, "test-service", resp.GetId().GetName())
		assert.Equal(t, "zone1", resp.GetId().GetCell())
	})
}

func TestConsensusService_AllMethods(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
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
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadata.PoolerType_REPLICA,
		ServingStatus: clustermetadata.PoolerServingStatus_SERVING,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "testdb",
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Create temporary directory for pooler
	tmpDir := t.TempDir()

	multipooler.PoolerDir = tmpDir

	// ConnPoolConfig now requires ResolvePgPassword to run before any pool is
	// opened; seed POSTGRES_PASSWORD so the env path resolves cleanly. The
	// value is irrelevant — fake postgres uses trust auth.
	t.Setenv(constants.PgPasswordEnvVar, "test-password")
	connPoolConfig := connpoolmanager.NewConfig(viperutil.NewRegistry())
	require.NoError(t, connPoolConfig.ResolvePgPassword())
	config := &manager.Config{
		TopoClient:       ts,
		PgctldAddr:       pgctldAddr,
		ConsensusEnabled: true,
		ConnPoolConfig:   connPoolConfig,
	}
	pm, err := manager.NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	// Mark as initialized to skip auto-restore (not testing backup functionality)
	// Create both PG_VERSION and the marker file since setInitialized() is not exported
	pgDataDir := tmpDir + "/pg_data"
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(pgDataDir+"/PG_VERSION", []byte("16\n"), 0o644))
	t.Setenv(constants.PgDataDirEnvVar, pgDataDir)
	require.NoError(t, os.MkdirAll(filepath.Join(pgDataDir, constants.MultigresMarkerDirectory), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pgDataDir, constants.MultigresMarkerDirectory, "MULTIGRES_INITIALIZED"), []byte("initialized\n"), 0o644))
	defer pm.Shutdown()

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
