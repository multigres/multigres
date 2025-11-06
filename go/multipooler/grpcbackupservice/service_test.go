// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcbackupservice

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/multipooler/manager"
	"github.com/multigres/multigres/go/servenv"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	backupservicepb "github.com/multigres/multigres/go/pb/multipoolerbackupservice"
)

func TestBackupService_Backup(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	t.Cleanup(cleanupPgctld)

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
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Create temporary directory for pooler
	tmpDir := t.TempDir()

	config := &manager.Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PgctldAddr: pgctldAddr,
		PoolerDir:  tmpDir,
	}
	pm := manager.NewMultiPoolerManager(logger, config)
	defer pm.Close()

	// Start the async loader
	senv := servenv.NewServEnv()
	go pm.Start(senv)

	// Wait for the manager to become ready
	require.Eventually(t, func() bool {
		return pm.GetState() == manager.ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	svc := &backupService{
		manager: pm,
	}

	t.Run("Backup with missing table_group", func(t *testing.T) {
		req := &backupservicepb.BackupRequest{
			TableGroup:   "",
			Shard:        "shard-1",
			ForcePrimary: false,
			Type:         "full",
		}

		resp, err := svc.Backup(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, resp)
		mterr := mterrors.FromGRPC(err)
		assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(mterr))
		assert.Contains(t, err.Error(), "table_group is required")
	})

	t.Run("Backup with missing shard", func(t *testing.T) {
		req := &backupservicepb.BackupRequest{
			TableGroup:   "tg-1",
			Shard:        "",
			ForcePrimary: false,
			Type:         "full",
		}

		resp, err := svc.Backup(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, resp)
		mterr := mterrors.FromGRPC(err)
		assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(mterr))
		assert.Contains(t, err.Error(), "shard is required")
	})

	t.Run("Backup with missing type", func(t *testing.T) {
		req := &backupservicepb.BackupRequest{
			TableGroup:   "tg-1",
			Shard:        "shard-1",
			ForcePrimary: false,
			Type:         "",
		}

		resp, err := svc.Backup(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, resp)
		mterr := mterrors.FromGRPC(err)
		assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(mterr))
		assert.Contains(t, err.Error(), "type is required")
	})

	t.Run("Backup with invalid type", func(t *testing.T) {
		req := &backupservicepb.BackupRequest{
			TableGroup:   "tg-1",
			Shard:        "shard-1",
			ForcePrimary: false,
			Type:         "invalid",
		}

		resp, err := svc.Backup(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, resp)
		mterr := mterrors.FromGRPC(err)
		assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(mterr))
		assert.Contains(t, err.Error(), "invalid backup type")
	})

	t.Run("Backup with valid request fails without PostgreSQL", func(t *testing.T) {
		req := &backupservicepb.BackupRequest{
			TableGroup:   "tg-1",
			Shard:        "shard-1",
			ForcePrimary: false,
			Type:         "full",
		}

		resp, err := svc.Backup(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, resp)
		mterr := mterrors.FromGRPC(err)
		assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(mterr))
		assert.Contains(t, err.Error(), "pgbackrest")
	})

	t.Run("Backup with differential type", func(t *testing.T) {
		req := &backupservicepb.BackupRequest{
			TableGroup:   "tg-1",
			Shard:        "shard-1",
			ForcePrimary: true,
			Type:         "differential",
		}

		resp, err := svc.Backup(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, resp)
		mterr := mterrors.FromGRPC(err)
		assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(mterr))
	})

	t.Run("Backup with incremental type", func(t *testing.T) {
		req := &backupservicepb.BackupRequest{
			TableGroup:   "tg-1",
			Shard:        "shard-1",
			ForcePrimary: false,
			Type:         "incremental",
		}

		resp, err := svc.Backup(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, resp)
		mterr := mterrors.FromGRPC(err)
		assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(mterr))
	})
}

func TestBackupService_RestoreFromBackup(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	t.Cleanup(cleanupPgctld)

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
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Create temporary directory for pooler
	tmpDir := t.TempDir()

	config := &manager.Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PgctldAddr: pgctldAddr,
		PoolerDir:  tmpDir,
	}
	pm := manager.NewMultiPoolerManager(logger, config)
	defer pm.Close()

	// Start the async loader
	senv := servenv.NewServEnv()
	go pm.Start(senv)

	// Wait for the manager to become ready
	require.Eventually(t, func() bool {
		return pm.GetState() == manager.ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	svc := &backupService{
		manager: pm,
	}

	t.Run("RestoreFromBackup with specific backup_id fails without PostgreSQL", func(t *testing.T) {
		req := &backupservicepb.RestoreFromBackupRequest{
			BackupId: "20250104-100000F",
		}

		resp, err := svc.RestoreFromBackup(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, resp)
		// Now fails earlier when checking recovery status (before pgbackrest is called)
		assert.Contains(t, err.Error(), "database connection not established")
	})

	t.Run("RestoreFromBackup with empty backup_id fails without PostgreSQL", func(t *testing.T) {
		req := &backupservicepb.RestoreFromBackupRequest{
			BackupId: "",
		}

		resp, err := svc.RestoreFromBackup(ctx, req)

		assert.Error(t, err)
		assert.Nil(t, resp)
		// Now fails earlier when checking recovery status (before pgbackrest is called)
		assert.Contains(t, err.Error(), "database connection not established")
	})
}

func TestBackupService_GetBackups(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	t.Cleanup(cleanupPgctld)

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
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Create temporary directory for pooler
	tmpDir := t.TempDir()

	config := &manager.Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PgctldAddr: pgctldAddr,
		PoolerDir:  tmpDir,
	}
	pm := manager.NewMultiPoolerManager(logger, config)
	defer pm.Close()

	// Start the async loader
	senv := servenv.NewServEnv()
	go pm.Start(senv)

	// Wait for the manager to become ready
	require.Eventually(t, func() bool {
		return pm.GetState() == manager.ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	svc := &backupService{
		manager: pm,
	}

	t.Run("GetBackups with no limit returns empty list when stanza doesn't exist", func(t *testing.T) {
		req := &backupservicepb.GetBackupsRequest{
			Limit: 0,
		}

		resp, err := svc.GetBackups(ctx, req)

		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Empty(t, resp.Backups)
	})

	t.Run("GetBackups with limit returns empty list when stanza doesn't exist", func(t *testing.T) {
		req := &backupservicepb.GetBackupsRequest{
			Limit: 10,
		}

		resp, err := svc.GetBackups(ctx, req)

		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Empty(t, resp.Backups)
	})
}

func TestBackupService_AllMethods(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	t.Cleanup(cleanupPgctld)

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
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	// Create temporary directory for pooler
	tmpDir := t.TempDir()

	config := &manager.Config{
		TopoClient: ts,
		ServiceID:  serviceID,
		PgctldAddr: pgctldAddr,
		PoolerDir:  tmpDir,
	}
	pm := manager.NewMultiPoolerManager(logger, config)
	defer pm.Close()

	// Start the async loader
	senv := servenv.NewServEnv()
	go pm.Start(senv)

	// Wait for the manager to become ready
	require.Eventually(t, func() bool {
		return pm.GetState() == manager.ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	svc := &backupService{
		manager: pm,
	}

	tests := []struct {
		name         string
		method       func() error
		expectedCode mtrpcpb.Code
	}{
		{
			name: "Backup with valid params",
			method: func() error {
				req := &backupservicepb.BackupRequest{
					TableGroup:   "tg-1",
					Shard:        "shard-1",
					ForcePrimary: false,
					Type:         "full",
				}
				_, err := svc.Backup(ctx, req)
				return err
			},
			expectedCode: mtrpcpb.Code_INTERNAL,
		},
		{
			name: "Backup with invalid type",
			method: func() error {
				req := &backupservicepb.BackupRequest{
					TableGroup:   "tg-1",
					Shard:        "shard-1",
					ForcePrimary: false,
					Type:         "invalid",
				}
				_, err := svc.Backup(ctx, req)
				return err
			},
			expectedCode: mtrpcpb.Code_INVALID_ARGUMENT,
		},
		{
			name: "RestoreFromBackup",
			method: func() error {
				req := &backupservicepb.RestoreFromBackupRequest{
					BackupId: "20250104-100000F",
				}
				_, err := svc.RestoreFromBackup(ctx, req)
				return err
			},
			// Now fails when checking recovery status (no DB connection), not at pgbackrest execution
			expectedCode: mtrpcpb.Code_UNKNOWN,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.method()

			assert.Error(t, err, "Method %s should return an error", tt.name)
			mterr := mterrors.FromGRPC(err)
			code := mterrors.Code(mterr)
			assert.Equal(t, tt.expectedCode, code, "Method %s should return expected error code", tt.name)
		})
	}
}
