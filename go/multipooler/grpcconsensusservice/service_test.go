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

	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/multipooler/manager"
	"github.com/multigres/multigres/go/servenv"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdata "github.com/multigres/multigres/go/pb/consensusdata"
)

func TestConsensusService_RequestVote(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	defer cleanupPgctld()

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

	svc := &consensusService{
		manager: pm,
	}

	t.Run("RequestVote without database connection should fail", func(t *testing.T) {
		req := &consensusdata.RequestVoteRequest{
			Term:         5,
			CandidateId:  "candidate-1",
			ShardId:      "shard-1",
			LastLogIndex: 100,
			LastLogTerm:  4,
		}

		resp, err := svc.RequestVote(ctx, req)

		// Should fail because no database connection
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "postgres")
	})
}

func TestConsensusService_Status(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	defer cleanupPgctld()

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
		assert.Equal(t, "test-service", resp.NodeId)
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
	defer cleanupPgctld()

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

	svc := &consensusService{
		manager: pm,
	}

	t.Run("GetLeadershipView without replication tracker should fail", func(t *testing.T) {
		req := &consensusdata.LeadershipViewRequest{
			ShardId: "shard-1",
		}

		resp, err := svc.GetLeadershipView(ctx, req)

		// Should fail because no replication tracker
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "replication tracker not initialized")
	})
}

func TestConsensusService_GetWALPosition(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	defer cleanupPgctld()

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

	svc := &consensusService{
		manager: pm,
	}

	t.Run("GetWALPosition without database should fail", func(t *testing.T) {
		req := &consensusdata.GetWALPositionRequest{}

		resp, err := svc.GetWALPosition(ctx, req)

		// Should fail because no database connection
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestConsensusService_CanReachPrimary(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Start mock pgctld server
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t)
	defer cleanupPgctld()

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
	defer cleanupPgctld()

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

	svc := &consensusService{
		manager: pm,
	}

	tests := []struct {
		name          string
		method        func() error
		shouldSucceed bool
	}{
		{
			name: "RequestVote",
			method: func() error {
				req := &consensusdata.RequestVoteRequest{
					Term:         5,
					CandidateId:  "candidate-1",
					ShardId:      "shard-1",
					LastLogIndex: 100,
					LastLogTerm:  4,
				}
				_, err := svc.RequestVote(ctx, req)
				return err
			},
			shouldSucceed: false, // No database connection
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
			name: "GetWALPosition",
			method: func() error {
				req := &consensusdata.GetWALPositionRequest{}
				_, err := svc.GetWALPosition(ctx, req)
				return err
			},
			shouldSucceed: false, // No database connection
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
