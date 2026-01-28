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

package actions

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/services/multiorch/coordinator"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// testRPCClient wraps FakeClient to capture bootstrap-related RPC calls
type testRPCClient struct {
	*rpcclient.FakeClient
	initializedPrimaries []*clustermetadatapb.MultiPooler
	syncReplicationCalls []*multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest
}

// newTestCoordinator creates a mock coordinator for tests
func newTestCoordinator(ts topoclient.Store, rpcClient rpcclient.MultiPoolerClient, logger *slog.Logger) *coordinator.Coordinator {
	coordinatorID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "cell1",
		Name:      "test-coordinator",
	}
	return coordinator.NewCoordinator(coordinatorID, ts, rpcClient, logger)
}

func (t *testRPCClient) ConfigureSynchronousReplication(
	ctx context.Context,
	pooler *clustermetadatapb.MultiPooler,
	request *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest,
) (*multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse, error) {
	t.syncReplicationCalls = append(t.syncReplicationCalls, request)
	// Call the underlying FakeClient method
	return t.FakeClient.ConfigureSynchronousReplication(ctx, pooler, request)
}

func (t *testRPCClient) InitializeEmptyPrimary(
	ctx context.Context,
	pooler *clustermetadatapb.MultiPooler,
	request *multipoolermanagerdatapb.InitializeEmptyPrimaryRequest,
) (*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse, error) {
	t.initializedPrimaries = append(t.initializedPrimaries, pooler)
	// Call the underlying FakeClient method
	return t.FakeClient.InitializeEmptyPrimary(ctx, pooler, request)
}

func TestBootstrapShardAction_ExecuteNoCohort(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	logger := slog.Default()
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	coord := newTestCoordinator(ts, nil, logger)

	action := NewBootstrapShardAction(nil, nil, poolerStore, ts, coord, logger)

	problem := types.Problem{
		Code: types.ProblemShardNeedsBootstrap,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
	}

	err := action.Execute(ctx, problem)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no poolers found for shard")
}

func TestBootstrapShardAction_ParsePolicyANY_2(t *testing.T) {
	logger := slog.Default()
	coord := newTestCoordinator(nil, nil, logger)
	action := NewBootstrapShardAction(nil, nil, nil, nil, coord, logger)

	rule, err := action.parsePolicy("ANY_2")
	assert.NoError(t, err)
	assert.NotNil(t, rule)
	assert.Equal(t, clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N, rule.QuorumType)
	assert.Equal(t, int32(2), rule.RequiredCount)
	assert.Equal(t, "Any 2 nodes must acknowledge", rule.Description)
}

func TestBootstrapShardAction_ParsePolicyMULTI_CELL_ANY_2(t *testing.T) {
	logger := slog.Default()
	coord := newTestCoordinator(nil, nil, logger)
	action := NewBootstrapShardAction(nil, nil, nil, nil, coord, logger)

	rule, err := action.parsePolicy("MULTI_CELL_ANY_2")
	assert.NoError(t, err)
	assert.NotNil(t, rule)
	assert.Equal(t, clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N, rule.QuorumType)
	assert.Equal(t, int32(2), rule.RequiredCount)
	assert.Equal(t, "Any 2 nodes from different cells must acknowledge", rule.Description)
}

func TestBootstrapShardAction_ParsePolicyInvalid(t *testing.T) {
	logger := slog.Default()
	coord := newTestCoordinator(nil, nil, logger)
	action := NewBootstrapShardAction(nil, nil, nil, nil, coord, logger)

	rule, err := action.parsePolicy("INVALID_POLICY")
	assert.Error(t, err)
	assert.Nil(t, rule)
	assert.Contains(t, err.Error(), "unsupported policy name")
}

func TestBootstrapShardAction_ConcurrentExecutionPrevented(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	logger := slog.Default()
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Add a pooler to the store so we get past the "no poolers found" check
	poolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler1",
	}
	poolerStore.Set("multipooler-cell1-pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         poolerID,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
	})

	// Acquire lock manually to simulate another recovery in progress
	// The LockShard API uses path: databases/<db>/<tg>/<shard>
	lockPath := "databases/testdb/default/0"
	conn, err := ts.ConnForCell(ctx, "global")
	require.NoError(t, err)
	lock1, err := conn.LockName(ctx, lockPath, "test lock")
	require.NoError(t, err)
	defer func() {
		err := lock1.Unlock(ctx)
		require.NoError(t, err)
	}()

	// Now try to execute recovery - should fail to acquire lock
	coord := newTestCoordinator(ts, nil, logger)
	action := NewBootstrapShardAction(nil, nil, poolerStore, ts, coord, logger)
	problem := types.Problem{
		Code: types.ProblemShardNeedsBootstrap,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
	}

	// Use a short timeout so the test doesn't wait 45 seconds
	shortCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	err = action.Execute(shortCtx, problem)

	// Should fail because lock is already held (times out trying to acquire)
	assert.Error(t, err)
	// The error message is "deadline exceeded" when lock acquisition times out
	assert.Contains(t, err.Error(), "deadline exceeded")
}

func TestBootstrapShardAction_Metadata(t *testing.T) {
	logger := slog.Default()
	coord := newTestCoordinator(nil, nil, logger)
	action := NewBootstrapShardAction(nil, nil, nil, nil, coord, logger)

	metadata := action.Metadata()

	assert.Equal(t, "BootstrapShard", metadata.Name)
	assert.Equal(t, "Initialize empty shard with primary and standbys", metadata.Description)
	assert.False(t, metadata.Retryable)
}

func TestBootstrapShardAction_RequiresHealthyPrimary(t *testing.T) {
	logger := slog.Default()
	coord := newTestCoordinator(nil, nil, logger)
	action := NewBootstrapShardAction(nil, nil, nil, nil, coord, logger)

	// Bootstrap doesn't require healthy primary - it's creating one
	assert.False(t, action.RequiresHealthyPrimary())
}

func TestBootstrapShardAction_Priority(t *testing.T) {
	logger := slog.Default()
	coord := newTestCoordinator(nil, nil, logger)
	action := NewBootstrapShardAction(nil, nil, nil, nil, coord, logger)

	assert.Equal(t, types.PriorityShardBootstrap, action.Priority())
}

func TestBootstrapShardAction_ConfiguresSyncReplication(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	logger := slog.Default()
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Create mock RPC client with custom callback to track ConfigureSynchronousReplication calls
	mockClient := &testRPCClient{
		FakeClient: rpcclient.NewFakeClient(),
	}

	// Setup 3 poolers in the shard (1 primary + 2 standbys for ANY_2)
	primary := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "pooler1",
			},
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Hostname:   "host1",
			PortMap:    map[string]int32{"postgres": 5432},
		},
	}
	standby1 := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "pooler2",
			},
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Hostname:   "host2",
			PortMap:    map[string]int32{"postgres": 5432},
		},
	}
	standby2 := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "pooler3",
			},
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Hostname:   "host3",
			PortMap:    map[string]int32{"postgres": 5432},
		},
	}

	poolerStore.Set("multipooler-cell1-pooler1", primary)
	poolerStore.Set("multipooler-cell1-pooler2", standby1)
	poolerStore.Set("multipooler-cell1-pooler3", standby2)

	// Configure mock responses for all RPCs that bootstrap needs
	poolerID := "multipooler-cell1-pooler1"
	standby1ID := "multipooler-cell1-pooler2"
	standby2ID := "multipooler-cell1-pooler3"

	// Status responses: all nodes uninitialized
	mockClient.SetStatusResponse(poolerID, &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: false,
		},
	})
	mockClient.SetStatusResponse(standby1ID, &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: false,
		},
	})
	mockClient.SetStatusResponse(standby2ID, &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: false,
		},
	})

	// InitializeEmptyPrimary response - set for all poolers since selection order is non-deterministic
	mockClient.InitializeEmptyPrimaryResponses[poolerID] = &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{
		Success:  true,
		BackupId: "backup-123",
	}
	mockClient.InitializeEmptyPrimaryResponses[standby1ID] = &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{
		Success:  true,
		BackupId: "backup-123",
	}
	mockClient.InitializeEmptyPrimaryResponses[standby2ID] = &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{
		Success:  true,
		BackupId: "backup-123",
	}

	// ChangeType responses
	mockClient.ChangeTypeResponses[poolerID] = &multipoolermanagerdatapb.ChangeTypeResponse{}
	mockClient.ChangeTypeResponses[standby1ID] = &multipoolermanagerdatapb.ChangeTypeResponse{}
	mockClient.ChangeTypeResponses[standby2ID] = &multipoolermanagerdatapb.ChangeTypeResponse{}

	// ConfigureSynchronousReplication response - set for all poolers since selection order is non-deterministic
	mockClient.ConfigureSynchronousReplicationResponses[poolerID] = &multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse{}
	mockClient.ConfigureSynchronousReplicationResponses[standby1ID] = &multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse{}
	mockClient.ConfigureSynchronousReplicationResponses[standby2ID] = &multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse{}

	// Create database in topology with ANY_2 policy
	err := ts.CreateDatabase(ctx, "testdb", &clustermetadatapb.Database{
		Name:             "testdb",
		DurabilityPolicy: "ANY_2",
	})
	require.NoError(t, err)

	// Execute bootstrap action
	coord := newTestCoordinator(ts, mockClient, logger)
	action := NewBootstrapShardAction(nil, mockClient, poolerStore, ts, coord, logger)
	problem := types.Problem{
		Code: types.ProblemShardNeedsBootstrap,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
	}

	err = action.Execute(ctx, problem)
	require.NoError(t, err)

	// Verify exactly one pooler was initialized as primary
	require.Len(t, mockClient.initializedPrimaries, 1,
		"Exactly one pooler should be initialized as primary")
	primaryPooler := mockClient.initializedPrimaries[0]
	primaryName := primaryPooler.Id.Name

	// Verify ConfigureSynchronousReplication was called exactly once (on the primary)
	require.Len(t, mockClient.syncReplicationCalls, 1,
		"ConfigureSynchronousReplication should be called exactly once on the primary")
	syncReq := mockClient.syncReplicationCalls[0]

	// Verify the request has correct num_sync for ANY_2 policy (requires 2 nodes, so 1 sync standby)
	assert.Equal(t, int32(1), syncReq.NumSync,
		"ANY_2 policy with RequiredCount=2 should set NumSync=1")

	// Verify the request has exactly 2 standby IDs
	require.Len(t, syncReq.StandbyIds, 2,
		"Should configure both standbys in the sync replication list")

	// Collect actual standbys from the sync replication request
	actualStandbys := make(map[string]bool)
	for _, standbyID := range syncReq.StandbyIds {
		actualStandbys[standbyID.Name] = true
	}

	// Verify standbys are unique (no duplicates)
	require.Len(t, actualStandbys, 2,
		"Should have 2 unique standbys (no duplicates)")

	// Verify all standbys are from our pooler set
	allPoolers := map[string]bool{"pooler1": true, "pooler2": true, "pooler3": true}
	for standby := range actualStandbys {
		assert.True(t, allPoolers[standby],
			"Standby %s should be from the pooler set", standby)
	}

	// Verify primary is NOT in the standby list
	assert.False(t, actualStandbys[primaryName],
		"Primary %s should not be in standby list", primaryName)
}

// setupTestDatabase creates the database in topology with the given durability policy
func setupTestDatabase(ctx context.Context, t *testing.T, ts topoclient.Store, dbName, durabilityPolicy string) {
	t.Helper()
	err := ts.CreateDatabase(ctx, dbName, &clustermetadatapb.Database{
		Name:             dbName,
		DurabilityPolicy: durabilityPolicy,
	})
	require.NoError(t, err)
}

// TestBootstrapShardAction_QuorumCheckFailsWithInsufficientPoolers tests that bootstrap
// fails when there are not enough reachable poolers to satisfy the quorum requirement.
func TestBootstrapShardAction_QuorumCheckFailsWithInsufficientPoolers(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	logger := slog.Default()
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Setup database with ANY_2 policy (requires 2 nodes)
	setupTestDatabase(ctx, t, ts, "testdb", "ANY_2")

	// Create fake RPC client - only pooler1 is reachable
	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetStatusResponse("multipooler-cell1-pooler1", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: false,
		},
	})
	// pooler2 returns an error (unreachable)
	fakeClient.Errors["multipooler-cell1-pooler2"] = errors.New("connection refused")

	// Add two poolers to the store
	poolerID1 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler1",
	}
	poolerID2 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler2",
	}

	poolerStore.Set("multipooler-cell1-pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         poolerID1,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Hostname:   "host1",
			PortMap:    map[string]int32{"postgres": 5432},
		},
	})
	poolerStore.Set("multipooler-cell1-pooler2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         poolerID2,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Hostname:   "host2",
			PortMap:    map[string]int32{"postgres": 5432},
		},
	})

	coord := newTestCoordinator(ts, fakeClient, logger)
	action := NewBootstrapShardAction(nil, fakeClient, poolerStore, ts, coord, logger)

	problem := types.Problem{
		Code: types.ProblemShardNeedsBootstrap,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
	}

	err := action.Execute(ctx, problem)

	// Should fail because only 1 pooler is reachable but ANY_2 requires 2
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient reachable poolers")
	assert.Contains(t, err.Error(), "have 1, need 2")
}

// TestBootstrapShardAction_QuorumCheckPassesWithEnoughPoolers tests that bootstrap
// proceeds when there are enough reachable poolers to satisfy the quorum requirement.
func TestBootstrapShardAction_QuorumCheckPassesWithEnoughPoolers(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	logger := slog.Default()
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Setup database with ANY_2 policy (requires 2 nodes)
	setupTestDatabase(ctx, t, ts, "testdb", "ANY_2")

	// Create fake RPC client - both poolers are reachable
	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetStatusResponse("multipooler-cell1-pooler1", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: false,
		},
	})
	fakeClient.SetStatusResponse("multipooler-cell1-pooler2", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: false,
		},
	})

	// Setup responses for the bootstrap flow (either pooler could be selected as primary)
	fakeClient.InitializeEmptyPrimaryResponses["multipooler-cell1-pooler1"] = &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{
		Success:  true,
		BackupId: "backup-123",
	}
	fakeClient.InitializeEmptyPrimaryResponses["multipooler-cell1-pooler2"] = &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{
		Success:  true,
		BackupId: "backup-123",
	}
	fakeClient.ChangeTypeResponses["multipooler-cell1-pooler1"] = &multipoolermanagerdatapb.ChangeTypeResponse{}
	fakeClient.ChangeTypeResponses["multipooler-cell1-pooler2"] = &multipoolermanagerdatapb.ChangeTypeResponse{}
	// Add ConfigureSynchronousReplication responses for both (since selection order is non-deterministic)
	fakeClient.ConfigureSynchronousReplicationResponses["multipooler-cell1-pooler1"] = &multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse{}
	fakeClient.ConfigureSynchronousReplicationResponses["multipooler-cell1-pooler2"] = &multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse{}

	// Add two poolers to the store
	poolerID1 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler1",
	}
	poolerID2 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler2",
	}

	poolerStore.Set("multipooler-cell1-pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         poolerID1,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Hostname:   "host1",
			PortMap:    map[string]int32{"postgres": 5432},
		},
	})
	poolerStore.Set("multipooler-cell1-pooler2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         poolerID2,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Hostname:   "host2",
			PortMap:    map[string]int32{"postgres": 5432},
		},
	})

	coord := newTestCoordinator(ts, fakeClient, logger)
	action := NewBootstrapShardAction(nil, fakeClient, poolerStore, ts, coord, logger)

	problem := types.Problem{
		Code: types.ProblemShardNeedsBootstrap,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
	}

	err := action.Execute(ctx, problem)

	// Should succeed - we have 2 reachable poolers and ANY_2 requires 2
	assert.NoError(t, err)

	// Verify that exactly one primary was initialized
	// Standbys rely on auto-restore from the backup created by the primary
	callCounts := countCallsByMethod(fakeClient.CallLog)
	assert.Equal(t, 1, callCounts["InitializeEmptyPrimary"], "exactly one primary should be initialized")
}

// TestBootstrapShardAction_FullBootstrapFlow tests the complete bootstrap flow
// with 3 poolers where one becomes primary and two become standbys.
func TestBootstrapShardAction_FullBootstrapFlow(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	logger := slog.Default()
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Setup database with ANY_2 policy
	setupTestDatabase(ctx, t, ts, "testdb", "ANY_2")

	// Create fake RPC client - all 3 poolers are reachable and uninitialized
	fakeClient := rpcclient.NewFakeClient()
	for _, name := range []string{"pooler1", "pooler2", "pooler3"} {
		key := "multipooler-cell1-" + name
		fakeClient.SetStatusResponse(key, &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{
				IsInitialized: false,
			},
		})
		fakeClient.ChangeTypeResponses[key] = &multipoolermanagerdatapb.ChangeTypeResponse{}
		// Any pooler could be selected as primary (store iteration order is non-deterministic)
		fakeClient.InitializeEmptyPrimaryResponses[key] = &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{
			Success:  true,
			BackupId: "backup-abc123",
		}
		fakeClient.ConfigureSynchronousReplicationResponses[key] = &multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse{}
	}

	// Add 3 poolers to the store
	for i, name := range []string{"pooler1", "pooler2", "pooler3"} {
		poolerID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      name,
		}
		poolerStore.Set("multipooler-cell1-"+name, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:         poolerID,
				Database:   "testdb",
				TableGroup: "default",
				Shard:      "0",
				Hostname:   "host" + string(rune('1'+i)),
				PortMap:    map[string]int32{"postgres": 5432},
			},
		})
	}

	coord := newTestCoordinator(ts, fakeClient, logger)
	action := NewBootstrapShardAction(nil, fakeClient, poolerStore, ts, coord, logger)

	problem := types.Problem{
		Code: types.ProblemShardNeedsBootstrap,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
	}

	err := action.Execute(ctx, problem)

	assert.NoError(t, err)

	// Count RPC calls by method name
	// We expect: 1 InitializeEmptyPrimary, 0
	// Standbys rely on auto-restore from the backup created by the primary
	callCounts := countCallsByMethod(fakeClient.CallLog)

	assert.Equal(t, 1, callCounts["InitializeEmptyPrimary"], "exactly one primary should be initialized")
}

// countCallsByMethod counts RPC calls by method name from the FakeClient call log.
// Call log entries are in format "MethodName(poolerID)".
func countCallsByMethod(callLog []string) map[string]int {
	counts := make(map[string]int)
	for _, call := range callLog {
		// Extract method name (everything before the first '(')
		if idx := strings.Index(call, "("); idx > 0 {
			method := call[:idx]
			counts[method]++
		}
	}
	return counts
}

// TestBootstrapShardAction_SkipsIfAlreadyInitialized tests that bootstrap is skipped
// if any node is already initialized (detected during revalidation).
func TestBootstrapShardAction_SkipsIfAlreadyInitialized(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	logger := slog.Default()
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Setup database with ANY_2 policy
	setupTestDatabase(ctx, t, ts, "testdb", "ANY_2")

	// Create fake RPC client - pooler1 is already initialized
	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetStatusResponse("multipooler-cell1-pooler1", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: true, // Already initialized!
			PostgresRole:  "primary",
		},
	})
	fakeClient.SetStatusResponse("multipooler-cell1-pooler2", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: false,
		},
	})

	// Add two poolers to the store
	poolerID1 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler1",
	}
	poolerID2 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler2",
	}

	poolerStore.Set("multipooler-cell1-pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         poolerID1,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Hostname:   "host1",
			PortMap:    map[string]int32{"postgres": 5432},
		},
	})
	poolerStore.Set("multipooler-cell1-pooler2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         poolerID2,
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
			Hostname:   "host2",
			PortMap:    map[string]int32{"postgres": 5432},
		},
	})

	coord := newTestCoordinator(ts, fakeClient, logger)
	action := NewBootstrapShardAction(nil, fakeClient, poolerStore, ts, coord, logger)

	problem := types.Problem{
		Code: types.ProblemShardNeedsBootstrap,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
	}

	err := action.Execute(ctx, problem)

	// Should succeed without error (no-op since already initialized)
	assert.NoError(t, err)

	// Verify InitializeEmptyPrimary was NOT called
	for _, call := range fakeClient.CallLog {
		assert.NotContains(t, call, "InitializeEmptyPrimary")
	}
}

// TestBootstrapShardAction_CountReachablePoolers tests the countReachablePoolers helper.
func TestBootstrapShardAction_CountReachablePoolers(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	fakeClient := rpcclient.NewFakeClient()

	// pooler1 and pooler3 are reachable, pooler2 is not
	fakeClient.SetStatusResponse("multipooler-cell1-pooler1", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{IsInitialized: false},
	})
	fakeClient.Errors["multipooler-cell1-pooler2"] = errors.New("connection refused")
	fakeClient.SetStatusResponse("multipooler-cell1-pooler3", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{IsInitialized: false},
	})

	coord := newTestCoordinator(nil, fakeClient, logger)
	action := NewBootstrapShardAction(nil, fakeClient, nil, nil, coord, logger)

	cohort := []*multiorchdatapb.PoolerHealthState{
		{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "pooler1",
				},
			},
		},
		{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "pooler2",
				},
			},
		},
		{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "pooler3",
				},
			},
		},
	}

	count := action.countReachablePoolers(ctx, cohort)

	assert.Equal(t, 2, count)
}

// TestBootstrapShardAction_CountReachablePoolersTimeout tests that slow poolers
// are treated as unreachable due to the per-RPC timeout.
func TestBootstrapShardAction_CountReachablePoolersTimeout(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	fakeClient := rpcclient.NewFakeClient()

	// pooler1 responds immediately, pooler2 is slow (100ms delay > 10ms timeout)
	fakeClient.SetStatusResponse("multipooler-cell1-pooler1", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{IsInitialized: false},
	})
	fakeClient.SetStatusResponseWithDelay("multipooler-cell1-pooler2", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{IsInitialized: false},
	}, 100*time.Millisecond)

	coord := newTestCoordinator(nil, fakeClient, logger)
	action := NewBootstrapShardAction(nil, fakeClient, nil, nil, coord, logger).
		WithStatusRPCTimeout(10 * time.Millisecond)

	cohort := []*multiorchdatapb.PoolerHealthState{
		{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "pooler1",
				},
			},
		},
		{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "pooler2",
				},
			},
		},
	}

	start := time.Now()
	count := action.countReachablePoolers(ctx, cohort)
	elapsed := time.Since(start)

	// Only pooler1 should be counted as reachable (pooler2 timed out)
	assert.Equal(t, 1, count)

	// Should complete in ~10ms (the timeout), not 100ms (the full delay)
	assert.Less(t, elapsed, 50*time.Millisecond, "should timeout quickly, not wait for full delay")
}
