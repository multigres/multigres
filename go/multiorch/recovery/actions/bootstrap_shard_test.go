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
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// testRPCClient wraps FakeClient to capture ConfigureSynchronousReplication calls
type testRPCClient struct {
	*rpcclient.FakeClient
	syncReplicationCalled  bool
	syncReplicationRequest *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest
}

func (t *testRPCClient) ConfigureSynchronousReplication(
	ctx context.Context,
	pooler *clustermetadatapb.MultiPooler,
	request *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest,
) (*multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse, error) {
	t.syncReplicationCalled = true
	t.syncReplicationRequest = request
	// Call the underlying FakeClient method
	return t.FakeClient.ConfigureSynchronousReplication(ctx, pooler, request)
}

func TestBootstrapShardAction_ExecuteNoCohort(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	logger := slog.Default()
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	action := NewBootstrapShardAction(nil, poolerStore, ts, logger)

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
	action := NewBootstrapShardAction(nil, nil, nil, logger)

	rule, err := action.parsePolicy("ANY_2")
	assert.NoError(t, err)
	assert.NotNil(t, rule)
	assert.Equal(t, clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N, rule.QuorumType)
	assert.Equal(t, int32(2), rule.RequiredCount)
	assert.Equal(t, "Any 2 nodes must acknowledge", rule.Description)
}

func TestBootstrapShardAction_ParsePolicyMULTI_CELL_ANY_2(t *testing.T) {
	logger := slog.Default()
	action := NewBootstrapShardAction(nil, nil, nil, logger)

	rule, err := action.parsePolicy("MULTI_CELL_ANY_2")
	assert.NoError(t, err)
	assert.NotNil(t, rule)
	assert.Equal(t, clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N, rule.QuorumType)
	assert.Equal(t, int32(2), rule.RequiredCount)
	assert.Equal(t, "Any 2 nodes from different cells must acknowledge", rule.Description)
}

func TestBootstrapShardAction_ParsePolicyInvalid(t *testing.T) {
	logger := slog.Default()
	action := NewBootstrapShardAction(nil, nil, nil, logger)

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
	action := NewBootstrapShardAction(nil, poolerStore, ts, logger)
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
	action := NewBootstrapShardAction(nil, nil, nil, logger)

	metadata := action.Metadata()

	assert.Equal(t, "BootstrapShard", metadata.Name)
	assert.Equal(t, "Initialize empty shard with primary and standbys", metadata.Description)
	assert.False(t, metadata.Retryable)
}

func TestBootstrapShardAction_RequiresHealthyPrimary(t *testing.T) {
	logger := slog.Default()
	action := NewBootstrapShardAction(nil, nil, nil, logger)

	// Bootstrap doesn't require healthy primary - it's creating one
	assert.False(t, action.RequiresHealthyPrimary())
}

func TestBootstrapShardAction_Priority(t *testing.T) {
	logger := slog.Default()
	action := NewBootstrapShardAction(nil, nil, nil, logger)

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
	mockClient.StatusResponses[poolerID] = &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: false,
		},
	}
	mockClient.StatusResponses[standby1ID] = &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: false,
		},
	}
	mockClient.StatusResponses[standby2ID] = &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized: false,
		},
	}

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

	// CreateDurabilityPolicy response - set for all poolers since selection order is non-deterministic
	mockClient.CreateDurabilityPolicyResponses[poolerID] = &multipoolermanagerdatapb.CreateDurabilityPolicyResponse{
		Success: true,
	}
	mockClient.CreateDurabilityPolicyResponses[standby1ID] = &multipoolermanagerdatapb.CreateDurabilityPolicyResponse{
		Success: true,
	}
	mockClient.CreateDurabilityPolicyResponses[standby2ID] = &multipoolermanagerdatapb.CreateDurabilityPolicyResponse{
		Success: true,
	}

	// InitializeAsStandby responses
	mockClient.InitializeAsStandbyResponses[standby1ID] = &multipoolermanagerdatapb.InitializeAsStandbyResponse{
		Success: true,
	}
	mockClient.InitializeAsStandbyResponses[standby2ID] = &multipoolermanagerdatapb.InitializeAsStandbyResponse{
		Success: true,
	}

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
	action := NewBootstrapShardAction(mockClient, poolerStore, ts, logger)
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

	// Verify ConfigureSynchronousReplication was called
	assert.True(t, mockClient.syncReplicationCalled, "ConfigureSynchronousReplication should be called during bootstrap")
	assert.NotNil(t, mockClient.syncReplicationRequest, "ConfigureSynchronousReplication request should be captured")

	// Verify the request has correct num_sync for ANY_2 policy (requires 2 nodes, so 1 sync standby)
	assert.Equal(t, int32(1), mockClient.syncReplicationRequest.NumSync,
		"ANY_2 policy with RequiredCount=2 should set NumSync=1")

	// Verify the request has standby IDs
	assert.Len(t, mockClient.syncReplicationRequest.StandbyIds, 2,
		"Should configure both standbys in the sync replication list")

	// Verify the standby IDs are correct (order doesn't matter)
	standbyNames := make(map[string]bool)
	for _, standbyID := range mockClient.syncReplicationRequest.StandbyIds {
		standbyNames[standbyID.Name] = true
	}
	assert.True(t, standbyNames["pooler2"], "Should include pooler2 in standby list")
	assert.True(t, standbyNames["pooler3"], "Should include pooler3 in standby list")
}
