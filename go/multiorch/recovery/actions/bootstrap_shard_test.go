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
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

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

	action := NewBootstrapShardAction(fakeClient, poolerStore, ts, logger)

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

	// Setup responses for the bootstrap flow
	fakeClient.InitializeEmptyPrimaryResponses["multipooler-cell1-pooler1"] = &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{
		Success:  true,
		BackupId: "backup-123",
	}
	fakeClient.ChangeTypeResponses["multipooler-cell1-pooler1"] = &multipoolermanagerdatapb.ChangeTypeResponse{}
	fakeClient.CreateDurabilityPolicyResponses["multipooler-cell1-pooler1"] = &multipoolermanagerdatapb.CreateDurabilityPolicyResponse{
		Success: true,
	}
	fakeClient.InitializeAsStandbyResponses["multipooler-cell1-pooler2"] = &multipoolermanagerdatapb.InitializeAsStandbyResponse{
		Success: true,
	}
	fakeClient.ChangeTypeResponses["multipooler-cell1-pooler2"] = &multipoolermanagerdatapb.ChangeTypeResponse{}

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

	action := NewBootstrapShardAction(fakeClient, poolerStore, ts, logger)

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

	// Verify the expected RPC calls were made
	assert.Contains(t, fakeClient.CallLog, "InitializeEmptyPrimary(multipooler-cell1-pooler1)")
	assert.Contains(t, fakeClient.CallLog, "CreateDurabilityPolicy(multipooler-cell1-pooler1)")
	assert.Contains(t, fakeClient.CallLog, "InitializeAsStandby(multipooler-cell1-pooler2)")
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
		fakeClient.InitializeAsStandbyResponses[key] = &multipoolermanagerdatapb.InitializeAsStandbyResponse{
			Success: true,
		}
		// Any pooler could be selected as primary (store iteration order is non-deterministic)
		fakeClient.InitializeEmptyPrimaryResponses[key] = &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{
			Success:  true,
			BackupId: "backup-abc123",
		}
		fakeClient.CreateDurabilityPolicyResponses[key] = &multipoolermanagerdatapb.CreateDurabilityPolicyResponse{
			Success: true,
		}
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

	action := NewBootstrapShardAction(fakeClient, poolerStore, ts, logger)

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
	// We expect: 1 InitializeEmptyPrimary, 1 CreateDurabilityPolicy, 2 InitializeAsStandby
	callCounts := countCallsByMethod(fakeClient.CallLog)

	assert.Equal(t, 1, callCounts["InitializeEmptyPrimary"], "exactly one primary should be initialized")
	assert.Equal(t, 1, callCounts["CreateDurabilityPolicy"], "durability policy should be created once")
	assert.Equal(t, 2, callCounts["InitializeAsStandby"], "two standbys should be initialized")

	// Verify the database is marked as initialized after successful bootstrap
	db, err := ts.GetDatabase(ctx, "testdb")
	require.NoError(t, err)
	assert.True(t, db.Initialized, "database should be marked as initialized after bootstrap")
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

	action := NewBootstrapShardAction(fakeClient, poolerStore, ts, logger)

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

// TestBootstrapShardAction_FailsIfDatabaseAlreadyInitialized tests that bootstrap fails
// with an error if the database is already marked as initialized in topology,
// indicating a partial bootstrap failure that requires manual intervention.
func TestBootstrapShardAction_FailsIfDatabaseAlreadyInitialized(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	logger := slog.Default()
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Setup database with ANY_2 policy AND marked as initialized
	err := ts.CreateDatabase(ctx, "testdb", &clustermetadatapb.Database{
		Name:             "testdb",
		DurabilityPolicy: "ANY_2",
		Initialized:      true, // Already initialized!
	})
	require.NoError(t, err)

	// Create fake RPC client - both poolers are reachable but uninitialized
	// (simulating a partial failure where the flag was set but nodes weren't fully set up)
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

	action := NewBootstrapShardAction(fakeClient, poolerStore, ts, logger)

	problem := types.Problem{
		Code: types.ProblemShardNeedsBootstrap,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
	}

	err = action.Execute(ctx, problem)

	// Should fail because database is marked as initialized but shard still needs bootstrap
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "marked as initialized")
	assert.Contains(t, err.Error(), "manual intervention")

	// Verify no bootstrap RPCs were made
	for _, call := range fakeClient.CallLog {
		assert.NotContains(t, call, "InitializeEmptyPrimary")
		assert.NotContains(t, call, "InitializeAsStandby")
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

	action := NewBootstrapShardAction(fakeClient, nil, nil, logger)

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

	action := NewBootstrapShardAction(fakeClient, nil, nil, logger).
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
