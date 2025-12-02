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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestBootstrapShardAction_ExecuteNoCohort(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	logger := slog.Default()
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	action := NewBootstrapShardAction(nil, poolerStore, ts, logger)

	problem := types.Problem{
		Code:       types.ProblemShardNeedsBootstrap,
		Database:   "testdb",
		TableGroup: "default",
		Shard:      "0",
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
	lockPath := "recovery/testdb/default/0"
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
		Code:       types.ProblemShardNeedsBootstrap,
		Database:   "testdb",
		TableGroup: "default",
		Shard:      "0",
	}

	err = action.Execute(ctx, problem)

	// Should fail because lock is already held
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lock")
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
