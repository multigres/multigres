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

package store

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	commontypes "github.com/multigres/multigres/go/common/types"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestPoolerStore_FindPoolersInShard(t *testing.T) {
	protoStore := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	poolerStore := NewPoolerStore(protoStore, nil, slog.Default())

	// Add poolers to different shards
	protoStore.Set("pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
		},
	})
	protoStore.Set("pooler2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         &clustermetadatapb.ID{Cell: "cell1", Name: "pooler2"},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
		},
	})
	protoStore.Set("pooler3", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         &clustermetadatapb.ID{Cell: "cell2", Name: "pooler3"},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "1", // different shard
		},
	})

	t.Run("finds poolers in shard", func(t *testing.T) {
		shardKey := commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
		poolers := poolerStore.FindPoolersInShard(shardKey)

		assert.Len(t, poolers, 2)
		names := []string{poolers[0].MultiPooler.Id.Name, poolers[1].MultiPooler.Id.Name}
		assert.Contains(t, names, "pooler1")
		assert.Contains(t, names, "pooler2")
	})

	t.Run("returns empty for non-existent shard", func(t *testing.T) {
		shardKey := commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "999"}
		poolers := poolerStore.FindPoolersInShard(shardKey)

		assert.Empty(t, poolers)
	})

	t.Run("skips nil entries", func(t *testing.T) {
		protoStore.Set("nil-pooler", nil)
		protoStore.Set("nil-multipooler", &multiorchdatapb.PoolerHealthState{MultiPooler: nil})

		shardKey := commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
		poolers := poolerStore.FindPoolersInShard(shardKey)

		// Should still find the 2 valid poolers
		assert.Len(t, poolers, 2)
	})
}

func TestPoolerStore_FindPoolerByID(t *testing.T) {
	protoStore := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	poolerStore := NewPoolerStore(protoStore, nil, slog.Default())

	protoStore.Set("pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		},
	})
	protoStore.Set("pooler2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Cell: "cell2", Name: "pooler2"},
		},
	})

	t.Run("finds pooler by ID", func(t *testing.T) {
		id := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
		found, err := poolerStore.FindPoolerByID(id)

		require.NoError(t, err)
		assert.Equal(t, "pooler1", found.MultiPooler.Id.Name)
		assert.Equal(t, "cell1", found.MultiPooler.Id.Cell)
	})

	t.Run("returns error for non-existent pooler", func(t *testing.T) {
		id := &clustermetadatapb.ID{Cell: "cell1", Name: "non-existent"}
		found, err := poolerStore.FindPoolerByID(id)

		assert.Error(t, err)
		assert.Nil(t, found)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("matches both cell and name", func(t *testing.T) {
		// Same name, different cell - should not match
		id := &clustermetadatapb.ID{Cell: "cell2", Name: "pooler1"}
		found, err := poolerStore.FindPoolerByID(id)

		assert.Error(t, err)
		assert.Nil(t, found)
	})
}

func TestPoolerStore_FindHealthyPrimary(t *testing.T) {
	ctx := context.Background()
	protoStore := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	t.Run("finds healthy primary", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{
			StatusResponses: map[string]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
				"multipooler-cell1-primary": {
					Response: &multipoolermanagerdatapb.StatusResponse{
						Status: &multipoolermanagerdatapb.Status{
							IsInitialized: true,
						},
					},
				},
			},
		}
		poolerStore := NewPoolerStore(protoStore, fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"},
					Type: clustermetadatapb.PoolerType_REPLICA,
				},
			},
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"},
					Type: clustermetadatapb.PoolerType_PRIMARY,
				},
			},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		require.NoError(t, err)
		assert.Equal(t, "primary", primary.MultiPooler.Id.Name)
	})

	t.Run("returns error when no primary exists", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{}
		poolerStore := NewPoolerStore(protoStore, fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"},
					Type: clustermetadatapb.PoolerType_REPLICA,
				},
			},
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica2"},
					Type: clustermetadatapb.PoolerType_REPLICA,
				},
			},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		assert.Error(t, err)
		assert.Nil(t, primary)
		assert.Contains(t, err.Error(), "no healthy primary found")
	})

	t.Run("skips uninitialized primary", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{
			StatusResponses: map[string]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
				"multipooler-cell1-primary": {
					Response: &multipoolermanagerdatapb.StatusResponse{
						Status: &multipoolermanagerdatapb.Status{
							IsInitialized: false, // not initialized
						},
					},
				},
			},
		}
		poolerStore := NewPoolerStore(protoStore, fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"},
					Type: clustermetadatapb.PoolerType_PRIMARY,
				},
			},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		assert.Error(t, err)
		assert.Nil(t, primary)
	})

	t.Run("skips unreachable primary and finds next", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{
			// primary1 has no response configured (will error)
			StatusResponses: map[string]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
				"multipooler-cell2-primary2": {
					Response: &multipoolermanagerdatapb.StatusResponse{
						Status: &multipoolermanagerdatapb.Status{
							IsInitialized: true,
						},
					},
				},
			},
		}
		poolerStore := NewPoolerStore(protoStore, fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary1"},
					Type: clustermetadatapb.PoolerType_PRIMARY,
				},
			},
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell2", Name: "primary2"},
					Type: clustermetadatapb.PoolerType_PRIMARY,
				},
			},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		require.NoError(t, err)
		assert.Equal(t, "primary2", primary.MultiPooler.Id.Name)
	})

	t.Run("returns error when multiple healthy primaries found", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{
			StatusResponses: map[string]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
				"multipooler-cell1-primary1": {
					Response: &multipoolermanagerdatapb.StatusResponse{
						Status: &multipoolermanagerdatapb.Status{
							IsInitialized: true,
						},
					},
				},
				"multipooler-cell2-primary2": {
					Response: &multipoolermanagerdatapb.StatusResponse{
						Status: &multipoolermanagerdatapb.Status{
							IsInitialized: true,
						},
					},
				},
			},
		}
		poolerStore := NewPoolerStore(protoStore, fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary1"},
					Type: clustermetadatapb.PoolerType_PRIMARY,
				},
			},
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell2", Name: "primary2"},
					Type: clustermetadatapb.PoolerType_PRIMARY,
				},
			},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		assert.Error(t, err)
		assert.Nil(t, primary)
		assert.Contains(t, err.Error(), "multiple primaries found")
	})
}
