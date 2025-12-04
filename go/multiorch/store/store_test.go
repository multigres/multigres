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

package store

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestProtoStore_BasicOperations(t *testing.T) {
	store := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Test Set and Get
	poolerID := "zone1/multipooler-1"
	info := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "multipooler-1",
			},
			Database:   "postgres",
			TableGroup: types.DefaultTableGroup,
			Shard:      "-",
			Type:       clustermetadata.PoolerType_PRIMARY,
		},
		LastSeen:         timestamppb.Now(),
		IsUpToDate:       true,
		IsLastCheckValid: true,
	}

	store.Set(poolerID, info)

	// Get should return the value
	retrieved, ok := store.Get(poolerID)
	require.True(t, ok)
	require.Equal(t, info.MultiPooler.Id.Name, retrieved.MultiPooler.Id.Name)
	require.Equal(t, info.MultiPooler.Database, retrieved.MultiPooler.Database)

	// Get non-existent key
	_, ok = store.Get("nonexistent")
	require.False(t, ok)
}

func TestProtoStore_Delete(t *testing.T) {
	store := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	key := "test-key"
	info := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "test",
			},
		},
	}

	// Set and verify
	store.Set(key, info)
	_, ok := store.Get(key)
	require.True(t, ok)

	// Delete existing key
	deleted := store.Delete(key)
	require.True(t, deleted)

	// Verify deleted
	_, ok = store.Get(key)
	require.False(t, ok)

	// Delete non-existent key
	deleted = store.Delete("nonexistent")
	require.False(t, deleted)
}

func TestProtoStore_Len(t *testing.T) {
	store := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	require.Equal(t, 0, store.Len())

	store.Set("key1", &multiorchdatapb.PoolerHealthState{})
	require.Equal(t, 1, store.Len())

	store.Set("key2", &multiorchdatapb.PoolerHealthState{})
	require.Equal(t, 2, store.Len())

	store.Delete("key1")
	require.Equal(t, 1, store.Len())
}

func TestProtoStore_Clear(t *testing.T) {
	store := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Add items
	store.Set("key1", &multiorchdatapb.PoolerHealthState{})
	store.Set("key2", &multiorchdatapb.PoolerHealthState{})
	require.Equal(t, 2, store.Len())

	// Clear
	store.Clear()
	require.Equal(t, 0, store.Len())

	// Verify items are gone
	_, ok := store.Get("key1")
	require.False(t, ok)
}

func TestProtoStore_Range(t *testing.T) {
	store := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Add test items
	store.Set("key1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "pooler1",
			},
			Database: "db1",
		},
	})
	store.Set("key2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "pooler2",
			},
			Database: "db2",
		},
	})
	store.Set("key3", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "pooler3",
			},
			Database: "db3",
		},
	})

	// Test iterating over all items
	visited := make(map[string]bool)
	store.Range(func(key string, value *multiorchdatapb.PoolerHealthState) bool {
		visited[key] = true
		require.NotNil(t, value)
		require.NotNil(t, value.MultiPooler)
		require.NotNil(t, value.MultiPooler.Id)
		return true // continue iteration
	})

	require.Equal(t, 3, len(visited))
	require.True(t, visited["key1"])
	require.True(t, visited["key2"])
	require.True(t, visited["key3"])
}

func TestProtoStore_Range_EarlyStop(t *testing.T) {
	store := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Add test items
	store.Set("key1", &multiorchdatapb.PoolerHealthState{})
	store.Set("key2", &multiorchdatapb.PoolerHealthState{})
	store.Set("key3", &multiorchdatapb.PoolerHealthState{})

	// Test early stop - should only visit one item
	count := 0
	store.Range(func(key string, value *multiorchdatapb.PoolerHealthState) bool {
		count++
		return false // stop after first item
	})

	require.Equal(t, 1, count)
}

func TestProtoStore_Range_EmptyStore(t *testing.T) {
	store := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Range on empty store should not call callback
	called := false
	store.Range(func(key string, value *multiorchdatapb.PoolerHealthState) bool {
		called = true
		return true
	})

	require.False(t, called)
}

func TestProtoStore_Range_ModifyDuringIteration(t *testing.T) {
	store := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Add test items
	store.Set("key1", &multiorchdatapb.PoolerHealthState{IsUpToDate: true})
	store.Set("key2", &multiorchdatapb.PoolerHealthState{IsUpToDate: false})

	// Collect keys to delete (cannot delete during Range due to lock)
	var toDelete []string
	store.Range(func(key string, value *multiorchdatapb.PoolerHealthState) bool {
		if value.IsUpToDate {
			toDelete = append(toDelete, key)
		}
		return true
	})

	// Delete after iteration
	for _, key := range toDelete {
		store.Delete(key)
	}

	// Verify deletion
	require.Equal(t, 1, store.Len())
	_, ok := store.Get("key1")
	require.False(t, ok)
	_, ok = store.Get("key2")
	require.True(t, ok)
}

func TestProtoStore_CloningBehavior(t *testing.T) {
	store := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Create and store original value
	original := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "pooler1",
			},
			Database: "original_db",
		},
		IsUpToDate: true,
	}
	store.Set("key1", original)

	// Modify original after storing - should not affect stored value
	original.MultiPooler.Database = "modified_db"
	original.IsUpToDate = false

	// Get should return clone of original stored value, not the modified one
	retrieved, ok := store.Get("key1")
	require.True(t, ok)
	require.Equal(t, "original_db", retrieved.MultiPooler.Database)
	require.True(t, retrieved.IsUpToDate)

	// Modify retrieved value - should not affect stored value
	retrieved.MultiPooler.Database = "retrieved_modified"
	retrieved.IsUpToDate = false

	// Get again - should still return original stored value
	retrieved2, ok := store.Get("key1")
	require.True(t, ok)
	require.Equal(t, "original_db", retrieved2.MultiPooler.Database)
	require.True(t, retrieved2.IsUpToDate)
}

func TestProtoStore_RangeCloningBehavior(t *testing.T) {
	store := NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Store a value
	store.Set("key1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "pooler1",
			},
			Database: "original_db",
		},
		IsUpToDate: true,
	})

	// Modify value during Range iteration
	store.Range(func(key string, value *multiorchdatapb.PoolerHealthState) bool {
		value.MultiPooler.Database = "modified_in_range"
		value.IsUpToDate = false
		return true
	})

	// Get should still return original value (Range returns clones)
	retrieved, ok := store.Get("key1")
	require.True(t, ok)
	require.Equal(t, "original_db", retrieved.MultiPooler.Database)
	require.True(t, retrieved.IsUpToDate)
}
