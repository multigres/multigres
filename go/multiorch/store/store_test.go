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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestStore_BasicOperations(t *testing.T) {
	store := NewStore[string, *PoolerHealth]()

	// Test Set and Get
	poolerID := "zone1/multipooler-1"
	info := &PoolerHealth{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "multipooler-1",
			},
			Database:   "postgres",
			TableGroup: "default",
			Shard:      "-",
			Type:       clustermetadata.PoolerType_PRIMARY,
		},
		LastSeen:         time.Now(),
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

func TestStore_Delete(t *testing.T) {
	store := NewStore[string, *PoolerHealth]()

	key := "test-key"
	info := &PoolerHealth{
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

func TestStore_Len(t *testing.T) {
	store := NewStore[string, *PoolerHealth]()

	require.Equal(t, 0, store.Len())

	store.Set("key1", &PoolerHealth{})
	require.Equal(t, 1, store.Len())

	store.Set("key2", &PoolerHealth{})
	require.Equal(t, 2, store.Len())

	store.Delete("key1")
	require.Equal(t, 1, store.Len())
}

func TestStore_Clear(t *testing.T) {
	store := NewStore[string, *PoolerHealth]()

	// Add items
	store.Set("key1", &PoolerHealth{})
	store.Set("key2", &PoolerHealth{})
	require.Equal(t, 2, store.Len())

	// Clear
	store.Clear()
	require.Equal(t, 0, store.Len())

	// Verify items are gone
	_, ok := store.Get("key1")
	require.False(t, ok)
}

func TestStore_GenericTypes(t *testing.T) {
	// Test with different key/value types
	intStore := NewStore[int, string]()
	intStore.Set(1, "one")
	intStore.Set(2, "two")

	val, ok := intStore.Get(1)
	require.True(t, ok)
	require.Equal(t, "one", val)

	require.Equal(t, 2, intStore.Len())
}

func TestStore_Range(t *testing.T) {
	store := NewStore[string, *PoolerHealth]()

	// Add test items
	store.Set("key1", &PoolerHealth{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "pooler1",
			},
			Database: "db1",
		},
	})
	store.Set("key2", &PoolerHealth{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "pooler2",
			},
			Database: "db2",
		},
	})
	store.Set("key3", &PoolerHealth{
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
	store.Range(func(key string, value *PoolerHealth) bool {
		visited[key] = true
		require.NotNil(t, value)
		require.NotNil(t, value.MultiPooler)
		return true // continue iteration
	})

	require.Equal(t, 3, len(visited))
	require.True(t, visited["key1"])
	require.True(t, visited["key2"])
	require.True(t, visited["key3"])
}

func TestStore_Range_EarlyStop(t *testing.T) {
	store := NewStore[string, int]()

	// Add test items
	store.Set("key1", 1)
	store.Set("key2", 2)
	store.Set("key3", 3)

	// Test early stop - should only visit one item
	count := 0
	store.Range(func(key string, value int) bool {
		count++
		return false // stop after first item
	})

	require.Equal(t, 1, count)
}

func TestStore_Range_EmptyStore(t *testing.T) {
	store := NewStore[string, int]()

	// Range on empty store should not call callback
	called := false
	store.Range(func(key string, value int) bool {
		called = true
		return true
	})

	require.False(t, called)
}

func TestStore_Range_ModifyDuringIteration(t *testing.T) {
	store := NewStore[string, int]()

	// Add test items
	store.Set("key1", 1)
	store.Set("key2", 2)

	// Collect keys to delete (cannot delete during Range due to lock)
	var toDelete []string
	store.Range(func(key string, value int) bool {
		if value == 1 {
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
