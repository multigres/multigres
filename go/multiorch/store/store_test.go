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
	store := NewStore[string, *PoolerHealthCheckStatus]()

	// Test Set and Get
	poolerID := "zone1/multipooler-1"
	info := &PoolerHealthCheckStatus{
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
	store := NewStore[string, *PoolerHealthCheckStatus]()

	key := "test-key"
	info := &PoolerHealthCheckStatus{
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

func TestStore_GetAll(t *testing.T) {
	store := NewStore[string, *PoolerHealthCheckStatus]()

	// Add multiple items
	for i := 0; i < 3; i++ {
		key := string(rune('a' + i))
		info := &PoolerHealthCheckStatus{
			MultiPooler: &clustermetadata.MultiPooler{
				Id: &clustermetadata.ID{
					Component: clustermetadata.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      key,
				},
			},
		}
		store.Set(key, info)
	}

	all := store.GetAll()
	require.Len(t, all, 3)
}

func TestStore_Len(t *testing.T) {
	store := NewStore[string, *PoolerHealthCheckStatus]()

	require.Equal(t, 0, store.Len())

	store.Set("key1", &PoolerHealthCheckStatus{})
	require.Equal(t, 1, store.Len())

	store.Set("key2", &PoolerHealthCheckStatus{})
	require.Equal(t, 2, store.Len())

	store.Delete("key1")
	require.Equal(t, 1, store.Len())
}

func TestStore_Clear(t *testing.T) {
	store := NewStore[string, *PoolerHealthCheckStatus]()

	// Add items
	store.Set("key1", &PoolerHealthCheckStatus{})
	store.Set("key2", &PoolerHealthCheckStatus{})
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

func TestStore_GetAllWithKeys(t *testing.T) {
	store := NewStore[string, *PoolerHealthCheckStatus]()

	// Test empty store
	all := store.GetAllWithKeys()
	require.Empty(t, all)

	// Add multiple items
	poolers := make(map[string]*PoolerHealthCheckStatus)
	for i := 0; i < 3; i++ {
		key := string(rune('a' + i))
		info := &PoolerHealthCheckStatus{
			MultiPooler: &clustermetadata.MultiPooler{
				Id: &clustermetadata.ID{
					Component: clustermetadata.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      key,
				},
				Database: "db" + key,
			},
		}
		poolers[key] = info
		store.Set(key, info)
	}

	// Get all with keys
	all = store.GetAllWithKeys()
	require.Len(t, all, 3)

	// Verify all keys and values are present
	for key, expectedInfo := range poolers {
		actualInfo, ok := all[key]
		require.True(t, ok, "key %s should exist", key)
		require.Equal(t, expectedInfo.MultiPooler.Id.Name, actualInfo.MultiPooler.Id.Name)
		require.Equal(t, expectedInfo.MultiPooler.Database, actualInfo.MultiPooler.Database)
	}

	// Verify modifications to returned map don't affect store
	all["new-key"] = &PoolerHealthCheckStatus{}
	require.Equal(t, 3, store.Len(), "modifications to returned map should not affect store")

	// Delete an item and verify
	store.Delete("a")
	all = store.GetAllWithKeys()
	require.Len(t, all, 2)
	_, ok := all["a"]
	require.False(t, ok, "deleted key should not be in result")
}
