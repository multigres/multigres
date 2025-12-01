// Copyright 2019 The Vitess Authors.
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
//
// Modifications Copyright 2025 Supabase, Inc.

package topo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeLockDescriptor implements the topo.LockDescriptor interface for testing.
type fakeLockDescriptor struct{}

// Check implements the topo.LockDescriptor interface.
func (f fakeLockDescriptor) Check(ctx context.Context) error {
	return nil
}

// Unlock implements the topo.LockDescriptor interface.
func (f fakeLockDescriptor) Unlock(ctx context.Context) error {
	return nil
}

var _ LockDescriptor = (*fakeLockDescriptor)(nil)

// lockedShardContext returns a context that appears to have a lock on the given shard.
// This is useful for testing code that requires a shard lock to be held.
func lockedShardContext(database, tableGroup, shard string) context.Context {
	ctx := context.Background()
	resourceName := (&shardLock{database: database, tableGroup: tableGroup, shard: shard}).ResourceName()
	return context.WithValue(ctx, locksKey, &locksInfo{
		info: map[string]*lockInfo{
			resourceName: {
				lockDescriptor: fakeLockDescriptor{},
			},
		},
	})
}

func TestCheckShardLocked(t *testing.T) {
	database := "testdb"
	tableGroup := "default"
	shard := "0"

	t.Run("returns error when no lock info in context", func(t *testing.T) {
		ctx := context.Background()
		err := CheckShardLocked(ctx, database, tableGroup, shard)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is not locked (no locksInfo)")
	})

	t.Run("returns error when shard is not locked", func(t *testing.T) {
		// Create a context with lock info, but for a different shard
		ctx := lockedShardContext(database, tableGroup, "other-shard")
		err := CheckShardLocked(ctx, database, tableGroup, shard)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is not locked (no lockInfo in map)")
	})

	t.Run("returns nil when shard is locked", func(t *testing.T) {
		ctx := lockedShardContext(database, tableGroup, shard)
		err := CheckShardLocked(ctx, database, tableGroup, shard)
		require.NoError(t, err)
	})
}

func TestShardLockInterface(t *testing.T) {
	lock := &shardLock{
		database:   "mydb",
		tableGroup: "default",
		shard:      "0",
	}

	t.Run("Type returns shard", func(t *testing.T) {
		require.Equal(t, "shard", lock.Type())
	})

	t.Run("ResourceName returns database/tableGroup/shard", func(t *testing.T) {
		require.Equal(t, "mydb/default/0", lock.ResourceName())
	})

	t.Run("Path returns full topo path", func(t *testing.T) {
		require.Equal(t, "databases/mydb/default/0", lock.Path())
	})
}

func TestShardLockResourceNameUniqueness(t *testing.T) {
	// Test that different shards have different resource names
	lock1 := &shardLock{database: "db1", tableGroup: "tg1", shard: "0"}
	lock2 := &shardLock{database: "db1", tableGroup: "tg1", shard: "1"}
	lock3 := &shardLock{database: "db1", tableGroup: "tg2", shard: "0"}
	lock4 := &shardLock{database: "db2", tableGroup: "tg1", shard: "0"}

	require.NotEqual(t, lock1.ResourceName(), lock2.ResourceName())
	require.NotEqual(t, lock1.ResourceName(), lock3.ResourceName())
	require.NotEqual(t, lock1.ResourceName(), lock4.ResourceName())
	require.NotEqual(t, lock2.ResourceName(), lock3.ResourceName())
	require.NotEqual(t, lock2.ResourceName(), lock4.ResourceName())
	require.NotEqual(t, lock3.ResourceName(), lock4.ResourceName())
}

func TestLockedShardContextMultipleShards(t *testing.T) {
	// Test that we can have multiple shards locked in the same context
	database := "testdb"
	tableGroup := "default"
	shard1 := "0"
	shard2 := "1"

	// Create context with first shard locked
	ctx := lockedShardContext(database, tableGroup, shard1)

	// Add second shard to the same context
	resourceName2 := (&shardLock{database: database, tableGroup: tableGroup, shard: shard2}).ResourceName()
	locksInfoVal := ctx.Value(locksKey).(*locksInfo)
	locksInfoVal.info[resourceName2] = &lockInfo{
		lockDescriptor: fakeLockDescriptor{},
	}

	// Both shards should be locked
	require.NoError(t, CheckShardLocked(ctx, database, tableGroup, shard1))
	require.NoError(t, CheckShardLocked(ctx, database, tableGroup, shard2))

	// A different shard should not be locked
	err := CheckShardLocked(ctx, database, tableGroup, "other")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not locked (no lockInfo in map)")
}
