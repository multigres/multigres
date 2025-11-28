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

package topo_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
)

func TestLockShardForRecovery(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "global")
	defer ts.Close()

	// Acquire lock
	lockDesc, err := ts.LockShardForRecovery(ctx, "db1", "tg1", "shard1", "test operation", 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, lockDesc)

	// Verify lock blocks second acquisition (with short timeout)
	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = ts.LockShardForRecovery(shortCtx, "db1", "tg1", "shard1", "second operation", 100*time.Millisecond)
	assert.Error(t, err, "second lock acquisition should fail")

	// Release first lock
	err = lockDesc.Unlock(ctx)
	require.NoError(t, err)

	// Now should be able to acquire again
	lockDesc2, err := ts.LockShardForRecovery(ctx, "db1", "tg1", "shard1", "third operation", 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, lockDesc2)
	defer func() {
		require.NoError(t, lockDesc2.Unlock(ctx))
	}()
}

func TestLockShardForRecovery_DifferentShards(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "global")
	defer ts.Close()

	// Acquire lock on shard1
	lock1, err := ts.LockShardForRecovery(ctx, "db1", "tg1", "shard1", "op1", 5*time.Second)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lock1.Unlock(ctx))
	}()

	// Should be able to acquire lock on shard2 (different shard)
	lock2, err := ts.LockShardForRecovery(ctx, "db1", "tg1", "shard2", "op2", 5*time.Second)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lock2.Unlock(ctx))
	}()
}

func TestLockShardForRecovery_Timeout(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "global")
	defer ts.Close()

	// Acquire lock
	lock1, err := ts.LockShardForRecovery(ctx, "db1", "tg1", "shard1", "op1", 5*time.Second)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lock1.Unlock(ctx))
	}()

	// Try to acquire with very short timeout - should fail with timeout error
	start := time.Now()
	_, err = ts.LockShardForRecovery(ctx, "db1", "tg1", "shard1", "op2", 50*time.Millisecond)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Less(t, elapsed, 200*time.Millisecond, "should timeout quickly")
}
