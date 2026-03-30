// Copyright 2026 Supabase, Inc.
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

package topoclient_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/common/types"
)

func newTestStore(t *testing.T) topoclient.Store {
	t.Helper()
	ctx := t.Context()
	config := topoclient.NewDefaultTopoConfig()
	config.SetLockTimeout(100 * time.Millisecond)
	ts, _ := memorytopo.NewServerAndFactoryWithConfig(ctx, config, "zone1")
	t.Cleanup(func() { ts.Close() })
	return ts
}

var testShardKey = types.ShardKey{
	Database:   "testdb",
	TableGroup: constants.DefaultTableGroup,
	Shard:      "0",
}

func TestTryLockBackup(t *testing.T) {
	ctx := t.Context()

	config := topoclient.NewDefaultTopoConfig()
	config.SetLockTimeout(100 * time.Millisecond)

	ts, _ := memorytopo.NewServerAndFactoryWithConfig(ctx, config, "zone1")
	defer ts.Close()

	shardKey := types.ShardKey{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "0"}

	// Acquire backup lock
	lockCtx, unlock, err := ts.TryLockBackup(ctx, shardKey, "backup by pooler-1")
	require.NoError(t, err)

	// Verify the lock is held
	err = topoclient.AssertBackupLockHeld(lockCtx, shardKey)
	require.NoError(t, err)

	// Second acquire should fail immediately (not block)
	_, _, err = ts.TryLockBackup(ctx, shardKey, "backup by pooler-2")
	require.Error(t, err)

	// Different shard should succeed
	shardKey2 := types.ShardKey{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "1"}
	_, unlock2, err := ts.TryLockBackup(ctx, shardKey2, "backup by pooler-1")
	require.NoError(t, err)
	var unlockErr error
	unlock2(&unlockErr)
	require.NoError(t, unlockErr)

	// Release first lock
	unlock(&unlockErr)
	require.NoError(t, unlockErr)

	// Should be acquirable again after release
	_, unlock3, err := ts.TryLockBackup(ctx, shardKey, "backup by pooler-2")
	require.NoError(t, err)
	unlock3(&unlockErr)
	require.NoError(t, unlockErr)
}

func TestForceUnlockBackup(t *testing.T) {
	ctx := t.Context()

	config := topoclient.NewDefaultTopoConfig()
	config.SetLockTimeout(100 * time.Millisecond)

	ts, _ := memorytopo.NewServerAndFactoryWithConfig(ctx, config, "zone1")
	defer ts.Close()

	shardKey := types.ShardKey{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "0"}

	// Acquire backup lock
	_, _, err := ts.TryLockBackup(ctx, shardKey, "backup by pooler-1")
	require.NoError(t, err)

	// Force unlock should succeed
	err = ts.RevokeBackup(ctx, shardKey)
	require.NoError(t, err)

	// Should be acquirable again after force unlock
	_, unlock, err := ts.TryLockBackup(ctx, shardKey, "backup by pooler-2")
	require.NoError(t, err)
	var unlockErr error
	unlock(&unlockErr)
	require.NoError(t, unlockErr)

	// Force unlock when no lock exists should be a no-op
	err = ts.RevokeBackup(ctx, shardKey)
	require.NoError(t, err)
}

func TestAssertBackupLockHeld(t *testing.T) {
	ctx := t.Context()

	config := topoclient.NewDefaultTopoConfig()
	config.SetLockTimeout(100 * time.Millisecond)

	ts, _ := memorytopo.NewServerAndFactoryWithConfig(ctx, config, "zone1")
	defer ts.Close()

	shardKey := types.ShardKey{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "0"}

	// Assert should fail when no lock is held
	err := topoclient.AssertBackupLockHeld(ctx, shardKey)
	require.Error(t, err)

	// Acquire and check
	lockCtx, unlock, err := ts.TryLockBackup(ctx, shardKey, "backup")
	require.NoError(t, err)

	err = topoclient.AssertBackupLockHeld(lockCtx, shardKey)
	require.NoError(t, err)

	// Force unlock — assert should fail on memorytopo after force unlock
	err = ts.RevokeBackup(ctx, shardKey)
	require.NoError(t, err)

	err = topoclient.AssertBackupLockHeld(lockCtx, shardKey)
	require.Error(t, err)

	// Clean up the lock descriptor
	var unlockErr error
	unlock(&unlockErr)
}

func TestWithBackupLease(t *testing.T) {
	ts := newTestStore(t)
	ctx := t.Context()
	logger := slog.Default()

	called := false
	err := ts.WithBackupLease(ctx, testShardKey, "pooler-1", "backup", logger, func(ctx context.Context) error {
		called = true
		require.NoError(t, topoclient.AssertBackupLockHeld(ctx, testShardKey))
		return nil
	})
	require.NoError(t, err)
	assert.True(t, called)

	// Should be acquirable again after release
	err = ts.WithBackupLease(ctx, testShardKey, "pooler-2", "backup", logger, func(ctx context.Context) error {
		return nil
	})
	require.NoError(t, err)
}

func TestWithBackupLeaseFailsWhenLocked(t *testing.T) {
	ts := newTestStore(t)
	ctx := t.Context()
	logger := slog.Default()

	held := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- ts.WithBackupLease(ctx, testShardKey, "pooler-1", "backup", logger, func(ctx context.Context) error {
			close(held)
			<-ctx.Done()
			return nil
		})
	}()
	<-held

	// Second acquire should fail immediately
	err := ts.WithBackupLease(ctx, testShardKey, "pooler-2", "backup", logger, func(ctx context.Context) error {
		return nil
	})
	require.Error(t, err)

	// Clean up: revoke so the goroutine finishes
	require.NoError(t, ts.RevokeBackup(ctx, testShardKey))
	<-done
}

func TestWithStolenBackupLease(t *testing.T) {
	ts := newTestStore(t)
	ctx := t.Context()
	logger := slog.Default()

	// Hold a lease in background with a short check interval
	held := make(chan struct{})
	done := make(chan error, 1)
	acquire := func(ctx context.Context, action string) (context.Context, func(*error), error) {
		return ts.TryLockBackup(ctx, testShardKey, action)
	}
	revoke := func(ctx context.Context) error { return ts.RevokeBackup(ctx, testShardKey) }
	check := func(ctx context.Context) error { return topoclient.AssertBackupLockHeld(ctx, testShardKey) }
	go func() {
		done <- topoclient.WithLease(ctx, "backup by pooler-1", acquire, revoke, check, func(ctx context.Context) error {
			close(held)
			<-ctx.Done()
			return context.Cause(ctx)
		},
			topoclient.WithLeaseCheckInterval(100*time.Millisecond),
		)
	}()
	<-held

	// Steal the lease
	called := false
	err := ts.WithStolenBackupLease(ctx, testShardKey, "pooler-2", "failover-backup", logger, func(ctx context.Context) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, called)

	// Original should have received ErrLeaseLost
	err = <-done
	assert.ErrorIs(t, err, topoclient.ErrLeaseLost)
}

func TestWithStolenBackupLeaseWhenNoLockExists(t *testing.T) {
	ts := newTestStore(t)
	ctx := t.Context()
	logger := slog.Default()

	// Steal when no lock exists should succeed (acquire directly)
	called := false
	err := ts.WithStolenBackupLease(ctx, testShardKey, "pooler-1", "backup", logger, func(ctx context.Context) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, called)
}

func TestWithBackupLeaseDifferentShards(t *testing.T) {
	ts := newTestStore(t)
	ctx := t.Context()
	logger := slog.Default()

	shardKey2 := types.ShardKey{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "1"}

	err := ts.WithBackupLease(ctx, testShardKey, "pooler-1", "backup", logger, func(ctx context.Context) error {
		return nil
	})
	require.NoError(t, err)

	err = ts.WithBackupLease(ctx, shardKey2, "pooler-1", "backup", logger, func(ctx context.Context) error {
		return nil
	})
	require.NoError(t, err)
}
