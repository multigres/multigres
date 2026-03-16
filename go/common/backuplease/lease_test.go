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

package backuplease_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backuplease"
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

func TestAcquireAndRelease(t *testing.T) {
	ts := newTestStore(t)
	ctx := t.Context()
	logger := slog.Default()

	// Acquire lease
	ctx, lease, err := backuplease.Acquire(ctx, ts, testShardKey, "pooler-1", "backup", logger)
	require.NoError(t, err)
	require.NotNil(t, lease)

	// Verify lock is held in context
	err = topoclient.AssertBackupLockHeld(ctx, testShardKey)
	require.NoError(t, err)

	// Release
	lease.Release(ctx)

	// Should be acquirable again
	_, lease2, err := backuplease.Acquire(ctx, ts, testShardKey, "pooler-2", "backup", logger)
	require.NoError(t, err)
	lease2.Release(ctx)
}

func TestAcquireFailsWhenLocked(t *testing.T) {
	ts := newTestStore(t)
	ctx := t.Context()
	logger := slog.Default()

	// Acquire first lease
	_, lease, err := backuplease.Acquire(ctx, ts, testShardKey, "pooler-1", "backup", logger)
	require.NoError(t, err)
	defer lease.Release(ctx)

	// Second acquire should fail immediately
	_, _, err = backuplease.Acquire(ctx, ts, testShardKey, "pooler-2", "backup", logger)
	require.Error(t, err)
}

func TestLostChannelFiresOnForceUnlock(t *testing.T) {
	ts := newTestStore(t)
	ctx := t.Context()
	logger := slog.Default()

	// Acquire lease
	_, lease, err := backuplease.Acquire(ctx, ts, testShardKey, "pooler-1", "backup", logger)
	require.NoError(t, err)

	// Force unlock (simulating steal)
	err = ts.ForceUnlockBackup(ctx, testShardKey)
	require.NoError(t, err)

	// Lost channel should fire within the check interval + some margin
	select {
	case <-lease.Lost():
		// Expected
	case <-time.After(backuplease.CheckInterval + 2*time.Second):
		t.Fatal("lease.Lost() should have fired after force unlock")
	}
}

func TestSteal(t *testing.T) {
	ts := newTestStore(t)
	ctx := t.Context()
	logger := slog.Default()

	// Acquire initial lease
	_, lease1, err := backuplease.Acquire(ctx, ts, testShardKey, "pooler-1", "backup", logger)
	require.NoError(t, err)

	// Steal the lease
	_, lease2, err := backuplease.Steal(ctx, ts, testShardKey, "pooler-2", "failover-backup", logger)
	require.NoError(t, err)
	require.NotNil(t, lease2)
	defer lease2.Release(ctx)

	// Original lease should be lost
	select {
	case <-lease1.Lost():
		// Expected
	case <-time.After(backuplease.CheckInterval + 2*time.Second):
		t.Fatal("original lease should have been lost after steal")
	}
}

func TestStealWhenNoLockExists(t *testing.T) {
	ts := newTestStore(t)
	ctx := t.Context()
	logger := slog.Default()

	// Steal when no lock exists should succeed (acquire directly after grace period)
	_, lease, err := backuplease.Steal(ctx, ts, testShardKey, "pooler-1", "backup", logger)
	require.NoError(t, err)
	require.NotNil(t, lease)
	defer lease.Release(ctx)
}

func TestAcquireDifferentShards(t *testing.T) {
	ts := newTestStore(t)
	ctx := t.Context()
	logger := slog.Default()

	shardKey2 := types.ShardKey{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "1"}

	// Acquire leases on different shards — both should succeed
	_, lease1, err := backuplease.Acquire(ctx, ts, testShardKey, "pooler-1", "backup", logger)
	require.NoError(t, err)
	defer lease1.Release(ctx)

	_, lease2, err := backuplease.Acquire(ctx, ts, shardKey2, "pooler-1", "backup", logger)
	require.NoError(t, err)
	defer lease2.Release(ctx)

	// Verify both are independent
	assert.NotEqual(t, lease1, lease2)
}
