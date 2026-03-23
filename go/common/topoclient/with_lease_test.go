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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/common/types"
)

var withLeaseShardKey = types.ShardKey{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "0"}

// backupLeaseOps returns the acquire/revoke/check closures for backup lease tests.
func backupLeaseOps(ts topoclient.Store, shardKey types.ShardKey) (topoclient.LeaseAcquirer, topoclient.LeaseRevoker, topoclient.LeaseChecker) {
	return func(ctx context.Context, action string) (context.Context, func(*error), error) {
			return ts.TryLockBackup(ctx, shardKey, action)
		},
		func(ctx context.Context) error { return ts.RevokeBackup(ctx, shardKey) },
		func(ctx context.Context) error { return topoclient.AssertBackupLockHeld(ctx, shardKey) }
}

func TestWithLease(t *testing.T) {
	ctx := t.Context()
	config := topoclient.NewDefaultTopoConfig()
	config.SetLockTimeout(100 * time.Millisecond)
	ts, _ := memorytopo.NewServerAndFactoryWithConfig(ctx, config, "zone1")
	t.Cleanup(func() { ts.Close() })

	acquire, revoke, check := backupLeaseOps(ts, withLeaseShardKey)

	called := false
	err := topoclient.WithLease(ctx, "test-op by pooler-1", acquire, revoke, check, func(ctx context.Context) error {
		called = true
		require.NoError(t, topoclient.AssertBackupLockHeld(ctx, withLeaseShardKey))
		return nil
	})
	require.NoError(t, err)
	assert.True(t, called)

	// Lock should be released — acquire again to verify
	err = topoclient.WithLease(ctx, "test-op by pooler-2", acquire, revoke, check, func(ctx context.Context) error {
		return nil
	})
	require.NoError(t, err)
}

func TestWithLeaseSteal(t *testing.T) {
	ctx := t.Context()
	config := topoclient.NewDefaultTopoConfig()
	config.SetLockTimeout(100 * time.Millisecond)
	ts, _ := memorytopo.NewServerAndFactoryWithConfig(ctx, config, "zone1")
	t.Cleanup(func() { ts.Close() })

	acquire, revoke, check := backupLeaseOps(ts, withLeaseShardKey)

	// Hold a lease in background with a short check interval so it detects
	// revocation quickly.
	held := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- topoclient.WithLease(ctx, "op by pooler-1", acquire, revoke, check, func(ctx context.Context) error {
			close(held)
			<-ctx.Done()
			return context.Cause(ctx)
		},
			topoclient.WithLeaseCheckInterval(100*time.Millisecond),
		)
	}()
	<-held

	// Steal with grace period
	called := false
	err := topoclient.WithLease(ctx, "op by pooler-2", acquire, revoke, check, func(ctx context.Context) error {
		called = true
		return nil
	},
		topoclient.WithStealGracePeriod(100*time.Millisecond),
	)
	require.NoError(t, err)
	assert.True(t, called)

	// Original should have received ErrLeaseLost
	err = <-done
	assert.ErrorIs(t, err, topoclient.ErrLeaseLost)
}
