// Copyright 2026 Supabase, Inc.
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

package reserved

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
)

// newFinalizerTestPool builds a reserved pool with a release-callback counter so
// tests can assert lent accounting fires only after finalization completes.
func newFinalizerTestPool(t *testing.T, server *fakepgserver.Server, onRelease func()) (*Pool, *connstate.SettingsCache) {
	t.Helper()
	cache := connstate.NewSettingsCache(10)
	return NewPool(context.Background(), &PoolConfig{
		InactivityTimeout: 5 * time.Second,
		OnRelease:         onRelease,
		SettingsCache:     cache,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     4,
				MaxIdleCount: 4,
			},
		},
	}), cache
}

// TestReleaseClean_Trusted_NoReconcileSQL verifies that a clean release of a
// trusted connection recycles the backend without issuing reconciliation SQL.
func TestReleaseClean_Trusted_NoReconcileSQL(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool, _ := newFinalizerTestPool(t, server, nil)
	defer pool.Close()

	cache := connstate.NewSettingsCache(10)
	settings := cache.GetOrCreate(map[string]string{"search_path": "myschema"})

	conn, err := pool.NewConn(context.Background(), settings)
	require.NoError(t, err)

	server.ResetQueryLog()
	conn.Release(ReleaseCommit, nil)

	log := server.QueryLog()
	assert.NotContains(t, log, "reset all", "trusted release must not reconcile")
	assert.NotContains(t, log, "set_config", "trusted release must not reconcile")
	assert.False(t, conn.IsClosed(), "trusted clean release must recycle, not close")
}

// TestReleaseClean_Untrusted_SyncsConnstateFromGateway verifies that when the
// connection is marked untrusted (e.g. after ROLLBACK TO SAVEPOINT), clean
// release syncs connstate in-memory from the gateway's authoritative settings
// without issuing SQL.
func TestReleaseClean_Untrusted_SyncsConnstateFromGateway(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool, cache := newFinalizerTestPool(t, server, nil)
	defer pool.Close()

	stale := cache.GetOrCreate(map[string]string{"client_min_messages": "error"})
	conn, err := pool.NewConn(context.Background(), stale)
	require.NoError(t, err)

	conn.MarkSessionStateUntrusted()
	require.True(t, conn.SessionStateUntrusted())

	gatewayDesired := map[string]string{"client_min_messages": "notice"}

	server.ResetQueryLog()
	conn.Release(ReleaseCommit, gatewayDesired)

	log := server.QueryLog()
	assert.NotContains(t, log, "reset all", "untrusted release must not issue SQL")
	assert.NotContains(t, log, "set_config", "untrusted release must not issue SQL")
	assert.False(t, conn.IsClosed(), "successful in-memory sync must recycle, not close")
	assert.False(t, conn.SessionStateUntrusted(), "successful sync must clear untrusted")

	expected := cache.GetOrCreate(gatewayDesired)
	assert.Equal(t, expected, conn.Conn().Settings(), "connstate must match gateway settings")
}

// TestReleaseClean_UntrustedNoGatewaySettings_ClearsToNil verifies that an
// untrusted connection released without gateway settings clears connstate to nil.
func TestReleaseClean_UntrustedNoGatewaySettings_ClearsToNil(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool, cache := newFinalizerTestPool(t, server, nil)
	defer pool.Close()

	settings := cache.GetOrCreate(map[string]string{"search_path": "myschema"})
	conn, err := pool.NewConn(context.Background(), settings)
	require.NoError(t, err)
	conn.MarkSessionStateUntrusted()

	server.ResetQueryLog()
	conn.Release(ReleaseCommit, nil)

	assert.NotContains(t, server.QueryLog(), "reset all", "untrusted release must not issue SQL")
	assert.Nil(t, conn.Conn().Settings(), "nil gateway settings must clear connstate")
	assert.False(t, conn.IsClosed(), "successful sync must recycle, not close")
}

// TestReleaseClean_UntrustedMissingCache_Taints verifies that an untrusted
// release without a settings cache taints the backend instead of recycling.
func TestReleaseClean_UntrustedMissingCache_Taints(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := NewPool(context.Background(), &PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     4,
				MaxIdleCount: 4,
			},
		},
	})
	defer pool.Close()

	conn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)
	conn.MarkSessionStateUntrusted()

	conn.Release(ReleaseCommit, map[string]string{"search_path": "public"})

	assert.True(t, conn.IsClosed(), "missing cache on untrusted release must taint/close the backend")
}

// TestReleaseDirty_AlwaysTaints verifies that a release with a reason that
// prevents reuse closes the backend regardless of session state.
func TestReleaseDirty_AlwaysTaints(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool, _ := newFinalizerTestPool(t, server, nil)
	defer pool.Close()

	conn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)

	conn.Release(ReleaseError, nil)

	assert.True(t, conn.IsClosed(), "dirty release must taint/close the backend")
}

// TestReleaseClean_OnReleaseFiresAfterFinalization verifies that the lent
// accounting callback (OnRelease) fires exactly once after finalization.
func TestReleaseClean_OnReleaseFiresAfterFinalization(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	var releaseCount atomic.Int64
	var finalizedBeforeOnRelease atomic.Bool

	pool, _ := newFinalizerTestPool(t, server, func() {
		if releaseCount.Load() == 0 {
			finalizedBeforeOnRelease.Store(true)
		}
		releaseCount.Add(1)
	})
	defer pool.Close()

	cache := connstate.NewSettingsCache(10)
	settings := cache.GetOrCreate(map[string]string{"search_path": "myschema"})

	conn, err := pool.NewConn(context.Background(), settings)
	require.NoError(t, err)
	conn.MarkSessionStateUntrusted()

	conn.Release(ReleaseCommit, map[string]string{"search_path": "myschema"})

	assert.Equal(t, int64(1), releaseCount.Load(), "OnRelease must fire exactly once")
	assert.True(t, finalizedBeforeOnRelease.Load(), "OnRelease must run after finalization while still lent")
}
