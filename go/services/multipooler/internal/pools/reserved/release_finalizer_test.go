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
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
)

// newFinalizerTestPool builds a reserved pool with a release-callback counter so
// tests can assert lent accounting fires only after finalization completes.
func newFinalizerTestPool(t *testing.T, server *fakepgserver.Server, onRelease func()) *Pool {
	t.Helper()
	return NewPool(context.Background(), &PoolConfig{
		InactivityTimeout:          5 * time.Second,
		ReleaseFinalizationTimeout: 2 * time.Second,
		OnRelease:                  onRelease,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     4,
				MaxIdleCount: 4,
			},
		},
	})
}

// TestReleaseClean_TrustedMatching_NoReconcileSQL verifies that a clean release
// of a connection whose connstate already matches its authoritative settings
// recycles the backend without issuing any reconciliation SQL.
func TestReleaseClean_TrustedMatching_NoReconcileSQL(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newFinalizerTestPool(t, server, nil)
	defer pool.Close()

	cache := connstate.NewSettingsCache(10)
	settings := cache.GetOrCreate(map[string]string{"search_path": "myschema"})

	conn, err := pool.NewConn(context.Background(), settings)
	require.NoError(t, err)

	server.ResetQueryLog()
	conn.ReleaseClean(CleanReleaseCommit)

	log := server.QueryLog()
	assert.NotContains(t, log, "reset all", "trusted matching release must not reconcile")
	assert.NotContains(t, log, "set_config", "trusted matching release must not reconcile")
	assert.False(t, conn.IsClosed(), "trusted clean release must recycle, not close")
}

// TestReleaseClean_Untrusted_ForceReconciles verifies that when the connection
// is marked untrusted (e.g. after ROLLBACK TO SAVEPOINT), clean release forces
// reconciliation even though connstate pointer-equals the authoritative value.
func TestReleaseClean_Untrusted_ForceReconciles(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newFinalizerTestPool(t, server, nil)
	defer pool.Close()

	cache := connstate.NewSettingsCache(10)
	settings := cache.GetOrCreate(map[string]string{"search_path": "myschema"})

	conn, err := pool.NewConn(context.Background(), settings)
	require.NoError(t, err)

	// Mark untrusted: the backend may have reverted GUCs invisibly.
	conn.MarkSessionStateUntrusted()
	require.True(t, conn.SessionStateUntrusted())

	server.ResetQueryLog()
	conn.ReleaseClean(CleanReleaseCommit)

	log := server.QueryLog()
	assert.Contains(t, log, "reset all", "untrusted release must force a reset")
	assert.Contains(t, log, "set_config", "untrusted release must re-apply desired settings")
	assert.False(t, conn.IsClosed(), "successful force reconcile must recycle, not close")
	assert.False(t, conn.SessionStateUntrusted(), "successful force reconcile must clear untrusted")
}

// TestReleaseClean_KnownEmpty_ResetsStaleBackend verifies that a known-clean
// (empty) authoritative state resets a backend that still carries settings.
func TestReleaseClean_KnownEmpty_ResetsStaleBackend(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newFinalizerTestPool(t, server, nil)
	defer pool.Close()

	cache := connstate.NewSettingsCache(10)
	settings := cache.GetOrCreate(map[string]string{"search_path": "myschema"})

	conn, err := pool.NewConn(context.Background(), settings)
	require.NoError(t, err)

	// Desired baseline is now known-clean (nil), so the stale backend settings
	// must be reset on release.
	conn.SetAuthoritativeSettings(nil, true)

	server.ResetQueryLog()
	conn.ReleaseClean(CleanReleaseCommit)

	assert.Contains(t, server.QueryLog(), "reset all", "known-clean release must reset stale settings")
	assert.False(t, conn.IsClosed(), "successful reset must recycle, not close")
}

// TestReleaseClean_ReconcileFailure_Taints verifies that a finalizer failure
// taints/closes the backend instead of recycling stale state.
func TestReleaseClean_ReconcileFailure_Taints(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	// Force-reconcile SQL fails: the finalizer must taint rather than recycle.
	server.RejectQueryPattern("RESET ROLE.*", "injected reconcile failure")

	pool := newFinalizerTestPool(t, server, nil)
	defer pool.Close()

	cache := connstate.NewSettingsCache(10)
	settings := cache.GetOrCreate(map[string]string{"search_path": "myschema"})

	conn, err := pool.NewConn(context.Background(), settings)
	require.NoError(t, err)
	conn.MarkSessionStateUntrusted()

	conn.ReleaseClean(CleanReleaseCommit)

	assert.True(t, conn.IsClosed(), "failed finalization must taint/close the backend")
}

// TestReleaseClean_UnknownSettings_Taints verifies that a clean release with
// unknown authoritative settings taints rather than recycling.
func TestReleaseClean_UnknownSettings_Taints(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newFinalizerTestPool(t, server, nil)
	defer pool.Close()

	conn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)
	// Authoritative settings unknown: finalizer cannot safely reconcile.
	conn.SetAuthoritativeSettings(nil, false)

	conn.ReleaseClean(CleanReleaseCommit)

	assert.True(t, conn.IsClosed(), "unknown authoritative settings must taint/close")
}

// TestReleaseDirty_AlwaysTaints verifies that a dirty release closes the backend
// regardless of session state.
func TestReleaseDirty_AlwaysTaints(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newFinalizerTestPool(t, server, nil)
	defer pool.Close()

	conn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)

	conn.ReleaseDirty(DirtyReleaseError)

	assert.True(t, conn.IsClosed(), "dirty release must taint/close the backend")
}

// TestReleaseClean_OnReleaseFiresAfterFinalization verifies that the lent
// accounting callback (OnRelease) fires exactly once, after finalization
// reconciliation has run. This keeps finalizing backends counted as lent.
func TestReleaseClean_OnReleaseFiresAfterFinalization(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	var releaseCount atomic.Int64
	var resetSeenAtRelease atomic.Bool
	// When the reconcile SQL runs, record whether OnRelease has fired yet. It
	// must not have: finalization runs before lent accounting decrements.
	server.AddQueryPatternWithCallback("RESET ROLE.*", &sqltypes.Result{}, func(string) {
		if releaseCount.Load() == 0 {
			resetSeenAtRelease.Store(true)
		}
	})

	pool := newFinalizerTestPool(t, server, func() { releaseCount.Add(1) })
	defer pool.Close()

	cache := connstate.NewSettingsCache(10)
	settings := cache.GetOrCreate(map[string]string{"search_path": "myschema"})

	conn, err := pool.NewConn(context.Background(), settings)
	require.NoError(t, err)
	conn.MarkSessionStateUntrusted()

	conn.ReleaseClean(CleanReleaseCommit)

	assert.Equal(t, int64(1), releaseCount.Load(), "OnRelease must fire exactly once")
	assert.True(t, resetSeenAtRelease.Load(), "reconciliation must run before OnRelease (still lent during finalization)")
}
