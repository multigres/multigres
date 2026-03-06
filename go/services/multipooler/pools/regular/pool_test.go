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

package regular

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/connstate"
	"github.com/multigres/multigres/go/services/multipooler/pools/connpool"
)

func newTestPool(_ *testing.T, server *fakepgserver.Server) *Pool {
	pool := NewPool(context.Background(), &PoolConfig{
		ClientConfig: server.ClientConfig(),
		ConnPoolConfig: &connpool.Config{
			Capacity:     2,
			MaxIdleCount: 2,
		},
		AdminPool: nil, // Not needed for basic tests
	})
	pool.Open()
	return pool
}

func TestPool_GetConnection(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Get a connection.
	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, pooled)

	// Verify connection is working.
	assert.False(t, pooled.Conn.IsClosed())

	// Return connection.
	pooled.Recycle()

	// Verify stats.
	stats := pool.Stats()
	assert.Equal(t, int64(1), stats.Active)
	assert.Equal(t, int64(1), stats.Idle)
}

func TestPool_GetWithSettings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept any SET and RESET commands.
	server.AddQueryPattern(`SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})
	server.AddQueryPattern(`RESET .+`, &sqltypes.Result{})

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	settings := connstate.NewSettings(map[string]string{
		"search_path": "public",
	}, 0)

	// Get a connection with settings.
	pooled, err := pool.GetWithSettings(ctx, settings)
	require.NoError(t, err)
	require.NotNil(t, pooled)

	// Verify settings were applied.
	assert.Equal(t, settings, pooled.Conn.Settings())

	pooled.Recycle()

	// Verify SET was actually called.
	assert.Greater(t, server.GetPatternCalledNum(`SELECT pg_catalog\.set_config\(.+\)`), 0, "SET command should have been called")
}

func TestPool_Close(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)

	ctx := context.Background()

	// Get and return a connection.
	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	pooled.Recycle()

	// Close the pool.
	pool.Close()

	// Stats should show closed state.
	stats := pool.Stats()
	assert.Equal(t, int64(0), stats.Active)
}

func TestPool_Stats(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Initial stats.
	stats := pool.Stats()
	assert.Equal(t, int64(2), stats.Capacity)

	// Get a connection.
	pooled1, err := pool.Get(ctx)
	require.NoError(t, err)

	stats = pool.Stats()
	assert.Equal(t, int64(1), stats.Active)
	assert.Equal(t, int64(0), stats.Idle)
	assert.Equal(t, int64(1), stats.Borrowed)

	// Return the connection.
	pooled1.Recycle()

	stats = pool.Stats()
	assert.Equal(t, int64(1), stats.Active)
	assert.Equal(t, int64(1), stats.Idle)
	assert.Equal(t, int64(0), stats.Borrowed)
}

func TestPool_InnerPool(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	inner := pool.InnerPool()
	require.NotNil(t, inner)
	assert.Equal(t, "unnamed", inner.Name)
}

func TestConn_State(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	defer pooled.Recycle()

	// State should not be nil.
	state := pooled.Conn.State()
	require.NotNil(t, state)
}

func TestConn_Settings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	defer pooled.Recycle()

	// Without applying settings, should be nil.
	assert.Nil(t, pooled.Conn.Settings())
}

func TestConn_ResetAllSettings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET and RESET commands.
	server.AddQueryPattern(`SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})
	server.AddQueryPattern(`RESET .+`, &sqltypes.Result{})

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	settings := connstate.NewSettings(map[string]string{
		"search_path": "public",
	}, 0)

	// Get connection with settings.
	pooled, err := pool.GetWithSettings(ctx, settings)
	require.NoError(t, err)

	// Reset settings.
	err = pooled.Conn.ResetAllSettings(ctx)
	require.NoError(t, err)

	// Settings should be nil after reset.
	assert.Nil(t, pooled.Conn.Settings())

	pooled.Recycle()

	// Verify both SET and RESET were actually called.
	assert.Greater(t, server.GetPatternCalledNum(`SELECT pg_catalog\.set_config\(.+\)`), 0, "SET command should have been called")
	assert.Greater(t, server.GetPatternCalledNum(`RESET .+`), 0, "RESET command should have been called")
}

func TestConn_ApplySettings_ResetsRemovedVariables(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET, individual RESET, and combined RESET+SET commands.
	server.AddQueryPattern(`SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})
	server.AddQueryPattern(`RESET search_path`, &sqltypes.Result{})
	server.AddQueryPattern(`RESET search_path; SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	initial := connstate.NewSettings(map[string]string{
		"search_path": "public",
		"work_mem":    "256MB",
	}, 0)

	// Get connection with settings.
	pooled, err := pool.GetWithSettings(ctx, initial)
	require.NoError(t, err)

	// Apply desired state that only has work_mem (search_path removed).
	desired := connstate.NewSettings(map[string]string{
		"work_mem": "256MB",
	}, 0)
	err = pooled.Conn.ApplySettings(ctx, desired)
	require.NoError(t, err)

	// Verify RESET+SET was called for the combined command.
	assert.Greater(t, server.GetPatternCalledNum(`RESET search_path; SELECT pg_catalog\.set_config\(.+\)`), 0, "combined RESET+SET should have been called")

	// Verify tracked state is updated to desired.
	assert.Equal(t, desired, pooled.Conn.Settings())

	pooled.Recycle()
}

func TestConn_ApplySettings_NilDesiredResetsAll(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddQueryPattern(`SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})
	server.AddQueryPattern(`RESET ROLE; RESET ALL`, &sqltypes.Result{})

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	initial := connstate.NewSettings(map[string]string{
		"search_path": "public",
	}, 0)

	// Get connection with settings.
	pooled, err := pool.GetWithSettings(ctx, initial)
	require.NoError(t, err)

	// Apply nil desired — should reset all since current has settings.
	err = pooled.Conn.ApplySettings(ctx, nil)
	require.NoError(t, err)

	// Verify RESET ROLE; RESET ALL was called.
	assert.Greater(t, server.GetPatternCalledNum(`RESET ROLE; RESET ALL`), 0, "RESET ROLE; RESET ALL should have been called")

	// Tracked state should be nil.
	assert.Nil(t, pooled.Conn.Settings())

	pooled.Recycle()
}

func TestConn_ApplySettings_NilDesiredNoopWhenClean(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Get a clean connection (no settings).
	pooled, err := pool.Get(ctx)
	require.NoError(t, err)

	// Apply nil desired — should be a no-op since no current settings.
	err = pooled.Conn.ApplySettings(ctx, nil)
	require.NoError(t, err)

	assert.Nil(t, pooled.Conn.Settings())

	pooled.Recycle()
}

func TestConn_ApplySettings_OverwritesExistingVariable(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddQueryPattern(`SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	first := connstate.NewSettings(map[string]string{
		"work_mem": "256MB",
	}, 0)

	// Get connection with initial settings.
	pooled, err := pool.GetWithSettings(ctx, first)
	require.NoError(t, err)

	// Apply new value for the same variable.
	second := connstate.NewSettings(map[string]string{
		"work_mem": "512MB",
	}, 0)
	err = pooled.Conn.ApplySettings(ctx, second)
	require.NoError(t, err)

	// Tracked state should have the second value.
	assert.Equal(t, second, pooled.Conn.Settings())

	pooled.Recycle()
}
