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
	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/pb/query"
)

func newTestPool(_ *testing.T, server *fakepgserver.Server) *Pool {
	pool := NewPool(&PoolConfig{
		ClientConfig: server.ClientConfig(),
		ConnPoolConfig: &connpool.Config{
			Capacity:     2,
			MaxIdleCount: 2,
		},
		AdminPool: nil, // Not needed for basic tests
	})
	pool.Open(context.Background())
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
	server.AddQueryPattern(`SET SESSION .+ = .+`, &query.QueryResult{})
	server.AddQueryPattern(`RESET .+`, &query.QueryResult{})

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
	assert.Equal(t, "regular", inner.Name)
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

func TestConn_ResetSettings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET and RESET commands.
	server.AddQueryPattern(`SET SESSION .+ = .+`, &query.QueryResult{})
	server.AddQueryPattern(`RESET .+`, &query.QueryResult{})

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
	err = pooled.Conn.ResetSettings(ctx)
	require.NoError(t, err)

	// Settings should be nil after reset.
	assert.Nil(t, pooled.Conn.Settings())

	pooled.Recycle()
}
