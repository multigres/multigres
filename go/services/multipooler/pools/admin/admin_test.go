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

package admin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/services/multipooler/pools/connpool"
)

func newTestPool(_ *testing.T, server *fakepgserver.Server) *Pool {
	pool := NewPool(context.Background(), &PoolConfig{
		ClientConfig: server.ClientConfig(),
		ConnPoolConfig: &connpool.Config{
			Capacity:     2,
			MaxIdleCount: 2,
		},
	})
	pool.Open()
	return pool
}

func TestPool_GetConnection(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true) // Allow any query

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

func TestPool_TerminateBackend(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Set up expected pg_terminate_backend response.
	server.AddQueryPattern(`SELECT pg_terminate_backend\(\d+\)`, fakepgserver.MakeResult(
		[]string{"pg_terminate_backend"},
		[][]any{{"t"}}, // "t" = true in PostgreSQL
	))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Terminate a backend.
	success, err := pool.TerminateBackend(ctx, 12345)
	require.NoError(t, err)
	assert.True(t, success)

	// Verify the query was actually executed.
	server.VerifyAllPatternsUsedOrFail()
}

func TestPool_TerminateBackend_NotFound(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Set up expected pg_terminate_backend response for not found.
	server.AddQueryPattern(`SELECT pg_terminate_backend\(\d+\)`, fakepgserver.MakeResult(
		[]string{"pg_terminate_backend"},
		[][]any{{"f"}}, // "f" = false - backend not found
	))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Terminate a non-existent backend.
	success, err := pool.TerminateBackend(ctx, 99999)
	require.NoError(t, err)
	assert.False(t, success)

	// Verify the query was actually executed.
	server.VerifyAllPatternsUsedOrFail()
}

func TestPool_CancelBackend(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Set up expected pg_cancel_backend response.
	server.AddQueryPattern(`SELECT pg_cancel_backend\(\d+\)`, fakepgserver.MakeResult(
		[]string{"pg_cancel_backend"},
		[][]any{{"t"}}, // "t" = true
	))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Cancel a backend.
	success, err := pool.CancelBackend(ctx, 12345)
	require.NoError(t, err)
	assert.True(t, success)

	// Verify the query was actually executed.
	server.VerifyAllPatternsUsedOrFail()
}

func TestPool_CancelBackend_NotFound(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Set up expected pg_cancel_backend response for not found.
	server.AddQueryPattern(`SELECT pg_cancel_backend\(\d+\)`, fakepgserver.MakeResult(
		[]string{"pg_cancel_backend"},
		[][]any{{"f"}}, // "f" = false - backend not found
	))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Cancel a non-existent backend.
	success, err := pool.CancelBackend(ctx, 99999)
	require.NoError(t, err)
	assert.False(t, success)

	// Verify the query was actually executed.
	server.VerifyAllPatternsUsedOrFail()
}

func TestPool_Close(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)

	ctx := context.Background()

	// Get a connection.
	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	pooled.Recycle()

	// Close the pool.
	pool.Close()

	// Verify stats show closed state.
	stats := pool.Stats()
	assert.Equal(t, int64(0), stats.Active)
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

	// Admin connections should have nil settings.
	assert.Nil(t, pooled.Conn.Settings())
}

func TestConn_ApplySettings_Panics(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	defer pooled.Recycle()

	// ApplySettings should panic for admin connections.
	assert.Panics(t, func() {
		_ = pooled.Conn.ApplySettings(ctx, nil)
	})
}

func TestConn_ResetSettings_Noop(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	defer pooled.Recycle()

	// ResetSettings should be a no-op for admin connections.
	err = pooled.Conn.ResetSettings(ctx)
	assert.NoError(t, err)
}
