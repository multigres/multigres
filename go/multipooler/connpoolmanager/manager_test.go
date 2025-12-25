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

package connpoolmanager

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/viperutil"
)

// newTestConfig creates a Config with test-friendly defaults.
func newTestConfig(t *testing.T) *Config {
	t.Helper()
	reg := viperutil.NewRegistry()
	return NewConfig(reg)
}

// newTestManager creates a Manager from a test config.
func newTestManager(t *testing.T) *Manager {
	t.Helper()
	cfg := newTestConfig(t)
	return cfg.NewManager()
}

func TestManagerClosedState(t *testing.T) {
	mgr := newTestManager(t)

	// Mark as closed manually (simulating state after Close)
	mgr.mu.Lock()
	mgr.closed = true
	mgr.mu.Unlock()

	// getOrCreateUserPool should fail on closed manager
	ctx := context.Background()
	_, err := mgr.getOrCreateUserPool(ctx, "testuser")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manager is closed")
}

func TestGetRegularConnWithSettings_UsesSettingsCache(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	connConfig := &ConnectionConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
	}

	mgr.Open(ctx, slog.Default(), connConfig)
	defer mgr.Close()

	// Verify settings cache is used
	settings := map[string]string{"timezone": "UTC", "search_path": "public"}

	// Initial cache should be empty
	assert.Equal(t, 0, mgr.settingsCache.Size())

	ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	// First call should add to cache
	_, _ = mgr.GetRegularConnWithSettings(ctxTimeout, settings, "user1")

	// Cache should have one entry now
	assert.Equal(t, 1, mgr.settingsCache.Size())

	// Same settings should hit cache
	_, _ = mgr.GetRegularConnWithSettings(ctxTimeout, settings, "user2")
	assert.Equal(t, 1, mgr.settingsCache.Size()) // Still 1, cache hit
}

func TestGetReservedConn_DifferentUsers(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	connConfig := &ConnectionConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
	}

	mgr.Open(ctx, slog.Default(), connConfig)
	defer mgr.Close()

	// Create pools for multiple users
	ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	_, _ = mgr.GetRegularConn(ctxTimeout, "user1")
	_, _ = mgr.GetRegularConn(ctxTimeout, "user2")

	// Both should have pools
	mgr.mu.Lock()
	_, exists1 := mgr.userPools["user1"]
	_, exists2 := mgr.userPools["user2"]
	mgr.mu.Unlock()

	assert.True(t, exists1)
	assert.True(t, exists2)

	// GetReservedConn for wrong user should not find connection
	conn, ok := mgr.GetReservedConn(1, "user3")
	assert.False(t, ok)
	assert.Nil(t, conn)
}

func TestPoolOperations_MaxUsersLimit(t *testing.T) {
	reg := viperutil.NewRegistry()
	cfg := NewConfig(reg)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	cfg.RegisterFlags(fs)
	err := fs.Parse([]string{"--connpool-max-users=2"})
	require.NoError(t, err)

	mgr := cfg.NewManager()
	ctx := context.Background()

	connConfig := &ConnectionConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
	}

	mgr.Open(ctx, slog.Default(), connConfig)
	defer mgr.Close()

	ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	// Create 2 user pools (at limit)
	_, _ = mgr.GetRegularConn(ctxTimeout, "user1")
	_, _ = mgr.GetRegularConn(ctxTimeout, "user2")

	// Third user should fail due to max users limit
	_, err = mgr.GetRegularConn(ctxTimeout, "user3")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "maximum number of user pools")
}

func TestPoolOperations_SameUserMultipleCalls(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	connConfig := &ConnectionConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
	}

	mgr.Open(ctx, slog.Default(), connConfig)
	defer mgr.Close()

	ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	// Multiple calls for same user should use same pool
	_, _ = mgr.GetRegularConn(ctxTimeout, "sameuser")
	_, _ = mgr.GetRegularConn(ctxTimeout, "sameuser")
	_, _ = mgr.GetRegularConn(ctxTimeout, "sameuser")

	// Should only have 1 user pool
	mgr.mu.Lock()
	count := len(mgr.userPools)
	mgr.mu.Unlock()

	assert.Equal(t, 1, count)
}
