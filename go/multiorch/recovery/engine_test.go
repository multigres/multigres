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

package recovery

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/multiorch/config"
	"github.com/multigres/multigres/go/multiorch/store"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/viperutil"
)

func newTestTopoStore() topo.Store {
	return memorytopo.NewServer(context.Background(), "zone1")
}

func TestRecoveryEngine_ConfigReload(t *testing.T) {
	// Create a test topology store
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(1*time.Minute),
		config.WithClusterMetadataRefreshInterval(15*time.Second),
		config.WithClusterMetadataRefreshTimeout(30*time.Second),
	)

	// Initial config
	initialTargets := []config.WatchTarget{
		{Database: "db1"},
		{Database: "db2"},
	}

	// Create recovery engine
	re := NewEngine(
		ts,
		logger,
		cfg,
		initialTargets,
	)

	// Verify initial config
	if !slices.Equal(re.shardWatchTargets, initialTargets) {
		t.Errorf("initial targets mismatch: got %v, want %v", re.shardWatchTargets, initialTargets)
	}

	// Set up config reloader
	newRawTargets := []string{"db1", "db2", "db3"}
	reloadCalled := false
	re.SetConfigReloader(func() []string {
		reloadCalled = true
		return newRawTargets
	})

	// Trigger config reload
	re.reloadConfigs()

	// Verify reload was called
	if !reloadCalled {
		t.Error("config reloader was not called")
	}

	// Verify targets were updated
	re.mu.Lock()
	updatedTargets := re.shardWatchTargets
	re.mu.Unlock()

	expectedTargets := []config.WatchTarget{
		{Database: "db1"},
		{Database: "db2"},
		{Database: "db3"},
	}
	if !slices.Equal(updatedTargets, expectedTargets) {
		t.Errorf("targets not updated: got %v, want %v", updatedTargets, expectedTargets)
	}
}

func TestRecoveryEngine_ConfigReload_NoChange(t *testing.T) {
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(1*time.Minute),
		config.WithClusterMetadataRefreshInterval(15*time.Second),
		config.WithClusterMetadataRefreshTimeout(30*time.Second),
	)

	initialTargets := []config.WatchTarget{
		{Database: "db1"},
		{Database: "db2"},
	}

	re := NewEngine(
		ts,
		logger,
		cfg,
		initialTargets,
	)

	// Set up config reloader that returns same targets
	reloadCalled := false
	re.SetConfigReloader(func() []string {
		reloadCalled = true
		return []string{"db1", "db2"}
	})

	// Trigger config reload
	re.reloadConfigs()

	// Verify reload was called
	if !reloadCalled {
		t.Error("config reloader was not called")
	}

	// Verify targets unchanged
	re.mu.Lock()
	updatedTargets := re.shardWatchTargets
	re.mu.Unlock()

	if !slices.Equal(updatedTargets, initialTargets) {
		t.Errorf("targets should be unchanged: got %v, want %v", updatedTargets, initialTargets)
	}
}

func TestRecoveryEngine_ConfigReload_EmptyTargets(t *testing.T) {
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(1*time.Minute),
		config.WithClusterMetadataRefreshInterval(15*time.Second),
		config.WithClusterMetadataRefreshTimeout(30*time.Second),
	)

	initialTargets := []config.WatchTarget{
		{Database: "db1"},
		{Database: "db2"},
	}

	re := NewEngine(
		ts,
		logger,
		cfg,
		initialTargets,
	)

	// Set up config reloader that returns empty targets
	re.SetConfigReloader(func() []string {
		return []string{}
	})

	// Trigger config reload
	re.reloadConfigs()

	// Verify targets unchanged (empty targets should be ignored)
	re.mu.Lock()
	updatedTargets := re.shardWatchTargets
	re.mu.Unlock()

	if !slices.Equal(updatedTargets, initialTargets) {
		t.Errorf("targets should be unchanged when empty: got %v, want %v", updatedTargets, initialTargets)
	}
}

func TestRecoveryEngine_StartStop(t *testing.T) {
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(1*time.Minute),
		config.WithClusterMetadataRefreshInterval(15*time.Second),
		config.WithClusterMetadataRefreshTimeout(30*time.Second),
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db1"}},
	)

	// Start the engine
	err := re.Start()
	if err != nil {
		t.Fatalf("failed to start recovery engine: %v", err)
	}

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Stop the engine
	re.Stop()

	// Verify context is cancelled
	select {
	case <-re.ctx.Done():
		// Success - context is cancelled
	case <-time.After(1 * time.Second):
		t.Error("context was not cancelled after Stop()")
	}
}

func TestRecoveryEngine_MaintenanceLoop(t *testing.T) {
	// Create a test topology store
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Use shorter intervals for testing
	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(200*time.Millisecond),
		config.WithClusterMetadataRefreshInterval(100*time.Millisecond),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db1"}},
	)

	// Track config reloads
	var reloadCount int32
	re.SetConfigReloader(func() []string {
		atomic.AddInt32(&reloadCount, 1)
		return []string{"db1", "db2"}
	})

	// Start the engine
	err := re.Start()
	if err != nil {
		t.Fatalf("failed to start recovery engine: %v", err)
	}

	// Let it run for a bit to trigger both loops
	time.Sleep(500 * time.Millisecond)

	// Stop the engine
	re.Stop()

	// Verify config reload was called at least once
	if atomic.LoadInt32(&reloadCount) == 0 {
		t.Error("config reloader was never called during maintenance loop")
	}

	// Verify targets were updated
	re.mu.Lock()
	finalTargets := re.shardWatchTargets
	re.mu.Unlock()

	expectedTargets := []config.WatchTarget{
		{Database: "db1"},
		{Database: "db2"},
	}
	if !slices.Equal(finalTargets, expectedTargets) {
		t.Errorf("targets not updated during maintenance: got %v, want %v", finalTargets, expectedTargets)
	}
}

func TestRecoveryEngine_ConfigReloadError(t *testing.T) {
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(1*time.Minute),
		config.WithClusterMetadataRefreshInterval(15*time.Second),
		config.WithClusterMetadataRefreshTimeout(30*time.Second),
	)

	initialTargets := []config.WatchTarget{
		{Database: "db1"},
	}

	re := NewEngine(
		ts,
		logger,
		cfg,
		initialTargets,
	)

	// Set up config reloader that returns invalid targets
	re.SetConfigReloader(func() []string {
		return []string{"invalid/too/many/parts/here"}
	})

	// Trigger config reload
	re.reloadConfigs()

	// Verify targets unchanged (invalid targets should be rejected)
	re.mu.Lock()
	currentTargets := re.shardWatchTargets
	re.mu.Unlock()

	if !slices.Equal(currentTargets, initialTargets) {
		t.Errorf("targets should be unchanged when parse error: got %v, want %v", currentTargets, initialTargets)
	}
}

func TestRecoveryEngine_GoroutinePileupPrevention(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var executionCount int32
	var inProgress atomic.Bool

	// Slow operation: first call proceeds immediately, subsequent calls sleep first
	slowOperation := func() {
		// Read current count before incrementing
		currentCount := atomic.LoadInt32(&executionCount)

		// If count > 0, sleep before incrementing (simulates slow operation)
		if currentCount > 0 {
			sleepTime := time.Duration(currentCount*1000) * time.Millisecond
			select {
			case <-time.After(sleepTime):
			case <-ctx.Done():
				return
			}
		}

		// Increment after sleep (or immediately for first call)
		atomic.AddInt32(&executionCount, 1)
	}

	// Try to trigger the operation 10 times rapidly
	for range 10 {
		runIfNotRunning(logger, &inProgress, "test_operation", slowOperation)
	}

	// Wait for the first goroutine to complete (it doesn't sleep, so should be fast)
	// Use Eventually to avoid flakiness while keeping test fast
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&executionCount) == 1
	}, 100*time.Millisecond, 5*time.Millisecond, "expected exactly 1 execution due to pile-up prevention")

	// Clean up - cancel context to stop the sleeping goroutine
	cancel()

	// Wait a bit for cleanup
	time.Sleep(50 * time.Millisecond)
}

func TestRecoveryEngine_ViperDynamicConfig(t *testing.T) {
	// Create a test topology store
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create a viperutil registry and dynamic value
	reg := viperutil.NewRegistry()
	shardWatchTargets := viperutil.Configure(reg, "watch-targets", viperutil.Options[[]string]{
		FlagName: "watch-targets",
		Dynamic:  true,
	})

	// Set initial value
	shardWatchTargets.Set([]string{"db1"})

	// Parse initial targets
	initialTargets, err := config.ParseShardWatchTargets(shardWatchTargets.Get())
	require.NoError(t, err)

	// Create recovery engine with initial config
	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(200*time.Millisecond),
		config.WithClusterMetadataRefreshInterval(100*time.Millisecond),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		initialTargets,
	)

	// Set up config reloader that reads from viperutil.Value
	re.SetConfigReloader(func() []string {
		return shardWatchTargets.Get()
	})

	// Start the engine
	err = re.Start()
	require.NoError(t, err, "failed to start recovery engine")

	// Verify initial targets
	re.mu.Lock()
	currentTargets := re.shardWatchTargets
	re.mu.Unlock()

	expectedInit := []config.WatchTarget{{Database: "db1"}}
	require.Equal(t, expectedInit, currentTargets, "initial targets mismatch")

	// Update viper config using Set()
	shardWatchTargets.Set([]string{"db1", "db2", "db3"})

	// Use require.Eventually to wait for the update to be picked up by bookkeeping loop
	expectedFinal := []config.WatchTarget{
		{Database: "db1"},
		{Database: "db2"},
		{Database: "db3"},
	}
	require.Eventually(t, func() bool {
		re.mu.Lock()
		defer re.mu.Unlock()
		return slices.Equal(re.shardWatchTargets, expectedFinal)
	}, 1*time.Second, 50*time.Millisecond, "targets should be updated from viperutil config")

	// Stop the engine
	re.Stop()
}

// TestRecoveryEngine_DiscoveryLoop_Integration tests that the refresh ticker
// actually discovers new poolers added to topology over time
func TestRecoveryEngine_DiscoveryLoop_Integration(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Add poolers to topology BEFORE starting engine
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		Database: "mydb", TableGroup: "tg1", Shard: "1",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler3"},
		Database: "mydb", TableGroup: "tg2", Shard: "0",
	}))

	// Create engine with short refresh interval for testing
	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(5*time.Second),                   // bookkeeping interval (not relevant for this test)
		config.WithClusterMetadataRefreshInterval(100*time.Millisecond), // metadata refresh interval - short for testing
		config.WithClusterMetadataRefreshTimeout(5*time.Second),         // metadata refresh timeout
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
	)

	// Start the engine - it should discover existing poolers
	err := re.Start()
	require.NoError(t, err, "failed to start recovery engine")
	defer re.Stop()

	// Wait for discovery loop to pick up all poolers
	require.Eventually(t, func() bool {
		return re.poolerStore.Len() == 3
	}, 1*time.Second, 50*time.Millisecond, "all 3 poolers should be discovered")

	key1 := poolerKey("zone1", "pooler1")
	key2 := poolerKey("zone1", "pooler2")
	key3 := poolerKey("zone1", "pooler3")

	_, ok := re.poolerStore.Get(key1)
	require.True(t, ok, "pooler1 should be in store")
	_, ok = re.poolerStore.Get(key2)
	require.True(t, ok, "pooler2 should be in store")
	_, ok = re.poolerStore.Get(key3)
	require.True(t, ok, "pooler3 should be in store")
}

// TestRecoveryEngine_BookkeepingLoop_Integration tests that the bookkeeping ticker
// actually removes old poolers that haven't been seen in over 4 hours
func TestRecoveryEngine_BookkeepingLoop_Integration(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Create engine with short bookkeeping interval for testing
	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(100*time.Millisecond),     // bookkeeping interval - short for testing
		config.WithClusterMetadataRefreshInterval(5*time.Second), // metadata refresh interval (not relevant for this test)
		config.WithClusterMetadataRefreshTimeout(5*time.Second),  // metadata refresh timeout
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
	)

	// Add poolers to store BEFORE starting engine
	key1 := poolerKey("zone1", "old-pooler")
	oldTime := time.Now().Add(-5 * time.Hour) // 5 hours ago (> 4 hour threshold)
	re.poolerStore.Set(key1, &store.PoolerHealth{
		MultiPooler: &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "old-pooler"},
			Database: "mydb", TableGroup: "tg1", Shard: "0",
		},
		LastSeen:            oldTime,
		LastCheckAttempted:  oldTime,
		LastCheckSuccessful: oldTime,
		IsUpToDate:          true,
	})

	key2 := poolerKey("zone1", "never-seen")
	re.poolerStore.Set(key2, &store.PoolerHealth{
		MultiPooler: &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "never-seen"},
			Database: "mydb", TableGroup: "tg1", Shard: "1",
		},
		LastCheckAttempted: oldTime,
		LastSeen:           time.Time{}, // Zero - never seen
		IsUpToDate:         false,
	})

	key3 := poolerKey("zone1", "healthy-pooler")
	recentTime := time.Now().Add(-1 * time.Hour) // 1 hour ago (< 4 hour threshold)
	re.poolerStore.Set(key3, &store.PoolerHealth{
		MultiPooler: &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "healthy-pooler"},
			Database: "mydb", TableGroup: "tg1", Shard: "2",
		},
		LastSeen:            recentTime,
		LastCheckAttempted:  recentTime,
		LastCheckSuccessful: recentTime,
		IsUpToDate:          true,
	})

	// Initially 3 poolers
	require.Equal(t, 3, re.poolerStore.Len())

	// Start the engine - bookkeeping should begin removing old poolers
	err := re.Start()
	require.NoError(t, err, "failed to start recovery engine")
	defer re.Stop()

	// Wait for bookkeeping loop to remove the old poolers
	require.Eventually(t, func() bool {
		return re.poolerStore.Len() == 1
	}, 2*time.Second, 50*time.Millisecond, "old poolers should be forgotten")

	// Verify old poolers are gone
	_, ok := re.poolerStore.Get(key1)
	require.False(t, ok, "old-pooler should be removed")

	_, ok = re.poolerStore.Get(key2)
	require.False(t, ok, "never-seen pooler should be removed")

	// Verify healthy pooler remains
	_, ok = re.poolerStore.Get(key3)
	require.True(t, ok, "healthy-pooler should remain")
}

// TestRecoveryEngine_FullIntegration tests discovery and bookkeeping together
func TestRecoveryEngine_FullIntegration(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Create engine with short intervals for testing
	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(150*time.Millisecond),            // bookkeeping interval
		config.WithClusterMetadataRefreshInterval(100*time.Millisecond), // metadata refresh interval
		config.WithClusterMetadataRefreshTimeout(5*time.Second),         // metadata refresh timeout
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
	)

	// Add pooler to topology BEFORE starting
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "new-pooler"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
		Hostname: "host1",
	}))

	// Add an old pooler to store BEFORE starting
	keyOld := poolerKey("zone1", "old-pooler")
	oldTime := time.Now().Add(-5 * time.Hour)
	re.poolerStore.Set(keyOld, &store.PoolerHealth{
		MultiPooler: &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "old-pooler"},
			Database: "mydb", TableGroup: "tg1", Shard: "1",
		},
		LastSeen:            oldTime,
		LastCheckAttempted:  oldTime,
		LastCheckSuccessful: oldTime,
	})

	// Start the engine
	err := re.Start()
	require.NoError(t, err, "failed to start recovery engine")
	defer re.Stop()

	// Wait for discovery of new pooler
	keyNew := poolerKey("zone1", "new-pooler")
	require.Eventually(t, func() bool {
		_, ok := re.poolerStore.Get(keyNew)
		return ok
	}, 1*time.Second, 50*time.Millisecond, "new pooler should be discovered")

	// Verify initial state (not health checked)
	info, ok := re.poolerStore.Get(keyNew)
	require.True(t, ok)
	require.True(t, info.LastSeen.IsZero(), "LastSeen should be zero initially")
	require.False(t, info.IsUpToDate, "IsUpToDate should be false initially")

	// Simulate health check
	now := time.Now()
	info.LastSeen = now
	info.LastCheckAttempted = now
	info.LastCheckSuccessful = now
	info.IsUpToDate = true
	re.poolerStore.Set(keyNew, info)

	// Update topology (change hostname)
	retrieved, err := ts.GetMultiPooler(ctx, &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "new-pooler",
	})
	require.NoError(t, err)
	retrieved.MultiPooler.Hostname = "host2"
	require.NoError(t, ts.UpdateMultiPooler(ctx, retrieved))

	// Wait for refresh to pick up the change
	require.Eventually(t, func() bool {
		info, ok := re.poolerStore.Get(keyNew)
		return ok && info.MultiPooler.Hostname == "host2"
	}, 1*time.Second, 50*time.Millisecond, "hostname update should be discovered")

	// Verify timestamps were preserved
	updatedInfo, ok := re.poolerStore.Get(keyNew)
	require.True(t, ok)
	require.Equal(t, "host2", updatedInfo.MultiPooler.Hostname, "hostname should be updated")
	require.Equal(t, now.Unix(), updatedInfo.LastSeen.Unix(), "LastSeen should be preserved")
	require.True(t, updatedInfo.IsUpToDate, "IsUpToDate should be preserved")

	// Wait for bookkeeping to remove old pooler
	require.Eventually(t, func() bool {
		_, ok := re.poolerStore.Get(keyOld)
		return !ok
	}, 2*time.Second, 50*time.Millisecond, "old pooler should be forgotten")

	// Verify new pooler still exists
	_, ok = re.poolerStore.Get(keyNew)
	require.True(t, ok, "new pooler should still exist")
}
