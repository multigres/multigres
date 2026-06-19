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

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/tools/timer"
	"github.com/multigres/multigres/go/tools/viperutil"
)

func newTestTopoStore() topoclient.Store {
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
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
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
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
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
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
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
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db1"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)

	// Start the engine
	err := re.Start()
	if err != nil {
		t.Fatalf("failed to start recovery engine: %v", err)
	}

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Stop the engine
	re.Shutdown()

	// Verify context is cancelled
	select {
	case <-re.shutdownCtx.Done():
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
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db1"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)

	// Track config reloads
	var reloadCount atomic.Int32
	re.SetConfigReloader(func() []string {
		reloadCount.Add(1)
		return []string{"db1", "db2"}
	})

	// Start the engine
	err := re.Start()
	if err != nil {
		t.Fatalf("failed to start recovery engine: %v", err)
	}
	defer re.Shutdown()

	// Wait for config reload to be called at least once
	require.Eventually(t, func() bool {
		return reloadCount.Load() > 0
	}, 2*time.Second, 50*time.Millisecond, "config reloader was never called during maintenance loop")

	// Wait for targets to be updated
	expectedTargets := []config.WatchTarget{
		{Database: "db1"},
		{Database: "db2"},
	}
	require.Eventually(t, func() bool {
		re.mu.Lock()
		defer re.mu.Unlock()
		return slices.Equal(re.shardWatchTargets, expectedTargets)
	}, 2*time.Second, 50*time.Millisecond, "targets not updated during maintenance")
}

func TestRecoveryEngine_ConfigReloadError(t *testing.T) {
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(1*time.Minute),
	)

	initialTargets := []config.WatchTarget{
		{Database: "db1"},
	}

	re := NewEngine(
		ts,
		logger,
		cfg,
		initialTargets,
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
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
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		initialTargets,
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
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
	re.Shutdown()
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
		Id: &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
		},
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "1",
		},
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler3"},
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg2",
			Shard:      "0",
		},
	}))

	// Create engine with short refresh interval for testing
	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(5*time.Second), // bookkeeping interval (not relevant for this test)
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)

	// Start the engine - it should discover existing poolers
	err := re.Start()
	require.NoError(t, err, "failed to start recovery engine")
	defer re.Shutdown()

	// Wait for discovery loop to pick up all poolers
	require.Eventually(t, func() bool {
		return re.poolerStore.Len() == 3
	}, 1*time.Second, 50*time.Millisecond, "all 3 poolers should be discovered")

	key1 := poolerKey("zone1", "pooler1")
	key2 := poolerKey("zone1", "pooler2")
	key3 := poolerKey("zone1", "pooler3")

	_, ok := re.poolerStore.GetRider(key1)
	require.True(t, ok, "pooler1 should be in store")
	_, ok = re.poolerStore.GetRider(key2)
	require.True(t, ok, "pooler2 should be in store")
	_, ok = re.poolerStore.GetRider(key3)
	require.True(t, ok, "pooler3 should be in store")
}

func TestRecoveryDisableEnable(t *testing.T) {
	ctx := context.Background()
	interval := 100 * time.Millisecond

	// Create minimal engine with just the recovery runner
	runner := timer.NewPeriodicRunner(ctx, interval)

	callCount := atomic.Int32{}
	callback := func(ctx context.Context) {
		callCount.Add(1)
	}

	// Start recovery
	runner.Start(callback, nil)
	require.True(t, runner.Running())

	// Let it run a few times
	time.Sleep(350 * time.Millisecond)
	count1 := callCount.Load()
	require.Greater(t, count1, int32(2), "should have run at least 3 times")

	// Stop - should stop immediately
	runner.Stop()
	require.False(t, runner.Running())

	// Wait and verify no more executions
	count2 := callCount.Load()
	time.Sleep(300 * time.Millisecond)
	count3 := callCount.Load()
	require.Equal(t, count2, count3, "no executions should occur while stopped")

	// Restart
	runner.Start(callback, nil)
	require.True(t, runner.Running())

	// Verify it resumes
	time.Sleep(350 * time.Millisecond)
	count4 := callCount.Load()
	require.Greater(t, count4, count3, "should resume executing after restart")

	// Clean up
	runner.Stop()
}

func TestRecoveryDisableWaitsForInFlight(t *testing.T) {
	ctx := context.Background()
	interval := 50 * time.Millisecond

	runner := timer.NewPeriodicRunner(ctx, interval)

	inCallback := make(chan struct{})
	canReturn := make(chan struct{})

	callback := func(ctx context.Context) {
		inCallback <- struct{}{}
		<-canReturn
	}

	// Start runner
	runner.Start(callback, nil)

	// Wait for callback to start
	<-inCallback

	// Stop should block until callback completes
	done := make(chan struct{})
	go func() {
		runner.Stop()
		close(done)
	}()

	// Verify Stop is still blocking
	select {
	case <-done:
		t.Fatal("Stop returned before callback completed")
	case <-time.After(100 * time.Millisecond):
		// Expected - still waiting
	}

	// Allow callback to complete
	close(canReturn)

	// Now Stop should return
	select {
	case <-done:
		// Expected
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Stop did not return after callback completed")
	}

	require.False(t, runner.Running())
}
