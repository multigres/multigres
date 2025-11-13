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

package multiorch

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
	"github.com/multigres/multigres/go/viperutil"
)

func newTestTopoStore(t *testing.T) topo.Store {
	return memorytopo.NewServer(context.Background(), "zone1")
}

func TestRecoveryEngine_ConfigReload(t *testing.T) {
	// Create a test topology store
	ts := newTestTopoStore(t)
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Initial config
	initialTargets := []WatchTarget{
		{Database: "db1"},
		{Database: "db2"},
	}

	// Create recovery engine
	re := NewRecoveryEngine(
		"zone1",
		ts,
		logger,
		initialTargets,
		1*time.Minute,
		15*time.Second,
		30*time.Second,
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

	expectedTargets := []WatchTarget{
		{Database: "db1"},
		{Database: "db2"},
		{Database: "db3"},
	}
	if !slices.Equal(updatedTargets, expectedTargets) {
		t.Errorf("targets not updated: got %v, want %v", updatedTargets, expectedTargets)
	}
}

func TestRecoveryEngine_ConfigReload_NoChange(t *testing.T) {
	ts := newTestTopoStore(t)
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	initialTargets := []WatchTarget{
		{Database: "db1"},
		{Database: "db2"},
	}

	re := NewRecoveryEngine(
		"zone1",
		ts,
		logger,
		initialTargets,
		1*time.Minute,
		15*time.Second,
		30*time.Second,
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
	ts := newTestTopoStore(t)
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	initialTargets := []WatchTarget{
		{Database: "db1"},
		{Database: "db2"},
	}

	re := NewRecoveryEngine(
		"zone1",
		ts,
		logger,
		initialTargets,
		1*time.Minute,
		15*time.Second,
		30*time.Second,
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
	ts := newTestTopoStore(t)
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	re := NewRecoveryEngine(
		"zone1",
		ts,
		logger,
		[]WatchTarget{{Database: "db1"}},
		1*time.Minute,
		15*time.Second,
		30*time.Second,
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
	ts := newTestTopoStore(t)
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Use shorter intervals for testing
	re := NewRecoveryEngine(
		"zone1",
		ts,
		logger,
		[]WatchTarget{{Database: "db1"}},
		200*time.Millisecond, // bookkeeping interval
		100*time.Millisecond, // metadata refresh interval
		5*time.Second,        // metadata refresh timeout
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

	expectedTargets := []WatchTarget{
		{Database: "db1"},
		{Database: "db2"},
	}
	if !slices.Equal(finalTargets, expectedTargets) {
		t.Errorf("targets not updated during maintenance: got %v, want %v", finalTargets, expectedTargets)
	}
}

func TestRecoveryEngine_ConfigReloadError(t *testing.T) {
	ts := newTestTopoStore(t)
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	initialTargets := []WatchTarget{
		{Database: "db1"},
	}

	re := NewRecoveryEngine(
		"zone1",
		ts,
		logger,
		initialTargets,
		1*time.Minute,
		15*time.Second,
		30*time.Second,
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
	ts := newTestTopoStore(t)
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
	initialTargets, err := ParseShardWatchTargets(shardWatchTargets.Get())
	require.NoError(t, err)

	// Create recovery engine with initial config
	re := NewRecoveryEngine(
		"zone1",
		ts,
		logger,
		initialTargets,
		200*time.Millisecond, // bookkeeping interval
		100*time.Millisecond, // metadata refresh interval
		5*time.Second,        // metadata refresh timeout
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

	expectedInit := []WatchTarget{{Database: "db1"}}
	require.Equal(t, expectedInit, currentTargets, "initial targets mismatch")

	// Update viper config using Set()
	shardWatchTargets.Set([]string{"db1", "db2", "db3"})

	// Use require.Eventually to wait for the update to be picked up by bookkeeping loop
	expectedFinal := []WatchTarget{
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
