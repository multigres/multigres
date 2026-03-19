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

package poolerserver

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/connpoolmanager"
)

func TestNewQueryPoolerServer_NilPoolManager(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Creating with nil pool manager should work but executor will be nil
	pooler := NewQueryPoolerServer(logger, nil, nil, nil)

	assert.NotNil(t, pooler)
	assert.Equal(t, logger, pooler.logger)
	// Executor should be nil since pool manager was nil
	exec, err := pooler.Executor()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pool manager was nil")
	assert.Nil(t, exec)
}

func TestIsHealthy_NotInitialized(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	pooler := NewQueryPoolerServer(logger, nil, nil, nil)

	// IsHealthy should fail since the pool manager is not initialized
	err := pooler.IsHealthy()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "executor not initialized")
}

// mockPoolManager implements PoolManager for testing
type mockPoolManager struct {
	connpoolmanager.PoolManager // embed for default nil implementations
}

func TestNewQueryPoolerServer_WithPoolManager(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	mockMgr := &mockPoolManager{}
	pooler := NewQueryPoolerServer(logger, mockMgr, nil, nil)

	require.NotNil(t, pooler)
	assert.Equal(t, logger, pooler.logger)
	assert.Equal(t, mockMgr, pooler.poolManager)

	exec, err := pooler.Executor()
	require.NoError(t, err)
	require.NotNil(t, exec)
}

func newTestPooler() *QueryPoolerServer {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return NewQueryPoolerServer(logger, nil, nil, nil)
}

func TestStartRequest_SucceedsWhenServing(t *testing.T) {
	pooler := newTestPooler()
	ctx := context.Background()
	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	err := pooler.StartRequest(false)
	require.NoError(t, err)
	pooler.EndRequest()
}

func TestStartRequest_FailsWhenNotServing(t *testing.T) {
	pooler := newTestPooler()
	// Default state is NOT_SERVING

	err := pooler.StartRequest(false)
	assert.ErrorIs(t, err, ErrNotServing)

	// allowOnShutdown=true still fails when NOT_SERVING
	err = pooler.StartRequest(true)
	assert.ErrorIs(t, err, ErrNotServing)
}

func TestStartRequest_ShuttingDown(t *testing.T) {
	pooler := newTestPooler()
	pooler.SetGracePeriod(5 * time.Second)
	ctx := context.Background()

	// Transition to SERVING
	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	// Start an in-flight request to keep drain waiting
	require.NoError(t, pooler.StartRequest(false))

	// Begin NOT_SERVING transition in background (will block on drain)
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		_ = pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING)
	}()

	// Wait for shuttingDown to be set
	require.Eventually(t, func() bool {
		pooler.mu.Lock()
		defer pooler.mu.Unlock()
		return pooler.shuttingDown
	}, time.Second, 10*time.Millisecond)

	// allowOnShutdown=false should fail during shutdown
	err := pooler.StartRequest(false)
	assert.ErrorIs(t, err, ErrShuttingDown)

	// allowOnShutdown=true should succeed during shutdown
	err = pooler.StartRequest(true)
	assert.NoError(t, err)
	pooler.EndRequest()

	// End the original in-flight request to let drain complete
	pooler.EndRequest()

	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not complete after in-flight request ended")
	}
}

func TestOnStateChange_WaitsForInflightRequests(t *testing.T) {
	pooler := newTestPooler()
	pooler.SetGracePeriod(5 * time.Second)
	ctx := context.Background()

	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	// Start two in-flight requests
	require.NoError(t, pooler.StartRequest(false))
	require.NoError(t, pooler.StartRequest(false))

	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		_ = pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING)
	}()

	// Drain should not complete while requests are in-flight
	select {
	case <-drainDone:
		t.Fatal("drain should not complete with in-flight requests")
	case <-time.After(100 * time.Millisecond):
	}

	// Complete both requests
	pooler.EndRequest()
	pooler.EndRequest()

	select {
	case <-drainDone:
		// Drain completed after all requests finished
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not complete after all requests ended")
	}

	// Should be NOT_SERVING now
	assert.False(t, pooler.IsServing())
}

func TestOnStateChange_GracePeriodExpires(t *testing.T) {
	pooler := newTestPooler()
	pooler.SetGracePeriod(100 * time.Millisecond)
	ctx := context.Background()

	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	// Start a request that will never complete
	require.NoError(t, pooler.StartRequest(false))

	start := time.Now()
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		_ = pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING)
	}()

	select {
	case <-drainDone:
		elapsed := time.Since(start)
		assert.GreaterOrEqual(t, elapsed, 80*time.Millisecond, "should wait at least close to grace period")
		assert.Less(t, elapsed, 2*time.Second, "should not wait much longer than grace period")
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not complete after grace period")
	}

	// Should be NOT_SERVING even though request is still in-flight
	assert.False(t, pooler.IsServing())

	// Clean up
	pooler.EndRequest()
}

func TestOnStateChange_BackToServing(t *testing.T) {
	pooler := newTestPooler()
	pooler.SetGracePeriod(50 * time.Millisecond)
	ctx := context.Background()

	// SERVING → NOT_SERVING → SERVING
	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	assert.True(t, pooler.IsServing())

	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING))
	assert.False(t, pooler.IsServing())

	// Transition back to SERVING
	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	assert.True(t, pooler.IsServing())

	// Requests should work again
	err := pooler.StartRequest(false)
	require.NoError(t, err)
	pooler.EndRequest()
}

func TestOnStateChange_ConcurrentRequests(t *testing.T) {
	pooler := newTestPooler()
	pooler.SetGracePeriod(2 * time.Second)
	ctx := context.Background()

	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	const numRequests = 50
	var wg sync.WaitGroup
	wg.Add(numRequests)

	// Start many concurrent requests
	for range numRequests {
		go func() {
			defer wg.Done()
			if err := pooler.StartRequest(false); err != nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
			pooler.EndRequest()
		}()
	}

	// Give requests time to start
	time.Sleep(10 * time.Millisecond)

	// Transition to NOT_SERVING while requests are in-flight
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		_ = pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING)
	}()

	// Wait for all requests and drain to complete
	wg.Wait()
	select {
	case <-drainDone:
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not complete")
	}

	assert.False(t, pooler.IsServing())
}
