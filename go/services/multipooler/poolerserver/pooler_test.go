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

package poolerserver

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/connpoolmanager"
)

func TestNewQueryPoolerServer_NilPoolManager(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Creating with nil pool manager should work but executor will be nil
	pooler := NewQueryPoolerServer(logger, nil, nil, "", "", nil, 0)

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
	pooler := NewQueryPoolerServer(logger, nil, nil, "", "", nil, 0)

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
	pooler := NewQueryPoolerServer(logger, mockMgr, nil, "", "", nil, 0)

	require.NotNil(t, pooler)
	assert.Equal(t, logger, pooler.logger)
	assert.Equal(t, mockMgr, pooler.poolManager)

	exec, err := pooler.Executor()
	require.NoError(t, err)
	require.NotNil(t, exec)
}

func newStartRequestTestServer() *QueryPoolerServer {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &QueryPoolerServer{
		logger:        logger,
		servingStatus: clustermetadatapb.PoolerServingStatus_NOT_SERVING,
		gracePeriod:   3 * time.Second,
		stateChanged:  make(chan struct{}),
	}
}

func TestStartRequest_Serving(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING

	// Both allowOnShutdown=true and false should succeed when serving
	err := s.StartRequest(nil, false)
	require.NoError(t, err)

	err = s.StartRequest(nil, true)
	require.NoError(t, err)
}

func TestStartRequest_NotServing(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_NOT_SERVING

	// Both should fail when not serving with MTF01 (triggers gateway buffering)
	err := s.StartRequest(nil, false)
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF01.ID), "expected MTF01 error, got: %v", err)

	err = s.StartRequest(nil, true)
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF01.ID), "expected MTF01 error, got: %v", err)
}

func TestStartRequest_ShuttingDown_NewRequestRejected(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.shuttingDown = true

	// New requests (allowOnShutdown=false) should be rejected
	err := s.StartRequest(nil, false)
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF01.ID), "expected MTF01 error, got: %v", err)
}

func TestStartRequest_ShuttingDown_ExistingReservedAllowed(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.shuttingDown = true

	// Existing reserved connections (allowOnShutdown=true) should be allowed
	err := s.StartRequest(nil, true)
	require.NoError(t, err)
}

func TestStartRequest_PrimaryQueryOnReplica_Rejected(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.poolerType = clustermetadatapb.PoolerType_REPLICA

	// PRIMARY query hitting a REPLICA pooler should be rejected with MTF01
	target := &query.Target{PoolerType: clustermetadatapb.PoolerType_PRIMARY}
	err := s.StartRequest(target, false)
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF01.ID), "expected MTF01 error, got: %v", err)
}

func TestStartRequest_PrimaryQueryOnPrimary_Allowed(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.poolerType = clustermetadatapb.PoolerType_PRIMARY

	target := &query.Target{PoolerType: clustermetadatapb.PoolerType_PRIMARY}
	err := s.StartRequest(target, false)
	require.NoError(t, err)
}

func TestStartRequest_ReplicaQueryOnPrimary_Allowed(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.poolerType = clustermetadatapb.PoolerType_PRIMARY

	// PRIMARY pooler can serve REPLICA traffic
	target := &query.Target{PoolerType: clustermetadatapb.PoolerType_REPLICA}
	err := s.StartRequest(target, false)
	require.NoError(t, err)
}

func TestStartRequest_NilTarget_Skipped(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.poolerType = clustermetadatapb.PoolerType_REPLICA

	// Nil target should skip validation (e.g., GetAuthCredentials)
	err := s.StartRequest(nil, false)
	require.NoError(t, err)
}

func TestStartRequest_TableGroupMismatch_Bug(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.poolerType = clustermetadatapb.PoolerType_PRIMARY
	s.tableGroup = "tg1"
	s.shard = "0"

	// Tablegroup mismatch is a routing bug (MTD01)
	target := &query.Target{
		TableGroup: "tg2",
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	err := s.StartRequest(target, false)
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTD01.ID), "expected MTD01 error, got: %v", err)
	assert.Contains(t, err.Error(), "tg2")
	assert.Contains(t, err.Error(), "tg1")
}

func TestStartRequest_ShardMismatch_Bug(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.poolerType = clustermetadatapb.PoolerType_PRIMARY
	s.tableGroup = "tg1"
	s.shard = "0"

	// Shard mismatch is a routing bug (MTD01)
	target := &query.Target{
		TableGroup: "tg1",
		Shard:      "1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	err := s.StartRequest(target, false)
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTD01.ID), "expected MTD01 error, got: %v", err)
	assert.Contains(t, err.Error(), "shard")
}

func TestStartRequest_FullTargetMatch_Allowed(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.poolerType = clustermetadatapb.PoolerType_PRIMARY
	s.tableGroup = "tg1"
	s.shard = "0"

	target := &query.Target{
		TableGroup: "tg1",
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	err := s.StartRequest(target, false)
	require.NoError(t, err)
}

// drainMockPoolManager is a mock pool manager that supports drain tracking
// via lentAdd/WaitForDrain, similar to connpoolmanager.Manager.
type drainMockPoolManager struct {
	connpoolmanager.PoolManager // embed for default nil implementations

	mu        sync.Mutex
	lentCount int64
	zeroCh    chan struct{}

	closeReservedCount int
}

func newDrainMockPoolManager() *drainMockPoolManager {
	ch := make(chan struct{})
	close(ch) // starts drained
	return &drainMockPoolManager{zeroCh: ch}
}

func (m *drainMockPoolManager) lentAdd(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lentCount += n
	if m.lentCount == 0 {
		select {
		case <-m.zeroCh:
		default:
			close(m.zeroCh)
		}
	} else if m.lentCount == n && n > 0 {
		m.zeroCh = make(chan struct{})
	}
}

func (m *drainMockPoolManager) WaitForDrain(ctx context.Context) error {
	m.mu.Lock()
	ch := m.zeroCh
	m.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *drainMockPoolManager) CloseReservedConnections(ctx context.Context) int {
	return m.closeReservedCount
}

func newTestPoolerWithDrain(mock *drainMockPoolManager) *QueryPoolerServer {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &QueryPoolerServer{
		logger:        logger,
		poolManager:   mock,
		servingStatus: clustermetadatapb.PoolerServingStatus_NOT_SERVING,
		gracePeriod:   3 * time.Second,
		stateChanged:  make(chan struct{}),
	}
}

// TestStartRequest_ShuttingDown_WithDrain tests the full drain lifecycle:
// in-flight connections keep drain blocked, allowOnShutdown=false is rejected,
// allowOnShutdown=true is allowed, and drain completes when connections return.
func TestStartRequest_ShuttingDown_WithDrain(t *testing.T) {
	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 5 * time.Second
	ctx := t.Context()

	// Transition to SERVING
	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	// Simulate an in-flight connection to keep drain waiting
	mock.lentAdd(1)

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
	err := pooler.StartRequest(nil, false)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF01.ID), "expected MTF01 error, got: %v", err)

	// allowOnShutdown=true should succeed during shutdown
	err = pooler.StartRequest(nil, true)
	assert.NoError(t, err)

	// Return the in-flight connection to let drain complete
	mock.lentAdd(-1)

	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not complete after in-flight connection returned")
	}
}

// TestOnStateChange_WaitsForInflightRequests verifies that OnStateChange blocks
// until all lent connections are returned before completing the NOT_SERVING transition.
func TestOnStateChange_WaitsForInflightRequests(t *testing.T) {
	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 5 * time.Second
	ctx := t.Context()

	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	// Simulate two in-flight connections
	mock.lentAdd(1)
	mock.lentAdd(1)

	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		_ = pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING)
	}()

	// Drain should not complete while connections are in-flight
	select {
	case <-drainDone:
		t.Fatal("drain should not complete with in-flight connections")
	case <-time.After(100 * time.Millisecond):
	}

	// Return both connections
	mock.lentAdd(-1)
	mock.lentAdd(-1)

	select {
	case <-drainDone:
		// Drain completed after all connections returned
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not complete after all connections returned")
	}

	// Should be NOT_SERVING now
	assert.False(t, pooler.IsServing())
}

// TestOnStateChange_GracePeriodExpires verifies that OnStateChange completes
// after the grace period even if connections haven't drained.
func TestOnStateChange_GracePeriodExpires(t *testing.T) {
	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 100 * time.Millisecond
	ctx := t.Context()

	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	// Simulate a connection that will never return
	mock.lentAdd(1)

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

	// Should be NOT_SERVING even though connection is still in-flight
	assert.False(t, pooler.IsServing())

	// Clean up
	mock.lentAdd(-1)
}

// TestOnStateChange_BackToServing verifies the SERVING → NOT_SERVING → SERVING round-trip.
func TestOnStateChange_BackToServing(t *testing.T) {
	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 50 * time.Millisecond
	ctx := t.Context()

	// SERVING → NOT_SERVING → SERVING
	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	assert.True(t, pooler.IsServing())

	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING))
	assert.False(t, pooler.IsServing())

	// Transition back to SERVING
	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	assert.True(t, pooler.IsServing())

	// Requests should work again
	err := pooler.StartRequest(nil, false)
	require.NoError(t, err)
}

// TestOnStateChange_ConcurrentRequests verifies drain behavior with many
// concurrent simulated connections during a NOT_SERVING transition.
func TestOnStateChange_ConcurrentRequests(t *testing.T) {
	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 2 * time.Second
	ctx := t.Context()

	require.NoError(t, pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	const numRequests = 50
	var wg sync.WaitGroup
	wg.Add(numRequests)

	// Simulate many concurrent connections
	for range numRequests {
		go func() {
			defer wg.Done()
			if err := pooler.StartRequest(nil, false); err != nil {
				return
			}
			mock.lentAdd(1)
			time.Sleep(50 * time.Millisecond)
			mock.lentAdd(-1)
		}()
	}

	// Give goroutines time to start
	time.Sleep(10 * time.Millisecond)

	// Transition to NOT_SERVING while connections are in-flight
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		_ = pooler.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_NOT_SERVING)
	}()

	// Wait for all goroutines and drain to complete
	wg.Wait()
	select {
	case <-drainDone:
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not complete")
	}

	assert.False(t, pooler.IsServing())
}
