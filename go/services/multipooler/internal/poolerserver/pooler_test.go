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
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protoutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

func TestNewQueryPoolerServer_NilPoolManager(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Creating with nil pool manager should work but executor will be nil
	pooler := NewQueryPoolerServer(logger, nil, nil, "", "", nil, 0, false)

	assert.NotNil(t, pooler)
	assert.Equal(t, logger, pooler.logger)
	// Executor should be nil since pool manager was nil
	exec, err := pooler.Executor()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pool manager was nil")
	assert.Nil(t, exec)
}

func TestQueryPoolerServer_ReplicationMetrics(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	pooler := NewQueryPoolerServer(logger, nil, nil, "", "", nil, 0, false)

	// The shared replication instruments are built at construction and exposed
	// for the StreamReplication handler to derive per-stream recorders.
	require.NotNil(t, pooler.ReplicationMetrics())
}

func TestIsHealthy_NotInitialized(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	pooler := NewQueryPoolerServer(logger, nil, nil, "", "", nil, 0, false)

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
	pooler := NewQueryPoolerServer(logger, mockMgr, nil, "", "", nil, 0, false)

	require.NotNil(t, pooler)
	assert.Equal(t, logger, pooler.logger)
	assert.Equal(t, mockMgr, pooler.poolManager)

	exec, err := pooler.Executor()
	require.NoError(t, err)
	require.NotNil(t, exec)
}

func newStartRequestTestServer() *QueryPoolerServer {
	logger := slog.New(slog.DiscardHandler)
	return &QueryPoolerServer{
		logger:        logger,
		servingStatus: clustermetadatapb.PoolerServingStatus_DISABLED,
		gracePeriod:   3 * time.Second,
		stateChanged:  make(chan struct{}),
		drainStats:    newDrainStats(),
	}
}

// requireMTF01 asserts that err is the MTF01 (planned-failover) gateway-buffering signal.
func requireMTF01(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF01.ID), "expected MTF01 error, got: %v", err)
}

func TestStartRequest_Serving(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING

	// All request kinds are admitted while serving normally.
	require.NoError(t, s.StartRequest(nil, RequestExistingReserved))
	require.NoError(t, s.StartRequest(nil, RequestSingleQuery))
	require.NoError(t, s.StartRequest(nil, RequestNewReservation))
}

func TestStartRequest_NotServing(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_DISABLED // drainPhase is drainNone (post-flip)

	// Existing reserved ops are still admitted (the connection's existence is the
	// gate; a force-closed connection surfaces an honest 40001 from the executor).
	require.NoError(t, s.StartRequest(nil, RequestExistingReserved))
	// Single queries and new reservations are rejected with MTF01 (gateway buffers).
	requireMTF01(t, s.StartRequest(nil, RequestSingleQuery))
	requireMTF01(t, s.StartRequest(nil, RequestNewReservation))
}

func TestStartRequest_DrainReserved_ServesSingleQueries(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.drainPhase = drainReserved

	// Stage 1: single queries keep flowing and existing reserved ops finish, but
	// new reservations are rejected so no new transactions accumulate.
	require.NoError(t, s.StartRequest(nil, RequestSingleQuery))
	require.NoError(t, s.StartRequest(nil, RequestExistingReserved))
	requireMTF01(t, s.StartRequest(nil, RequestNewReservation))
}

func TestStartRequest_DrainRegular_RejectsSingleQueries(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.drainPhase = drainRegular

	// Stage 2: single queries are now rejected too; only existing reserved ops
	// (concluding in-flight transactions) are still admitted.
	requireMTF01(t, s.StartRequest(nil, RequestSingleQuery))
	requireMTF01(t, s.StartRequest(nil, RequestNewReservation))
	require.NoError(t, s.StartRequest(nil, RequestExistingReserved))
}

func TestStartRequest_PrimaryOpOnReplica_ReservedConnAllowed(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	s.routingRole = servingstate.RoutingRoleReplica

	target := &query.Target{Mode: query.Mode_MODE_WRITABLE}
	// A fresh PRIMARY-targeted request on a demoted (REPLICA) pooler is rejected
	// with MTF01...
	requireMTF01(t, s.StartRequest(target, RequestSingleQuery))
	// ...but an in-flight op on an existing reserved connection (e.g. a COMMIT
	// after a planned demotion) is admitted, so the executor can conclude the
	// transaction on the live backend or return an honest 40001 if it was
	// force-closed during the drain.
	require.NoError(t, s.StartRequest(target, RequestExistingReserved))
}

func TestStartRequest_PrimaryQueryOnReplica_Rejected(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.routingRole = servingstate.RoutingRoleReplica

	// PRIMARY query hitting a REPLICA pooler should be rejected with MTF01.
	target := &query.Target{Mode: query.Mode_MODE_WRITABLE}
	requireMTF01(t, s.StartRequest(target, RequestSingleQuery))
}

func TestStartRequest_PrimaryQueryOnPrimary_Allowed(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.routingRole = servingstate.RoutingRolePrimary // writable leader

	target := &query.Target{Mode: query.Mode_MODE_WRITABLE}
	require.NoError(t, s.StartRequest(target, RequestSingleQuery))
}

// TestStartRequest_WritableGatesOnRoutingRole is the write-safety regression:
// both leader-bound query modes (WRITABLE and CONSISTENT) gate on the same
// write-safety routing role. A pooler that is NOT yet writable (routingRole !=
// PRIMARY — e.g. the pg_promote()→WAL-commit window) must reject BOTH with MTF01
// so the gateway buffers; once it is the writable leader (RoutingRolePrimary)
// both are admitted.
func TestStartRequest_WritableGatesOnRoutingRole(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.routingRole = servingstate.RoutingRoleReplica // not yet the writable leader

	// Not writable: both leader-bound modes are rejected. Leadership alone no
	// longer admits either — routing role (write-safety) is the single gate.
	requireMTF01(t, s.StartRequest(&query.Target{Mode: query.Mode_MODE_WRITABLE}, RequestSingleQuery))
	requireMTF01(t, s.StartRequest(&query.Target{Mode: query.Mode_MODE_CONSISTENT}, RequestSingleQuery))

	// Once the writable leader, both modes are admitted.
	s.routingRole = servingstate.RoutingRolePrimary
	require.NoError(t, s.StartRequest(&query.Target{Mode: query.Mode_MODE_WRITABLE}, RequestSingleQuery))
	require.NoError(t, s.StartRequest(&query.Target{Mode: query.Mode_MODE_CONSISTENT}, RequestSingleQuery))
}

func TestStartRequest_ReplicaQueryOnPrimary_Allowed(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.routingRole = servingstate.RoutingRolePrimary

	// PRIMARY pooler can serve REPLICA traffic.
	target := &query.Target{Mode: query.Mode_MODE_INCONSISTENT}
	require.NoError(t, s.StartRequest(target, RequestSingleQuery))
}

func TestStartRequest_NilTarget_Skipped(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.routingRole = servingstate.RoutingRoleReplica

	// Nil target should skip target validation (e.g., GetAuthCredentials).
	require.NoError(t, s.StartRequest(nil, RequestSingleQuery))
}

func TestStartRequest_TableGroupMismatch_Bug(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.routingRole = servingstate.RoutingRolePrimary
	s.tableGroup = "tg1"
	s.shard = "0"

	// Tablegroup mismatch is a routing bug (MTD01), rejected for every kind.
	target := protoutil.NewTarget("", "tg2", "0", query.Mode_MODE_WRITABLE)
	err := s.StartRequest(target, RequestExistingReserved)
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTD01.ID), "expected MTD01 error, got: %v", err)
	assert.Contains(t, err.Error(), "tg2")
	assert.Contains(t, err.Error(), "tg1")
}

func TestStartRequest_ShardMismatch_Bug(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.routingRole = servingstate.RoutingRolePrimary
	s.tableGroup = "tg1"
	s.shard = "0"

	// Shard mismatch is a routing bug (MTD01).
	target := protoutil.NewTarget("", "tg1", "1", query.Mode_MODE_WRITABLE)
	err := s.StartRequest(target, RequestSingleQuery)
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTD01.ID), "expected MTD01 error, got: %v", err)
	assert.Contains(t, err.Error(), "shard")
}

func TestStartRequest_FullTargetMatch_Allowed(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.routingRole = servingstate.RoutingRolePrimary // writable leader
	s.tableGroup = "tg1"
	s.shard = "0"

	target := protoutil.NewTarget("", "tg1", "0", query.Mode_MODE_WRITABLE)
	require.NoError(t, s.StartRequest(target, RequestSingleQuery))
}

// drainMockPoolManager is a mock pool manager that supports two-stage drain
// tracking, similar to connpoolmanager.Manager. regularAdd simulates a regular
// (single-query) borrow; reservedAdd simulates a reserved connection. The
// combined drain (WaitForDrain) sees regularCount + reservedCount; the
// reserved-only drain (WaitForReservedDrain) sees reservedCount.
type drainMockPoolManager struct {
	connpoolmanager.PoolManager // embed for default nil implementations

	mu             sync.Mutex
	regularCount   int64
	reservedCount  int64
	zeroCh         chan struct{}
	reservedZeroCh chan struct{}

	closeReservedCount int
	closeReservedCalls int
}

func newDrainMockPoolManager() *drainMockPoolManager {
	ch := make(chan struct{})
	close(ch) // starts drained
	rch := make(chan struct{})
	close(rch)
	return &drainMockPoolManager{zeroCh: ch, reservedZeroCh: rch}
}

// signalZeroChan reconciles a drain channel with its total, mirroring
// Manager.signalZeroChanLocked. Caller must hold m.mu.
func signalZeroChan(total int64, ch *chan struct{}) {
	if total == 0 {
		select {
		case <-*ch:
		default:
			close(*ch)
		}
		return
	}
	select {
	case <-*ch:
		*ch = make(chan struct{})
	default:
	}
}

func (m *drainMockPoolManager) regularAdd(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.regularCount += n
	signalZeroChan(m.regularCount+m.reservedCount, &m.zeroCh)
}

func (m *drainMockPoolManager) reservedAdd(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reservedCount += n
	signalZeroChan(m.reservedCount, &m.reservedZeroCh)
	signalZeroChan(m.regularCount+m.reservedCount, &m.zeroCh)
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

func (m *drainMockPoolManager) WaitForReservedDrain(ctx context.Context) error {
	m.mu.Lock()
	ch := m.reservedZeroCh
	m.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *drainMockPoolManager) CloseReservedConnections(ctx context.Context) int {
	m.mu.Lock()
	m.closeReservedCalls++
	m.mu.Unlock()
	return m.closeReservedCount
}

func (m *drainMockPoolManager) closeReservedCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeReservedCalls
}

func newTestPoolerWithDrain(mock *drainMockPoolManager) *QueryPoolerServer {
	logger := slog.New(slog.DiscardHandler)
	return &QueryPoolerServer{
		logger:        logger,
		poolManager:   mock,
		servingStatus: clustermetadatapb.PoolerServingStatus_DISABLED,
		gracePeriod:   3 * time.Second,
		stateChanged:  make(chan struct{}),
		drainStats:    newDrainStats(),
	}
}

// waitForDrainPhase blocks until the pooler reaches the given drain phase.
func waitForDrainPhase(t *testing.T, pooler *QueryPoolerServer, phase drainPhase) {
	t.Helper()
	require.Eventually(t, func() bool {
		pooler.mu.Lock()
		defer pooler.mu.Unlock()
		return pooler.drainPhase == phase
	}, time.Second, 10*time.Millisecond, "pooler did not reach drain phase %d", phase)
}

// TestOnStateChange_TwoStageDrain exercises the two-stage graceful drain:
// stage 1 (drainReserved) serves single queries while a transaction is in
// flight; stage 2 (drainRegular) starts only after the transaction finishes and
// then rejects single queries too; the transition completes once the in-flight
// single query also finishes.
func TestOnStateChange_TwoStageDrain(t *testing.T) {
	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 5 * time.Second
	ctx := t.Context()

	require.NoError(t, pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))

	// One in-flight transaction (reserved) and one in-flight single query (regular).
	mock.reservedAdd(1)
	mock.regularAdd(1)

	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		_ = pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED})
	}()

	// Stage 1: blocked on the in-flight transaction. Single queries are served,
	// existing reserved ops are admitted, new reservations are rejected.
	waitForDrainPhase(t, pooler, drainReserved)
	require.NoError(t, pooler.StartRequest(nil, RequestSingleQuery))
	require.NoError(t, pooler.StartRequest(nil, RequestExistingReserved))
	requireMTF01(t, pooler.StartRequest(nil, RequestNewReservation))

	// Finish the transaction → advances to stage 2.
	mock.reservedAdd(-1)
	waitForDrainPhase(t, pooler, drainRegular)

	// Stage 2: single queries are now rejected too; existing reserved ops still admitted.
	requireMTF01(t, pooler.StartRequest(nil, RequestSingleQuery))
	require.NoError(t, pooler.StartRequest(nil, RequestExistingReserved))

	// Finish the in-flight single query → drain completes.
	mock.regularAdd(-1)
	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not complete after in-flight work returned")
	}
	assert.False(t, pooler.IsServing())
}

// TestOnStateChange_GracePeriodExpiresInStage1 covers the force-close path: a
// transaction that never finishes keeps stage 1 (reserved drain) blocked past
// the shared grace period, so the pooler force-closes reserved connections and
// completes the transition anyway.
func TestOnStateChange_GracePeriodExpiresInStage1(t *testing.T) {
	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 100 * time.Millisecond
	ctx := t.Context()

	require.NoError(t, pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))

	// A transaction that never commits — stage 1 (WaitForReservedDrain) blocks.
	mock.reservedAdd(1)

	start := time.Now()
	require.NoError(t, pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}))
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, 80*time.Millisecond, "should block ~grace period waiting on the held transaction")
	assert.Less(t, elapsed, 2*time.Second, "should not block much past the grace period")
	assert.Positive(t, mock.closeReservedCallCount(), "reserved connections should be force-closed on grace expiry")
	assert.False(t, pooler.IsServing(), "transition completes despite the held transaction")

	mock.reservedAdd(-1) // clean up
}

// TestOnStateChange_WaitsForInflightRequests verifies that OnStateChange blocks
// until all lent connections are returned before completing the DISABLED transition.
func TestOnStateChange_WaitsForInflightRequests(t *testing.T) {
	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 5 * time.Second
	ctx := t.Context()

	require.NoError(t, pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))

	// Simulate two in-flight connections
	mock.regularAdd(1)
	mock.regularAdd(1)

	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		_ = pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED})
	}()

	// Drain should not complete while connections are in-flight
	select {
	case <-drainDone:
		t.Fatal("drain should not complete with in-flight connections")
	case <-time.After(100 * time.Millisecond):
	}

	// Return both connections
	mock.regularAdd(-1)
	mock.regularAdd(-1)

	select {
	case <-drainDone:
		// Drain completed after all connections returned
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not complete after all connections returned")
	}

	// Should be DISABLED now
	assert.False(t, pooler.IsServing())
}

// TestOnStateChange_GracePeriodExpires verifies that OnStateChange completes
// after the grace period even if connections haven't drained.
func TestOnStateChange_GracePeriodExpires(t *testing.T) {
	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 100 * time.Millisecond
	ctx := t.Context()

	require.NoError(t, pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))

	// Simulate a connection that will never return
	mock.regularAdd(1)

	start := time.Now()
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		_ = pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED})
	}()

	select {
	case <-drainDone:
		elapsed := time.Since(start)
		assert.GreaterOrEqual(t, elapsed, 80*time.Millisecond, "should wait at least close to grace period")
		assert.Less(t, elapsed, 2*time.Second, "should not wait much longer than grace period")
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not complete after grace period")
	}

	// Should be DISABLED even though connection is still in-flight
	assert.False(t, pooler.IsServing())

	// Clean up
	mock.regularAdd(-1)
}

// TestOnStateChange_BackToServing verifies the SERVING → DISABLED → SERVING round-trip.
func TestOnStateChange_BackToServing(t *testing.T) {
	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 50 * time.Millisecond
	ctx := t.Context()

	// SERVING → DISABLED → SERVING
	require.NoError(t, pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))
	assert.True(t, pooler.IsServing())

	require.NoError(t, pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}))
	assert.False(t, pooler.IsServing())

	// Transition back to SERVING
	require.NoError(t, pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))
	assert.True(t, pooler.IsServing())

	// Requests should work again
	err := pooler.StartRequest(nil, RequestSingleQuery)
	require.NoError(t, err)
}

// TestOnStateChange_ConcurrentRequests verifies drain behavior with many
// concurrent simulated connections during a DISABLED transition.
func TestOnStateChange_ConcurrentRequests(t *testing.T) {
	mock := newDrainMockPoolManager()
	pooler := newTestPoolerWithDrain(mock)
	pooler.gracePeriod = 2 * time.Second
	ctx := t.Context()

	require.NoError(t, pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))

	const numRequests = 50
	var wg sync.WaitGroup
	wg.Add(numRequests)

	// Simulate many concurrent connections
	for range numRequests {
		go func() {
			defer wg.Done()
			if err := pooler.StartRequest(nil, RequestSingleQuery); err != nil {
				return
			}
			mock.regularAdd(1)
			time.Sleep(50 * time.Millisecond)
			mock.regularAdd(-1)
		}()
	}

	// Give goroutines time to start
	time.Sleep(10 * time.Millisecond)

	// Transition to DISABLED while connections are in-flight
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		_ = pooler.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED})
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

func TestAwaitStateChange_AlreadyMatches(t *testing.T) {
	s := newStartRequestTestServer()
	ctx := t.Context()

	// Transition to PRIMARY/SERVING
	require.NoError(t, s.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))

	// AwaitStateChange should return immediately when state already matches
	done := make(chan struct{})
	go func() {
		s.AwaitStateChange(ctx, servingstate.RoutingRolePrimary, clustermetadatapb.PoolerServingStatus_SERVING)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("AwaitStateChange should return immediately when state matches")
	}
}

func TestAwaitStateChange_BlocksUntilTransition(t *testing.T) {
	s := newStartRequestTestServer()
	ctx := t.Context()

	// Start as DISABLED (default)
	done := make(chan struct{})
	go func() {
		s.AwaitStateChange(ctx, servingstate.RoutingRolePrimary, clustermetadatapb.PoolerServingStatus_SERVING)
		close(done)
	}()

	// Should still be blocking
	select {
	case <-done:
		t.Fatal("AwaitStateChange should block until state matches")
	case <-time.After(50 * time.Millisecond):
	}

	// Transition to the awaited state
	require.NoError(t, s.OnStateChange(ctx, servingstate.State{RoutingRole: servingstate.RoutingRolePrimary, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("AwaitStateChange should unblock after OnStateChange")
	}
}

func TestAwaitStateChange_RespectsContextCancellation(t *testing.T) {
	s := newStartRequestTestServer()
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		// Wait for a state that will never happen
		s.AwaitStateChange(ctx, servingstate.RoutingRolePrimary, clustermetadatapb.PoolerServingStatus_SERVING)
		close(done)
	}()

	// Should be blocking
	select {
	case <-done:
		t.Fatal("AwaitStateChange should block")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("AwaitStateChange should return on context cancellation")
	}
}
