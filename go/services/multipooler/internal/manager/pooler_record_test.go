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

package manager

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/tools/testpoll"
)

// fakeTopoStore is a minimal poolerTopoStore for testing.
type fakeTopoStore struct {
	attempts     atomic.Int32 // incremented on every RegisterMultipooler call, success or failure
	calls        atomic.Int32 // incremented only on successful Register calls
	err          atomic.Pointer[error]
	lastSeen     atomic.Pointer[clustermetadatapb.Multipooler]
	updateCalls  atomic.Int32
	lastUpdateID atomic.Pointer[clustermetadatapb.ID]
	lastUpdated  atomic.Pointer[clustermetadatapb.Multipooler]

	// record is the in-memory topology entry: Register stores it, wipe clears it.
	recordMu       sync.Mutex
	record         *clustermetadatapb.Multipooler
	createAttempts int
	// beforeCreate runs inside CreateMultipooler ahead of the existence check,
	// letting a test simulate a registration that lands in between.
	beforeCreate func()
	// getFailures is how many GetMultipooler calls still fail with a
	// transient error before reads succeed again.
	getFailures int
	// createErr, when set, is returned by CreateMultipooler.
	createErr error

	// recoveries is the channel handed to subscribers, replaced by
	// replaceConn the way a cell reconfiguration replaces the connection.
	recoveries    chan struct{}
	subscriptions int
}

func (f *fakeTopoStore) RegisterMultipooler(_ context.Context, mp *clustermetadatapb.Multipooler, _ bool) error {
	f.attempts.Add(1)
	if ep := f.err.Load(); ep != nil {
		return *ep
	}
	f.calls.Add(1)
	f.lastSeen.Store(mp)
	f.setRecord(mp)
	return nil
}

func (f *fakeTopoStore) GetMultipooler(ctx context.Context, _ *clustermetadatapb.ID) (*topoclient.MultipoolerInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.recordMu.Lock()
	defer f.recordMu.Unlock()
	if f.getFailures > 0 {
		f.getFailures--
		return nil, errors.New("topo unavailable")
	}
	if f.record == nil {
		return nil, topoclient.NewError(topoclient.NoNode, "multipooler")
	}
	return &topoclient.MultipoolerInfo{Multipooler: proto.Clone(f.record).(*clustermetadatapb.Multipooler)}, nil
}

func (f *fakeTopoStore) CreateMultipooler(ctx context.Context, mp *clustermetadatapb.Multipooler) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.recordMu.Lock()
	f.createAttempts++
	hook := f.beforeCreate
	f.recordMu.Unlock()
	if hook != nil {
		hook()
	}
	f.recordMu.Lock()
	defer f.recordMu.Unlock()
	if f.createErr != nil {
		return f.createErr
	}
	if f.record != nil {
		return topoclient.NewError(topoclient.NodeExists, "multipooler")
	}
	f.record = proto.Clone(mp).(*clustermetadatapb.Multipooler)
	return nil
}

func (f *fakeTopoStore) setRecord(mp *clustermetadatapb.Multipooler) {
	f.recordMu.Lock()
	defer f.recordMu.Unlock()
	f.record = proto.Clone(mp).(*clustermetadatapb.Multipooler)
}

func (f *fakeTopoStore) ConnRecoveries(ctx context.Context, _ string) (<-chan struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.recordMu.Lock()
	defer f.recordMu.Unlock()
	if f.recoveries == nil {
		f.recoveries = make(chan struct{}, 1)
	}
	f.subscriptions++
	return f.recoveries, nil
}

// recoverConn simulates the topology connection being lost and regained.
func (f *fakeTopoStore) recoverConn() {
	f.recordMu.Lock()
	defer f.recordMu.Unlock()
	select {
	case f.recoveries <- struct{}{}:
	default:
	}
}

// replaceConn simulates the cell connection being replaced: the old recovery
// channel closes and the next subscription gets a fresh one.
func (f *fakeTopoStore) replaceConn() {
	f.recordMu.Lock()
	defer f.recordMu.Unlock()
	close(f.recoveries)
	f.recoveries = make(chan struct{}, 1)
}

func (f *fakeTopoStore) createCount() int {
	f.recordMu.Lock()
	defer f.recordMu.Unlock()
	return f.createAttempts
}

func (f *fakeTopoStore) subscriptionCount() int {
	f.recordMu.Lock()
	defer f.recordMu.Unlock()
	return f.subscriptions
}

// wipe simulates the topology etcd losing the record.
func (f *fakeTopoStore) wipe() {
	f.recordMu.Lock()
	defer f.recordMu.Unlock()
	f.record = nil
}

func (f *fakeTopoStore) stored() *clustermetadatapb.Multipooler {
	f.recordMu.Lock()
	defer f.recordMu.Unlock()
	return f.record
}

func (f *fakeTopoStore) UpdateMultipoolerFields(_ context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.Multipooler) error) (*clustermetadatapb.Multipooler, error) {
	f.updateCalls.Add(1)
	f.lastUpdateID.Store(id)
	mp := &clustermetadatapb.Multipooler{Id: id}
	if err := update(mp); err != nil {
		return nil, err
	}
	f.lastUpdated.Store(mp)
	return mp, nil
}

func (f *fakeTopoStore) setError(err error) {
	f.err.Store(&err)
}

func (f *fakeTopoStore) clearError() {
	f.err.Store(nil)
}

func newTestPoolerProto(poolerType clustermetadatapb.PoolerType, status clustermetadatapb.PoolerServingStatus) *clustermetadatapb.Multipooler {
	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	mp := &clustermetadatapb.Multipooler{
		Id:            id,
		Type:          poolerType,
		ServingStatus: status,
	}
	// Keep the Type ⇔ SelfLeadership invariant so the record validates: a
	// PRIMARY names itself; any other type carries no self-leadership.
	if poolerType == clustermetadatapb.PoolerType_PRIMARY {
		mp.RoutingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY}
	}
	return mp
}

// newActionLockedCtx returns a context that satisfies actionlock.AssertActionLockHeld,
// and releases the lock automatically when the test ends.
func newActionLockedCtx(t *testing.T) context.Context {
	t.Helper()
	al := actionlock.NewActionLock()
	ctx, err := al.Acquire(t.Context(), "test")
	require.NoError(t, err)
	t.Cleanup(func() { al.Release(ctx) })
	return ctx
}

// --- publishIfNeeded unit tests (no goroutines, fully deterministic) ---

func TestPoolerRecord_PublishIfNeeded_WritesOnFirstCall(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	r.publishIfNeeded(t.Context())

	assert.Equal(t, int32(1), ts.calls.Load())
	seen := ts.lastSeen.Load()
	require.NotNil(t, seen)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, seen.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, seen.ServingStatus)
}

func TestPoolerRecord_PublishIfNeeded_NoopWhenStateUnchanged(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	r.publishIfNeeded(t.Context())
	require.Equal(t, int32(1), ts.calls.Load())

	// Same state again: should not write.
	r.publishIfNeeded(t.Context())
	assert.Equal(t, int32(1), ts.calls.Load(), "duplicate publish for unchanged state")
}

func TestPoolerRecord_PublishIfNeeded_WritesOnStateChange(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))

	r.publishIfNeeded(t.Context())
	require.Equal(t, int32(1), ts.calls.Load())

	require.NoError(t, r.Mutate(newActionLockedCtx(t), func(s *MutablePoolerRecordState) {
		s.RoutingState = primaryObs()
	}))
	r.publishIfNeeded(t.Context())
	assert.Equal(t, int32(2), ts.calls.Load())

	seen := ts.lastSeen.Load()
	require.NotNil(t, seen)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, seen.Type)
}

func TestPoolerRecord_PublishIfNeeded_RetriesAfterFailure(t *testing.T) {
	ts := &fakeTopoStore{}
	ts.setError(errors.New("etcd unavailable"))
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED))

	// First attempt fails.
	r.publishIfNeeded(t.Context())
	assert.Equal(t, int32(0), ts.calls.Load())

	// After the error clears, next attempt succeeds.
	ts.clearError()
	r.publishIfNeeded(t.Context())
	assert.Equal(t, int32(1), ts.calls.Load())
}

// --- Mutate behaviour ---

func TestPoolerRecord_Mutate_UpdatesDesiredAndSchedulesPublish(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED))

	require.NoError(t, r.Mutate(newActionLockedCtx(t), func(s *MutablePoolerRecordState) {
		s.RoutingState = primaryObs()
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	}))

	// Wakeup channel should be signalled.
	select {
	case <-r.wakeup:
	default:
		t.Fatal("Mutate did not signal wakeup channel")
	}

	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, r.ServingStatus())
}

func TestPoolerRecord_Mutate_RequiresActionLock(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED))

	err := r.Mutate(t.Context(), func(s *MutablePoolerRecordState) {
		s.RoutingState = primaryObs()
	})
	require.Error(t, err)

	// State must not have changed.
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, r.Type())
}

func TestPoolerRecord_Mutate_CoalescesPendingWakeups(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED))

	// Three back-to-back mutations with no consumer of the wakeup channel.
	// The size-1 buffer must absorb them without blocking.
	ctx := newActionLockedCtx(t)
	require.NoError(t, r.Mutate(ctx, func(s *MutablePoolerRecordState) {
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	}))
	require.NoError(t, r.Mutate(ctx, func(s *MutablePoolerRecordState) {
		s.RoutingState = primaryObs()
	}))
	require.NoError(t, r.Mutate(ctx, func(s *MutablePoolerRecordState) {
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	}))

	// Exactly one wakeup is pending — drain it.
	select {
	case <-r.wakeup:
	default:
		t.Fatal("expected one wakeup pending after Mutate")
	}
	// Channel is now empty.
	select {
	case <-r.wakeup:
		t.Fatal("expected at most one wakeup pending")
	default:
	}

	// The latest desired state is what publish should see.
	r.publishIfNeeded(t.Context())
	seen := ts.lastSeen.Load()
	require.NotNil(t, seen)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, seen.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, seen.ServingStatus)
}

func TestPoolerRecord_Snapshot_ReturnsClone(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	snap := r.Snapshot()
	snap.Type = clustermetadatapb.PoolerType_REPLICA

	// Mutating the returned snapshot must not affect the record.
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
}

func TestPoolerRecord_ImmutableAccessors(t *testing.T) {
	ts := &fakeTopoStore{}
	initial := newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)
	initial.PoolerDir = "/tmp/pooler"
	initial.PgDataDir = "/tmp/pgdata"
	initial.Hostname = "host.example.com"
	initial.PortMap = map[string]int32{"grpc": 15300, "postgres": 5432}
	initial.ShardRef = &clustermetadatapb.ShardRef{Database: "db", TableGroup: "tg", Shard: "0"}

	r := mustNewPoolerRecord(t, ts, initial)

	assert.Equal(t, "/tmp/pooler", r.PoolerDir())
	assert.Equal(t, "/tmp/pgdata", r.PgDataDir())
	assert.Equal(t, "host.example.com", r.Hostname())
	assert.Equal(t, int32(15300), r.Port("grpc"))
	assert.Equal(t, int32(5432), r.Port("postgres"))
	assert.Equal(t, int32(0), r.Port("unknown"))
	require.NotNil(t, r.ShardRef())
	assert.Equal(t, "tg", r.ShardRef().TableGroup)
	require.NotNil(t, r.Id())
	assert.Equal(t, "test-pooler", r.Id().Name)
}

// --- Goroutine integration tests (wakeup channel and ticker wiring) ---

// TestPoolerRecord_WakeupTriggersImmediatePublish verifies that Mutate
// triggers an immediate write without waiting for a ticker tick.
func TestPoolerRecord_WakeupTriggersImmediatePublish(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED))

	tickC := make(chan time.Time)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go r.publisherLoop(ctx, tickC)

	require.NoError(t, r.Mutate(newActionLockedCtx(t), func(s *MutablePoolerRecordState) {
		s.RoutingState = primaryObs()
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	}))

	require.Eventually(t, func() bool {
		return ts.calls.Load() == 1
	}, time.Second, time.Millisecond)

	seen := ts.lastSeen.Load()
	require.NotNil(t, seen)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, seen.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, seen.ServingStatus)
}

// TestPoolerRecord_TickerDrivesRetry verifies that a ticker signal retries a
// previously failed write without needing Mutate to be called again.
func TestPoolerRecord_TickerDrivesRetry(t *testing.T) {
	ts := &fakeTopoStore{}
	ts.setError(errors.New("etcd unavailable"))
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED))

	tickC := make(chan time.Time)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go r.publisherLoop(ctx, tickC)

	require.NoError(t, r.Mutate(newActionLockedCtx(t), func(s *MutablePoolerRecordState) {
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	}))

	// Wait for the goroutine to attempt (and fail) the wakeup-triggered write.
	require.Eventually(t, func() bool {
		return ts.attempts.Load() >= 1
	}, time.Second, time.Millisecond)
	assert.Equal(t, int32(0), ts.calls.Load())

	// Clear the error and fire a ticker tick; the retry should succeed.
	ts.clearError()
	tickC <- time.Time{}

	require.Eventually(t, func() bool {
		return ts.calls.Load() >= 1
	}, time.Second, time.Millisecond)
}

func TestPoolerRecord_PublisherLoop_ExitsOnContextCancel(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED))

	tickC := make(chan time.Time)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		r.publisherLoop(ctx, tickC)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publisherLoop did not exit after context cancellation")
	}
}

// TestPoolerRecord_RegisterAndUnregister verifies that Register triggers
// the initial topology write and Unregister applies its finalize callback
// and surfaces the final state via the publisher's RegisterMultipooler
// path. The record itself is agnostic about what "shutdown state" means —
// it's the caller's finalize callback that stamps the shutdown type.
func TestPoolerRecord_RegisterAndUnregister(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	r.Register(t.Context(), func(string) {})

	// Wait for the initial registration to succeed.
	require.Eventually(t, func() bool {
		return ts.calls.Load() >= 1
	}, time.Second, time.Millisecond)

	// Mirror the production shutdown finalize (StopTopoRegistration): a leader
	// stepping down clears its routing_state and marks the lifecycle SHUTDOWN, so
	// the published Type derives to UNKNOWN.
	r.Unregister(t.Context(), func(s *MutablePoolerRecordState) {
		s.RoutingState = nil
		s.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN}
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	})

	// The final publish should carry whatever state finalize stamped.
	seen := ts.lastSeen.Load()
	require.NotNil(t, seen)
	assert.Equal(t, clustermetadatapb.PoolerType_UNKNOWN, seen.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, seen.ServingStatus)
}

// TestPoolerRecord_Unregister_NoFinalize verifies that Unregister with a nil
// finalize callback still cancels the publisher and toporeg goroutines and
// publishes any state the caller wrote via Mutate beforehand.
func TestPoolerRecord_Unregister_NoFinalize(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED))
	r.Register(t.Context(), func(string) {})

	// Mutate to PRIMARY before Unregister.
	require.NoError(t, r.Mutate(newActionLockedCtx(t), func(s *MutablePoolerRecordState) {
		s.RoutingState = primaryObs()
	}))

	r.Unregister(t.Context(), nil)

	seen := ts.lastSeen.Load()
	require.NotNil(t, seen)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, seen.Type)
}

// TestPoolerRecord_DerivesTypeFromRoutingState verifies the published PoolerType
// is a pure projection of routing_state + lifecycle (see typeForState): callers
// set routing_state, never Type. A PRIMARY routing_state publishes Type PRIMARY;
// clearing it publishes REPLICA; a SHUTDOWN lifecycle publishes UNKNOWN.
func TestPoolerRecord_DerivesTypeFromRoutingState(t *testing.T) {
	t.Run("routing_state PRIMARY publishes Type PRIMARY", func(t *testing.T) {
		r := mustNewPoolerRecord(t, &fakeTopoStore{}, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
		require.NoError(t, r.Mutate(newActionLockedCtx(t), func(s *MutablePoolerRecordState) {
			s.RoutingState = primaryObs()
		}))
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
		require.NotNil(t, r.RoutingState())
		assert.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, r.RoutingState().GetRole())
	})

	t.Run("no routing_state publishes Type REPLICA", func(t *testing.T) {
		r := mustNewPoolerRecord(t, &fakeTopoStore{}, newTestPoolerProto(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
		require.NoError(t, r.Mutate(newActionLockedCtx(t), func(s *MutablePoolerRecordState) {
			s.RoutingState = nil
		}))
		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, r.Type())
		assert.Nil(t, r.RoutingState())
	})

	t.Run("SHUTDOWN lifecycle publishes Type UNKNOWN", func(t *testing.T) {
		r := mustNewPoolerRecord(t, &fakeTopoStore{}, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
		require.NoError(t, r.Mutate(newActionLockedCtx(t), func(s *MutablePoolerRecordState) {
			s.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN}
		}))
		assert.Equal(t, clustermetadatapb.PoolerType_UNKNOWN, r.Type())
	})
}

// --- Record restoration after the topology connection is regained (MUL-1924) ---

// startRecoveryLoop runs the recovery loop with a near-zero backoff and waits
// until it has subscribed to the connection, so a recovery fired afterwards is
// never missed.
func startRecoveryLoop(t *testing.T, r *poolerRecord, ts *fakeTopoStore) {
	t.Helper()
	base, maxDelay := topoRecoveryBackoffBase, topoRecoveryBackoffMax
	topoRecoveryBackoffBase, topoRecoveryBackoffMax = time.Millisecond, 5*time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.recoveryLoop(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		topoRecoveryBackoffBase, topoRecoveryBackoffMax = base, maxDelay
	})
	require.Eventually(t, func() bool { return ts.subscriptionCount() == 1 }, time.Second, time.Millisecond)
}

func publishedRecord(t *testing.T, ts *fakeTopoStore, r *poolerRecord) {
	t.Helper()
	r.publishIfNeeded(t.Context())
	require.NotNil(t, ts.stored())
}

func TestPoolerRecord_Recovery_RecreatesWipedRecord(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	publishedRecord(t, ts, r)
	startRecoveryLoop(t, r, ts)

	ts.wipe()
	ts.recoverConn()

	require.Eventually(t, func() bool { return ts.stored() != nil }, time.Second, time.Millisecond, "record not recreated after recovery")
	got := ts.stored()
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, got.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, got.ServingStatus)
	assert.Equal(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, got.GetRoutingState().GetRole())
}

// The point of the design: without a lost-and-regained connection nothing
// touches the topology server, however long the record stays missing.
func TestPoolerRecord_NoRestoreWithoutRecovery(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
	publishedRecord(t, ts, r)
	startRecoveryLoop(t, r, ts)
	tickC := make(chan time.Time)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go r.publisherLoop(ctx, tickC)

	ts.wipe()
	tickC <- time.Time{}
	tickC <- time.Time{}

	testpoll.Never(t, func() bool { return ts.stored() != nil || ts.createCount() > 0 }, 50*time.Millisecond, 5*time.Millisecond)
}

func TestPoolerRecord_Recovery_LeavesExistingRecordUntouched(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	publishedRecord(t, ts, r)
	startRecoveryLoop(t, r, ts)

	newer := newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)
	newer.Hostname = "newer-registration"
	ts.setRecord(newer)
	ts.recoverConn()

	testpoll.Never(t, func() bool { return !proto.Equal(newer, ts.stored()) || ts.createCount() > 0 }, 50*time.Millisecond, 5*time.Millisecond)
	assert.Equal(t, int32(1), ts.calls.Load(), "recovery must not re-register an existing record")
}

func TestPoolerRecord_Recovery_ToleratesNodeExistsOnCreate(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	publishedRecord(t, ts, r)
	startRecoveryLoop(t, r, ts)

	newer := newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)
	newer.Hostname = "raced-registration"
	ts.beforeCreate = func() { ts.setRecord(newer) }
	ts.wipe()
	ts.recoverConn()

	require.Eventually(t, func() bool { return ts.createCount() == 1 }, time.Second, time.Millisecond)
	testpoll.Never(t, func() bool { return !proto.Equal(newer, ts.stored()) || ts.createCount() > 1 }, 50*time.Millisecond, 5*time.Millisecond)
	assert.Equal(t, int32(1), ts.calls.Load())
}

func TestPoolerRecord_Recovery_RetriesFailedCheck(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
	publishedRecord(t, ts, r)
	startRecoveryLoop(t, r, ts)

	ts.wipe()
	ts.recordMu.Lock()
	ts.getFailures = 3
	ts.recordMu.Unlock()
	ts.recoverConn()

	require.Eventually(t, func() bool { return ts.stored() != nil }, time.Second, time.Millisecond, "record not restored after transient read errors")
}

func TestPoolerRecord_Recovery_MissingCellIsReportedAndRetried(t *testing.T) {
	ts := &fakeTopoStore{}
	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	r, err := newPoolerRecord(logger, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
	require.NoError(t, err)
	publishedRecord(t, ts, r)
	startRecoveryLoop(t, r, ts)

	// With the global etcd wiped, the cell lookup fails for the read and the
	// create alike, so both report NoNode.
	ts.wipe()
	ts.recordMu.Lock()
	ts.createErr = mterrors.Wrapf(topoclient.NewError(topoclient.NoNode, "cells/zone1"), "failed to look up cell %q definition from global topology", "zone1")
	ts.recordMu.Unlock()
	ts.recoverConn()

	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "cell definition missing from global topology")
	}, time.Second, time.Millisecond, "missing cell not reported")
	require.NotContains(t, logs.String(), "failed to restore missing multipooler record")
	require.Nil(t, ts.stored())

	ts.recordMu.Lock()
	ts.createErr = nil
	ts.recordMu.Unlock()
	require.Eventually(t, func() bool { return ts.stored() != nil }, time.Second, time.Millisecond, "record not restored once the cell came back")
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestPoolerRecord_Recovery_FollowsReplacedConnection(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
	publishedRecord(t, ts, r)
	startRecoveryLoop(t, r, ts)

	// A replaced connection points at a server that may not have the record,
	// so the replacement alone triggers a check.
	ts.wipe()
	ts.replaceConn()

	require.Eventually(t, func() bool { return ts.stored() != nil }, 5*time.Second, time.Millisecond, "record not restored after the connection was replaced")
	require.Eventually(t, func() bool { return ts.subscriptionCount() == 2 }, 5*time.Second, time.Millisecond)

	ts.wipe()
	ts.recoverConn()
	require.Eventually(t, func() bool { return ts.stored() != nil }, time.Second, time.Millisecond, "recovery on the new connection was missed")
}

func TestPoolerRecord_Recovery_DoesNotRestoreWhenShuttingDown(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
	publishedRecord(t, ts, r)
	startRecoveryLoop(t, r, ts)

	r.applyMutation(func(s *MutablePoolerRecordState) {
		s.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN}
	})
	r.publishIfNeeded(t.Context())
	ts.wipe()
	ts.recoverConn()

	testpoll.Never(t, func() bool { return ts.stored() != nil || ts.createCount() > 0 }, 50*time.Millisecond, 5*time.Millisecond)
}

func TestPoolerRecord_NoRestoreAfterUnregister(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
	r.Register(t.Context(), func(string) {})
	require.Eventually(t, func() bool { return ts.calls.Load() >= 1 && ts.subscriptionCount() >= 1 }, time.Second, time.Millisecond)

	r.Unregister(t.Context(), nil)
	ts.wipe()
	ts.recoverConn()

	testpoll.Never(t, func() bool { return ts.stored() != nil || ts.createCount() > 0 }, 50*time.Millisecond, 5*time.Millisecond)
}

// The newer-registration fence for this record lives in the operator and orch writers (MUL-1536); the pooler must not race them.
func TestPoolerRecord_Recovery_LeavesOperatorMarkedShutdownRecordUntouched(t *testing.T) {
	ts := &fakeTopoStore{}
	r := mustNewPoolerRecord(t, ts, newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
	publishedRecord(t, ts, r)
	startRecoveryLoop(t, r, ts)

	marked := newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING)
	marked.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{
		Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
		Reason: "dead pooler",
	}
	ts.setRecord(marked)
	ts.recoverConn()

	testpoll.Never(t, func() bool { return !proto.Equal(marked, ts.stored()) || ts.createCount() > 0 }, 50*time.Millisecond, 5*time.Millisecond)
	assert.Equal(t, int32(1), ts.calls.Load(), "recovery must not re-register over a SHUTDOWN record")
}
