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
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
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
}

func (f *fakeTopoStore) RegisterMultipooler(_ context.Context, mp *clustermetadatapb.Multipooler, _ bool) error {
	f.attempts.Add(1)
	if ep := f.err.Load(); ep != nil {
		return *ep
	}
	f.calls.Add(1)
	f.lastSeen.Store(mp)
	return nil
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
	initial.ShardKey = &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	r := mustNewPoolerRecord(t, ts, initial)

	assert.Equal(t, "/tmp/pooler", r.PoolerDir())
	assert.Equal(t, "/tmp/pgdata", r.PgDataDir())
	assert.Equal(t, "host.example.com", r.Hostname())
	assert.Equal(t, int32(15300), r.Port("grpc"))
	assert.Equal(t, int32(5432), r.Port("postgres"))
	assert.Equal(t, int32(0), r.Port("unknown"))
	require.NotNil(t, r.ShardKey())
	assert.Equal(t, "tg", r.ShardKey().TableGroup)
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
