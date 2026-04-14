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
)

// fakeRegistrar is a minimal topoRegistrar for testing.
type fakeRegistrar struct {
	attempts atomic.Int32 // incremented on every RegisterMultiPooler call, success or failure
	calls    atomic.Int32 // incremented only on successful calls
	err      atomic.Pointer[error]
	lastSeen atomic.Pointer[clustermetadatapb.MultiPooler]
}

func (f *fakeRegistrar) RegisterMultiPooler(_ context.Context, mp *clustermetadatapb.MultiPooler, _ bool) error {
	f.attempts.Add(1)
	if ep := f.err.Load(); ep != nil {
		return *ep
	}
	f.calls.Add(1)
	f.lastSeen.Store(mp)
	return nil
}

func (f *fakeRegistrar) setError(err error) {
	f.err.Store(&err)
}

func (f *fakeRegistrar) clearError() {
	f.err.Store(nil)
}

func newTestPooler(poolerType clustermetadatapb.PoolerType, status clustermetadatapb.PoolerServingStatus) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-pooler",
		},
		Type:          poolerType,
		ServingStatus: status,
	}
}

// --- publishIfNeeded unit tests (no goroutines, fully deterministic) ---

func TestTopoPublisher_PublishIfNeeded_WritesOnFirstCall(t *testing.T) {
	reg := &fakeRegistrar{}
	tp := newTopoPublisher(newTestLogger(), reg)

	tp.Notify(newTestPooler(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	tp.publishIfNeeded(t.Context())

	assert.Equal(t, int32(1), reg.calls.Load())
	seen := reg.lastSeen.Load()
	require.NotNil(t, seen)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, seen.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, seen.ServingStatus)
}

func TestTopoPublisher_PublishIfNeeded_NoopWhenStateUnchanged(t *testing.T) {
	reg := &fakeRegistrar{}
	tp := newTopoPublisher(newTestLogger(), reg)

	tp.Notify(newTestPooler(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	tp.publishIfNeeded(t.Context())
	require.Equal(t, int32(1), reg.calls.Load())

	// Same state again: should not write.
	tp.publishIfNeeded(t.Context())
	assert.Equal(t, int32(1), reg.calls.Load(), "duplicate publish for unchanged state")
}

func TestTopoPublisher_PublishIfNeeded_WritesOnStateChange(t *testing.T) {
	reg := &fakeRegistrar{}
	tp := newTopoPublisher(newTestLogger(), reg)

	tp.Notify(newTestPooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
	tp.publishIfNeeded(t.Context())
	require.Equal(t, int32(1), reg.calls.Load())

	tp.Notify(newTestPooler(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	tp.publishIfNeeded(t.Context())
	assert.Equal(t, int32(2), reg.calls.Load())

	seen := reg.lastSeen.Load()
	require.NotNil(t, seen)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, seen.Type)
}

func TestTopoPublisher_PublishIfNeeded_RetriesAfterFailure(t *testing.T) {
	reg := &fakeRegistrar{}
	reg.setError(errors.New("etcd unavailable"))
	tp := newTopoPublisher(newTestLogger(), reg)

	tp.Notify(newTestPooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_NOT_SERVING))

	// First attempt fails.
	tp.publishIfNeeded(t.Context())
	assert.Equal(t, int32(0), reg.calls.Load())

	// After the error clears, next attempt succeeds.
	reg.clearError()
	tp.publishIfNeeded(t.Context())
	assert.Equal(t, int32(1), reg.calls.Load())
}

func TestTopoPublisher_PublishIfNeeded_NoopWhenNoDesiredState(t *testing.T) {
	reg := &fakeRegistrar{}
	tp := newTopoPublisher(newTestLogger(), reg)

	tp.publishIfNeeded(t.Context())
	assert.Equal(t, int32(0), reg.calls.Load())
	assert.Equal(t, int32(0), reg.attempts.Load())
}

// --- Goroutine integration tests (wakeup channel and ticker wiring) ---

// TestTopoPublisher_NotifyWakesGoroutine verifies that Notify triggers an
// immediate write without waiting for a ticker tick.
func TestTopoPublisher_NotifyWakesGoroutine(t *testing.T) {
	reg := &fakeRegistrar{}
	tp := newTopoPublisher(newTestLogger(), reg)

	// tickC is never sent to in this test — the wakeup channel does all the work.
	tickC := make(chan time.Time)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go tp.run(ctx, tickC)

	tp.Notify(newTestPooler(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	require.Eventually(t, func() bool {
		return reg.calls.Load() == 1
	}, time.Second, time.Millisecond)

	seen := reg.lastSeen.Load()
	require.NotNil(t, seen)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, seen.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, seen.ServingStatus)
}

// TestTopoPublisher_TickerDrivesRetry verifies that a ticker signal retries a
// previously failed write without needing Notify to be called again.
func TestTopoPublisher_TickerDrivesRetry(t *testing.T) {
	reg := &fakeRegistrar{}
	reg.setError(errors.New("etcd unavailable"))
	tp := newTopoPublisher(newTestLogger(), reg)

	tickC := make(chan time.Time)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go tp.run(ctx, tickC)

	tp.Notify(newTestPooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_NOT_SERVING))

	// Wait for the goroutine to attempt (and fail) the wakeup-triggered write.
	require.Eventually(t, func() bool {
		return reg.attempts.Load() >= 1
	}, time.Second, time.Millisecond)
	assert.Equal(t, int32(0), reg.calls.Load())

	// Clear the error and fire a ticker tick; the retry should succeed.
	reg.clearError()
	tickC <- time.Time{}

	require.Eventually(t, func() bool {
		return reg.calls.Load() >= 1
	}, time.Second, time.Millisecond)
}

func TestTopoPublisher_RunExitsOnContextCancel(t *testing.T) {
	reg := &fakeRegistrar{}
	tp := newTopoPublisher(newTestLogger(), reg)

	tickC := make(chan time.Time)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		tp.run(ctx, tickC)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run did not exit after context cancellation")
	}
}
