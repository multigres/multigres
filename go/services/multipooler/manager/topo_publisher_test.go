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
	calls    atomic.Int32
	err      atomic.Pointer[error]
	lastSeen atomic.Pointer[clustermetadatapb.MultiPooler]
}

func (f *fakeRegistrar) RegisterMultiPooler(_ context.Context, mp *clustermetadatapb.MultiPooler, _ bool) error {
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

func TestTopoPublisher_NotifyTriggersWrite(t *testing.T) {
	reg := &fakeRegistrar{}
	tp := newTopoPublisher(newTestLogger(), reg)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go tp.Run(ctx)

	mp := newTestPooler(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
	tp.Notify(mp)

	require.Eventually(t, func() bool {
		return reg.calls.Load() == 1
	}, time.Second, 5*time.Millisecond)

	seen := reg.lastSeen.Load()
	require.NotNil(t, seen)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, seen.Type)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, seen.ServingStatus)
}

func TestTopoPublisher_RetriesAfterFailure(t *testing.T) {
	reg := &fakeRegistrar{}
	reg.setError(errors.New("etcd unavailable"))

	tp := newTopoPublisher(newTestLogger(), reg)
	// Use a short retry interval so the test doesn't wait 30 seconds.
	tp.retryInterval = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go tp.Run(ctx)

	mp := newTestPooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_NOT_SERVING)
	tp.Notify(mp)

	// Allow some time for at least one failed attempt.
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, int32(0), reg.calls.Load(), "expected no successful writes while erroring")

	// Clear the error; the periodic ticker should now succeed.
	reg.clearError()

	require.Eventually(t, func() bool {
		return reg.calls.Load() >= 1
	}, time.Second, 5*time.Millisecond)
}

func TestTopoPublisher_NoDuplicateWrites(t *testing.T) {
	reg := &fakeRegistrar{}
	tp := newTopoPublisher(newTestLogger(), reg)
	tp.retryInterval = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go tp.Run(ctx)

	mp := newTestPooler(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
	tp.Notify(mp)

	// Wait for the first write to succeed.
	require.Eventually(t, func() bool {
		return reg.calls.Load() == 1
	}, time.Second, 5*time.Millisecond)

	// Let a few ticker intervals pass — state hasn't changed, so no more writes.
	time.Sleep(60 * time.Millisecond)
	assert.Equal(t, int32(1), reg.calls.Load(), "expected no additional writes for unchanged state")
}

func TestTopoPublisher_SecondNotifyWritesNewState(t *testing.T) {
	reg := &fakeRegistrar{}
	tp := newTopoPublisher(newTestLogger(), reg)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go tp.Run(ctx)

	// First transition: REPLICA SERVING
	tp.Notify(newTestPooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
	require.Eventually(t, func() bool { return reg.calls.Load() == 1 }, time.Second, 5*time.Millisecond)

	// Second transition: PRIMARY SERVING
	tp.Notify(newTestPooler(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	require.Eventually(t, func() bool { return reg.calls.Load() == 2 }, time.Second, 5*time.Millisecond)

	seen := reg.lastSeen.Load()
	require.NotNil(t, seen)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, seen.Type)
}

func TestTopoPublisher_RunExitsOnContextCancel(t *testing.T) {
	reg := &fakeRegistrar{}
	tp := newTopoPublisher(newTestLogger(), reg)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		tp.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}
}
