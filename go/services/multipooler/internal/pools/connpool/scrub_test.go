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

package connpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
)

// scrubMockConnection is a mockConnection carrying an injectable per-conn
// verdict that mockChecker reports.
type scrubMockConnection struct {
	mockConnection
	div           Divergence
	verifyErr     error
	closeOnVerify bool // simulate the probe killing a dead socket
	verifyCalls   atomic.Int64
}

// mockChecker is a registered ConnChecker that reports whatever verdict the
// probed connection carries.
type mockChecker struct{ name string }

func (c mockChecker) Name() string {
	if c.name == "" {
		return "mock"
	}
	return c.name
}

func (mockChecker) Check(ctx context.Context, m *scrubMockConnection) (Divergence, error) {
	m.verifyCalls.Add(1)
	if m.closeOnVerify {
		m.closed.Store(true)
	}
	return m.div, m.verifyErr
}

func newScrubTestPool(t *testing.T, capacity int64, connect Connector[*scrubMockConnection]) *Pool[*scrubMockConnection] {
	t.Helper()
	pool := NewPool[*scrubMockConnection](context.Background(), &Config{
		Name:         "scrub-test",
		Capacity:     capacity,
		MaxIdleCount: capacity,
	})
	pool.RegisterChecker(mockChecker{})
	if connect == nil {
		connect = func(ctx context.Context, poolCtx context.Context) (*scrubMockConnection, error) {
			return &scrubMockConnection{}, nil
		}
	}
	pool.Open(connect, nil)
	t.Cleanup(pool.Close)
	return pool
}

// recycleIdle gets one connection and returns it to the idle stacks.
func recycleIdle(t *testing.T, pool *Pool[*scrubMockConnection], settings *connstate.Settings) *scrubMockConnection {
	t.Helper()
	pooled, err := pool.GetWithSettings(context.Background(), settings)
	require.NoError(t, err)
	conn := pooled.Conn
	pooled.Recycle()
	return conn
}

func TestScrubCleanConnReturnsToPool(t *testing.T) {
	pool := newScrubTestPool(t, 2, nil)
	conn := recycleIdle(t, pool, nil)

	cursor := 0
	assert.True(t, pool.scrubOne(&cursor))

	assert.EqualValues(t, 1, conn.verifyCalls.Load())
	assert.EqualValues(t, 1, pool.Metrics.ScrubCheckedCount())
	assert.EqualValues(t, 0, pool.Metrics.ScrubDivergentCount())
	assert.False(t, conn.IsClosed())

	// The same connection is handed out again.
	pooled, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Same(t, conn, pooled.Conn)
	pooled.Recycle()
}

func TestScrubDivergentConnReplaced(t *testing.T) {
	pool := newScrubTestPool(t, 2, nil)
	conn := recycleIdle(t, pool, nil)
	conn.div = Divergence{Untracked: []string{"work_mem"}}

	cursor := 0
	assert.True(t, pool.scrubOne(&cursor))

	assert.True(t, conn.IsClosed(), "divergent backend must be closed")
	assert.EqualValues(t, 1, pool.Metrics.ScrubDivergentCount())
	assert.EqualValues(t, 1, pool.Active(), "replacement must keep the slot accounted")

	// The next borrower gets the replacement, never the divergent backend.
	pooled, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.NotSame(t, conn, pooled.Conn)
	assert.False(t, pooled.Conn.IsClosed())
	pooled.Recycle()
}

func TestScrubDivergentConnInSettingsStack(t *testing.T) {
	pool := newScrubTestPool(t, 2, nil)
	settings := connstate.NewSettings(map[string]string{"work_mem": "64MB"}, 3)
	conn := recycleIdle(t, pool, settings)
	conn.div = Divergence{Mismatched: []string{"work_mem"}}

	// One scrub pass finds the connection regardless of which settings
	// bucket it sits in.
	cursor := 0
	assert.True(t, pool.scrubOne(&cursor))

	assert.True(t, conn.IsClosed())
	assert.EqualValues(t, 1, pool.Metrics.ScrubDivergentCount())
	assert.EqualValues(t, 1, pool.Active())
}

func TestScrubProbeErrorKeepsLiveConn(t *testing.T) {
	pool := newScrubTestPool(t, 2, nil)
	conn := recycleIdle(t, pool, nil)
	conn.verifyErr = errors.New("probe timeout")

	cursor := 0
	assert.True(t, pool.scrubOne(&cursor))

	assert.False(t, conn.IsClosed(), "a live conn is not punished for a probe failure")
	assert.EqualValues(t, 1, pool.Metrics.ScrubErrorCount())
	assert.EqualValues(t, 0, pool.Metrics.ScrubDivergentCount())

	pooled, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Same(t, conn, pooled.Conn)
	pooled.Recycle()
}

func TestScrubProbeErrorOnDeadConnReplaces(t *testing.T) {
	pool := newScrubTestPool(t, 2, nil)
	conn := recycleIdle(t, pool, nil)
	conn.verifyErr = errors.New("connection reset")
	conn.closeOnVerify = true

	cursor := 0
	assert.True(t, pool.scrubOne(&cursor))

	assert.EqualValues(t, 1, pool.Metrics.ScrubErrorCount())
	assert.EqualValues(t, 1, pool.Active(), "dead conn's slot must be freed and replaced")

	pooled, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.NotSame(t, conn, pooled.Conn)
	pooled.Recycle()
}

func TestScrubPreservesIdleClock(t *testing.T) {
	pool := newScrubTestPool(t, 2, nil)
	recycleIdle(t, pool, nil)

	// Read the idle stamp without borrowing it, then put the conn back.
	pooled, ok := pool.clean.Pop()
	require.True(t, ok)
	stamp := pooled.timeUsed.get()
	pool.clean.Push(pooled)

	cursor := 0
	assert.True(t, pool.scrubOne(&cursor))

	// Scrubbing must not refresh the idle clock, or small pools would never
	// shrink via idle timeout.
	scrubbed, ok := pool.clean.Pop()
	require.True(t, ok)
	assert.Same(t, pooled, scrubbed)
	assert.Equal(t, stamp, scrubbed.timeUsed.get())
	pool.clean.Push(scrubbed)
}

func TestScrubEmptyPoolNoop(t *testing.T) {
	pool := newScrubTestPool(t, 2, nil)
	cursor := 0
	assert.True(t, pool.scrubOne(&cursor))
	assert.EqualValues(t, 0, pool.Metrics.ScrubCheckedCount())
}

func TestScrubNoopWithoutCheckers(t *testing.T) {
	// A pool with no registered checkers has nothing to verify: scrubOne
	// must not touch any connection (and open() never starts the worker).
	pool := newTestPool(2)
	defer pool.Close()

	pooled, err := pool.Get(context.Background())
	require.NoError(t, err)
	conn := pooled.Conn
	pooled.Recycle()

	cursor := 0
	assert.True(t, pool.scrubOne(&cursor))
	assert.EqualValues(t, 0, pool.Metrics.ScrubCheckedCount())

	got, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Same(t, conn, got.Conn)
	got.Recycle()
}

func TestScrubRunsAllRegisteredCheckers(t *testing.T) {
	// Findings from multiple checkers merge onto one replacement, and a
	// checker error after an earlier checker's finding still fails closed.
	pool := NewPool[*scrubMockConnection](context.Background(), &Config{
		Name:         "scrub-multi-test",
		Capacity:     2,
		MaxIdleCount: 2,
	})
	pool.RegisterChecker(mockChecker{name: "first"})
	pool.RegisterChecker(erroringChecker{})
	pool.Open(func(ctx context.Context, poolCtx context.Context) (*scrubMockConnection, error) {
		return &scrubMockConnection{}, nil
	}, nil)
	defer pool.Close()

	conn := recycleIdle(t, pool, nil)
	conn.div = Divergence{Untracked: []string{"work_mem"}}

	cursor := 0
	assert.True(t, pool.scrubOne(&cursor))

	assert.EqualValues(t, 1, conn.verifyCalls.Load(), "first checker ran")
	assert.True(t, conn.IsClosed(), "finding before the error must still replace the backend")
	assert.EqualValues(t, 1, pool.Metrics.ScrubDivergentCount())
	assert.EqualValues(t, 1, pool.Metrics.ScrubErrorCount())
	assert.EqualValues(t, 1, pool.Active())
}

// erroringChecker always fails to produce a verdict.
type erroringChecker struct{}

func (erroringChecker) Name() string { return "erroring" }
func (erroringChecker) Check(ctx context.Context, m *scrubMockConnection) (Divergence, error) {
	return Divergence{}, errors.New("no verdict")
}

func TestScrubWorkerReplacesDivergentConn(t *testing.T) {
	// End-to-end through the background worker: a divergent idle connection
	// is detected and replaced without any Get/Recycle traffic.
	var created atomic.Int64
	pool := NewPool[*scrubMockConnection](context.Background(), &Config{
		Name:          "scrub-worker-test",
		Capacity:      2,
		MaxIdleCount:  2,
		ScrubInterval: 10 * time.Millisecond,
	})
	pool.RegisterChecker(mockChecker{})
	pool.Open(func(ctx context.Context, poolCtx context.Context) (*scrubMockConnection, error) {
		conn := &scrubMockConnection{}
		if created.Add(1) == 1 {
			// Only the first connection carries hidden session state.
			conn.div = Divergence{Untracked: []string{"work_mem"}}
		}
		return conn, nil
	}, nil)
	defer pool.Close()

	first, err := pool.Get(context.Background())
	require.NoError(t, err)
	divergent := first.Conn
	first.Recycle()

	require.Eventually(t, func() bool {
		return pool.Metrics.ScrubDivergentCount() == 1 && divergent.IsClosed()
	}, 5*time.Second, 5*time.Millisecond, "scrub worker must replace the divergent backend")
	assert.EqualValues(t, 1, pool.Active())
}
