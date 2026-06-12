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

package connpoolmanager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDrainTestManager() *Manager {
	m := &Manager{}
	zeroCh := make(chan struct{})
	close(zeroCh)
	m.zeroCh = zeroCh
	reservedZeroCh := make(chan struct{})
	close(reservedZeroCh)
	m.reservedZeroCh = reservedZeroCh
	return m
}

func TestRegularAdd_CounterAndChannel(t *testing.T) {
	m := newDrainTestManager()

	// Initially at zero, channel should be closed (drained)
	select {
	case <-m.zeroCh:
		// expected: channel is closed
	default:
		t.Fatal("expected zeroCh to be closed at zero count")
	}

	// Add one: should create a new open channel
	m.regularAdd(1)
	assert.Equal(t, int64(1), m.regularCount)
	select {
	case <-m.zeroCh:
		t.Fatal("expected zeroCh to be open when count > 0")
	default:
		// expected: channel is open
	}

	// Add another
	m.regularAdd(1)
	assert.Equal(t, int64(2), m.regularCount)

	// Return one: still > 0, channel should still be open
	m.regularAdd(-1)
	assert.Equal(t, int64(1), m.regularCount)
	select {
	case <-m.zeroCh:
		t.Fatal("expected zeroCh to be open when count > 0")
	default:
		// expected
	}

	// Return last one: back to zero, channel should be closed
	m.regularAdd(-1)
	assert.Equal(t, int64(0), m.regularCount)
	select {
	case <-m.zeroCh:
		// expected: channel is closed
	default:
		t.Fatal("expected zeroCh to be closed when count reaches 0")
	}
}

func TestWaitForDrain_ImmediateWhenZero(t *testing.T) {
	m := newDrainTestManager()

	// Should return immediately since count is already zero
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := m.WaitForDrain(ctx)
	require.NoError(t, err)
}

func TestWaitForDrain_BlocksUntilZero(t *testing.T) {
	m := newDrainTestManager()

	// Simulate a lent connection
	m.regularAdd(1)

	var wg sync.WaitGroup
	var drainErr error

	wg.Go(func() {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		drainErr = m.WaitForDrain(ctx)
	})

	// Give the goroutine time to start waiting
	time.Sleep(10 * time.Millisecond)

	// Return the connection
	m.regularAdd(-1)

	wg.Wait()
	require.NoError(t, drainErr)
}

func TestWaitForDrain_RespectsContextCancellation(t *testing.T) {
	m := newDrainTestManager()

	// Simulate a lent connection that never returns
	m.regularAdd(1)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := m.WaitForDrain(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestWaitForReservedDrain_IgnoresRegularBorrows is the crux of the two-stage
// drain: regular (single-query) borrows must NOT hold up the reserved-only wait,
// so stage 1 can complete while single queries keep flowing.
func TestWaitForReservedDrain_IgnoresRegularBorrows(t *testing.T) {
	m := newDrainTestManager()

	// A regular borrow bumps the regular count but not the reserved count.
	m.regularAdd(1)
	assert.Equal(t, int64(1), m.regularCount)
	assert.Equal(t, int64(0), m.reservedCount)

	// WaitForReservedDrain returns immediately despite the in-flight regular borrow.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, m.WaitForReservedDrain(ctx))

	// WaitForDrain, by contrast, still blocks on the regular borrow (combined > 0).
	require.Error(t, m.WaitForDrain(ctx))

	m.regularAdd(-1)
}

// TestSignalZeroChan_ReArmsAcrossCycles drives the combined drain channel
// through 0 → positive → 0 → positive with mixed regular and reserved adds,
// confirming zeroCh closes at zero and re-arms (a fresh open channel) when the
// total goes positive again.
func TestSignalZeroChan_ReArmsAcrossCycles(t *testing.T) {
	m := newDrainTestManager()

	isClosed := func(ch chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}

	// Starts drained.
	assert.True(t, isClosed(m.zeroCh), "zeroCh should start closed")

	// Cycle 1: a regular borrow and a reserved conn both lift the combined total.
	m.regularAdd(1)
	assert.False(t, isClosed(m.zeroCh), "combined open after regular borrow")
	m.reservedAdd(1)
	assert.False(t, isClosed(m.zeroCh), "combined still open with both")
	assert.False(t, isClosed(m.reservedZeroCh), "reserved open with a reserved conn")

	// Release the regular borrow: combined still > 0 (reserved remains).
	m.regularAdd(-1)
	assert.False(t, isClosed(m.zeroCh), "combined still open while reserved > 0")

	// Release the reserved conn: both reach zero and close.
	m.reservedAdd(-1)
	assert.True(t, isClosed(m.zeroCh), "combined closed when total reaches zero")
	assert.True(t, isClosed(m.reservedZeroCh), "reserved closed when reserved reaches zero")

	// Cycle 2: the channel must re-arm — a new borrow re-opens the closed zeroCh.
	m.regularAdd(1)
	assert.False(t, isClosed(m.zeroCh), "zeroCh must re-arm (re-open) on 0→positive")
	m.regularAdd(-1)
	assert.True(t, isClosed(m.zeroCh), "zeroCh closes again at zero")
}

// TestWaitForReservedDrain_BlocksUntilReservedZero verifies a reserved
// connection (a single reservedAdd) holds the reserved wait open and also keeps
// the combined drain non-zero.
func TestWaitForReservedDrain_BlocksUntilReservedZero(t *testing.T) {
	m := newDrainTestManager()

	// onReserve is a single reservedAdd; the combined total is regular + reserved.
	m.reservedAdd(1)
	assert.Equal(t, int64(1), m.reservedCount)
	assert.Equal(t, int64(0), m.regularCount)

	var wg sync.WaitGroup
	var drainErr error
	wg.Go(func() {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		drainErr = m.WaitForReservedDrain(ctx)
	})

	time.Sleep(10 * time.Millisecond)
	// onRelease.
	m.reservedAdd(-1)

	wg.Wait()
	require.NoError(t, drainErr)
	assert.Equal(t, int64(0), m.reservedCount)
	// Combined drain is also satisfied now.
	require.NoError(t, m.WaitForDrain(t.Context()))
}
