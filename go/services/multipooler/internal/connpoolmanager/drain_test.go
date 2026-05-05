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
	return m
}

func TestLentAdd_CounterAndChannel(t *testing.T) {
	m := newDrainTestManager()

	// Initially at zero, channel should be closed (drained)
	select {
	case <-m.zeroCh:
		// expected: channel is closed
	default:
		t.Fatal("expected zeroCh to be closed at zero count")
	}

	// Add one: should create a new open channel
	m.lentAdd(1)
	assert.Equal(t, int64(1), m.lentCount)
	select {
	case <-m.zeroCh:
		t.Fatal("expected zeroCh to be open when count > 0")
	default:
		// expected: channel is open
	}

	// Add another
	m.lentAdd(1)
	assert.Equal(t, int64(2), m.lentCount)

	// Return one: still > 0, channel should still be open
	m.lentAdd(-1)
	assert.Equal(t, int64(1), m.lentCount)
	select {
	case <-m.zeroCh:
		t.Fatal("expected zeroCh to be open when count > 0")
	default:
		// expected
	}

	// Return last one: back to zero, channel should be closed
	m.lentAdd(-1)
	assert.Equal(t, int64(0), m.lentCount)
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
	m.lentAdd(1)

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
	m.lentAdd(-1)

	wg.Wait()
	require.NoError(t, drainErr)
}

func TestWaitForDrain_RespectsContextCancellation(t *testing.T) {
	m := newDrainTestManager()

	// Simulate a lent connection that never returns
	m.lentAdd(1)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := m.WaitForDrain(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
