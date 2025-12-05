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

package connpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/multipooler/connstate"
)

// TestWaitlistBasicOperations tests init, waiting count, and tryReturnConn.
func TestWaitlistBasicOperations(t *testing.T) {
	var wl waitlist[*mockConnection]
	wl.init()

	// 1. Initially empty
	assert.Equal(t, 0, wl.waiting())

	// 2. tryReturnConn returns false when no waiters
	conn := &Pooled[*mockConnection]{Conn: newMockConnection()}
	returned := wl.tryReturnConn(conn)
	assert.False(t, returned)

	// 3. maybeStarvingCount on empty list returns 0
	starving := wl.maybeStarvingCount()
	assert.Equal(t, 0, starving)
}

// TestWaitlistHandover tests that connections are handed over to waiters.
func TestWaitlistHandover(t *testing.T) {
	var wl waitlist[*mockConnection]
	wl.init()

	ctx := context.Background()
	conn := &Pooled[*mockConnection]{Conn: newMockConnection()}
	closeChan := make(chan struct{})

	// Start a waiter in a goroutine
	var receivedConn *Pooled[*mockConnection]
	var receivedErr error
	var wg sync.WaitGroup
	wg.Go(func() {
		receivedConn, receivedErr = wl.waitForConn(ctx, nil, closeChan)
	})

	// Give the waiter time to register
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 1, wl.waiting())

	// Hand over the connection
	returned := wl.tryReturnConn(conn)
	assert.True(t, returned)

	// Wait for the waiter to receive it
	wg.Wait()

	// Verify the waiter received the connection
	require.NoError(t, receivedErr)
	assert.Same(t, conn, receivedConn)
	assert.Equal(t, 0, wl.waiting())
}

// TestWaitlistExpire tests that waiters with cancelled contexts return immediately.
func TestWaitlistExpire(t *testing.T) {
	var wl waitlist[*mockConnection]
	wl.init()

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	closeChan := make(chan struct{})

	// Start a waiter
	var receivedConn *Pooled[*mockConnection]
	var receivedErr error
	var wg sync.WaitGroup
	wg.Go(func() {
		receivedConn, receivedErr = wl.waitForConn(ctx, nil, closeChan)
	})

	// Give waiter time to register
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 1, wl.waiting())

	// Cancel the context - the waiter should handle this via select
	cancel()

	wg.Wait()

	// Waiter should get context error, no connection
	assert.Nil(t, receivedConn)
	assert.ErrorIs(t, receivedErr, context.Canceled)
	assert.Equal(t, 0, wl.waiting())
}

// TestWaitlistPoolClose tests that closing the pool wakes all waiters.
func TestWaitlistPoolClose(t *testing.T) {
	var wl waitlist[*mockConnection]
	wl.init()

	ctx := context.Background()
	closeChan := make(chan struct{})
	var wg sync.WaitGroup

	// Start multiple waiters
	errors := make(chan error, 3)
	for range 3 {
		wg.Go(func() {
			_, err := wl.waitForConn(ctx, nil, closeChan)
			errors <- err
		})
	}

	// Give waiters time to register
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 3, wl.waiting())

	// Close the pool - all waiters should wake up
	close(closeChan)

	wg.Wait()
	close(errors)

	// All should have received ErrPoolClosed
	for err := range errors {
		assert.ErrorIs(t, err, ErrPoolClosed)
	}
	assert.Equal(t, 0, wl.waiting())
}

// TestWaitlistPoolCloseWithMultipleWaiters tests pool close behavior with multiple waiters.
// This test is based on the Vitess race fix test.
func TestWaitlistPoolCloseWithMultipleWaiters(t *testing.T) {
	var wl waitlist[*mockConnection]
	wl.init()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	closeChan := make(chan struct{})

	waiterCount := 2
	expireCount := atomic.Int32{}

	for range waiterCount {
		go func() {
			_, err := wl.waitForConn(ctx, nil, closeChan)
			if err != nil {
				expireCount.Add(1)
			}
		}()
	}

	close(closeChan)

	// Wait for the context to expire
	<-ctx.Done()

	// Wait for the notified goroutines to finish
	timeout := time.After(1 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for expireCount.Load() != int32(waiterCount) {
		select {
		case <-timeout:
			require.Failf(t, "Timed out waiting for all waiters to expire", "Wanted %d, got %d", waiterCount, expireCount.Load())
		case <-ticker.C:
			// try again
		}
	}

	assert.Equal(t, int32(waiterCount), expireCount.Load())
}

// TestWaitlistSettingsMatching tests that connections are matched to waiters
// with the same settings when possible.
func TestWaitlistSettingsMatching(t *testing.T) {
	var wl waitlist[*mockConnection]
	wl.init()

	ctx := context.Background()
	closeChan := make(chan struct{})
	settings1 := connstate.NewSettings(map[string]string{"tz": "UTC"})
	settings2 := connstate.NewSettings(map[string]string{"tz": "PST"})

	// Start two waiters with different settings
	var wg sync.WaitGroup
	var conn1Result, conn2Result *Pooled[*mockConnection]

	wg.Add(2)
	go func() {
		defer wg.Done()
		conn1Result, _ = wl.waitForConn(ctx, settings1, closeChan) // wants UTC
	}()
	go func() {
		defer wg.Done()
		conn2Result, _ = wl.waitForConn(ctx, settings2, closeChan) // wants PST
	}()

	// Give waiters time to register
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 2, wl.waiting())

	// Create connections with specific settings
	mockConn1 := newMockConnection()
	mockConn1.settings = settings1 // UTC
	connWithSettings1 := &Pooled[*mockConnection]{Conn: mockConn1}

	mockConn2 := newMockConnection()
	mockConn2.settings = settings2 // PST
	connWithSettings2 := &Pooled[*mockConnection]{Conn: mockConn2}

	// Return connections - they should be matched to waiters with same settings
	wl.tryReturnConn(connWithSettings1)
	wl.tryReturnConn(connWithSettings2)

	wg.Wait()

	// Both waiters should have received connections
	assert.NotNil(t, conn1Result)
	assert.NotNil(t, conn2Result)
	assert.Equal(t, 0, wl.waiting())
}
