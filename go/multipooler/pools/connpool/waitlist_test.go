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

	// 3. expire on empty list returns 0
	starving := wl.expire(false)
	assert.Equal(t, 0, starving)
}

// TestWaitlistHandover tests that connections are handed over to waiters.
func TestWaitlistHandover(t *testing.T) {
	var wl waitlist[*mockConnection]
	wl.init()

	ctx := context.Background()
	conn := &Pooled[*mockConnection]{Conn: newMockConnection()}

	// Start a waiter in a goroutine
	var receivedConn *Pooled[*mockConnection]
	var receivedErr error
	var wg sync.WaitGroup
	wg.Go(func() {
		receivedConn, receivedErr = wl.waitForConn(ctx, nil)
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

// TestWaitlistExpire tests expiring waiters with cancelled contexts.
func TestWaitlistExpire(t *testing.T) {
	var wl waitlist[*mockConnection]
	wl.init()

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Start a waiter with the cancelled context
	var receivedConn *Pooled[*mockConnection]
	var receivedErr error
	var wg sync.WaitGroup
	wg.Go(func() {
		receivedConn, receivedErr = wl.waitForConn(ctx, nil)
	})

	// Give waiter time to register
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 1, wl.waiting())

	// Expire should remove the cancelled waiter
	wl.expire(false)

	wg.Wait()

	// Waiter should get context error, no connection
	assert.Nil(t, receivedConn)
	assert.ErrorIs(t, receivedErr, context.Canceled)
	assert.Equal(t, 0, wl.waiting())
}

// TestWaitlistForceExpire tests force-expiring all waiters.
func TestWaitlistForceExpire(t *testing.T) {
	var wl waitlist[*mockConnection]
	wl.init()

	ctx := context.Background()
	var wg sync.WaitGroup

	// Start multiple waiters
	results := make(chan *Pooled[*mockConnection], 3)
	for range 3 {
		wg.Go(func() {
			conn, _ := wl.waitForConn(ctx, nil)
			results <- conn
		})
	}

	// Give waiters time to register
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 3, wl.waiting())

	// Force expire all
	wl.expire(true)

	wg.Wait()
	close(results)

	// All should have received nil
	for conn := range results {
		assert.Nil(t, conn)
	}
	assert.Equal(t, 0, wl.waiting())
}

// TestWaitlistSettingsMatching tests that connections are matched to waiters
// with the same settings when possible.
func TestWaitlistSettingsMatching(t *testing.T) {
	var wl waitlist[*mockConnection]
	wl.init()

	ctx := context.Background()
	settings1 := connstate.NewSettings(map[string]string{"tz": "UTC"})
	settings2 := connstate.NewSettings(map[string]string{"tz": "PST"})

	// Start two waiters with different settings
	var wg sync.WaitGroup
	var conn1Result, conn2Result *Pooled[*mockConnection]

	wg.Add(2)
	go func() {
		defer wg.Done()
		conn1Result, _ = wl.waitForConn(ctx, settings1) // wants UTC
	}()
	go func() {
		defer wg.Done()
		conn2Result, _ = wl.waitForConn(ctx, settings2) // wants PST
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
