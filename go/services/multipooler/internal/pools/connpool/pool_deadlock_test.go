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
	"testing"
	"time"
)

// TestCloseIdleResources_MutexDeadlock verifies that closeIdleResources does not
// deadlock when replacing expired idle connections.
//
// The bug: closeIdleResources calls getNew() and tryReturnConn() inside a
// ForEach callback that holds the stack mutex. When getNew() succeeds and
// tryReturnConn() pushes the replacement connection to the same stack,
// Push() tries to acquire the same mutex → self-deadlock (sync.Mutex is
// not reentrant).
//
// This test creates a pool with capacity=1, gets and returns a connection
// (so it sits idle on the clean stack), then calls closeIdleResources with
// a time far enough in the future to expire it. If the deadlock exists,
// closeIdleResources will hang and the subsequent Get will also hang
// (blocked on the locked mutex). The test uses a timeout to detect this.
func TestCloseIdleResources_MutexDeadlock(t *testing.T) {
	idleTimeout := 100 * time.Millisecond

	pool := NewPool[*mockConnection](context.Background(), &Config{
		Name:         "deadlock-test",
		Capacity:     1,
		MaxIdleCount: 1,
		IdleTimeout:  idleTimeout,
	})
	pool.Open(func(ctx context.Context, poolCtx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}, nil)
	defer pool.Close()

	// Get a connection and return it so it sits idle on the clean stack.
	conn, err := pool.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	conn.Recycle()

	// Wait for the connection to become idle long enough to expire.
	time.Sleep(idleTimeout + 50*time.Millisecond)

	// Call closeIdleResources. If the deadlock exists, this will hang
	// forever because tryReturnConn → clean.Push tries to lock the
	// same mutex that ForEach is holding.
	done := make(chan struct{})
	go func() {
		pool.closeIdleResources(time.Now())
		close(done)
	}()

	select {
	case <-done:
		// closeIdleResources completed without deadlocking.
	case <-time.After(2 * time.Second):
		t.Fatal("closeIdleResources deadlocked: ForEach holds stack mutex while tryReturnConn tries to Push to the same stack")
	}

	// Verify the pool is still usable — a Get should not hang.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn2, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Get after closeIdleResources: %v", err)
	}
	conn2.Recycle()
}
