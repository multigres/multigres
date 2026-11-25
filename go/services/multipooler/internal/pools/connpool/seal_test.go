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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTestSealed = errors.New("sealed for the test")

// A sealed pool lends nothing: Get fails with the seal's error, the idle
// connections are closed, a returned connection is closed rather than kept,
// and the connections still lent are the open ones. Unseal lends again.
func TestPoolSeal(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(4)
	defer pool.Close()

	idle, err := pool.Get(ctx)
	require.NoError(t, err)
	lent, err := pool.Get(ctx)
	require.NoError(t, err)
	idleConn := idle.Conn
	idle.Recycle()
	require.False(t, idleConn.IsClosed(), "precondition: the returned connection is idle")

	pool.Seal(errTestSealed)
	assert.True(t, idleConn.IsClosed(), "sealing closes the idle connections")
	_, err = pool.Get(ctx)
	assert.ErrorIs(t, err, errTestSealed)
	_, err = pool.GetWithSettings(ctx, nil)
	assert.ErrorIs(t, err, errTestSealed)

	open := pool.OpenConns()
	require.Len(t, open, 1, "a sealed pool's open connections are the lent ones")
	assert.Same(t, lent.Conn, open[0])

	lentConn := lent.Conn
	lent.Recycle()
	assert.True(t, lentConn.IsClosed(), "a connection returned to a sealed pool is closed")
	assert.Empty(t, pool.OpenConns())
	assert.Zero(t, pool.Active())

	pool.Unseal()
	again, err := pool.Get(ctx)
	require.NoError(t, err, "an unsealed pool lends again")
	again.Recycle()
}

// Sealing wakes the clients waiting for a connection with the seal's error,
// and refuses to queue new ones.
func TestPoolSealWakesWaiters(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(1)
	defer pool.Close()

	held, err := pool.Get(ctx)
	require.NoError(t, err)

	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := pool.Get(ctx)
			errs <- err
		}()
	}
	require.Eventually(t, func() bool { return pool.Waiting() == 2 }, time.Second, 5*time.Millisecond)

	pool.Seal(errTestSealed)
	for range 2 {
		select {
		case err := <-errs:
			assert.ErrorIs(t, err, errTestSealed)
		case <-time.After(time.Second):
			t.Fatal("a waiter was not woken by the seal")
		}
	}
	assert.Zero(t, pool.Waiting())

	held.Recycle()
	pool.Unseal()
}

// A client already past Get's check when the pool is sealed gets no
// connection: the waitlist refuses to queue it.
func TestWaitlistSealRefusesNewWaiters(t *testing.T) {
	var wl waitlist[*mockConnection]
	wl.init()
	wl.seal()
	_, err := wl.waitForConn(context.Background(), nil, make(chan struct{}))
	assert.ErrorIs(t, err, errWaitSealed)
	wl.unseal()
	assert.False(t, wl.sealed)
}

// The record of dialed connections drops the closed ones as it grows.
func TestOpenConnsPrunesClosed(t *testing.T) {
	var o openConns[*mockConnection]
	for range 40 {
		c := newMockConnection()
		o.add(c)
		c.closed.Store(true)
	}
	assert.Empty(t, o.snapshot())
	assert.Less(t, len(o.conns), 40, "closed connections are dropped as the record grows")

	live := newMockConnection()
	o.add(live)
	assert.Equal(t, []*mockConnection{live}, o.snapshot())
}
