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

package topoclient

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recoveringConn is a mockConn that also reports recoveries.
type recoveringConn struct {
	*mockConn
	recoveries chan struct{}
}

func (c *recoveringConn) Recoveries() <-chan struct{} { return c.recoveries }

func (c *recoveringConn) Close() error {
	close(c.recoveries)
	return c.mockConn.Close()
}

func recoveringFactory() (newFunc func() (Conn, error), conns func() []*recoveringConn) {
	var mu sync.Mutex
	var all []*recoveringConn
	newFunc = func() (Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		c := &recoveringConn{mockConn: newMockConn(len(all) + 1), recoveries: make(chan struct{}, 1)}
		all = append(all, c)
		return c, nil
	}
	conns = func() []*recoveringConn {
		mu.Lock()
		defer mu.Unlock()
		return append([]*recoveringConn(nil), all...)
	}
	return newFunc, conns
}

func expectRecovery(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case _, ok := <-ch:
		require.True(t, ok, "recovery channel closed instead of firing")
	case <-time.After(5 * time.Second):
		t.Fatal("no recovery reported")
	}
}

func expectNoRecovery(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("unexpected recovery")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWrapperConn_ForwardsRecoveriesOfWrappedConn(t *testing.T) {
	newFunc, conns := recoveringFactory()
	wrapper := NewWrapperConn(newFunc, nil)
	defer wrapper.Close()

	expectNoRecovery(t, wrapper.Recoveries())

	conns()[0].recoveries <- struct{}{}
	expectRecovery(t, wrapper.Recoveries())
}

func TestWrapperConn_ReportsRecoveryWhenReplacingFailedConnection(t *testing.T) {
	factory := newMockFactory()
	factory.setShouldFail(true)
	wrapper := NewWrapperConn(factory.newConn, nil)
	defer wrapper.Close()

	expectNoRecovery(t, wrapper.Recoveries())

	factory.setShouldFail(false)
	expectRecovery(t, wrapper.Recoveries())
}

func TestWrapperConn_RecoveriesCoalesce(t *testing.T) {
	newFunc, conns := recoveringFactory()
	wrapper := NewWrapperConn(newFunc, nil)
	defer wrapper.Close()

	inner := conns()[0]
	for range 3 {
		inner.recoveries <- struct{}{}
		require.Eventually(t, func() bool { return len(wrapper.Recoveries()) == 1 }, time.Second, time.Millisecond)
	}

	// The loop above can return before the forwarder has relayed every signal,
	// because len == 1 is already true after the first one. Waiting for silence
	// with a timeout would be racy, so close the wrapper instead: Close takes the
	// lock the forwarder holds while relaying, so afterwards nothing more can
	// arrive, and the buffered signal is still readable from the closed channel.
	require.NoError(t, wrapper.Close())

	expectRecovery(t, wrapper.Recoveries())
	_, ok := <-wrapper.Recoveries()
	assert.False(t, ok, "signals must have coalesced into a single pending recovery")
}

func TestWrapperConn_CloseClosesRecoveries(t *testing.T) {
	newFunc, _ := recoveringFactory()
	wrapper := NewWrapperConn(newFunc, nil)

	require.NoError(t, wrapper.Close())

	_, ok := <-wrapper.Recoveries()
	assert.False(t, ok, "Close must close the recovery channel")
	require.NoError(t, wrapper.Close(), "Close must be idempotent")
}
