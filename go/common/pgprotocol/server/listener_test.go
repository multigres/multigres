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

package server

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/pid"
)

func testListenerWithGatewayID(t *testing.T, gatewayID uint32) *Listener {
	listener, err := NewListener(ListenerConfig{
		Address:            "localhost:0",
		Handler:            &mockHandler{},
		CredentialProvider: newMockCredentialProvider("postgres"),
		GatewayID:          gatewayID,
		Logger:             testLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		listener.Close()
	})
	return listener
}

func TestNextLocalID_WrapsToOne(t *testing.T) {
	l := testListenerWithGatewayID(t, 1)

	// Set counter just below the max so the next call wraps.
	l.nextConnectionID = pid.MaxLocalConnID - 1

	id1 := l.nextLocalID()
	assert.Equal(t, uint32(pid.MaxLocalConnID), id1)

	// Next call should wrap to 1, skipping 0.
	id2 := l.nextLocalID()
	assert.Equal(t, uint32(1), id2)
}

func TestNextLocalID_NeverReturnsZero(t *testing.T) {
	l := testListenerWithGatewayID(t, 1)

	// Set counter so the increment would land on MaxLocalConnID+1 which wraps.
	l.nextConnectionID = pid.MaxLocalConnID

	id := l.nextLocalID()
	assert.Equal(t, uint32(1), id)
}

func TestAssignConnectionID_EncodesGatewayPrefix(t *testing.T) {
	gatewayID := uint32(42)
	l := testListenerWithGatewayID(t, gatewayID)

	connID, ok := l.assignConnectionID()
	require.True(t, ok)

	prefix, localID := pid.DecodePID(connID)
	assert.Equal(t, gatewayID, prefix)
	assert.Greater(t, localID, uint32(0))
}

func TestAssignConnectionID_SkipsInUsePIDs(t *testing.T) {
	gatewayID := uint32(7)
	l := testListenerWithGatewayID(t, gatewayID)

	// Pre-register a connection at the first local ID (1) so it's occupied.
	occupiedPID := pid.EncodePID(gatewayID, 1)
	l.connsMu.Lock()
	l.conns[occupiedPID] = &Conn{} // placeholder
	l.connsMu.Unlock()

	connID, ok := l.assignConnectionID()
	require.True(t, ok)

	// The assigned PID must differ from the occupied one.
	assert.NotEqual(t, occupiedPID, connID)

	prefix, localID := pid.DecodePID(connID)
	assert.Equal(t, gatewayID, prefix)
	assert.Greater(t, localID, uint32(0))
}

func TestAssignConnectionID_FailsWhenAllIDsUsed(t *testing.T) {
	gatewayID := uint32(3)
	l := testListenerWithGatewayID(t, gatewayID)

	// Fill every possible local ID (1 through MaxLocalConnID).
	l.connsMu.Lock()
	for id := uint32(1); id <= pid.MaxLocalConnID; id++ {
		l.conns[pid.EncodePID(gatewayID, id)] = &Conn{}
	}
	l.connsMu.Unlock()

	_, ok := l.assignConnectionID()
	assert.False(t, ok, "should fail when all local IDs are in use")
}

func TestAssignConnectionID_WrapsAndFindsSlot(t *testing.T) {
	gatewayID := uint32(5)
	l := testListenerWithGatewayID(t, gatewayID)

	// Set counter near the max.
	l.nextConnectionID = pid.MaxLocalConnID - 1

	// Occupy MaxLocalConnID so the first attempt (at max) is taken.
	occupiedPID := pid.EncodePID(gatewayID, pid.MaxLocalConnID)
	l.connsMu.Lock()
	l.conns[occupiedPID] = &Conn{}
	l.connsMu.Unlock()

	connID, ok := l.assignConnectionID()
	require.True(t, ok)

	// After wrapping, it should land on local ID 1 (skipping the occupied max).
	prefix, localID := pid.DecodePID(connID)
	assert.Equal(t, gatewayID, prefix)
	assert.Equal(t, uint32(1), localID)
}

// servingListener returns a listener serving on a free local port.
func servingListener(t *testing.T) *Listener {
	t.Helper()
	l := testListenerWithGatewayID(t, 1)
	go func() { _ = l.Serve() }()
	return l
}

// dialOpen opens a client connection to l and waits until l handles it.
func dialOpen(t *testing.T, l *Listener) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", l.listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	require.Eventually(t, func() bool {
		l.connsMu.Lock()
		defer l.connsMu.Unlock()
		return len(l.open) == 1
	}, 5*time.Second, 5*time.Millisecond)
	return conn
}

// TestListenerShutdown_ReturnsWhenConnectionsEnd checks that Shutdown stops
// accepting connections and returns once the open ones end, before ctx does.
func TestListenerShutdown_ReturnsWhenConnectionsEnd(t *testing.T) {
	l := servingListener(t)
	conn := dialOpen(t, l)

	done := make(chan error, 1)
	go func() { done <- l.Shutdown(t.Context()) }()
	select {
	case <-done:
		t.Fatal("Shutdown returned while a connection was open")
	case <-time.After(100 * time.Millisecond):
	}
	_, err := net.Dial("tcp", l.listener.Addr().String())
	assert.Error(t, err, "new connections are refused once Shutdown starts")

	require.NoError(t, conn.Close())
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return once the connection ended")
	}
}

// TestListenerShutdown_CutsConnectionsWhenCtxEnds checks that a connection
// still open when ctx ends is cut: its socket is closed, as the process
// exiting would close it, and Shutdown returns.
func TestListenerShutdown_CutsConnectionsWhenCtxEnds(t *testing.T) {
	l := servingListener(t)
	conn := dialOpen(t, l)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.NoError(t, l.Shutdown(ctx))
	assert.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond, "Shutdown waits for ctx")

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err := conn.Read(make([]byte, 1))
	require.Error(t, err)
	assert.NotErrorIs(t, err, os.ErrDeadlineExceeded, "the connection must be cut, not left open")
	require.Eventually(t, func() bool {
		l.connsMu.Lock()
		defer l.connsMu.Unlock()
		return len(l.open) == 0
	}, 5*time.Second, 5*time.Millisecond, "the cut connection's handler ends")
}
