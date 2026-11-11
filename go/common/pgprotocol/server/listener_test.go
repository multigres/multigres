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
	"bytes"
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/pid"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
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

// delayedAcceptListener models an Accept that succeeded before Close but whose
// result has not yet reached Serve. No production scheduling hooks are needed.
type delayedAcceptListener struct {
	net.Listener
	accepted chan struct{}
	release  chan struct{}
}

func (l *delayedAcceptListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		close(l.accepted)
		<-l.release
	}
	return conn, err
}

func waitListenerSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("listener lifecycle did not advance")
	}
}

func TestListenerShutdown_RejectsLateAccept(t *testing.T) {
	for _, method := range []string{"Close", "CloseListener", "Shutdown"} {
		t.Run(method, func(t *testing.T) {
			l := testListenerWithGatewayID(t, 1)
			delayed := &delayedAcceptListener{
				Listener: l.listener,
				accepted: make(chan struct{}),
				release:  make(chan struct{}),
			}
			l.listener = delayed
			release := sync.OnceFunc(func() { close(delayed.release) })
			t.Cleanup(release)
			served := make(chan struct{})
			go func() {
				_ = l.Serve()
				close(served)
			}()
			client, err := net.Dial("tcp", l.Addr().String())
			require.NoError(t, err)
			t.Cleanup(func() { _ = client.Close() })
			waitListenerSignal(t, delayed.accepted)

			// The handler count is still zero. Shutdown must not wait for
			// Accept to return, nor may it admit this socket afterwards.
			stopped := make(chan struct{})
			go func() {
				switch method {
				case "Close":
					_ = l.Close()
				case "CloseListener":
					_ = l.CloseListener()
				case "Shutdown":
					_ = l.Shutdown(t.Context())
				}
				close(stopped)
			}()
			waitListenerSignal(t, stopped)
			release()
			waitListenerSignal(t, served)
			l.connsMu.Lock()
			assert.Empty(t, l.open, "late accepts must not publish a handler")
			assert.Empty(t, l.conns, "late accepts must not register a PID")
			l.connsMu.Unlock()
			require.NoError(t, client.SetReadDeadline(time.Now().Add(5*time.Second)))
			_, err = client.Read(make([]byte, 1))
			require.Error(t, err)
			assert.NotErrorIs(t, err, os.ErrDeadlineExceeded)
		})
	}
}

type shutdownQueryHandler struct {
	testHandler
	cleanupStarted chan struct{}
	cleanupRelease chan struct{}
}

func (h *shutdownQueryHandler) ConnectionClosed(*Conn) {
	close(h.cleanupStarted)
	<-h.cleanupRelease
}

func TestListenerShutdown_QueryLifecycle(t *testing.T) {
	for _, cut := range []bool{false, true} {
		name := "drain"
		if cut {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			queryStarted := make(chan struct{})
			queryRelease := make(chan struct{})
			queryContext := make(chan context.Context, 1)
			h := &shutdownQueryHandler{
				cleanupStarted: make(chan struct{}),
				cleanupRelease: make(chan struct{}),
			}
			releaseCleanup := sync.OnceFunc(func() { close(h.cleanupRelease) })
			releaseQuery := sync.OnceFunc(func() { close(queryRelease) })
			h.queryFunc = func(ctx context.Context, _ *Conn, _ string, callback func(context.Context, *sqltypes.Result) error) error {
				queryContext <- ctx
				close(queryStarted)
				select {
				case <-queryRelease:
					return callback(ctx, &sqltypes.Result{CommandTag: "SELECT 0"})
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			l := testListenerWithGatewayID(t, 1)
			t.Cleanup(releaseCleanup)
			t.Cleanup(releaseQuery)
			l.handler = h
			l.trustAuthProvider = allowAllTrust{}
			served := make(chan struct{})
			go func() { _ = l.Serve(); close(served) }()
			client := dialOpen(t, l)
			require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
			var startup bytes.Buffer
			writeStartupPacket(&startup, protocol.ProtocolVersionNumber, map[string]string{"user": "postgres"})
			_, err := client.Write(startup.Bytes())
			require.NoError(t, err)
			for {
				typ, _ := readMessage(t, client)
				if typ == protocol.MsgReadyForQuery {
					break
				}
			}
			writeMessage(t, client, protocol.MsgQuery, []byte("SELECT 0\x00"))
			waitListenerSignal(t, queryStarted)
			queryCtx := <-queryContext
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stopped := make(chan struct{})
			go func() { _ = l.Shutdown(ctx); close(stopped) }()
			waitListenerSignal(t, l.ctx.Done())
			waitListenerSignal(t, served)
			assert.NoError(t, queryCtx.Err(), "stopping accepts must not cancel established queries")
			select {
			case <-stopped:
				t.Fatal("Shutdown returned before the query ended")
			default:
			}
			if cut {
				cancel()
				waitListenerSignal(t, stopped)
				waitListenerSignal(t, queryCtx.Done())
			} else {
				releaseQuery()
				typ, _ := readMessage(t, client)
				assert.Equal(t, byte(protocol.MsgCommandComplete), typ)
				typ, _ = readMessage(t, client)
				assert.Equal(t, byte(protocol.MsgReadyForQuery), typ)
				require.NoError(t, client.Close())
			}
			waitListenerSignal(t, h.cleanupStarted)
			assert.Zero(t, l.ConnectionCount(), "PID must be unregistered before cleanup")
			l.connsMu.Lock()
			assert.Empty(t, l.open)
			l.connsMu.Unlock()
			if !cut {
				select {
				case <-stopped:
					t.Fatal("graceful Shutdown did not wait for handler cleanup")
				default:
				}
			}
			releaseCleanup()
			waitListenerSignal(t, stopped)
		})
	}
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
