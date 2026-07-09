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
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

type idleSessionTimeoutHandler struct {
	mockHandler
	timeout time.Duration
}

func (h *idleSessionTimeoutHandler) IdleSessionTimeout(*Conn) time.Duration {
	return h.timeout
}

// idleSessionTimeoutDrainHandler fails every Parse, driving the connection
// into extended-query error-drain mode so tests can exercise
// idle_session_timeout while c.discardingUntilSync is set.
type idleSessionTimeoutDrainHandler struct {
	mockHandler
	timeout time.Duration
}

func (h *idleSessionTimeoutDrainHandler) IdleSessionTimeout(*Conn) time.Duration {
	return h.timeout
}

func (h *idleSessionTimeoutDrainHandler) HandleParse(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
	return errors.New("parse failed")
}

func writeIdleSessionTimeoutParse(t *testing.T, conn net.Conn, stmtName, query string) {
	t.Helper()
	var body bytes.Buffer
	body.WriteString(stmtName)
	body.WriteByte(0)
	body.WriteString(query)
	body.WriteByte(0)
	require.NoError(t, binary.Write(&body, binary.BigEndian, int16(0)))
	writeMessage(t, conn, protocol.MsgParse, body.Bytes())
}

func TestIdleSessionTimeout_EmitsFatalAndCloses(t *testing.T) {
	const timeout = 150 * time.Millisecond
	h := &idleSessionTimeoutHandler{timeout: timeout}

	listener, err := NewListener(ListenerConfig{
		Address:            "127.0.0.1:0",
		Handler:            h,
		CredentialProvider: newMockCredentialProvider("postgres"),
		Logger:             testLogger(t),
	})
	require.NoError(t, err)
	defer listener.Close()

	go func() {
		_ = listener.Serve()
	}()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer clientConn.Close()
	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(5*time.Second)))

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber,
		map[string]string{"user": "idleuser", "database": "testdb"})
	scramClientHelper(t, clientConn, "idleuser", "postgres")

	start := time.Now()
	msgType, body := readMessage(t, clientConn)
	elapsed := time.Since(start)

	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	assert.True(t, containsErrField(body, 'S', "FATAL"), "severity should be FATAL")
	assert.True(t, containsErrField(body, 'C', "57P05"), "SQLSTATE should be 57P05 (idle_session_timeout)")
	assert.True(t, containsErrField(body, 'M', "terminating connection due to idle-session timeout"))
	assert.GreaterOrEqual(t, elapsed, timeout-50*time.Millisecond,
		"server should not respond before the idle-session deadline")
	assert.Less(t, elapsed, 5*time.Second,
		"server should respond shortly after the idle-session deadline")
}

func TestIdleSessionTimeout_DoesNotFireMidExtendedQueryCycle(t *testing.T) {
	const timeout = 150 * time.Millisecond
	const readDeadline = 5 * time.Second
	h := &idleSessionTimeoutHandler{timeout: timeout}

	listener, err := NewListener(ListenerConfig{
		Address:            "127.0.0.1:0",
		Handler:            h,
		CredentialProvider: newMockCredentialProvider("postgres"),
		Logger:             testLogger(t),
	})
	require.NoError(t, err)
	defer listener.Close()

	go func() {
		_ = listener.Serve()
	}()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer clientConn.Close()
	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(readDeadline)))

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber,
		map[string]string{"user": "idleuser", "database": "testdb"})
	scramClientHelper(t, clientConn, "idleuser", "postgres")

	writeIdleSessionTimeoutParse(t, clientConn, "stmt", "SELECT 1")
	// PostgreSQL arms idle_session_timeout only at ReadyForQuery/command-cycle
	// boundaries. Parse is an extended-protocol message within the current
	// command cycle, so the session must not time out while the client waits
	// before sending Sync.
	time.Sleep(3 * timeout)
	writeMessage(t, clientConn, protocol.MsgSync, nil)

	msgType, _ := readMessage(t, clientConn)
	assert.Equal(t, byte(protocol.MsgParseComplete), msgType)
	msgType, _ = readMessage(t, clientConn)
	assert.Equal(t, byte(protocol.MsgReadyForQuery), msgType)

	// Once Sync completes and ReadyForQuery has been emitted, the connection is
	// idle at a new command boundary and the timeout should fire again.
	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(readDeadline)))
	start := time.Now()
	msgType, body := readMessage(t, clientConn)
	elapsed := time.Since(start)

	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	assert.True(t, containsErrField(body, 'S', "FATAL"), "severity should be FATAL")
	assert.True(t, containsErrField(body, 'C', "57P05"), "SQLSTATE should be 57P05 (idle_session_timeout)")
	assert.True(t, containsErrField(body, 'M', "terminating connection due to idle-session timeout"))
	assert.GreaterOrEqual(t, elapsed, timeout-50*time.Millisecond,
		"server should not respond before the idle-session deadline after Sync")
}

// TestIdleSessionTimeout_DoesNotFireAfterDrainedQueryBeforeSync verifies that
// a pipelined simple Query discarded during extended-query error drain (see
// TestExtendedQueryErrorDiscardsSimpleQueryUntilSync) is not mistaken for a
// batch boundary by serve()'s idle_session_timeout arming logic. Before the
// fix, serve() treated every MsgQuery as ending the command cycle — even one
// silently discarded mid-drain — which let idle_session_timeout arm before
// the client's required Sync arrived, killing the connection out from under
// a client that was still mid-batch.
func TestIdleSessionTimeout_DoesNotFireAfterDrainedQueryBeforeSync(t *testing.T) {
	const timeout = 150 * time.Millisecond
	const readDeadline = 5 * time.Second
	h := &idleSessionTimeoutDrainHandler{timeout: timeout}

	listener, err := NewListener(ListenerConfig{
		Address:            "127.0.0.1:0",
		Handler:            h,
		CredentialProvider: newMockCredentialProvider("postgres"),
		Logger:             testLogger(t),
	})
	require.NoError(t, err)
	defer listener.Close()

	go func() {
		_ = listener.Serve()
	}()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer clientConn.Close()
	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(readDeadline)))

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber,
		map[string]string{"user": "idleuser", "database": "testdb"})
	scramClientHelper(t, clientConn, "idleuser", "postgres")

	// Parse fails -> enters drain mode and buffers an ErrorResponse.
	writeIdleSessionTimeoutParse(t, clientConn, "stmt", "SELECT 1")

	// A pipelined simple Query arrives before Sync — it must be discarded,
	// not treated as a batch boundary.
	var body bytes.Buffer
	body.WriteString("RELEASE SAVEPOINT postgrex_query")
	body.WriteByte(0)
	writeMessage(t, clientConn, protocol.MsgQuery, body.Bytes())

	// The client pauses before sending the required Sync. If the discarded
	// Query were wrongly treated as a boundary, idle_session_timeout would
	// arm here and the connection would be killed before Sync arrives.
	time.Sleep(3 * timeout)
	writeMessage(t, clientConn, protocol.MsgSync, nil)

	msgType, errBody := readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgErrorResponse), msgType,
		"only the buffered Parse error should be emitted — no frame from the discarded Query")
	assert.True(t, containsErrField(errBody, 'M', "parse failed"))
	msgType, _ = readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgReadyForQuery), msgType,
		"Sync must complete the drain and emit ReadyForQuery, not a premature idle-session FATAL")

	// Once Sync completes and ReadyForQuery has been emitted, the connection
	// is idle at a new command boundary and the timeout should fire normally.
	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(readDeadline)))
	start := time.Now()
	msgType, fatalBody := readMessage(t, clientConn)
	elapsed := time.Since(start)

	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	assert.True(t, containsErrField(fatalBody, 'S', "FATAL"), "severity should be FATAL")
	assert.True(t, containsErrField(fatalBody, 'C', "57P05"), "SQLSTATE should be 57P05 (idle_session_timeout)")
	assert.GreaterOrEqual(t, elapsed, timeout-50*time.Millisecond,
		"server should not respond before the idle-session deadline after Sync")
}
