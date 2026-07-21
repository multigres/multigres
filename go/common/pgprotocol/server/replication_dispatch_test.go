// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// replicationDispatchHandler embeds mockHandler for the rest of the Handler
// surface and adds HandleReplicationStream, so it satisfies ReplicationHandler
// and Conn.serve() can dispatch a replication=database connection to it.
type replicationDispatchHandler struct {
	mockHandler
	called  bool
	gotConn *Conn
	err     error
}

func (h *replicationDispatchHandler) HandleReplicationStream(_ context.Context, conn *Conn) error {
	h.called = true
	h.gotConn = conn
	return h.err
}

// TestServe_DispatchesLogicalReplicationToHandler verifies that once startup
// completes on a replication=database connection, Conn.serve() hands the
// connection to the installed ReplicationHandler instead of entering the SQL
// command loop, and returns whatever error the handler produces.
func TestServe_DispatchesLogicalReplicationToHandler(t *testing.T) {
	provider := newMockCredentialProvider("postgres")
	provider.isReplicationRole = true
	handler := &replicationDispatchHandler{err: errors.New("tunnel done")}

	listener, err := NewListener(ListenerConfig{
		Address:            "localhost:0",
		Handler:            handler,
		CredentialProvider: provider,
		Logger:             testLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	serverConn, clientConn := newPipeConnPair()
	c := newConn(serverConn, listener, 1)
	c.handler = listener.handler
	c.credentialProvider = listener.credentialProvider
	c.trustAuthProvider = listener.trustAuthProvider

	errCh := make(chan error, 1)
	go func() { errCh <- c.serve() }()

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":        "repluser",
		"database":    "appdb",
		"replication": "database",
	})
	scramClientHelper(t, clientConn, "repluser", "postgres")

	require.Equal(t, handler.err, <-errCh, "serve() must return the replication handler's error")
	assert.True(t, handler.called, "HandleReplicationStream must be invoked")
	assert.Same(t, c, handler.gotConn)
}

// replicationAndQueryHandler embeds replicationDispatchHandler (which already
// satisfies ReplicationHandler via HandleReplicationStream) and additionally
// tracks whether HandleQuery ran, so a test can prove the command loop — not
// the replication hijack — is what processed a message.
type replicationAndQueryHandler struct {
	replicationDispatchHandler
	queryHandled bool
}

func (h *replicationAndQueryHandler) HandleQuery(
	_ context.Context, _ *Conn, _ string, _ func(ctx context.Context, result *sqltypes.Result) error,
) error {
	h.queryHandled = true
	return nil
}

// TestServe_PhysicalReplicationNeverDispatchesToHandler verifies the guard at
// conn.go's dispatch gate: ReplicationPhysical (replication=true) must always
// run the normal command loop, even when the installed handler implements
// ReplicationHandler. Only ReplicationLogical (replication=database) is
// eligible for the byte-tunnel hijack today.
func TestServe_PhysicalReplicationNeverDispatchesToHandler(t *testing.T) {
	provider := newMockCredentialProvider("postgres")
	provider.isReplicationRole = true
	handler := &replicationAndQueryHandler{}

	listener, err := NewListener(ListenerConfig{
		Address:            "localhost:0",
		Handler:            handler,
		CredentialProvider: provider,
		Logger:             testLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	serverConn, clientConn := newPipeConnPair()
	c := newConn(serverConn, listener, 1)
	c.handler = listener.handler
	c.credentialProvider = listener.credentialProvider
	c.trustAuthProvider = listener.trustAuthProvider

	errCh := make(chan error, 1)
	go func() { errCh <- c.serve() }()

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":        "repluser",
		"database":    "appdb",
		"replication": "true",
	})
	scramClientHelper(t, clientConn, "repluser", "postgres")

	// The command loop is live (not hijacked): send a simple Query and
	// confirm the handler's HandleQuery ran. Wait for the ReadyForQuery
	// reply before closing the client, so the server isn't left writing to
	// an already-closed pipe.
	var body bytes.Buffer
	body.WriteString("SELECT 1")
	body.WriteByte(0)
	writeMessage(t, clientConn, protocol.MsgQuery, body.Bytes())

	msgType, _ := readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgReadyForQuery), msgType)

	require.NoError(t, clientConn.Close())
	require.NoError(t, <-errCh, "serve() must return cleanly on client disconnect")

	assert.True(t, handler.queryHandled, "HandleQuery must run: physical replication uses the command loop")
	assert.False(t, handler.called, "HandleReplicationStream must never run for physical replication")
}

// TestServe_LogicalReplicationFallsThroughWithoutHandler verifies the other
// guard at conn.go's dispatch gate: a ReplicationLogical (replication=database)
// connection whose installed handler does NOT implement ReplicationHandler
// must fall through to the normal command loop gracefully, not error out or
// panic.
func TestServe_LogicalReplicationFallsThroughWithoutHandler(t *testing.T) {
	provider := newMockCredentialProvider("postgres")
	provider.isReplicationRole = true
	handler := &queryTrackingHandler{}

	listener, err := NewListener(ListenerConfig{
		Address:            "localhost:0",
		Handler:            handler,
		CredentialProvider: provider,
		Logger:             testLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	serverConn, clientConn := newPipeConnPair()
	c := newConn(serverConn, listener, 1)
	c.handler = listener.handler
	c.credentialProvider = listener.credentialProvider
	c.trustAuthProvider = listener.trustAuthProvider

	errCh := make(chan error, 1)
	go func() { errCh <- c.serve() }()

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":        "repluser",
		"database":    "appdb",
		"replication": "database",
	})
	scramClientHelper(t, clientConn, "repluser", "postgres")

	var body bytes.Buffer
	body.WriteString("SELECT 1")
	body.WriteByte(0)
	writeMessage(t, clientConn, protocol.MsgQuery, body.Bytes())

	msgType, _ := readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgReadyForQuery), msgType)

	require.NoError(t, clientConn.Close())
	require.NoError(t, <-errCh, "serve() must return cleanly on client disconnect")

	assert.True(t, handler.queryHandled,
		"HandleQuery must run: a handler without ReplicationHandler falls through to the command loop")
}

// queryTrackingHandler wraps mockHandler and records whether HandleQuery ran.
// Deliberately does NOT implement ReplicationHandler.
type queryTrackingHandler struct {
	mockHandler
	queryHandled bool
}

func (h *queryTrackingHandler) HandleQuery(
	_ context.Context, _ *Conn, _ string, _ func(ctx context.Context, result *sqltypes.Result) error,
) error {
	h.queryHandled = true
	return nil
}
