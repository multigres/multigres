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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
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
