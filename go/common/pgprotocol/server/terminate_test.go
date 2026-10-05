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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

func TestTerminate_EmitsFatalAndCloses(t *testing.T) {
	listener, err := NewListener(ListenerConfig{
		Address:            "127.0.0.1:0",
		Handler:            &mockHandler{},
		CredentialProvider: newMockCredentialProvider("postgres"),
		Logger:             testLogger(t),
	})
	require.NoError(t, err)
	defer listener.Close()
	go func() { _ = listener.Serve() }()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer clientConn.Close()
	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(5*time.Second)))

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber,
		map[string]string{"user": "termuser", "database": "testdb"})
	scramClientHelper(t, clientConn, "termuser", "postgres")

	var conns []*Conn
	require.Eventually(t, func() bool {
		conns = listener.Conns()
		return len(conns) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, "testdb", conns[0].Database())

	// Idle connection blocked in its read: Terminate must unblock it.
	conns[0].Terminate()

	msgType, body := readMessage(t, clientConn)
	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	assert.True(t, containsErrField(body, 'S', "FATAL"), "severity should be FATAL")
	assert.True(t, containsErrField(body, 'C', "57P01"), "SQLSTATE should be 57P01 (admin_shutdown)")
	assert.True(t, containsErrField(body, 'M', "terminating connection due to administrator command"))

	buf := make([]byte, 1)
	_, err = clientConn.Read(buf)
	require.Error(t, err, "server should close the socket after the FATAL")
	require.Eventually(t, func() bool { return listener.ConnectionCount() == 0 }, 5*time.Second, 10*time.Millisecond)
}
