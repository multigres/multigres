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

package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/protocol"
)

// mockConn is a mock network connection for testing.
type mockConn struct {
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
}

func newMockConn() *mockConn {
	return &mockConn{
		readBuf:  &bytes.Buffer{},
		writeBuf: &bytes.Buffer{},
	}
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return m.readBuf.Read(b)
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	return m.writeBuf.Write(b)
}

func (m *mockConn) Close() error {
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5432}
}

func (m *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 54321}
}

func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// testLogger creates a logger for testing.
func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

// testListener creates a test listener using NewListener.
func testListener(t *testing.T) *Listener {
	listener, err := NewListener(ListenerConfig{
		Address: "localhost:0", // Use random available port
		Handler: &mockHandler{},
		Logger:  testLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		listener.Close()
	})
	return listener
}

// mockHandler is a simple handler for testing.
type mockHandler struct{}

func (m *mockHandler) HandleQuery(ctx context.Context, conn *Conn, queryStr string, callback func(ctx context.Context, result *query.QueryResult) error) error {
	return nil
}

func (m *mockHandler) HandleParse(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
	return nil
}

func (m *mockHandler) HandleBind(ctx context.Context, conn *Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	return nil
}

func (m *mockHandler) HandleExecute(ctx context.Context, conn *Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *query.QueryResult) error) error {
	return nil
}

func (m *mockHandler) HandleDescribe(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error) {
	return nil, nil
}

func (m *mockHandler) HandleClose(ctx context.Context, conn *Conn, typ byte, name string) error {
	return nil
}

func (m *mockHandler) HandleSync(ctx context.Context, conn *Conn) error {
	return nil
}

// writeStartupPacket writes a startup packet to the buffer.
func writeStartupPacket(buf *bytes.Buffer, protocolCode uint32, params map[string]string) {
	// Calculate packet length.
	length := 4 + 4 // length field + protocol code
	for key, value := range params {
		length += len(key) + 1 + len(value) + 1 // key + null + value + null
	}
	length++ // Final null terminator

	// Write length (error ignored - bytes.Buffer.Write never fails).
	_ = binary.Write(buf, binary.BigEndian, uint32(length))

	// Write protocol code.
	_ = binary.Write(buf, binary.BigEndian, protocolCode)

	// Write parameters.
	for key, value := range params {
		buf.WriteString(key)
		buf.WriteByte(0)
		buf.WriteString(value)
		buf.WriteByte(0)
	}

	// Write final null.
	buf.WriteByte(0)
}

func TestHandleStartupMessage(t *testing.T) {
	tests := []struct {
		name           string
		params         map[string]string
		expectedUser   string
		expectedDB     string
		expectedParams map[string]string
	}{
		{
			name: "basic startup with user and database",
			params: map[string]string{
				"user":     "postgres",
				"database": "testdb",
			},
			expectedUser: "postgres",
			expectedDB:   "testdb",
			expectedParams: map[string]string{
				"user":     "postgres",
				"database": "testdb",
			},
		},
		{
			name: "startup with only user (database defaults to user)",
			params: map[string]string{
				"user": "myuser",
			},
			expectedUser: "myuser",
			expectedDB:   "myuser",
			expectedParams: map[string]string{
				"user": "myuser",
			},
		},
		{
			name: "startup with additional parameters",
			params: map[string]string{
				"user":             "postgres",
				"database":         "postgres",
				"application_name": "psql",
				"client_encoding":  "UTF8",
			},
			expectedUser: "postgres",
			expectedDB:   "postgres",
			expectedParams: map[string]string{
				"user":             "postgres",
				"database":         "postgres",
				"application_name": "psql",
				"client_encoding":  "UTF8",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock connection.
			mock := newMockConn()

			// Write startup packet.
			writeStartupPacket(mock.readBuf, protocol.ProtocolVersionNumber, tt.params)

			// Create a test listener.
			listener := testListener(t)
			c := &Conn{
				conn:           mock,
				listener:       listener,
				bufferedReader: bufio.NewReader(mock),
				bufferedWriter: bufio.NewWriter(mock),
				params:         make(map[string]string),
				txnStatus:      protocol.TxnStatusIdle,
			}
			c.logger = testLogger(t)

			// Actually call handleStartup to test the full flow.
			err := c.handleStartup()
			require.NoError(t, err)

			// Verify connection state was set correctly.
			assert.Equal(t, tt.expectedUser, c.user)
			assert.Equal(t, tt.expectedDB, c.database)
			assert.Equal(t, tt.expectedParams, c.params)
			assert.Equal(t, protocol.ProtocolVersion(protocol.ProtocolVersionNumber), c.protocolVersion)

			// Verify authentication messages were sent.
			output := mock.writeBuf.Bytes()
			assert.NotEmpty(t, output, "should have sent authentication messages")
		})
	}
}

func TestSSLRequest(t *testing.T) {
	// Create mock connection.
	mock := newMockConn()

	// Write SSL request followed by startup message.
	_ = binary.Write(mock.readBuf, binary.BigEndian, uint32(8))
	_ = binary.Write(mock.readBuf, binary.BigEndian, uint32(protocol.SSLRequestCode))

	params := map[string]string{
		"user":     "testuser",
		"database": "testdb",
	}
	writeStartupPacket(mock.readBuf, protocol.ProtocolVersionNumber, params)

	// Create a test listener.
	listener := testListener(t)
	c := &Conn{
		conn:           mock,
		listener:       listener,
		bufferedReader: bufio.NewReader(mock),
		bufferedWriter: bufio.NewWriter(mock),
		params:         make(map[string]string),
		txnStatus:      protocol.TxnStatusIdle,
	}
	c.logger = testLogger(t)

	// Test handleSSLRequest through handleStartup.
	err := c.handleStartup()
	require.NoError(t, err)

	// Verify 'N' was sent to decline SSL.
	output := mock.writeBuf.Bytes()
	assert.NotEmpty(t, output)
	assert.Equal(t, byte('N'), output[0], "should send 'N' to decline SSL")

	// Verify startup completed successfully.
	assert.Equal(t, "testuser", c.user)
	assert.Equal(t, "testdb", c.database)
}

func TestGSSENCRequest(t *testing.T) {
	// Create mock connection.
	mock := newMockConn()

	// Write GSSENC request followed by startup message.
	_ = binary.Write(mock.readBuf, binary.BigEndian, uint32(8))
	_ = binary.Write(mock.readBuf, binary.BigEndian, uint32(protocol.GSSENCRequestCode))

	params := map[string]string{
		"user":     "gssuser",
		"database": "gssdb",
	}
	writeStartupPacket(mock.readBuf, protocol.ProtocolVersionNumber, params)

	// Create a test listener.
	listener := testListener(t)
	c := &Conn{
		conn:           mock,
		listener:       listener,
		bufferedReader: bufio.NewReader(mock),
		bufferedWriter: bufio.NewWriter(mock),
		params:         make(map[string]string),
		txnStatus:      protocol.TxnStatusIdle,
	}
	c.logger = testLogger(t)

	// Test handleGSSENCRequest through handleStartup.
	err := c.handleStartup()
	require.NoError(t, err)

	// Verify 'N' was sent to decline GSSENC.
	output := mock.writeBuf.Bytes()
	assert.NotEmpty(t, output)
	assert.Equal(t, byte('N'), output[0], "should send 'N' to decline GSSENC")

	// Verify startup completed successfully.
	assert.Equal(t, "gssuser", c.user)
	assert.Equal(t, "gssdb", c.database)
}

func TestAuthenticationMessages(t *testing.T) {
	// Create mock connection.
	mock := newMockConn()

	// Create a test listener.
	listener := testListener(t)
	c := &Conn{
		conn:           mock,
		listener:       listener,
		bufferedReader: bufio.NewReader(mock),
		bufferedWriter: bufio.NewWriter(mock),
		params:         make(map[string]string),
		connectionID:   12345,
		backendKeyData: 67890,
		txnStatus:      protocol.TxnStatusIdle,
	}
	c.logger = testLogger(t)

	// Send authentication messages.
	err := c.sendAuthenticationOk()
	require.NoError(t, err)

	err = c.sendBackendKeyData()
	require.NoError(t, err)

	err = c.sendParameterStatus("server_version", "16.0")
	require.NoError(t, err)

	err = c.sendReadyForQuery()
	require.NoError(t, err)

	// Flush to ensure all messages are written.
	err = c.flush()
	require.NoError(t, err)

	// Verify messages were written.
	output := mock.writeBuf.Bytes()
	assert.NotEmpty(t, output)

	// Parse and verify AuthenticationOk message.
	msgType := output[0]
	assert.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)

	msgLen := binary.BigEndian.Uint32(output[1:5])
	assert.Equal(t, uint32(8), msgLen) // 4 bytes length + 4 bytes auth code

	authCode := binary.BigEndian.Uint32(output[5:9])
	assert.Equal(t, uint32(protocol.AuthOk), authCode)

	// Verify BackendKeyData message.
	offset := 9
	msgType = output[offset]
	assert.Equal(t, byte(protocol.MsgBackendKeyData), msgType)

	msgLen = binary.BigEndian.Uint32(output[offset+1 : offset+5])
	assert.Equal(t, uint32(12), msgLen) // 4 bytes length + 4 bytes process ID + 4 bytes secret key

	processID := binary.BigEndian.Uint32(output[offset+5 : offset+9])
	assert.Equal(t, uint32(12345), processID)

	secretKey := binary.BigEndian.Uint32(output[offset+9 : offset+13])
	assert.Equal(t, uint32(67890), secretKey)
}

func TestCancelRequest(t *testing.T) {
	// Create mock connection.
	mock := newMockConn()

	// Write cancel request packet.
	// Note: The length field INCLUDES itself
	// For startup packets: length = length_field(4) + protocol_code(4) + data
	_ = binary.Write(mock.readBuf, binary.BigEndian, uint32(16)) // length = 16 (4 length + 4 code + 4 PID + 4 key)
	_ = binary.Write(mock.readBuf, binary.BigEndian, uint32(protocol.CancelRequestCode))
	_ = binary.Write(mock.readBuf, binary.BigEndian, uint32(12345)) // Process ID
	_ = binary.Write(mock.readBuf, binary.BigEndian, uint32(67890)) // Secret key

	// Create a test connection - use newConn to properly initialize context.
	listener := testListener(t)
	c := newConn(mock, listener, 1)

	// Test handleCancelRequest through handleStartup.
	// Cancel requests should close the connection without sending a response.
	err := c.handleStartup()
	require.NoError(t, err)

	// Verify connection was closed (atomic bool should be true).
	assert.True(t, c.closed.Load(), "connection should be closed after cancel request")

	// Verify no response was sent (cancel requests don't get responses).
	output := mock.writeBuf.Bytes()
	assert.Empty(t, output, "should not send any response to cancel request")
}
