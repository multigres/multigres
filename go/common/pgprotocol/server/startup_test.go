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
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
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

// pipeConn wraps two pipes to create a bidirectional net.Conn for testing.
// serverReader/serverWriter are used by the server, clientReader/clientWriter by the test client.
type pipeConn struct {
	reader *io.PipeReader
	writer *io.PipeWriter
}

func (p *pipeConn) Read(b []byte) (n int, err error)  { return p.reader.Read(b) }
func (p *pipeConn) Write(b []byte) (n int, err error) { return p.writer.Write(b) }
func (p *pipeConn) Close() error                      { p.reader.Close(); p.writer.Close(); return nil }
func (p *pipeConn) LocalAddr() net.Addr               { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5432} }
func (p *pipeConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 54321}
}
func (p *pipeConn) SetDeadline(t time.Time) error      { return nil }
func (p *pipeConn) SetReadDeadline(t time.Time) error  { return nil }
func (p *pipeConn) SetWriteDeadline(t time.Time) error { return nil }

// newPipeConnPair creates a connected pair of pipeConns for testing.
// serverConn is used by the server side, clientConn by the test client.
func newPipeConnPair() (serverConn, clientConn *pipeConn) {
	// Client writes -> Server reads
	clientToServerR, clientToServerW := io.Pipe()
	// Server writes -> Client reads
	serverToClientR, serverToClientW := io.Pipe()

	serverConn = &pipeConn{reader: clientToServerR, writer: serverToClientW}
	clientConn = &pipeConn{reader: serverToClientR, writer: clientToServerW}
	return serverConn, clientConn
}

// scramClientHelper performs SCRAM authentication from the client side.
// It reads server messages from clientConn and responds appropriately.
func scramClientHelper(t *testing.T, clientConn io.ReadWriter, username, password string) {
	t.Helper()
	client := scram.NewSCRAMClientWithPassword(username, password)

	// Read AuthenticationSASL from server.
	msgType, body := readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	authType := binary.BigEndian.Uint32(body[:4])
	require.Equal(t, uint32(protocol.AuthSASL), authType)

	// Send SASLInitialResponse with client-first-message.
	clientFirst, err := client.ClientFirstMessage()
	require.NoError(t, err)
	writeSASLInitialResponse(t, clientConn, "SCRAM-SHA-256", clientFirst)

	// Read AuthenticationSASLContinue from server.
	msgType, body = readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	authType = binary.BigEndian.Uint32(body[:4])
	require.Equal(t, uint32(protocol.AuthSASLContinue), authType)
	serverFirst := string(body[4:])

	// Send SASLResponse with client-final-message.
	clientFinal, err := client.ProcessServerFirst(serverFirst)
	require.NoError(t, err)
	writeSASLResponse(t, clientConn, clientFinal)

	// Read AuthenticationSASLFinal from server.
	msgType, body = readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	authType = binary.BigEndian.Uint32(body[:4])
	require.Equal(t, uint32(protocol.AuthSASLFinal), authType)
	serverFinal := string(body[4:])

	// Verify server signature.
	err = client.VerifyServerFinal(serverFinal)
	require.NoError(t, err)

	// Read AuthenticationOk.
	msgType, body = readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	authType = binary.BigEndian.Uint32(body[:4])
	require.Equal(t, uint32(protocol.AuthOk), authType)

	// Read remaining messages until ReadyForQuery.
	for {
		msgType, _ = readMessage(t, clientConn)
		if msgType == byte(protocol.MsgReadyForQuery) {
			break
		}
	}
}

// readMessage reads a PostgreSQL message from the connection.
func readMessage(t *testing.T, conn io.Reader) (byte, []byte) {
	t.Helper()
	header := make([]byte, 5)
	_, err := io.ReadFull(conn, header)
	require.NoError(t, err)

	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:5]) - 4 // Length includes itself

	body := make([]byte, length)
	_, err = io.ReadFull(conn, body)
	require.NoError(t, err)

	return msgType, body
}

// writeSASLInitialResponse writes a SASLInitialResponse message.
func writeSASLInitialResponse(t *testing.T, conn io.Writer, mechanism, data string) {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString(mechanism)
	buf.WriteByte(0)
	_ = binary.Write(&buf, binary.BigEndian, int32(len(data)))
	buf.WriteString(data)

	writeMessage(t, conn, 'p', buf.Bytes())
}

// writeSASLResponse writes a SASLResponse message.
func writeSASLResponse(t *testing.T, conn io.Writer, data string) {
	t.Helper()
	writeMessage(t, conn, 'p', []byte(data))
}

// writeMessage writes a PostgreSQL message to the connection.
func writeMessage(t *testing.T, conn io.Writer, msgType byte, body []byte) {
	t.Helper()
	header := make([]byte, 5)
	header[0] = msgType
	binary.BigEndian.PutUint32(header[1:5], uint32(len(body)+4))
	_, err := conn.Write(header)
	require.NoError(t, err)
	_, err = conn.Write(body)
	require.NoError(t, err)
}

// writeStartupPacketToPipe writes a startup packet to a writer.
func writeStartupPacketToPipe(t *testing.T, conn io.Writer, protocolCode uint32, params map[string]string) {
	t.Helper()
	var buf bytes.Buffer
	writeStartupPacket(&buf, protocolCode, params)
	_, err := conn.Write(buf.Bytes())
	require.NoError(t, err)
}

// testLogger creates a logger for testing.
func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

// testListener creates a test listener using NewListener.
func testListener(t *testing.T) *Listener {
	listener, err := NewListener(ListenerConfig{
		Address:            "localhost:0", // Use random available port
		Handler:            &mockHandler{},
		CredentialProvider: newMockCredentialProvider("postgres"),
		Logger:             testLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		listener.Close()
	})
	return listener
}

// mockHandler is a simple handler for testing.
type mockHandler struct{}

func (m *mockHandler) HandleQuery(ctx context.Context, conn *Conn, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	return nil
}

func (m *mockHandler) HandleParse(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
	return nil
}

func (m *mockHandler) HandleBind(ctx context.Context, conn *Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	return nil
}

func (m *mockHandler) HandleExecute(ctx context.Context, conn *Conn, portalName string, maxRows int32, _ bool, callback func(ctx context.Context, result *sqltypes.Result) error) error {
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

func (m *mockHandler) ConnectionClosed(conn *Conn) {}

func (m *mockHandler) GetPreparedStatementInfo(connID uint32, name string) *preparedstatement.PreparedStatementInfo {
	return nil
}

// mockCredentialProvider implements server.CredentialProvider for testing.
// It returns a fixed SCRAM hash for any user/database combination and a
// configurable rolreplication flag / lookup error so tests can exercise
// both the SCRAM path and the post-auth replication gate.
type mockCredentialProvider struct {
	hash              *scram.ScramHash
	isReplicationRole bool
	err               error

	// Call-tracking fields used by replication tests.
	calls    int
	username string
	database string
}

func newMockCredentialProvider(password string) *mockCredentialProvider {
	// Use a fixed salt for reproducibility in testing.
	salt := []byte("multigres-test-salt!")
	iterations := 4096

	saltedPassword := scram.ComputeSaltedPassword(password, salt, iterations)
	clientKey := scram.ComputeClientKey(saltedPassword)

	return &mockCredentialProvider{
		hash: &scram.ScramHash{
			Iterations: iterations,
			Salt:       salt,
			StoredKey:  scram.ComputeStoredKey(clientKey),
			ServerKey:  scram.ComputeServerKey(saltedPassword),
		},
	}
}

func (p *mockCredentialProvider) GetCredentials(_ context.Context, username, database string) (*Credentials, error) {
	p.calls++
	p.username = username
	p.database = database
	if p.err != nil {
		return nil, p.err
	}
	return &Credentials{
		Hash:              p.hash,
		IsReplicationRole: p.isReplicationRole,
	}, nil
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
			// Create pipe-based connection for bidirectional communication.
			serverConn, clientConn := newPipeConnPair()
			defer serverConn.Close()
			defer clientConn.Close()

			// Create a test listener.
			listener := testListener(t)
			c := &Conn{
				conn:               serverConn,
				listener:           listener,
				handler:            listener.handler,
				credentialProvider: listener.credentialProvider,
				bufferedReader:     bufio.NewReader(serverConn),
				bufferedWriter:     bufio.NewWriter(serverConn),
				params:             make(map[string]string),
				txnStatus:          protocol.TxnStatusIdle,
			}
			c.ctx = context.Background()
			c.logger = testLogger(t)

			// Run server-side handleStartup in a goroutine.
			errCh := make(chan error, 1)
			go func() {
				errCh <- c.handleStartup()
			}()

			// Client side: send startup packet and perform SCRAM auth.
			writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, tt.params)
			scramClientHelper(t, clientConn, tt.expectedUser, "postgres") // password is "postgres" per mockHashProvider

			// Wait for server to complete.
			err := <-errCh
			require.NoError(t, err)

			// Verify connection state was set correctly.
			assert.Equal(t, tt.expectedUser, c.user)
			assert.Equal(t, tt.expectedDB, c.database)
			assert.Equal(t, tt.expectedParams, c.params)
			assert.Equal(t, protocol.ProtocolVersion(protocol.ProtocolVersionNumber), c.protocolVersion)
		})
	}
}

func TestSSLRequest(t *testing.T) {
	// Create pipe-based connection for bidirectional communication.
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	// Create a test listener.
	listener := testListener(t)
	c := &Conn{
		conn:               serverConn,
		listener:           listener,
		handler:            listener.handler,
		credentialProvider: listener.credentialProvider,
		bufferedReader:     bufio.NewReader(serverConn),
		bufferedWriter:     bufio.NewWriter(serverConn),
		params:             make(map[string]string),
		txnStatus:          protocol.TxnStatusIdle,
	}
	c.ctx = context.Background()
	c.logger = testLogger(t)

	// Run server-side handleStartup in a goroutine.
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	// Client side: send SSL request.
	var sslReqBuf bytes.Buffer
	_ = binary.Write(&sslReqBuf, binary.BigEndian, uint32(8))
	_ = binary.Write(&sslReqBuf, binary.BigEndian, uint32(protocol.SSLRequestCode))
	_, err := clientConn.Write(sslReqBuf.Bytes())
	require.NoError(t, err)

	// Read 'N' response for SSL decline.
	sslResponse := make([]byte, 1)
	_, err = io.ReadFull(clientConn, sslResponse)
	require.NoError(t, err)
	assert.Equal(t, byte('N'), sslResponse[0], "should send 'N' to decline SSL")

	// Now send startup packet and complete SCRAM auth.
	params := map[string]string{
		"user":     "testuser",
		"database": "testdb",
	}
	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, params)
	scramClientHelper(t, clientConn, "testuser", "postgres")

	// Wait for server to complete.
	err = <-errCh
	require.NoError(t, err)

	// Verify startup completed successfully.
	assert.Equal(t, "testuser", c.user)
	assert.Equal(t, "testdb", c.database)
}

func TestGSSENCRequest(t *testing.T) {
	// Create pipe-based connection for bidirectional communication.
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	// Create a test listener.
	listener := testListener(t)
	c := &Conn{
		conn:               serverConn,
		listener:           listener,
		handler:            listener.handler,
		credentialProvider: listener.credentialProvider,
		bufferedReader:     bufio.NewReader(serverConn),
		bufferedWriter:     bufio.NewWriter(serverConn),
		params:             make(map[string]string),
		txnStatus:          protocol.TxnStatusIdle,
	}
	c.ctx = context.Background()
	c.logger = testLogger(t)

	// Run server-side handleStartup in a goroutine.
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	// Client side: send GSSENC request.
	var gssReqBuf bytes.Buffer
	_ = binary.Write(&gssReqBuf, binary.BigEndian, uint32(8))
	_ = binary.Write(&gssReqBuf, binary.BigEndian, uint32(protocol.GSSENCRequestCode))
	_, err := clientConn.Write(gssReqBuf.Bytes())
	require.NoError(t, err)

	// Read 'N' response for GSSENC decline.
	gssResponse := make([]byte, 1)
	_, err = io.ReadFull(clientConn, gssResponse)
	require.NoError(t, err)
	assert.Equal(t, byte('N'), gssResponse[0], "should send 'N' to decline GSSENC")

	// Now send startup packet and complete SCRAM auth.
	params := map[string]string{
		"user":     "gssuser",
		"database": "gssdb",
	}
	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, params)
	scramClientHelper(t, clientConn, "gssuser", "postgres")

	// Wait for server to complete.
	err = <-errCh
	require.NoError(t, err)

	// Verify startup completed successfully.
	assert.Equal(t, "gssuser", c.user)
	assert.Equal(t, "gssdb", c.database)
}

func TestSCRAMAuthenticationWrongPassword(t *testing.T) {
	// Create pipe-based connection for bidirectional communication.
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	// Create a test listener with password "postgres".
	listener := testListener(t)
	c := &Conn{
		conn:               serverConn,
		listener:           listener,
		handler:            listener.handler,
		credentialProvider: listener.credentialProvider,
		bufferedReader:     bufio.NewReader(serverConn),
		bufferedWriter:     bufio.NewWriter(serverConn),
		params:             make(map[string]string),
		txnStatus:          protocol.TxnStatusIdle,
	}
	c.ctx = context.Background()
	c.logger = testLogger(t)

	// Run server-side handleStartup in a goroutine.
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	// Client side: send startup packet.
	params := map[string]string{
		"user":     "testuser",
		"database": "testdb",
	}
	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, params)

	// Perform SCRAM handshake with WRONG password.
	client := scram.NewSCRAMClientWithPassword("testuser", "wrongpassword")

	// Read AuthenticationSASL from server.
	msgType, body := readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	authType := binary.BigEndian.Uint32(body[:4])
	require.Equal(t, uint32(protocol.AuthSASL), authType)

	// Send SASLInitialResponse with client-first-message.
	clientFirst, err := client.ClientFirstMessage()
	require.NoError(t, err)
	writeSASLInitialResponse(t, clientConn, "SCRAM-SHA-256", clientFirst)

	// Read AuthenticationSASLContinue from server.
	msgType, body = readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	authType = binary.BigEndian.Uint32(body[:4])
	require.Equal(t, uint32(protocol.AuthSASLContinue), authType)
	serverFirst := string(body[4:])

	// Send SASLResponse with client-final-message (wrong proof due to wrong password).
	clientFinal, err := client.ProcessServerFirst(serverFirst)
	require.NoError(t, err)
	writeSASLResponse(t, clientConn, clientFinal)

	// Server should send ErrorResponse.
	msgType, body = readMessage(t, clientConn)
	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)

	// Verify error contains auth failure message.
	assert.Contains(t, string(body), "password authentication failed")

	// handleStartup propagates errAuthRejected so serve() can short-circuit
	// without entering the command loop on a half-completed session.
	err = <-errCh
	require.ErrorIs(t, err, errAuthRejected)
}

func TestAuthenticationMessages(t *testing.T) {
	// Create mock connection.
	mock := newMockConn()

	// Create a test listener.
	listener := testListener(t)
	c := &Conn{
		conn:               mock,
		listener:           listener,
		handler:            listener.handler,
		credentialProvider: listener.credentialProvider,
		bufferedReader:     bufio.NewReader(mock),
		bufferedWriter:     bufio.NewWriter(mock),
		params:             make(map[string]string),
		connectionID:       12345,
		backendKeyData:     67890,
		txnStatus:          protocol.TxnStatusIdle,
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

func TestGetStartupParams(t *testing.T) {
	tests := []struct {
		name     string
		params   map[string]string
		expected map[string]string
	}{
		{
			name: "excludes user and database",
			params: map[string]string{
				"user":             "postgres",
				"database":         "testdb",
				"application_name": "psql",
				"client_encoding":  "UTF8",
			},
			expected: map[string]string{
				"application_name": "psql",
				"client_encoding":  "UTF8",
			},
		},
		{
			name: "only user and database returns nil",
			params: map[string]string{
				"user":     "postgres",
				"database": "testdb",
			},
			expected: nil,
		},
		{
			name:     "empty params returns nil",
			params:   map[string]string{},
			expected: nil,
		},
		{
			name: "no user or database returns all params",
			params: map[string]string{
				"application_name": "myapp",
				"DateStyle":        "German",
			},
			expected: map[string]string{
				"application_name": "myapp",
				"DateStyle":        "German",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Conn{
				params: tt.params,
			}
			result := c.GetStartupParams()
			assert.Equal(t, tt.expected, result)

			// Verify returned map is a copy (not a reference to internal state).
			if result != nil {
				result["extra"] = "value"
				assert.NotContains(t, c.params, "extra")
			}
		})
	}
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

func TestSplitOptionsTokens(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple tokens",
			input:    "-c work_mem=64MB",
			expected: []string{"-c", "work_mem=64MB"},
		},
		{
			name:     "escaped space",
			input:    `-c DateStyle=ISO,\ MDY`,
			expected: []string{"-c", "DateStyle=ISO, MDY"},
		},
		{
			name:     "escaped backslash",
			input:    `-c search_path=a\\b`,
			expected: []string{"-c", `search_path=a\b`},
		},
		{
			name:     "multiple spaces between tokens",
			input:    "-c   work_mem=64MB",
			expected: []string{"-c", "work_mem=64MB"},
		},
		{
			name:     "tabs as separators",
			input:    "-c\twork_mem=64MB",
			expected: []string{"-c", "work_mem=64MB"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "whitespace only",
			input:    "   \t  ",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitOptionsTokens(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseOptions(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  map[string]string
		expectErr string
	}{
		{
			name:     "single -c with space",
			input:    "-c work_mem=64MB",
			expected: map[string]string{"work_mem": "64MB"},
		},
		{
			name:     "-c without space",
			input:    "-cwork_mem=64MB",
			expected: map[string]string{"work_mem": "64MB"},
		},
		{
			name:     "double-dash format",
			input:    "--work-mem=64MB",
			expected: map[string]string{"work_mem": "64MB"},
		},
		{
			name:     "double-dash with hyphens in key",
			input:    "--statement-timeout=5min",
			expected: map[string]string{"statement_timeout": "5min"},
		},
		{
			name:  "multiple flags",
			input: "-c work_mem=64MB -c geqo=off",
			expected: map[string]string{
				"work_mem": "64MB",
				"geqo":     "off",
			},
		},
		{
			name:  "mixed -c and -- formats",
			input: `-c work_mem=64MB --statement-timeout=5min`,
			expected: map[string]string{
				"work_mem":          "64MB",
				"statement_timeout": "5min",
			},
		},
		{
			name:     "escaped space in value",
			input:    `-c DateStyle=ISO,\ MDY`,
			expected: map[string]string{"DateStyle": "ISO, MDY"},
		},
		{
			name:     "empty value",
			input:    "-c search_path=",
			expected: map[string]string{"search_path": ""},
		},
		{
			name:     "empty string",
			input:    "",
			expected: map[string]string{},
		},
		{
			name:     "whitespace only",
			input:    "   ",
			expected: map[string]string{},
		},
		{
			name:      "missing value after -c",
			input:     "-c",
			expectErr: "missing value after -c",
		},
		{
			name:      "unsupported flag",
			input:     "-x something",
			expectErr: "unsupported option flag",
		},
		{
			name:      "empty key after --",
			input:     "--=value",
			expectErr: "invalid -- option",
		},
		{
			name:      "-c with invalid format",
			input:     "-c noequals",
			expectErr: "invalid -c option",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseOptions(tt.input)
			if tt.expectErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestSCRAMAuthenticationCapturesKeys verifies that the Conn retains the
// ClientKey and ServerKey extracted during the SCRAM handshake. These keys
// feed the SCRAM passthrough path (multipooler → postgres) and must be
// populated on every successful SCRAM authentication.
func TestSCRAMAuthenticationCapturesKeys(t *testing.T) {
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	listener := testListener(t)
	c := &Conn{
		conn:               serverConn,
		listener:           listener,
		handler:            listener.handler,
		credentialProvider: listener.credentialProvider,
		bufferedReader:     bufio.NewReader(serverConn),
		bufferedWriter:     bufio.NewWriter(serverConn),
		params:             make(map[string]string),
		txnStatus:          protocol.TxnStatusIdle,
	}
	c.ctx = context.Background()
	c.logger = testLogger(t)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	params := map[string]string{
		"user":     "scramuser",
		"database": "scramdb",
	}
	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, params)
	scramClientHelper(t, clientConn, "scramuser", "postgres")

	require.NoError(t, <-errCh)

	// SCRAM-SHA-256 keys are always 32 bytes (output of HMAC-SHA-256).
	assert.Len(t, c.scramClientKey, 32, "ClientKey should be captured after SCRAM auth")
	assert.Len(t, c.scramServerKey, 32, "ServerKey should be captured after SCRAM auth")
	assert.NotEqual(t, make([]byte, 32), c.scramClientKey, "ClientKey should not be all zeros")
	assert.NotEqual(t, make([]byte, 32), c.scramServerKey, "ServerKey should not be all zeros")
}

// TestConnCloseZeroizesSCRAMKeys verifies that Close scrubs the SCRAM
// passthrough keys so that a later memory dump cannot recover credentials
// from a closed session.
func TestConnCloseZeroizesSCRAMKeys(t *testing.T) {
	serverConn, clientConn := newPipeConnPair()
	defer clientConn.Close()

	listener := testListener(t)
	c := &Conn{
		conn:           serverConn,
		listener:       listener,
		bufferedReader: bufio.NewReader(serverConn),
		bufferedWriter: bufio.NewWriter(serverConn),
		params:         make(map[string]string),
		scramClientKey: []byte{1, 2, 3, 4},
		scramServerKey: []byte{5, 6, 7, 8},
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.logger = testLogger(t)

	// Hold references to the underlying slices to prove the bytes are scrubbed
	// in place, not just detached.
	clientBacking := c.scramClientKey
	serverBacking := c.scramServerKey

	require.NoError(t, c.Close())

	assert.Nil(t, c.scramClientKey)
	assert.Nil(t, c.scramServerKey)
	assert.Equal(t, []byte{0, 0, 0, 0}, clientBacking, "ClientKey bytes must be scrubbed in place")
	assert.Equal(t, []byte{0, 0, 0, 0}, serverBacking, "ServerKey bytes must be scrubbed in place")
}

// TestConnCloseSafeWithoutSCRAMKeys ensures Close does not panic when the
// connection never completed SCRAM auth (keys remain nil).
// parseErrorFields decodes the field-tag/value pairs of an ErrorResponse
// body into a map keyed by tag (e.g. 'C' for SQLSTATE, 'M' for message).
func parseErrorFields(body []byte) map[byte]string {
	fields := make(map[byte]string)
	i := 0
	for i < len(body) {
		tag := body[i]
		if tag == 0 {
			break
		}
		i++
		end := i
		for end < len(body) && body[end] != 0 {
			end++
		}
		fields[tag] = string(body[i:end])
		i = end + 1
	}
	return fields
}

// newReplicationTestConn constructs a Conn wired up against a pipe pair and
// the given credential provider. The returned errCh receives the result
// of handleStartup once the goroutine completes.
//
// Pass a *mockCredentialProvider configured with isReplicationRole/err to
// exercise the replication-role gate; pass nil to test the no-provider
// fail-closed path.
func newReplicationTestConn(t *testing.T, provider CredentialProvider) (*Conn, *pipeConn, chan error) {
	t.Helper()
	serverConn, clientConn := newPipeConnPair()

	config := ListenerConfig{
		Address:            "localhost:0",
		Handler:            &mockHandler{},
		CredentialProvider: provider,
		Logger:             testLogger(t),
	}
	// The listener requires a credential provider (or trust auth); supply
	// a passthrough trust provider when the caller wants to test the
	// no-credential-provider path.
	if provider == nil {
		config.TrustAuthProvider = allowAllTrust{}
	}
	listener, err := NewListener(config)
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	c := &Conn{
		conn:               serverConn,
		listener:           listener,
		handler:            listener.handler,
		credentialProvider: listener.credentialProvider,
		trustAuthProvider:  listener.trustAuthProvider,
		bufferedReader:     bufio.NewReader(serverConn),
		bufferedWriter:     bufio.NewWriter(serverConn),
		params:             make(map[string]string),
		txnStatus:          protocol.TxnStatusIdle,
	}
	c.ctx = context.Background()
	c.logger = testLogger(t)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
		serverConn.Close()
	}()

	t.Cleanup(func() { clientConn.Close() })
	return c, clientConn, errCh
}

// allowAllTrust is a TrustAuthProvider used to keep listener construction
// valid in tests that want to exercise the no-credential-provider path.
type allowAllTrust struct{}

func (allowAllTrust) AllowTrustAuth(_ context.Context, _, _ string) bool { return true }

func TestParseReplicationMode(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    ReplicationMode
		wantErr bool
	}{
		{"empty", "", ReplicationOff, false},
		{"false-lowercase", "false", ReplicationOff, false},
		{"FALSE-uppercase", "FALSE", ReplicationOff, false},
		{"off", "off", ReplicationOff, false},
		{"no", "no", ReplicationOff, false},
		{"zero", "0", ReplicationOff, false},
		{"f-abbrev", "f", ReplicationOff, false},
		{"n-abbrev", "n", ReplicationOff, false},
		{"true", "true", ReplicationPhysical, false},
		{"True-mixedcase", "True", ReplicationPhysical, false},
		{"on", "on", ReplicationPhysical, false},
		{"yes", "yes", ReplicationPhysical, false},
		{"one", "1", ReplicationPhysical, false},
		{"t-abbrev", "t", ReplicationPhysical, false},
		{"y-abbrev", "y", ReplicationPhysical, false},
		{"database", "database", ReplicationLogical, false},
		{"DATABASE-uppercase", "DATABASE", ReplicationLogical, false},
		{"banana-rejected", "banana", ReplicationOff, true},
		{"two-rejected", "2", ReplicationOff, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseReplicationMode(tt.value)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestReplicationStartup_IgnoredViaPGOPTIONS verifies that `-c replication=true`
// inside PGOPTIONS is dropped — PG only honors `replication` as a direct
// startup field, not as a GUC. The auth gate still rejects bad combinations
// elsewhere; this test pins the behavior so we don't admit a source PG
// itself rejects.
func TestReplicationStartup_IgnoredViaPGOPTIONS(t *testing.T) {
	// isReplicationRole=false would reject if the replication gate ran.
	provider := newMockCredentialProvider("postgres")
	c, clientConn, errCh := newReplicationTestConn(t, provider)

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "regular",
		"database": "postgres",
		"options":  "-c replication=true",
	})
	// PGOPTIONS-injected `replication` is filtered, so the connection is
	// treated as a normal session and SCRAM completes through ReadyForQuery.
	scramClientHelper(t, clientConn, "regular", "postgres")

	require.NoError(t, <-errCh)
	// SCRAM fetched credentials once; the replication gate read the
	// cached flag so no second fetch happened.
	assert.Equal(t, 1, provider.calls)
	assert.Equal(t, ReplicationOff, c.replicationMode)
	_, hasReplication := c.params["replication"]
	assert.False(t, hasReplication)
}

// TestReplicationStartup_RejectedWithoutRolReplication verifies that a
// SCRAM-authenticated client requesting replication=true is rejected with
// the exact PG message and SQLSTATE 42501 when the role lacks
// rolreplication. Acceptance criterion #1 of MUL-387.
func TestReplicationStartup_RejectedWithoutRolReplication(t *testing.T) {
	provider := newMockCredentialProvider("postgres") // isReplicationRole=false
	c, clientConn, errCh := newReplicationTestConn(t, provider)

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":        "repluser",
		"database":    "postgres",
		"replication": "true",
	})

	// SCRAM completes successfully — the rejection happens AFTER auth.
	client := scram.NewSCRAMClientWithPassword("repluser", "postgres")
	msgType, body := readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	require.Equal(t, uint32(protocol.AuthSASL), binary.BigEndian.Uint32(body[:4]))

	clientFirst, err := client.ClientFirstMessage()
	require.NoError(t, err)
	writeSASLInitialResponse(t, clientConn, "SCRAM-SHA-256", clientFirst)

	msgType, body = readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	require.Equal(t, uint32(protocol.AuthSASLContinue), binary.BigEndian.Uint32(body[:4]))
	clientFinal, err := client.ProcessServerFirst(string(body[4:]))
	require.NoError(t, err)
	writeSASLResponse(t, clientConn, clientFinal)

	msgType, _ = readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType) // SASLFinal

	// Next frame must be FATAL ErrorResponse, not AuthenticationOk.
	msgType, body = readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	fields := parseErrorFields(body)
	assert.Equal(t, "FATAL", fields['S'])
	assert.Equal(t, "42501", fields['C'])
	assert.Equal(t, "must be superuser or replication role to start walsender", fields['M'])

	require.ErrorIs(t, <-errCh, errAuthRejected)
	// One credential fetch served both SCRAM and the replication gate.
	assert.Equal(t, 1, provider.calls)
	assert.Equal(t, "repluser", provider.username)
	assert.Equal(t, "postgres", provider.database)
	assert.Equal(t, ReplicationPhysical, c.replicationMode)
}

// TestReplicationStartup_AcceptedWithRolReplication verifies the gateway
// accepts a role with rolreplication=true and the connection completes
// through ReadyForQuery. Acceptance criterion #2 of MUL-387.
func TestReplicationStartup_AcceptedWithRolReplication(t *testing.T) {
	provider := newMockCredentialProvider("postgres")
	provider.isReplicationRole = true
	c, clientConn, errCh := newReplicationTestConn(t, provider)

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":        "repluser",
		"database":    "postgres",
		"replication": "true",
	})
	// scramClientHelper drives SCRAM through ReadyForQuery, asserting the
	// AuthenticationOk + ParameterStatus + ReadyForQuery sequence is sent.
	scramClientHelper(t, clientConn, "repluser", "postgres")

	require.NoError(t, <-errCh)
	// One credential fetch suffices for both SCRAM and the replication gate.
	assert.Equal(t, 1, provider.calls)
	assert.Equal(t, ReplicationPhysical, c.replicationMode)
	// Regression: `replication` is a protocol-only param, not a GUC. It
	// must be stripped from c.params after parsing so it doesn't propagate
	// through GetStartupParams into a `SET SESSION "replication" = ...`
	// on the backing PostgreSQL, which PG would reject as unrecognized.
	_, hasReplication := c.params["replication"]
	assert.False(t, hasReplication, "replication startup param must be stripped from c.params")
}

// TestReplicationStartup_DatabaseModeAcceptedWithRolReplication exercises
// the logical-replication value (replication=database) which is what
// CREATE SUBSCRIPTION sends.
func TestReplicationStartup_DatabaseModeAcceptedWithRolReplication(t *testing.T) {
	provider := newMockCredentialProvider("postgres")
	provider.isReplicationRole = true
	c, clientConn, errCh := newReplicationTestConn(t, provider)

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":        "repluser",
		"database":    "appdb",
		"replication": "database",
	})
	scramClientHelper(t, clientConn, "repluser", "postgres")

	require.NoError(t, <-errCh)
	assert.Equal(t, 1, provider.calls)
	assert.Equal(t, "appdb", provider.database)
	assert.Equal(t, ReplicationLogical, c.replicationMode)
}

// TestReplicationStartup_NormalConnectionUsesOneFetch verifies that a
// non-replication startup pays exactly one credential fetch (for SCRAM)
// and does not incur a second lookup for the replication gate.
func TestReplicationStartup_NormalConnectionUsesOneFetch(t *testing.T) {
	provider := newMockCredentialProvider("postgres") // isReplicationRole=false would reject if consulted
	_, clientConn, errCh := newReplicationTestConn(t, provider)

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "regular",
		"database": "postgres",
	})
	scramClientHelper(t, clientConn, "regular", "postgres")

	require.NoError(t, <-errCh)
	assert.Equal(t, 1, provider.calls, "exactly one credential fetch should serve SCRAM and replication gate")
}

// TestReplicationStartup_TrustAuthNoCredentialProviderFailsClosed verifies
// that a trust-authenticated replication startup is rejected when no
// credential provider is wired up — the gateway has no way to confirm
// rolreplication, so it cannot admit a possibly-unauthorized walsender
// session. Trust auth is test-only; production always wires a credential
// provider, but the fail-closed branch must still hold.
func TestReplicationStartup_TrustAuthNoCredentialProviderFailsClosed(t *testing.T) {
	c, clientConn, errCh := newReplicationTestConn(t, nil)

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":        "repluser",
		"database":    "postgres",
		"replication": "true",
	})

	// Trust auth: no SCRAM exchange. The rejection arrives on the first
	// post-startup frame.
	msgType, body := readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	fields := parseErrorFields(body)
	assert.Equal(t, "42501", fields['C'])
	assert.Equal(t, "must be superuser or replication role to start walsender", fields['M'])

	require.ErrorIs(t, <-errCh, errAuthRejected)
	assert.Equal(t, ReplicationPhysical, c.replicationMode)
}

// TestReplicationStartup_RejectedOnCredentialLookupError verifies that the
// gateway fails closed when the credential provider returns a generic
// error (e.g. the pooler is unreachable). Because the lookup happens up
// front, the rejection arrives before SCRAM completes — as an opaque
// "password authentication failed" rather than the 42501 the replication
// gate emits when the role lacks rolreplication. Both outcomes prevent
// admission of an unauthorized session.
func TestReplicationStartup_RejectedOnCredentialLookupError(t *testing.T) {
	provider := newMockCredentialProvider("postgres")
	provider.err = errors.New("pooler unreachable")
	_, clientConn, errCh := newReplicationTestConn(t, provider)

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":        "repluser",
		"database":    "postgres",
		"replication": "true",
	})

	// No AuthenticationSASL because the credential lookup short-circuits
	// the SCRAM exchange. The first server frame is the ErrorResponse.
	msgType, body := readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	fields := parseErrorFields(body)
	assert.Equal(t, "28P01", fields['C'])
	assert.Contains(t, fields['M'], "password authentication failed")

	require.ErrorIs(t, <-errCh, errAuthRejected)
	assert.Equal(t, 1, provider.calls)
}

// TestReplicationStartup_InvalidValueRejected confirms an unrecognized
// `replication` value produces InvalidParameterValue (22023) before
// authentication runs, matching PostgreSQL's GUC parsing. The error
// surfaces as a PgDiagnostic from handleStartup; in production, serve()
// writes it to the client and closes the connection.
func TestReplicationStartup_InvalidValueRejected(t *testing.T) {
	provider := newMockCredentialProvider("postgres")
	provider.isReplicationRole = true
	_, clientConn, errCh := newReplicationTestConn(t, provider)

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":        "repluser",
		"database":    "postgres",
		"replication": "banana",
	})

	err := <-errCh
	require.Error(t, err)
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag)
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, mterrors.PgSSInvalidParameterValue, diag.Code)
	assert.Contains(t, diag.Message, `invalid value for parameter "replication"`)
	assert.Contains(t, diag.Message, "banana")
	assert.Equal(t, 0, provider.calls, "credential provider must not be called on parse error")
}

func TestConnCloseSafeWithoutSCRAMKeys(t *testing.T) {
	serverConn, clientConn := newPipeConnPair()
	defer clientConn.Close()

	listener := testListener(t)
	c := &Conn{
		conn:           serverConn,
		listener:       listener,
		bufferedReader: bufio.NewReader(serverConn),
		bufferedWriter: bufio.NewWriter(serverConn),
		params:         make(map[string]string),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.logger = testLogger(t)

	assert.NotPanics(t, func() {
		require.NoError(t, c.Close())
	})
}
