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
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
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
func scramClientHelper(t *testing.T, clientConn *pipeConn, username, password string) {
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
func readMessage(t *testing.T, conn *pipeConn) (byte, []byte) {
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
func writeSASLInitialResponse(t *testing.T, conn *pipeConn, mechanism, data string) {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString(mechanism)
	buf.WriteByte(0)
	_ = binary.Write(&buf, binary.BigEndian, int32(len(data)))
	buf.WriteString(data)

	writeMessage(t, conn, 'p', buf.Bytes())
}

// writeSASLResponse writes a SASLResponse message.
func writeSASLResponse(t *testing.T, conn *pipeConn, data string) {
	t.Helper()
	writeMessage(t, conn, 'p', []byte(data))
}

// writeMessage writes a PostgreSQL message to the connection.
func writeMessage(t *testing.T, conn *pipeConn, msgType byte, body []byte) {
	t.Helper()
	header := make([]byte, 5)
	header[0] = msgType
	binary.BigEndian.PutUint32(header[1:5], uint32(len(body)+4))
	_, err := conn.Write(header)
	require.NoError(t, err)
	_, err = conn.Write(body)
	require.NoError(t, err)
}

// writeStartupPacketToPipe writes a startup packet to a pipeConn.
func writeStartupPacketToPipe(t *testing.T, conn *pipeConn, protocolCode uint32, params map[string]string) {
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
		Address:      "localhost:0", // Use random available port
		Handler:      &mockHandler{},
		HashProvider: newMockHashProvider("postgres"),
		Logger:       testLogger(t),
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

func (m *mockHandler) HandleExecute(ctx context.Context, conn *Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) error {
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

func (m *mockHandler) HandleStartup(ctx context.Context, conn *Conn) (map[string]string, error) {
	return nil, nil
}

// mockHashProvider implements scram.PasswordHashProvider for testing.
// It accepts a single password for any user/database combination.
type mockHashProvider struct {
	hash *scram.ScramHash
}

func newMockHashProvider(password string) *mockHashProvider {
	// Use a fixed salt for reproducibility in testing.
	salt := []byte("multigres-test-salt!")
	iterations := 4096

	saltedPassword := scram.ComputeSaltedPassword(password, salt, iterations)
	clientKey := scram.ComputeClientKey(saltedPassword)

	return &mockHashProvider{
		hash: &scram.ScramHash{
			Iterations: iterations,
			Salt:       salt,
			StoredKey:  scram.ComputeStoredKey(clientKey),
			ServerKey:  scram.ComputeServerKey(saltedPassword),
		},
	}
}

func (p *mockHashProvider) GetPasswordHash(_ context.Context, _, _ string) (*scram.ScramHash, error) {
	return p.hash, nil
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
				conn:           serverConn,
				listener:       listener,
				handler:        listener.handler,
				hashProvider:   listener.hashProvider,
				bufferedReader: bufio.NewReader(serverConn),
				bufferedWriter: bufio.NewWriter(serverConn),
				params:         make(map[string]string),
				txnStatus:      protocol.TxnStatusIdle,
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
		conn:           serverConn,
		listener:       listener,
		handler:        listener.handler,
		hashProvider:   listener.hashProvider,
		bufferedReader: bufio.NewReader(serverConn),
		bufferedWriter: bufio.NewWriter(serverConn),
		params:         make(map[string]string),
		txnStatus:      protocol.TxnStatusIdle,
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
		conn:           serverConn,
		listener:       listener,
		handler:        listener.handler,
		hashProvider:   listener.hashProvider,
		bufferedReader: bufio.NewReader(serverConn),
		bufferedWriter: bufio.NewWriter(serverConn),
		params:         make(map[string]string),
		txnStatus:      protocol.TxnStatusIdle,
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
		conn:           serverConn,
		listener:       listener,
		handler:        listener.handler,
		hashProvider:   listener.hashProvider,
		bufferedReader: bufio.NewReader(serverConn),
		bufferedWriter: bufio.NewWriter(serverConn),
		params:         make(map[string]string),
		txnStatus:      protocol.TxnStatusIdle,
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

	// Server should return nil (auth error was sent successfully).
	err = <-errCh
	require.NoError(t, err)
}

func TestAuthenticationMessages(t *testing.T) {
	// Create mock connection.
	mock := newMockConn()

	// Create a test listener.
	listener := testListener(t)
	c := &Conn{
		conn:           mock,
		listener:       listener,
		hashProvider:   listener.hashProvider,
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

func TestParseOptions(t *testing.T) {
	tests := []struct {
		name     string
		options  string
		expected map[string]string
		wantErr  bool
	}{
		{
			name:     "single -c with space",
			options:  "-c work_mem=64MB",
			expected: map[string]string{"work_mem": "64MB"},
		},
		{
			name:     "single -c without space",
			options:  "-cwork_mem=64MB",
			expected: map[string]string{"work_mem": "64MB"},
		},
		{
			name:     "double dash format",
			options:  "--work_mem=64MB",
			expected: map[string]string{"work_mem": "64MB"},
		},
		{
			name:     "double dash with hyphens in key",
			options:  "--statement-timeout=5min",
			expected: map[string]string{"statement_timeout": "5min"},
		},
		{
			name:     "double dash with multiple hyphens",
			options:  "--idle-in-transaction-session-timeout=10s",
			expected: map[string]string{"idle_in_transaction_session_timeout": "10s"},
		},
		{
			name:    "multiple -c flags",
			options: "-c work_mem=64MB -c statement_timeout=5000",
			expected: map[string]string{
				"work_mem":          "64MB",
				"statement_timeout": "5000",
			},
		},
		{
			name:    "mixed formats",
			options: "-c work_mem=64MB --statement_timeout=5000 -cDateStyle=ISO",
			expected: map[string]string{
				"work_mem":          "64MB",
				"statement_timeout": "5000",
				"DateStyle":         "ISO",
			},
		},
		{
			name:    "mixed formats with hyphens",
			options: "-c geqo=off --statement-timeout=5min",
			expected: map[string]string{
				"geqo":              "off",
				"statement_timeout": "5min",
			},
		},
		{
			name:     "backslash escaped space in value",
			options:  `-c DateStyle=ISO,\ MDY`,
			expected: map[string]string{"DateStyle": "ISO, MDY"},
		},
		{
			name:     "backslash escaped tab in value",
			options:  `-c search_path=public,\	myschema`,
			expected: map[string]string{"search_path": "public,\tmyschema"},
		},
		{
			name:     "empty value",
			options:  "-c search_path=",
			expected: map[string]string{"search_path": ""},
		},
		{
			name:     "empty options string",
			options:  "",
			expected: map[string]string{},
		},
		{
			name:     "whitespace only",
			options:  "   \t  ",
			expected: map[string]string{},
		},
		{
			name:    "multiple spaces between options",
			options: "-c work_mem=64MB   -c  statement_timeout=5000",
			expected: map[string]string{
				"work_mem":          "64MB",
				"statement_timeout": "5000",
			},
		},
		{
			name:    "-c missing value",
			options: "-c",
			wantErr: true,
		},
		{
			name:    "-c with value missing equals",
			options: "-c work_mem",
			wantErr: true,
		},
		{
			name:    "unsupported option flag",
			options: "-x work_mem=64MB",
			wantErr: true,
		},
		{
			name:    "-- with empty key",
			options: "--=value",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseOptions(tt.options)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitOptionsTokens(t *testing.T) {
	tests := []struct {
		name     string
		options  string
		expected []string
	}{
		{
			name:     "simple tokens",
			options:  "-c work_mem=64MB",
			expected: []string{"-c", "work_mem=64MB"},
		},
		{
			name:     "escaped space",
			options:  `-c DateStyle=ISO,\ MDY`,
			expected: []string{"-c", "DateStyle=ISO, MDY"},
		},
		{
			name:     "escaped backslash",
			options:  `-c path=C:\\temp`,
			expected: []string{"-c", `path=C:\temp`},
		},
		{
			name:     "multiple spaces",
			options:  "  -c   work_mem=64MB  ",
			expected: []string{"-c", "work_mem=64MB"},
		},
		{
			name:     "tabs as delimiters",
			options:  "-c\twork_mem=64MB",
			expected: []string{"-c", "work_mem=64MB"},
		},
		{
			name:     "empty string",
			options:  "",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitOptionsTokens(tt.options)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHandleStartupMessageWithOptions(t *testing.T) {
	// Create pipe-based connection for bidirectional communication.
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	// Create a test listener.
	listener := testListener(t)
	c := &Conn{
		conn:           serverConn,
		listener:       listener,
		handler:        listener.handler,
		hashProvider:   listener.hashProvider,
		bufferedReader: bufio.NewReader(serverConn),
		bufferedWriter: bufio.NewWriter(serverConn),
		params:         make(map[string]string),
		txnStatus:      protocol.TxnStatusIdle,
	}
	c.ctx = context.Background()
	c.logger = testLogger(t)

	// Run server-side handleStartup in a goroutine.
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	// Client side: send startup packet with options parameter.
	params := map[string]string{
		"user":     "postgres",
		"database": "testdb",
		"options":  "-c work_mem=64MB -c statement_timeout=5000",
	}
	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, params)
	scramClientHelper(t, clientConn, "postgres", "postgres")

	// Wait for server to complete.
	err := <-errCh
	require.NoError(t, err)

	// Verify options were parsed and merged into params.
	assert.Equal(t, "postgres", c.user)
	assert.Equal(t, "testdb", c.database)
	assert.Equal(t, "64MB", c.params["work_mem"])
	assert.Equal(t, "5000", c.params["statement_timeout"])
	// "options" key should be removed after parsing.
	_, hasOptions := c.params["options"]
	assert.False(t, hasOptions, "options key should be removed after parsing")
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
