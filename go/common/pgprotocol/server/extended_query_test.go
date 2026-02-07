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
	"sync"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testHandler is a mock handler for testing extended query protocol.
type testHandler struct {
	parseFunc    func(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error
	bindFunc     func(ctx context.Context, conn *Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error
	executeFunc  func(ctx context.Context, conn *Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) error
	describeFunc func(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error)
	closeFunc    func(ctx context.Context, conn *Conn, typ byte, name string) error
	syncFunc     func(ctx context.Context, conn *Conn) error
}

func (h *testHandler) HandleQuery(ctx context.Context, conn *Conn, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	return nil
}

func (h *testHandler) HandleParse(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
	if h.parseFunc != nil {
		return h.parseFunc(ctx, conn, name, queryStr, paramTypes)
	}
	return nil
}

func (h *testHandler) HandleBind(ctx context.Context, conn *Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	if h.bindFunc != nil {
		return h.bindFunc(ctx, conn, portalName, stmtName, params, paramFormats, resultFormats)
	}
	return nil
}

func (h *testHandler) HandleExecute(ctx context.Context, conn *Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	if h.executeFunc != nil {
		return h.executeFunc(ctx, conn, portalName, maxRows, callback)
	}
	// Return a simple result for testing via callback.
	return callback(ctx, &sqltypes.Result{
		Fields: []*query.Field{
			{Name: "id", DataTypeOid: 23},
			{Name: "name", DataTypeOid: 25},
		},
		Rows: []*sqltypes.Row{
			{Values: []sqltypes.Value{[]byte("1"), []byte("test")}},
		},
		CommandTag: "SELECT 1",
	})
}

func (h *testHandler) HandleDescribe(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error) {
	if h.describeFunc != nil {
		return h.describeFunc(ctx, conn, typ, name)
	}
	return &query.StatementDescription{}, nil
}

func (h *testHandler) HandleClose(ctx context.Context, conn *Conn, typ byte, name string) error {
	if h.closeFunc != nil {
		return h.closeFunc(ctx, conn, typ, name)
	}
	return nil
}

func (h *testHandler) HandleSync(ctx context.Context, conn *Conn) error {
	if h.syncFunc != nil {
		return h.syncFunc(ctx, conn)
	}
	return nil
}

func (h *testHandler) HandleStartup(ctx context.Context, conn *Conn) (map[string]string, error) {
	return nil, nil
}

// testConn wraps both read and write buffers for testing.
type testConn struct {
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
}

func (tc *testConn) Read(b []byte) (n int, err error) {
	return tc.readBuf.Read(b)
}

func (tc *testConn) Write(b []byte) (n int, err error) {
	return tc.writeBuf.Write(b)
}

func (tc *testConn) Close() error                       { return nil }
func (tc *testConn) LocalAddr() net.Addr                { return nil }
func (tc *testConn) RemoteAddr() net.Addr               { return nil }
func (tc *testConn) SetDeadline(t time.Time) error      { return nil }
func (tc *testConn) SetReadDeadline(t time.Time) error  { return nil }
func (tc *testConn) SetWriteDeadline(t time.Time) error { return nil }

// createExtendedQueryTestConn creates a test connection for extended query protocol testing.
func createExtendedQueryTestConn(t *testing.T, readBuf *bytes.Buffer, writeBuf *bytes.Buffer, handler Handler) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Create test connection that can read from readBuf and write to writeBuf.
	tc := &testConn{readBuf: readBuf, writeBuf: writeBuf}

	// Create a minimal listener with writer pool for testing.
	listener := &Listener{
		writersPool: &sync.Pool{
			New: func() any {
				return bufio.NewWriter(tc)
			},
		},
	}

	return &Conn{
		conn:           tc,
		bufferedReader: bufio.NewReader(readBuf),
		listener:       listener,
		handler:        handler,
		logger:         slog.Default(),
		txnStatus:      protocol.TxnStatusIdle,
		state:          nil, // Handler will initialize its own state
		ctx:            ctx,
		cancel:         cancel,
	}
}

// getTestConnectionState retrieves the test connection state from a connection.
func getTestConnectionState(conn *Conn) *testConnectionState {
	state := conn.GetConnectionState()
	if state == nil {
		return nil
	}
	return state.(*testConnectionState)
}

// writeTestString writes a null-terminated string to the buffer.
func writeTestString(buf *bytes.Buffer, s string) {
	buf.WriteString(s)
	buf.WriteByte(0)
}

// writeTestInt16 writes a 16-bit integer in big-endian format.
func writeTestInt16(buf *bytes.Buffer, v int16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(v))
	buf.Write(b[:])
}

// writeTestInt32 writes a 32-bit integer in big-endian format.
func writeTestInt32(buf *bytes.Buffer, v int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	buf.Write(b[:])
}

// readMessageTypeAndLength reads the message type and length from the buffer,
// reads the message body, and returns all three so the next call can read the next message.
func readMessageTypeAndLength(t *testing.T, buf *bytes.Buffer) (byte, int32, []byte) {
	msgType, err := buf.ReadByte()
	require.NoError(t, err, "failed to read message type")

	var lenBuf [4]byte
	_, err = buf.Read(lenBuf[:])
	require.NoError(t, err, "failed to read message length")

	length := binary.BigEndian.Uint32(lenBuf[:])
	bodyLen := int32(length - 4) // Subtract 4 for the length field itself

	// Read the message body
	var body []byte
	if bodyLen > 0 {
		body = make([]byte, bodyLen)
		_, err = buf.Read(body)
		require.NoError(t, err, "failed to read message body")
	}

	return msgType, bodyLen, body
}

// TestHandleParse tests the Parse message handler.
func TestHandleParse(t *testing.T) {
	tests := []struct {
		name          string
		stmtName      string
		query         string
		paramTypes    []uint32
		expectError   bool
		handlerReturn error
	}{
		{
			name:        "simple query without parameters",
			stmtName:    "stmt1",
			query:       "SELECT 1",
			paramTypes:  nil,
			expectError: false,
		},
		{
			name:        "unnamed statement",
			stmtName:    "",
			query:       "SELECT 2",
			paramTypes:  nil,
			expectError: false,
		},
		{
			name:        "query with parameter types",
			stmtName:    "stmt2",
			query:       "SELECT $1, $2",
			paramTypes:  []uint32{23, 25}, // INT4, TEXT
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build Parse message.
			var readBuf bytes.Buffer

			// Calculate message length: stmt name + query + param count (2 bytes) + param types (4 bytes each).
			length := int32(4 + len(tt.stmtName) + 1 + len(tt.query) + 1 + 2 + len(tt.paramTypes)*4)
			writeTestInt32(&readBuf, length)

			writeTestString(&readBuf, tt.stmtName)
			writeTestString(&readBuf, tt.query)
			writeTestInt16(&readBuf, int16(len(tt.paramTypes)))
			for _, oid := range tt.paramTypes {
				writeTestInt32(&readBuf, int32(oid))
			}

			var writeBuf bytes.Buffer
			handler := &testHandlerWithState{}
			conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

			// Call handleParse.
			err := conn.handleParse()

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Verify ParseComplete message was sent.
			msgType, bodyLen, _ := readMessageTypeAndLength(t, &writeBuf)
			assert.Equal(t, byte(protocol.MsgParseComplete), msgType)
			assert.Equal(t, int32(0), bodyLen) // ParseComplete has no body

			// Verify statement was stored.
			state := getTestConnectionState(conn)
			require.NotNil(t, state)

			state.mu.Lock()
			stmt := state.preparedStatements[tt.stmtName]
			state.mu.Unlock()

			require.NotNil(t, stmt)
			assert.Equal(t, tt.stmtName, stmt.Name)
			assert.Equal(t, tt.query, stmt.Query)
			if len(tt.paramTypes) == 0 && len(stmt.ParamTypes) == 0 {
				// Both empty, consider them equal (nil vs empty slice)
			} else {
				assert.Equal(t, tt.paramTypes, stmt.ParamTypes)
			}
		})
	}
}

// TestHandleBind tests the Bind message handler.
func TestHandleBind(t *testing.T) {
	tests := []struct {
		name        string
		portalName  string
		stmtName    string
		expectError bool
	}{
		{
			name:        "bind to named statement",
			portalName:  "portal1",
			stmtName:    "stmt1",
			expectError: false,
		},
		{
			name:        "bind to unnamed statement",
			portalName:  "",
			stmtName:    "",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// First, create a prepared statement.
			var readBuf bytes.Buffer
			var writeBuf bytes.Buffer
			handler := &testHandlerWithState{}
			conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

			// Manually add a prepared statement.
			stmt := &testPreparedStatement{
				Name:       tt.stmtName,
				Query:      "SELECT 1",
				ParamTypes: nil,
			}

			state := handler.getConnectionState(conn)
			state.mu.Lock()
			state.preparedStatements[tt.stmtName] = stmt
			state.mu.Unlock()

			// Build Bind message (Phase 1 - no parameters).
			// Length: portal name + stmt name + param format count + param count + result format count
			length := int32(4 + len(tt.portalName) + 1 + len(tt.stmtName) + 1 + 2 + 2 + 2)
			writeTestInt32(&readBuf, length)

			writeTestString(&readBuf, tt.portalName)
			writeTestString(&readBuf, tt.stmtName)
			writeTestInt16(&readBuf, 0) // No parameter formats
			writeTestInt16(&readBuf, 0) // No parameters
			writeTestInt16(&readBuf, 0) // No result formats

			// Call handleBind.
			err := conn.handleBind()

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Verify BindComplete message was sent.
			msgType, bodyLen, _ := readMessageTypeAndLength(t, &writeBuf)
			assert.Equal(t, byte(protocol.MsgBindComplete), msgType)
			assert.Equal(t, int32(0), bodyLen) // BindComplete has no body

			// Verify portal was stored.
			state = handler.getConnectionState(conn)
			state.mu.Lock()
			portal := state.portals[tt.portalName]
			state.mu.Unlock()

			require.NotNil(t, portal)
			assert.Equal(t, tt.portalName, portal.Name)
			assert.Equal(t, stmt, portal.Statement)
		})
	}
}

// TestHandleExecute tests the Execute message handler.
func TestHandleExecute(t *testing.T) {
	// Create a prepared statement and portal.
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	handler := &testHandlerWithState{}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Add statement and portal.
	stmt := &testPreparedStatement{
		Name:       "stmt1",
		Query:      "SELECT 1",
		ParamTypes: nil,
	}
	portal := &testPortal{
		Name:      "portal1",
		Statement: stmt,
	}

	state := handler.getConnectionState(conn)
	state.mu.Lock()
	state.preparedStatements["stmt1"] = stmt
	state.portals["portal1"] = portal
	state.mu.Unlock()

	// Build Execute message.
	// Length: portal name + max rows (4 bytes)
	portalName := "portal1"
	maxRows := int32(0) // 0 = no limit

	length := int32(4 + len(portalName) + 1 + 4)
	writeTestInt32(&readBuf, length)

	writeTestString(&readBuf, portalName)
	writeTestInt32(&readBuf, maxRows)

	// Call handleExecute.
	err := conn.handleExecute()
	require.NoError(t, err)

	// Verify response messages.
	// Should have: RowDescription, DataRow, CommandComplete

	// 1. RowDescription
	msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgRowDescription), msgType)

	// 2. DataRow
	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgDataRow), msgType)

	// 3. CommandComplete
	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgCommandComplete), msgType)
}

// TestHandleSync tests the Sync message handler.
func TestHandleSync(t *testing.T) {
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	handler := &testHandler{}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Build Sync message (no body).
	writeTestInt32(&readBuf, 4) // Length is just the length field itself

	// Call handleSync.
	err := conn.handleSync()
	require.NoError(t, err)

	// Verify ReadyForQuery message was sent.
	msgType, bodyLen, body := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgReadyForQuery), msgType)
	assert.Equal(t, int32(1), bodyLen) // ReadyForQuery has 1 byte body (txn status)

	// Verify transaction status from the body.
	require.Len(t, body, 1, "ReadyForQuery body should be 1 byte")
	assert.Equal(t, byte(protocol.TxnStatusIdle), body[0])
}

// TestExtendedQueryProtocolEndToEnd tests the full extended query protocol flow.
func TestExtendedQueryProtocolEndToEnd(t *testing.T) {
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	handler := &testHandler{}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Step 1: Parse
	stmtName := "test_stmt"
	query := "SELECT id, name FROM users"

	parseLength := int32(4 + len(stmtName) + 1 + len(query) + 1 + 2)
	writeTestInt32(&readBuf, parseLength)
	writeTestString(&readBuf, stmtName)
	writeTestString(&readBuf, query)
	writeTestInt16(&readBuf, 0) // No parameters

	err := conn.handleParse()
	require.NoError(t, err)

	// Verify ParseComplete
	msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgParseComplete), msgType)

	// Step 2: Bind
	portalName := "test_portal"

	bindLength := int32(4 + len(portalName) + 1 + len(stmtName) + 1 + 2 + 2 + 2)
	writeTestInt32(&readBuf, bindLength)
	writeTestString(&readBuf, portalName)
	writeTestString(&readBuf, stmtName)
	writeTestInt16(&readBuf, 0) // No parameter formats
	writeTestInt16(&readBuf, 0) // No parameters
	writeTestInt16(&readBuf, 0) // No result formats

	err = conn.handleBind()
	require.NoError(t, err)

	// Verify BindComplete
	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgBindComplete), msgType)

	// Step 3: Execute
	executeLength := int32(4 + len(portalName) + 1 + 4)
	writeTestInt32(&readBuf, executeLength)
	writeTestString(&readBuf, portalName)
	writeTestInt32(&readBuf, 0) // No max rows

	err = conn.handleExecute()
	require.NoError(t, err)

	// Verify result messages (RowDescription, DataRow, CommandComplete)
	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgRowDescription), msgType)

	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgDataRow), msgType)

	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgCommandComplete), msgType)

	// Step 4: Sync
	writeTestInt32(&readBuf, 4)

	err = conn.handleSync()
	require.NoError(t, err)

	// Verify ReadyForQuery
	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgReadyForQuery), msgType)
}

// TestExtendedQueryProtocolUnnamedStatements tests unnamed statements and portals.
func TestExtendedQueryProtocolUnnamedStatements(t *testing.T) {
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	handler := &testHandlerWithState{}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Parse with empty name (unnamed statement).
	query := "SELECT 42"

	parseLength := int32(4 + 1 + len(query) + 1 + 2)
	writeTestInt32(&readBuf, parseLength)
	writeTestString(&readBuf, "") // Empty name
	writeTestString(&readBuf, query)
	writeTestInt16(&readBuf, 0) // No parameters

	err := conn.handleParse()
	require.NoError(t, err)

	// Verify unnamed statement was stored.
	state := getTestConnectionState(conn)
	require.NotNil(t, state)

	state.mu.Lock()
	stmt := state.preparedStatements[""]
	state.mu.Unlock()

	require.NotNil(t, stmt)
	assert.Equal(t, "", stmt.Name)
	assert.Equal(t, query, stmt.Query)

	// Clear write buffer.
	writeBuf.Reset()

	// Bind with empty names (unnamed portal from unnamed statement).
	bindLength := int32(4 + 1 + 1 + 2 + 2 + 2)
	writeTestInt32(&readBuf, bindLength)
	writeTestString(&readBuf, "") // Empty portal name
	writeTestString(&readBuf, "") // Empty statement name
	writeTestInt16(&readBuf, 0)
	writeTestInt16(&readBuf, 0)
	writeTestInt16(&readBuf, 0)

	err = conn.handleBind()
	require.NoError(t, err)

	// Verify unnamed portal was stored.
	state.mu.Lock()
	portal := state.portals[""]
	state.mu.Unlock()

	require.NotNil(t, portal)
	assert.Equal(t, "", portal.Name)
	assert.Equal(t, stmt, portal.Statement)
}

// TestHandleDescribe tests the Describe message handler.
func TestHandleDescribe(t *testing.T) {
	tests := []struct {
		name          string
		describeType  byte
		targetName    string
		setupStmt     bool
		setupPortal   bool
		paramTypes    []uint32
		expectError   bool
		errorContains string
	}{
		{
			name:         "describe prepared statement without parameters",
			describeType: 'S',
			targetName:   "stmt1",
			setupStmt:    true,
			paramTypes:   nil,
			expectError:  false,
		},
		{
			name:         "describe prepared statement with parameters",
			describeType: 'S',
			targetName:   "stmt2",
			setupStmt:    true,
			paramTypes:   []uint32{23, 25}, // INT4, TEXT
			expectError:  false,
		},
		{
			name:         "describe portal",
			describeType: 'P',
			targetName:   "portal1",
			setupStmt:    true,
			setupPortal:  true,
			paramTypes:   []uint32{23},
			expectError:  false,
		},
		{
			name:          "describe non-existent prepared statement",
			describeType:  'S',
			targetName:    "nonexistent",
			expectError:   true,
			errorContains: "does not exist",
		},
		{
			name:          "describe non-existent portal",
			describeType:  'P',
			targetName:    "nonexistent",
			expectError:   true,
			errorContains: "does not exist",
		},
		{
			name:          "invalid describe type",
			describeType:  'X',
			targetName:    "test",
			expectError:   true,
			errorContains: "invalid describe type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var readBuf bytes.Buffer
			var writeBuf bytes.Buffer
			handler := &testHandlerWithState{}
			conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

			// Setup statement if needed.
			if tt.setupStmt {
				stmt := &testPreparedStatement{
					Name:       tt.targetName,
					Query:      "SELECT $1",
					ParamTypes: tt.paramTypes,
				}
				state := handler.getConnectionState(conn)
				state.mu.Lock()
				state.preparedStatements[tt.targetName] = stmt
				state.mu.Unlock()

				// Setup portal if needed.
				if tt.setupPortal {
					portal := &testPortal{
						Name:      tt.targetName,
						Statement: stmt,
					}
					state.mu.Lock()
					state.portals[tt.targetName] = portal
					state.mu.Unlock()
				}
			}

			// Build Describe message.
			// Length: type (1 byte) + name
			length := int32(4 + 1 + len(tt.targetName) + 1)
			writeTestInt32(&readBuf, length)
			readBuf.WriteByte(tt.describeType)
			writeTestString(&readBuf, tt.targetName)

			// Call handleDescribe.
			err := conn.handleDescribe()
			require.NoError(t, err) // handleDescribe writes errors to client, doesn't return them

			if tt.expectError {
				// Should have sent an ErrorResponse message.
				msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
				assert.Equal(t, byte(protocol.MsgErrorResponse), msgType, "should send error response")
				return
			}

			// Verify ParameterDescription message was sent (if statement has parameters).
			msgType, _, body := readMessageTypeAndLength(t, &writeBuf)

			if len(tt.paramTypes) > 0 {
				// ParameterDescription message
				assert.Equal(t, byte(protocol.MsgParameterDescription), msgType)

				// Parse parameter count (first 2 bytes of body).
				paramCount := binary.BigEndian.Uint16(body[0:2])
				assert.Equal(t, uint16(len(tt.paramTypes)), paramCount)

				// Verify each parameter OID.
				for i, expectedOid := range tt.paramTypes {
					offset := 2 + (i * 4)
					actualOid := binary.BigEndian.Uint32(body[offset : offset+4])
					assert.Equal(t, expectedOid, actualOid)
				}

				// NoData message should follow (since we don't return field descriptions yet).
				msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
			}

			// Should send NoData since we don't return field descriptions yet.
			assert.Equal(t, byte(protocol.MsgNoData), msgType)
		})
	}
}

// TestHandleClose tests the Close message handler.
func TestHandleClose(t *testing.T) {
	tests := []struct {
		name          string
		closeType     byte
		targetName    string
		setupStmt     bool
		setupPortal   bool
		expectError   bool
		errorContains string
	}{
		{
			name:        "close prepared statement",
			closeType:   'S',
			targetName:  "stmt1",
			setupStmt:   true,
			expectError: false,
		},
		{
			name:        "close portal",
			closeType:   'P',
			targetName:  "portal1",
			setupPortal: true,
			expectError: false,
		},
		{
			name:        "close non-existent prepared statement (should not error)",
			closeType:   'S',
			targetName:  "nonexistent",
			expectError: false,
		},
		{
			name:        "close non-existent portal (should not error)",
			closeType:   'P',
			targetName:  "nonexistent",
			expectError: false,
		},
		{
			name:          "invalid close type",
			closeType:     'X',
			targetName:    "test",
			expectError:   true,
			errorContains: "invalid close type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var readBuf bytes.Buffer
			var writeBuf bytes.Buffer
			handler := &testHandlerWithState{}
			conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

			state := handler.getConnectionState(conn)

			// Setup statement if needed.
			if tt.setupStmt {
				stmt := &testPreparedStatement{
					Name:       tt.targetName,
					Query:      "SELECT 1",
					ParamTypes: nil,
				}
				state.mu.Lock()
				state.preparedStatements[tt.targetName] = stmt
				state.mu.Unlock()
			}

			// Setup portal if needed.
			if tt.setupPortal {
				stmt := &testPreparedStatement{
					Name:       "stmt_for_portal",
					Query:      "SELECT 1",
					ParamTypes: nil,
				}
				portal := &testPortal{
					Name:      tt.targetName,
					Statement: stmt,
				}
				state.mu.Lock()
				state.portals[tt.targetName] = portal
				state.mu.Unlock()
			}

			// Build Close message.
			// Length: type (1 byte) + name
			length := int32(4 + 1 + len(tt.targetName) + 1)
			writeTestInt32(&readBuf, length)
			readBuf.WriteByte(tt.closeType)
			writeTestString(&readBuf, tt.targetName)

			// Call handleClose.
			err := conn.handleClose()
			require.NoError(t, err) // handleClose writes errors to client, doesn't return them

			if tt.expectError {
				// Should have sent an ErrorResponse message.
				msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
				assert.Equal(t, byte(protocol.MsgErrorResponse), msgType, "should send error response")
				return
			}

			// Verify CloseComplete message was sent.
			msgType, bodyLen, _ := readMessageTypeAndLength(t, &writeBuf)
			assert.Equal(t, byte(protocol.MsgCloseComplete), msgType)
			assert.Equal(t, int32(0), bodyLen) // CloseComplete has no body

			// Verify the item was actually removed from state.
			state.mu.Lock()
			defer state.mu.Unlock()

			if tt.closeType == 'S' && tt.setupStmt {
				_, exists := state.preparedStatements[tt.targetName]
				assert.False(t, exists, "prepared statement should be deleted")
			}

			if tt.closeType == 'P' && tt.setupPortal {
				_, exists := state.portals[tt.targetName]
				assert.False(t, exists, "portal should be deleted")
			}
		})
	}
}

// TestExtendedQueryProtocolWithParameters tests parameterized queries end-to-end.
func TestExtendedQueryProtocolWithParameters(t *testing.T) {
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	handler := &testHandlerWithState{}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Step 1: Parse with parameter types
	stmtName := "param_stmt"
	query := "SELECT $1 + $2"
	paramTypes := []uint32{23, 23} // INT4, INT4

	parseLength := int32(4 + len(stmtName) + 1 + len(query) + 1 + 2 + len(paramTypes)*4)
	writeTestInt32(&readBuf, parseLength)
	writeTestString(&readBuf, stmtName)
	writeTestString(&readBuf, query)
	writeTestInt16(&readBuf, int16(len(paramTypes)))
	for _, oid := range paramTypes {
		writeTestInt32(&readBuf, int32(oid))
	}

	err := conn.handleParse()
	require.NoError(t, err)

	// Verify ParseComplete
	msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgParseComplete), msgType)

	// Step 2: Bind with parameter values
	portalName := "param_portal"
	param1 := []byte("10")
	param2 := []byte("32")

	// Build Bind message with parameters
	// Length: portal name + stmt name + param format count + param count + params + result format count
	paramLength := int32(len(param1)) + int32(len(param2))
	bindLength := int32(4) + int32(len(portalName)) + 1 + int32(len(stmtName)) + 1 + 2 + 2 + 8 + paramLength + 2
	writeTestInt32(&readBuf, bindLength)
	writeTestString(&readBuf, portalName)
	writeTestString(&readBuf, stmtName)
	writeTestInt16(&readBuf, 0) // No parameter formats (defaults to text)
	writeTestInt16(&readBuf, 2) // 2 parameters
	writeTestInt32(&readBuf, int32(len(param1)))
	readBuf.Write(param1)
	writeTestInt32(&readBuf, int32(len(param2)))
	readBuf.Write(param2)
	writeTestInt16(&readBuf, 0) // No result formats

	err = conn.handleBind()
	require.NoError(t, err)

	// Verify BindComplete
	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgBindComplete), msgType)

	// Verify portal was stored with parameters
	state := getTestConnectionState(conn)
	require.NotNil(t, state)
	state.mu.Lock()
	portal := state.portals[portalName]
	state.mu.Unlock()
	require.NotNil(t, portal)
	assert.Equal(t, 2, len(portal.Parameters))
	assert.Equal(t, param1, portal.Parameters[0])
	assert.Equal(t, param2, portal.Parameters[1])
}

// TestExtendedQueryProtocolWithNullParameter tests NULL parameter handling.
func TestExtendedQueryProtocolWithNullParameter(t *testing.T) {
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	handler := &testHandlerWithState{}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Parse
	stmtName := "null_stmt"
	query := "SELECT $1, $2"
	paramTypes := []uint32{23, 25} // INT4, TEXT

	parseLength := int32(4 + len(stmtName) + 1 + len(query) + 1 + 2 + len(paramTypes)*4)
	writeTestInt32(&readBuf, parseLength)
	writeTestString(&readBuf, stmtName)
	writeTestString(&readBuf, query)
	writeTestInt16(&readBuf, int16(len(paramTypes)))
	for _, oid := range paramTypes {
		writeTestInt32(&readBuf, int32(oid))
	}

	err := conn.handleParse()
	require.NoError(t, err)
	readMessageTypeAndLength(t, &writeBuf) // ParseComplete

	// Bind with NULL parameter
	portalName := "null_portal"
	param1 := []byte("42")

	bindLength := int32(4 + len(portalName) + 1 + len(stmtName) + 1 + 2 + 2 + 4 + len(param1) + 4 + 2)
	writeTestInt32(&readBuf, bindLength)
	writeTestString(&readBuf, portalName)
	writeTestString(&readBuf, stmtName)
	writeTestInt16(&readBuf, 0) // No parameter formats
	writeTestInt16(&readBuf, 2) // 2 parameters
	writeTestInt32(&readBuf, int32(len(param1)))
	readBuf.Write(param1)
	writeTestInt32(&readBuf, -1) // NULL parameter (length = -1)
	writeTestInt16(&readBuf, 0)  // No result formats

	err = conn.handleBind()
	require.NoError(t, err)
	readMessageTypeAndLength(t, &writeBuf) // BindComplete

	// Verify portal was stored with NULL parameter
	state := getTestConnectionState(conn)
	require.NotNil(t, state)
	state.mu.Lock()
	portal := state.portals[portalName]
	state.mu.Unlock()
	require.NotNil(t, portal)
	assert.Equal(t, 2, len(portal.Parameters))
	assert.Equal(t, param1, portal.Parameters[0])
	assert.Nil(t, portal.Parameters[1]) // NULL parameter
}
