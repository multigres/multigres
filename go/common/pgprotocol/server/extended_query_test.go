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
	"sync"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testHandler is a mock handler for testing extended query protocol.
type testHandler struct {
	queryFunc    func(ctx context.Context, conn *Conn, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error
	parseFunc    func(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error
	bindFunc     func(ctx context.Context, conn *Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error
	executeFunc  func(ctx context.Context, conn *Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) error
	describeFunc func(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error)
	closeFunc    func(ctx context.Context, conn *Conn, typ byte, name string) error
	syncFunc     func(ctx context.Context, conn *Conn) error
}

func (h *testHandler) HandleQuery(ctx context.Context, conn *Conn, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	if h.queryFunc != nil {
		return h.queryFunc(ctx, conn, queryStr, callback)
	}
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

func (h *testHandler) HandleExecute(ctx context.Context, conn *Conn, portalName string, maxRows int32, _ bool, callback func(ctx context.Context, result *sqltypes.Result) error) error {
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

func (h *testHandler) ConnectionClosed(conn *Conn) {}

func (h *testHandler) GetPreparedStatementInfo(connID uint32, name string) *preparedstatement.PreparedStatementInfo {
	return nil
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

			// Describe('P') is held by handleDescribe and resolved on the next
			// non-Execute message. Drive that resolution directly so the test
			// observes the same wire bytes a real client would.
			if tt.describeType == 'P' {
				require.NoError(t, conn.resolveDeferredPortalDescribe())
			}

			if tt.expectError {
				// Should have sent an ErrorResponse message.
				msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
				assert.Equal(t, byte(protocol.MsgErrorResponse), msgType, "should send error response")
				return
			}

			// ParameterDescription is only sent for statement describes ('S'),
			// not portal describes ('P') — per PostgreSQL protocol spec.
			if tt.describeType == 'S' {
				msgType, _, body := readMessageTypeAndLength(t, &writeBuf)
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
			}

			// NoData message should follow (since we don't return field descriptions yet).
			msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
			assert.Equal(t, byte(protocol.MsgNoData), msgType)
		})
	}
}

// TestDescribePortalThenFlush exercises the Describe('P') + Flush ('H')
// sequence that libpq uses to receive a portal description without a full
// Sync cycle. Describe('P') is held by handleDescribe; the Flush handler
// must drain it before flushing the write buffer, otherwise the client
// blocks waiting for a RowDescription/NoData that was never written.
func TestDescribePortalThenFlush(t *testing.T) {
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	handler := &testHandlerWithState{}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	state := handler.getConnectionState(conn)
	stmt := &testPreparedStatement{Name: "p_flush", Query: "SELECT 1"}
	state.mu.Lock()
	state.preparedStatements["p_flush"] = stmt
	state.portals["p_flush"] = &testPortal{Name: "p_flush", Statement: stmt}
	state.mu.Unlock()

	// Build a Describe('P', "p_flush") body for handleDescribe to consume.
	length := int32(4 + 1 + len("p_flush") + 1)
	writeTestInt32(&readBuf, length)
	readBuf.WriteByte('P')
	writeTestString(&readBuf, "p_flush")

	require.NoError(t, conn.handleDescribe())
	// Nothing should have been written yet — the describe is held.
	assert.Equal(t, 0, writeBuf.Len(), "Describe('P') must not write before resolution")

	// MsgFlush has a 4-byte length on the wire (always 4: just the
	// length field itself). handleMessage's MsgFlush case reads and
	// discards it before flushing, so we have to feed it.
	writeTestInt32(&readBuf, 4)

	// Drive the Flush branch directly. It must resolve the deferred
	// describe before flushing so the client sees the reply.
	require.NoError(t, conn.handleMessage(protocol.MsgFlush))

	msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgNoData), msgType,
		"Flush after Describe('P') must surface NoData/RowDescription")
}

func TestUnsupportedFunctionCallDrainsMessageBody(t *testing.T) {
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, &testHandler{})

	body := []byte{
		0, 0, 0, 177, // function oid: int4pl
		0, 1, // one argument format code
		0, 0, // text format
		0, 2, // two arguments
		0, 0, 0, 1, '2',
		0, 0, 0, 1, '3',
		0, 0, // text result format
	}
	writeTestInt32(&readBuf, int32(4+len(body)))
	readBuf.Write(body)

	err := conn.handleMessage(protocol.MsgFunctionCall)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported message type: F")
	assert.Zero(t, readBuf.Len(), "unsupported FunctionCall must be drained before rejection")
}

func TestUnsupportedFunctionCallDrainsEmptyMessageBody(t *testing.T) {
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, &testHandler{})

	writeTestInt32(&readBuf, 4)

	err := conn.handleMessage(protocol.MsgFunctionCall)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported message type: F")
	assert.Zero(t, readBuf.Len(), "unsupported FunctionCall must consume the length field")
}

func TestUnsupportedFunctionCallReportsDrainError(t *testing.T) {
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, &testHandler{})

	err := conn.handleMessage(protocol.MsgFunctionCall)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to discard unsupported FunctionCall message")
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

// TestHandleDescribeMissingNullTerminator verifies that handleDescribe
// rejects a body whose name field doesn't end in NUL, instead of
// silently slicing off the trailing byte.
func TestHandleDescribeMissingNullTerminator(t *testing.T) {
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	handler := &testHandlerWithState{}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Body: 'S' + "stmt" with NO null terminator. Length = 4 (length
	// field) + 1 (type byte) + 4 (name bytes) = 9.
	writeTestInt32(&readBuf, 9)
	readBuf.WriteByte('S')
	readBuf.WriteString("stmt")

	err := conn.handleDescribe()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing null terminator")
}

// TestHandleCloseMissingNullTerminator verifies the same for handleClose.
func TestHandleCloseMissingNullTerminator(t *testing.T) {
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	handler := &testHandlerWithState{}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	writeTestInt32(&readBuf, 9)
	readBuf.WriteByte('S')
	readBuf.WriteString("stmt")

	err := conn.handleClose()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing null terminator")
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

// TestParseErrorDoesNotSendReadyForQuery verifies that when HandleParse fails,
// only ErrorResponse is sent — NOT ReadyForQuery. ReadyForQuery must only come
// from Sync to avoid protocol desynchronization with pipelined clients (e.g., pgx).
func TestParseErrorDoesNotSendReadyForQuery(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		parseFunc: func(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
			return errors.New("parse failed")
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Write a Parse message that will trigger the error.
	stmtName := "test_stmt"
	queryStr := "SELECT 1"
	length := int32(4 + len(stmtName) + 1 + len(queryStr) + 1 + 2) // length + name + query + paramCount
	writeTestInt32(&readBuf, length)
	writeTestString(&readBuf, stmtName)
	writeTestString(&readBuf, queryStr)
	writeTestInt16(&readBuf, 0) // No parameters

	err := conn.handleParse()
	require.NoError(t, err) // handleParse writes errors to client, returns nil

	// Should have sent ErrorResponse.
	msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)

	// Must NOT have sent ReadyForQuery — the buffer should be empty.
	assert.Equal(t, 0, writeBuf.Len(),
		"handleParse must not send ReadyForQuery; only Sync should")
}

// TestBindErrorDoesNotSendReadyForQuery verifies the same for HandleBind.
func TestBindErrorDoesNotSendReadyForQuery(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		bindFunc: func(ctx context.Context, conn *Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
			return errors.New("bind failed")
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Write a Bind message that will trigger the error.
	portalName := ""
	stmtName := "test_stmt"
	length := int32(4 + len(portalName) + 1 + len(stmtName) + 1 + 2 + 2 + 2) // length + portal + stmt + formatCount + paramCount + resultFormatCount
	writeTestInt32(&readBuf, length)
	writeTestString(&readBuf, portalName)
	writeTestString(&readBuf, stmtName)
	writeTestInt16(&readBuf, 0) // No parameter formats
	writeTestInt16(&readBuf, 0) // No parameters
	writeTestInt16(&readBuf, 0) // No result formats

	err := conn.handleBind()
	require.NoError(t, err) // handleBind writes errors to client, returns nil

	// Should have sent ErrorResponse.
	msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)

	// Must NOT have sent ReadyForQuery.
	assert.Equal(t, 0, writeBuf.Len(),
		"handleBind must not send ReadyForQuery; only Sync should")
}

// TestPipelinedParseErrorRecovery verifies that a pipelined Parse+Describe+Sync
// produces the correct response sequence when Parse fails: ErrorResponse from
// Parse, ErrorResponse from Describe (statement not found), then ReadyForQuery
// from Sync. No premature ReadyForQuery that would desync pgx.
func TestPipelinedParseErrorRecovery(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		parseFunc: func(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
			return errors.New("parse failed")
		},
		describeFunc: func(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error) {
			return nil, errors.New("statement not found")
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Write Parse message.
	stmtName := "test_stmt"
	queryStr := "SELECT 1"
	parseLen := int32(4 + len(stmtName) + 1 + len(queryStr) + 1 + 2)
	writeTestInt32(&readBuf, parseLen)
	writeTestString(&readBuf, stmtName)
	writeTestString(&readBuf, queryStr)
	writeTestInt16(&readBuf, 0)

	// Write Describe message.
	descLen := int32(4 + 1 + len(stmtName) + 1)
	writeTestInt32(&readBuf, descLen)
	readBuf.WriteByte('S')
	writeTestString(&readBuf, stmtName)

	// Write Sync message.
	writeTestInt32(&readBuf, 4) // Sync has no body, just length=4

	// Process all three messages.
	err := conn.handleParse()
	require.NoError(t, err)
	err = conn.handleDescribe()
	require.NoError(t, err)
	err = conn.handleSync()
	require.NoError(t, err)

	// Expected response sequence:
	// 1. ErrorResponse (from Parse failure)
	msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType, "first message should be ErrorResponse from Parse")

	// 2. ErrorResponse (from Describe failure — statement not found)
	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType, "second message should be ErrorResponse from Describe")

	// 3. ReadyForQuery (from Sync — the only place it should appear)
	msgType, _, body := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgReadyForQuery), msgType, "third message should be ReadyForQuery from Sync")
	assert.Equal(t, byte('I'), body[0], "transaction status should be Idle")

	// Buffer should be empty — no extra messages.
	assert.Equal(t, 0, writeBuf.Len(), "no extra messages should be in the buffer")
}

// TestExtendedQueryErrorDiscardsUntilSync pins the PostgreSQL extended-query
// error-recovery rule:
//
//	"When an error is detected while processing any extended-query message,
//	 the backend issues ErrorResponse, then reads and discards messages until
//	 a Sync message is reached, then issues ReadyForQuery."
//	 — https://www.postgresql.org/docs/current/protocol-flow.html
//
// Minimum reproducer: one extended-query message that errors (Parse),
// then any extended-query follow-up (Close) that would otherwise emit a
// *Complete reply, then Sync. The follow-up's reply frame must be
// suppressed; only ErrorResponse and ReadyForQuery may appear on the
// wire. Strict drivers (Postgrex, JDBC) crash on any frame between
// ErrorResponse and ReadyForQuery; the original wire trace came from a
// Parse / Bind / Execute / Close / Sync flight, but the gate applies to
// every Parse/Bind/Describe/Execute/Close after an error.
// See ~/dev/multigres-plans/investigations/2026-05-20-multigateway-extended-query-close-after-error.md.
func TestExtendedQueryErrorDiscardsUntilSync(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		parseFunc: func(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
			return errors.New("parse failed")
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Drive the serve()-loop's ReadMessageType + handleMessage path —
	// that's where the discard-until-Sync gate lives. (Calling handlers
	// individually bypasses the gate by design.)

	// Parse 'P' — parseFunc above makes this fail.
	readBuf.WriteByte(protocol.MsgParse)
	writeTestInt32(&readBuf, int32(4+1+1+2)) // empty stmt name + empty query + 0 param types
	writeTestString(&readBuf, "")
	writeTestString(&readBuf, "")
	writeTestInt16(&readBuf, 0)

	// Close 'C' — the frame whose CloseComplete must NOT be emitted.
	readBuf.WriteByte(protocol.MsgClose)
	writeTestInt32(&readBuf, int32(4+1+1)) // type byte + empty name + null
	readBuf.WriteByte('S')
	writeTestString(&readBuf, "")

	// Sync 'S'
	readBuf.WriteByte(protocol.MsgSync)
	writeTestInt32(&readBuf, 4)

	conn.startWriterBuffering()
	for i := range 3 {
		msgType, err := conn.ReadMessageType()
		require.NoError(t, err, "ReadMessageType iter %d", i)
		require.NoError(t, conn.handleMessage(msgType), "handleMessage iter %d", i)
	}
	require.NoError(t, conn.endWriterBuffering())

	var got []byte
	for writeBuf.Len() > 0 {
		msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
		got = append(got, msgType)
	}

	want := []byte{protocol.MsgErrorResponse, protocol.MsgReadyForQuery}
	assert.Equal(t, want, got,
		"after ErrorResponse the gateway must discard the pipelined Close "+
			"and emit only ReadyForQuery — any frame between Error and RFQ "+
			"(notably CloseComplete) breaks strict clients like Postgrex/JDBC")
}

// TestExtendedQueryErrorDiscardsSimpleQueryUntilSync verifies that a simple Query
// ('Q') pipelined after an extended-query error is DISCARDED until Sync — matching
// PostgreSQL, which "reads and discards messages until a Sync is reached". Executing it
// (e.g. Postgrex mode: :savepoint's "RELEASE SAVEPOINT postgrex_query") emits extra
// frames between ErrorResponse and the Sync's ReadyForQuery, desyncing strict clients.
func TestExtendedQueryErrorDiscardsSimpleQueryUntilSync(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	queryInvoked := false
	handler := &testHandler{
		parseFunc: func(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
			return errors.New("parse failed")
		},
		queryFunc: func(ctx context.Context, conn *Conn, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error {
			queryInvoked = true
			return nil
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Parse 'P' — parseFunc makes this fail → enters drain mode.
	readBuf.WriteByte(protocol.MsgParse)
	writeTestInt32(&readBuf, int32(4+1+1+2)) // empty stmt name + empty query + 0 param types
	writeTestString(&readBuf, "")
	writeTestString(&readBuf, "")
	writeTestInt16(&readBuf, 0)

	// Simple Query 'Q' pipelined BEFORE Sync — must be discarded, not executed.
	q := "RELEASE SAVEPOINT postgrex_query"
	readBuf.WriteByte(protocol.MsgQuery)
	writeTestInt32(&readBuf, int32(4+len(q)+1))
	writeTestString(&readBuf, q)

	// Sync 'S' — the only drain boundary.
	readBuf.WriteByte(protocol.MsgSync)
	writeTestInt32(&readBuf, 4)

	conn.startWriterBuffering()
	for i := range 3 {
		msgType, err := conn.ReadMessageType()
		require.NoError(t, err, "ReadMessageType iter %d", i)
		require.NoError(t, conn.handleMessage(msgType), "handleMessage iter %d", i)
	}
	require.NoError(t, conn.endWriterBuffering())

	var got []byte
	for writeBuf.Len() > 0 {
		msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
		got = append(got, msgType)
	}

	assert.False(t, queryInvoked,
		"a simple Query pipelined after an extended-query error must be discarded, not executed")
	want := []byte{protocol.MsgErrorResponse, protocol.MsgReadyForQuery}
	assert.Equal(t, want, got,
		"after ErrorResponse the gateway must discard the pipelined simple Query and emit "+
			"only ReadyForQuery — any frame between Error and RFQ breaks strict clients "+
			"(Postgrex mode: :savepoint)")
}

// TestDeferredDescribeErrorDiscardsTriggeringMessage covers a subtle
// second path into drain mode: handleDescribe('P') defers; the next
// inbound message (here, Bind) calls handleMessage → resolveDeferredPortalDescribe
// → HandleDescribe fails → writeExtendedQueryError sets the drain flag
// and returns nil. handleMessage's flag check at the top has already
// run, so without a second check the triggering Bind still dispatches
// to handleBind, which emits BindComplete after the just-buffered
// ErrorResponse — the exact protocol violation the PR is fixing.
//
// Wire must be: ErrorResponse (from the deferred Describe), ReadyForQuery.
// No BindComplete.
func TestDeferredDescribeErrorDiscardsTriggeringMessage(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		describeFunc: func(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error) {
			return nil, errors.New("portal does not exist")
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Describe('P', "missing") — deferred until the next non-Execute msg.
	readBuf.WriteByte(protocol.MsgDescribe)
	writeTestInt32(&readBuf, int32(4+1+len("missing")+1))
	readBuf.WriteByte('P')
	writeTestString(&readBuf, "missing")

	// Bind — triggers resolveDeferredPortalDescribe, which fails.
	readBuf.WriteByte(protocol.MsgBind)
	writeTestInt32(&readBuf, int32(4+1+1+2+2+2)) // empty portal + empty stmt + 0/0/0 counts
	writeTestString(&readBuf, "")
	writeTestString(&readBuf, "")
	writeTestInt16(&readBuf, 0)
	writeTestInt16(&readBuf, 0)
	writeTestInt16(&readBuf, 0)

	// Sync
	readBuf.WriteByte(protocol.MsgSync)
	writeTestInt32(&readBuf, 4)

	conn.startWriterBuffering()
	for i := range 3 {
		msgType, err := conn.ReadMessageType()
		require.NoError(t, err, "ReadMessageType iter %d", i)
		require.NoError(t, conn.handleMessage(msgType), "handleMessage iter %d", i)
	}
	require.NoError(t, conn.endWriterBuffering())

	var got []byte
	for writeBuf.Len() > 0 {
		msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
		got = append(got, msgType)
	}

	want := []byte{protocol.MsgErrorResponse, protocol.MsgReadyForQuery}
	assert.Equal(t, want, got,
		"when the deferred Describe flush emits ErrorResponse, the message "+
			"that triggered the flush (Bind) must not also emit its own "+
			"BindComplete — that frame would land between Error and RFQ")
}

// TestDeferredDescribeErrorDiscardsTriggeringExecute covers the same
// gap inside handleExecute: Execute with a portal name that doesn't
// match the deferred describe forces resolveDeferredPortalDescribe to
// flush via HandleDescribe; if that fails and enters drain mode, the
// subsequent HandleExecute call must not run — otherwise its
// CommandComplete (or DataRow / RowDescription) frames leak after
// ErrorResponse.
func TestDeferredDescribeErrorDiscardsTriggeringExecute(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		describeFunc: func(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error) {
			return nil, errors.New("portal does not exist")
		},
		// HandleExecute default in testHandler emits a row — if the gate
		// fails to suppress it, RowDescription/DataRow/CommandComplete
		// will appear in the wire output.
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Describe('P', "deferred-name") — sets the deferred-describe state.
	readBuf.WriteByte(protocol.MsgDescribe)
	writeTestInt32(&readBuf, int32(4+1+len("deferred-name")+1))
	readBuf.WriteByte('P')
	writeTestString(&readBuf, "deferred-name")

	// Execute("other-portal") — mismatched name, so the deferred describe
	// flushes via the handler, which errors out.
	readBuf.WriteByte(protocol.MsgExecute)
	writeTestInt32(&readBuf, int32(4+len("other-portal")+1+4))
	writeTestString(&readBuf, "other-portal")
	writeTestInt32(&readBuf, 0)

	// Sync
	readBuf.WriteByte(protocol.MsgSync)
	writeTestInt32(&readBuf, 4)

	conn.startWriterBuffering()
	for i := range 3 {
		msgType, err := conn.ReadMessageType()
		require.NoError(t, err, "ReadMessageType iter %d", i)
		require.NoError(t, conn.handleMessage(msgType), "handleMessage iter %d", i)
	}
	require.NoError(t, conn.endWriterBuffering())

	var got []byte
	for writeBuf.Len() > 0 {
		msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
		got = append(got, msgType)
	}

	want := []byte{protocol.MsgErrorResponse, protocol.MsgReadyForQuery}
	assert.Equal(t, want, got,
		"deferred Describe flush erroring inside handleExecute must abort "+
			"the HandleExecute call — no RowDescription/DataRow/CommandComplete "+
			"may appear between ErrorResponse and ReadyForQuery")
}

// TestDrainModeFlushPushesErrorWithoutReply covers the Flush branch of
// maybeDispatchDrain. A client that issues `Parse(err) + Flush` (the
// shape libpq uses to surface a Parse error without committing to a
// Sync yet) must see the ErrorResponse on the wire after the Flush,
// but no further reply frame from the Flush itself. The drain flag
// stays set; the next Parse is still drained; Sync clears the flag.
func TestDrainModeFlushPushesErrorWithoutReply(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		parseFunc: func(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
			return errors.New("parse failed")
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Parse (errors).
	readBuf.WriteByte(protocol.MsgParse)
	writeTestInt32(&readBuf, int32(4+1+1+2))
	writeTestString(&readBuf, "")
	writeTestString(&readBuf, "")
	writeTestInt16(&readBuf, 0)

	// Flush — should push the buffered ErrorResponse without adding a frame.
	readBuf.WriteByte(protocol.MsgFlush)
	writeTestInt32(&readBuf, 4)

	// Another Parse — must be drained, no reply.
	readBuf.WriteByte(protocol.MsgParse)
	writeTestInt32(&readBuf, int32(4+1+1+2))
	writeTestString(&readBuf, "")
	writeTestString(&readBuf, "")
	writeTestInt16(&readBuf, 0)

	// Sync — clears drain mode and emits RFQ.
	readBuf.WriteByte(protocol.MsgSync)
	writeTestInt32(&readBuf, 4)

	conn.startWriterBuffering()
	for i := range 4 {
		msgType, err := conn.ReadMessageType()
		require.NoError(t, err, "ReadMessageType iter %d", i)
		require.NoError(t, conn.handleMessage(msgType), "handleMessage iter %d", i)
	}
	require.NoError(t, conn.endWriterBuffering())

	var got []byte
	for writeBuf.Len() > 0 {
		msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
		got = append(got, msgType)
	}

	want := []byte{protocol.MsgErrorResponse, protocol.MsgReadyForQuery}
	assert.Equal(t, want, got,
		"Flush in drain mode must push buffered ErrorResponse but emit "+
			"no reply of its own; subsequent Parse must drain; Sync ends "+
			"drain mode with ReadyForQuery")
	assert.False(t, conn.discardingUntilSync,
		"drain flag must be cleared by Sync")
}

// TestDrainModeTruncatedStreamSurfacesError pins the I/O error paths
// in maybeDispatchDrain (Flush) and drainExtendedQueryMessage (Bind):
// if the wire is truncated mid-header, both must return a wrapped error
// rather than crashing or silently advancing.
func TestDrainModeTruncatedStreamSurfacesError(t *testing.T) {
	t.Run("drained_message_missing_length", func(t *testing.T) {
		var readBuf, writeBuf bytes.Buffer
		handler := &testHandler{
			parseFunc: func(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
				return errors.New("parse failed")
			},
		}
		conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

		// Parse (errors) — enters drain mode.
		readBuf.WriteByte(protocol.MsgParse)
		writeTestInt32(&readBuf, int32(4+1+1+2))
		writeTestString(&readBuf, "")
		writeTestString(&readBuf, "")
		writeTestInt16(&readBuf, 0)

		// Bind type byte with no length — truncated header.
		readBuf.WriteByte(protocol.MsgBind)

		conn.startWriterBuffering()
		msgType, err := conn.ReadMessageType()
		require.NoError(t, err)
		require.NoError(t, conn.handleMessage(msgType))
		assert.True(t, conn.discardingUntilSync)

		msgType, err = conn.ReadMessageType()
		require.NoError(t, err)
		err = conn.handleMessage(msgType)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read length while draining")
	})

	t.Run("drain_flush_missing_length", func(t *testing.T) {
		var readBuf, writeBuf bytes.Buffer
		handler := &testHandler{
			parseFunc: func(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
				return errors.New("parse failed")
			},
		}
		conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

		// Parse (errors) — enters drain mode.
		readBuf.WriteByte(protocol.MsgParse)
		writeTestInt32(&readBuf, int32(4+1+1+2))
		writeTestString(&readBuf, "")
		writeTestString(&readBuf, "")
		writeTestInt16(&readBuf, 0)

		// Flush type byte with no length — truncated header.
		readBuf.WriteByte(protocol.MsgFlush)

		conn.startWriterBuffering()
		msgType, err := conn.ReadMessageType()
		require.NoError(t, err)
		require.NoError(t, conn.handleMessage(msgType))

		msgType, err = conn.ReadMessageType()
		require.NoError(t, err)
		err = conn.handleMessage(msgType)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read Flush message length")
	})
}

// TestDrainModeTerminateExits covers the Terminate branch of
// maybeDispatchDrain. A client that errors during an extended-query
// batch and then closes the connection (Parse(err) + Terminate) must
// still see Terminate handled as connection teardown — the gate falls
// through to handleMessage's normal Terminate dispatch (return io.EOF).
func TestDrainModeTerminateExits(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		parseFunc: func(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
			return errors.New("parse failed")
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Parse (errors).
	readBuf.WriteByte(protocol.MsgParse)
	writeTestInt32(&readBuf, int32(4+1+1+2))
	writeTestString(&readBuf, "")
	writeTestString(&readBuf, "")
	writeTestInt16(&readBuf, 0)

	// Terminate (no body).
	readBuf.WriteByte(protocol.MsgTerminate)

	conn.startWriterBuffering()

	// Parse: writes ErrorResponse, enters drain mode.
	msgType, err := conn.ReadMessageType()
	require.NoError(t, err)
	require.NoError(t, conn.handleMessage(msgType))
	assert.True(t, conn.discardingUntilSync, "Parse error must enter drain mode")

	// Terminate: must surface io.EOF so the serve() loop tears the
	// connection down cleanly even from drain mode.
	msgType, err = conn.ReadMessageType()
	require.NoError(t, err)
	require.Equal(t, byte(protocol.MsgTerminate), msgType)
	require.ErrorIs(t, conn.handleMessage(msgType), io.EOF,
		"Terminate in drain mode must return io.EOF, not silently drain")
}

// An Execute whose handler signals an empty query (callback with nil result)
// must produce a single EmptyQueryResponse ('I'), mirroring the simple-query path.
func TestHandleExecute_EmptyQueryResponse(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		executeFunc: func(ctx context.Context, conn *Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) error {
			return callback(ctx, nil) // signal empty query
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	portalName := "portal1"
	writeTestInt32(&readBuf, int32(4+len(portalName)+1+4))
	writeTestString(&readBuf, portalName)
	writeTestInt32(&readBuf, 0) // maxRows

	require.NoError(t, conn.handleExecute())

	msgType, bodyLen, _ := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgEmptyQueryResponse), msgType)
	assert.Equal(t, int32(0), bodyLen)
	// No further messages.
	assert.Equal(t, 0, writeBuf.Len(), "EmptyQueryResponse must be the only message")
}

// When Describe('P') is folded into Execute (includeDescribe=true) for an empty
// portal, the wire order is NoData (the describe answer) then EmptyQueryResponse.
func TestHandleExecute_EmptyQueryFoldedDescribe(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		describeFunc: func(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error) {
			return &query.StatementDescription{}, nil // empty: NoData
		},
		executeFunc: func(ctx context.Context, conn *Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) error {
			return callback(ctx, nil)
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Describe('P', "portal1") — deferred/folded.
	portalName := "portal1"
	writeTestInt32(&readBuf, int32(4+1+len(portalName)+1))
	readBuf.WriteByte('P')
	writeTestString(&readBuf, portalName)
	require.NoError(t, conn.handleDescribe())

	// Execute on the same portal — folds the describe in.
	writeTestInt32(&readBuf, int32(4+len(portalName)+1+4))
	writeTestString(&readBuf, portalName)
	writeTestInt32(&readBuf, 0)
	require.NoError(t, conn.handleExecute())

	msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgNoData), msgType, "folded Describe('P') answer")
	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgEmptyQueryResponse), msgType, "Execute answer")
}
