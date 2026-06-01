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
	"errors"
	"testing"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/pb/query"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errorResponseSQLSTATE reads the next message off buf, asserts it is an
// ErrorResponse, and returns the SQLSTATE carried in its 'C' field. The
// ErrorResponse body is a sequence of fields, each a one-byte field type
// followed by a NUL-terminated value, terminated by a lone zero byte.
func errorResponseSQLSTATE(t *testing.T, buf *bytes.Buffer) string {
	t.Helper()
	msgType, _, body := readMessageTypeAndLength(t, buf)
	require.Equal(t, byte(protocol.MsgErrorResponse), msgType, "expected ErrorResponse")
	for i := 0; i < len(body) && body[i] != 0; {
		field := body[i]
		i++
		start := i
		for i < len(body) && body[i] != 0 {
			i++
		}
		val := string(body[start:i])
		i++ // skip the NUL terminator
		if field == 'C' {
			return val
		}
	}
	t.Fatalf("ErrorResponse had no 'C' (SQLSTATE) field: %q", body)
	return ""
}

// These tests pin the multigateway's extended-query error SQLSTATEs to match
// PostgreSQL. The handlers (handleBind/handleDescribe) used to blanket-wrap any
// handler error as an internal MTDxx code, clobbering the real SQLSTATE the
// handler produced; drivers branch on these codes, so that was a genuine
// protocol divergence (caught by the pgproto conformance suite's
// bind_before_parse and describe_unknown corpus files). preserveExtendedQueryError
// now forwards a structured *PgDiagnostic unchanged and only wraps opaque errors.

// TestBindUnknownStatementPreservesSQLSTATE: a Bind referencing a prepared
// statement that was never Parsed must surface PostgreSQL's 26000
// (invalid_sql_statement_name), not the internal MTD05.
func TestBindUnknownStatementPreservesSQLSTATE(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		bindFunc: func(ctx context.Context, conn *Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
			return mterrors.NewInvalidPreparedStatementError(stmtName)
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	writeBindMessage(&readBuf, "p_ghost", "ghost")
	require.NoError(t, conn.handleBind())

	assert.Equal(t, mterrors.PgSSInvalidSQLStatementName, errorResponseSQLSTATE(t, &writeBuf))
}

// TestBindOpaqueErrorWrappedAsMTD05: an error with no SQLSTATE still falls back
// to MTD05 — preserving the diagnostic must not swallow the internal-failure
// signal for genuinely opaque errors.
func TestBindOpaqueErrorWrappedAsMTD05(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		bindFunc: func(ctx context.Context, conn *Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
			return errors.New("something opaque went wrong")
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	writeBindMessage(&readBuf, "p", "s")
	require.NoError(t, conn.handleBind())

	assert.Equal(t, mterrors.MTD05.ID, errorResponseSQLSTATE(t, &writeBuf))
}

// TestDescribeUnknownStatementPreservesSQLSTATE: Describe('S') of an unknown
// prepared statement must surface 26000 (invalid_sql_statement_name).
func TestDescribeUnknownStatementPreservesSQLSTATE(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		describeFunc: func(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error) {
			return nil, mterrors.NewInvalidPreparedStatementError(name)
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// handleDescribe reads a length-prefixed body of type byte + name.
	writeTestInt32(&readBuf, int32(4+1+len("no_such_stmt")+1))
	readBuf.WriteByte('S')
	writeTestString(&readBuf, "no_such_stmt")
	require.NoError(t, conn.handleDescribe())

	assert.Equal(t, mterrors.PgSSInvalidSQLStatementName, errorResponseSQLSTATE(t, &writeBuf))
}

// TestDescribeUnknownPortalPreservesSQLSTATE: Describe('P') of an unknown portal
// must surface 34000 (invalid_cursor_name) — a distinct SQLSTATE from the
// statement case, which the old blanket MTD06 collapsed into one code. The 'P'
// describe is deferred, so it is driven through the message loop the way the
// server resolves it (resolveDeferredPortalDescribe on the following Sync).
func TestDescribeUnknownPortalPreservesSQLSTATE(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		describeFunc: func(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error) {
			return nil, mterrors.NewInvalidPortalError(name)
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	// Describe('P', "no_such_portal") — deferred until the next message.
	readBuf.WriteByte(protocol.MsgDescribe)
	writeTestInt32(&readBuf, int32(4+1+len("no_such_portal")+1))
	readBuf.WriteByte('P')
	writeTestString(&readBuf, "no_such_portal")

	// Sync — resolves the deferred Describe, which errors.
	readBuf.WriteByte(protocol.MsgSync)
	writeTestInt32(&readBuf, 4)

	conn.startWriterBuffering()
	for i := range 2 {
		msgType, err := conn.ReadMessageType()
		require.NoError(t, err, "ReadMessageType iter %d", i)
		require.NoError(t, conn.handleMessage(msgType), "handleMessage iter %d", i)
	}
	require.NoError(t, conn.endWriterBuffering())

	assert.Equal(t, mterrors.PgSSInvalidCursorName, errorResponseSQLSTATE(t, &writeBuf))
}

// writeBindMessage writes a zero-parameter Bind message body (length-prefixed)
// into buf, ready for handleBind to read.
func writeBindMessage(buf *bytes.Buffer, portalName, stmtName string) {
	length := int32(4 + len(portalName) + 1 + len(stmtName) + 1 + 2 + 2 + 2)
	writeTestInt32(buf, length)
	writeTestString(buf, portalName)
	writeTestString(buf, stmtName)
	writeTestInt16(buf, 0) // parameter format count
	writeTestInt16(buf, 0) // parameter count
	writeTestInt16(buf, 0) // result format count
}
