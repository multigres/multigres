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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
)

var errInterruptedDataRow = errors.New("backend connection lost")

// PostgreSQL ends a session after a FATAL: the ErrorResponse is the last
// frame, no ReadyForQuery follows, and the server closes the connection.
// These tests pin that the gateway mirrors this when a handler relays a
// backend FATAL (e.g. pg_terminate_backend, crash, immediate shutdown).

func newFatalDiag() *mterrors.PgDiagnostic {
	return mterrors.NewPgError("FATAL", "57P01", "terminating connection due to administrator command", "")
}

func TestHandleQuery_FatalDiagnosticClosesWithoutReadyForQuery(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		queryFunc: func(ctx context.Context, conn *Conn, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error {
			return newFatalDiag()
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	sql := "SELECT pg_terminate_backend(pg_backend_pid())"
	writeTestInt32(&readBuf, int32(4+len(sql)+1))
	writeTestString(&readBuf, sql)

	conn.startWriterBuffering()
	err := conn.handleMessage(protocol.MsgQuery)
	require.ErrorIs(t, err, errFatalDiagnosticSent,
		"handleQuery must signal serve() to close after relaying a FATAL")
	require.NoError(t, conn.endWriterBuffering())

	msgType, _, body := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	assert.Contains(t, string(body), "FATAL")
	assert.Contains(t, string(body), "57P01")
	assert.Contains(t, string(body), "terminating connection due to administrator command")
	assert.Zero(t, writeBuf.Len(),
		"no ReadyForQuery may follow a FATAL — PostgreSQL closes the connection instead")
}

func TestHandleQuery_NonFatalErrorKeepsSessionAlive(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		queryFunc: func(ctx context.Context, conn *Conn, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error {
			return mterrors.NewPgError("ERROR", "42601", "syntax error", "")
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	sql := "SELEC 1"
	writeTestInt32(&readBuf, int32(4+len(sql)+1))
	writeTestString(&readBuf, sql)

	conn.startWriterBuffering()
	err := conn.handleMessage(protocol.MsgQuery)
	require.NoError(t, err, "a plain ERROR must not tear down the session")
	require.NoError(t, conn.endWriterBuffering())

	msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgReadyForQuery), msgType,
		"a plain ERROR is followed by ReadyForQuery as usual")
}

func TestHandleQuery_IncompleteDataRowClosesWithoutErrorFrames(t *testing.T) {
	partial := []byte("D\x00\x00\x10\x00partial-row")
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		queryFunc: func(ctx context.Context, conn *Conn, queryStr string, callback func(context.Context, *sqltypes.Result) error) error {
			require.NoError(t, callback(ctx, &sqltypes.Result{
				PassthroughBlock:         partial,
				PassthroughRowInProgress: true,
			}))
			return errInterruptedDataRow
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	sql := "SELECT huge_value"
	writeTestInt32(&readBuf, int32(4+len(sql)+1))
	writeTestString(&readBuf, sql)

	conn.startWriterBuffering()
	err := conn.handleMessage(protocol.MsgQuery)
	require.ErrorIs(t, err, errIncompleteDataRow)
	require.ErrorIs(t, err, errInterruptedDataRow)
	require.NoError(t, conn.endWriterBuffering())
	assert.Equal(t, partial, writeBuf.Bytes(),
		"an ErrorResponse or ReadyForQuery must not follow an incomplete DataRow")
}

func TestHandleQuery_ErrorAfterCompleteOpaqueRowRemainsFrameSafe(t *testing.T) {
	complete := []byte("D\x00\x00\x00\x04")
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		queryFunc: func(ctx context.Context, conn *Conn, queryStr string, callback func(context.Context, *sqltypes.Result) error) error {
			require.NoError(t, callback(ctx, &sqltypes.Result{
				PassthroughBlock:         complete,
				PassthroughRowCount:      1,
				PassthroughRowInProgress: false,
			}))
			return errInterruptedDataRow
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	sql := "SELECT rows_then_error"
	writeTestInt32(&readBuf, int32(4+len(sql)+1))
	writeTestString(&readBuf, sql)

	conn.startWriterBuffering()
	require.NoError(t, conn.handleMessage(protocol.MsgQuery))
	require.NoError(t, conn.endWriterBuffering())

	msgType, _, _ := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgDataRow), msgType)
	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	msgType, _, _ = readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgReadyForQuery), msgType)
}

func TestAbortWriterBufferingDiscardsIncompleteFrame(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, &testHandler{})
	conn.startWriterBuffering()
	require.NoError(t, conn.WriteRawMessage([]byte("buffered partial frame")))

	conn.abortWriterBuffering()

	assert.Empty(t, writeBuf.Bytes(), "aborting must not flush an incomplete frame")
	assert.Nil(t, conn.bufferedWriter, "Close must not later flush the discarded writer")
}

func TestHandleExecute_IncompleteDataRowClosesWithoutErrorFrames(t *testing.T) {
	partial := []byte("D\x00\x00\x10\x00partial-row")
	var readBuf, writeBuf bytes.Buffer
	handler := &testHandler{
		executeFunc: func(ctx context.Context, conn *Conn, portalName string, maxRows int32, callback func(context.Context, *sqltypes.Result) error) error {
			require.NoError(t, callback(ctx, &sqltypes.Result{
				PassthroughBlock:         partial,
				PassthroughRowInProgress: true,
			}))
			return errInterruptedDataRow
		},
	}
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, handler)

	writeTestInt32(&readBuf, 4+1+4) // length + empty portal name + maxRows
	writeTestString(&readBuf, "")
	writeTestInt32(&readBuf, 0)

	conn.startWriterBuffering()
	err := conn.handleExecute()
	require.ErrorIs(t, err, errIncompleteDataRow)
	require.ErrorIs(t, err, errInterruptedDataRow)
	require.NoError(t, conn.endWriterBuffering())
	assert.Equal(t, partial, writeBuf.Bytes(),
		"an ErrorResponse must not follow an incomplete DataRow")
}

func TestWriteExtendedQueryError_FatalReturnsCloseSentinel(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, &testHandler{})

	conn.startWriterBuffering()
	err := conn.writeExtendedQueryError(newFatalDiag())
	require.ErrorIs(t, err, errFatalDiagnosticSent)
	require.NoError(t, conn.endWriterBuffering())

	msgType, _, body := readMessageTypeAndLength(t, &writeBuf)
	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	assert.Contains(t, string(body), "FATAL")
	assert.Zero(t, writeBuf.Len(), "the FATAL ErrorResponse must be the last frame")
}

func TestWriteExtendedQueryError_NonFatalEntersDrainMode(t *testing.T) {
	var readBuf, writeBuf bytes.Buffer
	conn := createExtendedQueryTestConn(t, &readBuf, &writeBuf, &testHandler{})

	conn.startWriterBuffering()
	err := conn.writeExtendedQueryError(mterrors.NewPgError("ERROR", "42601", "syntax error", ""))
	require.NoError(t, err)
	require.NoError(t, conn.endWriterBuffering())
	assert.True(t, conn.discardingUntilSync,
		"a plain ERROR drains until Sync instead of closing")
}
