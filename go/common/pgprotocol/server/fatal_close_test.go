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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
)

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
