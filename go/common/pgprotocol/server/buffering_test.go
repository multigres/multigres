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
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/bufpool"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingNetConn is a net.Conn that records the number of underlying
// Write calls — i.e. how many times the bufio.Writer above us flushes
// to the socket. The whole point of the start/end buffering window is
// to drop this count from "one per pgwire reply message" to "one per
// pipelined batch", so a counting net.Conn is the cleanest fixture for
// asserting the optimization.
type countingNetConn struct {
	readBuf  *bytes.Buffer
	writeBuf bytes.Buffer
	writes   atomic.Int64
}

func (c *countingNetConn) Read(b []byte) (int, error)         { return c.readBuf.Read(b) }
func (c *countingNetConn) Write(b []byte) (int, error)        { c.writes.Add(1); return c.writeBuf.Write(b) }
func (c *countingNetConn) Close() error                       { return nil }
func (c *countingNetConn) LocalAddr() net.Addr                { return nil }
func (c *countingNetConn) RemoteAddr() net.Addr               { return nil }
func (c *countingNetConn) SetDeadline(t time.Time) error      { return nil }
func (c *countingNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *countingNetConn) SetWriteDeadline(t time.Time) error { return nil }

// newBufferingTestConn wires a Conn to a counting net.Conn with the
// pool plumbing (writersPool, bufPool) needed for startPacket /
// writePacket to take their fast/slow paths exactly as production
// does. Unlike createExtendedQueryTestConn, we do NOT pre-attach a
// bufferedWriter — that's the whole point of testing buffering-window
// lifecycle, so the caller drives startWriterBuffering /
// endWriterBuffering itself.
func newBufferingTestConn(t *testing.T, readBuf *bytes.Buffer, handler Handler) (*Conn, *countingNetConn) {
	t.Helper()
	netConn := &countingNetConn{readBuf: readBuf}
	listener := &Listener{
		logger: slog.New(slog.DiscardHandler),
		writersPool: &sync.Pool{
			New: func() any { return bufio.NewWriterSize(nil, connBufferSize) },
		},
		bufPool: bufpool.New(16*1024, 64*1024*1024),
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	c := &Conn{
		conn:           netConn,
		bufferedReader: bufio.NewReader(netConn),
		listener:       listener,
		handler:        handler,
		logger:         listener.logger,
		txnStatus:      protocol.TxnStatusIdle,
		ctx:            ctx,
		cancel:         cancel,
	}
	return c, netConn
}

// TestBufferingWindow_OneSyscallPerBatch is the headline assertion: a
// pipelined extended-protocol batch (Parse + Bind + Describe + Execute
// + Sync), driven through the real handlers, must produce exactly one
// Write call against the underlying socket. Before this PR the same
// sequence triggered one Write per inbound message because each
// handler flushed individually; this test pins the new behavior so a
// regression that re-introduces a per-handler flush() lights up
// immediately.
func TestBufferingWindow_OneSyscallPerBatch(t *testing.T) {
	var readBuf bytes.Buffer
	handler := &testHandler{}

	// Encode the inbound batch: Parse + Bind + Describe + Execute + Sync.
	// We skip the type bytes here because each handler is invoked
	// directly (the dispatch in serve() is what pulls the type byte
	// off the wire).
	stmtName := "s1"
	queryStr := "SELECT 1"
	parseLength := int32(4 + len(stmtName) + 1 + len(queryStr) + 1 + 2)
	writeTestInt32(&readBuf, parseLength)
	writeTestString(&readBuf, stmtName)
	writeTestString(&readBuf, queryStr)
	writeTestInt16(&readBuf, 0)

	portalName := "p1"
	bindLength := int32(4 + len(portalName) + 1 + len(stmtName) + 1 + 2 + 2 + 2)
	writeTestInt32(&readBuf, bindLength)
	writeTestString(&readBuf, portalName)
	writeTestString(&readBuf, stmtName)
	writeTestInt16(&readBuf, 0)
	writeTestInt16(&readBuf, 0)
	writeTestInt16(&readBuf, 0)

	// Describe the portal ('P') — exercises the path that emits
	// RowDescription mid-batch.
	describeLength := int32(4 + 1 + len(portalName) + 1)
	writeTestInt32(&readBuf, describeLength)
	readBuf.WriteByte('P')
	writeTestString(&readBuf, portalName)

	executeLength := int32(4 + len(portalName) + 1 + 4)
	writeTestInt32(&readBuf, executeLength)
	writeTestString(&readBuf, portalName)
	writeTestInt32(&readBuf, 0)

	writeTestInt32(&readBuf, 4) // Sync has no body

	c, netConn := newBufferingTestConn(t, &readBuf, handler)

	// Drive the same sequence serve() does: open the buffering
	// window, dispatch each handler, only release at the Sync
	// boundary.
	c.startWriterBuffering()
	require.NoError(t, c.handleParse())
	require.NoError(t, c.handleBind())
	require.NoError(t, c.handleDescribe())
	require.NoError(t, c.handleExecute())
	require.NoError(t, c.handleSync())
	require.NoError(t, c.endWriterBuffering())

	assert.Equal(t, int64(1), netConn.writes.Load(),
		"the whole batch should land in exactly one socket Write — got %d", netConn.writes.Load())

	// Sanity-check that the bytes actually contain the reply
	// messages. If the buffering window were silently dropping
	// data, writes would be 1 but the buffer would be empty.
	assert.Greater(t, netConn.writeBuf.Len(), 0,
		"flushed buffer must contain reply bytes")
}

// TestBufferingWindow_PerHandlerFlushBaseline verifies the baseline
// shape (one flush after every handler) actually does produce one
// Write per handler — i.e. the assertion above is meaningful, not an
// artifact of bufio swallowing flushes. The pre-PR production
// behavior is what this test simulates, but it never matched a real
// production code path; we keep it here only so the diff between this
// test and TestBufferingWindow_OneSyscallPerBatch documents the win.
func TestBufferingWindow_PerHandlerFlushBaseline(t *testing.T) {
	var readBuf bytes.Buffer
	handler := &testHandler{}

	stmtName := "s1"
	queryStr := "SELECT 1"
	parseLength := int32(4 + len(stmtName) + 1 + len(queryStr) + 1 + 2)
	writeTestInt32(&readBuf, parseLength)
	writeTestString(&readBuf, stmtName)
	writeTestString(&readBuf, queryStr)
	writeTestInt16(&readBuf, 0)

	portalName := "p1"
	bindLength := int32(4 + len(portalName) + 1 + len(stmtName) + 1 + 2 + 2 + 2)
	writeTestInt32(&readBuf, bindLength)
	writeTestString(&readBuf, portalName)
	writeTestString(&readBuf, stmtName)
	writeTestInt16(&readBuf, 0)
	writeTestInt16(&readBuf, 0)
	writeTestInt16(&readBuf, 0)

	executeLength := int32(4 + len(portalName) + 1 + 4)
	writeTestInt32(&readBuf, executeLength)
	writeTestString(&readBuf, portalName)
	writeTestInt32(&readBuf, 0)

	writeTestInt32(&readBuf, 4)

	c, netConn := newBufferingTestConn(t, &readBuf, handler)

	// Force a flush after every handler — the pre-PR shape.
	c.startWriterBuffering()
	require.NoError(t, c.handleParse())
	require.NoError(t, c.flush())
	require.NoError(t, c.handleBind())
	require.NoError(t, c.flush())
	require.NoError(t, c.handleExecute())
	require.NoError(t, c.flush())
	require.NoError(t, c.handleSync())
	require.NoError(t, c.endWriterBuffering())

	assert.Equal(t, int64(4), netConn.writes.Load(),
		"per-handler flush should land 4 socket Writes — got %d", netConn.writes.Load())
}

// TestBufferingWindow_SimpleQueryFlushesOnce drives the simple-Q path
// with a result-producing handler. The reply is a 4-message stream —
// RowDescription + DataRow + CommandComplete + ReadyForQuery — and the
// whole thing must land in exactly one socket Write. Without an explicit
// streaming callback that actually emits rows, handleQuery would only
// write ReadyForQuery, so this test plumbs a HandleQuery that invokes
// the callback with a one-row Result. That makes the "one syscall for
// a multi-message reply" assertion meaningful.
func TestBufferingWindow_SimpleQueryFlushesOnce(t *testing.T) {
	var readBuf bytes.Buffer
	handler := &testHandler{
		queryFunc: func(ctx context.Context, conn *Conn, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error {
			return callback(ctx, &sqltypes.Result{
				Fields: []*query.Field{
					{Name: "id", DataTypeOid: 23},
				},
				Rows: []*sqltypes.Row{
					{Values: []sqltypes.Value{[]byte("1")}},
				},
				CommandTag: "SELECT 1",
			})
		},
	}

	queryStr := "SELECT 1"
	queryLength := int32(4 + len(queryStr) + 1)
	writeTestInt32(&readBuf, queryLength)
	writeTestString(&readBuf, queryStr)

	c, netConn := newBufferingTestConn(t, &readBuf, handler)

	c.startWriterBuffering()
	require.NoError(t, c.handleQuery())
	require.NoError(t, c.endWriterBuffering())

	assert.Equal(t, int64(1), netConn.writes.Load(),
		"simple-Q reply (RowDescription + DataRow + CommandComplete + ReadyForQuery) "+
			"should land in one socket Write — got %d", netConn.writes.Load())

	// The reply has to be RowDescription + DataRow + CommandComplete
	// + ReadyForQuery — four message-type bytes. Probe the buffer to
	// catch a regression where the single Write is just ReadyForQuery
	// (which would happen if HandleQuery silently swallowed the result).
	wire := netConn.writeBuf.Bytes()
	want := []byte{
		protocol.MsgRowDescription,
		protocol.MsgDataRow,
		protocol.MsgCommandComplete,
		protocol.MsgReadyForQuery,
	}
	gotTypes := make([]byte, 0, 4)
	for i := 0; i+5 <= len(wire); {
		gotTypes = append(gotTypes, wire[i])
		// Skip past message body using the 4-byte length field.
		bodyLen := int(wire[i+1])<<24 | int(wire[i+2])<<16 | int(wire[i+3])<<8 | int(wire[i+4])
		i += 1 + bodyLen
	}
	assert.Equal(t, want, gotTypes,
		"simple-Q reply must contain RowDescription, DataRow, CommandComplete, "+
			"ReadyForQuery in order — got %v", gotTypes)
}

// TestBufferingWindow_ParseErrorDeferredToSync verifies that a Parse
// error packet stays buffered and is delivered together with the
// trailing Sync's ReadyForQuery — matching what postgres itself does.
// If we accidentally re-introduce an early flush in the error path,
// this test catches it.
func TestBufferingWindow_ParseErrorDeferredToSync(t *testing.T) {
	var readBuf bytes.Buffer
	handler := &testHandler{
		parseFunc: func(ctx context.Context, conn *Conn, name, query string, paramTypes []uint32) error {
			return assert.AnError
		},
	}

	stmtName := "s1"
	queryStr := "BAD"
	parseLength := int32(4 + len(stmtName) + 1 + len(queryStr) + 1 + 2)
	writeTestInt32(&readBuf, parseLength)
	writeTestString(&readBuf, stmtName)
	writeTestString(&readBuf, queryStr)
	writeTestInt16(&readBuf, 0)

	writeTestInt32(&readBuf, 4) // Sync

	c, netConn := newBufferingTestConn(t, &readBuf, handler)

	c.startWriterBuffering()
	require.NoError(t, c.handleParse()) // writes ErrorResponse, no flush
	require.Equal(t, int64(0), netConn.writes.Load(),
		"Parse error must not trigger an early flush — got %d writes", netConn.writes.Load())

	require.NoError(t, c.handleSync())
	require.NoError(t, c.endWriterBuffering())

	assert.Equal(t, int64(1), netConn.writes.Load(),
		"ErrorResponse + ReadyForQuery should coalesce — got %d", netConn.writes.Load())
}

// TestHandleMessage_FlushConsumesLength pins the regression for an
// off-by-four bug that lived in main pre-PR: handleMessage's MsgFlush
// case never read the 4-byte length field, so the bytes leaked into
// the read stream and the next ReadMessageType picked up the high
// byte of the length (always 0x00) as a bogus message-type.
//
// Real-world clients (libpq, pgx in non-batch flows) never send a
// standalone 'H' Flush, which is why no end-to-end test ever caught
// this. The test here drives the serve()-loop's read+dispatch shape
// directly: stuff 'H'+length(4) followed by 'S'+length(4) into the
// read buffer, run two ReadMessageType+handleMessage iterations, and
// assert the buffer ends EOF-clean. If the length leak comes back,
// the second ReadMessageType returns 0x00 instead of 'S' and this
// test fails.
func TestHandleMessage_FlushConsumesLength(t *testing.T) {
	var readBuf bytes.Buffer
	handler := &testHandler{}

	// First message: 'H' (Flush). Type byte + 4-byte length (= 4,
	// because the length field includes itself and there's no body).
	readBuf.WriteByte(protocol.MsgFlush)
	writeTestInt32(&readBuf, 4)

	// Second message: 'S' (Sync). Same shape.
	readBuf.WriteByte(protocol.MsgSync)
	writeTestInt32(&readBuf, 4)

	c, _ := newBufferingTestConn(t, &readBuf, handler)
	c.startWriterBuffering()
	defer func() { _ = c.endWriterBuffering() }()

	// Iteration 1: read 'H', dispatch — must consume the 4 length bytes.
	msgType, err := c.ReadMessageType()
	require.NoError(t, err)
	require.Equal(t, byte(protocol.MsgFlush), msgType)
	require.NoError(t, c.handleMessage(msgType))

	// Iteration 2: read 'S'. If MsgFlush leaked its length, we get
	// 0x00 here instead of 'S' and the test fails with a clear msg.
	msgType, err = c.ReadMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgSync), msgType,
		"after handleMessage(MsgFlush), the next ReadMessageType must "+
			"return 'S' — if it returned %#x instead, MsgFlush leaked "+
			"its 4-byte length into the stream", msgType)
	require.NoError(t, c.handleMessage(msgType))

	// Buffer must now be empty.
	_, err = c.ReadMessageType()
	assert.ErrorIs(t, err, io.EOF,
		"read buffer should be drained after both messages consumed")
}
