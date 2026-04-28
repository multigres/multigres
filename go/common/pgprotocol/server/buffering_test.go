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
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/bufpool"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"

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
	c.endWriterBuffering()

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
	c.endWriterBuffering()

	assert.Equal(t, int64(4), netConn.writes.Load(),
		"per-handler flush should land 4 socket Writes — got %d", netConn.writes.Load())
}

// TestBufferingWindow_SimpleQueryFlushesOnce drives the simple-Q path
// (a single 'Q' message produces RowDescription + DataRow +
// CommandComplete + ReadyForQuery) and asserts the reply is one
// syscall.
func TestBufferingWindow_SimpleQueryFlushesOnce(t *testing.T) {
	var readBuf bytes.Buffer
	handler := &testHandler{}

	queryStr := "SELECT 1"
	queryLength := int32(4 + len(queryStr) + 1)
	writeTestInt32(&readBuf, queryLength)
	writeTestString(&readBuf, queryStr)

	c, netConn := newBufferingTestConn(t, &readBuf, handler)

	c.startWriterBuffering()
	require.NoError(t, c.handleQuery())
	c.endWriterBuffering()

	assert.Equal(t, int64(1), netConn.writes.Load(),
		"simple-Q reply should land in one socket Write — got %d", netConn.writes.Load())
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
	c.endWriterBuffering()

	assert.Equal(t, int64(1), netConn.writes.Load(),
		"ErrorResponse + ReadyForQuery should coalesce — got %d", netConn.writes.Load())
}
