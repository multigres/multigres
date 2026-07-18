// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// wireMsg is one synthetic backend message: a type byte and a body (already
// stripped of the 5-byte header, matching what readMessage hands to callers).
type wireMsg struct {
	typ  byte
	body []byte
}

// buildBackendStream frames each message as the backend would send it on the
// wire: type byte, int32 length that counts itself plus the body (but not the
// type byte), then the body.
func buildBackendStream(msgs ...wireMsg) []byte {
	var out []byte
	for _, m := range msgs {
		out = append(out, m.typ)
		out = binary.BigEndian.AppendUint32(out, uint32(len(m.body)+4))
		out = append(out, m.body...)
	}
	return out
}

func rowDescriptionBody(name string) []byte {
	w := NewMessageWriter()
	w.WriteInt16(1)     // one field
	w.WriteString(name) // field name
	w.WriteUint32(0)    // table OID
	w.WriteInt16(0)     // attribute number
	w.WriteUint32(25)   // data type OID (text)
	w.WriteInt16(-1)    // data type size (variable)
	w.WriteInt32(-1)    // type modifier
	w.WriteInt16(0)     // format code (text)
	return w.Bytes()
}

func dataRowBody(value string) []byte {
	w := NewMessageWriter()
	w.WriteInt16(1) // one column
	w.WriteByteString([]byte(value))
	return w.Bytes()
}

func commandCompleteBody(tag string) []byte {
	w := NewMessageWriter()
	w.WriteString(tag)
	return w.Bytes()
}

// readyForQueryBody carries a single transaction-status byte ('I' = idle).
func readyForQueryBody() []byte { return []byte{'I'} }

// collectResults returns a callback that appends every delivered Result and the
// slice it writes to.
func collectResults() (func(context.Context, *sqltypes.Result) error, *[]*sqltypes.Result) {
	var got []*sqltypes.Result
	cb := func(_ context.Context, r *sqltypes.Result) error {
		got = append(got, r)
		return nil
	}
	return cb, &got
}

// bigValue exceeds DefaultStreamingBatchSize so the first DataRow forces a
// mid-stream flush, exercising the flush path in addition to the final batch.
var bigValue = strings.Repeat("x", DefaultStreamingBatchSize+16)

// responseLoop names one of the streaming response readers and how to drive it,
// so a single table can exercise every loop in both opaque and structured mode.
type responseLoop struct {
	name     string
	preamble []wireMsg // messages that precede the execute results (BindComplete, etc.)
	invoke   func(c *Conn, cb func(context.Context, *sqltypes.Result) error) error
}

func responseLoops() []responseLoop {
	drop := func(_ bool, err error) error { return err } // discard the completed flag
	return []responseLoop{
		{"query", nil, func(c *Conn, cb func(context.Context, *sqltypes.Result) error) error {
			return c.processQueryResponses(context.Background(), cb)
		}},
		{"execute", nil, func(c *Conn, cb func(context.Context, *sqltypes.Result) error) error {
			return drop(c.processExecuteResponses(context.Background(), cb))
		}},
		{"bindExecute", []wireMsg{{protocol.MsgBindComplete, nil}}, func(c *Conn, cb func(context.Context, *sqltypes.Result) error) error {
			return drop(c.processBindAndExecuteResponses(context.Background(), cb))
		}},
		{"prepareExecute", []wireMsg{{protocol.MsgParseComplete, nil}, {protocol.MsgBindComplete, nil}}, func(c *Conn, cb func(context.Context, *sqltypes.Result) error) error {
			return c.processPrepareAndExecuteResponses(context.Background(), cb)
		}},
		{"bindDescribeExecute", []wireMsg{{protocol.MsgBindComplete, nil}}, func(c *Conn, cb func(context.Context, *sqltypes.Result) error) error {
			return drop(c.processBindDescribeAndExecuteResponses(context.Background(), cb))
		}},
	}
}

// TestProcessResponseLoops drives every streaming response reader in both
// opaque passthrough and structured mode. The stream carries one row larger
// than the batch threshold followed by a small row, so each loop performs a
// mid-stream flush and a final CommandComplete batch. It asserts two rows are
// delivered in total, carried opaquely (PassthroughBlock) or structurally (Rows) per the
// mode, and never both.
func TestProcessResponseLoops(t *testing.T) {
	for _, loop := range responseLoops() {
		for _, opaque := range []bool{true, false} {
			mode := "structured"
			if opaque {
				mode = "opaque"
			}
			t.Run(loop.name+"/"+mode, func(t *testing.T) {
				msgs := append(append([]wireMsg{}, loop.preamble...),
					wireMsg{protocol.MsgRowDescription, rowDescriptionBody("col")},
					wireMsg{protocol.MsgDataRow, dataRowBody(bigValue)},
					wireMsg{protocol.MsgDataRow, dataRowBody("small")},
					wireMsg{protocol.MsgCommandComplete, commandCompleteBody("SELECT 2")},
					wireMsg{protocol.MsgReadyForQuery, readyForQueryBody()},
				)
				c := &Conn{bufferedReader: bufio.NewReader(bytes.NewReader(buildBackendStream(msgs...)))}
				c.SetPassthroughRow(opaque)

				cb, got := collectResults()
				require.NoError(t, loop.invoke(c, cb))
				require.GreaterOrEqual(t, len(*got), 2, "a mid-stream flush plus a final batch")

				var passthroughRows, structRows int
				for _, r := range *got {
					passthroughRows += r.PassthroughRowCount
					structRows += len(r.Rows)
				}
				if opaque {
					assert.Equal(t, 2, passthroughRows, "both rows delivered opaquely")
					assert.Zero(t, structRows, "opaque mode must not parse structured rows")
				} else {
					assert.Equal(t, 2, structRows, "both rows delivered structured")
					assert.Zero(t, passthroughRows, "structured mode must not produce opaque blocks")
				}
			})
		}
	}
}

// TestProcessQueryResponsesParseError verifies a malformed DataRow in structured
// mode surfaces a parse error rather than being silently dropped.
func TestProcessQueryResponsesParseError(t *testing.T) {
	malformed := func() []byte {
		w := NewMessageWriter()
		w.WriteInt16(1)   // claims one column...
		w.WriteInt32(100) // ...of 100 bytes, but none follow
		return w.Bytes()
	}()
	stream := buildBackendStream(
		wireMsg{protocol.MsgRowDescription, rowDescriptionBody("col")},
		wireMsg{protocol.MsgDataRow, malformed},
		wireMsg{protocol.MsgReadyForQuery, readyForQueryBody()},
	)
	c := &Conn{bufferedReader: bufio.NewReader(bytes.NewReader(stream))}
	cb, _ := collectResults()
	assert.Error(t, c.processQueryResponses(context.Background(), cb))
}

// TestResultBatcherStructured exercises the structured branch of the shared
// resultBatcher: parsing DataRow bodies into rows, flushing an accumulated
// batch, and emitting a final batch with the command tag. The opaque branch is
// covered end-to-end by the process-loop tests above.
func TestResultBatcherStructured(t *testing.T) {
	b := &resultBatcher{c: &Conn{}} // passthroughRow defaults to false
	b.fields = []*query.Field{{Name: "col"}}

	require.NoError(t, b.addDataRow(dataRowBody("aaa")))
	require.NoError(t, b.addDataRow(dataRowBody("bbb")))
	assert.False(t, b.overThreshold(), "two tiny rows stay under the flush threshold")

	flushed := b.flush()
	require.NotNil(t, flushed)
	assert.Len(t, flushed.Rows, 2, "flush returns the accumulated rows")
	assert.Nil(t, flushed.PassthroughBlock)
	assert.Nil(t, b.flush(), "flush after reset returns nil")

	require.NoError(t, b.addDataRow(dataRowBody("ccc")))
	final := b.final("SELECT 3")
	assert.Equal(t, "SELECT 3", final.CommandTag)
	assert.Len(t, final.Rows, 1, "final carries rows accumulated since the last flush")

	// A malformed DataRow surfaces the parse error in structured mode.
	bad := NewMessageWriter()
	bad.WriteInt16(1)   // one column...
	bad.WriteInt32(100) // ...of 100 bytes, but none follow
	assert.Error(t, b.addDataRow(bad.Bytes()))
}

// TestResultBatcherEmptyFlush verifies flush returns nil (nothing to deliver)
// when no rows have been accumulated, in both modes.
func TestResultBatcherEmptyFlush(t *testing.T) {
	structured := &resultBatcher{c: &Conn{}}
	assert.Nil(t, structured.flush(), "structured flush with no rows")

	opaque := &Conn{}
	opaque.SetPassthroughRow(true)
	assert.Nil(t, (&resultBatcher{c: opaque}).flush(), "opaque flush with no rows")
}
