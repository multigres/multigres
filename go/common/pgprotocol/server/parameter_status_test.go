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

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// writeSimpleQuery appends a simple-query ('Q' body, no type byte — handleQuery
// reads the length-prefixed body directly) message for queryStr to buf.
func writeSimpleQuery(buf *bytes.Buffer, queryStr string) {
	writeTestInt32(buf, int32(4+len(queryStr)+1))
	writeTestString(buf, queryStr)
}

// wireParameterStatuses walks a pgwire byte stream and returns the (name, value)
// of every ParameterStatus ('S') message, in order.
func wireParameterStatuses(t *testing.T, wire []byte) [][2]string {
	t.Helper()
	var out [][2]string
	for i := 0; i+5 <= len(wire); {
		bodyLen := int(wire[i+1])<<24 | int(wire[i+2])<<16 | int(wire[i+3])<<8 | int(wire[i+4])
		msgEnd := i + 1 + bodyLen
		if bodyLen < 4 || msgEnd > len(wire) {
			break
		}
		if wire[i] == protocol.MsgParameterStatus {
			parts := bytes.SplitN(wire[i+5:msgEnd], []byte{0}, 3)
			require.GreaterOrEqual(t, len(parts), 2, "ParameterStatus body must be name\\0value\\0")
			out = append(out, [2]string{string(parts[0]), string(parts[1])})
		}
		i = msgEnd
	}
	return out
}

// wireMsgIndex returns the byte offset of the first message of typ, or -1.
func wireMsgIndex(wire []byte, typ byte) int {
	for i := 0; i+5 <= len(wire); {
		bodyLen := int(wire[i+1])<<24 | int(wire[i+2])<<16 | int(wire[i+3])<<8 | int(wire[i+4])
		if bodyLen < 4 || i+1+bodyLen > len(wire) {
			return -1
		}
		if wire[i] == typ {
			return i
		}
		i += 1 + bodyLen
	}
	return -1
}

func runOneQuery(t *testing.T, c *Conn, netConn *countingNetConn) []byte {
	t.Helper()
	c.startWriterBuffering()
	require.NoError(t, c.handleQuery())
	require.NoError(t, c.endWriterBuffering())
	wire := append([]byte(nil), netConn.writeBuf.Bytes()...)
	netConn.writeBuf.Reset()
	return wire
}

// TestHandleQuery_EmitsParameterStatusAfterCommandComplete verifies that a SET
// result carrying a GUC_REPORT value is emitted as a ParameterStatus message,
// after CommandComplete and before ReadyForQuery — PostgreSQL's order.
func TestHandleQuery_EmitsParameterStatusAfterCommandComplete(t *testing.T) {
	var readBuf bytes.Buffer
	writeSimpleQuery(&readBuf, "SET client_encoding = 'LATIN1'")
	handler := &testHandler{
		queryFunc: func(ctx context.Context, _ *Conn, _ string, callback func(context.Context, *sqltypes.Result) error) error {
			return callback(ctx, &sqltypes.Result{
				CommandTag:      "SET",
				ParameterStatus: map[string]string{"client_encoding": "LATIN1"},
			})
		},
	}

	c, netConn := newBufferingTestConn(t, &readBuf, handler)
	wire := runOneQuery(t, c, netConn)

	ps := wireParameterStatuses(t, wire)
	require.Len(t, ps, 1, "expected exactly one ParameterStatus")
	assert.Equal(t, [2]string{"client_encoding", "LATIN1"}, ps[0])

	cc := wireMsgIndex(wire, protocol.MsgCommandComplete)
	psIdx := wireMsgIndex(wire, protocol.MsgParameterStatus)
	rfq := wireMsgIndex(wire, protocol.MsgReadyForQuery)
	require.NotEqual(t, -1, cc)
	require.NotEqual(t, -1, psIdx)
	require.NotEqual(t, -1, rfq)
	assert.Less(t, cc, psIdx, "ParameterStatus must come after CommandComplete")
	assert.Less(t, psIdx, rfq, "ParameterStatus must come before ReadyForQuery")
}

// TestHandleQuery_ParameterStatusOnlyResult verifies that a CommandTag-less
// result carrying a ParameterStatus (the routed-forward path, e.g. an in-txn SET
// whose value arrives from the backend) still emits the message.
func TestHandleQuery_ParameterStatusOnlyResult(t *testing.T) {
	var readBuf bytes.Buffer
	writeSimpleQuery(&readBuf, "SET client_encoding = 'UTF8'")
	handler := &testHandler{
		queryFunc: func(ctx context.Context, _ *Conn, _ string, callback func(context.Context, *sqltypes.Result) error) error {
			if err := callback(ctx, &sqltypes.Result{CommandTag: "SET"}); err != nil {
				return err
			}
			return callback(ctx, &sqltypes.Result{ParameterStatus: map[string]string{"client_encoding": "UTF8"}})
		},
	}

	c, netConn := newBufferingTestConn(t, &readBuf, handler)
	wire := runOneQuery(t, c, netConn)

	ps := wireParameterStatuses(t, wire)
	require.Len(t, ps, 1)
	assert.Equal(t, [2]string{"client_encoding", "UTF8"}, ps[0])
}

// TestHandleQuery_ParameterStatusChangeDetection verifies the wire server does
// not re-emit a ParameterStatus whose value did not change — PostgreSQL reports
// a GUC_REPORT parameter on change only.
func TestHandleQuery_ParameterStatusChangeDetection(t *testing.T) {
	var readBuf bytes.Buffer
	writeSimpleQuery(&readBuf, "SET client_encoding = 'LATIN1'")
	writeSimpleQuery(&readBuf, "SET client_encoding = 'LATIN1'")
	handler := &testHandler{
		queryFunc: func(ctx context.Context, _ *Conn, _ string, callback func(context.Context, *sqltypes.Result) error) error {
			return callback(ctx, &sqltypes.Result{
				CommandTag:      "SET",
				ParameterStatus: map[string]string{"client_encoding": "LATIN1"},
			})
		},
	}

	c, netConn := newBufferingTestConn(t, &readBuf, handler)

	first := runOneQuery(t, c, netConn)
	assert.Len(t, wireParameterStatuses(t, first), 1, "first SET reports the change")

	second := runOneQuery(t, c, netConn)
	assert.Empty(t, wireParameterStatuses(t, second), "identical second SET must be deduped")
}
