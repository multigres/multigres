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

package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// writeRawMessage encodes a backend message with the standard 1-byte tag +
// 4-byte length-including-itself + body layout used by the PostgreSQL
// frontend/backend protocol.
func writeRawMessage(buf *bytes.Buffer, msgType byte, body []byte) {
	buf.WriteByte(msgType)
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(body)+4))
	buf.Write(lenBytes)
	buf.Write(body)
}

// buildReadyForQuery returns a serialized ReadyForQuery message with the
// given transaction status byte.
func buildReadyForQuery(status protocol.TransactionStatus) []byte {
	var b bytes.Buffer
	writeRawMessage(&b, protocol.MsgReadyForQuery, []byte{byte(status)})
	return b.Bytes()
}

// buildCommandComplete returns a serialized CommandComplete message carrying
// the given null-terminated tag.
func buildCommandComplete(tag string) []byte {
	var b bytes.Buffer
	body := append([]byte(tag), 0)
	writeRawMessage(&b, protocol.MsgCommandComplete, body)
	return b.Bytes()
}

// buildErrorResponse returns a serialized ErrorResponse message with the
// minimum field set needed for parseError (SQLSTATE + message). Each field
// is a 1-byte type, a null-terminated value; the field list ends with a
// single 0 byte.
func buildErrorResponse(sqlstate, message string) []byte {
	var body bytes.Buffer
	body.WriteByte('S') // severity
	body.WriteString("ERROR")
	body.WriteByte(0)
	body.WriteByte('C') // SQLSTATE
	body.WriteString(sqlstate)
	body.WriteByte(0)
	body.WriteByte('M') // human-readable message
	body.WriteString(message)
	body.WriteByte(0)
	body.WriteByte(0) // end of fields

	var out bytes.Buffer
	writeRawMessage(&out, protocol.MsgErrorResponse, body.Bytes())
	return out.Bytes()
}

// newTestReadOnlyConn builds a Conn whose reader is fed from the supplied
// in-memory bytes. Suitable for read-path unit tests.
func newTestReadOnlyConn(input []byte) *Conn {
	r := bytes.NewReader(input)
	return &Conn{
		conn:           &mockNetConn{buf: bytes.NewBuffer(nil)},
		bufferedReader: bufio.NewReader(r),
		bufferedWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
	}
}

func TestReadCopyDoneResponse_Success(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildCommandComplete("COPY 7"))
	input.Write(buildReadyForQuery(protocol.TxnStatusIdle))

	c := newTestReadOnlyConn(input.Bytes())

	tag, rows, err := c.ReadCopyDoneResponse(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "COPY 7", tag)
	assert.Equal(t, uint64(7), rows)
	assert.Equal(t, protocol.TxnStatusIdle, c.txnStatus,
		"txnStatus must be updated from the trailing ReadyForQuery on the success path")
}

// TestReadCopyDoneResponse_ErrorResponseDrainsReadyForQuery exercises the
// new error path: ReadCopyDoneResponse must parse the PG error AND read
// past the trailing ReadyForQuery so the next operation on this socket
// does not see a leftover RFQ as its first response. The drained RFQ also
// updates txnStatus so callers can observe TxnStatusFailed when a COPY
// finalization error happens inside a transaction.
func TestReadCopyDoneResponse_ErrorResponseDrainsReadyForQuery(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildErrorResponse("23505", "duplicate key value violates unique constraint"))
	input.Write(buildReadyForQuery(protocol.TxnStatusFailed))

	c := newTestReadOnlyConn(input.Bytes())

	tag, rows, err := c.ReadCopyDoneResponse(context.Background())
	require.Error(t, err)
	assert.Empty(t, tag)
	assert.Equal(t, uint64(0), rows)

	// Underlying error should be the PgDiagnostic from the ErrorResponse.
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "wrapped error should expose *PgDiagnostic")
	assert.Equal(t, "23505", diag.Code)
	assert.Contains(t, diag.Message, "duplicate key")

	// RFQ must have been drained — the buffered reader has no leftover bytes.
	leftover, _ := c.bufferedReader.Peek(1)
	assert.Empty(t, leftover, "trailing ReadyForQuery must be fully consumed")

	// Transaction status must reflect what the server signaled in the RFQ.
	assert.Equal(t, protocol.TxnStatusFailed, c.txnStatus,
		"txnStatus must be updated from the drained ReadyForQuery on the error path")
}

func TestReadCopyDoneResponse_IgnoresNotices(t *testing.T) {
	var input bytes.Buffer
	// NoticeResponse uses the same wire shape as ErrorResponse and must be
	// skipped silently before the CommandComplete.
	var notice bytes.Buffer
	notice.WriteByte('S')
	notice.WriteString("NOTICE")
	notice.WriteByte(0)
	notice.WriteByte('M')
	notice.WriteString("hello")
	notice.WriteByte(0)
	notice.WriteByte(0)
	writeRawMessage(&input, protocol.MsgNoticeResponse, notice.Bytes())
	input.Write(buildCommandComplete("COPY 3"))
	input.Write(buildReadyForQuery(protocol.TxnStatusInBlock))

	c := newTestReadOnlyConn(input.Bytes())

	tag, rows, err := c.ReadCopyDoneResponse(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "COPY 3", tag)
	assert.Equal(t, uint64(3), rows)
	assert.Equal(t, protocol.TxnStatusInBlock, c.txnStatus)
}

func TestReadCopyDoneResponse_ReadyForQueryWithoutCommandComplete(t *testing.T) {
	// Defensive case: server returned RFQ without a preceding CommandComplete
	// or ErrorResponse. The function must surface that as an error rather
	// than silently returning empty success.
	c := newTestReadOnlyConn(buildReadyForQuery(protocol.TxnStatusIdle))

	_, _, err := c.ReadCopyDoneResponse(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ReadyForQuery without CommandComplete")
}
