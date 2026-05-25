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

	tag, rows, notices, err := c.ReadCopyDoneResponse(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "COPY 7", tag)
	assert.Equal(t, uint64(7), rows)
	assert.Empty(t, notices, "no notices on the success path")
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

	tag, rows, notices, err := c.ReadCopyDoneResponse(context.Background())
	require.Error(t, err)
	assert.Empty(t, tag)
	assert.Equal(t, uint64(0), rows)
	assert.Empty(t, notices, "no preceding notices in this fixture")

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

func TestReadCopyDoneResponse_CapturesNotices(t *testing.T) {
	var input bytes.Buffer
	// NoticeResponse uses the same wire shape as ErrorResponse and arrives
	// between CopyDone and CommandComplete when triggers fire during the COPY.
	// Trigger / progress notices must be captured and surfaced to the
	// gateway so the client sees the expected NOTICE / INFO lines.
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

	tag, rows, notices, err := c.ReadCopyDoneResponse(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "COPY 3", tag)
	assert.Equal(t, uint64(3), rows)
	require.Len(t, notices, 1, "trigger NOTICE between CopyDone and CommandComplete must be captured")
	assert.Equal(t, "NOTICE", notices[0].Severity)
	assert.Equal(t, "hello", notices[0].Message)
	assert.Equal(t, protocol.TxnStatusInBlock, c.txnStatus)
}

func TestReadCopyDoneResponse_ReadyForQueryWithoutCommandComplete(t *testing.T) {
	// Defensive case: server returned RFQ without a preceding CommandComplete
	// or ErrorResponse. The function must surface that as an error rather
	// than silently returning empty success.
	c := newTestReadOnlyConn(buildReadyForQuery(protocol.TxnStatusIdle))

	_, _, _, err := c.ReadCopyDoneResponse(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ReadyForQuery without CommandComplete")
}

// buildCopyData returns a serialized CopyData ('d') message carrying the
// given payload.
func buildCopyData(payload []byte) []byte {
	var b bytes.Buffer
	writeRawMessage(&b, protocol.MsgCopyData, payload)
	return b.Bytes()
}

// buildCopyDone returns a serialized CopyDone ('c') message (zero-length body).
func buildCopyDone() []byte {
	var b bytes.Buffer
	writeRawMessage(&b, protocol.MsgCopyDone, nil)
	return b.Bytes()
}

// buildNoticeResponse returns a serialized NoticeResponse message with the
// minimum field set parseNotice cares about.
func buildNoticeResponse(severity, message string) []byte {
	var body bytes.Buffer
	body.WriteByte('S')
	body.WriteString(severity)
	body.WriteByte(0)
	body.WriteByte('M')
	body.WriteString(message)
	body.WriteByte(0)
	body.WriteByte(0)

	var out bytes.Buffer
	writeRawMessage(&out, protocol.MsgNoticeResponse, body.Bytes())
	return out.Bytes()
}

// TestReadCopyOutMessage covers the three terminal frame kinds the COPY TO
// STDOUT response stream can deliver: CopyData (row payload), CopyDone
// (end-of-stream), and NoticeResponse (interleaved diagnostic). Each is
// returned via a distinct field on CopyOutMessage so the caller's
// dispatch table can be exhaustive.
func TestReadCopyOutMessage(t *testing.T) {
	t.Run("CopyData returns payload", func(t *testing.T) {
		c := newTestReadOnlyConn(buildCopyData([]byte("1\tAlice\n")))

		msg, err := c.ReadCopyOutMessage(context.Background())
		require.NoError(t, err)
		assert.Equal(t, []byte("1\tAlice\n"), msg.Data)
		assert.Nil(t, msg.Notice)
		assert.False(t, msg.Done)
	})

	t.Run("CopyDone returns Done=true", func(t *testing.T) {
		c := newTestReadOnlyConn(buildCopyDone())

		msg, err := c.ReadCopyOutMessage(context.Background())
		require.NoError(t, err)
		assert.True(t, msg.Done)
		assert.Nil(t, msg.Data)
		assert.Nil(t, msg.Notice)
	})

	t.Run("NoticeResponse returns Notice", func(t *testing.T) {
		c := newTestReadOnlyConn(buildNoticeResponse("NOTICE", "trigger fired"))

		msg, err := c.ReadCopyOutMessage(context.Background())
		require.NoError(t, err)
		require.NotNil(t, msg.Notice)
		assert.Equal(t, "NOTICE", msg.Notice.Severity)
		assert.Equal(t, "trigger fired", msg.Notice.Message)
		assert.Nil(t, msg.Data)
		assert.False(t, msg.Done)
	})

	t.Run("ErrorResponse parses to PgDiagnostic and drains RFQ", func(t *testing.T) {
		// PG ErrorResponse mid-stream — the helper must parse the error,
		// drain the trailing ReadyForQuery so the conn is left clean, and
		// surface a *PgDiagnostic to the caller (via errors.As).
		var input bytes.Buffer
		input.Write(buildErrorResponse("57014", "canceling statement due to user request"))
		input.Write(buildReadyForQuery(protocol.TxnStatusFailed))

		c := newTestReadOnlyConn(input.Bytes())

		_, err := c.ReadCopyOutMessage(context.Background())
		require.Error(t, err)
		var diag *mterrors.PgDiagnostic
		require.True(t, errors.As(err, &diag))
		assert.Equal(t, "57014", diag.Code)

		// RFQ drained — buffered reader has no leftover bytes.
		leftover, _ := c.bufferedReader.Peek(1)
		assert.Empty(t, leftover)
	})
}

// TestFinishCopyToStdout exercises the trailing CommandComplete +
// ReadyForQuery drain after CopyDone. Notices arriving in this window
// (AFTER STATEMENT triggers, COPY progress finalization) must be
// returned alongside the command tag so the gateway can re-emit them
// to the client before CommandComplete.
func TestFinishCopyToStdout(t *testing.T) {
	t.Run("Success returns tag, rows, and trailing notices", func(t *testing.T) {
		var input bytes.Buffer
		input.Write(buildNoticeResponse("NOTICE", "after-statement"))
		input.Write(buildCommandComplete("COPY 5"))
		input.Write(buildReadyForQuery(protocol.TxnStatusIdle))

		c := newTestReadOnlyConn(input.Bytes())

		tag, rows, notices, err := c.FinishCopyToStdout(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "COPY 5", tag)
		assert.Equal(t, uint64(5), rows)
		require.Len(t, notices, 1)
		assert.Equal(t, "after-statement", notices[0].Message)
		assert.Equal(t, protocol.TxnStatusIdle, c.txnStatus)
	})

	t.Run("ErrorResponse drains RFQ and returns PgDiagnostic", func(t *testing.T) {
		// Server-side abort mid-COPY (e.g. statement_timeout). FinishCopyToStdout
		// must drain RFQ so the conn is reusable.
		var input bytes.Buffer
		input.Write(buildErrorResponse("57014", "canceling statement"))
		input.Write(buildReadyForQuery(protocol.TxnStatusFailed))

		c := newTestReadOnlyConn(input.Bytes())

		_, _, _, err := c.FinishCopyToStdout(context.Background())
		require.Error(t, err)
		var diag *mterrors.PgDiagnostic
		require.True(t, errors.As(err, &diag))
		assert.Equal(t, "57014", diag.Code)
		leftover, _ := c.bufferedReader.Peek(1)
		assert.Empty(t, leftover)
	})

	t.Run("ReadyForQuery without CommandComplete is an error", func(t *testing.T) {
		// Defensive: server returned RFQ without CommandComplete or ErrorResponse.
		c := newTestReadOnlyConn(buildReadyForQuery(protocol.TxnStatusIdle))

		_, _, _, err := c.FinishCopyToStdout(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ReadyForQuery without CommandComplete")
	})
}

// buildCopyOutResponse returns a serialized CopyOutResponse ('H') message
// with the given overall format and column formats (text=0/binary=1 each).
func buildCopyOutResponse(format int16, columnFormats []int16) []byte {
	body := make([]byte, 0, 3+2*len(columnFormats))
	body = append(body, byte(format))
	body = append(body, byte(len(columnFormats)>>8), byte(len(columnFormats)))
	for _, f := range columnFormats {
		body = append(body, byte(f>>8), byte(f))
	}
	var out bytes.Buffer
	writeRawMessage(&out, protocol.MsgCopyOutResponse, body)
	return out.Bytes()
}

// newTestReadWriteConn builds a Conn where writes go to a discard buffer
// and reads pull from the supplied response bytes. Suitable for testing
// methods that send a query then read the response (InitiateCopyToStdout,
// InitiateCopyFromStdin, etc.).
func newTestReadWriteConn(response []byte) *Conn {
	return &Conn{
		conn:           &mockNetConn{buf: bytes.NewBuffer(nil)},
		bufferedReader: bufio.NewReader(bytes.NewReader(response)),
		bufferedWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
	}
}

// TestInitiateCopyToStdout covers the request/response flow of the
// COPY TO STDOUT initiator: send a query, read CopyOutResponse, capture
// any pre-CopyOutResponse notices, and surface PG ErrorResponse
// un-wrapped on the error path.
func TestInitiateCopyToStdout(t *testing.T) {
	t.Run("success returns format, columns, notices", func(t *testing.T) {
		var input bytes.Buffer
		input.Write(buildNoticeResponse("NOTICE", "before-statement trigger"))
		input.Write(buildCopyOutResponse(0, []int16{0, 0, 0}))

		c := newTestReadWriteConn(input.Bytes())
		format, columnFormats, notices, err := c.InitiateCopyToStdout(context.Background(), "COPY t TO STDOUT")
		require.NoError(t, err)
		assert.Equal(t, int16(0), format, "text format")
		assert.Equal(t, []int16{0, 0, 0}, columnFormats)
		require.Len(t, notices, 1)
		assert.Equal(t, "before-statement trigger", notices[0].Message)
	})

	t.Run("ErrorResponse drains RFQ and returns PgDiagnostic", func(t *testing.T) {
		// PG rejects the COPY (e.g. relation does not exist) before sending
		// CopyOutResponse. The helper must parse the error, drain RFQ so the
		// conn is reusable, and surface *PgDiagnostic via errors.As.
		var input bytes.Buffer
		input.Write(buildErrorResponse("42P01", "relation \"t\" does not exist"))
		input.Write(buildReadyForQuery(protocol.TxnStatusIdle))

		c := newTestReadWriteConn(input.Bytes())
		_, _, _, err := c.InitiateCopyToStdout(context.Background(), "COPY t TO STDOUT")
		require.Error(t, err)
		var diag *mterrors.PgDiagnostic
		require.True(t, errors.As(err, &diag))
		assert.Equal(t, "42P01", diag.Code)
		leftover, _ := c.bufferedReader.Peek(1)
		assert.Empty(t, leftover, "trailing RFQ drained")
	})

	t.Run("ReadyForQuery before CopyOutResponse is an error", func(t *testing.T) {
		// Defensive: server returned RFQ before CopyOutResponse / ErrorResponse.
		c := newTestReadWriteConn(buildReadyForQuery(protocol.TxnStatusIdle))
		_, _, _, err := c.InitiateCopyToStdout(context.Background(), "COPY t TO STDOUT")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ReadyForQuery before CopyOutResponse")
	})
}

// buildCopyInResponse returns a serialized CopyInResponse ('G') message.
func buildCopyInResponse(format int16, columnFormats []int16) []byte {
	body := make([]byte, 0, 3+2*len(columnFormats))
	body = append(body, byte(format))
	body = append(body, byte(len(columnFormats)>>8), byte(len(columnFormats)))
	for _, f := range columnFormats {
		body = append(body, byte(f>>8), byte(f))
	}
	var out bytes.Buffer
	writeRawMessage(&out, protocol.MsgCopyInResponse, body)
	return out.Bytes()
}

// TestInitiateCopyFromStdin mirrors TestInitiateCopyToStdout for the
// FROM STDIN direction. The PR adapted this method's return signature to
// expose pre-CopyInResponse notices; this test pins both the success and
// PG-error paths to that contract.
func TestInitiateCopyFromStdin(t *testing.T) {
	t.Run("success returns format, columns, notices", func(t *testing.T) {
		var input bytes.Buffer
		input.Write(buildNoticeResponse("NOTICE", "before-statement"))
		input.Write(buildCopyInResponse(0, []int16{0}))

		c := newTestReadWriteConn(input.Bytes())
		format, columnFormats, notices, err := c.InitiateCopyFromStdin(context.Background(), "COPY t FROM STDIN")
		require.NoError(t, err)
		assert.Equal(t, int16(0), format)
		assert.Equal(t, []int16{0}, columnFormats)
		require.Len(t, notices, 1)
		assert.Equal(t, "before-statement", notices[0].Message)
	})

	t.Run("ErrorResponse surfaces PgDiagnostic and drains RFQ", func(t *testing.T) {
		var input bytes.Buffer
		input.Write(buildErrorResponse("42703", "column \"xyz\" of relation \"t\" does not exist"))
		input.Write(buildReadyForQuery(protocol.TxnStatusIdle))

		c := newTestReadWriteConn(input.Bytes())
		_, _, _, err := c.InitiateCopyFromStdin(context.Background(), "COPY t (xyz) FROM STDIN")
		require.Error(t, err)
		var diag *mterrors.PgDiagnostic
		require.True(t, errors.As(err, &diag))
		assert.Equal(t, "42703", diag.Code)
	})
}

// TestReadCopyFailResponse_CapturesNotices verifies that NoticeResponse
// diagnostics arriving between CopyFail and ReadyForQuery are returned to
// the caller (rather than silently dropped) — symmetric with the notice
// capture in ReadCopyDoneResponse.
func TestReadCopyFailResponse_CapturesNotices(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildNoticeResponse("WARNING", "trigger ran before abort"))
	input.Write(buildErrorResponse("57014", "COPY from stdin failed: aborted"))
	input.Write(buildReadyForQuery(protocol.TxnStatusFailed))

	c := newTestReadOnlyConn(input.Bytes())

	notices, err := c.ReadCopyFailResponse(context.Background())
	require.NoError(t, err)
	require.Len(t, notices, 1)
	assert.Equal(t, "WARNING", notices[0].Severity)
	assert.Equal(t, "trigger ran before abort", notices[0].Message)
	// RFQ payload updates txnStatus on the failed-transaction abort.
	assert.Equal(t, protocol.TxnStatusFailed, c.txnStatus)
}
