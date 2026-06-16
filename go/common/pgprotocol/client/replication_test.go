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
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// newTestConnFromNetConn builds a *Conn whose reader/writer/SetReadDeadline
// target a real net.Conn. Used for tests that need a read to genuinely block
// (the in-memory mockNetConn's SetReadDeadline is a no-op and cannot unblock).
func newTestConnFromNetConn(nc net.Conn) *Conn {
	return &Conn{
		conn:           nc,
		bufferedReader: bufio.NewReader(nc),
		bufferedWriter: bufio.NewWriter(nc),
		serverParams:   make(map[string]string),
	}
}

// buildCopyBothResponse builds a 'W' message: Int8 format + Int16 N + Int16[N].
func buildCopyBothResponse(format byte, colFormats []int16) []byte {
	var body bytes.Buffer
	body.WriteByte(format)
	n := make([]byte, 2)
	binary.BigEndian.PutUint16(n, uint16(len(colFormats)))
	body.Write(n)
	for _, f := range colFormats {
		fb := make([]byte, 2)
		binary.BigEndian.PutUint16(fb, uint16(f))
		body.Write(fb)
	}
	var out bytes.Buffer
	writeRawMessage(&out, protocol.MsgCopyBothResponse, body.Bytes())
	return out.Bytes()
}

// buildNoticeBody builds the diagnostic-field body for a NoticeResponse (same
// field layout as ErrorResponse): severity NOTICE + SQLSTATE + message. Wrap
// it with writeRawMessage(.., MsgNoticeResponse, ..).
func buildNoticeBody(sqlstate, message string) []byte {
	var body bytes.Buffer
	body.WriteByte('S') // severity
	body.WriteString("NOTICE")
	body.WriteByte(0)
	body.WriteByte('C') // SQLSTATE
	body.WriteString(sqlstate)
	body.WriteByte(0)
	body.WriteByte('M') // human-readable message
	body.WriteString(message)
	body.WriteByte(0)
	body.WriteByte(0) // end of fields
	return body.Bytes()
}

// buildParameterStatus builds an 'S' message with a key/value pair.
func buildParameterStatus(key, val string) []byte {
	var body bytes.Buffer
	body.WriteString(key)
	body.WriteByte(0)
	body.WriteString(val)
	body.WriteByte(0)
	var out bytes.Buffer
	writeRawMessage(&out, protocol.MsgParameterStatus, body.Bytes())
	return out.Bytes()
}

func TestReplicationStartReplication_EntersCopyBoth(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildParameterStatus("in_hot_standby", "off")) // interleaved, must be consumed
	input.Write(buildCopyBothResponse(0, nil))

	c := newTestReadOnlyConn(input.Bytes())
	notices, err := c.StartReplication(t.Context(),
		"START_REPLICATION SLOT s LOGICAL 0/0 (proto_version '2', publication_names 'p')")
	require.NoError(t, err)
	assert.Empty(t, notices)
	assert.Equal(t, replStreamStreaming, c.replState)
	// ParameterStatus must be recorded, not dropped.
	assert.Equal(t, "off", c.serverParams["in_hot_standby"])
}

func TestReplicationStartReplication_SurfacesNotices(t *testing.T) {
	var input bytes.Buffer
	writeRawMessage(&input, protocol.MsgNoticeResponse, buildNoticeBody("00000", "logical decoding will begin using saved snapshot"))
	input.Write(buildCopyBothResponse(0, nil))

	c := newTestReadOnlyConn(input.Bytes())
	notices, err := c.StartReplication(t.Context(), "START_REPLICATION SLOT s LOGICAL 0/0")
	require.NoError(t, err)
	require.Len(t, notices, 1, "handshake notice must be surfaced, not dropped")
	assert.Contains(t, notices[0].Message, "saved snapshot")
	assert.Equal(t, replStreamStreaming, c.replState)
}

func TestReplicationStartReplication_ErrorResponseDrains(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildErrorResponse("42704", `replication slot "s" does not exist`))
	input.Write(buildReadyForQuery(protocol.TxnStatusIdle))

	c := newTestReadOnlyConn(input.Bytes())
	_, err := c.StartReplication(t.Context(), "START_REPLICATION SLOT s LOGICAL 0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
	assert.Equal(t, replStreamIdle, c.replState, "failed start must leave conn idle/reusable")
}

// buildReplCopyData wraps a payload in a 'd' CopyData envelope.
func buildReplCopyData(payload []byte) []byte {
	var out bytes.Buffer
	writeRawMessage(&out, protocol.MsgCopyData, payload)
	return out.Bytes()
}

func TestReplicationRead_ReturnsPayloadVerbatim(t *testing.T) {
	// 'w' XLogData payload: leading 'w' byte must survive untouched.
	payload := append([]byte{'w'}, []byte("rawwalbytes")...)
	c := newTestReadOnlyConn(buildReplCopyData(payload))
	c.replState = replStreamStreaming

	got, err := c.ReadReplicationMessage(t.Context())
	require.NoError(t, err)
	assert.Equal(t, payload, got.Data, "payload returned verbatim, including sub-type byte")
	assert.Nil(t, got.Notice)
}

func TestReplicationRead_SurfacesMidStreamNotice(t *testing.T) {
	// A NoticeResponse interleaved in the stream must be surfaced, not dropped,
	// and must NOT terminate the stream.
	var input bytes.Buffer
	writeRawMessage(&input, protocol.MsgNoticeResponse, buildNoticeBody("01000", "wal segment recycled soon"))
	input.Write(buildReplCopyData(append([]byte{'w'}, []byte("more wal")...)))
	c := newTestReadOnlyConn(input.Bytes())
	c.replState = replStreamStreaming

	msg, err := c.ReadReplicationMessage(t.Context())
	require.NoError(t, err)
	require.NotNil(t, msg.Notice, "mid-stream notice must be surfaced")
	assert.Contains(t, msg.Notice.Message, "recycled")
	assert.Nil(t, msg.Data)
	assert.Equal(t, replStreamStreaming, c.replState, "a notice must not kill the stream")

	// The stream is still live: the next read returns the CopyData payload.
	next, err := c.ReadReplicationMessage(t.Context())
	require.NoError(t, err)
	assert.Equal(t, append([]byte{'w'}, []byte("more wal")...), next.Data)
}

func TestReplicationRead_ParameterStatusRecordedAndContinues(t *testing.T) {
	// ParameterStatus mid-stream is recorded (observable, not dropped) and the
	// read continues to the next data frame rather than terminating.
	var input bytes.Buffer
	input.Write(buildParameterStatus("application_name", "walreceiver"))
	input.Write(buildReplCopyData(append([]byte{'k'}, make([]byte, 17)...)))
	c := newTestReadOnlyConn(input.Bytes())
	c.replState = replStreamStreaming

	msg, err := c.ReadReplicationMessage(t.Context())
	require.NoError(t, err)
	require.NotNil(t, msg.Data)
	assert.Equal(t, byte('k'), msg.Data[0])
	assert.Equal(t, "walreceiver", c.serverParams["application_name"], "ParameterStatus must be recorded")
	assert.Equal(t, replStreamStreaming, c.replState)
}

func TestReplicationRead_MalformedParameterStatusErrors(t *testing.T) {
	// A ParameterStatus body with no null terminator can't be parsed; rather
	// than swallow it, the stream surfaces the corruption and goes dead.
	var input bytes.Buffer
	writeRawMessage(&input, protocol.MsgParameterStatus, []byte("no_null_terminator"))
	c := newTestReadOnlyConn(input.Bytes())
	c.replState = replStreamStreaming

	_, err := c.ReadReplicationMessage(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "malformed ParameterStatus")
	assert.Equal(t, replStreamDead, c.replState)
}

func TestReplicationRead_ServerCopyDoneIsEOF(t *testing.T) {
	var input bytes.Buffer
	writeRawMessage(&input, protocol.MsgCopyDone, nil)
	c := newTestReadOnlyConn(input.Bytes())
	c.replState = replStreamStreaming

	_, err := c.ReadReplicationMessage(t.Context())
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, replStreamDead, c.replState)
}

func TestReplicationRead_ErrorResponseMarksDeadAndDrains(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildErrorResponse("XX000", "decoding terminated"))
	input.Write(buildReadyForQuery(protocol.TxnStatusIdle))
	c := newTestReadOnlyConn(input.Bytes())
	c.replState = replStreamStreaming

	_, err := c.ReadReplicationMessage(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decoding terminated")
	assert.Equal(t, replStreamDead, c.replState, "error terminates the stream both directions")
}

func TestReplicationRead_ContextCancelUnblocks(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { _ = serverSide.Close(); _ = clientSide.Close() })

	c := newTestConnFromNetConn(clientSide)
	c.replState = replStreamStreaming

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := c.ReadReplicationMessage(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), 2*time.Second, "must unblock promptly on cancel, not hang")
}

func TestReplicationWrite_AfterDeadStreamErrors(t *testing.T) {
	c := newTestReadOnlyConn(nil)
	c.replState = replStreamDead

	err := c.WriteReplicationData(append([]byte{'r'}, make([]byte, 33)...))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not streaming")
}

func TestReplicationWrite_IdleStreamErrors(t *testing.T) {
	c := newTestReadOnlyConn(nil)
	// Zero value: replStreamIdle — never entered copy-both.
	err := c.WriteReplicationData(append([]byte{'r'}, make([]byte, 33)...))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not streaming")
}

func TestReplicationWrite_StreamingSucceeds(t *testing.T) {
	// newTestReadOnlyConn's writer discards; we only assert no error here.
	c := newTestReadOnlyConn(nil)
	c.replState = replStreamStreaming
	err := c.WriteReplicationData(append([]byte{'r'}, make([]byte, 33)...))
	require.NoError(t, err)
}

func TestReplicationFinish_DrainsInFlightThenCompletes(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildReplCopyData(append([]byte{'w'}, []byte("late wal")...))) // in-flight server data
	writeRawMessage(&input, protocol.MsgCopyDone, nil)                         // server CopyDone
	input.Write(buildCommandComplete("START_REPLICATION"))
	input.Write(buildReadyForQuery(protocol.TxnStatusIdle))

	c := newTestReadOnlyConn(input.Bytes())
	c.replState = replStreamStreaming

	tag, notices, err := c.FinishReplication(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "START_REPLICATION", tag)
	assert.Empty(t, notices)
	assert.Equal(t, replStreamDead, c.replState)
}

func TestReplicationFinish_SurfacesNotices(t *testing.T) {
	var input bytes.Buffer
	writeRawMessage(&input, protocol.MsgCopyDone, nil)
	writeRawMessage(&input, protocol.MsgNoticeResponse, buildNoticeBody("00000", "logical decoding stopped"))
	input.Write(buildCommandComplete("START_REPLICATION"))
	input.Write(buildReadyForQuery(protocol.TxnStatusIdle))

	c := newTestReadOnlyConn(input.Bytes())
	c.replState = replStreamStreaming

	_, notices, err := c.FinishReplication(t.Context())
	require.NoError(t, err)
	require.Len(t, notices, 1, "shutdown notice must be surfaced, not dropped")
	assert.Contains(t, notices[0].Message, "decoding stopped")
}

func TestReplicationFinish_TwoCommandCompletes(t *testing.T) {
	// Physical-format START_REPLICATION can emit two CommandCompletes; loop to 'Z'.
	var input bytes.Buffer
	writeRawMessage(&input, protocol.MsgCopyDone, nil)
	input.Write(buildCommandComplete("START_REPLICATION"))
	input.Write(buildCommandComplete("START_REPLICATION"))
	input.Write(buildReadyForQuery(protocol.TxnStatusIdle))

	c := newTestReadOnlyConn(input.Bytes())
	c.replState = replStreamStreaming
	_, _, err := c.FinishReplication(t.Context())
	require.NoError(t, err)
}

func TestReplicationStartReplication_MaxSlotsExhausted(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildErrorResponse("53400", "all replication slots are in use"))
	input.Write(buildReadyForQuery(protocol.TxnStatusIdle))
	c := newTestReadOnlyConn(input.Bytes())
	_, err := c.StartReplication(t.Context(), "START_REPLICATION SLOT s LOGICAL 0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replication slots")
}

func TestReplicationRead_GarbageMessageTypeErrorsCleanly(t *testing.T) {
	var input bytes.Buffer
	writeRawMessage(&input, 'X', []byte("nonsense")) // unexpected type
	c := newTestReadOnlyConn(input.Bytes())
	c.replState = replStreamStreaming
	_, err := c.ReadReplicationMessage(t.Context())
	require.Error(t, err)
	assert.Equal(t, replStreamDead, c.replState)
}

func TestReplicationStartReplication_WALRemoved(t *testing.T) {
	// Deterministic stand-in for the e2e "recycled LSN" case: surface the error.
	var input bytes.Buffer
	input.Write(buildErrorResponse("58P01", `requested WAL segment has already been removed`))
	input.Write(buildReadyForQuery(protocol.TxnStatusIdle))
	c := newTestReadOnlyConn(input.Bytes())
	_, err := c.StartReplication(t.Context(), "START_REPLICATION SLOT s LOGICAL 0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already been removed")
}

// --- coverage: StartReplication error/edge paths ---

func TestReplicationStartReplication_ReadErrorWrapped(t *testing.T) {
	// Empty input: the first read hits EOF. With a non-cancelable ctx the error
	// is wrapped (not a ctx error).
	c := newTestReadOnlyConn(nil)
	_, err := c.StartReplication(context.Background(), "START_REPLICATION SLOT s LOGICAL 0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read START_REPLICATION response")
}

func TestReplicationStartReplication_MalformedParameterStatus(t *testing.T) {
	var input bytes.Buffer
	writeRawMessage(&input, protocol.MsgParameterStatus, []byte("no_null_terminator"))
	c := newTestReadOnlyConn(input.Bytes())
	_, err := c.StartReplication(t.Context(), "START_REPLICATION SLOT s LOGICAL 0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "malformed ParameterStatus during START_REPLICATION")
}

func TestReplicationStartReplication_UnexpectedMessage(t *testing.T) {
	// A CopyData frame before CopyBothResponse is unexpected during the handshake.
	c := newTestReadOnlyConn(buildReplCopyData([]byte("x")))
	_, err := c.StartReplication(t.Context(), "START_REPLICATION SLOT s LOGICAL 0/0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected message during START_REPLICATION")
}

// --- coverage: ReadReplicationMessage read error + deadline arming ---

func TestReplicationRead_ReadErrorWrapped(t *testing.T) {
	c := newTestReadOnlyConn(nil) // EOF on first read
	c.replState = replStreamStreaming
	_, err := c.ReadReplicationMessage(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read replication message")
}

func TestReplicationRead_DeadlineContextArmsAndReads(t *testing.T) {
	// A ctx with a deadline exercises makeReadsCancelable's SetReadDeadline path
	// (and stop's deadline clear) while still returning the frame.
	c := newTestReadOnlyConn(buildReplCopyData(append([]byte{'k'}, make([]byte, 17)...)))
	c.replState = replStreamStreaming
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	msg, err := c.ReadReplicationMessage(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg.Data)
	assert.Equal(t, byte('k'), msg.Data[0])
}

// --- coverage: FinishReplication error/edge paths ---

func TestReplicationFinish_ReadErrorWrapped(t *testing.T) {
	c := newTestReadOnlyConn(nil) // EOF mid-drain
	c.replState = replStreamStreaming
	_, _, err := c.FinishReplication(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "drain replication shutdown")
}

func TestReplicationFinish_ErrorResponseDuringDrain(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildErrorResponse("XX000", "shutdown failed"))
	input.Write(buildReadyForQuery(protocol.TxnStatusIdle))
	c := newTestReadOnlyConn(input.Bytes())
	c.replState = replStreamStreaming
	_, _, err := c.FinishReplication(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shutdown failed")
	assert.Equal(t, replStreamDead, c.replState)
}

func TestReplicationFinish_UnexpectedMessage(t *testing.T) {
	var input bytes.Buffer
	writeRawMessage(&input, 'X', []byte("junk"))
	c := newTestReadOnlyConn(input.Bytes())
	c.replState = replStreamStreaming
	_, _, err := c.FinishReplication(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected message during replication shutdown")
}
