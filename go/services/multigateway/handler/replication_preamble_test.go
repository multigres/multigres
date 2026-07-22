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

package handler

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	multipoolerservice "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// frameMessage builds a single pgwire message: type byte + 4-byte length
// (includes itself) + body.
func frameMessage(msgType byte, body []byte) []byte {
	raw := make([]byte, 5+len(body))
	raw[0] = msgType
	binary.BigEndian.PutUint32(raw[1:5], uint32(4+len(body)))
	copy(raw[5:], body)
	return raw
}

func writeQueryMessage(buf *bytes.Buffer, query string) {
	body := append([]byte(query), 0)
	buf.Write(frameMessage(protocol.MsgQuery, body))
}

// scriptedReplStream is a stream fake that, for each client message the
// preamble sends, hands back one scripted blob of raw backend-message bytes
// via a single Recv call. Used to drive runReplicationPreamble through
// canned command/response cycles without a real pooler.
type scriptedReplStream struct {
	ctx       context.Context
	responses [][]byte
	sendCount int
	pending   [][]byte
	sentRaw   [][]byte
}

func (s *scriptedReplStream) Send(req *multipoolerservice.StreamReplicationRequest) error {
	s.sentRaw = append(s.sentRaw, append([]byte(nil), req.GetData()...))
	if s.sendCount >= len(s.responses) {
		return errors.New("scriptedReplStream: unexpected Send beyond scripted responses")
	}
	s.pending = [][]byte{s.responses[s.sendCount]}
	s.sendCount++
	return nil
}

func (s *scriptedReplStream) Recv() (*multipoolerservice.StreamReplicationResponse, error) {
	if len(s.pending) == 0 {
		return nil, io.EOF
	}
	chunk := s.pending[0]
	s.pending = s.pending[1:]
	return &multipoolerservice.StreamReplicationResponse{
		Msg: &multipoolerservice.StreamReplicationResponse_Data{Data: chunk},
	}, nil
}

func (s *scriptedReplStream) Context() context.Context     { return s.ctx }
func (s *scriptedReplStream) Header() (metadata.MD, error) { return nil, nil }
func (s *scriptedReplStream) Trailer() metadata.MD         { return nil }
func (s *scriptedReplStream) CloseSend() error             { return nil }
func (s *scriptedReplStream) SendMsg(m any) error          { return nil }
func (s *scriptedReplStream) RecvMsg(m any) error          { return nil }

// TestPgMsgReaderNext covers message reassembly from opaque byte chunks:
// a message split across multiple chunks, multiple messages arriving in a
// single chunk, and EOF interrupting a partial message.
func TestPgMsgReaderNext(t *testing.T) {
	t.Run("message split across multiple chunks", func(t *testing.T) {
		full := frameMessage(protocol.MsgReadyForQuery, []byte{'I'})
		chunks := [][]byte{full[:2], full[2:4], full[4:]}
		idx := 0
		r := &pgMsgReader{recv: func() ([]byte, error) {
			if idx >= len(chunks) {
				return nil, io.EOF
			}
			c := chunks[idx]
			idx++
			return c, nil
		}}
		msgType, raw, err := r.next()
		require.NoError(t, err)
		assert.Equal(t, byte(protocol.MsgReadyForQuery), msgType)
		assert.Equal(t, full, raw)
	})

	t.Run("multiple messages in one chunk", func(t *testing.T) {
		m1 := frameMessage(protocol.MsgCommandComplete, []byte("TAG\x00"))
		m2 := frameMessage(protocol.MsgReadyForQuery, []byte{'I'})
		combined := append(append([]byte(nil), m1...), m2...)
		called := false
		r := &pgMsgReader{recv: func() ([]byte, error) {
			if called {
				return nil, io.EOF
			}
			called = true
			return combined, nil
		}}

		msgType1, raw1, err := r.next()
		require.NoError(t, err)
		assert.Equal(t, byte(protocol.MsgCommandComplete), msgType1)
		assert.Equal(t, m1, raw1)

		msgType2, raw2, err := r.next()
		require.NoError(t, err)
		assert.Equal(t, byte(protocol.MsgReadyForQuery), msgType2)
		assert.Equal(t, m2, raw2)
	})

	t.Run("EOF mid-message", func(t *testing.T) {
		full := frameMessage(protocol.MsgReadyForQuery, []byte{'I'})
		partial := full[:3]
		calls := 0
		r := &pgMsgReader{recv: func() ([]byte, error) {
			calls++
			if calls == 1 {
				return partial, nil
			}
			return nil, io.EOF
		}}
		_, _, err := r.next()
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("length field below the 4-byte minimum is rejected", func(t *testing.T) {
		// A well-formed length field always includes itself, so the minimum
		// valid value is 4 (type byte + zero-length body). A corrupt/malformed
		// frame claiming a smaller length must be rejected rather than
		// desynchronizing the reader by slicing off fewer bytes than the
		// 5-byte header already buffered.
		malformed := []byte{protocol.MsgReadyForQuery, 0x00, 0x00, 0x00, 0x02}
		called := false
		r := &pgMsgReader{recv: func() ([]byte, error) {
			if called {
				return nil, io.EOF
			}
			called = true
			return malformed, nil
		}}
		_, _, err := r.next()
		assert.ErrorContains(t, err, "invalid backend message length")
	})
}

// TestNonTemporaryCreateReplicationSlotError pins the tokenizer's accept/
// reject decisions: TEMPORARY present anywhere before PHYSICAL/LOGICAL is
// accepted, absent is rejected, non-CREATE_REPLICATION_SLOT commands always
// pass through, and TEMPORARY appearing only after the PHYSICAL/LOGICAL
// boundary does not count.
func TestNonTemporaryCreateReplicationSlotError(t *testing.T) {
	tests := []struct {
		name    string
		cmd     string
		wantErr bool
	}{
		{"temporary logical accepted", "CREATE_REPLICATION_SLOT s1 TEMPORARY LOGICAL pgoutput", false},
		{"temporary physical accepted", "CREATE_REPLICATION_SLOT s1 TEMPORARY PHYSICAL", false},
		{"non-temporary logical rejected", "CREATE_REPLICATION_SLOT s1 LOGICAL pgoutput", true},
		{"non-temporary physical rejected", "CREATE_REPLICATION_SLOT s1 PHYSICAL", true},
		{"case-insensitive command name", "create_replication_slot s1 temporary logical pgoutput", false},
		{"case-insensitive temporary keyword", "CREATE_REPLICATION_SLOT s1 TEMPORARY logical pgoutput", false},
		{"temporary after physical/logical boundary does not count", "CREATE_REPLICATION_SLOT s1 PHYSICAL TEMPORARY", true},
		{"non-CREATE_REPLICATION_SLOT command passes through", "IDENTIFY_SYSTEM", false},
		{"START_REPLICATION passes through", "START_REPLICATION SLOT s1 LOGICAL 0/0", false},
		{"empty command", "", false},
		// Regression: the slot name itself must never be mistaken for the
		// TEMPORARY keyword — a non-temporary slot named "temporary" must
		// still be rejected, and a genuinely temporary slot named "temporary"
		// must still be accepted.
		{"slot named 'temporary', non-temporary, rejected", "CREATE_REPLICATION_SLOT temporary PHYSICAL", true},
		{"slot named 'temporary', actually temporary, accepted", "CREATE_REPLICATION_SLOT temporary TEMPORARY PHYSICAL", false},
		{"slot named 'logical', non-temporary, rejected", "CREATE_REPLICATION_SLOT logical LOGICAL pgoutput", true},
		{"slot named 'logical', actually temporary, accepted", "CREATE_REPLICATION_SLOT logical TEMPORARY LOGICAL pgoutput", false},
		{"malformed: no slot name", "CREATE_REPLICATION_SLOT", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nonTemporaryCreateReplicationSlotError(tt.cmd)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "requires TEMPORARY")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestRunReplicationPreamble_HappyPath drives IDENTIFY_SYSTEM ->
// CREATE_REPLICATION_SLOT TEMPORARY -> START_REPLICATION through the
// preamble and confirms every byte is relayed verbatim in both directions
// until CopyBothResponse is observed, at which point the preamble reports
// streaming=true.
func TestRunReplicationPreamble_HappyPath(t *testing.T) {
	var clientInput bytes.Buffer
	writeQueryMessage(&clientInput, "IDENTIFY_SYSTEM")
	writeQueryMessage(&clientInput, "CREATE_REPLICATION_SLOT s1 TEMPORARY LOGICAL pgoutput")
	writeQueryMessage(&clientInput, "START_REPLICATION SLOT s1 LOGICAL 0/0")

	identifyResp := bytes.Join([][]byte{
		frameMessage(protocol.MsgRowDescription, []byte("rowdesc1")),
		frameMessage(protocol.MsgDataRow, []byte("datarow1")),
		frameMessage(protocol.MsgCommandComplete, []byte("IDENTIFY_SYSTEM\x00")),
		frameMessage(protocol.MsgReadyForQuery, []byte{'I'}),
	}, nil)
	createSlotResp := bytes.Join([][]byte{
		frameMessage(protocol.MsgRowDescription, []byte("rowdesc2")),
		frameMessage(protocol.MsgDataRow, []byte("datarow2")),
		frameMessage(protocol.MsgCommandComplete, []byte("CREATE_REPLICATION_SLOT\x00")),
		frameMessage(protocol.MsgReadyForQuery, []byte{'I'}),
	}, nil)
	startReplResp := frameMessage(protocol.MsgCopyBothResponse, []byte{0, 0, 0})

	stream := &scriptedReplStream{
		ctx:       context.Background(),
		responses: [][]byte{identifyResp, createSlotResp, startReplResp},
	}

	testConn := server.NewTestConn(&clientInput)
	streaming, leftover, err := runReplicationPreamble(context.Background(), testConn.Conn, stream)
	require.NoError(t, err)
	assert.True(t, streaming)
	assert.Empty(t, leftover)

	wantClientOutput := bytes.Join([][]byte{identifyResp, createSlotResp, startReplResp}, nil)
	assert.Equal(t, wantClientOutput, testConn.WriteBuf.Bytes())

	require.Len(t, stream.sentRaw, 3)
	assert.Equal(t, frameMessage(protocol.MsgQuery, append([]byte("IDENTIFY_SYSTEM"), 0)), stream.sentRaw[0])
	assert.Equal(t, frameMessage(protocol.MsgQuery, append([]byte("CREATE_REPLICATION_SLOT s1 TEMPORARY LOGICAL pgoutput"), 0)), stream.sentRaw[1])
	assert.Equal(t, frameMessage(protocol.MsgQuery, append([]byte("START_REPLICATION SLOT s1 LOGICAL 0/0"), 0)), stream.sentRaw[2])
}

// TestRunReplicationPreamble_ReturnsLeftoverAfterCopyBothResponse verifies
// that bytes following the CopyBothResponse frame in the same backend chunk
// (e.g. the start of XLogData streaming) are handed back as leftover rather
// than relayed during the preamble or dropped.
func TestRunReplicationPreamble_ReturnsLeftoverAfterCopyBothResponse(t *testing.T) {
	var clientInput bytes.Buffer
	writeQueryMessage(&clientInput, "START_REPLICATION SLOT s1 LOGICAL 0/0")

	copyBoth := frameMessage(protocol.MsgCopyBothResponse, []byte{0, 0, 0})
	extra := []byte("extra-xlogdata-bytes")
	startReplResp := append(append([]byte(nil), copyBoth...), extra...)

	stream := &scriptedReplStream{
		ctx:       context.Background(),
		responses: [][]byte{startReplResp},
	}

	testConn := server.NewTestConn(&clientInput)
	streaming, leftover, err := runReplicationPreamble(context.Background(), testConn.Conn, stream)
	require.NoError(t, err)
	assert.True(t, streaming)
	assert.Equal(t, extra, leftover)

	// Only the CopyBothResponse frame itself was relayed to the client during
	// the preamble; the leftover bytes are handed to the caller instead.
	assert.Equal(t, copyBoth, testConn.WriteBuf.Bytes())
}

// TestRunReplicationPreamble_RejectsNonTemporarySlot verifies that a
// CREATE_REPLICATION_SLOT without TEMPORARY is rejected before the pooler
// ever sees it: the client gets an ErrorResponse, and stream.Send is never
// called for that command.
func TestRunReplicationPreamble_RejectsNonTemporarySlot(t *testing.T) {
	var clientInput bytes.Buffer
	writeQueryMessage(&clientInput, "CREATE_REPLICATION_SLOT s1 LOGICAL pgoutput")

	stream := &scriptedReplStream{ctx: context.Background()}
	testConn := server.NewTestConn(&clientInput)

	streaming, leftover, err := runReplicationPreamble(context.Background(), testConn.Conn, stream)
	require.Error(t, err)
	assert.False(t, streaming)
	assert.Nil(t, leftover)
	assert.Contains(t, err.Error(), "requires TEMPORARY")

	assert.Empty(t, stream.sentRaw, "the pooler must never see the rejected command")

	require.NotEmpty(t, testConn.WriteBuf.Bytes())
	assert.Equal(t, byte(protocol.MsgErrorResponse), testConn.WriteBuf.Bytes()[0])
	assert.Contains(t, testConn.WriteBuf.String(), "requires TEMPORARY")
}

// TestRunReplicationPreamble_CleanEOF verifies a client that disconnects
// before sending anything ends the preamble cleanly (no error, not
// streaming).
func TestRunReplicationPreamble_CleanEOF(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{})
	stream := &scriptedReplStream{ctx: context.Background()}

	streaming, leftover, err := runReplicationPreamble(context.Background(), testConn.Conn, stream)
	require.NoError(t, err)
	assert.False(t, streaming)
	assert.Nil(t, leftover)
}

// TestRunReplicationPreamble_ClientTerminate verifies a Terminate message
// ends the preamble cleanly without ever reaching the pooler.
func TestRunReplicationPreamble_ClientTerminate(t *testing.T) {
	var clientInput bytes.Buffer
	clientInput.Write(frameMessage(protocol.MsgTerminate, nil))

	stream := &scriptedReplStream{ctx: context.Background()}
	testConn := server.NewTestConn(&clientInput)

	streaming, leftover, err := runReplicationPreamble(context.Background(), testConn.Conn, stream)
	require.NoError(t, err)
	assert.False(t, streaming)
	assert.Nil(t, leftover)
	assert.Empty(t, stream.sentRaw)
}

// TestRunReplicationPreamble_RejectsExtendedProtocol verifies that any
// non-Query frontend message (the extended query protocol: Parse, Bind,
// Describe, Execute, Close, Flush, Sync) is rejected outright before
// streaming begins, rather than forwarded to the pooler. A survey of the
// most widely used open-source software that consumes PostgreSQL logical
// replication found none that use the extended query protocol on this
// connection; supporting it here would mean mirroring postgres's own
// multi-message-per-command, ignore-till-sync state machine for a case
// nothing in practice exercises.
func TestRunReplicationPreamble_RejectsExtendedProtocol(t *testing.T) {
	tests := []struct {
		name    string
		msgType byte
	}{
		{"Parse", protocol.MsgParse},
		{"Bind", protocol.MsgBind},
		{"Describe", protocol.MsgDescribe},
		{"Execute", protocol.MsgExecute},
		{"Close", protocol.MsgClose},
		{"Flush", protocol.MsgFlush},
		{"Sync", protocol.MsgSync},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var clientInput bytes.Buffer
			clientInput.Write(frameMessage(tt.msgType, []byte("body")))

			stream := &scriptedReplStream{ctx: context.Background()}
			testConn := server.NewTestConn(&clientInput)

			streaming, leftover, err := runReplicationPreamble(context.Background(), testConn.Conn, stream)
			require.Error(t, err)
			assert.False(t, streaming)
			assert.Nil(t, leftover)
			assert.Contains(t, err.Error(), "not supported")

			assert.Empty(t, stream.sentRaw, "the pooler must never see a rejected message")
			require.NotEmpty(t, testConn.WriteBuf.Bytes())
			assert.Equal(t, byte(protocol.MsgErrorResponse), testConn.WriteBuf.Bytes()[0])
		})
	}
}
