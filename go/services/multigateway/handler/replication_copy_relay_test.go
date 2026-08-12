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
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// chunkQueue is a recv-shaped fake: each call pops the next scripted chunk,
// returning io.EOF once exhausted (unless overridden per-test).
type chunkQueue struct {
	chunks [][]byte
	idx    int
	atEnd  error // returned once chunks is exhausted; defaults to io.EOF
}

func (q *chunkQueue) recv() ([]byte, error) {
	if q.idx >= len(q.chunks) {
		if q.atEnd != nil {
			return nil, q.atEnd
		}
		return nil, io.EOF
	}
	c := q.chunks[q.idx]
	q.idx++
	return c, nil
}

func TestCopyModeDownstreamReader_RelaysCopyDataIncrementally(t *testing.T) {
	// A CopyData frame whose body arrives split across three chunks: the
	// reader must return each chunk as soon as it's available, never
	// buffering the whole body first.
	body := []byte("0123456789") // 10 bytes
	header := frameMessage(protocol.MsgCopyData, body)[:5]
	q := &chunkQueue{chunks: [][]byte{
		header,
		body[:4],
		body[4:7],
		body[7:],
	}}
	r := &copyModeDownstreamReader{recv: q.recv}

	got1, err := r.next()
	require.NoError(t, err)
	assert.Equal(t, header, got1, "header returned on its own, before any body bytes arrive")

	got2, err := r.next()
	require.NoError(t, err)
	assert.Equal(t, body[:4], got2)

	got3, err := r.next()
	require.NoError(t, err)
	assert.Equal(t, body[4:7], got3)

	got4, err := r.next()
	require.NoError(t, err)
	assert.Equal(t, body[7:], got4)
}

func TestCopyModeDownstreamReader_RelaysCopyDataAcrossMultipleFrames(t *testing.T) {
	frame1 := frameMessage(protocol.MsgCopyData, []byte("frame-one"))
	frame2 := frameMessage(protocol.MsgCopyData, []byte("frame-two"))
	q := &chunkQueue{chunks: [][]byte{append(append([]byte(nil), frame1...), frame2...)}}
	r := &copyModeDownstreamReader{recv: q.recv}

	got1, err := r.next()
	require.NoError(t, err)
	assert.Equal(t, frame1, got1, "first frame's header+full body returned in one call since both were already buffered")

	got2, err := r.next()
	require.NoError(t, err)
	assert.Equal(t, frame2, got2)
}

func TestCopyModeDownstreamReader_PassesThroughSmallControlMessages(t *testing.T) {
	commandComplete := frameMessage(protocol.MsgCommandComplete, []byte("COPY 0\x00"))
	q := &chunkQueue{chunks: [][]byte{commandComplete}}
	r := &copyModeDownstreamReader{recv: q.recv}

	got, err := r.next()
	require.NoError(t, err)
	assert.Equal(t, commandComplete, got)
}

func TestCopyModeDownstreamReader_ReadyForQuerySignalsSessionResumed(t *testing.T) {
	readyForQuery := frameMessage(protocol.MsgReadyForQuery, []byte{'I'})
	q := &chunkQueue{chunks: [][]byte{readyForQuery}}
	r := &copyModeDownstreamReader{recv: q.recv}

	got, err := r.next()
	assert.Equal(t, readyForQuery, got, "ReadyForQuery bytes must still be forwarded to the client")
	require.ErrorIs(t, err, errReplicationSessionResumed)
}

func TestCopyModeDownstreamReader_HeaderSplitAcrossChunks(t *testing.T) {
	readyForQuery := frameMessage(protocol.MsgReadyForQuery, []byte{'I'})
	q := &chunkQueue{chunks: [][]byte{readyForQuery[:2], readyForQuery[2:4], readyForQuery[4:]}}
	r := &copyModeDownstreamReader{recv: q.recv}

	got, err := r.next()
	assert.Equal(t, readyForQuery, got)
	require.ErrorIs(t, err, errReplicationSessionResumed)
}

func TestCopyModeDownstreamReader_BytesDeliveredBeforeUnderlyingEOF(t *testing.T) {
	// Regression: a recv() call can return (n>0 bytes, io.EOF) in the same
	// call (matches io.Reader's documented contract for the underlying
	// net.Conn/gRPC stream read). Those bytes must still be used/returned,
	// not discarded because an error also came back.
	readyForQuery := frameMessage(protocol.MsgReadyForQuery, []byte{'I'})
	callCount := 0
	recv := func() ([]byte, error) {
		callCount++
		if callCount == 1 {
			return readyForQuery, io.EOF // whole message plus EOF in one call
		}
		return nil, io.EOF
	}
	r := &copyModeDownstreamReader{recv: recv}

	got, err := r.next()
	assert.Equal(t, readyForQuery, got, "the message bytes must be used even though EOF arrived alongside them")
	require.ErrorIs(t, err, errReplicationSessionResumed)
}

func TestCopyModeDownstreamReader_PassthroughBytesDeliveredBeforeUnderlyingEOF(t *testing.T) {
	// Same "bytes must be used even though an error arrived alongside them"
	// contract as TestCopyModeDownstreamReader_BytesDeliveredBeforeUnderlyingEOF,
	// but exercised mid-CopyData-body via readPassthrough rather than at a
	// message boundary via ensureBuffered: readPassthrough is the path that
	// handles large CopyData bodies, the whole reason this reader exists, so
	// it needs its own coverage of a recv() call returning (n>0 bytes, io.EOF)
	// together while more body is still expected.
	body := []byte("0123456789") // 10 bytes; only 6 arrive before EOF
	header := frameMessage(protocol.MsgCopyData, body)[:5]
	callCount := 0
	recv := func() ([]byte, error) {
		callCount++
		switch callCount {
		case 1:
			return header, nil
		case 2:
			return body[:6], io.EOF // partial body plus EOF in one call
		default:
			return nil, io.EOF
		}
	}
	r := &copyModeDownstreamReader{recv: recv}

	got1, err := r.next()
	require.NoError(t, err)
	assert.Equal(t, header, got1, "header returned on its own, before any body bytes arrive")

	got2, err := r.next()
	require.NoError(t, err, "the partial body bytes must be used even though EOF arrived alongside them")
	assert.Equal(t, body[:6], got2)

	_, err = r.next()
	assert.ErrorIs(t, err, io.EOF, "the EOF resurfaces once recv() is asked again with nothing left to deliver")
}

func TestCopyModeDownstreamReader_MalformedLengthRejected(t *testing.T) {
	malformed := []byte{protocol.MsgReadyForQuery, 0x00, 0x00, 0x00, 0x02}
	q := &chunkQueue{chunks: [][]byte{malformed}}
	r := &copyModeDownstreamReader{recv: q.recv}

	_, err := r.next()
	assert.ErrorContains(t, err, "invalid backend message length")
}

func TestCopyModeDownstreamReader_UnderlyingRecvErrorPropagates(t *testing.T) {
	wantErr := errors.New("stream broke")
	q := &chunkQueue{atEnd: wantErr}
	r := &copyModeDownstreamReader{recv: q.recv}

	_, err := r.next()
	assert.ErrorIs(t, err, wantErr)
}

// fixedReader serves bytes from a queue of chunks via io.Reader, one chunk
// per Read() call regardless of len(p) (so tests can force multi-call
// reassembly), returning io.EOF once exhausted.
type fixedReader struct {
	chunks [][]byte
	idx    int
}

func (f *fixedReader) Read(p []byte) (int, error) {
	if f.idx >= len(f.chunks) {
		return 0, io.EOF
	}
	c := f.chunks[f.idx]
	f.idx++
	n := copy(p, c)
	if n < len(c) {
		panic("fixedReader test fixture: p too small for chunk")
	}
	return n, nil
}

func TestCopyModeUpstreamReader_RelaysCopyDataUnchanged(t *testing.T) {
	frame := frameMessage(protocol.MsgCopyData, []byte("standby-status-update"))
	r := newCopyModeUpstreamReader(&fixedReader{chunks: [][]byte{frame}})

	got := make([]byte, len(frame))
	n, err := io.ReadFull(r, got)
	require.NoError(t, err)
	assert.Equal(t, frame, got[:n])
}

func TestCopyModeUpstreamReader_CopyDoneForwardedThenTerminates(t *testing.T) {
	copyDone := frameMessage(protocol.MsgCopyDone, nil)
	r := newCopyModeUpstreamReader(&fixedReader{chunks: [][]byte{copyDone}})

	got := make([]byte, len(copyDone))
	n, err := io.ReadFull(r, got)
	require.NoError(t, err, "CopyDone bytes themselves must be delivered")
	assert.Equal(t, copyDone, got[:n])

	_, err = r.Read(make([]byte, 16))
	require.ErrorIs(t, err, errReplicationSessionEndedByClient)
}

func TestCopyModeUpstreamReader_PipelinedMessageAfterCopyDoneNeverDelivered(t *testing.T) {
	copyDone := frameMessage(protocol.MsgCopyDone, nil)
	bogus := frameMessage(protocol.MsgQuery, append([]byte("CREATE_REPLICATION_SLOT s LOGICAL pgoutput"), 0))
	// Both arrive in a single underlying chunk, simulating a client that
	// pipelines a follow-up command immediately after CopyDone without
	// waiting for any reply.
	combined := append(append([]byte(nil), copyDone...), bogus...)
	r := newCopyModeUpstreamReader(&fixedReader{chunks: [][]byte{combined}})

	got := make([]byte, len(copyDone))
	n, err := io.ReadFull(r, got)
	require.NoError(t, err)
	assert.Equal(t, copyDone, got[:n])

	n2, err := r.Read(make([]byte, 4096))
	assert.Equal(t, 0, n2, "the pipelined bogus message must never be delivered")
	require.ErrorIs(t, err, errReplicationSessionEndedByClient)
}

func TestCopyModeUpstreamReader_CopyFailTerminates(t *testing.T) {
	copyFail := frameMessage(protocol.MsgCopyFail, append([]byte("client gave up"), 0))
	r := newCopyModeUpstreamReader(&fixedReader{chunks: [][]byte{copyFail}})

	got := make([]byte, len(copyFail))
	n, err := io.ReadFull(r, got)
	require.NoError(t, err)
	assert.Equal(t, copyFail, got[:n])

	_, err = r.Read(make([]byte, 16))
	require.ErrorIs(t, err, errReplicationSessionEndedByClient)
}

func TestCopyModeUpstreamReader_MessageSplitAcrossSmallReadBuffer(t *testing.T) {
	frame := frameMessage(protocol.MsgCopyData, []byte("a somewhat longer standby status payload"))
	r := newCopyModeUpstreamReader(&fixedReader{chunks: [][]byte{frame}})

	var got []byte
	small := make([]byte, 4) // force multiple Read() calls to drain one message
	for len(got) < len(frame) {
		n, err := r.Read(small)
		got = append(got, small[:n]...)
		require.NoError(t, err)
	}
	assert.Equal(t, frame, got)
}

func TestCopyModeUpstreamReader_HeaderSplitAcrossUnderlyingReads(t *testing.T) {
	copyDone := frameMessage(protocol.MsgCopyDone, nil) // 5 bytes, header only
	r := newCopyModeUpstreamReader(&fixedReader{chunks: [][]byte{copyDone[:2], copyDone[2:4], copyDone[4:]}})

	got := make([]byte, len(copyDone))
	n, err := io.ReadFull(r, got)
	require.NoError(t, err)
	assert.Equal(t, copyDone, got[:n])

	_, err = r.Read(make([]byte, 16))
	require.ErrorIs(t, err, errReplicationSessionEndedByClient)
}

func TestCopyModeUpstreamReader_MalformedLengthRejected(t *testing.T) {
	malformed := []byte{protocol.MsgQuery, 0x00, 0x00, 0x00, 0x02}
	r := newCopyModeUpstreamReader(&fixedReader{chunks: [][]byte{malformed}})

	_, err := r.Read(make([]byte, 16))
	assert.ErrorContains(t, err, "invalid frontend message length")
}

// erroringReader is an io.Reader fake that always returns a fixed, non-EOF
// error, mirroring chunkQueue's atEnd used by
// TestCopyModeDownstreamReader_UnderlyingRecvErrorPropagates.
type erroringReader struct {
	err error
}

func (r *erroringReader) Read(p []byte) (int, error) {
	return 0, r.err
}

func TestCopyModeUpstreamReader_UnderlyingReadErrorPropagates(t *testing.T) {
	wantErr := errors.New("stream broke")
	r := newCopyModeUpstreamReader(&erroringReader{err: wantErr})

	_, err := r.Read(make([]byte, 16))
	assert.ErrorIs(t, err, wantErr)
}
