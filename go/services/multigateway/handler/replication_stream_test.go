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
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/multigres/multigres/go/common/callerid"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	multipoolerservice "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// fakeReplStream is an in-memory stand-in for
// multipoolerservice.MultipoolerService_StreamReplicationClient. Bytes sent by
// the gateway (client->pooler) are echoed straight back as Data responses
// (pooler->client), so a tunnel test can assert a clean byte round trip.
type fakeReplStream struct {
	ctx    context.Context
	mu     sync.Mutex
	cond   *sync.Cond
	queue  [][]byte
	closed bool

	// preambleResponses, if set, are consumed in Send-call order: the Nth
	// Send() (for N < len(preambleResponses)) enqueues preambleResponses[N]
	// as the response instead of echoing the sent bytes. Once exhausted,
	// Send falls back to the normal echo behavior. Models the real
	// command/scripted-response handshake (e.g. START_REPLICATION ->
	// CopyBothResponse) that the preamble drives before handing off to the
	// byte-blind tunnel, which these tests exercise via plain echo.
	preambleResponses [][]byte
	sendCount         int

	// sent records every chunk this stream's Send() has been given, in call
	// order — i.e. everything the gateway has forwarded to the (fake)
	// pooler, preamble responses aside. Used by tests to assert what did or
	// did not reach the pooler.
	sent [][]byte
}

func newFakeReplStream(ctx context.Context) *fakeReplStream {
	s := &fakeReplStream{ctx: ctx}
	s.cond = sync.NewCond(&s.mu)
	// Wake any blocked Recv when the stream context is cancelled, mirroring
	// real gRPC stream teardown.
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	}()
	return s
}

func (s *fakeReplStream) Send(req *multipoolerservice.StreamReplicationRequest) error {
	// Mirror real gRPC: a cancelled stream context aborts Send.
	if err := s.ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return io.EOF
	}
	if data := req.GetData(); data != nil {
		s.sent = append(s.sent, append([]byte(nil), data...))
	}
	if s.sendCount < len(s.preambleResponses) {
		s.queue = append(s.queue, append([]byte(nil), s.preambleResponses[s.sendCount]...))
		s.sendCount++
		s.cond.Broadcast()
		return nil
	}
	s.sendCount++
	if data := req.GetData(); data != nil {
		// Echo the client bytes back as a server Data response.
		s.queue = append(s.queue, append([]byte(nil), data...))
		s.cond.Broadcast()
	}
	return nil
}

// sentDataChunks returns every chunk recorded by Send so far, in call order.
// Safe to call concurrently with an in-flight Send.
func (s *fakeReplStream) sentDataChunks() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.sent))
	copy(out, s.sent)
	return out
}

// queueDownstream pushes raw bytes directly onto the stream's response
// queue, bypassing the echo-on-Send behavior, so a test can script an exact
// downstream (pooler -> client) byte sequence for Recv to hand back.
func (s *fakeReplStream) queueDownstream(raw []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = append(s.queue, append([]byte(nil), raw...))
	s.cond.Broadcast()
}

func (s *fakeReplStream) Recv() (*multipoolerservice.StreamReplicationResponse, error) {
	// Mirror real gRPC: cancelling the stream context unblocks Recv with the
	// context error (newFakeReplStream wakes the cond on ctx.Done()).
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.queue) == 0 && !s.closed && s.ctx.Err() == nil {
		s.cond.Wait()
	}
	if err := s.ctx.Err(); err != nil && len(s.queue) == 0 {
		return nil, err
	}
	if len(s.queue) > 0 {
		chunk := s.queue[0]
		s.queue = s.queue[1:]
		return &multipoolerservice.StreamReplicationResponse{
			Msg: &multipoolerservice.StreamReplicationResponse_Data{Data: chunk},
		}, nil
	}
	return nil, io.EOF
}

// close unblocks a pending Recv with EOF.
func (s *fakeReplStream) close() {
	s.mu.Lock()
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *fakeReplStream) Context() context.Context     { return s.ctx }
func (s *fakeReplStream) Header() (metadata.MD, error) { return nil, nil }
func (s *fakeReplStream) Trailer() metadata.MD         { return nil }
func (s *fakeReplStream) CloseSend() error             { s.close(); return nil }
func (s *fakeReplStream) SendMsg(m any) error          { return nil }
func (s *fakeReplStream) RecvMsg(m any) error          { return nil }

// fakeReplExecutor records the init it receives and hands back a fake stream.
type fakeReplExecutor struct {
	mockExecutor
	stream  *fakeReplStream
	initErr error
	gotInit *multipoolerservice.StreamReplicationInit
	gotCtx  context.Context

	// streamOverride, when set, is returned as-is instead of stream/newFakeReplStream.
	// Used to hand back a stream type other than *fakeReplStream (e.g. one that
	// returns a canned error response).
	streamOverride multipoolerservice.MultipoolerService_StreamReplicationClient

	// preambleResponses, when set, is applied to the lazily-created
	// fakeReplStream so its scripted preamble handshake (see
	// fakeReplStream.preambleResponses) is wired up with the real ctx
	// StreamReplication receives.
	preambleResponses [][]byte
}

func (e *fakeReplExecutor) StreamReplication(
	ctx context.Context,
	conn *server.Conn,
	state *MultigatewayConnectionState,
	init *multipoolerservice.StreamReplicationInit,
) (multipoolerservice.MultipoolerService_StreamReplicationClient, error) {
	e.gotCtx = ctx
	e.gotInit = init
	if e.initErr != nil {
		return nil, e.initErr
	}
	if e.streamOverride != nil {
		return e.streamOverride, nil
	}
	if e.stream == nil {
		e.stream = newFakeReplStream(ctx)
		e.stream.preambleResponses = e.preambleResponses
	}
	return e.stream, nil
}

// errorReplStream returns a single canned response from Recv (typically an
// error response), then blocks until its context is cancelled. Used to drive
// the tunnel's pooler-error mapping without the full echo behavior of
// fakeReplStream.
type errorReplStream struct {
	ctx  context.Context
	resp *multipoolerservice.StreamReplicationResponse
	sent bool
}

func (s *errorReplStream) Send(*multipoolerservice.StreamReplicationRequest) error { return nil }

func (s *errorReplStream) Recv() (*multipoolerservice.StreamReplicationResponse, error) {
	if !s.sent {
		s.sent = true
		return s.resp, nil
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *errorReplStream) Context() context.Context     { return s.ctx }
func (s *errorReplStream) Header() (metadata.MD, error) { return nil, nil }
func (s *errorReplStream) Trailer() metadata.MD         { return nil }
func (s *errorReplStream) CloseSend() error             { return nil }
func (s *errorReplStream) SendMsg(m any) error          { return nil }
func (s *errorReplStream) RecvMsg(m any) error          { return nil }

func TestHandleReplicationStream_RoundTripsAndExitsOnClientEOF(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close() })

	copyBothResp := frameMessage(protocol.MsgCopyBothResponse, []byte{0, 0, 0})
	fakeExec := &fakeReplExecutor{preambleResponses: [][]byte{copyBothResp}}
	h := NewMultigatewayHandler(fakeExec, slog.Default(), 0)

	conn := server.NewReplicationTestConn(
		server.WithTestReplicationMode(server.ReplicationLogical),
		server.WithTestUser("repl_user"),
		server.WithTestScramKeys([]byte("client-key"), []byte("server-key")),
		server.WithTestNetConn(serverEnd),
	)

	done := make(chan error, 1)
	go func() {
		done <- h.HandleReplicationStream(context.Background(), conn.Conn)
	}()

	// Drive the preamble first: a single START_REPLICATION whose scripted
	// response is CopyBothResponse, handing off to the byte-blind tunnel.
	startReplCmd := frameMessage(protocol.MsgQuery, append([]byte("START_REPLICATION SLOT s1 LOGICAL 0/0"), 0))
	go func() { _, _ = clientEnd.Write(startReplCmd) }()

	gotCopyBoth := make([]byte, len(copyBothResp))
	_ = clientEnd.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := io.ReadFull(clientEnd, gotCopyBoth)
	require.NoError(t, err)
	assert.Equal(t, copyBothResp, gotCopyBoth)

	// Client -> gateway -> (fake) pooler -> echoed back -> client. Framed as
	// CopyData since the relay is frame-aware, not byte-blind, post-handoff.
	want := frameMessage(protocol.MsgCopyData, []byte("hello replication world"))
	go func() {
		_, _ = clientEnd.Write(want)
	}()

	got := make([]byte, len(want))
	_ = clientEnd.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := io.ReadFull(clientEnd, got)
	require.NoError(t, err)
	assert.Equal(t, want, got[:n])

	// Client half-closes: the tunnel must exit cleanly (nil error).
	require.NoError(t, clientEnd.Close())

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("HandleReplicationStream did not exit after client half-close")
	}

	// The init must carry the connection's identity, built before DetachConn
	// wiped the SCRAM keys.
	require.NotNil(t, fakeExec.gotInit)
	assert.Equal(t, "repl_user", fakeExec.gotInit.GetUser())
	assert.Equal(t, multipoolerservice.ReplicationMode_REPLICATION_MODE_DATABASE, fakeExec.gotInit.GetMode())
	require.NotNil(t, fakeExec.gotInit.GetUserAuth())
	assert.Equal(t, []byte("client-key"), fakeExec.gotInit.GetUserAuth().GetClientKey())
	assert.Equal(t, []byte("server-key"), fakeExec.gotInit.GetUserAuth().GetServerKey())
}

// TestHandleReplicationStream_SurvivesCtxCancelDuringDetach is a regression
// test for the stream-context lifetime bug: DetachConn cancels the connection
// context (the ctx serve() hands to HandleReplicationStream), so the pooler
// stream must be opened on a context that survives that cancellation. Here the
// fake stream honors its context (like real gRPC), and the incoming ctx is
// cancelled immediately after the handler starts — the tunnel must still round
// trip and exit cleanly, proving the stream is not bound to the cancelled ctx.
func TestHandleReplicationStream_SurvivesCtxCancelDuringDetach(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close() })

	copyBothResp := frameMessage(protocol.MsgCopyBothResponse, []byte{0, 0, 0})
	fakeExec := &fakeReplExecutor{preambleResponses: [][]byte{copyBothResp}}
	h := NewMultigatewayHandler(fakeExec, slog.Default(), 0)

	conn := server.NewReplicationTestConn(
		server.WithTestReplicationMode(server.ReplicationLogical),
		server.WithTestUser("repl_user"),
		server.WithTestNetConn(serverEnd),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- h.HandleReplicationStream(ctx, conn.Conn)
	}()

	// Simulate DetachConn cancelling the connection context.
	cancel()

	// Drive the preamble first: a single START_REPLICATION whose scripted
	// response is CopyBothResponse, handing off to the byte-blind tunnel.
	startReplCmd := frameMessage(protocol.MsgQuery, append([]byte("START_REPLICATION SLOT s1 LOGICAL 0/0"), 0))
	go func() { _, _ = clientEnd.Write(startReplCmd) }()

	gotCopyBoth := make([]byte, len(copyBothResp))
	_ = clientEnd.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := io.ReadFull(clientEnd, gotCopyBoth)
	require.NoError(t, err)
	assert.Equal(t, copyBothResp, gotCopyBoth)

	// Framed as CopyData since the relay is frame-aware, not byte-blind,
	// post-handoff.
	want := frameMessage(protocol.MsgCopyData, []byte("post-detach replication bytes"))
	go func() { _, _ = clientEnd.Write(want) }()

	got := make([]byte, len(want))
	_ = clientEnd.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := io.ReadFull(clientEnd, got)
	require.NoError(t, err)
	assert.Equal(t, want, got[:n])

	require.NoError(t, clientEnd.Close())
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("HandleReplicationStream did not exit after client half-close")
	}
}

func TestHandleReplicationStream_SurfacesOpenError(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close() })

	// Drain whatever the handler writes to the client (the ErrorResponse) so
	// the unbuffered pipe write does not block.
	go func() { _, _ = io.Copy(io.Discard, clientEnd) }()

	fakeExec := &fakeReplExecutor{initErr: io.ErrUnexpectedEOF}
	h := NewMultigatewayHandler(fakeExec, slog.Default(), 0)

	conn := server.NewReplicationTestConn(
		server.WithTestReplicationMode(server.ReplicationLogical),
		server.WithTestUser("repl_user"),
		server.WithTestNetConn(serverEnd),
	)

	err := h.HandleReplicationStream(context.Background(), conn.Conn)
	require.Error(t, err)
}

// TestHandleReplicationStream_OpenErrorWriteAlsoFails verifies that the
// original backend-open error is still returned even when the best-effort
// WriteError itself fails. The peer is closed before any write, and the error
// message is made bigger than the connection's write buffer so the write
// bypasses buffering and hits the broken pipe immediately (rather than
// succeeding into the buffer and only failing later on Flush).
func TestHandleReplicationStream_OpenErrorWriteAlsoFails(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	require.NoError(t, clientEnd.Close())

	openErr := errors.New(strings.Repeat("x", 20000))
	fakeExec := &fakeReplExecutor{initErr: openErr}
	h := NewMultigatewayHandler(fakeExec, slog.Default(), 0)

	conn := server.NewReplicationTestConn(
		server.WithTestReplicationMode(server.ReplicationLogical),
		server.WithTestUser("repl_user"),
		server.WithTestNetConn(serverEnd),
	)

	err := h.HandleReplicationStream(context.Background(), conn.Conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "xxxx",
		"the original open error must still be returned even when WriteError fails")
}

// TestHandleReplicationStream_SurfacesConnReadError verifies that a
// connection already unusable when the preamble tries to read from it (e.g.
// torn down by some other path) surfaces as an error from
// HandleReplicationStream, after the pooler stream was already opened.
//
// The preamble now runs before DetachConn, so it is the first thing to touch
// the connection — a preemptive conn.DetachConn() (the previous way this
// test constructed "already closed") returns bufferedReader to the pool,
// and reading from a nil bufferedReader panics rather than erroring. Closing
// the raw socket directly reaches the same "connection is unusable" case
// without that panic.
func TestHandleReplicationStream_SurfacesConnReadError(t *testing.T) {
	_, serverEnd := net.Pipe()
	require.NoError(t, serverEnd.Close())

	fakeExec := &fakeReplExecutor{}
	h := NewMultigatewayHandler(fakeExec, slog.Default(), 0)

	conn := server.NewReplicationTestConn(
		server.WithTestReplicationMode(server.ReplicationLogical),
		server.WithTestUser("repl_user"),
		server.WithTestNetConn(serverEnd),
	)

	err := h.HandleReplicationStream(context.Background(), conn.Conn)
	require.Error(t, err)
}

// TestHandleReplicationStream_PoolerErrorWithDiagnostic verifies that a
// structured pooler error carrying a PgDiagnostic ends the tunnel with that
// diagnostic recoverable from the returned error.
func TestHandleReplicationStream_PoolerErrorWithDiagnostic(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := &errorReplStream{
		ctx: ctx,
		resp: &multipoolerservice.StreamReplicationResponse{
			Msg: &multipoolerservice.StreamReplicationResponse_Error{
				Error: &multipoolerservice.StreamReplicationError{
					Diagnostic: &query.PgDiagnostic{Code: "57P01", Message: "terminating connection"},
				},
			},
		},
	}
	fakeExec := &fakeReplExecutor{streamOverride: stream}
	h := NewMultigatewayHandler(fakeExec, slog.Default(), 0)

	conn := server.NewReplicationTestConn(
		server.WithTestReplicationMode(server.ReplicationLogical),
		server.WithTestUser("repl_user"),
		server.WithTestNetConn(serverEnd),
	)

	// The preamble now runs before the tunnel, so it must see a client
	// command before errorReplStream's scripted error response is reached.
	go func() {
		_, _ = clientEnd.Write(frameMessage(protocol.MsgQuery, append([]byte("START_REPLICATION SLOT s1 LOGICAL 0/0"), 0)))
	}()

	err := h.HandleReplicationStream(context.Background(), conn.Conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "terminating connection")
}

// TestHandleReplicationStream_PoolerErrorWithoutDiagnostic verifies that a
// structured pooler error with no attached diagnostic still ends the tunnel
// with a clean internal error rather than panicking.
func TestHandleReplicationStream_PoolerErrorWithoutDiagnostic(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := &errorReplStream{
		ctx: ctx,
		resp: &multipoolerservice.StreamReplicationResponse{
			Msg: &multipoolerservice.StreamReplicationResponse_Error{
				Error: &multipoolerservice.StreamReplicationError{},
			},
		},
	}
	fakeExec := &fakeReplExecutor{streamOverride: stream}
	h := NewMultigatewayHandler(fakeExec, slog.Default(), 0)

	conn := server.NewReplicationTestConn(
		server.WithTestReplicationMode(server.ReplicationLogical),
		server.WithTestUser("repl_user"),
		server.WithTestNetConn(serverEnd),
	)

	// The preamble now runs before the tunnel, so it must see a client
	// command before errorReplStream's scripted error response is reached.
	go func() {
		_, _ = clientEnd.Write(frameMessage(protocol.MsgQuery, append([]byte("START_REPLICATION SLOT s1 LOGICAL 0/0"), 0)))
	}()

	err := h.HandleReplicationStream(context.Background(), conn.Conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "without a diagnostic")
}

// TestHandleReplicationStream_PropagatesCallerIdentity verifies the context
// passed to the executor carries the connection's caller identity, matching
// every other handler entry point (HandleQuery, HandleExecute, ...).
func TestHandleReplicationStream_PropagatesCallerIdentity(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close() })

	fakeExec := &fakeReplExecutor{}
	h := NewMultigatewayHandler(fakeExec, slog.Default(), 0)

	conn := server.NewReplicationTestConn(
		server.WithTestReplicationMode(server.ReplicationLogical),
		server.WithTestUser("repl_user"),
		server.WithTestNetConn(serverEnd),
	)

	done := make(chan error, 1)
	go func() {
		done <- h.HandleReplicationStream(context.Background(), conn.Conn)
	}()

	require.NoError(t, clientEnd.Close())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("HandleReplicationStream did not exit after client close")
	}

	require.NotNil(t, fakeExec.gotCtx)
	cid := callerid.FromContext(fakeExec.gotCtx)
	require.NotNil(t, cid, "caller identity must be attached to the context passed to the executor")
	assert.Equal(t, "repl_user", cid.GetPrincipal())
}

// TestHandleReplicationStream_TerminatesOnClientCopyDone verifies that once
// the client sends CopyDone, HandleReplicationStream stops relaying —
// including a pipelined follow-up message sent immediately after, without
// waiting for any reply — and returns a non-nil error rather than allowing
// the connection to continue.
func TestHandleReplicationStream_TerminatesOnClientCopyDone(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close() })

	copyBothResp := frameMessage(protocol.MsgCopyBothResponse, []byte{0, 0, 0})
	fakeExec := &fakeReplExecutor{preambleResponses: [][]byte{copyBothResp}}
	h := NewMultigatewayHandler(fakeExec, slog.Default(), 0)

	conn := server.NewReplicationTestConn(
		server.WithTestReplicationMode(server.ReplicationLogical),
		server.WithTestUser("repl_user"),
		server.WithTestNetConn(serverEnd),
	)

	done := make(chan error, 1)
	go func() {
		done <- h.HandleReplicationStream(context.Background(), conn.Conn)
	}()

	startReplCmd := frameMessage(protocol.MsgQuery, append([]byte("START_REPLICATION SLOT s1 LOGICAL 0/0"), 0))
	go func() { _, _ = clientEnd.Write(startReplCmd) }()

	gotCopyBoth := make([]byte, len(copyBothResp))
	_ = clientEnd.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := io.ReadFull(clientEnd, gotCopyBoth)
	require.NoError(t, err)

	// CopyDone immediately followed (pipelined, no wait for a reply) by a
	// bogus command — must never reach the pooler.
	copyDone := frameMessage(protocol.MsgCopyDone, nil)
	bogus := frameMessage(protocol.MsgQuery, append([]byte("CREATE_REPLICATION_SLOT s2 LOGICAL pgoutput"), 0))
	go func() { _, _ = clientEnd.Write(append(append([]byte(nil), copyDone...), bogus...)) }()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("HandleReplicationStream did not terminate after client CopyDone + pipelined command")
	}

	assert.Equal(t, startReplCmd, fakeExec.stream.sentDataChunks()[0], "the START_REPLICATION command was relayed")
	assert.Equal(t, copyDone, fakeExec.stream.sentDataChunks()[len(fakeExec.stream.sentDataChunks())-1],
		"the last thing forwarded to the pooler must be CopyDone itself, never the pipelined bogus command")
}

// TestHandleReplicationStream_TerminatesOnServerReadyForQuery verifies that
// once postgres (via the pooler stream) sends ReadyForQuery — signalling it
// has returned to command mode after the COPY session ended —
// HandleReplicationStream relays that ReadyForQuery to the client, then
// returns a non-nil error rather than allowing the connection to continue.
func TestHandleReplicationStream_TerminatesOnServerReadyForQuery(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close() })

	xlogData := frameMessage(protocol.MsgCopyData, []byte("fake-xlogdata"))
	commandComplete := frameMessage(protocol.MsgCommandComplete, []byte("COPY 0\x00"))
	readyForQuery := frameMessage(protocol.MsgReadyForQuery, []byte{'I'})
	copyBothResp := frameMessage(protocol.MsgCopyBothResponse, []byte{0, 0, 0})

	fakeExec := &fakeReplExecutor{preambleResponses: [][]byte{copyBothResp}}
	h := NewMultigatewayHandler(fakeExec, slog.Default(), 0)

	conn := server.NewReplicationTestConn(
		server.WithTestReplicationMode(server.ReplicationLogical),
		server.WithTestUser("repl_user"),
		server.WithTestNetConn(serverEnd),
	)

	done := make(chan error, 1)
	go func() {
		done <- h.HandleReplicationStream(context.Background(), conn.Conn)
	}()

	startReplCmd := frameMessage(protocol.MsgQuery, append([]byte("START_REPLICATION SLOT s1 LOGICAL 0/0"), 0))
	go func() { _, _ = clientEnd.Write(startReplCmd) }()

	gotCopyBoth := make([]byte, len(copyBothResp))
	_ = clientEnd.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := io.ReadFull(clientEnd, gotCopyBoth)
	require.NoError(t, err)

	// Script the pooler stream's post-handoff responses: some XLogData, then
	// postgres ending the COPY session on its own.
	fakeExec.stream.queueDownstream(append(append(append([]byte(nil), xlogData...), commandComplete...), readyForQuery...))

	wantClientBytes := append(append(append([]byte(nil), xlogData...), commandComplete...), readyForQuery...)
	gotClientBytes := make([]byte, len(wantClientBytes))
	_ = clientEnd.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = io.ReadFull(clientEnd, gotClientBytes)
	require.NoError(t, err)
	assert.Equal(t, wantClientBytes, gotClientBytes, "the client must still see XLogData, CommandComplete, and ReadyForQuery")

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("HandleReplicationStream did not terminate after server ReadyForQuery")
	}
}
