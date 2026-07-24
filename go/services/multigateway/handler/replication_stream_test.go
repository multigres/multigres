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
		// Echo the client bytes back as a server Data response.
		s.queue = append(s.queue, append([]byte(nil), data...))
		s.cond.Broadcast()
	}
	return nil
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

	fakeExec := &fakeReplExecutor{}
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

	// Client -> gateway -> (fake) pooler -> echoed back -> client.
	want := []byte("hello replication world")
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

	fakeExec := &fakeReplExecutor{}
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

	want := []byte("post-detach replication bytes")
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

// TestHandleReplicationStream_SurfacesDetachError verifies that a DetachConn
// failure (the connection is already closed for protocol use) is surfaced
// directly, after the pooler stream was already opened.
func TestHandleReplicationStream_SurfacesDetachError(t *testing.T) {
	_, serverEnd := net.Pipe()

	fakeExec := &fakeReplExecutor{}
	h := NewMultigatewayHandler(fakeExec, slog.Default(), 0)

	conn := server.NewReplicationTestConn(
		server.WithTestReplicationMode(server.ReplicationLogical),
		server.WithTestUser("repl_user"),
		server.WithTestNetConn(serverEnd),
	)

	// Detach once up front so the handler's own DetachConn call fails.
	raw, _, err := conn.Conn.DetachConn()
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })

	err = h.HandleReplicationStream(context.Background(), conn.Conn)
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
