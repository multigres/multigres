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

package poolerserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/replication"
)

// fakeReplStream stands in for the gateway's side of a replication stream:
// the test feeds recvCh and reads what the tunnel sends from sendCh.
type fakeReplStream struct {
	ctx    context.Context
	recvCh chan replRecv
	sendCh chan *multipoolerpb.StreamReplicationResponse
}

type replRecv struct {
	req *multipoolerpb.StreamReplicationRequest
	err error
}

func newFakeReplStream(ctx context.Context) *fakeReplStream {
	return &fakeReplStream{
		ctx:    ctx,
		recvCh: make(chan replRecv, 8),
		sendCh: make(chan *multipoolerpb.StreamReplicationResponse, 32),
	}
}

func (f *fakeReplStream) Send(resp *multipoolerpb.StreamReplicationResponse) error {
	select {
	case f.sendCh <- resp:
		return nil
	case <-f.ctx.Done():
		return f.ctx.Err()
	}
}

func (f *fakeReplStream) Recv() (*multipoolerpb.StreamReplicationRequest, error) {
	select {
	case r := <-f.recvCh:
		return r.req, r.err
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

func dataReq(b string) *multipoolerpb.StreamReplicationRequest {
	return &multipoolerpb.StreamReplicationRequest{
		Msg: &multipoolerpb.StreamReplicationRequest_Data{Data: []byte(b)},
	}
}

// TestRunReplicationTunnel_DataBothWays exercises the transport seam with an
// in-memory backend: ready is sent, bytes flow both directions, and a client
// disconnect (ctx cancel) tears the backend down.
func TestRunReplicationTunnel_DataBothWays(t *testing.T) {
	backendA, backendB := net.Pipe() // backendB stands in for postgres
	t.Cleanup(func() { _ = backendA.Close(); _ = backendB.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f := newFakeReplStream(ctx)

	done := make(chan error, 1)
	go func() { done <- runReplicationTunnel(ctx, f.Send, f.Recv, backendA, nil) }()

	// First response must be ready.
	select {
	case resp := <-f.sendCh:
		require.NotNil(t, resp.GetReady(), "first response should be ready, got %v", resp.GetMsg())
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ready")
	}

	// client -> backend
	f.recvCh <- replRecv{req: dataReq("ping")}
	got := make([]byte, 4)
	_, err := io.ReadFull(backendB, got)
	require.NoError(t, err)
	require.Equal(t, "ping", string(got))

	// backend -> client
	_, err = backendB.Write([]byte("pong"))
	require.NoError(t, err)
	select {
	case resp := <-f.sendCh:
		require.Equal(t, "pong", string(resp.GetData()))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server->client data")
	}

	// Client disconnect tears down the tunnel and closes the backend.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runReplicationTunnel did not return after disconnect")
	}
	if _, err := backendB.Read(make([]byte, 1)); err == nil {
		t.Fatal("backend should be closed after client disconnect")
	}
}

// errReadBackend fails the downstream (backend -> client) copy on its first Read.
type errReadBackend struct{ rErr error }

func (b *errReadBackend) Read([]byte) (int, error)    { return 0, b.rErr }
func (b *errReadBackend) Write(p []byte) (int, error) { return len(p), nil }
func (b *errReadBackend) Close() error                { return nil }

// TestRunReplicationTunnel_BackendErrorSendsStructuredError covers the infra-error
// branch: a non-EOF backend error after the tunnel is live yields a structured
// error response (after ready) and a non-nil return.
func TestRunReplicationTunnel_BackendErrorSendsStructuredError(t *testing.T) {
	f := newFakeReplStream(t.Context())
	backend := &errReadBackend{rErr: errors.New("backend exploded")}

	require.Error(t, runReplicationTunnel(t.Context(), f.Send, f.Recv, backend, nil))

	var sawReady, sawErr bool
	for !sawErr {
		select {
		case resp := <-f.sendCh:
			switch {
			case resp.GetReady() != nil:
				sawReady = true
			case resp.GetError() != nil:
				sawErr = true
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for structured error response")
		}
	}
	assert.True(t, sawReady, "ready must be sent before the error")
	assert.True(t, sawErr)
}

// TestTerminationReason covers the full classification table.
func TestTerminationReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"clean exit", nil, replication.TerminationClientDisconnect},
		{"context canceled", context.Canceled, replication.TerminationClientDisconnect},
		{"deadline exceeded", context.DeadlineExceeded, replication.TerminationClientDisconnect},
		{"wrapped cancel", fmt.Errorf("teardown: %w", context.Canceled), replication.TerminationClientDisconnect},
		{"infra error", errors.New("boom"), replication.TerminationBackendError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, terminationReason(tt.err))
		})
	}
}

// concurrentSendStream is a fake stream that detects concurrent Send calls
// (which gRPC forbids). A "data" send blocks until released, so the test can
// hold the tunnel's downstream send in-flight while the handler attempts its
// post-Run error send.
type concurrentSendStream struct {
	ctx           context.Context
	recvCh        chan replRecv
	inSend        atomic.Int32
	sawConcurrent atomic.Bool
	dataEntered   chan struct{} // closed once a data send is in-flight
	releaseData   chan struct{} // test closes to release the blocked data send
	dataOnce      sync.Once
}

func (f *concurrentSendStream) Recv() (*multipoolerpb.StreamReplicationRequest, error) {
	select {
	case r := <-f.recvCh:
		return r.req, r.err
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

func (f *concurrentSendStream) Send(resp *multipoolerpb.StreamReplicationResponse) error {
	if f.inSend.Add(1) > 1 {
		f.sawConcurrent.Store(true)
	}
	defer f.inSend.Add(-1)
	if resp.GetData() != nil {
		f.dataOnce.Do(func() { close(f.dataEntered) })
		<-f.releaseData // hold the downstream send in-flight
	}
	return nil
}

// teardownBackend yields one chunk downstream, then fails the upstream write
// (only after the downstream send is in-flight) to force Run to return while the
// downstream send goroutine is still active.
type teardownBackend struct {
	readOnce    sync.Once
	closeOnce   sync.Once
	closed      chan struct{}
	dataEntered chan struct{}
}

func (b *teardownBackend) Read(p []byte) (int, error) {
	first := false
	b.readOnce.Do(func() { first = true })
	if first {
		return copy(p, []byte("wal")), nil
	}
	<-b.closed
	return 0, io.EOF
}

func (b *teardownBackend) Write([]byte) (int, error) {
	<-b.dataEntered // ensure the downstream send is in-flight first
	return 0, errors.New("backend write failed")
}

func (b *teardownBackend) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

// TestRunReplicationTunnel_NoConcurrentSendOnTeardown verifies the handler never
// issues a Send concurrently with the tunnel's downstream send goroutine. On an
// upstream (backend write) failure, Run returns while the downstream goroutine is
// still blocked in Send; the post-Run error send must not race it.
func TestRunReplicationTunnel_NoConcurrentSendOnTeardown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dataEntered := make(chan struct{})
	stream := &concurrentSendStream{
		ctx:         ctx,
		recvCh:      make(chan replRecv, 1),
		dataEntered: dataEntered,
		releaseData: make(chan struct{}),
	}
	backend := &teardownBackend{closed: make(chan struct{}), dataEntered: dataEntered}
	stream.recvCh <- replRecv{req: dataReq("upstream")} // drives the failing Write

	done := make(chan error, 1)
	go func() { done <- runReplicationTunnel(ctx, stream.Send, stream.Recv, backend, nil) }()

	// Once the downstream send is in-flight, the upstream write fails and Run
	// returns; the handler then attempts its error send. Give that interleaving
	// time to happen, then release the held send.
	<-dataEntered
	time.Sleep(100 * time.Millisecond)
	close(stream.releaseData)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runReplicationTunnel did not return")
	}
	assert.False(t, stream.sawConcurrent.Load(),
		"handler issued a Send concurrently with the tunnel's downstream send goroutine")
}
