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

package grpcpoolerservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
	"github.com/multigres/multigres/go/services/multipooler/internal/replication"
)

// fakeReplStream is a test double for MultiPoolerService_StreamReplicationServer.
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

func (f *fakeReplStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeReplStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeReplStream) SetTrailer(metadata.MD)       {}
func (f *fakeReplStream) SendMsg(any) error            { return nil }
func (f *fakeReplStream) RecvMsg(any) error            { return nil }

func (f *fakeReplStream) Context() context.Context {
	if f.ctx == nil {
		return context.Background()
	}
	return f.ctx
}

func (f *fakeReplStream) Send(resp *multipoolerpb.StreamReplicationResponse) error {
	select {
	case f.sendCh <- resp:
		return nil
	case <-f.Context().Done():
		return f.Context().Err()
	}
}

func (f *fakeReplStream) Recv() (*multipoolerpb.StreamReplicationRequest, error) {
	select {
	case r := <-f.recvCh:
		return r.req, r.err
	case <-f.Context().Done():
		return nil, f.Context().Err()
	}
}

func initReq(mode multipoolerpb.ReplicationMode) *multipoolerpb.StreamReplicationRequest {
	return &multipoolerpb.StreamReplicationRequest{
		Msg: &multipoolerpb.StreamReplicationRequest_Init{
			Init: &multipoolerpb.StreamReplicationInit{Mode: mode, User: "u"},
		},
	}
}

func dataReq(b string) *multipoolerpb.StreamReplicationRequest {
	return &multipoolerpb.StreamReplicationRequest{
		Msg: &multipoolerpb.StreamReplicationRequest_Data{Data: []byte(b)},
	}
}

// TestStreamReplication_RejectsUnimplementedModes verifies the handler refuses
// any mode other than REPLICATION_MODE_DATABASE before touching the pooler.
func TestStreamReplication_RejectsUnimplementedModes(t *testing.T) {
	for _, mode := range []multipoolerpb.ReplicationMode{
		multipoolerpb.ReplicationMode_REPLICATION_MODE_UNSPECIFIED,
		multipoolerpb.ReplicationMode_REPLICATION_MODE_TRUE,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			s := &poolerService{} // nil pooler: mode check must reject before use
			f := newFakeReplStream(t.Context())
			f.recvCh <- replRecv{req: initReq(mode)}

			err := s.StreamReplication(f)
			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.InvalidArgument, st.Code())
			assert.Contains(t, st.Message(), "unsupported replication mode")
		})
	}
}

// TestStreamReplication_RequiresInitFirst verifies a non-init first message and
// a receive failure are both rejected as InvalidArgument.
func TestStreamReplication_RequiresInitFirst(t *testing.T) {
	t.Run("data before init", func(t *testing.T) {
		s := &poolerService{}
		f := newFakeReplStream(t.Context())
		f.recvCh <- replRecv{req: dataReq("oops")}

		err := s.StreamReplication(f)
		st, _ := status.FromError(err)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "must be init")
	})

	t.Run("recv error", func(t *testing.T) {
		s := &poolerService{}
		f := newFakeReplStream(t.Context())
		f.recvCh <- replRecv{err: errors.New("boom")}

		err := s.StreamReplication(f)
		st, _ := status.FromError(err)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "failed to receive init")
	})
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
	s := &poolerService{} // nil replMetrics -> nil *Stream -> no-op recorders

	done := make(chan error, 1)
	go func() { done <- s.runReplicationTunnel(ctx, f, backendA, nil) }()

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

// fakeReplPoolManager embeds the PoolManager interface (left nil) and overrides
// only NewLogicalReplicationConn — the single method the handler calls before
// handing off to the tunnel. Any other call would panic, which is the intended
// guard: these tests must not exercise the rest of the pool.
type fakeReplPoolManager struct {
	connpoolmanager.PoolManager
	conn *reserved.Conn
	err  error
}

func (f *fakeReplPoolManager) NewLogicalReplicationConn(context.Context, string, []byte, []byte) (*reserved.Conn, error) {
	return f.conn, f.err
}

// servingPooler builds a real QueryPoolerServer wired to pm and transitions it
// to SERVING so StartRequest admits the replication stream.
func servingPooler(t *testing.T, pm connpoolmanager.PoolManager) *poolerserver.QueryPoolerServer {
	t.Helper()
	p := poolerserver.NewQueryPoolerServer(slog.Default(), pm, nil, "", "", nil, 0, false)
	require.NoError(t, p.OnStateChange(t.Context(),
		clustermetadatapb.PoolerType_PRIMARY,
		clustermetadatapb.PoolerServingStatus_SERVING))
	return p
}

// TestStreamReplication_StartRequestRejectedWhenNotServing verifies the handler
// rejects the stream at admission (before opening any backend) when the pooler
// is not serving.
func TestStreamReplication_StartRequestRejectedWhenNotServing(t *testing.T) {
	// Default state is NOT_SERVING.
	p := poolerserver.NewQueryPoolerServer(slog.Default(), nil, nil, "", "", nil, 0, false)
	s := &poolerService{pooler: p}
	f := newFakeReplStream(t.Context())
	f.recvCh <- replRecv{req: initReq(multipoolerpb.ReplicationMode_REPLICATION_MODE_DATABASE)}

	require.Error(t, s.StreamReplication(f))
}

// TestStreamReplication_NilPoolManagerUnavailable verifies that a serving pooler
// with no pool manager reports Unavailable rather than panicking.
func TestStreamReplication_NilPoolManagerUnavailable(t *testing.T) {
	s := &poolerService{pooler: servingPooler(t, nil)}
	f := newFakeReplStream(t.Context())
	f.recvCh <- replRecv{req: initReq(multipoolerpb.ReplicationMode_REPLICATION_MODE_DATABASE)}

	err := s.StreamReplication(f)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unavailable, st.Code())
}

// TestStreamReplication_BackendOpenErrorSendsStructuredError verifies that when
// opening the replication backend fails, the handler sends a structured
// infrastructure error on the stream and returns an error.
func TestStreamReplication_BackendOpenErrorSendsStructuredError(t *testing.T) {
	pm := &fakeReplPoolManager{err: errors.New("backend auth failed")}
	s := &poolerService{pooler: servingPooler(t, pm)}
	f := newFakeReplStream(t.Context())
	f.recvCh <- replRecv{req: initReq(multipoolerpb.ReplicationMode_REPLICATION_MODE_DATABASE)}

	require.Error(t, s.StreamReplication(f))

	select {
	case resp := <-f.sendCh:
		require.NotNil(t, resp.GetError(), "expected a structured error response, got %v", resp.GetMsg())
	case <-time.After(2 * time.Second):
		t.Fatal("no structured error response was sent")
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
	s := &poolerService{} // nil metrics -> no-op recorders
	backend := &errReadBackend{rErr: errors.New("backend exploded")}

	require.Error(t, s.runReplicationTunnel(t.Context(), f, backend, nil))

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
