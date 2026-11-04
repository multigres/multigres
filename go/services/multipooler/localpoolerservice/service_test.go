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

package localpoolerservice

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
	"github.com/multigres/multigres/go/services/multipooler/internal/pubsub"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// blockingQueryService answers ExecuteQuery once release is closed, reporting
// on started that the call is running. Other methods are not used.
type blockingQueryService struct {
	queryservice.QueryService
	started chan struct{}
	release chan struct{}
}

func (b *blockingQueryService) ExecuteQuery(context.Context, *query.Target, string, *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	close(b.started)
	<-b.release
	return &sqltypes.Result{}, nil, nil
}

func newBlockingQueryService() *blockingQueryService {
	return &blockingQueryService{started: make(chan struct{}), release: make(chan struct{})}
}

func requireNotServing(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.True(t, mterrors.IsPreExecutionUnavailable(err), "refusal must be retryable as not executed: %v", err)
}

func TestServer_RefusedBeforeRegistration(t *testing.T) {
	var l Server
	_, _, err := l.ExecuteQuery(t.Context(), &query.Target{}, "select 1", nil)
	requireNotServing(t, err)
}

func TestServer_RefusedAfterStop(t *testing.T) {
	l := Server{inner: newBlockingQueryService()}
	l.Stop() // nothing in flight: returns at once
	_, _, err := l.ExecuteQuery(t.Context(), &query.Target{}, "select 1", nil)
	requireNotServing(t, err)
}

// TestServer_StopWaitsForInFlight checks that Stop waits for a
// running call to return, as the gRPC server's graceful stop does, and that
// the running call completes normally.
func TestServer_StopWaitsForInFlight(t *testing.T) {
	inner := newBlockingQueryService()
	l := Server{inner: inner}

	callErr := make(chan error, 1)
	go func() {
		_, _, err := l.ExecuteQuery(t.Context(), &query.Target{}, "select 1", nil)
		callErr <- err
	}()
	<-inner.started

	stopped := make(chan struct{})
	go func() {
		l.Stop()
		close(stopped)
	}()

	// New calls are refused as soon as Stop begins, while the running one
	// holds Stop open.
	require.Eventually(t, func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.stopped
	}, 5*time.Second, time.Millisecond)
	_, _, err := l.ExecuteQuery(t.Context(), &query.Target{}, "select 1", nil)
	requireNotServing(t, err)
	select {
	case <-stopped:
		t.Fatal("Stop returned while a call was running")
	case <-time.After(50 * time.Millisecond):
	}

	close(inner.release)
	require.NoError(t, <-callErr)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the running call finished")
	}
}

// fakeHealthProvider subscribes with a fixed current state and sends updates
// written to updates. Like the manager, it closes a subscriber's channel when
// the subscriber's ctx ends.
type fakeHealthProvider struct {
	poolerserver.HealthProvider
	updates chan *poolerserver.HealthState
}

func (f *fakeHealthProvider) SubscribeHealth(ctx context.Context) (*poolerserver.HealthState, <-chan *poolerserver.HealthState, error) {
	out := make(chan *poolerserver.HealthState)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case state := <-f.updates:
				select {
				case out <- state:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return &poolerserver.HealthState{ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}, out, nil
}

func newRegisteredServer(t *testing.T) (*Server, *fakeHealthProvider) {
	t.Helper()
	hp := &fakeHealthProvider{updates: make(chan *poolerserver.HealthState)}
	p := poolerserver.NewQueryPoolerServer(slog.New(slog.DiscardHandler), nil, &clustermetadatapb.ID{Name: "p"}, "tg", "0", hp, 0, false)
	s := NewServer()
	s.register(p)
	return s, hp
}

// withListener gives the registered server's pooler a pubsub listener, running
// when start is set, as the manager does once the pooler is the serving leader.
func withListener(t *testing.T, s *Server, start bool) *pubsub.Listener {
	t.Helper()
	l := pubsub.NewListener(nil, slog.New(slog.DiscardHandler), nil)
	if start {
		l.Start(t.Context())
		t.Cleanup(l.Stop)
	}
	s.pooler.SetPubSubListener(l)
	return l
}

func TestServer_AuthAndHealthRefusedWhenNotServing(t *testing.T) {
	req := &multipoolerpb.GetAuthCredentialsRequest{Username: "u", Database: "d"}

	before := NewServer()
	_, err := before.GetAuthCredentials(t.Context(), req)
	requireNotServing(t, err)
	_, err = before.SubscribeHealth(t.Context())
	requireNotServing(t, err)
	_, err = before.NotificationStream(t.Context())
	requireNotServing(t, err)

	after, _ := newRegisteredServer(t)
	after.Stop()
	_, err = after.NotificationStream(t.Context())
	requireNotServing(t, err)
	_, err = after.GetAuthCredentials(t.Context(), req)
	requireNotServing(t, err)
	_, err = after.SubscribeHealth(t.Context())
	requireNotServing(t, err)
}

// TestServer_SubscribeHealth checks that a subscription yields the current
// state, then updates, as the gRPC health stream sends them.
func TestServer_SubscribeHealth(t *testing.T) {
	s, hp := newRegisteredServer(t)
	recv, err := s.SubscribeHealth(t.Context())
	require.NoError(t, err)

	msg, err := recv()
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, msg.GetServingStatus())

	go func() {
		hp.updates <- &poolerserver.HealthState{ServingStatus: clustermetadatapb.PoolerServingStatus_DRAINING, RecommendedStalenessTimeout: time.Second}
	}()
	msg, err = recv()
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DRAINING, msg.GetServingStatus())
	assert.Equal(t, time.Second, msg.GetRecommendedStalenessTimeout().AsDuration())
}

// TestServer_StopEndsHealthSubscriptions checks that Stop does not wait for a
// health subscription but ends it: its recv returns io.EOF, as a gRPC health
// stream ends at the server's graceful stop.
func TestServer_StopEndsHealthSubscriptions(t *testing.T) {
	s, _ := newRegisteredServer(t)
	recv, err := s.SubscribeHealth(t.Context())
	require.NoError(t, err)
	_, err = recv() // current state
	require.NoError(t, err)

	recvErr := make(chan error, 1)
	go func() {
		_, err := recv()
		recvErr <- err
	}()

	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop waited for a health subscription")
	}
	select {
	case err := <-recvErr:
		assert.ErrorIs(t, err, io.EOF)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not end the health subscription")
	}
}

// TestServer_HealthSubscriptionEndsWithCallerContext checks that a caller
// ending its ctx gets its own ctx error, not io.EOF.
func TestServer_HealthSubscriptionEndsWithCallerContext(t *testing.T) {
	s, _ := newRegisteredServer(t)
	ctx, cancel := context.WithCancel(t.Context())
	recv, err := s.SubscribeHealth(ctx)
	require.NoError(t, err)
	_, err = recv() // current state
	require.NoError(t, err)

	cancel()
	_, err = recv()
	assert.ErrorIs(t, err, context.Canceled)
}

// TestServer_NotificationStreamListenerNotRunning checks that a pooler that is
// not the serving leader (its listener stopped) refuses the stream as
// unavailable, as the gRPC handler does, so the gateway reconnects elsewhere.
func TestServer_NotificationStreamListenerNotRunning(t *testing.T) {
	s, _ := newRegisteredServer(t)
	withListener(t, s, false)

	stream, err := s.NotificationStream(t.Context())
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "got %v", err)
}

// TestServer_NotificationStreamListenerStopsBeforeSubscribe checks that once
// the listener stops, a subscription is refused rather than acknowledged, so
// the gateway reconnects instead of waiting on nothing. The loop starts in a
// goroutine, so it either sees the stopped listener at once (and Send finds
// the stream ended) or takes the subscribe and finds it refused; the gRPC
// handler's test covers the second order deterministically.
func TestServer_NotificationStreamListenerStopsBeforeSubscribe(t *testing.T) {
	s, _ := newRegisteredServer(t)
	l := withListener(t, s, true)

	stream, err := s.NotificationStream(t.Context())
	require.NoError(t, err)
	l.Stop()
	if err := stream.Send(&multipoolerpb.NotificationStreamRequest{SubscribeChannels: []string{"ch"}}); err != nil {
		require.ErrorIs(t, err, io.EOF)
	}
	_, err = stream.Recv()
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "got %v", err)
	assert.ErrorIs(t, stream.Send(&multipoolerpb.NotificationStreamRequest{}), io.EOF, "Send after the stream ended")
}

// TestServer_StopEndsNotificationStreams checks that Stop ends an open
// notification stream instead of waiting for it: Recv and Send report io.EOF.
func TestServer_StopEndsNotificationStreams(t *testing.T) {
	s, _ := newRegisteredServer(t)
	withListener(t, s, true)
	stream, err := s.NotificationStream(t.Context())
	require.NoError(t, err)

	recvErr := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		recvErr <- err
	}()
	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop waited for a notification stream")
	}
	select {
	case err := <-recvErr:
		assert.ErrorIs(t, err, io.EOF)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not end the notification stream")
	}
	assert.ErrorIs(t, stream.Send(&multipoolerpb.NotificationStreamRequest{}), io.EOF)
}

// TestServer_NotificationStreamEndsWithCallerContext checks that a caller
// ending its ctx gets its own ctx error.
func TestServer_NotificationStreamEndsWithCallerContext(t *testing.T) {
	s, _ := newRegisteredServer(t)
	withListener(t, s, true)
	ctx, cancel := context.WithCancel(t.Context())
	stream, err := s.NotificationStream(ctx)
	require.NoError(t, err)

	cancel()
	_, err = stream.Recv()
	assert.ErrorIs(t, err, context.Canceled)
}

// stalledPoolManager is a pool manager whose connections never come: the
// listener's first subscribe blocks until the listener stops, as a LISTEN
// does when PostgreSQL does not answer.
type stalledPoolManager struct {
	connpoolmanager.PoolManager
}

func (stalledPoolManager) PgUser() string { return "postgres" }

func (stalledPoolManager) NewReservedConn(ctx context.Context, _ map[string]string, _ string, _, _ []byte, _ ...reserved.ReservedConnOption) (*reserved.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestServer_NotificationStreamCallerCancelWhileLoopBlocked checks that a
// caller giving up on a stream whose loop is blocked on a subscribe (the
// gateway's ready timeout) is released at once, as a gRPC client stream is:
// Recv and Send return its ctx error instead of waiting on the loop.
func TestServer_NotificationStreamCallerCancelWhileLoopBlocked(t *testing.T) {
	s, _ := newRegisteredServer(t)
	l := pubsub.NewListener(stalledPoolManager{}, slog.New(slog.DiscardHandler), nil)
	l.Start(context.Background())
	t.Cleanup(l.Stop)
	s.pooler.SetPubSubListener(l)

	ctx, cancel := context.WithCancel(t.Context())
	stream, err := s.NotificationStream(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&multipoolerpb.NotificationStreamRequest{SubscribeChannels: []string{"ch"}}))

	recvErr := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		recvErr <- err
	}()
	select {
	case err := <-recvErr:
		t.Fatalf("Recv returned while the subscribe was blocked: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-recvErr:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Recv stayed blocked after the caller cancelled")
	}
	sendErr := make(chan error, 1)
	go func() {
		sendErr <- stream.Send(&multipoolerpb.NotificationStreamRequest{SubscribeChannels: []string{"other"}})
	}()
	select {
	case err := <-sendErr:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Send stayed blocked after the caller cancelled")
	}
}

// TestPipe_RecvDrainsBufferedBeforeEnd checks that messages the loop sent
// before ending are still delivered, in order, before the end is reported:
// a buffered pipe must not lose a final structured error.
func TestPipe_RecvDrainsBufferedBeforeEnd(t *testing.T) {
	p := newPipe[int, int](t.Context(), 4)
	send := p.serverSend(t.Context())
	require.NoError(t, send(1))
	require.NoError(t, send(2))
	loopErr := mterrors.New(mtrpcpb.Code_INTERNAL, "tunnel failed")
	p.finish(loopErr)

	for _, want := range []int{1, 2} {
		got, err := p.Recv()
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	_, err := p.Recv()
	assert.ErrorIs(t, err, loopErr)
	assert.ErrorIs(t, p.Send(3), io.EOF, "Send after the loop ended")
}

// TestPipe_EndOfStream checks how a loop's end reaches the caller: a clean or
// cancelled end is io.EOF, the caller's own cancellation its ctx error.
func TestPipe_EndOfStream(t *testing.T) {
	clean := newPipe[int, int](t.Context(), 0)
	clean.finish(nil)
	_, err := clean.Recv()
	assert.ErrorIs(t, err, io.EOF)

	cancelled := newPipe[int, int](t.Context(), 0)
	cancelled.finish(context.Canceled)
	_, err = cancelled.Recv()
	assert.ErrorIs(t, err, io.EOF, "the loop ended by Stop or the pooler, not by the caller")

	ctx, cancel := context.WithCancel(t.Context())
	byCaller := newPipe[int, int](ctx, 0)
	cancel()
	_, err = byCaller.Recv()
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, byCaller.Send(1), context.Canceled)
}

// TestCopyingData checks that a replication data message handed to the pipe
// keeps its bytes when the sender reuses its buffer, as the tunnel does once
// send returns.
func TestCopyingData(t *testing.T) {
	var got *multipoolerpb.StreamReplicationResponse
	send := copyingData(func(resp *multipoolerpb.StreamReplicationResponse) error {
		got = resp
		return nil
	})
	buf := []byte("wal-1")
	require.NoError(t, send(&multipoolerpb.StreamReplicationResponse{
		Msg: &multipoolerpb.StreamReplicationResponse_Data{Data: buf},
	}))
	copy(buf, "XXXXX") // the tunnel reads the next chunk into the same buffer
	assert.Equal(t, "wal-1", string(got.GetData()))
}

// failingReplPoolManager fails to open the replication backend.
type failingReplPoolManager struct {
	connpoolmanager.PoolManager
	err error
}

func (f failingReplPoolManager) NewLogicalReplicationConn(context.Context, string, []byte, []byte) (*reserved.Conn, error) {
	return nil, f.err
}

// newServingServer registers a server whose pooler is SERVING with pm.
func newServingServer(t *testing.T, pm connpoolmanager.PoolManager) *Server {
	t.Helper()
	p := poolerserver.NewQueryPoolerServer(slog.New(slog.DiscardHandler), pm, &clustermetadatapb.ID{Name: "p"}, "", "", nil, 0, false)
	require.NoError(t, p.OnStateChange(t.Context(), servingstate.State{
		Routing:       servingstate.RoutingState{Role: servingstate.RoutingRoleReplica},
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
	}))
	s := NewServer()
	s.register(p)
	return s
}

func replicationInit() *multipoolerpb.StreamReplicationInit {
	return &multipoolerpb.StreamReplicationInit{Mode: multipoolerpb.ReplicationMode_REPLICATION_MODE_DATABASE, User: "u"}
}

func TestServer_StreamReplicationRefusedWhenNotServing(t *testing.T) {
	_, err := NewServer().StreamReplication(t.Context(), replicationInit())
	requireNotServing(t, err)

	s := newServingServer(t, failingReplPoolManager{})
	s.Stop()
	_, err = s.StreamReplication(t.Context(), replicationInit())
	requireNotServing(t, err)
}

// TestServer_StreamReplicationBackendOpenError checks that a backend that
// cannot be opened reaches the caller through the pipe as its PostgreSQL
// diagnostic, as over gRPC, so the gateway re-emits it verbatim.
func TestServer_StreamReplicationBackendOpenError(t *testing.T) {
	diag := mterrors.NewPgError("FATAL", mterrors.PgSSAuthFailed, `password authentication failed for user "u"`, "")
	s := newServingServer(t, failingReplPoolManager{err: diag})

	_, err := s.StreamReplication(t.Context(), replicationInit())
	var got *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &got)
	assert.Equal(t, diag.Code, got.Code)
	assert.Equal(t, diag.Message, got.Message)
}

// TestServer_StreamReplicationInvalidInit checks that the pooler's init checks
// apply on the direct path: an unsupported mode is refused as invalid.
func TestServer_StreamReplicationInvalidInit(t *testing.T) {
	s := newServingServer(t, failingReplPoolManager{})
	init := replicationInit()
	init.Mode = multipoolerpb.ReplicationMode_REPLICATION_MODE_UNSPECIFIED

	_, err := s.StreamReplication(t.Context(), init)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err), "got %v", err)
}

// TestServer_StreamReplicationAdmissionRejected checks that a registered pooler
// that is not serving refuses a replication stream at admission on the direct
// path, before opening any backend, as over gRPC.
func TestServer_StreamReplicationAdmissionRejected(t *testing.T) {
	pm := newStalledReplPoolManager(nil)
	// The pooler starts NOT_SERVING.
	p := poolerserver.NewQueryPoolerServer(slog.New(slog.DiscardHandler), pm, &clustermetadatapb.ID{Name: "p"}, "", "", nil, 0, false)
	s := NewServer()
	s.register(p)

	_, err := s.StreamReplication(t.Context(), replicationInit())
	require.Error(t, err)
	select {
	case <-pm.entered:
		t.Fatal("a backend was opened for a stream admission should have refused")
	default:
	}
}

// TestServer_StreamReplicationNilPoolManager checks that a serving pooler with
// no pool manager reports unavailable on the direct path instead of panicking,
// as over gRPC.
func TestServer_StreamReplicationNilPoolManager(t *testing.T) {
	s := newServingServer(t, nil)

	_, err := s.StreamReplication(t.Context(), replicationInit())
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "got %v", err)
}

// TestServer_StreamReplicationBackendOpenInfraError checks that a backend
// that cannot be opened for a reason that is not a PostgreSQL error reaches the
// caller as that error, not as a generic one.
func TestServer_StreamReplicationBackendOpenInfraError(t *testing.T) {
	cause := mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pool wait timeout")
	s := newServingServer(t, failingReplPoolManager{err: cause})

	_, err := s.StreamReplication(t.Context(), replicationInit())
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "got %v", err)
	assert.Contains(t, err.Error(), "pool wait timeout")
}

// stalledReplPoolManager opens no replication backend: NewLogicalReplicationConn
// reports on entered, then waits for release, or for its ctx to end when
// release is nil, and takes a moment more to return, as tearing down a
// backend does. returned is closed once it has.
type stalledReplPoolManager struct {
	connpoolmanager.PoolManager
	entered  chan struct{}
	release  chan struct{}
	returned chan struct{}
}

func newStalledReplPoolManager(release chan struct{}) *stalledReplPoolManager {
	return &stalledReplPoolManager{entered: make(chan struct{}), release: release, returned: make(chan struct{})}
}

func (m *stalledReplPoolManager) NewLogicalReplicationConn(ctx context.Context, _ string, _, _ []byte) (*reserved.Conn, error) {
	defer close(m.returned)
	close(m.entered)
	if m.release != nil {
		<-m.release
	} else {
		<-ctx.Done()
	}
	time.Sleep(50 * time.Millisecond)
	return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "backend not opened")
}

// TestServer_StopEndsReplicationStreams checks that Stop ends an opening
// replication stream and waits for its loop to return, so nothing the loop
// holds outlives Stop.
func TestServer_StopEndsReplicationStreams(t *testing.T) {
	pm := newStalledReplPoolManager(nil)
	s := newServingServer(t, pm)

	openErr := make(chan error, 1)
	go func() {
		_, err := s.StreamReplication(t.Context(), replicationInit())
		openErr <- err
	}()
	<-pm.entered

	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not end the replication stream")
	}
	select {
	case <-pm.returned:
	default:
		t.Fatal("Stop returned before the replication loop did")
	}
	select {
	case err := <-openErr:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the caller was not released after Stop")
	}
}

// TestServer_StreamReplicationCallerCancelWhileLoopBlocked checks that a
// caller giving up on a replication stream whose loop is blocked opening the
// backend is released at once, as a gRPC client stream is, instead of waiting
// on the loop.
func TestServer_StreamReplicationCallerCancelWhileLoopBlocked(t *testing.T) {
	release := make(chan struct{})
	pm := newStalledReplPoolManager(release)
	s := newServingServer(t, pm)
	t.Cleanup(func() {
		close(release)
		<-pm.returned
	})

	ctx, cancel := context.WithCancel(t.Context())
	openErr := make(chan error, 1)
	go func() {
		_, err := s.StreamReplication(ctx, replicationInit())
		openErr <- err
	}()
	<-pm.entered
	select {
	case err := <-openErr:
		t.Fatalf("StreamReplication returned while the backend was opening: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-openErr:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("StreamReplication stayed blocked after the caller cancelled")
	}
}
