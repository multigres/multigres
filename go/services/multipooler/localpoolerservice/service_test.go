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
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/pubsub"
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
