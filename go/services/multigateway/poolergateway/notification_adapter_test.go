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

package poolergateway

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// fakeNotificationStream acks every Send with Ready and otherwise returns
// whatever the test pushes into resps. Closing resps makes Recv return io.EOF,
// which is how a demoted pooler ends the stream.
type fakeNotificationStream struct {
	grpc.BidiStreamingClient[multipoolerpb.NotificationStreamRequest, multipoolerpb.NotificationStreamResponse]
	ctx   context.Context
	resps chan *multipoolerpb.NotificationStreamResponse

	mu   sync.Mutex
	sent []*multipoolerpb.NotificationStreamRequest
}

func (f *fakeNotificationStream) Send(req *multipoolerpb.NotificationStreamRequest) error {
	f.mu.Lock()
	f.sent = append(f.sent, req)
	f.mu.Unlock()
	f.resps <- &multipoolerpb.NotificationStreamResponse{Ready: true}
	return nil
}

func (f *fakeNotificationStream) Recv() (*multipoolerpb.NotificationStreamResponse, error) {
	select {
	case resp, ok := <-f.resps:
		if !ok {
			return nil, io.EOF
		}
		return resp, nil
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

func (f *fakeNotificationStream) subscribed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, req := range f.sent {
		out = append(out, req.GetSubscribeChannels()...)
	}
	return out
}

type fakeNotificationClient struct {
	multipoolerpb.MultipoolerServiceClient

	mu      sync.Mutex
	streams []*fakeNotificationStream
}

func (c *fakeNotificationClient) NotificationStream(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[multipoolerpb.NotificationStreamRequest, multipoolerpb.NotificationStreamResponse], error) {
	st := &fakeNotificationStream{ctx: ctx, resps: make(chan *multipoolerpb.NotificationStreamResponse, 16)}
	c.mu.Lock()
	c.streams = append(c.streams, st)
	c.mu.Unlock()
	return st, nil
}

func (c *fakeNotificationClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.streams)
}

func (c *fakeNotificationClient) stream(i int) *fakeNotificationStream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streams[i]
}

// An idle session whose stream ends with EOF (pooler demotion) opens a new
// stream on its own and resends its active channel set, so notifications keep
// flowing without the client re-issuing LISTEN.
func TestNotificationSession_ReconnectsAfterEOF(t *testing.T) {
	client := &fakeNotificationClient{}
	m := NewGRPCNotificationManager(
		func() multipoolerpb.MultipoolerServiceClient { return client },
		slog.New(slog.DiscardHandler), nil)
	notifCh := make(chan *sqltypes.Notification, 8)

	m.Subscribe("ch", notifCh)
	require.Equal(t, 1, client.count())
	require.Equal(t, []string{"ch"}, client.stream(0).subscribed())

	// Demotion: the pooler handler returns and the gateway sees EOF.
	close(client.stream(0).resps)

	require.Eventually(t, func() bool {
		return client.count() == 2 && len(client.stream(1).subscribed()) == 1
	}, 5*time.Second, 5*time.Millisecond, "expected a second stream with the channel resubscribed")
	second := client.stream(1)
	require.Equal(t, []string{"ch"}, second.subscribed())

	// The new stream is the one feeding the session.
	second.resps <- &multipoolerpb.NotificationStreamResponse{
		Notification: &query.PgNotification{Channel: "ch", Payload: "after"},
	}
	select {
	case n := <-notifCh:
		require.Equal(t, "after", n.Payload)
	case <-time.After(5 * time.Second):
		t.Fatal("notification on the reopened stream was not delivered")
	}

	// Once the session ends, a stream ending opens nothing new.
	m.UnsubscribeAll(notifCh)
	close(second.resps)
	m.OnLeaderServing(nil)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 2, client.count())
}

// When the stream ends and no leader is known yet (mid-failover), the
// immediate attempt fails and the session waits, with no timer, for the load
// balancer to report the new leader serving. That hook opens the stream.
func TestNotificationSession_ReconnectsOnLeaderServing(t *testing.T) {
	client := &fakeNotificationClient{}
	var leaderKnown atomic.Bool
	leaderKnown.Store(true)
	m := NewGRPCNotificationManager(
		func() multipoolerpb.MultipoolerServiceClient {
			if !leaderKnown.Load() {
				return nil
			}
			return client
		},
		slog.New(slog.DiscardHandler), nil)
	notifCh := make(chan *sqltypes.Notification, 8)

	m.Subscribe("ch", notifCh)
	require.Equal(t, 1, client.count())

	leaderKnown.Store(false)
	close(client.stream(0).resps)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, client.count(), "no leader: the session must wait, not open a stream")

	leaderKnown.Store(true)
	m.OnLeaderServing(nil)
	require.Eventually(t, func() bool {
		return client.count() == 2 && len(client.stream(1).subscribed()) == 1
	}, 5*time.Second, 5*time.Millisecond, "leader-serving hook should reopen the stream")
	require.Equal(t, []string{"ch"}, client.stream(1).subscribed())

	m.UnsubscribeAll(notifCh)
}
