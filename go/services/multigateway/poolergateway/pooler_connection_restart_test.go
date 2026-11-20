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
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/queryservice"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
)

// scriptedPooler is a pooler whose health subscription is scripted and counted.
// Other methods panic through the nil embedded interface.
type scriptedPooler struct {
	queryservice.Pooler

	// open decides what the subscription does; attempts counts the calls.
	open     func(ctx context.Context) (func() (*multipoolerservice.StreamPoolerHealthResponse, error), error)
	attempts atomic.Int32
}

func (p *scriptedPooler) SubscribeHealth(ctx context.Context) (func() (*multipoolerservice.StreamPoolerHealthResponse, error), error) {
	p.attempts.Add(1)
	return p.open(ctx)
}

func (p *scriptedPooler) Close() error { return nil }

var errPoolerDown = errors.New("pooler is down")

// downPooler refuses every health subscription, as a crashed pooler's address does.
func downPooler() *scriptedPooler {
	return &scriptedPooler{open: func(context.Context) (func() (*multipoolerservice.StreamPoolerHealthResponse, error), error) {
		return nil, errPoolerDown
	}}
}

// healthyPooler sends one health response and then holds the stream open until
// its context ends. closed records the end of the stream.
func healthyPooler(closed *atomic.Bool) *scriptedPooler {
	return &scriptedPooler{open: func(ctx context.Context) (func() (*multipoolerservice.StreamPoolerHealthResponse, error), error) {
		sent := false
		return func() (*multipoolerservice.StreamPoolerHealthResponse, error) {
			if !sent {
				sent = true
				return &multipoolerservice.StreamPoolerHealthResponse{ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}, nil
			}
			<-ctx.Done()
			closed.Store(true)
			return nil, ctx.Err()
		}, nil
	}}
}

// longBackoff makes the wait between attempts last for hours, whatever the jitter
// draws, so that an attempt within moments of a call is one the call caused.
func longBackoff(t *testing.T) {
	t.Helper()
	prevDelay, prevMax := healthRetryDelay, healthRetryMaxDelay
	healthRetryDelay, healthRetryMaxDelay = 1000*time.Hour, 1000*time.Hour
	t.Cleanup(func() { healthRetryDelay, healthRetryMaxDelay = prevDelay, prevMax })
}

// startScripted opens a connection to p, as the pooler cache does for a pooler
// it sees. The connection is shut down, and its health loop waited for, at the end of
// the test.
func startScripted(t *testing.T, p *scriptedPooler) (*poolerConnection, *clustermetadatapb.Multipooler) {
	t.Helper()
	record := unreachablePooler("zone1", "pooler-1")
	record.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{
		Status:  clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		Updated: timestamppb.New(time.Unix(1000, 0)),
	}
	local := &LocalPooler{ID: func() *clustermetadatapb.ID { return record.Id }, Service: p}
	conn, err := newPoolerConnection(t.Context(), record, slog.New(slog.DiscardHandler),
		grpc.WithTransportCredentials(insecure.NewCredentials()), local, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Shutdown()
		select {
		case <-conn.checkConnDone:
		case <-time.After(5 * time.Second):
			t.Error("the health loop did not stop after Shutdown")
		}
	})
	return conn, record
}

// restartOf is the record of a pooler that has restarted: the process wrote its
// lifecycle entry again.
func restartOf(prev *clustermetadatapb.Multipooler, status clustermetadatapb.PoolerLifecycleStatus) *clustermetadatapb.Multipooler {
	curr := proto.Clone(prev).(*clustermetadatapb.Multipooler)
	curr.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{
		Status:  status,
		Reason:  "process starting",
		Updated: timestamppb.New(prev.GetLifecycleStatus().GetUpdated().AsTime().Add(time.Minute)),
	}
	return curr
}

// republishOf is a routine republish of the record: a serving status or routing
// state change, with the lifecycle entry as it was.
func republishOf(prev *clustermetadatapb.Multipooler) *clustermetadatapb.Multipooler {
	curr := proto.Clone(prev).(*clustermetadatapb.Multipooler)
	curr.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	curr.RoutingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY}
	return curr
}

func waitForAttempts(t *testing.T, p *scriptedPooler, want int32) {
	t.Helper()
	require.Eventually(t, func() bool { return p.attempts.Load() >= want }, 5*time.Second, 5*time.Millisecond,
		"expected %d health stream attempts, have %d", want, p.attempts.Load())
}

// waitForFailure waits until the connection has recorded an error from its health stream.
func waitForFailure(t *testing.T, conn *poolerConnection) {
	t.Helper()
	require.Eventually(t, func() bool { return errors.Is(conn.Health().LastError, errPoolerDown) }, 5*time.Second, 5*time.Millisecond)
}

func TestPoolerRestarted(t *testing.T) {
	prev := unreachablePooler("zone1", "pooler-1")
	prev.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{
		Status:  clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		Updated: timestamppb.New(time.Unix(1000, 0)),
	}
	noLifecycle := proto.Clone(prev).(*clustermetadatapb.Multipooler)
	noLifecycle.LifecycleStatus = nil

	tests := []struct {
		name       string
		prev, curr *clustermetadatapb.Multipooler
		want       bool
	}{
		{"a restarted process registers", prev, restartOf(prev, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING), true},
		{"a restarted process finishes starting", prev, restartOf(prev, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE), true},
		{"a first lifecycle entry", noLifecycle, restartOf(prev, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING), true},
		{"a routine republish", prev, republishOf(prev), false},
		{"the same lifecycle entry", prev, proto.Clone(prev).(*clustermetadatapb.Multipooler), false},
		{"a pooler that is stopping", prev, restartOf(prev, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STOPPING), false},
		{"a quarantined pooler", prev, restartOf(prev, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED), false},
		{"a record with no lifecycle entry", prev, noLifecycle, false},
		{"neither record has one", noLifecycle, proto.Clone(noLifecycle).(*clustermetadatapb.Multipooler), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, poolerRestarted(tt.prev, tt.curr))
		})
	}
}

// A restart of a pooler whose health stream is backing off makes the stream retry
// at once, once.
func TestRetryHealthNow_RestartEndsTheBackoff(t *testing.T) {
	longBackoff(t)
	p := downPooler()
	conn, record := startScripted(t, p)

	waitForAttempts(t, p, 1)
	waitForFailure(t, conn)
	time.Sleep(200 * time.Millisecond)
	require.EqualValues(t, 1, p.attempts.Load(), "the backoff keeps the loop waiting")

	conn.applyTopologyUpdate(record, restartOf(record, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING))
	waitForAttempts(t, p, 2)

	// The retry failed, and the loop backs off again: one call, one attempt.
	time.Sleep(300 * time.Millisecond)
	assert.EqualValues(t, 2, p.attempts.Load())
}

// The pooler is back and serves: the retry the restart asked for succeeds.
func TestRetryHealthNow_RestartReachesAPoolerThatServesAgain(t *testing.T) {
	longBackoff(t)
	var up atomic.Bool
	var closed atomic.Bool
	healthy := healthyPooler(&closed)
	p := &scriptedPooler{open: func(ctx context.Context) (func() (*multipoolerservice.StreamPoolerHealthResponse, error), error) {
		if !up.Load() {
			return nil, errPoolerDown
		}
		return healthy.open(ctx)
	}}
	conn, record := startScripted(t, p)
	waitForFailure(t, conn)
	require.False(t, conn.Health().isServing())

	up.Store(true)
	conn.applyTopologyUpdate(record, restartOf(record, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING))
	require.Eventually(t, func() bool { return conn.Health().isServing() }, 5*time.Second, 5*time.Millisecond,
		"the connection must see the pooler serve again without waiting out the backoff")
	assert.NoError(t, conn.Health().LastError)
}

// A routine republish leaves the backoff alone.
func TestRetryHealthNow_RoutineUpdateDoesNotRetry(t *testing.T) {
	longBackoff(t)
	p := downPooler()
	conn, record := startScripted(t, p)
	waitForAttempts(t, p, 1)
	waitForFailure(t, conn)

	for range 3 {
		conn.applyTopologyUpdate(record, republishOf(record))
	}
	conn.applyTopologyUpdate(record, restartOf(record, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STOPPING))
	time.Sleep(300 * time.Millisecond)
	assert.EqualValues(t, 1, p.attempts.Load(), "no routine update may cut the backoff short")
}

// A healthy stream is not touched: not closed, not retried, not marked unhealthy.
func TestRetryHealthNow_HealthyStreamIsUntouched(t *testing.T) {
	longBackoff(t)
	var closed atomic.Bool
	p := healthyPooler(&closed)
	conn, record := startScripted(t, p)
	require.Eventually(t, func() bool { return conn.Health().isServing() }, 5*time.Second, 5*time.Millisecond)

	for range 3 {
		conn.applyTopologyUpdate(record, restartOf(record, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING))
	}
	time.Sleep(300 * time.Millisecond)
	assert.EqualValues(t, 1, p.attempts.Load(), "a healthy stream is not reopened")
	assert.False(t, closed.Load(), "a healthy stream is not closed")
	assert.True(t, conn.Health().isServing())
	assert.NoError(t, conn.Health().LastError)
}

// A call during an attempt in progress does not cut the attempt short. The attempt
// ends the way it would have, and the retry the call asked for follows it.
func TestRetryHealthNow_DoesNotInterruptAnAttemptInProgress(t *testing.T) {
	longBackoff(t)
	release := make(chan struct{})
	var cancelled atomic.Bool
	p := &scriptedPooler{}
	p.open = func(ctx context.Context) (func() (*multipoolerservice.StreamPoolerHealthResponse, error), error) {
		if p.attempts.Load() > 1 {
			return nil, errPoolerDown
		}
		// The first attempt opens the stream and then sits without a message.
		return func() (*multipoolerservice.StreamPoolerHealthResponse, error) {
			select {
			case <-release:
				return nil, errPoolerDown
			case <-ctx.Done():
				cancelled.Store(true)
				return nil, ctx.Err()
			}
		}, nil
	}
	conn, record := startScripted(t, p)
	waitForAttempts(t, p, 1)

	// The stream has no health yet: the connection is not healthy, but the attempt is running.
	require.Error(t, conn.Health().LastError)
	conn.applyTopologyUpdate(record, restartOf(record, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING))
	time.Sleep(200 * time.Millisecond)
	assert.False(t, cancelled.Load(), "the attempt in progress must not be cancelled")
	assert.EqualValues(t, 1, p.attempts.Load())

	close(release)
	waitForAttempts(t, p, 2)
}

// Calls from many goroutines, and a Shutdown among them, neither race nor leave
// the health loop running.
func TestRetryHealthNow_RacesWithShutdown(t *testing.T) {
	longBackoff(t)
	p := downPooler()
	record := unreachablePooler("zone1", "pooler-1")
	local := &LocalPooler{ID: func() *clustermetadatapb.ID { return record.Id }, Service: p}
	conn, err := newPoolerConnection(t.Context(), record, slog.New(slog.DiscardHandler),
		grpc.WithTransportCredentials(insecure.NewCredentials()), local, nil)
	require.NoError(t, err)
	waitForAttempts(t, p, 1)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 200 {
				conn.RetryHealthNow()
			}
		})
	}
	wg.Go(func() {
		time.Sleep(5 * time.Millisecond)
		_ = conn.Shutdown()
	})
	wg.Wait()
	// Calls after the Shutdown are harmless too.
	conn.RetryHealthNow()

	select {
	case <-conn.checkConnDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the health loop kept running after Shutdown")
	}
	attempts := p.attempts.Load()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, attempts, p.attempts.Load(), "no attempt starts after the loop has stopped")
}

// serveHealthOn serves a health stream on addr and returns the server and what it records.
func serveHealthOn(t *testing.T, addr string) (*grpc.Server, *controllableHealthServer) {
	t.Helper()
	var lis net.Listener
	require.Eventually(t, func() bool {
		var err error
		lis, err = net.Listen("tcp", addr)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond, "listen on %s", addr)
	mock := newControllableHealthServer()
	server := grpc.NewServer()
	multipoolerservice.RegisterMultipoolerServiceServer(server, mock)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return server, mock
}

// A killed pooler leaves the gateway's gRPC channel in TRANSIENT_FAILURE, where a
// call fails without trying to connect. The retry must reconnect the channel
// first, or it is spent on a call that cannot succeed and the backoff starts over.
func TestRetryHealthNow_ReconnectsTheChannelOfAKilledPooler(t *testing.T) {
	longBackoff(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	first, firstMock := serveHealthOn(t, addr)

	record := unreachablePooler("zone1", "pooler-1")
	record.Hostname = "127.0.0.1"
	record.PortMap["grpc"] = int32(lis.Addr().(*net.TCPAddr).Port)
	record.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{
		Status:  clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		Updated: timestamppb.New(time.Unix(1000, 0)),
	}
	conn, err := newPoolerConnection(t.Context(), record, slog.New(slog.DiscardHandler),
		grpc.WithTransportCredentials(insecure.NewCredentials()), nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Shutdown()
		<-conn.checkConnDone
	})
	waitForStreamOpened(t, firstMock)
	firstMock.responseCh <- makeHealthResponse(clustermetadatapb.PoolerServingStatus_SERVING)
	require.Eventually(t, func() bool { return conn.Health().isServing() }, 5*time.Second, 5*time.Millisecond)

	// The pooler is killed: the stream breaks, and a call to its address is refused.
	first.Stop()
	require.Eventually(t, func() bool { return conn.Health().LastError != nil }, 5*time.Second, 5*time.Millisecond)
	conn.conn.Connect()
	require.Eventually(t, func() bool { return conn.conn.GetState() == connectivity.TransientFailure }, 5*time.Second, 5*time.Millisecond,
		"the channel never gave up on the dead address")

	// The pooler starts again and registers: its record is rewritten.
	_, secondMock := serveHealthOn(t, addr)
	conn.applyTopologyUpdate(record, restartOf(record, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING))

	select {
	case <-secondMock.streamOpened:
	case <-time.After(3 * time.Second):
		t.Fatal("the health stream did not reach the pooler that started again")
	}
	secondMock.responseCh <- makeHealthResponse(clustermetadatapb.PoolerServingStatus_SERVING)
	require.Eventually(t, func() bool { return conn.Health().isServing() }, 5*time.Second, 5*time.Millisecond)
}
