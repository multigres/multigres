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
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// controllableHealthServer is a mock gRPC server that implements StreamPoolerHealth.
// Test code controls what responses are sent via channels, enabling precise
// testing of the client-side streaming logic.
type controllableHealthServer struct {
	multipoolerservice.UnimplementedMultiPoolerServiceServer

	// responseCh receives responses to send to the client.
	// The test pushes responses here to control what the client sees.
	responseCh chan *multipoolerservice.StreamPoolerHealthResponse

	// errCh receives errors to return from the stream.
	// Sending an error here causes StreamPoolerHealth to return that error.
	errCh chan error

	// streamOpened is signaled each time a client opens a new health stream.
	// This lets tests synchronize with the client's retry logic.
	streamOpened chan struct{}
}

func newControllableHealthServer() *controllableHealthServer {
	return &controllableHealthServer{
		responseCh:   make(chan *multipoolerservice.StreamPoolerHealthResponse, 10),
		errCh:        make(chan error, 1),
		streamOpened: make(chan struct{}, 10),
	}
}

func (s *controllableHealthServer) StreamPoolerHealth(
	_ *multipoolerservice.StreamPoolerHealthRequest,
	stream grpc.ServerStreamingServer[multipoolerservice.StreamPoolerHealthResponse],
) error {
	// Signal that a new stream was opened.
	s.streamOpened <- struct{}{}

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-s.errCh:
			return err
		case resp := <-s.responseCh:
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

// streamingTestSetup contains the components created by setupStreamingTest.
type streamingTestSetup struct {
	server     *controllableHealthServer
	grpcServer *grpc.Server
	conn       *PoolerConnection
}

// setupStreamingTest creates a controllable mock gRPC server and a PoolerConnection
// connected to it. The PoolerConnection immediately starts its health streaming loop.
func setupStreamingTest(t *testing.T, ctx context.Context) *streamingTestSetup {
	t.Helper()
	return setupStreamingTestWithCallback(t, ctx, nil)
}

// setupStreamingTestWithCallback is like setupStreamingTest but accepts an
// onHealthUpdate callback.
func setupStreamingTestWithCallback(
	t *testing.T,
	ctx context.Context,
	onHealthUpdate func(*PoolerConnection),
) *streamingTestSetup {
	t.Helper()

	mockServer := newControllableHealthServer()

	// Start a real gRPC server on a random port.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	multipoolerservice.RegisterMultiPoolerServiceServer(grpcServer, mockServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
	})

	// Create a MultiPooler proto pointing at our test server.
	port := lis.Addr().(*net.TCPAddr).Port
	pooler := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "test-cell",
			Name:      "test-pooler",
		},
		Hostname:   "127.0.0.1",
		TableGroup: "default",
		Shard:      "0",
		Type:       clustermetadatapb.PoolerType_PRIMARY,
		PortMap: map[string]int32{
			"grpc": int32(port),
		},
	}

	logger := slog.Default()
	conn, err := NewPoolerConnection(ctx, pooler, logger, onHealthUpdate)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	return &streamingTestSetup{
		server:     mockServer,
		grpcServer: grpcServer,
		conn:       conn,
	}
}

// waitForStreamOpened blocks until the mock server receives a new stream
// connection, or fails the test after a timeout.
func waitForStreamOpened(t *testing.T, server *controllableHealthServer) {
	t.Helper()
	select {
	case <-server.streamOpened:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for health stream to open")
	}
}

// makeHealthResponse creates a StreamPoolerHealthResponse with the given status.
func makeHealthResponse(
	status clustermetadatapb.PoolerServingStatus,
) *multipoolerservice.StreamPoolerHealthResponse {
	return &multipoolerservice.StreamPoolerHealthResponse{
		Target: &query.Target{
			TableGroup: "default",
			Shard:      "0",
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		},
		PoolerId: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "test-cell",
			Name:      "test-pooler",
		},
		ServingStatus: status,
	}
}

// TestPoolerConnection_StreamHealth_InitialState verifies that when a health
// stream opens and receives a SERVING response, the PoolerConnection's health
// transitions from NOT_SERVING (uninitialized) to SERVING.
func TestPoolerConnection_StreamHealth_InitialState(t *testing.T) {
	setup := setupStreamingTest(t, t.Context())
	waitForStreamOpened(t, setup.server)

	// Before sending any response, health should be NOT_SERVING (uninitialized).
	health := setup.conn.Health()
	require.NotNil(t, health)
	assert.False(t, health.IsServing(), "should not be serving before first response")
	assert.ErrorIs(t, health.LastError, errPoolerUninitialized)

	// Send a SERVING response.
	setup.server.responseCh <- makeHealthResponse(clustermetadatapb.PoolerServingStatus_SERVING)

	// Wait for health to become serving.
	require.Eventually(t, func() bool {
		return setup.conn.Health().IsServing()
	}, 2*time.Second, 10*time.Millisecond, "health should become serving after response")

	health = setup.conn.Health()
	assert.Nil(t, health.LastError, "no error after successful response")
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, health.ServingStatus)
	assert.False(t, health.LastResponse.IsZero(), "LastResponse should be set")
}

// TestPoolerConnection_StreamHealth_StateTransitions verifies that the health
// state tracks serving status changes from the server.
func TestPoolerConnection_StreamHealth_StateTransitions(t *testing.T) {
	setup := setupStreamingTest(t, t.Context())
	waitForStreamOpened(t, setup.server)

	// Transition 1: NOT_SERVING (initial) -> SERVING
	setup.server.responseCh <- makeHealthResponse(clustermetadatapb.PoolerServingStatus_SERVING)
	require.Eventually(t, func() bool {
		return setup.conn.Health().IsServing()
	}, 2*time.Second, 10*time.Millisecond)

	// Transition 2: SERVING -> NOT_SERVING
	setup.server.responseCh <- makeHealthResponse(clustermetadatapb.PoolerServingStatus_NOT_SERVING)
	require.Eventually(t, func() bool {
		return !setup.conn.Health().IsServing()
	}, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_NOT_SERVING,
		setup.conn.Health().ServingStatus)

	// Transition 3: NOT_SERVING -> SERVING again
	setup.server.responseCh <- makeHealthResponse(clustermetadatapb.PoolerServingStatus_SERVING)
	require.Eventually(t, func() bool {
		return setup.conn.Health().IsServing()
	}, 2*time.Second, 10*time.Millisecond)
}

// TestPoolerConnection_StreamHealth_StalenessTimeout verifies that when the
// server sends a short recommended_staleness_timeout and then stops sending
// responses, the health stream times out and the connection becomes not-serving.
func TestPoolerConnection_StreamHealth_StalenessTimeout(t *testing.T) {
	setup := setupStreamingTest(t, t.Context())
	waitForStreamOpened(t, setup.server)

	// Send a SERVING response with a very short staleness timeout.
	resp := makeHealthResponse(clustermetadatapb.PoolerServingStatus_SERVING)
	resp.RecommendedStalenessTimeout = durationpb.New(200 * time.Millisecond)
	setup.server.responseCh <- resp

	require.Eventually(t, func() bool {
		return setup.conn.Health().IsServing()
	}, 2*time.Second, 10*time.Millisecond)

	// Now stop sending responses. The staleness timer should fire.
	// After timeout, health should become NOT_SERVING with an error.
	require.Eventually(t, func() bool {
		h := setup.conn.Health()
		return !h.IsServing() && h.LastError != nil
	}, 5*time.Second, 50*time.Millisecond,
		"health should become not-serving after staleness timeout")

	health := setup.conn.Health()
	assert.Contains(t, health.LastError.Error(), "timed out")
}

// TestPoolerConnection_StreamHealth_RetryOnError verifies that when the health
// stream encounters an error, the client retries and recovers when a new
// stream succeeds.
func TestPoolerConnection_StreamHealth_RetryOnError(t *testing.T) {
	setup := setupStreamingTest(t, t.Context())
	waitForStreamOpened(t, setup.server)

	// Send initial SERVING response.
	setup.server.responseCh <- makeHealthResponse(clustermetadatapb.PoolerServingStatus_SERVING)
	require.Eventually(t, func() bool {
		return setup.conn.Health().IsServing()
	}, 2*time.Second, 10*time.Millisecond)

	// Inject a stream error. This causes the server handler to return an error,
	// which terminates the current stream.
	setup.server.errCh <- assert.AnError

	// Health should become not-serving after the stream error.
	require.Eventually(t, func() bool {
		h := setup.conn.Health()
		return !h.IsServing() && h.LastError != nil
	}, 5*time.Second, 50*time.Millisecond,
		"health should become not-serving after stream error")

	// The client should retry and open a new stream.
	waitForStreamOpened(t, setup.server)

	// Send SERVING on the new stream to recover.
	setup.server.responseCh <- makeHealthResponse(clustermetadatapb.PoolerServingStatus_SERVING)
	require.Eventually(t, func() bool {
		return setup.conn.Health().IsServing()
	}, 10*time.Second, 50*time.Millisecond,
		"health should recover after retry")
}

// TestPoolerConnection_StreamHealth_ContextCancellation verifies that when
// the parent context is cancelled, the health stream goroutine stops cleanly.
func TestPoolerConnection_StreamHealth_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	setup := setupStreamingTest(t, ctx)
	waitForStreamOpened(t, setup.server)

	// Send an initial response to establish the stream.
	setup.server.responseCh <- makeHealthResponse(clustermetadatapb.PoolerServingStatus_SERVING)
	require.Eventually(t, func() bool {
		return setup.conn.Health().IsServing()
	}, 2*time.Second, 10*time.Millisecond)

	// Cancel the context. This should cause checkConn to exit.
	cancel()

	// Give the goroutine time to exit. We verify it doesn't open a new stream
	// (no retry after context cancellation).
	select {
	case <-setup.server.streamOpened:
		// It's OK if a stream open was already buffered before cancel took effect.
		// But no further opens should happen.
		time.Sleep(200 * time.Millisecond)
		select {
		case <-setup.server.streamOpened:
			t.Fatal("stream should not be opened after context cancellation")
		default:
			// Good - no more stream opens.
		}
	case <-time.After(1 * time.Second):
		// Good - no stream opened after cancel.
	}
}

// TestPoolerConnection_StreamHealth_Callback verifies that the onHealthUpdate
// callback is invoked when health state changes from streaming responses.
func TestPoolerConnection_StreamHealth_Callback(t *testing.T) {
	var callbackCount atomic.Int32
	var mu sync.Mutex
	var lastCallbackConn *PoolerConnection

	callback := func(pc *PoolerConnection) {
		callbackCount.Add(1)
		mu.Lock()
		lastCallbackConn = pc
		mu.Unlock()
	}

	setup := setupStreamingTestWithCallback(t, t.Context(), callback)
	waitForStreamOpened(t, setup.server)

	// Send a SERVING response. The callback should fire.
	setup.server.responseCh <- makeHealthResponse(clustermetadatapb.PoolerServingStatus_SERVING)

	require.Eventually(t, func() bool {
		return callbackCount.Load() >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"callback should be invoked on health update")

	mu.Lock()
	assert.Equal(t, setup.conn, lastCallbackConn, "callback should receive the PoolerConnection")
	mu.Unlock()

	// Send another state change and verify callback fires again.
	prevCount := callbackCount.Load()
	setup.server.responseCh <- makeHealthResponse(clustermetadatapb.PoolerServingStatus_NOT_SERVING)

	require.Eventually(t, func() bool {
		return callbackCount.Load() > prevCount
	}, 2*time.Second, 10*time.Millisecond,
		"callback should be invoked on subsequent health updates")
}

// TestPoolerConnection_StreamHealth_PrimaryObservation verifies that
// PrimaryObservation data from the health stream is correctly stored in health state.
func TestPoolerConnection_StreamHealth_PrimaryObservation(t *testing.T) {
	setup := setupStreamingTest(t, t.Context())
	waitForStreamOpened(t, setup.server)

	// Send a response with PrimaryObservation.
	resp := makeHealthResponse(clustermetadatapb.PoolerServingStatus_SERVING)
	resp.PrimaryObservation = &multipoolerservice.PrimaryObservation{
		PrimaryId: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "primary-pooler",
		},
		PrimaryTerm: 42,
	}
	setup.server.responseCh <- resp

	require.Eventually(t, func() bool {
		return setup.conn.Health().IsServing()
	}, 2*time.Second, 10*time.Millisecond)

	health := setup.conn.Health()
	require.NotNil(t, health.PrimaryObservation)
	assert.Equal(t, int64(42), health.PrimaryObservation.PrimaryTerm)
	assert.Equal(t, "primary-pooler", health.PrimaryObservation.PrimaryId.GetName())
}
