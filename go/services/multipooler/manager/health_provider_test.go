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

package manager

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/poolerserver"
)

func TestHealthStreamer_BroadcastToSubscribers(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	hs := newHealthStreamer(logger, serviceID, "tg1", "0")

	// Subscribe two clients
	_, ch1 := hs.subscribe()
	_, ch2 := hs.subscribe()

	assert.Equal(t, 2, hs.clientCount())

	// Update state (triggers broadcast)
	require.NoError(t, hs.OnStateChange(context.Background(), clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))

	// Both clients should receive the state
	timeout1 := time.After(100 * time.Millisecond)
	timeout2 := time.After(100 * time.Millisecond)

	select {
	case received := <-ch1:
		assert.Equal(t, "tg1", received.Target.TableGroup)
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, received.ServingStatus)
	case <-timeout1:
		t.Fatal("ch1 did not receive broadcast")
	}

	select {
	case received := <-ch2:
		assert.Equal(t, "tg1", received.Target.TableGroup)
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, received.ServingStatus)
	case <-timeout2:
		t.Fatal("ch2 did not receive broadcast")
	}
}

func TestHealthStreamer_SubscribeReceivesCurrentState(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	hs := newHealthStreamer(logger, serviceID, "initial", "0")

	// Set initial state via OnStateChange
	require.NoError(t, hs.OnStateChange(context.Background(), clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))

	// Subscribe should return current state
	state, _ := hs.subscribe()
	assert.Equal(t, "initial", state.Target.TableGroup)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, state.ServingStatus)
}

func TestHealthStreamer_UnsubscribeRemovesClient(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	hs := newHealthStreamer(logger, nil, "tg1", "0")

	_, ch := hs.subscribe()
	assert.Equal(t, 1, hs.clientCount())

	hs.unsubscribe(ch)
	assert.Equal(t, 0, hs.clientCount())
}

func TestHealthStreamer_FullBufferClosesChannel(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	hs := newHealthStreamer(logger, nil, "tg1", "0")

	_, ch := hs.subscribe()

	// Send more than buffer size without draining
	for range defaultHealthStreamBufferSize + 5 {
		require.NoError(t, hs.OnStateChange(context.Background(), clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
	}

	// Channel should be closed due to buffer overflow
	assert.Equal(t, 0, hs.clientCount(), "client should be removed after buffer overflow")

	// Channel should be closed (reading should return zero value immediately)
	select {
	case _, ok := <-ch:
		if ok {
			// Drain any remaining buffered items
			for range ch {
				if t.Context().Err() != nil {
					t.Fatal("test context cancelled while draining channel")
				}
			}
		}
		// Channel is closed, which is expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("channel should be closed or have items")
	}
}

func TestHealthStreamer_GetState(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	hs := newHealthStreamer(logger, serviceID, "test", "0")

	// Get initial state
	got := hs.getState()
	require.NotNil(t, got)
	assert.Equal(t, "test", got.Target.TableGroup)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_NOT_SERVING, got.ServingStatus)

	// Update and verify
	require.NoError(t, hs.OnStateChange(context.Background(), clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING))
	got = hs.getState()
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, got.ServingStatus)
}

func TestHealthProvider_SubscribeWithContextCancellation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	// Create a minimal manager with healthStreamer
	pm := &MultiPoolerManager{
		logger:         logger,
		healthStreamer: newHealthStreamer(logger, serviceID, "tg1", "0"),
	}

	ctx, cancel := context.WithCancel(t.Context())

	_, ch, err := pm.SubscribeHealth(ctx)
	require.NoError(t, err)
	require.NotNil(t, ch)

	assert.Equal(t, 1, pm.healthStreamer.clientCount())

	// Cancel context
	cancel()

	// Give the goroutine time to unsubscribe
	require.Eventually(t, func() bool {
		return pm.healthStreamer.clientCount() == 0
	}, 100*time.Millisecond, 10*time.Millisecond, "client should be unsubscribed after context cancellation")
}

func TestHealthProvider_GetHealthStateReturnsNilWhenNoStreamer(t *testing.T) {
	pm := &MultiPoolerManager{
		healthStreamer: nil,
	}

	state, err := pm.GetHealthState(t.Context())
	assert.NoError(t, err)
	assert.Nil(t, state)
}

func TestHealthProvider_SubscribeHealthReturnsNilWhenNoStreamer(t *testing.T) {
	pm := &MultiPoolerManager{
		healthStreamer: nil,
	}

	state, ch, err := pm.SubscribeHealth(t.Context())
	assert.NoError(t, err)
	assert.Nil(t, state)
	assert.Nil(t, ch)
}

func TestHealthStreamer_UpdateLeaderObservation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	hs := newHealthStreamer(logger, serviceID, "tg1", "0")

	// Subscribe
	_, ch := hs.subscribe()

	// Update primary observation
	obs := &poolerserver.LeaderObservation{
		LeaderTerm: 42,
	}
	hs.UpdateLeaderObservation(obs)

	// Verify subscriber receives the updated state
	select {
	case received := <-ch:
		require.NotNil(t, received)
		require.NotNil(t, received.LeaderObservation)
		assert.Equal(t, int64(42), received.LeaderObservation.LeaderTerm)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("subscriber did not receive health broadcast")
	}
}

func TestHealthStreamer_OnStateChange(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	hs := newHealthStreamer(logger, serviceID, "tg1", "0")

	// Subscribe before the state change
	_, ch := hs.subscribe()

	// Call OnStateChange — updates both fields atomically with one broadcast
	err := hs.OnStateChange(context.Background(), clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
	require.NoError(t, err)

	// Verify subscriber receives a single broadcast with both fields updated
	select {
	case received := <-ch:
		require.NotNil(t, received)
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, received.Target.PoolerType)
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, received.ServingStatus)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("subscriber did not receive health broadcast")
	}

	// Verify getState reflects both changes
	state := hs.getState()
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, state.Target.PoolerType)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, state.ServingStatus)

	// Verify no extra broadcast was sent (only one message in channel)
	select {
	case <-ch:
		t.Fatal("unexpected extra broadcast")
	default:
		// Good — only one broadcast
	}
}

func TestHealthHeartbeat_BroadcastsPeriodically(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	pm := &MultiPoolerManager{
		logger:         logger,
		healthStreamer: newHealthStreamer(logger, serviceID, "tg1", "0"),
	}

	// Subscribe to get heartbeat broadcasts
	_, ch, err := pm.SubscribeHealth(t.Context())
	require.NoError(t, err)

	// Create a context that we'll cancel to stop the heartbeat loop
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Start the heartbeat with a short interval for testing
	testInterval := 10 * time.Millisecond
	go pm.runHealthHeartbeat(ctx, testInterval)

	// We should receive at least one heartbeat within a reasonable time
	select {
	case received := <-ch:
		require.NotNil(t, received)
		assert.Equal(t, "tg1", received.Target.TableGroup)
		assert.Equal(t, serviceID, received.PoolerID)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("did not receive heartbeat broadcast within expected interval")
	}
}

// TestHealthStreamer_WaitsForQueryServerOnServing verifies that the health
// streamer waits for the query server to complete its SERVING transition
// before broadcasting the new state to subscribers.
func TestHealthStreamer_WaitsForQueryServerOnServing(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	hs := newHealthStreamer(logger, nil, "tg1", "0")

	// Create a real QueryPoolerServer to use as the gate.
	qps := poolerserver.NewQueryPoolerServer(logger, nil, nil, "", "", nil, 0)
	hs.SetQueryServer(qps)

	// Subscribe to health updates.
	ch := make(chan *poolerserver.HealthState, 10)
	hs.clients[ch] = struct{}{}

	// Start hs.OnStateChange(PRIMARY, SERVING) in background.
	// It should block because qps hasn't transitioned yet.
	hsDone := make(chan struct{})
	go func() {
		_ = hs.OnStateChange(t.Context(), clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
		close(hsDone)
	}()

	// Health streamer should NOT have broadcast yet.
	select {
	case <-ch:
		t.Fatal("healthStreamer should not broadcast before query server is ready")
	case <-time.After(50 * time.Millisecond):
	}

	// Now transition the query server.
	require.NoError(t, qps.OnStateChange(t.Context(), clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))

	// Health streamer should unblock and broadcast.
	select {
	case <-hsDone:
	case <-time.After(time.Second):
		t.Fatal("healthStreamer should unblock after query server transitions")
	}

	// Verify the broadcast was sent.
	select {
	case state := <-ch:
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, state.Target.PoolerType)
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, state.ServingStatus)
	default:
		t.Fatal("expected a health broadcast after transition")
	}
}

// TestHealthStreamer_DoesNotWaitOnNotServing verifies that NOT_SERVING
// transitions broadcast immediately without waiting for the query server.
func TestHealthStreamer_DoesNotWaitOnNotServing(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	hs := newHealthStreamer(logger, nil, "tg1", "0")

	// Create a query server that is PRIMARY/SERVING.
	qps := poolerserver.NewQueryPoolerServer(logger, nil, nil, "", "", nil, 0)
	require.NoError(t, qps.OnStateChange(t.Context(), clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING))
	hs.SetQueryServer(qps)

	ch := make(chan *poolerserver.HealthState, 10)
	hs.clients[ch] = struct{}{}

	// NOT_SERVING should broadcast immediately, even though qps is still PRIMARY/SERVING.
	hsDone := make(chan struct{})
	go func() {
		_ = hs.OnStateChange(t.Context(), clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_NOT_SERVING)
		close(hsDone)
	}()

	select {
	case <-hsDone:
	case <-time.After(time.Second):
		t.Fatal("NOT_SERVING transition should not wait for query server")
	}
}
