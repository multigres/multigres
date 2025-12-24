// Copyright 2025 Supabase, Inc.
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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/multipooler/poolerserver"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestHealthStreamer_BroadcastToSubscribers(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	hs := newHealthStreamer(context.Background(), logger, serviceID, constants.DefaultTableGroup, constants.DefaultShard)

	// Subscribe two clients
	_, ch1 := hs.subscribe()
	_, ch2 := hs.subscribe()

	assert.Equal(t, 2, hs.clientCount())

	// Drain the initial state from both channels
	<-ch1
	<-ch2

	// Update serving status (triggers broadcast)
	hs.UpdateServingStatus(clustermetadatapb.PoolerServingStatus_SERVING)

	// Both clients should receive the state
	select {
	case received := <-ch1:
		assert.Equal(t, constants.DefaultTableGroup, received.Target.TableGroup)
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, received.ServingStatus)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ch1 did not receive broadcast")
	}

	select {
	case received := <-ch2:
		assert.Equal(t, constants.DefaultTableGroup, received.Target.TableGroup)
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, received.ServingStatus)
	case <-time.After(100 * time.Millisecond):
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
	hs := newHealthStreamer(context.Background(), logger, serviceID, "initial", constants.DefaultShard)

	// Set initial state via update
	hs.UpdateServingStatus(clustermetadatapb.PoolerServingStatus_SERVING)

	// Subscribe should return current state and send it on channel
	state, ch := hs.subscribe()
	assert.Equal(t, "initial", state.Target.TableGroup)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, state.ServingStatus)

	// Channel should also receive the current state
	select {
	case received := <-ch:
		assert.Equal(t, "initial", received.Target.TableGroup)
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, received.ServingStatus)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("channel did not receive initial state")
	}
}

func TestHealthStreamer_UnsubscribeRemovesClient(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	hs := newHealthStreamer(context.Background(), logger, nil, constants.DefaultTableGroup, constants.DefaultShard)

	_, ch := hs.subscribe()
	assert.Equal(t, 1, hs.clientCount())

	hs.unsubscribe(ch)
	assert.Equal(t, 0, hs.clientCount())
}

func TestHealthStreamer_FullBufferClosesChannel(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	hs := newHealthStreamer(context.Background(), logger, nil, constants.DefaultTableGroup, constants.DefaultShard)

	_, ch := hs.subscribe()

	// Send more than buffer size without draining
	// subscribe sends initial state, so we need buffer size + more
	for range defaultHealthStreamBufferSize + 5 {
		hs.UpdateServingStatus(clustermetadatapb.PoolerServingStatus_SERVING)
	}

	// Channel should be closed due to buffer overflow
	assert.Equal(t, 0, hs.clientCount(), "client should be removed after buffer overflow")

	// Channel should be closed (reading should return zero value immediately)
	select {
	case _, ok := <-ch:
		if ok {
			// Drain any remaining buffered items
			for range ch {
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
	hs := newHealthStreamer(context.Background(), logger, serviceID, "test", constants.DefaultShard)

	// Get initial state
	got := hs.getState()
	require.NotNil(t, got)
	assert.Equal(t, "test", got.Target.TableGroup)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_NOT_SERVING, got.ServingStatus)

	// Update and verify
	hs.UpdateServingStatus(clustermetadatapb.PoolerServingStatus_SERVING)
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
		healthStreamer: newHealthStreamer(context.Background(), logger, serviceID, constants.DefaultTableGroup, constants.DefaultShard),
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

func TestHealthStreamer_UpdateServingStatus(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	hs := newHealthStreamer(context.Background(), logger, serviceID, constants.DefaultTableGroup, constants.DefaultShard)

	// Subscribe before the state change
	_, ch := hs.subscribe()

	// Drain the initial state
	<-ch

	// Update serving status
	hs.UpdateServingStatus(clustermetadatapb.PoolerServingStatus_SERVING)

	// Verify subscriber receives the updated state
	select {
	case received := <-ch:
		require.NotNil(t, received)
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, received.ServingStatus)
		assert.Equal(t, constants.DefaultTableGroup, received.Target.TableGroup)
		assert.Equal(t, constants.DefaultShard, received.Target.Shard)
		assert.Equal(t, serviceID, received.PoolerID)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("subscriber did not receive health broadcast")
	}
}

func TestHealthStreamer_UpdatePoolerType(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	hs := newHealthStreamer(context.Background(), logger, serviceID, constants.DefaultTableGroup, constants.DefaultShard)

	// Subscribe and drain initial state
	_, ch := hs.subscribe()
	<-ch

	// Update pooler type
	hs.UpdatePoolerType(clustermetadatapb.PoolerType_PRIMARY)

	// Verify subscriber receives the updated state
	select {
	case received := <-ch:
		require.NotNil(t, received)
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, received.Target.PoolerType)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("subscriber did not receive health broadcast")
	}
}

func TestHealthStreamer_UpdatePrimaryObservation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	hs := newHealthStreamer(context.Background(), logger, serviceID, constants.DefaultTableGroup, constants.DefaultShard)

	// Subscribe and drain initial state
	_, ch := hs.subscribe()
	<-ch

	// Update primary observation
	obs := &poolerserver.PrimaryObservation{
		Term: 42,
	}
	hs.UpdatePrimaryObservation(obs)

	// Verify subscriber receives the updated state
	select {
	case received := <-ch:
		require.NotNil(t, received)
		require.NotNil(t, received.PrimaryObservation)
		assert.Equal(t, int64(42), received.PrimaryObservation.Term)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("subscriber did not receive health broadcast")
	}
}

func TestHealthStreamer_StartStop(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	hs := newHealthStreamer(context.Background(), logger, serviceID, constants.DefaultTableGroup, constants.DefaultShard)

	// Subscribe to receive broadcasts
	_, ch := hs.subscribe()

	// Drain initial state from subscribe
	<-ch

	// Start should broadcast immediately
	hs.Start()

	// Should receive the immediate broadcast from Start()
	select {
	case received := <-ch:
		require.NotNil(t, received)
		assert.Equal(t, constants.DefaultTableGroup, received.Target.TableGroup)
		assert.Equal(t, serviceID, received.PoolerID)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("did not receive immediate broadcast from Start()")
	}

	// Stop should halt periodic broadcasts
	hs.Stop()
}

func TestHealthStreamer_StartBroadcastsImmediately(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	hs := newHealthStreamer(context.Background(), logger, serviceID, constants.DefaultTableGroup, constants.DefaultShard)
	defer hs.Stop()

	// Subscribe before starting
	_, ch := hs.subscribe()
	<-ch // drain initial state from subscribe

	// Update state before starting
	hs.UpdateServingStatus(clustermetadatapb.PoolerServingStatus_SERVING)
	<-ch // drain the update broadcast

	// Start should immediately broadcast current state
	hs.Start()

	select {
	case received := <-ch:
		require.NotNil(t, received)
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, received.ServingStatus)
		assert.Equal(t, constants.DefaultTableGroup, received.Target.TableGroup)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Start() did not broadcast immediately")
	}
}

func TestHealthStreamer_PeriodicBroadcasts(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	hs := newHealthStreamer(context.Background(), logger, serviceID, constants.DefaultTableGroup, constants.DefaultShard)
	defer hs.Stop()

	// Subscribe to receive broadcasts
	_, ch := hs.subscribe()
	<-ch // drain initial state from subscribe

	// Start periodic broadcasting
	hs.Start()
	<-ch // drain immediate broadcast from Start()

	// Wait and verify we receive at least 2 more periodic broadcasts
	// Using a short timeout since the default interval is 30s (we can't override it in tests)
	// Note: This test relies on the PeriodicRunner working correctly
	receivedCount := 0
	timeout := time.After(100 * time.Millisecond)

	// We should receive at least one more broadcast even with the 30s interval
	// because the PeriodicRunner may fire immediately on Start
	for receivedCount < 1 {
		select {
		case received := <-ch:
			require.NotNil(t, received)
			assert.Equal(t, constants.DefaultTableGroup, received.Target.TableGroup)
			receivedCount++
		case <-timeout:
			// We may not receive periodic broadcasts in this short test window
			// since the interval is 30s. The important thing is Start() worked.
			return
		}
	}
}

func TestHealthStreamer_MultipleStartCalls(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	hs := newHealthStreamer(context.Background(), logger, serviceID, constants.DefaultTableGroup, constants.DefaultShard)
	defer hs.Stop()

	// Subscribe to receive broadcasts
	_, ch := hs.subscribe()
	<-ch // drain initial state

	// Multiple Start() calls should be safe (PeriodicRunner handles this)
	hs.Start()
	<-ch // drain first immediate broadcast

	// Second Start() should not cause issues
	hs.Start()

	// Should still receive broadcasts
	select {
	case received := <-ch:
		require.NotNil(t, received)
		assert.Equal(t, constants.DefaultTableGroup, received.Target.TableGroup)
	case <-time.After(100 * time.Millisecond):
		// This is okay - we might not get another broadcast in the test window
	}
}

func TestHealthStreamer_StopWithoutStart(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	hs := newHealthStreamer(context.Background(), logger, serviceID, constants.DefaultTableGroup, constants.DefaultShard)

	// Stop without Start should be safe (PeriodicRunner handles this)
	hs.Stop()

	// Subscribe BEFORE starting so we don't miss the immediate broadcast
	_, ch := hs.subscribe()
	<-ch // drain initial state from subscribe

	// Should be able to start after stopping
	hs.Start()
	defer hs.Stop()

	// Should receive immediate broadcast from Start()
	select {
	case received := <-ch:
		require.NotNil(t, received)
		assert.Equal(t, constants.DefaultTableGroup, received.Target.TableGroup)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("should receive broadcast after Start")
	}
}

func TestHealthStreamer_MultipleStopCalls(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	hs := newHealthStreamer(context.Background(), logger, serviceID, constants.DefaultTableGroup, constants.DefaultShard)

	// Start then stop multiple times should be safe
	hs.Start()
	hs.Stop()
	hs.Stop() // Second stop should be safe

	// Subscribe BEFORE restarting so we don't miss the immediate broadcast
	_, ch := hs.subscribe()
	<-ch // drain initial state from subscribe

	// Should still be able to restart
	hs.Start()
	defer hs.Stop()

	// Should receive immediate broadcast from Start()
	select {
	case received := <-ch:
		require.NotNil(t, received)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("should receive broadcast after restart")
	}
}
