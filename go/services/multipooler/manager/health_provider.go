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
	"sync"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/poolerserver"
)

const (
	// defaultHealthStreamBufferSize is the number of health updates that can be
	// buffered per client before we close the channel.
	defaultHealthStreamBufferSize = 20

	// defaultRecommendedStalenessTimeout is the duration clients should use
	// to detect a stale/dead health stream.
	defaultRecommendedStalenessTimeout = 90 * time.Second

	// defaultHealthHeartbeatInterval is the interval between periodic health
	// broadcasts when no state changes occur.
	defaultHealthHeartbeatInterval = 30 * time.Second
)

// healthStreamer streams health information to subscribers.
// It owns all health-related state and provides typed update methods
// that atomically update state and broadcast to clients.
// Following the Vitess healthStreamer pattern.
type healthStreamer struct {
	logger *slog.Logger

	mu sync.Mutex

	// queryServer, if set, is waited on before broadcasting SERVING
	// transitions. This ensures the query server has updated its type
	// before the gateway discovers the new state.
	queryServer poolerserver.PoolerController

	// Immutable fields (set once via Init)
	poolerID   *clustermetadatapb.ID
	tableGroup string
	shard      string

	// Mutable fields (updated via typed methods)
	servingStatus      clustermetadatapb.PoolerServingStatus
	poolerType         clustermetadatapb.PoolerType
	primaryObservation *poolerserver.PrimaryObservation

	// Client management
	clients map[chan *poolerserver.HealthState]struct{}

	// recommendedStalenessTimeout is advertised to clients
	recommendedStalenessTimeout time.Duration
}

// newHealthStreamer creates a new health streamer with the given identity.
func newHealthStreamer(logger *slog.Logger, poolerID *clustermetadatapb.ID, tableGroup, shard string) *healthStreamer {
	return &healthStreamer{
		logger:                      logger,
		poolerID:                    poolerID,
		tableGroup:                  tableGroup,
		shard:                       shard,
		clients:                     make(map[chan *poolerserver.HealthState]struct{}),
		recommendedStalenessTimeout: defaultRecommendedStalenessTimeout,
		servingStatus:               clustermetadatapb.PoolerServingStatus_NOT_SERVING,
	}
}

// SetQueryServer sets the query server that the healthStreamer waits on before
// broadcasting SERVING transitions. Must be called before any state transitions.
func (hs *healthStreamer) SetQueryServer(qs poolerserver.PoolerController) {
	hs.queryServer = qs
}

// UpdatePrimaryObservation updates the primary observation (term + primary ID)
// and broadcasts to clients.
func (hs *healthStreamer) UpdatePrimaryObservation(obs *poolerserver.PrimaryObservation) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.primaryObservation = obs
	hs.broadcastLocked()
}

// OnStateChange updates both poolerType and servingStatus atomically with a single
// broadcast. This implements the StateAware interface so the healthStreamer can be
// registered with StateManager.
//
// For SERVING transitions, it waits for the query server (via queryReadyGate)
// to finish updating before broadcasting. This prevents the gateway from
// discovering the new primary before the pooler can actually serve that type.
// NOT_SERVING transitions broadcast immediately so the gateway can start
// buffering without delay.
func (hs *healthStreamer) OnStateChange(ctx context.Context, poolerType clustermetadatapb.PoolerType, servingStatus clustermetadatapb.PoolerServingStatus) error {
	if servingStatus == clustermetadatapb.PoolerServingStatus_SERVING && hs.queryServer != nil {
		hs.queryServer.AwaitStateChange(ctx, poolerType, servingStatus)
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.poolerType = poolerType
	hs.servingStatus = servingStatus
	hs.broadcastLocked()
	return nil
}

// Broadcast sends the current state to all clients without changing any state.
// Used for periodic heartbeats.
func (hs *healthStreamer) Broadcast() {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.broadcastLocked()
}

// buildStateLocked builds the current health state. Caller must hold hs.mu.
func (hs *healthStreamer) buildStateLocked() *poolerserver.HealthState {
	return &poolerserver.HealthState{
		Target: &querypb.Target{
			TableGroup: hs.tableGroup,
			Shard:      hs.shard,
			PoolerType: hs.poolerType,
		},
		PoolerID:                    hs.poolerID,
		ServingStatus:               hs.servingStatus,
		PrimaryObservation:          hs.primaryObservation,
		RecommendedStalenessTimeout: hs.recommendedStalenessTimeout,
	}
}

// broadcastLocked sends the current health state to all registered clients.
// If a client's buffer is full, closes the channel to force reconnect.
// Caller must hold hs.mu.
func (hs *healthStreamer) broadcastLocked() {
	state := hs.buildStateLocked()

	for ch := range hs.clients {
		select {
		case ch <- state:
		default:
			// Buffer full - close the channel to force client reconnect.
			// This ensures clients don't operate on stale state indefinitely.
			// See Vitess healthStreamer for rationale.
			hs.logger.Warn("Health stream buffer full, closing channel to force reconnect")
			close(ch)
			delete(hs.clients, ch)
		}
	}
}

// getState returns the current health state.
func (hs *healthStreamer) getState() *poolerserver.HealthState {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	return hs.buildStateLocked()
}

// subscribe registers a new client for health updates.
// Returns the current state and a channel that receives updates.
func (hs *healthStreamer) subscribe() (*poolerserver.HealthState, chan *poolerserver.HealthState) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	ch := make(chan *poolerserver.HealthState, defaultHealthStreamBufferSize)
	hs.clients[ch] = struct{}{}

	state := hs.buildStateLocked()
	return state, ch
}

// unsubscribe removes a client from health updates.
func (hs *healthStreamer) unsubscribe(ch chan *poolerserver.HealthState) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	delete(hs.clients, ch)
}

// clientCount returns the number of active streaming clients.
func (hs *healthStreamer) clientCount() int {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	return len(hs.clients)
}

// HealthProvider implementation for MultiPoolerManager

// GetHealthState returns the current health state of the pooler.
// Implements poolerserver.HealthProvider.
func (pm *MultiPoolerManager) GetHealthState(ctx context.Context) (*poolerserver.HealthState, error) {
	if pm.healthStreamer == nil {
		return nil, nil
	}
	return pm.healthStreamer.getState(), nil
}

// SubscribeHealth subscribes to health state changes.
// Returns the current health state and a channel that receives updates.
// The channel is closed when the context is cancelled or if the client
// falls too far behind (buffer full).
// Implements poolerserver.HealthProvider.
func (pm *MultiPoolerManager) SubscribeHealth(ctx context.Context) (*poolerserver.HealthState, <-chan *poolerserver.HealthState, error) {
	if pm.healthStreamer == nil {
		return nil, nil, nil
	}

	state, ch := pm.healthStreamer.subscribe()

	// Start a goroutine to clean up when context is cancelled
	go func() {
		<-ctx.Done()
		pm.healthStreamer.unsubscribe(ch)
	}()

	return state, ch, nil
}

// runHealthHeartbeat runs the periodic health heartbeat loop.
// It broadcasts the current health state at the specified interval.
// This should be started as a goroutine when the manager opens.
func (pm *MultiPoolerManager) runHealthHeartbeat(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if pm.healthStreamer != nil {
				pm.healthStreamer.Broadcast()
			}
		}
	}
}
