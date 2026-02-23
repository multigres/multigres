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
	"sync"
	"time"

	"github.com/multigres/multigres/go/multipooler/poolerserver"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/tools/timer"
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

	// broadcaster manages periodic broadcasting of health state
	broadcaster *timer.PeriodicRunner
}

// newHealthStreamer creates a new health streamer with the given identity.
func newHealthStreamer(ctx context.Context, logger *slog.Logger, poolerID *clustermetadatapb.ID, tableGroup, shard string) *healthStreamer {
	broadcaster := timer.NewPeriodicRunner(ctx, defaultHealthHeartbeatInterval)
	return &healthStreamer{
		logger:                      logger,
		poolerID:                    poolerID,
		tableGroup:                  tableGroup,
		shard:                       shard,
		clients:                     make(map[chan *poolerserver.HealthState]struct{}),
		recommendedStalenessTimeout: defaultRecommendedStalenessTimeout,
		servingStatus:               clustermetadatapb.PoolerServingStatus_NOT_SERVING,
		broadcaster:                 broadcaster,
	}
}

// UpdateServingStatus updates the serving status and broadcasts to clients.
func (hs *healthStreamer) UpdateServingStatus(status clustermetadatapb.PoolerServingStatus) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.servingStatus = status
	hs.broadcastLocked()
}

// UpdatePoolerType updates the pooler type and broadcasts to clients.
func (hs *healthStreamer) UpdatePoolerType(poolerType clustermetadatapb.PoolerType) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.poolerType = poolerType
	hs.broadcastLocked()
}

// UpdatePrimaryObservation updates the primary observation (term + primary ID)
// and broadcasts to clients.
func (hs *healthStreamer) UpdatePrimaryObservation(obs *poolerserver.PrimaryObservation) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.primaryObservation = obs
	hs.broadcastLocked()
}

// TODO: Consider adding a ChangeState(servingStatus, poolerType, primaryObs) method
// that updates multiple fields atomically with a single broadcast, similar to Vitess's
// healthStreamer.ChangeState(). This would avoid multiple broadcasts when several
// fields change together (e.g., during promotion).

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

	// Send current state immediately
	state := hs.buildStateLocked()
	ch <- state

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

// Start begins periodic broadcasting of health state.
// Broadcasts immediately, then periodically at the configured interval.
// Should be called after the manager is opened.
func (hs *healthStreamer) Start() {
	hs.broadcaster.Start(func(ctx context.Context) {
		hs.Broadcast()
	}, nil)
	// Send initial state immediately to all subscribers
	hs.Broadcast()
}

// Stop halts periodic broadcasting of health state.
// Should be called when the manager is closing.
func (hs *healthStreamer) Stop() {
	hs.broadcaster.Stop()
}
