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
	"sync/atomic"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

const (
	// defaultHealthStreamBufferSize is the number of health updates that can be
	// buffered per client before we close the channel.
	defaultHealthStreamBufferSize = 20

	// defaultRecommendedStalenessTimeout is the duration clients should use
	// to detect a stale/dead health stream.
	defaultRecommendedStalenessTimeout = 90 * time.Second
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
	servingStatus     clustermetadatapb.PoolerServingStatus
	poolerType        clustermetadatapb.PoolerType
	leaderObservation *poolerserver.LeaderObservation

	// writable reports whether postgres can accept writes (!pg_is_in_recovery).
	// Published separately from poolerType (which tracks the consensus term, not
	// writability) so the gateway can hold write traffic for a leader that is
	// SERVING but still mid-promotion.
	writable bool

	// Client management
	clients map[chan *poolerserver.HealthState]struct{}

	// recommendedStalenessTimeout is advertised to clients
	recommendedStalenessTimeout time.Duration

	// replicationLagNs holds the most recent replication lag in nanoseconds.
	// Zero on the primary or when not yet measured. Updated via SetReplicationLag.
	replicationLagNs atomic.Int64

	// metrics publishes replication lag and serving-state transitions as OTel
	// metrics. Always non-nil after newHealthStreamer.
	metrics *healthMetrics
}

// newHealthStreamer creates a new health streamer with the given identity.
func newHealthStreamer(logger *slog.Logger, poolerID *clustermetadatapb.ID, tableGroup, shard string) *healthStreamer {
	hs := &healthStreamer{
		logger:                      logger,
		poolerID:                    poolerID,
		tableGroup:                  tableGroup,
		shard:                       shard,
		clients:                     make(map[chan *poolerserver.HealthState]struct{}),
		recommendedStalenessTimeout: defaultRecommendedStalenessTimeout,
		servingStatus:               clustermetadatapb.PoolerServingStatus_DISABLED,
	}

	// The observable gauge samples the lag atomic at collection time.
	metrics, err := newHealthMetrics(func() int64 { return hs.replicationLagNs.Load() })
	if err != nil && logger != nil {
		logger.Warn("failed to initialise some health metrics", "error", err)
	}
	hs.metrics = metrics

	return hs
}

// SetQueryServer sets the query server that the healthStreamer waits on before
// broadcasting SERVING transitions. Must be called before any state transitions.
func (hs *healthStreamer) SetQueryServer(qs poolerserver.PoolerController) {
	hs.queryServer = qs
}

// UpdateLeaderObservation updates the primary observation (term + primary ID)
// and broadcasts to clients.
func (hs *healthStreamer) UpdateLeaderObservation(obs *poolerserver.LeaderObservation) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.leaderObservation = obs
	hs.broadcastLocked()
}

// OnStateChange updates both poolerType and servingStatus atomically with a single
// broadcast. This implements the StateAware interface so the healthStreamer can be
// registered with StateManager.
//
// For SERVING transitions, it waits for the query server (via queryReadyGate)
// to finish updating before broadcasting. This prevents the gateway from
// discovering the new primary before the pooler can actually serve that type.
// not-serving transitions broadcast immediately so the gateway can start
// buffering without delay.
func (hs *healthStreamer) OnStateChange(ctx context.Context, state servingstate.State) error {
	if state.ServingStatus == clustermetadatapb.PoolerServingStatus_SERVING && hs.queryServer != nil {
		hs.queryServer.AwaitStateChange(ctx, state.RoutingRole, state.ServingStatus)
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	prev := hs.servingStatus
	// The health stream reports the routing role to the gateway as a PoolerType
	// label plus the writable flag. Both derive from the routing role: PRIMARY (and
	// writable) iff this pooler is the writable leader, REPLICA otherwise. A leader
	// that has not finished promoting is not yet routing PRIMARY, so the gateway
	// does not route writes to it until its rule commits.
	hs.poolerType = poolerTypeForLeader(state.RoutingRole.Writable())
	hs.writable = state.RoutingRole.Writable()
	hs.servingStatus = state.ServingStatus
	hs.broadcastLocked()
	if prev != state.ServingStatus {
		hs.metrics.recordTransition(ctx, prev, state.ServingStatus)
	}
	return nil
}

// poolerTypeForLeader maps a writable-leader boolean to the PoolerType label
// (published on the health stream and persisted to the topology record).
func poolerTypeForLeader(writableLeader bool) clustermetadatapb.PoolerType {
	if writableLeader {
		return clustermetadatapb.PoolerType_PRIMARY
	}
	return clustermetadatapb.PoolerType_REPLICA
}

// Broadcast sends the current state to all clients without changing any state.
// Used for periodic heartbeats.
func (hs *healthStreamer) Broadcast() {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.broadcastLocked()
}

// SetReplicationLag updates the replication lag reported in the health stream.
// Called by the manager's heartbeat loop with the latest measured lag.
// Safe to call concurrently with any method.
func (hs *healthStreamer) SetReplicationLag(lagNs int64) {
	hs.replicationLagNs.Store(lagNs)
}

// buildStateLocked builds the current health state. Caller must hold hs.mu.
func (hs *healthStreamer) buildStateLocked() *poolerserver.HealthState {
	return &poolerserver.HealthState{
		PoolerID:                    hs.poolerID,
		ServingStatus:               hs.servingStatus,
		LeaderObservation:           hs.leaderObservation,
		RecommendedStalenessTimeout: hs.recommendedStalenessTimeout,
		ReplicationLagNs:            hs.replicationLagNs.Load(),
		Writable:                    hs.writable,
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
			// If the buffer is full, the channel is closed to force client
			// reconnect. This ensures clients don't operate on stale state
			// indefinitely. This can happen if the client is too slow to
			// process updates or if there are too many updates in a short time
			// (e.g. due to flapping). The client should reconnect and receive
			// the latest state.
			//
			// TODO: consider adding a metric for this to detect if clients are
			// falling behind frequently.
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

// unsubscribe removes a client from health updates and closes the channel so
// the consumer's receive returns `ok=false`. Idempotent: if the channel was
// already removed (e.g. by broadcastLocked's buffer-full path, which also
// closes), the second call is a no-op.
func (hs *healthStreamer) unsubscribe(ch chan *poolerserver.HealthState) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if _, ok := hs.clients[ch]; !ok {
		return
	}
	delete(hs.clients, ch)
	close(ch)
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

	// Clean up on either:
	//   - the caller's ctx ending (gRPC stream finished, client disconnected,
	//     RPC cancelled), or
	//   - the manager's shutdownCtx firing at the end of GracefulShutdown
	//     (forces in-flight stream handlers to return so grpcServer.GracefulStop
	//     can complete without waiting for them).
	//
	// shutdownDone is nil for tests that bypass NewMultiPoolerManager; a
	// receive on a nil channel blocks forever, so the select degrades cleanly
	// to "wait on caller ctx only."
	var shutdownDone <-chan struct{}
	if pm.shutdownCtx != nil {
		shutdownDone = pm.shutdownCtx.Done()
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-shutdownDone:
		}
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
			// Refresh replication lag before broadcasting so clients see
			// up-to-date lag without requiring a separate state-change event.
			if pm.healthStreamer != nil {
				if lag, err := pm.ReplicationLag(ctx); err == nil {
					pm.healthStreamer.SetReplicationLag(lag.Nanoseconds())
				}
			}
			pm.broadcastHealth()
		}
	}
}
