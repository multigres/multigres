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

package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
	"github.com/multigres/multigres/go/tools/retry"
)

const (
	// streamReconnectInitialBackoff is the initial wait before retrying a
	// failed ManagerHealthStream connection.
	streamReconnectInitialBackoff = 1 * time.Second

	// streamReconnectMaxBackoff caps the exponential backoff between retries.
	streamReconnectMaxBackoff = 30 * time.Second

	// streamStalenessTimeout is the default maximum time to wait for a health
	// snapshot before treating the stream as stale and reconnecting.
	//
	// This is an application-level watchdog that catches the "server goroutine
	// stuck but TCP alive" failure mode that gRPC keepalive does not cover.
	// gRPC keepalive (10s ping / 10s timeout) handles dead TCP connections;
	// this constant handles a live connection whose send loop has deadlocked.
	//
	// The server guarantees a snapshot at least every managerHealthPollingInterval
	// (5s), so messages normally arrive far more frequently than this. The value
	// is seeded from the server's recommended timeout in each snapshot.
	streamStalenessTimeout = 90 * time.Second
)

// streamEntry holds the lifecycle handles for one active stream goroutine.
type streamEntry struct {
	cancel context.CancelFunc

	// mu protects stream; set to the live gRPC stream after the start message
	// is sent, and cleared on stream exit.
	mu     sync.Mutex
	stream rpcclient.ManagerHealthStream
}

// HealthStream maintains one HealthStream stream per pooler. It replaces the
// polling loop: instead of periodically calling the Status RPC, each pooler
// pushes health snapshots to multiorch via a long-lived gRPC stream.
//
// When a snapshot arrives the HealthStream writes the same health fields into
// the pooler store that the old pollPooler function wrote on success. On stream
// disconnect the pooler is marked unreachable and reconnection is attempted
// with exponential backoff (1s → 2s → … → 30s cap).
type HealthStream struct {
	logger    *slog.Logger
	rpcClient rpcclient.MultiPoolerClient
	store     *store.PoolerStore

	// stalenessTimeout overrides streamStalenessTimeout if positive.
	// Set via WithStalenessTimeout; used in tests.
	stalenessTimeout time.Duration

	// Active stream goroutines, keyed by pooler ID string.
	mu      sync.Mutex
	streams map[string]*streamEntry

	// Parent context; cancelled by Stop().
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Option is a functional option for NewHealthStream.
type Option func(*HealthStream)

// WithStalenessTimeout overrides the default staleness timeout. Intended for tests.
func WithStalenessTimeout(d time.Duration) Option {
	return func(hs *HealthStream) {
		hs.stalenessTimeout = d
	}
}

// NewHealthStream creates a HealthStream.
//
// Call Start() for each pooler that should be monitored.
func NewHealthStream(
	ctx context.Context,
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	logger *slog.Logger,
	options ...Option,
) *HealthStream {
	smCtx, cancel := context.WithCancel(ctx)
	hs := &HealthStream{
		logger:    logger,
		rpcClient: rpcClient,
		store:     poolerStore,
		streams:   make(map[string]*streamEntry),
		ctx:       smCtx,
		cancel:    cancel,
	}

	for _, opt := range options {
		opt(hs)
	}

	return hs
}

// Shutdown cancels all active streams and waits for their goroutines to exit.
func (hs *HealthStream) Shutdown() {
	hs.cancel()
	hs.wg.Wait()
}

// Start starts a health stream for poolerID.
// If a stream is already running for this pooler the call is a no-op.
// The pooler's MultiPooler metadata is read from the store on each
// reconnect attempt so topology updates are automatically picked up.
func (hs *HealthStream) Start(poolerID string) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if _, exists := hs.streams[poolerID]; exists {
		return
	}

	ctx, cancel := context.WithCancel(hs.ctx)
	entry := &streamEntry{cancel: cancel}
	hs.streams[poolerID] = entry

	hs.wg.Go(func() {
		defer func() {
			hs.mu.Lock()
			delete(hs.streams, poolerID)
			hs.mu.Unlock()
		}()
		hs.runStream(ctx, poolerID, entry)
	})
}

// Stop the health stream for a pooler.
//
// The stream goroutine will exit and the pooler will be marked unreachable
// until a new stream is started. The pooler's MultiPooler metadata must remain
// in the store for the stream to reconnect if Start() is called again.
//
// If no stream is running for this pooler the call is a no-op.
func (hs *HealthStream) Stop(poolerID string) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if entry, exists := hs.streams[poolerID]; exists {
		entry.cancel()
		// The goroutine removes itself from hs.streams via its defer.
	}
}

// runStream manages the lifecycle of one stream, reconnecting with backoff on failure.
// It reads the latest MultiPooler metadata from the store on each reconnect attempt
// so hostname/port changes are picked up automatically.
func (hs *HealthStream) runStream(ctx context.Context, poolerID string, entry *streamEntry) {
	r := retry.New(streamReconnectInitialBackoff, streamReconnectMaxBackoff, retry.WithInitialDelay())
	for _, err := range r.Attempts(ctx) {
		if err != nil {
			return
		}

		// Read current pooler metadata from store on every attempt.
		poolerHealth, ok := hs.store.Get(poolerID)
		if !ok || poolerHealth.MultiPooler == nil {
			hs.logger.WarnContext(ctx, "pooler not found in store, stopping health stream",
				"pooler_id", poolerID)
			return
		}

		connected, streamErr := hs.streamOnce(ctx, poolerID, poolerHealth, entry)
		if ctx.Err() != nil {
			return
		}

		if connected {
			// Stream was successfully established before failing — reset backoff.
			r.Reset()
		}

		hs.markDisconnected(poolerID)

		if streamErr != nil {
			hs.logger.WarnContext(ctx, "health stream disconnected",
				"pooler_id", poolerID,
				"error", streamErr,
			)
		}
	}
}

// streamOnce opens one HealthStream stream and reads until the stream fails or
// the context is cancelled. Returns (connected, err): connected is true if the
// stream was established before any error occurred.
func (hs *HealthStream) streamOnce(ctx context.Context, poolerID string, poolerHealth *multiorchdatapb.PoolerHealthState, entry *streamEntry) (connected bool, _ error) {
	// Staleness watchdog: cancel the stream if no snapshot arrives within the
	// timeout. This catches the "server goroutine stuck but TCP alive" failure
	// mode that gRPC keepalive does not cover.
	//
	// The watchdog context is what we pass to ManagerHealthStream; cancelling it
	// terminates the gRPC stream and causes stream.Recv() to return an error.
	watchdogCtx, cancelWatchdog := context.WithCancel(ctx)
	defer cancelWatchdog()

	// Resolve the effective staleness timeout (WithStalenessTimeout overrides the default).
	effectiveStalenessTimeout := streamStalenessTimeout
	if hs.stalenessTimeout > 0 {
		effectiveStalenessTimeout = hs.stalenessTimeout
	}

	// resetCh is used by the recv loop to reset the staleness timer. The
	// duration is taken from each snapshot's recommended timeout.
	resetCh := make(chan time.Duration, 1)
	go func() {
		timer := time.NewTimer(effectiveStalenessTimeout)
		defer timer.Stop()
		for {
			select {
			case d := <-resetCh:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(d)
			case <-timer.C:
				hs.logger.WarnContext(ctx, "health stream stale: no snapshot received within timeout, reconnecting",
					"pooler_id", poolerID,
					"timeout", effectiveStalenessTimeout,
				)
				cancelWatchdog()
				return
			case <-watchdogCtx.Done():
				return
			}
		}
	}()

	stream, err := hs.rpcClient.ManagerHealthStream(watchdogCtx, poolerHealth.MultiPooler)
	if err != nil {
		return false, fmt.Errorf("open stream: %w", err)
	}

	// Send the start message so the server starts sending snapshots.
	if err := stream.Send(&multipoolermanagerdatapb.ManagerHealthStreamClientMessage{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamClientMessage_Start{
			Start: &multipoolermanagerdatapb.ManagerHealthStreamStartRequest{},
		},
	}); err != nil {
		return false, fmt.Errorf("send start: %w", err)
	}

	// Expose the live stream so Poll() can send requests.
	entry.mu.Lock()
	entry.stream = stream
	entry.mu.Unlock()
	defer func() {
		entry.mu.Lock()
		entry.stream = nil
		entry.mu.Unlock()
	}()

	hs.markConnected(poolerID)

	for {
		resp, err := stream.Recv()
		if err != nil {
			// Distinguish a staleness-triggered cancellation from an external one
			// so the caller can log a useful error message.
			if watchdogCtx.Err() != nil && ctx.Err() == nil {
				return true, fmt.Errorf("staleness timeout: no snapshot received within %s", effectiveStalenessTimeout)
			}
			return true, fmt.Errorf("recv: %w", err)
		}
		if resp.Snapshot != nil {
			// Reset the staleness watchdog using the server's recommended timeout.
			timeout := resp.Snapshot.Timeout.AsDuration()
			if timeout <= 0 {
				timeout = effectiveStalenessTimeout
			}
			select {
			case resetCh <- timeout:
			default:
				// A reset is already pending; the watchdog will pick it up.
			}
			hs.applySnapshot(ctx, poolerID, poolerHealth, resp.Snapshot)
		}
	}
}

// Poll sends a poll request on the active stream for poolerID, triggering an
// immediate health snapshot from the pooler. Returns an error if no stream is
// active or the send fails.
func (hs *HealthStream) Poll(poolerID string) error {
	hs.mu.Lock()
	entry, exists := hs.streams[poolerID]
	hs.mu.Unlock()
	if !exists {
		return fmt.Errorf("no active stream for pooler %s", poolerID)
	}

	entry.mu.Lock()
	stream := entry.stream
	entry.mu.Unlock()
	if stream == nil {
		return fmt.Errorf("stream not yet established for pooler %s", poolerID)
	}

	return stream.Send(&multipoolermanagerdatapb.ManagerHealthStreamClientMessage{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamClientMessage_Poll{
			Poll: &multipoolermanagerdatapb.ManagerHealthStreamPollRequest{},
		},
	})
}

// applySnapshot writes health fields from a snapshot into the pooler store.
// This mirrors the field writes performed by the old pollPooler function on success.
func (hs *HealthStream) applySnapshot(ctx context.Context, poolerID string, poolerHealth *multiorchdatapb.PoolerHealthState, snapshot *multipoolermanagerdatapb.ManagerHealthSnapshot) {
	if snapshot.Status == nil || snapshot.Status.Status == nil {
		hs.logger.WarnContext(ctx, "received snapshot with nil status, skipping",
			"pooler_id", poolerID)
		return
	}

	status := snapshot.Status.Status
	now := timestamppb.Now()

	poolerIDStr := topoclient.MultiPoolerIDString(poolerHealth.MultiPooler.Id)
	update := func(existing *multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
		existing.LastCheckSuccessful = now
		existing.LastSeen = now
		existing.IsUpToDate = true
		existing.IsLastCheckValid = true
		existing.IsPostgresReady = status.PostgresReady
		if status.PostgresReady {
			existing.LastPostgresReadyTime = now
		}
		// NOTE: when PostgresReady is false, LastPostgresReadyTime is intentionally
		// left at its previous value so callers can reason about "last known good" time.
		existing.IsPostgresRunning = status.PostgresRunning
		existing.PoolerType = status.PoolerType
		existing.PrimaryStatus = status.PrimaryStatus
		existing.ReplicationStatus = status.ReplicationStatus
		existing.IsInitialized = status.IsInitialized
		existing.HasDataDirectory = status.HasDataDirectory
		existing.ConsensusTerm = status.ConsensusTerm
		existing.CohortMembers = status.CohortMembers
		return existing
	}

	hs.store.DoUpdate(poolerIDStr, update)

	hs.logger.DebugContext(ctx, "health snapshot applied",
		"pooler_id", poolerID,
		"pooler_type", status.PoolerType,
		"postgres_ready", status.PostgresReady,
		"postgres_running", status.PostgresRunning,
	)
}

// markConnected records that the stream is connected in the pooler store.
func (hs *HealthStream) markConnected(poolerID string) {
	now := timestamppb.Now()
	cb := func(existing *multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
		existing.StreamConnected = true
		existing.StreamConnectedSince = now
		return existing
	}
	hs.store.DoUpdate(poolerID, cb)
}

// markDisconnected records that the stream is disconnected and the pooler
// should be treated as unreachable.
func (hs *HealthStream) markDisconnected(poolerID string) {
	cb := func(existing *multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
		existing.IsLastCheckValid = false
		existing.IsPostgresReady = false
		existing.IsPostgresRunning = false
		existing.StreamConnected = false
		return existing
	}
	hs.store.DoUpdate(poolerID, cb)
}
