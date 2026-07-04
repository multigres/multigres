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

package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/tools/retry"
)

const (
	// streamReconnectInitialBackoff is the initial wait before retrying a
	// failed ManagerHealthStream connection.
	streamReconnectInitialBackoff = 1 * time.Second

	// streamReconnectMaxBackoff caps the exponential backoff between retries.
	streamReconnectMaxBackoff = 30 * time.Second
)

// HealthStreamFactory is the engine-scoped singleton that holds the shared
// configuration (rpcClient, snapshot interval, staleness timeout, logger) and
// the goroutine pool used by every per-pooler HealthStream. The factory's New
// method spawns a HealthStream goroutine for one pooler and returns the
// running stream. Shutdown cancels every per-pooler goroutine and waits for
// them to exit.
//
// HealthStreamFactory is cache-agnostic: it holds no per-pooler registry and
// no cache reference. Each spawned HealthStream closes over the cache + pooler
// ID provided at New time.
type HealthStreamFactory struct {
	logger    *slog.Logger
	rpcClient rpcclient.MultipoolerClient

	// snapshotInterval is requested from the server as the proactive snapshot
	// tick rate. Zero means use the server default (currently 5s).
	snapshotInterval time.Duration

	// stalenessTimeout is sent to the server and used to arm the staleness
	// watchdog (seeded from the start response). Zero means server default
	// (timeouts.DefaultHealthStreamStalenessTimeout).
	stalenessTimeout time.Duration

	// rootCtx is the parent of every per-pooler stream context. Cancelled by
	// Shutdown(); also propagates from the engine-level context passed to
	// NewHealthStreamFactory so engine ctx cancellation broadcasts to every
	// stream goroutine.
	rootCtx    context.Context
	rootCancel context.CancelFunc
	wg         sync.WaitGroup
}

// Option is a functional option for NewHealthStreamFactory.
type Option func(*HealthStreamFactory)

// WithSnapshotInterval sets the proactive snapshot interval sent to the server
// in the start message.
func WithSnapshotInterval(d time.Duration) Option {
	return func(f *HealthStreamFactory) {
		f.snapshotInterval = d
	}
}

// WithStalenessTimeout sets the staleness timeout sent to the server and used
// to arm the client-side staleness watchdog. Intended for tests.
func WithStalenessTimeout(d time.Duration) Option {
	return func(f *HealthStreamFactory) {
		f.stalenessTimeout = d
	}
}

// NewHealthStreamFactory creates a HealthStreamFactory.
func NewHealthStreamFactory(
	ctx context.Context,
	rpcClient rpcclient.MultipoolerClient,
	logger *slog.Logger,
	options ...Option,
) *HealthStreamFactory {
	rootCtx, cancel := context.WithCancel(ctx)
	f := &HealthStreamFactory{
		logger:     logger,
		rpcClient:  rpcClient,
		rootCtx:    rootCtx,
		rootCancel: cancel,
	}

	for _, opt := range options {
		opt(f)
	}

	return f
}

// Shutdown cancels every per-pooler stream goroutine and waits for them
// to exit. Safe to call after cache.Shutdown (in which case every
// goroutine has already exited via OnGone(GoneCacheShutdown) and Shutdown
// just observes a drained wg).
func (f *HealthStreamFactory) Shutdown() {
	f.rootCancel()
	f.wg.Wait()
}

// New constructs a HealthStream for poolerID, spawns its run loop, and returns
// the running stream. The cache+id are captured by the goroutine and used to
// read fresh Multipooler metadata on each reconnect and to serialize snapshot
// writes via DoUpdate. The returned HealthStream is stashed on the rider's
// HealthStream field by the cache's OnLive hook; OnGone calls Cancel to
// terminate the goroutine.
func (f *HealthStreamFactory) New(cache *PoolerCache, poolerID topoclient.ComponentID) *HealthStream {
	streamCtx, cancel := context.WithCancel(f.rootCtx)
	hs := &HealthStream{
		factory:  f,
		cache:    cache,
		poolerID: poolerID,
		cancel:   cancel,
	}
	f.wg.Go(func() {
		hs.run(streamCtx)
	})
	return hs
}

// HealthStream is the per-pooler stream owner. It owns this pooler's cancel
// function, current gRPC stream pointer (mu-guarded), and all per-stream
// logic. One HealthStream corresponds to one long-lived ManagerHealthStream
// gRPC stream to a single pooler.
//
// When a snapshot arrives the HealthStream writes the same health fields
// into the pooler store that the old pollPooler function wrote on success.
// On stream disconnect the pooler is marked unreachable and reconnection is
// attempted with exponential backoff (1s → 2s → … → 30s cap).
//
// The orchestrator sends its preferred snapshot_interval and staleness_timeout
// in the start message. The server echoes back the actual values it will use in
// a ManagerHealthStreamStartResponse, which the client uses to arm its staleness
// watchdog.
type HealthStream struct {
	factory  *HealthStreamFactory
	cache    *PoolerCache
	poolerID topoclient.ComponentID

	// cancel terminates the per-pooler stream goroutine.
	cancel context.CancelFunc

	mu     sync.Mutex
	stream rpcclient.ManagerHealthStream
}

// Cancel terminates the per-pooler stream goroutine.
func (hs *HealthStream) Cancel() {
	hs.cancel()
}

// setStream installs the live gRPC stream pointer. Called by streamOnce after
// the stream is established; cleared (with nil) on stream exit.
func (hs *HealthStream) setStream(s rpcclient.ManagerHealthStream) {
	hs.mu.Lock()
	hs.stream = s
	hs.mu.Unlock()
}

// liveStream returns the current live gRPC stream pointer, or nil if no
// stream is currently connected.
func (hs *HealthStream) liveStream() rpcclient.ManagerHealthStream {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	return hs.stream
}

// Poll sends a poll request on this pooler's active stream, triggering an
// immediate health snapshot from the pooler. Returns an error if no stream
// is active or the send fails.
func (hs *HealthStream) Poll() error {
	if hs == nil {
		return errors.New("no active stream for pooler")
	}
	stream := hs.liveStream()
	if stream == nil {
		return errors.New("stream not yet established for pooler")
	}
	return stream.Send(&multipoolermanagerdatapb.ManagerHealthStreamClientMessage{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamClientMessage_Poll{
			Poll: &multipoolermanagerdatapb.ManagerHealthStreamPollRequest{},
		},
	})
}

// run manages the lifecycle of this pooler's stream, reconnecting with backoff
// on failure. It reads the latest Multipooler metadata from the store on each
// reconnect attempt so hostname/port changes are picked up automatically.
func (hs *HealthStream) run(ctx context.Context) {
	logger := hs.factory.logger
	r := retry.New(streamReconnectInitialBackoff, streamReconnectMaxBackoff, retry.WithInitialDelay())
	for _, err := range r.Attempts(ctx) {
		if err != nil {
			return
		}

		// Read current pooler metadata from store on every attempt.
		poolerHealth, ok := hs.cache.GetRider(hs.poolerID)
		if !ok || poolerHealth.Health().Multipooler == nil {
			logger.WarnContext(ctx, "pooler not found in store, stopping health stream",
				"pooler_id", hs.poolerID)
			return
		}

		connected, streamErr := hs.streamOnce(ctx, poolerHealth)
		if ctx.Err() != nil {
			return
		}

		if connected {
			// Stream was successfully established before failing — reset backoff.
			r.Reset()
		}

		hs.markDisconnected()

		if streamErr != nil {
			logger.WarnContext(ctx, "health stream disconnected",
				"pooler_id", hs.poolerID,
				"error", streamErr,
			)
		}
	}
}

// streamOnce opens one ManagerHealthStream and reads until the stream fails or
// the context is cancelled. Returns (connected, err): connected is true if the
// stream was established before any error occurred.
func (hs *HealthStream) streamOnce(ctx context.Context, poolerHealth *Pooler) (connected bool, _ error) {
	logger := hs.factory.logger
	// Build the start request, sending the orchestrator's preferred timing.
	// Zero values are omitted so the server uses its own defaults.
	startReq := &multipoolermanagerdatapb.ManagerHealthStreamStartRequest{}
	if hs.factory.snapshotInterval > 0 {
		startReq.SnapshotInterval = durationpb.New(hs.factory.snapshotInterval)
	}
	if hs.factory.stalenessTimeout > 0 {
		startReq.StalenessTimeout = durationpb.New(hs.factory.stalenessTimeout)
	}

	// Seed the staleness watchdog before any message is received. This is the
	// value we sent; the server will confirm (or adjust) it in the start response,
	// at which point we reset the watchdog to the echoed value.
	initialStaleness := timeouts.DefaultHealthStreamStalenessTimeout
	if hs.factory.stalenessTimeout > 0 {
		initialStaleness = hs.factory.stalenessTimeout
	}

	// Staleness watchdog: cancel the stream if no message arrives within the
	// timeout. This catches the "server goroutine stuck but TCP alive" failure
	// mode that gRPC keepalive does not cover.
	//
	// The watchdog context is passed to ManagerHealthStream so cancelling it
	// terminates the gRPC stream and causes stream.Recv() to return an error.
	watchdogCtx, cancelWatchdog := context.WithCancel(ctx)
	defer cancelWatchdog()

	// resetCh carries the new timer duration whenever a message is received.
	// Buffered so the recv loop never blocks on the watchdog goroutine.
	resetCh := make(chan time.Duration, 1)
	go func() {
		current := initialStaleness
		timer := time.NewTimer(current)
		defer timer.Stop()
		for {
			select {
			case d := <-resetCh:
				current = d
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(current)
			case <-timer.C:
				logger.WarnContext(ctx, "health stream stale: no message received within timeout, reconnecting",
					"pooler_id", hs.poolerID,
					"timeout", current,
				)
				cancelWatchdog()
				return
			case <-watchdogCtx.Done():
				return
			}
		}
	}()

	stream, err := hs.factory.rpcClient.ManagerHealthStream(watchdogCtx, poolerHealth.Health().Multipooler)
	if err != nil {
		return false, fmt.Errorf("open stream: %w", err)
	}

	// Send the start message with the negotiated timing preferences.
	if err := stream.Send(&multipoolermanagerdatapb.ManagerHealthStreamClientMessage{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamClientMessage_Start{
			Start: startReq,
		},
	}); err != nil {
		return false, fmt.Errorf("send start: %w", err)
	}

	// Read the start response — the first server message confirms the actual
	// timing values the server will use.
	firstMsg, err := stream.Recv()
	if err != nil {
		if watchdogCtx.Err() != nil && ctx.Err() == nil {
			return false, errors.New("staleness timeout waiting for start response")
		}
		return false, fmt.Errorf("recv start response: %w", err)
	}
	startResp := firstMsg.GetStart()
	if startResp == nil {
		return false, fmt.Errorf("expected start response, got %T", firstMsg.GetMessage())
	}
	// Determine the effective staleness for the watchdog:
	//   - If a local override is set (WithStalenessTimeout), use it directly.
	//     This preserves sub-second precision used in tests.
	//   - Otherwise, use the server-confirmed value from the start response.
	confirmedStaleness := initialStaleness
	if hs.factory.stalenessTimeout == 0 {
		if s := startResp.StalenessTimeout.AsDuration(); s > 0 {
			confirmedStaleness = s
		}
	}
	select {
	case resetCh <- confirmedStaleness:
	default:
	}

	// Expose the live stream so Poll() can send requests.
	hs.setStream(stream)
	defer hs.setStream(nil)

	hs.markConnected()

	for {
		resp, err := stream.Recv()
		if err != nil {
			// Distinguish a staleness-triggered cancellation from an external one
			// so the caller can log a useful error message.
			if watchdogCtx.Err() != nil && ctx.Err() == nil {
				return true, fmt.Errorf("staleness timeout: no snapshot received within %s", confirmedStaleness)
			}
			return true, fmt.Errorf("recv: %w", err)
		}
		if snap := resp.GetSnapshot(); snap != nil {
			// Reset the staleness watchdog. Prefer the local override (which
			// preserves sub-second precision); fall back to the server-echoed value.
			timeout := confirmedStaleness
			if hs.factory.stalenessTimeout == 0 {
				if s := snap.Timeout.AsDuration(); s > 0 {
					timeout = s
				}
			}
			select {
			case resetCh <- timeout:
			default:
				// A reset is already pending; the watchdog will pick it up.
			}
			hs.applySnapshot(ctx, poolerHealth, snap)
		}
	}
}

// applySnapshot writes health fields from a snapshot into the pooler store.
// This mirrors the field writes performed by the old pollPooler function on success.
func (hs *HealthStream) applySnapshot(ctx context.Context, poolerHealth *Pooler, snapshot *multipoolermanagerdatapb.ManagerHealthSnapshot) {
	logger := hs.factory.logger
	if snapshot.Status == nil || snapshot.Status.Status == nil {
		logger.WarnContext(ctx, "received snapshot with nil status, skipping",
			"pooler_id", hs.poolerID)
		return
	}

	status := snapshot.Status.Status
	now := timestamppb.Now()

	poolerIDStr := topoclient.ComponentIDString(poolerHealth.Health().Multipooler.Id)
	update := func(existing *Pooler) *Pooler {
		existing.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			h.LastCheckSuccessful = now
			h.LastSeen = now
			h.IsUpToDate = true
			h.IsLastCheckValid = true
			h.Status = proto.Clone(status).(*multipoolermanagerdatapb.Status)
			if snapshot.Status.AvailabilityStatus != nil {
				h.AvailabilityStatus = proto.Clone(snapshot.Status.AvailabilityStatus).(*clustermetadatapb.AvailabilityStatus)
			} else {
				h.AvailabilityStatus = nil
			}
			if snapshot.Status.ConsensusStatus != nil {
				h.ConsensusStatus = proto.Clone(snapshot.Status.ConsensusStatus).(*clustermetadatapb.ConsensusStatus)
			} else {
				h.ConsensusStatus = nil
			}
			if status.PostgresReady {
				h.LastPostgresReadyTime = now
			}
			// NOTE: when PostgresReady is false, LastPostgresReadyTime is intentionally
			// left at its previous value so callers can reason about "last known good" time.
			h.StreamSnapshotsReceived++
		})
		return existing
	}

	hs.cache.DoUpdate(poolerIDStr, update)

	logger.DebugContext(ctx, "health snapshot applied",
		"pooler_id", hs.poolerID,
		"pooler_type", status.PoolerType,
		"postgres_ready", status.PostgresReady,
		"postgres_running", status.PostgresRunning,
	)
}

// markConnected records that the stream is connected in the pooler store.
func (hs *HealthStream) markConnected() {
	now := timestamppb.Now()
	cb := func(existing *Pooler) *Pooler {
		existing.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			h.StreamConnected = true
			h.StreamConnectedSince = now
		})
		return existing
	}
	hs.cache.DoUpdate(hs.poolerID, cb)
}

// markDisconnected records that the stream is disconnected and the pooler
// should be treated as unreachable.
func (hs *HealthStream) markDisconnected() {
	cb := func(existing *Pooler) *Pooler {
		existing.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			h.IsLastCheckValid = false
			if h.Status != nil {
				h.Status.PostgresReady = false
				h.Status.PostgresRunning = false
			}
			h.StreamConnected = false
		})
		return existing
	}
	hs.cache.DoUpdate(hs.poolerID, cb)
}
