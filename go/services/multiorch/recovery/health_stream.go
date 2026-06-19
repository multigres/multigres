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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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
)

// HealthStream maintains one ManagerHealthStream stream per pooler. It replaces
// the polling loop: instead of periodically calling the Status RPC, each pooler
// pushes health snapshots to multiorch via a long-lived gRPC stream.
//
// When a snapshot arrives the HealthStream writes the same health fields into
// the pooler store that the old pollPooler function wrote on success. On stream
// disconnect the pooler is marked unreachable and reconnection is attempted
// with exponential backoff (1s → 2s → … → 30s cap).
//
// The orchestrator sends its preferred snapshot_interval and staleness_timeout
// in the start message. The server echoes back the actual values it will use in
// a ManagerHealthStreamStartResponse, which the client uses to arm its staleness
// watchdog.
//
// HealthStream is cache-agnostic: it holds no per-pooler registry and no
// cache reference. spawnStream takes the cache + pooler ID at spawn time
// and the resulting goroutine closes over them. The handle that spawnStream
// returns is stashed on the rider's Stream field by the cache's OnLive
// hook; OnGone calls handle.Cancel to tear it down.
type HealthStream struct {
	logger    *slog.Logger
	rpcClient rpcclient.MultiPoolerClient

	// snapshotInterval is requested from the server as the proactive snapshot
	// tick rate. Zero means use the server default (currently 5s).
	snapshotInterval time.Duration

	// stalenessTimeout is sent to the server and used to arm the staleness
	// watchdog (seeded from the start response). Zero means server default
	// (timeouts.DefaultHealthStreamStalenessTimeout).
	stalenessTimeout time.Duration

	// rootCtx is the parent of every per-pooler stream context. Cancelled by
	// Shutdown(); also propagates from the engine-level context passed to
	// NewHealthStream so engine ctx cancellation broadcasts to every stream
	// goroutine.
	rootCtx    context.Context
	rootCancel context.CancelFunc
	wg         sync.WaitGroup
}

// Option is a functional option for NewHealthStream.
type Option func(*HealthStream)

// WithSnapshotInterval sets the proactive snapshot interval sent to the server
// in the start message.
func WithSnapshotInterval(d time.Duration) Option {
	return func(hs *HealthStream) {
		hs.snapshotInterval = d
	}
}

// WithStalenessTimeout sets the staleness timeout sent to the server and used
// to arm the client-side staleness watchdog. Intended for tests.
func WithStalenessTimeout(d time.Duration) Option {
	return func(hs *HealthStream) {
		hs.stalenessTimeout = d
	}
}

// NewHealthStream creates a HealthStream. The cache reference must be bound
// via SetCache before cache.Start() is called — the cache's OnLive hook
// reaches into HealthStream.spawnStream, which in turn reads the cache to
// pick up topology updates on each reconnect.
func NewHealthStream(
	ctx context.Context,
	rpcClient rpcclient.MultiPoolerClient,
	logger *slog.Logger,
	options ...Option,
) *HealthStream {
	rootCtx, cancel := context.WithCancel(ctx)
	hs := &HealthStream{
		logger:     logger,
		rpcClient:  rpcClient,
		rootCtx:    rootCtx,
		rootCancel: cancel,
	}

	for _, opt := range options {
		opt(hs)
	}

	return hs
}

// Shutdown cancels every per-pooler stream goroutine and waits for them
// to exit. Safe to call after cache.Shutdown (in which case every
// goroutine has already exited via OnGone(GoneCacheShutdown) and Shutdown
// just observes a drained wg).
func (hs *HealthStream) Shutdown() {
	hs.rootCancel()
	hs.wg.Wait()
}

// spawnStream is invoked from the cache's OnLive hook. It constructs a
// StreamHandle, registers it for shutdown waiting, and launches the
// runStream goroutine. The cache+id are captured by the goroutine and used
// to read fresh MultiPooler metadata on each reconnect and to serialize
// snapshot writes via DoUpdate. The handle is returned to the hook so it
// can be stored on the rider; OnGone calls handle.Cancel to terminate the
// goroutine.
func (hs *HealthStream) spawnStream(cache *store.PoolerCache, poolerID topoclient.ComponentID) *store.StreamHandle {
	streamCtx, cancel := context.WithCancel(hs.rootCtx)
	handle := store.NewStreamHandle(cancel)
	hs.wg.Go(func() {
		hs.runStream(streamCtx, cache, poolerID, handle)
	})
	return handle
}

// runStream manages the lifecycle of one stream, reconnecting with backoff on failure.
// It reads the latest MultiPooler metadata from the store on each reconnect attempt
// so hostname/port changes are picked up automatically.
func (hs *HealthStream) runStream(ctx context.Context, cache *store.PoolerCache, poolerID topoclient.ComponentID, entry *store.StreamHandle) {
	r := retry.New(streamReconnectInitialBackoff, streamReconnectMaxBackoff, retry.WithInitialDelay())
	for _, err := range r.Attempts(ctx) {
		if err != nil {
			return
		}

		// Read current pooler metadata from store on every attempt.
		poolerHealth, ok := cache.GetRider(poolerID)
		if !ok || poolerHealth.MultiPooler == nil {
			hs.logger.WarnContext(ctx, "pooler not found in store, stopping health stream",
				"pooler_id", poolerID)
			return
		}

		connected, streamErr := hs.streamOnce(ctx, cache, poolerID, poolerHealth, entry)
		if ctx.Err() != nil {
			return
		}

		if connected {
			// Stream was successfully established before failing — reset backoff.
			r.Reset()
		}

		hs.markDisconnected(cache, poolerID)

		if streamErr != nil {
			hs.logger.WarnContext(ctx, "health stream disconnected",
				"pooler_id", poolerID,
				"error", streamErr,
			)
		}
	}
}

// streamOnce opens one ManagerHealthStream and reads until the stream fails or
// the context is cancelled. Returns (connected, err): connected is true if the
// stream was established before any error occurred.
func (hs *HealthStream) streamOnce(ctx context.Context, cache *store.PoolerCache, poolerID topoclient.ComponentID, poolerHealth *store.Pooler, entry *store.StreamHandle) (connected bool, _ error) {
	// Build the start request, sending the orchestrator's preferred timing.
	// Zero values are omitted so the server uses its own defaults.
	startReq := &multipoolermanagerdatapb.ManagerHealthStreamStartRequest{}
	if hs.snapshotInterval > 0 {
		startReq.SnapshotInterval = durationpb.New(hs.snapshotInterval)
	}
	if hs.stalenessTimeout > 0 {
		startReq.StalenessTimeout = durationpb.New(hs.stalenessTimeout)
	}

	// Seed the staleness watchdog before any message is received. This is the
	// value we sent; the server will confirm (or adjust) it in the start response,
	// at which point we reset the watchdog to the echoed value.
	initialStaleness := timeouts.DefaultHealthStreamStalenessTimeout
	if hs.stalenessTimeout > 0 {
		initialStaleness = hs.stalenessTimeout
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
				hs.logger.WarnContext(ctx, "health stream stale: no message received within timeout, reconnecting",
					"pooler_id", poolerID,
					"timeout", current,
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
	if hs.stalenessTimeout == 0 {
		if s := startResp.StalenessTimeout.AsDuration(); s > 0 {
			confirmedStaleness = s
		}
	}
	select {
	case resetCh <- confirmedStaleness:
	default:
	}

	// Expose the live stream so Poll() can send requests.
	entry.SetStream(stream)
	defer entry.SetStream(nil)

	hs.markConnected(cache, poolerID)

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
			if hs.stalenessTimeout == 0 {
				if s := snap.Timeout.AsDuration(); s > 0 {
					timeout = s
				}
			}
			select {
			case resetCh <- timeout:
			default:
				// A reset is already pending; the watchdog will pick it up.
			}
			hs.applySnapshot(ctx, cache, poolerID, poolerHealth, snap)
		}
	}
}

// Poll sends a poll request on a pooler's active stream, triggering an
// immediate health snapshot from the pooler. Returns an error if no stream
// is active or the send fails.
func (hs *HealthStream) Poll(p *store.Pooler) error {
	if p == nil || p.Stream == nil {
		return errors.New("no active stream for pooler")
	}
	stream := p.Stream.Stream()
	if stream == nil {
		return errors.New("stream not yet established for pooler")
	}
	return stream.Send(&multipoolermanagerdatapb.ManagerHealthStreamClientMessage{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamClientMessage_Poll{
			Poll: &multipoolermanagerdatapb.ManagerHealthStreamPollRequest{},
		},
	})
}

// applySnapshot writes health fields from a snapshot into the pooler store.
// This mirrors the field writes performed by the old pollPooler function on success.
func (hs *HealthStream) applySnapshot(ctx context.Context, cache *store.PoolerCache, poolerID topoclient.ComponentID, poolerHealth *store.Pooler, snapshot *multipoolermanagerdatapb.ManagerHealthSnapshot) {
	if snapshot.Status == nil || snapshot.Status.Status == nil {
		hs.logger.WarnContext(ctx, "received snapshot with nil status, skipping",
			"pooler_id", poolerID)
		return
	}

	status := snapshot.Status.Status
	now := timestamppb.Now()

	poolerIDStr := topoclient.ComponentIDString(poolerHealth.MultiPooler.Id)
	update := func(existing *store.Pooler) *store.Pooler {
		existing.LastCheckSuccessful = now
		existing.LastSeen = now
		existing.IsUpToDate = true
		existing.IsLastCheckValid = true
		existing.Status = proto.Clone(status).(*multipoolermanagerdatapb.Status)
		if snapshot.Status.AvailabilityStatus != nil {
			existing.AvailabilityStatus = proto.Clone(snapshot.Status.AvailabilityStatus).(*clustermetadatapb.AvailabilityStatus)
		} else {
			existing.AvailabilityStatus = nil
		}
		if snapshot.Status.ConsensusStatus != nil {
			existing.ConsensusStatus = proto.Clone(snapshot.Status.ConsensusStatus).(*clustermetadatapb.ConsensusStatus)
		} else {
			existing.ConsensusStatus = nil
		}
		if status.PostgresReady {
			existing.LastPostgresReadyTime = now
		}
		// NOTE: when PostgresReady is false, LastPostgresReadyTime is intentionally
		// left at its previous value so callers can reason about "last known good" time.
		existing.StreamSnapshotsReceived++
		return existing
	}

	cache.DoUpdate(poolerIDStr, update)

	hs.logger.DebugContext(ctx, "health snapshot applied",
		"pooler_id", poolerID,
		"pooler_type", status.PoolerType,
		"postgres_ready", status.PostgresReady,
		"postgres_running", status.PostgresRunning,
	)
}

// markConnected records that the stream is connected in the pooler store.
func (hs *HealthStream) markConnected(cache *store.PoolerCache, poolerID topoclient.ComponentID) {
	now := timestamppb.Now()
	cb := func(existing *store.Pooler) *store.Pooler {
		existing.StreamConnected = true
		existing.StreamConnectedSince = now
		return existing
	}
	cache.DoUpdate(poolerID, cb)
}

// StartForTest spawns a stream goroutine for id and stashes the resulting
// StreamHandle on the existing cache rider. This mirrors what the cache's
// OnLive hook does in production, allowing tests to drive a single pooler's
// stream lifecycle without booting the full poolerwatch machinery. The
// *testing.T argument keeps this helper out of production call paths.
func (hs *HealthStream) StartForTest(t *testing.T, cache *store.PoolerCache, id *clustermetadatapb.ID) {
	t.Helper()
	poolerID := topoclient.ComponentIDString(id)
	handle := hs.spawnStream(cache, poolerID)
	cache.DoUpdate(poolerID, func(p *store.Pooler) *store.Pooler {
		p.Stream = handle
		return p
	})
}

// markDisconnected records that the stream is disconnected and the pooler
// should be treated as unreachable.
func (hs *HealthStream) markDisconnected(cache *store.PoolerCache, poolerID topoclient.ComponentID) {
	cb := func(existing *store.Pooler) *store.Pooler {
		existing.IsLastCheckValid = false
		if existing.Status != nil {
			existing.Status.PostgresReady = false
			existing.Status.PostgresRunning = false
		}
		existing.StreamConnected = false
		return existing
	}
	cache.DoUpdate(poolerID, cb)
}
