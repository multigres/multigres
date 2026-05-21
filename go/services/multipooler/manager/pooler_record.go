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
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/servenv/toporeg"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

const (
	topoPublisherRetryInterval = 30 * time.Second
	topoPublisherWriteTimeout  = 5 * time.Second
)

// poolerTopoStore is the subset of topoclient.Store used by poolerRecord.
type poolerTopoStore interface {
	RegisterMultiPooler(ctx context.Context, multipooler *clustermetadatapb.MultiPooler, allowUpdate bool) error
}

// poolerRecord is the single owner of the local MultiPooler topology entry.
//
// It centralises three concerns that used to be spread across init.go and
// manager.go:
//
//  1. Reads — every accessor returns a field of the latest desired proto. The
//     proto pointer is held in an atomic.Pointer; each Mutate stores a fresh
//     clone and the previous value is treated as immutable. Readers therefore
//     never take a lock and never see a half-written value.
//  2. Writes — Mutate is the only path that changes the proto. It atomically
//     stores the new desired state AND schedules an asynchronous publish so
//     callers cannot forget to publish after a mutation. Concurrent Mutate
//     calls are not safe; the caller (today: StateManager) must serialise.
//  3. Lifecycle — Register kicks off initial registration (with retry +
//     alarm) and starts the publisher goroutine. Unregister stops the
//     publisher, applies a caller-supplied final mutation, performs one
//     final synchronous publish, and cancels the toporeg retry goroutine.
//
// The publisher follows the eventual-consistency model: a wakeup channel
// signals an immediate write attempt and a 30s ticker recovers from
// transient etcd outages. proto.Equal short-circuits no-op publishes.
type poolerRecord struct {
	logger     *slog.Logger
	topoClient poolerTopoStore

	desired       atomic.Pointer[clustermetadatapb.MultiPooler]
	lastPublished atomic.Pointer[clustermetadatapb.MultiPooler]

	// wakeup is a size-1 buffered channel. A non-blocking send schedules a
	// publish without accumulating multiple pending signals.
	wakeup chan struct{}

	publisherMu     sync.Mutex
	publisherCancel context.CancelFunc
	publisherWG     sync.WaitGroup

	registerOnce sync.Once
	tr           *toporeg.TopoReg
}

// newPoolerRecord returns a poolerRecord seeded with initial as the desired
// state. The caller hands ownership of initial to the record; further access
// must go through Snapshot, Mutate, or the typed accessors.
func newPoolerRecord(logger *slog.Logger, topoClient poolerTopoStore, initial *clustermetadatapb.MultiPooler) *poolerRecord {
	r := &poolerRecord{
		logger:     logger,
		topoClient: topoClient,
		wakeup:     make(chan struct{}, 1),
	}
	r.desired.Store(proto.Clone(initial).(*clustermetadatapb.MultiPooler))
	return r
}

// Id returns the pooler's identity. Effectively immutable — Mutate must not
// change it.
func (r *poolerRecord) Id() *clustermetadatapb.ID { return r.desired.Load().Id }

// ShardKey returns the pooler's shard identity. Effectively immutable.
func (r *poolerRecord) ShardKey() *clustermetadatapb.ShardKey { return r.desired.Load().ShardKey }

// PoolerDir returns the on-disk pooler directory. Effectively immutable.
func (r *poolerRecord) PoolerDir() string { return r.desired.Load().PoolerDir }

// PgDataDir returns the PostgreSQL data directory. Effectively immutable.
func (r *poolerRecord) PgDataDir() string { return r.desired.Load().PgDataDir }

// Hostname returns the pooler's hostname. Effectively immutable.
func (r *poolerRecord) Hostname() string { return r.desired.Load().Hostname }

// Port returns the port advertised for the given named service (e.g.
// "postgres", "grpc", "http", "pgbackrest"). Returns 0 if the name is not in
// the port map.
func (r *poolerRecord) Port(name string) int32 { return r.desired.Load().PortMap[name] }

// Type returns the current pooler type.
func (r *poolerRecord) Type() clustermetadatapb.PoolerType { return r.desired.Load().Type }

// ServingStatus returns the current serving status.
func (r *poolerRecord) ServingStatus() clustermetadatapb.PoolerServingStatus {
	return r.desired.Load().ServingStatus
}

// Snapshot returns a deep clone of the current desired state. Use this when
// passing the record to code that requires a *MultiPooler value and may
// mutate it locally.
func (r *poolerRecord) Snapshot() *clustermetadatapb.MultiPooler {
	return proto.Clone(r.desired.Load()).(*clustermetadatapb.MultiPooler)
}

// Mutate clones the current desired state, applies fn, atomically stores the
// result, and schedules an asynchronous publish.
//
// ctx must carry an action lock (see AssertActionLockHeld). The action lock
// serialises state transitions across the whole manager — RPC handlers
// (promotion, demotion, type change) and lifecycle paths (Open, closeLocked)
// all reach Mutate via StateManager.SetState with an action-locked ctx
// threaded through from the caller. Mutate returns the assertion error
// without applying fn if the lock is not held.
//
// fn must not block or call back into poolerRecord; it should perform simple
// field assignments only. fn must not modify identity / topology fields (Id,
// ShardKey, PoolerDir, PgDataDir, Hostname, PortMap) — those are set at
// construction and are exposed through accessors that assume they never
// change. Mutate returns an error describing the offending field if fn
// changes any of them, leaving the desired state untouched.
func (r *poolerRecord) Mutate(ctx context.Context, fn func(*clustermetadatapb.MultiPooler)) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	current := r.desired.Load()
	next := proto.Clone(current).(*clustermetadatapb.MultiPooler)
	fn(next)
	if err := checkImmutableFieldsUnchanged(current, next); err != nil {
		return err
	}
	r.desired.Store(next)

	// Non-blocking send: if the channel is already full, a publish is already
	// pending and will pick up the latest desired state.
	select {
	case r.wakeup <- struct{}{}:
	default:
	}
	return nil
}

// checkImmutableFieldsUnchanged returns an error if a Mutate callback
// modified a field that poolerRecord treats as immutable. The only
// legitimate Mutate targets are Type and ServingStatus.
func checkImmutableFieldsUnchanged(before, after *clustermetadatapb.MultiPooler) error {
	if !proto.Equal(before.Id, after.Id) {
		return errors.New("poolerRecord.Mutate: Id is immutable")
	}
	if !proto.Equal(before.ShardKey, after.ShardKey) {
		return errors.New("poolerRecord.Mutate: ShardKey is immutable")
	}
	if before.PoolerDir != after.PoolerDir {
		return errors.New("poolerRecord.Mutate: PoolerDir is immutable")
	}
	if before.PgDataDir != after.PgDataDir {
		return errors.New("poolerRecord.Mutate: PgDataDir is immutable")
	}
	if before.Hostname != after.Hostname {
		return errors.New("poolerRecord.Mutate: Hostname is immutable")
	}
	if !portMapsEqual(before.PortMap, after.PortMap) {
		return errors.New("poolerRecord.Mutate: PortMap is immutable")
	}
	return nil
}

func portMapsEqual(a, b map[string]int32) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// Register starts the publisher goroutine and kicks off initial registration
// via toporeg.Register. alarm is invoked with error strings while
// registration is retrying and with "" once it succeeds — wire it to the
// status page so registration failures are visible. Idempotent.
//
// The publisher runs for the lifetime of the registration (until Unregister
// is called). Manager open/close cycles (Pause/resume) do not affect it —
// the topology entry continues to reflect Mutates throughout.
func (r *poolerRecord) Register(parent context.Context, alarm func(string)) {
	r.registerOnce.Do(func() {
		// Start publisher.
		ctx, cancel := context.WithCancel(parent)
		r.publisherMu.Lock()
		r.publisherCancel = cancel
		r.publisherMu.Unlock()
		r.publisherWG.Go(func() {
			r.runPublisher(ctx)
		})

		// Kick off initial registration retry loop. The unregister callback
		// is a no-op — the DRAINED write is handled by Unregister itself
		// (via Mutate + final publish) so toporeg only needs to manage the
		// retry goroutine's lifetime.
		registerFunc := func(ctx context.Context) error {
			return r.topoClient.RegisterMultiPooler(ctx, r.Snapshot(), true /* allowUpdate */)
		}
		r.tr = toporeg.Register(registerFunc, func(context.Context) error { return nil }, alarm)
	})
}

// Unregister stops the publisher, applies an optional final mutation,
// performs one synchronous publish if the result diverges from the last
// published state, and cancels the toporeg retry goroutine.
//
// finalize lets the caller stamp a shutdown state (e.g. Type=DRAINED,
// ServingStatus=NOT_SERVING) without going through Mutate. Pass nil to just
// publish the current desired state. The record stays agnostic about what
// the shutdown state means — that's the caller's domain knowledge.
//
// Unlike Mutate, Unregister does NOT require an action lock: the publisher
// is cancelled before finalize runs, so no concurrent publish can race the
// mutation, and any in-flight or subsequent Mutate from another goroutine
// is moot — there is no publisher to carry it. Making Unregister
// lock-free keeps shutdown reliable even when the rest of the system is in
// a bad state.
//
// Safe to call on a record that was never Registered.
func (r *poolerRecord) Unregister(ctx context.Context, finalize func(*clustermetadatapb.MultiPooler)) error {
	r.publisherMu.Lock()
	cancel := r.publisherCancel
	r.publisherCancel = nil
	r.publisherMu.Unlock()
	if cancel == nil {
		return nil
	}

	// Stop the publisher loop first so there is no concurrent publish in
	// flight when we mutate / read desired for the final write.
	cancel()
	r.publisherWG.Wait()

	if finalize != nil {
		current := r.desired.Load()
		next := proto.Clone(current).(*clustermetadatapb.MultiPooler)
		finalize(next)
		if err := checkImmutableFieldsUnchanged(current, next); err != nil {
			return err
		}
		r.desired.Store(next)
	}

	desired := r.desired.Load()
	if desired != nil && !proto.Equal(desired, r.lastPublished.Load()) {
		if err := r.topoClient.RegisterMultiPooler(ctx, desired, true); err != nil {
			r.logger.WarnContext(ctx, "Final publish during Unregister failed; topology may be stale",
				"error", err,
				"type", desired.Type,
				"serving_status", desired.ServingStatus)
		} else {
			r.lastPublished.Store(proto.Clone(desired).(*clustermetadatapb.MultiPooler))
		}
	}

	r.tr.Unregister()
	return nil
}

// runPublisher is the background loop. It exits when ctx is cancelled.
func (r *poolerRecord) runPublisher(ctx context.Context) {
	ticker := time.NewTicker(topoPublisherRetryInterval)
	defer ticker.Stop()
	r.publisherLoop(ctx, ticker.C)
}

// publisherLoop accepts an injectable ticker channel so tests can drive
// retries without real clock time.
func (r *poolerRecord) publisherLoop(ctx context.Context, tickC <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.wakeup:
			r.publishIfNeeded(ctx)
		case <-tickC:
			r.publishIfNeeded(ctx)
		}
	}
}

// publishIfNeeded writes the desired state to etcd if it differs from the
// last successfully published state. A no-op when state is already current.
func (r *poolerRecord) publishIfNeeded(ctx context.Context) {
	desired := r.desired.Load()
	lastPublished := r.lastPublished.Load()

	if desired == nil {
		return
	}

	if proto.Equal(desired, lastPublished) {
		return
	}

	r.logger.InfoContext(ctx, "Publishing multipooler state to topology",
		"type", desired.Type,
		"serving_status", desired.ServingStatus)

	publishCtx, cancel := context.WithTimeout(ctx, topoPublisherWriteTimeout)
	defer cancel()

	if err := r.topoClient.RegisterMultiPooler(publishCtx, desired, true); err != nil {
		r.logger.ErrorContext(ctx, "Failed to publish multipooler state to topology; will retry",
			"error", err,
			"type", desired.Type,
			"serving_status", desired.ServingStatus)
		return
	}

	r.lastPublished.Store(proto.Clone(desired).(*clustermetadatapb.MultiPooler))

	r.logger.InfoContext(ctx, "Published multipooler state to topology",
		"type", desired.Type,
		"serving_status", desired.ServingStatus)
}
