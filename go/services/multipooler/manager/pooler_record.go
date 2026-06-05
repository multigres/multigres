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
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/servenv/toporeg"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

const (
	topoPublisherRetryInterval = 30 * time.Second
	topoPublisherWriteTimeout  = 5 * time.Second
)

// MutablePoolerRecordState is the slice of the MultiPooler topology entry
// that callers can change through Mutate and Unregister. The Mutate /
// Unregister callbacks receive a pointer to a struct populated with the
// current values, so a caller can read and conditionally update them.
//
// All other MultiPooler proto fields (Id, ShardKey, PoolerDir, PgDataDir,
// Hostname, PortMap) are set at construction and the record treats them as
// immutable — exposing only this struct in the mutation API makes that
// contract a property of the type system rather than a runtime check.
type MutablePoolerRecordState struct {
	Type            clustermetadatapb.PoolerType
	ServingStatus   clustermetadatapb.PoolerServingStatus
	LifecycleStatus *clustermetadatapb.PoolerLifecycle
	// CurrentLeadership is the pooler's most recent recorded consensus
	// observation. Set ONLY when this pooler currently considers itself the
	// leader of its shard; replicas leave it nil. Published into etcd so
	// multigateway can bootstrap leader routing on discovery.
	CurrentLeadership *clustermetadatapb.LeaderObservation
}

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

// CurrentLeadership returns the pooler's most recent recorded consensus
// observation, or nil if this pooler is not currently the leader of its shard.
func (r *poolerRecord) CurrentLeadership() *clustermetadatapb.LeaderObservation {
	return r.desired.Load().CurrentLeadership
}

// Snapshot returns a deep clone of the current desired state. Use this when
// passing the record to code that requires a *MultiPooler value and may
// mutate it locally.
func (r *poolerRecord) Snapshot() *clustermetadatapb.MultiPooler {
	return proto.Clone(r.desired.Load()).(*clustermetadatapb.MultiPooler)
}

// Mutate atomically applies fn to the current MutablePoolerRecordState
// and schedules an asynchronous publish. fn receives a struct populated
// with the current values; any modifications it makes become the new
// desired state. Fields not exposed in MutablePoolerRecordState (Id,
// ShardKey, PoolerDir, etc.) cannot be touched.
//
// ctx must carry an action lock (see AssertActionLockHeld). The action lock
// serialises state transitions across the whole manager — RPC handlers
// (promotion, demotion, type change) and lifecycle paths (Open, closeLocked)
// all reach Mutate via StateManager.SetState with an action-locked ctx
// threaded through from the caller. Mutate returns the assertion error
// without applying fn if the lock is not held.
//
// fn must not block or call back into poolerRecord; it should perform
// simple field assignments only.
//
// Returns an error (without applying fn) if the resulting state violates
// the consistency invariant between Type and CurrentLeadership: a pooler
// is the leader iff Type == PRIMARY iff CurrentLeadership is set and
// names this pooler. Callers must keep the two fields in sync.
func (r *poolerRecord) Mutate(ctx context.Context, fn func(*MutablePoolerRecordState)) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	if err := r.applyMutation(fn); err != nil {
		return err
	}

	// Non-blocking send: if the channel is already full, a publish is already
	// pending and will pick up the latest desired state.
	select {
	case r.wakeup <- struct{}{}:
	default:
	}
	return nil
}

// applyMutation clones the current desired proto, hands a
// MutablePoolerRecordState view to fn, validates the Type ↔
// CurrentLeadership invariant, then atomically stores the result. Caller
// is responsible for sequencing (action lock, publisher state). Returns
// the validation error and leaves the stored state unchanged on violation.
func (r *poolerRecord) applyMutation(fn func(*MutablePoolerRecordState)) error {
	current := r.desired.Load()
	state := MutablePoolerRecordState{
		Type:              current.Type,
		ServingStatus:     current.ServingStatus,
		LifecycleStatus:   current.LifecycleStatus,
		CurrentLeadership: current.CurrentLeadership,
	}
	fn(&state)
	if err := r.validateState(&state); err != nil {
		return err
	}
	next := proto.Clone(current).(*clustermetadatapb.MultiPooler)
	next.Type = state.Type
	next.ServingStatus = state.ServingStatus
	next.CurrentLeadership = state.CurrentLeadership
	next.LifecycleStatus = state.LifecycleStatus
	r.desired.Store(next)
	return nil
}

// validateState enforces the Type ↔ CurrentLeadership consistency
// invariant: a pooler is the leader iff Type == PRIMARY iff
// CurrentLeadership is set and names this pooler. Any deviation is a
// caller bug.
func (r *poolerRecord) validateState(state *MutablePoolerRecordState) error {
	isPrimary := state.Type == clustermetadatapb.PoolerType_PRIMARY
	hasObs := state.CurrentLeadership != nil
	switch {
	case isPrimary && !hasObs:
		return errors.New("invariant violated: Type=PRIMARY but CurrentLeadership is nil")
	case !isPrimary && hasObs:
		return fmt.Errorf("invariant violated: Type=%s but CurrentLeadership is set", state.Type)
	case hasObs && !proto.Equal(state.CurrentLeadership.LeaderId, r.Id()):
		return fmt.Errorf("invariant violated: CurrentLeadership.LeaderId=%s does not match this pooler's Id=%s",
			topoclient.MultiPoolerIDString(state.CurrentLeadership.LeaderId),
			topoclient.MultiPoolerIDString(r.Id()))
	}
	return nil
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
// ServingStatus=NOT_SERVING). The callback receives a MutablePoolerRecordState
// populated with current values; modifications become the new desired state.
// Pass nil to just publish whatever the publisher hadn't yet written. The
// record stays agnostic about what the shutdown state means — that's the
// caller's domain knowledge.
//
// Unlike Mutate, Unregister does NOT require an action lock: the publisher
// is cancelled before finalize runs, so no concurrent publish can race the
// mutation, and any in-flight or subsequent Mutate from another goroutine
// is moot — there is no publisher to carry it. Making Unregister
// lock-free keeps shutdown reliable even when the rest of the system is in
// a bad state.
//
// Safe to call on a record that was never Registered.
func (r *poolerRecord) Unregister(ctx context.Context, finalize func(*MutablePoolerRecordState)) {
	r.publisherMu.Lock()
	cancel := r.publisherCancel
	r.publisherCancel = nil
	r.publisherMu.Unlock()
	if cancel == nil {
		return
	}

	// Stop the publisher loop first so there is no concurrent publish in
	// flight when we mutate / read desired for the final write.
	cancel()
	r.publisherWG.Wait()

	if finalize != nil {
		if err := r.applyMutation(finalize); err != nil {
			// Finalize must not put the record in an inconsistent state.
			// Log and continue: a final publish still helps surface
			// whatever the record currently holds, even if finalize was
			// a no-op.
			r.logger.WarnContext(ctx, "Final mutation during Unregister rejected by invariant; publishing prior desired state",
				"error", err)
		}
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
