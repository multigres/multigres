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
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/tools/retry"
)

const (
	topoPublisherRetryInterval = 30 * time.Second
	topoPublisherWriteTimeout  = 5 * time.Second
)

// Backoff for the record check that follows a regained topology connection.
// Every pooler sees the connection come back at the same moment, so the first
// check waits a random time up to topoRecoveryBackoffBase, and later attempts
// back off further, to avoid flooding a server that has just recovered.
// They are vars only so tests can shorten them.
var (
	topoRecoveryBackoffBase = 5 * time.Second
	topoRecoveryBackoffMax  = time.Minute
)

// MutablePoolerRecordState is the slice of the Multipooler topology entry
// that callers can change through Mutate and Unregister. The Mutate /
// Unregister callbacks receive a pointer to a struct populated with the
// current values, so a caller can read and conditionally update them.
//
// All other Multipooler proto fields (Id, ShardRef, PoolerDir, PgDataDir,
// Hostname, PortMap) are set at construction and the record treats them as
// immutable — exposing only this struct in the mutation API makes that
// contract a property of the type system rather than a runtime check.
type MutablePoolerRecordState struct {
	// TODO: ServingStatus and LifecycleStatus are still owned here (the record is
	// their source of truth). The direction is to move their ownership into the
	// StateManager so the record only reflects effective state — see the ownership
	// TODO on StateManager.
	ServingStatus   clustermetadatapb.PoolerServingStatus
	LifecycleStatus *clustermetadatapb.PoolerLifecycle
	// RoutingState is the pooler's self-reported routing/HA advertisement. Set
	// ONLY when this pooler is the writable PRIMARY; replicas — and a consensus
	// leader not yet writable — leave it nil. Published into etcd so multigateway
	// can bootstrap write routing on discovery.
	RoutingState *clustermetadatapb.RoutingState
}

// poolerTopoStore is the subset of topoclient.Store used by poolerRecord.
type poolerTopoStore interface {
	RegisterMultipooler(ctx context.Context, multipooler *clustermetadatapb.Multipooler, allowUpdate bool) error
	GetMultipooler(ctx context.Context, id *clustermetadatapb.ID) (*topoclient.MultipoolerInfo, error)
	CreateMultipooler(ctx context.Context, multipooler *clustermetadatapb.Multipooler) error
	ConnRecoveries(ctx context.Context, cell string) (<-chan struct{}, error)
}

// poolerRecord is the single owner of the local Multipooler topology entry.
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
//
// A separate recovery loop covers a topology server that comes back without
// the record, for example after the etcd was wiped or replaced. It fires only
// when the connection to the topology server is lost and regained, and then
// re-creates the record if it is missing. Steady state adds no etcd load, so
// the cost does not grow with the number of poolers.
type poolerRecord struct {
	logger     *slog.Logger
	topoClient poolerTopoStore

	desired       atomic.Pointer[clustermetadatapb.Multipooler]
	lastPublished atomic.Pointer[clustermetadatapb.Multipooler]

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
//
// The seed need not carry a PoolerType: it is a derived, publish-only label (see
// Type / routingStateForPublish). Returns an error to preserve the constructor's
// signature; it does not currently fail.
func newPoolerRecord(logger *slog.Logger, topoClient poolerTopoStore, initial *clustermetadatapb.Multipooler) (*poolerRecord, error) {
	r := &poolerRecord{
		logger:     logger,
		topoClient: topoClient,
		wakeup:     make(chan struct{}, 1),
	}
	r.desired.Store(proto.Clone(initial).(*clustermetadatapb.Multipooler))
	return r, nil
}

// Id returns the pooler's identity. Effectively immutable — Mutate must not
// change it.
func (r *poolerRecord) Id() *clustermetadatapb.ID { return r.desired.Load().Id }

// ShardRef returns the pooler's shard identity. Effectively immutable.
func (r *poolerRecord) ShardRef() *clustermetadatapb.ShardRef { return r.desired.Load().ShardRef }

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

// Type returns the pooler's derived PoolerType label (see typeForState). It is
// computed from lifecycle + routing_state, not read from the stored proto field:
// PoolerType is a publish-only projection, set on the wire copy by
// routingStateForPublish and never held as internal state.
func (r *poolerRecord) Type() clustermetadatapb.PoolerType {
	m := r.desired.Load()
	return typeForState(m.LifecycleStatus, m.RoutingState)
}

// ServingStatus returns the current serving status.
func (r *poolerRecord) ServingStatus() clustermetadatapb.PoolerServingStatus {
	return r.desired.Load().ServingStatus
}

// RoutingState returns the pooler's recorded routing/HA advertisement, or nil if
// this pooler is not currently the writable PRIMARY of its shard.
func (r *poolerRecord) RoutingState() *clustermetadatapb.RoutingState {
	return r.desired.Load().RoutingState
}

// Snapshot returns a deep clone of the current desired state. Use this when
// passing the record to code that requires a *Multipooler value and may
// mutate it locally.
func (r *poolerRecord) Snapshot() *clustermetadatapb.Multipooler {
	return proto.Clone(r.desired.Load()).(*clustermetadatapb.Multipooler)
}

// Mutate atomically applies fn to the current MutablePoolerRecordState
// and schedules an asynchronous publish. fn receives a struct populated
// with the current values; any modifications it makes become the new
// desired state. Fields not exposed in MutablePoolerRecordState (Id,
// ShardRef, PoolerDir, etc.) cannot be touched.
//
// ctx must carry an action lock (see actionlock.AssertActionLockHeld). The action lock
// serialises state transitions across the whole manager — RPC handlers
// (promotion, demotion, type change) and lifecycle paths (Open, closeLocked)
// all reach StateManager.Mutate with an action-locked ctx
// threaded through from the caller. Mutate returns the assertion error
// without applying fn if the lock is not held.
//
// fn must not block or call back into poolerRecord; it should perform
// simple field assignments only.
//
// Returns an error (without applying fn) if the resulting state violates the
// consistency invariant between Type and routing_state: a persisted routing_state
// must be PRIMARY and pairs with Type == PRIMARY (see validateState). Callers must
// keep the two fields in sync.
func (r *poolerRecord) Mutate(ctx context.Context, fn func(*MutablePoolerRecordState)) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return err
	}
	r.applyMutation(fn)

	// Non-blocking send: if the channel is already full, a publish is already
	// pending and will pick up the latest desired state.
	select {
	case r.wakeup <- struct{}{}:
	default:
	}
	return nil
}

// applyMutation clones the current desired proto, hands a
// MutablePoolerRecordState view to fn, then atomically stores the result with the
// published PoolerType recomputed from the mutated lifecycle + routing state (see
// typeForState). Caller is responsible for sequencing (action lock, publisher
// state). It cannot fail: PoolerType is derived, so there is no cross-field
// invariant to violate.
func (r *poolerRecord) applyMutation(fn func(*MutablePoolerRecordState)) {
	current := r.desired.Load()
	state := MutablePoolerRecordState{
		ServingStatus:   current.ServingStatus,
		LifecycleStatus: current.LifecycleStatus,
		RoutingState:    current.RoutingState,
	}
	fn(&state)
	next := proto.Clone(current).(*clustermetadatapb.Multipooler)
	next.ServingStatus = state.ServingStatus
	next.LifecycleStatus = state.LifecycleStatus
	next.RoutingState = state.RoutingState
	r.desired.Store(next)
}

// typeForState derives the published PoolerType — a pure projection, never stored
// independently, so it can never drift from the routing state. A shutting-down
// pooler is UNKNOWN; otherwise PRIMARY iff the routing role is PRIMARY (the
// writable leader), else REPLICA.
func typeForState(lifecycle *clustermetadatapb.PoolerLifecycle, routing *clustermetadatapb.RoutingState) clustermetadatapb.PoolerType {
	if lifecycle.GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN {
		return clustermetadatapb.PoolerType_UNKNOWN
	}
	if routing.GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return clustermetadatapb.PoolerType_PRIMARY
	}
	return clustermetadatapb.PoolerType_REPLICA
}

// poolerTypeFromRoutingRole projects a routing role onto the PoolerType label
// (PRIMARY iff the writable primary, else REPLICA). Used at publish boundaries
// that still emit the deprecated label — the Status RPC field and the backup
// annotation — sourced directly from the routing role.
func poolerTypeFromRoutingRole(role clustermetadatapb.RoutingRole) clustermetadatapb.PoolerType {
	if role == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return clustermetadatapb.PoolerType_PRIMARY
	}
	return clustermetadatapb.PoolerType_REPLICA
}

// routingStateForPublish produces the etcd form of a record. Two projections
// happen only on the wire, never in stored state:
//   - PoolerType is stamped from the derived label (typeForState) — it is a
//     publish-only field, kept for the external operator until it migrates to
//     routing_state.
//   - The writable PRIMARY's routing_state (role + rule) is persisted as-is.
//     Replicas publish role only, with rule dropped: a replica's highest intended
//     rule bumps frequently and carries no external meaning, so publishing it
//     would churn etcd on every bump. Dropping just the rule field (rather than
//     the whole routing_state, as before) lets external readers key off
//     routing_state.role for primary/replica identity without depending on the
//     deprecated Type label. A shutting-down pooler publishes no routing_state
//     at all, matching its UNKNOWN Type.
//
// Returns a clone; the input is not mutated.
func routingStateForPublish(m *clustermetadatapb.Multipooler) *clustermetadatapb.Multipooler {
	out := proto.Clone(m).(*clustermetadatapb.Multipooler)
	poolerType := typeForState(out.LifecycleStatus, out.RoutingState)
	//nolint:staticcheck // SA1019: PoolerType is a publish-only projection for the external operator; removal pending its migration to routing_state.
	out.Type = poolerType
	switch poolerType {
	case clustermetadatapb.PoolerType_PRIMARY:
		// Keep as published: role + rule are both the writable leader's
		// authoritative advertisement.
	case clustermetadatapb.PoolerType_REPLICA:
		out.RoutingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA}
	default:
		out.RoutingState = nil
	}
	return out
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
		r.publisherWG.Go(func() {
			r.recoveryLoop(ctx)
		})

		// Kick off initial registration retry loop. The unregister callback
		// is a no-op — the shutdown write is handled by Unregister itself
		// (via Mutate + final publish) so toporeg only needs to manage the
		// retry goroutine's lifetime.
		registerFunc := func(ctx context.Context) error {
			return r.topoClient.RegisterMultipooler(ctx, routingStateForPublish(r.Snapshot()), true /* allowUpdate */)
		}
		r.tr = toporeg.Register(registerFunc, func(context.Context) error { return nil }, alarm)
	})
}

// Unregister stops the publisher, applies an optional final mutation,
// performs one synchronous publish if the result diverges from the last
// published state, and cancels the toporeg retry goroutine.
//
// finalize lets the caller stamp a shutdown state (e.g. Type=UNKNOWN,
// ServingStatus=DISABLED). The callback receives a MutablePoolerRecordState
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
		r.applyMutation(finalize)
	}

	desired := r.desired.Load()
	if desired != nil {
		pub := routingStateForPublish(desired)
		if !proto.Equal(pub, r.lastPublished.Load()) {
			if err := r.topoClient.RegisterMultipooler(ctx, pub, true); err != nil {
				r.logger.WarnContext(ctx, "final publish during Unregister failed; topology may be stale",
					"error", err,
					"routing_role", pub.GetRoutingState().GetRole().String(),
					"serving_status", pub.ServingStatus.String())
			} else {
				r.lastPublished.Store(proto.Clone(pub).(*clustermetadatapb.Multipooler))
			}
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
	if desired == nil {
		return
	}
	// Reduce to the etcd form (rule kept only for the writable PRIMARY), then
	// dedup against the last published form: a replica's frequent
	// highest-intended-rule bumps reduce to an identical form and never churn etcd.
	pub := routingStateForPublish(desired)
	if proto.Equal(pub, r.lastPublished.Load()) {
		return
	}

	r.logger.InfoContext(ctx, "publishing multipooler state to topology",
		"routing_role", pub.GetRoutingState().GetRole().String(),
		"serving_status", pub.ServingStatus.String())

	publishCtx, cancel := context.WithTimeout(ctx, topoPublisherWriteTimeout)
	defer cancel()

	if err := r.topoClient.RegisterMultipooler(publishCtx, pub, true); err != nil {
		r.logger.ErrorContext(ctx, "failed to publish multipooler state to topology; will retry",
			"error", err,
			"routing_role", pub.GetRoutingState().GetRole().String(),
			"serving_status", pub.ServingStatus.String())
		return
	}

	r.lastPublished.Store(proto.Clone(pub).(*clustermetadatapb.Multipooler))

	r.logger.InfoContext(ctx, "published multipooler state to topology",
		"routing_role", pub.GetRoutingState().GetRole().String(),
		"serving_status", desired.ServingStatus.String())
}

// recoveryLoop re-creates the topology record after the connection to the
// topology server is regained, if the record is missing. It exits when ctx is
// cancelled.
//
// It follows the connection for the pooler's cell. When that connection is
// replaced, for example after the cell's server addresses change, it
// subscribes to the new one and checks the record once, because the new server
// may not have it.
func (r *poolerRecord) recoveryLoop(ctx context.Context) {
	cell := r.Id().GetCell()
	subscribe := retry.New(time.Second, 30*time.Second)
	subscribed := false
	for _, err := range subscribe.Attempts(ctx) {
		if err != nil {
			return
		}
		recoveries, err := r.topoClient.ConnRecoveries(ctx, cell)
		if err != nil {
			r.logger.WarnContext(ctx, "failed to follow topology connection; will retry",
				"cell", cell, "error", err)
			continue
		}
		if subscribed {
			r.restoreWithBackoff(ctx)
		}
		subscribed = true

		resetTimer := time.AfterFunc(time.Minute, subscribe.Reset)
		for open := true; open; {
			select {
			case <-ctx.Done():
				resetTimer.Stop()
				return
			case _, open = <-recoveries:
				if open {
					r.logger.InfoContext(ctx, "topology connection regained; checking multipooler record")
					r.restoreWithBackoff(ctx)
				}
			}
		}
		resetTimer.Stop()
	}
}

// restoreWithBackoff runs restoreIfMissing until it gets a definite answer,
// starting after a random delay and backing off between failed attempts.
func (r *poolerRecord) restoreWithBackoff(ctx context.Context) {
	backoff := retry.New(topoRecoveryBackoffBase, topoRecoveryBackoffMax, retry.WithInitialDelay())
	for _, err := range backoff.Attempts(ctx) {
		if err != nil {
			return
		}
		if r.restoreIfMissing(ctx) {
			return
		}
	}
}

// restoreIfMissing re-creates the topology record when it no longer exists. It
// returns false when the outcome is unknown and the caller should retry.
//
// It uses an atomic create-only write, so a registration that landed in
// between wins and is never overwritten. A record that exists is left alone
// even if its contents differ. A SHUTDOWN lifecycle never restores. That state
// is only set after Unregister has cancelled the publisher and waited for it,
// so the guard is defense in depth against a future path that sets it while the
// publisher runs. The real fence is Unregister's cancel-then-wait on ctx.
func (r *poolerRecord) restoreIfMissing(ctx context.Context) (done bool) {
	desired := r.desired.Load()
	if desired == nil ||
		desired.LifecycleStatus.GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN {
		return true
	}

	checkCtx, cancel := context.WithTimeout(ctx, topoPublisherWriteTimeout)
	defer cancel()

	_, err := r.topoClient.GetMultipooler(checkCtx, desired.Id)
	if err == nil {
		return true
	}
	if !errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
		r.logger.WarnContext(ctx, "failed to check multipooler record in topology; will retry",
			"error", err)
		return false
	}

	pub := routingStateForPublish(desired)
	err = r.topoClient.CreateMultipooler(checkCtx, pub)
	switch {
	case err == nil:
		r.logger.InfoContext(ctx, "restored missing multipooler record in topology",
			"routing_role", pub.GetRoutingState().GetRole().String(),
			"serving_status", pub.ServingStatus.String())
		return true
	case errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}):
		// Someone registered between the check and the create; keep theirs.
		return true
	case errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}):
		// The write is create-only, so NoNode cannot mean the record is
		// missing. It means the cell definition is missing from the global
		// topology, as after a wipe of the global etcd. Only an operator
		// restoring the cell fixes it, and retrying picks the pooler back up
		// once they do.
		r.logger.WarnContext(ctx, "cell definition missing from global topology; will retry restoring the multipooler record",
			"cell", pub.Id.GetCell(),
			"error", err)
		return false
	default:
		r.logger.WarnContext(ctx, "failed to restore missing multipooler record in topology; will retry",
			"error", err,
			"routing_role", pub.GetRoutingState().GetRole().String(),
			"serving_status", pub.ServingStatus.String())
		return false
	}
}

// RegisterPreparing publishes a DISABLED topology entry for an unmanaged pooler
// before it reads any metadata. It runs before the manager exists; once the
// manager starts, the normal record lifecycle takes ownership of the entry.
//
// Registering first closes a gap: a restarted pooler must not leave a stale
// SERVING entry in topology while it is still deciding whether it may serve.
// It overwrites an earlier entry of the same pooler, which is how a restart
// replaces its predecessor's.
func RegisterPreparing(ctx context.Context, ts poolerTopoStore, initial *clustermetadatapb.Multipooler) error {
	record, err := newPoolerRecord(nil, ts, initial)
	if err != nil {
		return err
	}
	rec := routingStateForPublish(record.Snapshot())
	rec.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	return record.topoClient.RegisterMultipooler(ctx, rec, true /* allowUpdate */)
}
