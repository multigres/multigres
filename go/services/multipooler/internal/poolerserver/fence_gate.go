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

package poolerserver

import (
	"context"
	"errors"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/shardingcatalog"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// The fence gate closes a primary pooler to new application work while an
// operation that moves its data has fenced its scope (its whole tablegroup, or
// its shard) in the sharding catalog. It is a condition of its own, separate
// from the serving status: a fenced pooler stays SERVING, keeps its consensus
// role and reports the same health, and only StartRequest changes, refusing
// new work with MTF02. The gateway then waits for a catalog snapshot that no
// longer fences the target and runs the statement again.
//
// The gate guards application work only, admitted through StartRequest. The
// internal query service (InternalQueryService) never consults it: the
// manager's catalog reads and writes, the fence writes that lift a fence on
// the catalog shard included, must run while that shard is fenced.
//
// The manager decides whether the pooler is fenced, from a catalog snapshot it
// reads, and applies the answer with ApplyFence. The gate is fail-closed: once
// armed (EnableFenceGate), and again each time the pooler becomes primary, it
// is unread, refusing new work until the next fence read is applied, because a
// fence may have been raised while the pooler was not reading. Replicas are
// not fenced: the gate is consulted only while the routing role is PRIMARY.
//
// Fence reads may run concurrently (a RefreshFences call and the manager's
// background reader); the gate orders them. Every transition is made under
// s.mu, and ApplyFence makes two checks before it applies a read, each
// against a stale read of its own kind:
//
//   - The epoch refuses a read that began before the pooler last became
//     primary. A read takes a network round trip, and a promotion does not
//     wait for it. Such a read can predate a fence the operation wrote while
//     the pooler was not primary, and so did not refresh the pooler for: it
//     happens when the pooler loses and regains the primary role during one
//     read. Read again after the promotion, the catalog holds that fence.
//   - The floor refuses a read of a snapshot older, by consensus rule and then
//     routing version, than the newest one the gate has applied. During a
//     failover of the catalog shard, the topology may still name the deposed
//     catalog primary, which still answers, with an older snapshot. Once the
//     gate has applied the successor's snapshot, that answer cannot reopen a
//     gate the successor's snapshot closed. The floor survives promotions,
//     since a catalog failover can overlap the pooler's own.
//
// Neither check can do the other's job. A position says what a read saw, not
// when it began, so the floor accepts a pre-promotion read when nothing newer
// was applied. And a read begun after the promotion can still reach a deposed
// catalog primary.
//
// Closing the gate of a primary drains it like a not-serving transition, in
// two stages under one timeout, with a phase of its own (fenceGate.drainPhase),
// so the fence drain and the not-serving drain never move each other's phase:
// first new reservations are refused while single queries continue and the
// open transactions finish; then single queries are refused too and the
// in-flight ones are waited out. Each stage also waits for the requests that
// were admitted before it began and do not hold a connection yet (see
// StartRequest). At the timeout the remaining reserved connections are
// force-closed, and their transactions get 40001. While the drain runs the
// gate is draining; it is closed only once the drain has ended, so a closed
// gate is a drained one.
//
// Transitions, each under s.mu:
//
//	from            | event                                  | to
//	----------------+----------------------------------------+---------
//	unread or open  | a read finds a fence, on a primary     | draining (the reader runs the drain)
//	unread or open  | a read finds a fence, on a replica     | closed (no drain)
//	draining        | its drain ends, in the same epoch      | closed
//	draining        | a read finds no fence                  | open (the drain is cancelled)
//	closed          | a read finds no fence                  | open
//	unread          | a read finds no fence                  | open
//	draining        | the pooler stops being primary         | closed (the drain is cancelled)
//	any but off     | the pooler becomes primary (reset)     | unread (any drain is cancelled)
//
// Exactly one caller runs a drain: the one whose read moved the gate to
// draining. A read that finds a fence while the gate is draining joins that
// drain: it waits, bounded by its own context, for the drain to end. A
// cancelled drain force-closes nothing, and its caller and every joiner get
// ABORTED and read the fences again.

// Bounds of the fence drain. A drain is bounded by the caller's drain timeout,
// else by the pooler's drain grace period, else by defaultFenceDrainTimeout, so
// it always ends. A caller may not ask for more than MaxFenceDrainTimeout.
const (
	defaultFenceDrainTimeout = 30 * time.Second
	MaxFenceDrainTimeout     = 10 * time.Minute
)

// fenceGateState is the state of the fence gate.
type fenceGateState int

const (
	// fenceGateOff: the gate never refuses anything. A server is built with
	// it off, and minigres, which has nothing to fence, leaves it off.
	fenceGateOff fenceGateState = iota
	// fenceGateUnread: no fence read has been applied since the gate was
	// armed or the pooler last became primary. A primary refuses new work.
	fenceGateUnread
	// fenceGateOpen: the last fence read applied found no fence on the
	// pooler's scope.
	fenceGateOpen
	// fenceGateDraining: the last fence read applied found a fence, and the
	// fence drain it started is running. A primary refuses new work, except
	// single queries during stage 1 of the drain.
	fenceGateDraining
	// fenceGateClosed: the last fence read applied found a fence, and the
	// pooler is drained (or was a replica when the gate closed). A primary
	// refuses new work.
	fenceGateClosed
)

// fenceGate is the admission fence gate's state on a QueryPoolerServer,
// guarded by the server's mu: StartRequest reads it under the lock it already
// holds.
type fenceGate struct {
	state fenceGateState
	// drainPhase is the stage of the fence drain while the gate is draining.
	// It is separate from the not-serving drain's phase, so the two drains
	// never move each other's phase.
	drainPhase drainPhase
	// drain is the running fence drain while the gate is draining, else nil.
	drain *fenceDrain
	// epoch advances on every reset to unread, which is when the pooler
	// becomes primary. A read takes it before it reads the catalog, and
	// ApplyFence refuses the read (ABORTED) when it has changed: the read
	// began before the promotion (see the checks at the top of this file).
	epoch uint64
	// floor is the position of the newest snapshot a read applied. ApplyFence
	// refuses a read of an older one (UNAVAILABLE): a deposed catalog primary
	// may still answer (see the checks at the top of this file). Resets keep
	// it.
	floor shardingcatalog.Position
	// changed receives a value when the fence reader has something to do: the
	// gate was armed or reset, or it closed. Buffered, so a signal is never
	// lost and never blocks.
	changed chan struct{}
}

// fenceDrain is a running fence drain.
type fenceDrain struct {
	// cancel stops the drain without a force-close.
	cancel context.CancelFunc
	// ended is closed when the drain ends: it finished, or it was cancelled.
	ended chan struct{}
}

// admits reports whether the gate, on a primary, admits a new request of kind.
// An open or off gate admits everything; a draining gate admits single queries
// during stage 1 of the fence drain; an unread or closed gate admits nothing
// new.
func (g *fenceGate) admits(kind RequestKind) bool {
	switch g.state {
	case fenceGateOff, fenceGateOpen:
		return true
	case fenceGateDraining:
		return g.drainPhase == drainReserved && kind == RequestSingleQuery
	default:
		return false
	}
}

// endDrain cancels the running fence drain, if any, and wakes its joiners.
func (g *fenceGate) endDrain() {
	if g.drain != nil {
		g.drain.cancel()
		close(g.drain.ended)
		g.drain = nil
	}
	g.drainPhase = drainNone
}

// reset makes the gate unread, cancelling any drain, advances its epoch and
// wakes the fence reader.
func (g *fenceGate) reset() {
	g.endDrain()
	g.state = fenceGateUnread
	g.epoch++
	g.notify()
}

// notify wakes the fence reader (see QueryPoolerServer.FenceGateChanged).
func (g *fenceGate) notify() {
	select {
	case g.changed <- struct{}{}:
	default:
	}
}

// String names the gate state for logs.
func (g fenceGateState) String() string {
	switch g {
	case fenceGateOff:
		return "off"
	case fenceGateUnread:
		return "unread"
	case fenceGateOpen:
		return "open"
	case fenceGateDraining:
		return "draining"
	case fenceGateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// checkFenceLocked admits or refuses a request the drain matrix of
// StartRequest admitted. It reads only fields under s.mu, which the caller
// holds, and allocates nothing unless it refuses. Existing reserved
// connections never reach it.
func (s *QueryPoolerServer) checkFenceLocked(kind RequestKind) error {
	if s.routingRole != servingstate.RoutingRolePrimary || s.fence.admits(kind) {
		return nil
	}
	return mterrors.MTF02.New()
}

// startUnfencedRequest admits a request as StartRequest does, except that the
// fence gate does not apply: for requests that run no application work. A
// fence closes its scope to work that reads or writes the data being moved;
// these run none, and refusing them only hurts:
//
//   - a login's credential lookup (GetAuthCredentials): a client must be able
//     to connect to a fenced pooler, if only to get MTF02 for its statements,
//     which the gateway retries, rather than a failed login, which it does not;
//   - a Describe of a statement or portal, which an extended-protocol client
//     sends before every Execute: refused, it would fail the statement
//     outright, since the gateway retries only Execute.
//
// The target check and the not-serving drain still apply. The request is not
// counted for the drains (see StartRequest): it runs no application work, and
// it is admitted while the gate is closed, so counting it would keep a fence
// drain waiting on logins that never stop.
func (s *QueryPoolerServer) startUnfencedRequest(target *query.Target, kind RequestKind) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkDrainLocked(target, kind)
}

// EnableFenceGate arms the fence gate, unread, so a primary refuses new
// application work until the first fence read is applied. The manager calls it
// once, at construction, unless the pooler has nothing to fence (minigres).
func (s *QueryPoolerServer) EnableFenceGate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fence.state != fenceGateOff {
		return
	}
	s.fence.reset()
}

// setRoutingRoleLocked sets the routing role. When the pooler becomes primary
// it resets an armed fence gate: it did not enforce fences as a replica, so the
// fences are read again before it admits primary work. When it stops being
// primary it cancels a running fence drain and leaves the gate closed: a
// replica's gate is not consulted, and is reset before it is again. Caller
// must hold s.mu.
func (s *QueryPoolerServer) setRoutingRoleLocked(role servingstate.RoutingRole) {
	wasPrimary := s.routingRole == servingstate.RoutingRolePrimary
	switch {
	case role == servingstate.RoutingRolePrimary && !wasPrimary && s.fence.state != fenceGateOff:
		s.fence.reset()
	case role != servingstate.RoutingRolePrimary && wasPrimary && s.fence.state == fenceGateDraining:
		s.fence.endDrain()
		s.fence.state = fenceGateClosed
	}
	s.routingRole = role
}

// FenceEpoch returns the gate's epoch. A fence read takes it before it reads
// the catalog and passes it to ApplyFence.
func (s *QueryPoolerServer) FenceEpoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fence.epoch
}

// FenceGateChanged returns the channel that receives a value whenever the gate
// becomes unread or closes, which is when the fence reader has a read to do or
// a re-read to schedule.
func (s *QueryPoolerServer) FenceGateChanged() <-chan struct{} {
	return s.fence.changed
}

// FenceReadState reports what the gate of a primary needs from the fence
// reader: unread, a read before it admits work again; closed (draining or
// closed), a periodic re-read, so an unfence it missed is eventually applied.
// Both are false on a replica and on a gate that is off or open.
func (s *QueryPoolerServer) FenceReadState() (unread, closed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.routingRole != servingstate.RoutingRolePrimary {
		return false, false
	}
	return s.fence.state == fenceGateUnread,
		s.fence.state == fenceGateDraining || s.fence.state == fenceGateClosed
}

// FenceRead is one read of the fences on the pooler's scope, for ApplyFence.
type FenceRead struct {
	// Epoch is the gate's epoch (FenceEpoch), taken before the catalog was
	// read.
	Epoch uint64
	// Position is the position of the snapshot read: the zero Position when
	// the catalog primary has no catalog.
	Position shardingcatalog.Position
	// Fenced reports whether the snapshot fences the pooler's scope.
	Fenced bool
}

// ApplyFence applies a fence read (see the transitions at the top of this
// file): a fence closes the gate, otherwise it opens. It fails, applying
// nothing, with ABORTED when the read was taken before the gate was last
// reset, and with UNAVAILABLE when the snapshot read is older than the floor;
// either way the caller reads again. Applying a fence to a closed gate, or no
// fence to an open one, changes nothing.
//
// Closing the gate of a primary runs the fence drain before returning, bounded
// by drainTimeout (see the bounds above). The drain is not bound to ctx: a
// caller that gives up must not leave reserved connections open on a pooler
// that refuses new work. A read that finds the gate draining waits for that
// drain to end, bounded by ctx. When the drain ApplyFence ran or waited for is
// cancelled, ApplyFence fails with ABORTED. ApplyFence returns nil once the
// gate is open, or closed and drained.
func (s *QueryPoolerServer) ApplyFence(ctx context.Context, read FenceRead, drainTimeout time.Duration) error {
	s.mu.Lock()
	g := &s.fence
	if g.state == fenceGateOff {
		s.mu.Unlock()
		return nil
	}
	if read.Epoch != g.epoch {
		s.mu.Unlock()
		return mterrors.New(mtrpcpb.Code_ABORTED, "the pooler became primary while its fences were read; read them again")
	}
	if read.Position.Compare(g.floor) < 0 {
		floor := g.floor
		s.mu.Unlock()
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"the catalog snapshot read (routing version %d) is older than one the pooler already applied (routing version %d); read again",
			read.Position.RoutingVersion, floor.RoutingVersion)
	}
	g.floor = read.Position
	from := g.state
	if !read.Fenced {
		g.endDrain()
		g.state = fenceGateOpen
		s.mu.Unlock()
		if from != fenceGateOpen {
			s.logger.InfoContext(ctx, "fence gate opened", "from", from.String())
		}
		return nil
	}
	switch from {
	case fenceGateClosed:
		s.mu.Unlock()
		return nil
	case fenceGateDraining:
		ended := g.drain.ended
		s.mu.Unlock()
		return s.awaitFenceDrain(ctx, read.Epoch, ended)
	}
	g.notify()
	role := s.routingRole
	if role != servingstate.RoutingRolePrimary {
		g.state = fenceGateClosed
		s.mu.Unlock()
		s.logger.InfoContext(ctx, "fence gate closed", "from", from.String(), "routing_role", role.String())
		return nil
	}
	drainCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	d := &fenceDrain{cancel: cancel, ended: make(chan struct{})}
	g.state = fenceGateDraining
	g.drainPhase = drainReserved
	g.drain = d
	s.mu.Unlock()
	s.logger.InfoContext(ctx, "fence gate closing; draining", "from", from.String())

	s.runFenceDrain(drainCtx, d, s.fenceDrainTimeout(drainTimeout), role)

	s.mu.Lock()
	if g.drain != d {
		s.mu.Unlock()
		return errFenceDrainCancelled()
	}
	g.drain = nil
	g.drainPhase = drainNone
	g.state = fenceGateClosed
	close(d.ended)
	s.mu.Unlock()
	s.logger.InfoContext(ctx, "fence gate closed")
	return nil
}

// errFenceDrainCancelled is ApplyFence's answer when the drain it ran or
// joined was cancelled: the caller reads the fences again.
func errFenceDrainCancelled() error {
	return mterrors.New(mtrpcpb.Code_ABORTED, "the fence drain was cancelled: the pooler's role changed or its fence was lifted; read the fences again")
}

// awaitFenceDrain waits, bounded by ctx, for the running fence drain to end
// (ended is closed), then reports whether the gate is closed in the same
// epoch: nil if so, ABORTED if the drain was cancelled.
func (s *QueryPoolerServer) awaitFenceDrain(ctx context.Context, epoch uint64, ended <-chan struct{}) error {
	select {
	case <-ended:
	case <-ctx.Done():
		code := mtrpcpb.Code_CANCELED
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			code = mtrpcpb.Code_DEADLINE_EXCEEDED
		}
		return mterrors.Errorf(code, "gave up waiting for the running fence drain: %v", ctx.Err())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fence.epoch == epoch && s.fence.state == fenceGateClosed {
		return nil
	}
	return errFenceDrainCancelled()
}

// fenceDrainTimeout bounds a fence drain: requested if positive, else the
// drain grace period if positive, else defaultFenceDrainTimeout.
func (s *QueryPoolerServer) fenceDrainTimeout(requested time.Duration) time.Duration {
	switch {
	case requested > 0:
		return requested
	case s.gracePeriod > 0:
		return s.gracePeriod
	default:
		return defaultFenceDrainTimeout
	}
}

// runFenceDrain runs the two stages of drain d under timeout, then
// force-closes the reserved connections left at the timeout. ctx is cancelled
// when the drain is (see fenceGate.endDrain): the drain then stops at once and
// force-closes nothing. A cancellation that comes once the force-close has
// begun does not stop it. It records the drain as the not-serving drain does,
// with reason fence.
func (s *QueryPoolerServer) runFenceDrain(ctx context.Context, d *fenceDrain, timeout time.Duration, role servingstate.RoutingRole) {
	if s.poolManager == nil {
		return
	}
	start := time.Now()
	drainCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	outcome := drainOutcomeGraceful
	// Stage 1: wait for transactions to finish while single queries flow.
	if err := s.poolManager.WaitForReservedDrain(drainCtx); err != nil {
		outcome = drainOutcomeForceClose
	} else if s.setFenceDrainPhase(d, drainRegular) {
		// Stage 2: refuse single queries too, and wait out the in-flight ones.
		if err := s.poolManager.WaitForDrain(drainCtx); err != nil {
			outcome = drainOutcomeForceClose
		}
	}
	// Neither the force-close nor the records stop for a cancellation.
	cancelled := ctx.Err() != nil
	ctx = context.WithoutCancel(ctx)
	if cancelled {
		s.logger.InfoContext(ctx, "fence drain cancelled")
		s.drainStats.recordDrain(ctx, time.Since(start).Seconds(), drainOutcomeCancelled, drainReasonFence, role)
		return
	}
	if outcome == drainOutcomeForceClose {
		s.logger.WarnContext(ctx, "fence drain did not complete within its timeout, force-closing reserved connections", "timeout", timeout)
		killed := s.poolManager.CloseReservedConnections(ctx)
		s.drainStats.recordForceClosed(ctx, killed, drainReasonFence, role)
		if killed > 0 {
			s.logger.WarnContext(ctx, "force-closed reserved connections after fence drain timeout", "killed", killed)
		}
	}
	s.drainStats.recordDrain(ctx, time.Since(start).Seconds(), outcome, drainReasonFence, role)
}

// setFenceDrainPhase moves drain d to phase, and reports whether d is still
// the running drain (it was not cancelled).
func (s *QueryPoolerServer) setFenceDrainPhase(d *fenceDrain, p drainPhase) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fence.drain != d {
		return false
	}
	s.fence.drainPhase = p
	return true
}
