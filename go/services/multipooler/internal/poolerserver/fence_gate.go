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
// new work with MTF02. The gateway then retries the statement with backoff
// until the gate admits it.
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
// Closing the gate of a primary drains it the way every drain runs (see
// drain.go), with a phase of its own (fenceGate.drainPhase), so the fence
// drain and the not-serving drain never move each other's phase: first new
// reservations are refused while single queries continue and the open
// transactions finish; then single queries are refused too and the in-flight
// ones are waited out. At the timeout the pools are sealed, the backends still
// lent are terminated (their transactions get 40001) and each kill is
// confirmed, and the drain waits until nothing admitted before it is left.
// While the drain runs the gate is draining; it is closed only once the drain
// has ended the fence's work, so a closed gate is a drained one. A drain that
// could not (a kill not confirmed, or work still left at its bound) leaves the
// gate draining with no drain running: it refuses all new work, the pools stay
// sealed, and the next read that finds the fence drains again.
//
// Transitions, each under s.mu:
//
//	from            | event                                  | to
//	----------------+----------------------------------------+---------
//	unread or open  | a read finds a fence, on a primary     | draining (the reader runs the drain)
//	unread or open  | a read finds a fence, on a replica     | closed (no drain)
//	draining        | its drain ends, in the same epoch      | closed
//	draining        | its drain fails                        | draining, with no drain running
//	draining        | a read finds a fence, no drain running | draining (the reader drains again)
//	draining        | a read finds no fence                  | open (the drain is cancelled)
//	closed          | a read finds no fence                  | open
//	unread          | a read finds no fence                  | open
//	draining        | the pooler stops being primary         | closed (the drain is cancelled)
//	any but off     | the pooler becomes primary (reset)     | unread (any drain is cancelled)
//
// Exactly one caller runs a drain: the one whose read moved the gate to
// draining. A read that finds a fence while the gate is draining joins that
// drain: it waits, bounded by its own context, for the drain to end. A
// cancelled drain stops and releases its seal hold, and its caller and every
// joiner get ABORTED and read the fences again. A failed drain's caller and
// joiners get its ErrDrainIncomplete (UNAVAILABLE).

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
	// A gate whose drain failed stays draining with no drain running.
	drain *fenceDrain
	// failedSeal is the seal hold a failed drain kept: the pools stay sealed
	// until the next drain takes it over, or the fence is lifted or the
	// pooler's role changes (see endFenceDrainLocked).
	failedSeal *bool
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
	// ended is closed when the drain ends: it finished, failed or was
	// cancelled.
	ended chan struct{}
	// seal is the drain's hold on the pool seal (see drain.go).
	seal *bool
	// err is the drain's error when it failed (ErrDrainIncomplete), set
	// under s.mu before ended is closed.
	err error
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
		s.releaseFailedSealLocked()
		s.fence.reset()
	case role != servingstate.RoutingRolePrimary && wasPrimary && s.fence.state == fenceGateDraining:
		s.endFenceDrainLocked()
		s.fence.state = fenceGateClosed
	}
	s.routingRole = role
}

// endFenceDrainLocked cancels the running fence drain, which then releases
// its own seal hold, and releases the hold a failed drain kept. Caller must
// hold s.mu.
func (s *QueryPoolerServer) endFenceDrainLocked() {
	s.releaseFailedSealLocked()
	s.fence.endDrain()
}

// releaseFailedSealLocked releases the seal hold a failed fence drain kept,
// if any. Caller must hold s.mu (sealMu is taken under it).
func (s *QueryPoolerServer) releaseFailedSealLocked() {
	if h := s.fence.failedSeal; h != nil {
		s.fence.failedSeal = nil
		s.releaseSeal(h)
	}
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
// by drainTimeout (see the bounds above), the way every drain runs (drain.go).
// The drain is not bound to ctx: a caller that gives up must not leave
// reserved connections open on a pooler that refuses new work. A read that
// finds a drain running waits for it to end, bounded by ctx. When the drain
// ApplyFence ran or waited for is cancelled, ApplyFence fails with ABORTED;
// when it failed, with its ErrDrainIncomplete (UNAVAILABLE), and the gate
// stays draining: it refuses all new work, and the next read that finds the
// fence drains again. ApplyFence returns nil once the gate is open, or closed
// and drained.
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
		s.endFenceDrainLocked()
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
		if d := g.drain; d != nil {
			s.mu.Unlock()
			return s.awaitFenceDrain(ctx, read.Epoch, d)
		}
		// The last drain failed: drain again.
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
	d := &fenceDrain{cancel: cancel, ended: make(chan struct{}), seal: new(bool)}
	g.drainPhase = drainReserved
	if g.failedSeal != nil {
		// The last drain failed and kept the pools sealed: this one takes
		// over its hold, released once this drain ends the fence's work,
		// and refuses everything from the start, as the failed one did.
		d.seal = g.failedSeal
		g.failedSeal = nil
		g.drainPhase = drainRegular
	}
	g.state = fenceGateDraining
	g.drain = d
	s.mu.Unlock()
	s.logger.InfoContext(ctx, "fence gate closing; draining", "from", from.String())

	drainErr := s.runDrain(drainCtx, drainSpec{
		reason:  drainReasonFence,
		role:    role,
		timeout: s.fenceDrainTimeout(drainTimeout),
		phase:   &g.drainPhase,
		seal:    d.seal,
		refusal: mterrors.MTF02.New(),
	})

	s.mu.Lock()
	if g.drain != d {
		s.mu.Unlock()
		return errFenceDrainCancelled()
	}
	g.drain = nil
	if drainErr != nil {
		// The drain could not end the fence's work: stay draining, refusing
		// all new work, with the pools sealed, until a later read drains
		// again or the fence is lifted.
		d.err = drainErr
		g.failedSeal = d.seal
		g.drainPhase = drainRegular
		close(d.ended)
		s.mu.Unlock()
		return drainErr
	}
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

// awaitFenceDrain waits, bounded by ctx, for the running fence drain d to
// end, then reports how it ended: nil if the gate is closed in the same epoch,
// d's error if it failed, ABORTED if it was cancelled.
func (s *QueryPoolerServer) awaitFenceDrain(ctx context.Context, epoch uint64, d *fenceDrain) error {
	select {
	case <-d.ended:
	case <-ctx.Done():
		code := mtrpcpb.Code_CANCELED
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			code = mtrpcpb.Code_DEADLINE_EXCEEDED
		}
		return mterrors.Errorf(code, "gave up waiting for the running fence drain: %v", ctx.Err())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if d.err != nil {
		return d.err
	}
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
