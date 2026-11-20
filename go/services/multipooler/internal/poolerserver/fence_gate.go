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
// Two checks decide whether ApplyFence applies a read, each against a stale
// read of its own kind:
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
// Closing the gate drains the pooler like a not-serving transition, in two
// stages under one timeout, with a phase of its own (fenceGate.drainPhase), so
// the fence drain and the not-serving drain never move each other's phase:
// first new reservations are refused while single queries continue and the
// open transactions finish; then single queries are refused too and the
// in-flight ones are waited out. Each stage also waits for the requests that
// were admitted before it began and do not hold a connection yet (see
// StartRequest). At the timeout the remaining reserved connections are
// force-closed, and their transactions get 40001.

// fenceGateState is the state of the fence gate.
type fenceGateState int

const (
	// fenceGateOff: the gate never refuses anything. A server is built with
	// it off, and minigres, which has nothing to fence, leaves it off.
	fenceGateOff fenceGateState = iota
	// fenceGateUnread: no fence read has been applied since the gate was
	// armed or the pooler last became primary. A primary refuses new work.
	fenceGateUnread
	// fenceGateOpen: the last fence read found no fence on the pooler's scope.
	fenceGateOpen
	// fenceGateClosed: the last fence read found a fence. A primary refuses
	// new work, except single queries during stage 1 of the fence drain.
	fenceGateClosed
)

// fenceGate is the admission fence gate's state on a QueryPoolerServer,
// guarded by the server's mu: StartRequest reads it under the lock it already
// holds.
type fenceGate struct {
	state fenceGateState
	// drainPhase is the stage of the fence drain while the gate closes on a
	// primary. It is separate from the not-serving drain's phase, so the two
	// drains never move each other's phase.
	drainPhase drainPhase
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

// admits reports whether the gate, on a primary, admits a new request of kind.
// An open or off gate admits everything; a closed gate admits single queries
// during stage 1 of the fence drain; an unread or otherwise closed gate admits
// nothing new.
func (g *fenceGate) admits(kind RequestKind) bool {
	switch g.state {
	case fenceGateOff, fenceGateOpen:
		return true
	case fenceGateClosed:
		return g.drainPhase == drainReserved && kind == RequestSingleQuery
	default:
		return false
	}
}

// reset makes the gate unread, advances its epoch and wakes the fence reader.
func (g *fenceGate) reset() {
	g.state = fenceGateUnread
	g.drainPhase = drainNone
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

// setRoutingRoleLocked sets the routing role, and resets an armed fence gate
// when the pooler becomes primary: it did not enforce fences as a replica, so
// the fences are read again before it admits primary work. Caller must hold
// s.mu.
func (s *QueryPoolerServer) setRoutingRoleLocked(role servingstate.RoutingRole) {
	if role == servingstate.RoutingRolePrimary && s.routingRole != servingstate.RoutingRolePrimary && s.fence.state != fenceGateOff {
		s.fence.reset()
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
// reader: unread, a read before it admits work again; closed, a periodic
// re-read, so an unfence it missed is eventually applied. Both are false on a
// replica and on a gate that is off or open.
func (s *QueryPoolerServer) FenceReadState() (unread, closed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.routingRole != servingstate.RoutingRolePrimary {
		return false, false
	}
	return s.fence.state == fenceGateUnread, s.fence.state == fenceGateClosed
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

// ApplyFence applies a fence read: a fence closes the gate, otherwise it
// opens. It applies nothing, and the caller reads again, when the read was
// taken before the gate was last reset (ABORTED), or when its snapshot is
// older than the floor (UNAVAILABLE). Applying the state the gate already has
// is a no-op.
//
// Closing the gate on a primary runs the fence drain before returning, bounded
// by drainTimeout, or the grace period when drainTimeout is not positive. The
// drain is not bound to ctx: a caller that gives up must not leave reserved
// connections open on a pooler that refuses new work, so the drain always ends
// in a completed drain or a force-close. On a replica the gate closes without a
// drain: it is consulted only once the pooler is primary, and becoming primary
// resets it anyway.
//
// Callers must serialize ApplyFence calls.
func (s *QueryPoolerServer) ApplyFence(ctx context.Context, read FenceRead, drainTimeout time.Duration) error {
	s.mu.Lock()
	if s.fence.state == fenceGateOff {
		s.mu.Unlock()
		return nil
	}
	if read.Epoch != s.fence.epoch {
		s.mu.Unlock()
		return mterrors.New(mtrpcpb.Code_ABORTED, "the pooler became primary while its fences were read; read them again")
	}
	if read.Position.Compare(s.fence.floor) < 0 {
		floor := s.fence.floor
		s.mu.Unlock()
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"the catalog snapshot read (routing version %d) is older than one the pooler already applied (routing version %d); read again",
			read.Position.RoutingVersion, floor.RoutingVersion)
	}
	s.fence.floor = read.Position
	from := s.fence.state
	if !read.Fenced {
		s.fence.state = fenceGateOpen
		s.fence.drainPhase = drainNone
		s.mu.Unlock()
		if from != fenceGateOpen {
			s.logger.InfoContext(ctx, "fence gate opened", "from", from.String())
		}
		return nil
	}
	if from == fenceGateClosed {
		s.mu.Unlock()
		return nil
	}
	s.fence.state = fenceGateClosed
	s.fence.notify()
	role := s.routingRole
	if role != servingstate.RoutingRolePrimary {
		s.mu.Unlock()
		s.logger.InfoContext(ctx, "fence gate closed", "from", from.String(), "routing_role", role.String())
		return nil
	}
	s.fence.drainPhase = drainReserved
	s.mu.Unlock()
	s.logger.InfoContext(ctx, "fence gate closing; draining", "from", from.String())

	if drainTimeout <= 0 {
		drainTimeout = s.gracePeriod
	}
	s.runFenceDrain(context.WithoutCancel(ctx), drainTimeout, role)

	s.mu.Lock()
	// An unfence cannot have run meanwhile (calls are serialized), but a
	// promotion reset can: leave the gate as the reset left it.
	if s.fence.epoch == read.Epoch {
		s.fence.drainPhase = drainNone
	}
	s.mu.Unlock()
	s.logger.InfoContext(ctx, "fence gate closed")
	return nil
}

// runFenceDrain runs the two stages of the fence drain under one timeout (none
// when timeout is not positive), then force-closes the reserved connections
// left at the timeout. It records the drain as the not-serving drain does,
// with reason fence.
func (s *QueryPoolerServer) runFenceDrain(ctx context.Context, timeout time.Duration, role servingstate.RoutingRole) {
	if s.poolManager == nil {
		return
	}
	start := time.Now()
	drainCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		drainCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	outcome := drainOutcomeGraceful
	// Stage 1: wait for transactions to finish while single queries flow.
	if err := s.poolManager.WaitForReservedDrain(drainCtx); err != nil {
		outcome = drainOutcomeForceClose
	} else {
		// Stage 2: refuse single queries too, and wait out the in-flight ones.
		s.setFenceDrainPhase(drainRegular)
		if err := s.poolManager.WaitForDrain(drainCtx); err != nil {
			outcome = drainOutcomeForceClose
		}
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

// setFenceDrainPhase moves the fence drain to phase under the mutex.
func (s *QueryPoolerServer) setFenceDrainPhase(p drainPhase) {
	s.mu.Lock()
	s.fence.drainPhase = p
	s.mu.Unlock()
}
