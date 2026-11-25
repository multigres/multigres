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
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// Draining
//
// Every drain runs the same way (runDrain): the not-serving drain of a
// transition out of SERVING, and the fence drain of a fenced primary. It
// refuses new work in two stages under one timeout: first new reservations,
// while single queries go on and the open transactions finish; then single
// queries too, while the in-flight ones finish. Each stage also waits for the
// requests admitted before it that hold no connection yet (see StartRequest).
//
// A drain that runs out of time ends what is left, so that once it reports
// drained nothing admitted before it still runs or can start:
//
//  1. It moves to the second stage, so nothing new is admitted.
//  2. It seals the user pools (connpoolmanager.SealPools): they lend nothing,
//     the requests waiting for a connection are refused, and so is every
//     request that asks for one later, with the drain's retryable error
//     (MTF01, or MTF02 for a fence).
//  3. It terminates the backends of the connections still lent, reserved
//     and regular, and confirms that each exited (KillLent). An unmanaged
//     pooler, whose database Multigres does not own, kills nothing.
//  4. It waits, bounded, until nothing is lent and nothing admitted is
//     left, then unseals the pools, which refill on demand.
//
// If a kill is not confirmed, or something is still lent or admitted at the
// bound, the drain fails with ErrDrainIncomplete and leaves the pools sealed:
// a request admitted before the drain must not get a connection later. The
// pools stay sealed until the condition the drain was for ends (the pooler
// serves again, or the fence is lifted), or a later drain succeeds. The
// manager's own queries use the admin pool, which is never sealed.
//
// Several drains can run at once (a fence drain during a not-serving drain);
// each holds the seal through a bool of its own (see holdSeal), and the pools
// are unsealed when the last hold is released.

// ErrDrainIncomplete is the error of a drain that ran out of time and could
// not end the work left: a kill was not confirmed, or something admitted
// before the drain was still running at its bound. The pools stay sealed.
var ErrDrainIncomplete = errors.New("drain incomplete")

// Bounds of the end of a timed-out drain, vars so tests can shorten them.
var (
	// drainKillTimeout is how long a drain waits for each backend it
	// terminates to exit.
	drainKillTimeout = 5 * time.Second
	// drainSettleTimeout is how long a drain, once it has ended the work
	// left, waits for nothing to be lent or admitted.
	drainSettleTimeout = 10 * time.Second
)

// drainIncomplete returns ErrDrainIncomplete with its cause, as UNAVAILABLE:
// the caller may retry.
func drainIncomplete(cause error) error {
	return mterrors.WithCode(fmt.Errorf("%w: %w", ErrDrainIncomplete, cause), mtrpcpb.Code_UNAVAILABLE)
}

// holdSeal takes a drain's hold on the pool seal, sealing the pools with
// refusal if no other hold is taken. Each drain holds through a bool of its
// own (held, guarded by sealMu), so it can only ever release its own hold.
// Taking a hold already taken does nothing.
func (s *QueryPoolerServer) holdSeal(held *bool, refusal error) {
	s.sealMu.Lock()
	defer s.sealMu.Unlock()
	if *held {
		return
	}
	*held = true
	s.sealHolds++
	if s.sealHolds == 1 {
		s.poolManager.SealPools(refusal)
	}
}

// releaseSeal releases a drain's hold on the pool seal, unsealing the pools if
// it was the last one. Releasing a hold not taken does nothing.
func (s *QueryPoolerServer) releaseSeal(held *bool) {
	s.sealMu.Lock()
	defer s.sealMu.Unlock()
	if !*held {
		return
	}
	*held = false
	s.sealHolds--
	if s.sealHolds == 0 {
		s.poolManager.UnsealPools()
	}
}

// drainSpec describes one drain for runDrain.
type drainSpec struct {
	// reason is the drain's reason attribute in the drain metrics.
	reason string
	// role is the routing role the drain is recorded with.
	role servingstate.RoutingRole
	// timeout bounds the two stages. Not positive means no bound: the drain
	// then never ends early.
	timeout time.Duration
	// phase is the drain's phase, which toSecondStage moves: the not-serving
	// drain's or the fence drain's, kept apart so that neither drain moves
	// the other's.
	phase *drainPhase
	// seal is the drain's hold on the pool seal.
	seal *bool
	// refusal is the error sealed pools refuse requests with.
	refusal error
}

// runDrain runs a drain (see the top of this file) and records it. It returns
// nil once the pooler is drained, the cause when ctx ended first (the drain
// was cancelled), and ErrDrainIncomplete when it could not end the work left.
// The drain's seal hold, which may be one an earlier failed drain left, is
// released unless the drain failed.
func (s *QueryPoolerServer) runDrain(ctx context.Context, d drainSpec) error {
	if s.poolManager == nil {
		return nil
	}
	start := time.Now()
	outcome, err := s.drain(ctx, d)
	logCtx := context.WithoutCancel(ctx)
	switch outcome {
	case drainOutcomeFailed:
		s.logger.WarnContext(logCtx, "drain could not end the work left; the pools stay sealed", "reason", d.reason, "error", err)
	case drainOutcomeCancelled:
		s.logger.InfoContext(logCtx, "drain cancelled", "reason", d.reason)
		s.releaseSeal(d.seal)
	default:
		// Drained: a hold an earlier, failed drain left is released too.
		s.releaseSeal(d.seal)
	}
	s.drainStats.recordDrain(logCtx, time.Since(start).Seconds(), outcome, d.reason, d.role)
	return err
}

// drain runs the drain's stages and, past the timeout, ends the work left;
// see runDrain.
func (s *QueryPoolerServer) drain(ctx context.Context, d drainSpec) (string, error) {
	stageCtx := ctx
	if d.timeout > 0 {
		var cancel context.CancelFunc
		stageCtx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}
	// Stage 1: wait for transactions to finish while single queries flow.
	if err := s.poolManager.WaitForReservedDrain(stageCtx); err == nil {
		if !s.toSecondStage(ctx, d.phase) {
			return drainOutcomeCancelled, ctx.Err()
		}
		// Stage 2: refuse single queries too, and wait out the in-flight ones.
		if err := s.poolManager.WaitForDrain(stageCtx); err == nil {
			return drainOutcomeGraceful, nil
		}
	}
	// Past the timeout, or cancelled: in either stage, refuse everything new.
	if !s.toSecondStage(ctx, d.phase) {
		return drainOutcomeCancelled, ctx.Err()
	}

	s.logger.WarnContext(ctx, "drain did not complete within its timeout; sealing the pools and ending the work left",
		"reason", d.reason, "timeout", d.timeout, "kill", s.killOnDrainTimeout)
	s.holdSeal(d.seal, d.refusal)
	if s.killOnDrainTimeout {
		killed, err := s.poolManager.KillLent(ctx, drainKillTimeout)
		s.drainStats.recordForceClosed(context.WithoutCancel(ctx), killed, d.reason, d.role)
		if killed > 0 {
			s.logger.WarnContext(ctx, "terminated connections still lent at the drain timeout", "reason", d.reason, "killed", killed)
		}
		if ctx.Err() != nil {
			return drainOutcomeCancelled, ctx.Err()
		}
		if err != nil {
			return drainOutcomeFailed, drainIncomplete(err)
		}
	}
	settleCtx, cancel := context.WithTimeout(ctx, drainSettleTimeout)
	defer cancel()
	if err := s.poolManager.WaitForDrain(settleCtx); err != nil {
		if ctx.Err() != nil {
			return drainOutcomeCancelled, ctx.Err()
		}
		return drainOutcomeFailed, drainIncomplete(fmt.Errorf("work admitted before the drain was still running %v after it was ended", drainSettleTimeout))
	}
	return drainOutcomeForceClose, nil
}

// toSecondStage moves a drain to its second stage, refusing single queries
// too, and reports whether the drain is still wanted. A cancelled drain (ctx
// done) moves nothing: a fence drain is cancelled under mu (fenceGate.
// endDrain), so a check under mu cannot race it.
func (s *QueryPoolerServer) toSecondStage(ctx context.Context, phase *drainPhase) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	*phase = drainRegular
	return true
}
