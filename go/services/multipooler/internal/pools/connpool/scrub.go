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

package connpool

import (
	"context"
	"time"
)

// The session-state scrubber is the safety net for the gateway-authoritative
// session model: the pool trusts each connection's tracked settings label
// absolutely (pointer-equal reuse and clean-stack reuse run zero SQL), so any
// session mutation that escaped tracking — a set_config hidden in a routine
// body, a tracking bug, an out-of-band DDL — silently leaks to the next
// borrower. The scrubber periodically probes idle connections through
// SessionStateVerifier, emits metrics when the real backend state diverges
// from the label, and closes and replaces divergent connections.
//
// It only touches idle connections, so it adds no latency to the query path.
// It is a sampler: it narrows the leak window and raises an alarm, but the
// gateway's tracking gates remain the correctness boundary.

// scrubProbeTimeout bounds one verification probe. Kept well under
// PoolCloseTimeout so an in-flight probe cannot stall pool shutdown.
const scrubProbeTimeout = 5 * time.Second

// scrubStackCount is the number of idle stacks the scrubber rotates over:
// the clean stack plus the settings stacks.
const scrubStackCount = stackMask + 2

// scrubStack maps a rotation index to an idle stack: 0 is the clean stack,
// 1..stackMask+1 are the settings stacks.
func (pool *Pool[C]) scrubStack(i int) *connStack[C] {
	if i == 0 {
		return &pool.clean
	}
	return &pool.states[i-1]
}

// scrubPop pops an idle connection like pool.pop, but also returns the
// connection's pre-borrow timeUsed value so the scrubber can restore it: a
// scrub must not refresh the idle clock, or a small pool would have every
// connection kept forever-fresh by scrubbing and the idle-timeout worker
// could never shrink it. The stamp read before borrow is reliable: the only
// concurrent writer for an in-stack connection is the expirer, and if it won
// the race the borrow fails and the connection (already closed by the
// expirer) is skipped.
func (pool *Pool[C]) scrubPop(stack *connStack[C]) (*Pooled[C], time.Duration) {
	for conn, ok := stack.Pop(); ok; conn, ok = stack.Pop() {
		stamp := conn.timeUsed.get()
		if conn.timeUsed.borrow() {
			return conn, stamp
		}
	}
	return nil, 0
}

// scrubOne probes one idle connection for session-state divergence, rotating
// the starting stack across calls so every bucket gets coverage. A clean
// connection returns to the pool with its idle clock intact; a divergent one
// is closed and replaced. Returns false to stop the worker when the pool's
// connections do not implement SessionStateVerifier (a wiring error).
func (pool *Pool[C]) scrubOne(cursor *int) bool {
	if pool.Capacity() == 0 {
		return true
	}

	var conn *Pooled[C]
	var stamp time.Duration
	for i := range scrubStackCount {
		idx := (*cursor + i) % scrubStackCount
		if conn, stamp = pool.scrubPop(pool.scrubStack(idx)); conn != nil {
			*cursor = (idx + 1) % scrubStackCount
			break
		}
	}
	if conn == nil {
		return true
	}

	verifier, ok := Connection(conn.Conn).(SessionStateVerifier)
	if !ok {
		pool.logger.Error("session-state scrubbing enabled on a pool whose connections cannot verify session state; disabling scrubber",
			"pool", pool.Name)
		conn.timeUsed.set(stamp)
		pool.tryReturnConn(conn)
		return false
	}

	ctx, cancel := context.WithTimeout(pool.ctx, scrubProbeTimeout)
	div, err := verifier.VerifySessionState(ctx)
	cancel()

	pool.Metrics.scrubChecked.Add(1)
	pool.scrubMetrics.RecordCheck(pool.ctx, pool.poolType)

	switch {
	case err != nil:
		pool.Metrics.scrubErrors.Add(1)
		pool.scrubMetrics.RecordError(pool.ctx, pool.poolType)
		if conn.Conn.IsClosed() {
			// The probe killed the connection (dead socket); free the slot
			// and replace it, mirroring closeIdleResources.
			pool.closedConn()
			pool.scrubReplace()
		} else {
			// The probe failed but the connection is alive (e.g. timeout);
			// don't punish the connection for a probe problem.
			pool.logger.Warn("session-state scrub probe failed",
				"pool", pool.Name, "error", err)
			conn.timeUsed.set(stamp)
			pool.tryReturnConn(conn)
		}
	case div.IsDiverged():
		pool.Metrics.scrubDivergent.Add(1)
		pool.scrubMetrics.RecordDivergence(pool.ctx, pool.poolType, div)
		// Divergence means tracking was bypassed, and the session GUCs are
		// only the observable part of what the untracked code did: fail
		// closed and replace the backend rather than reconcile it.
		// SessionDivergence carries GUC names only, never values.
		pool.logger.Warn("session-state divergence detected; replacing backend",
			"pool", pool.Name,
			"untracked", div.Untracked,
			"phantom", div.Phantom,
			"mismatched", div.Mismatched)
		conn.Close()
		pool.closedConn()
		pool.scrubReplace()
	default:
		conn.timeUsed.set(stamp)
		pool.tryReturnConn(conn)
	}
	return true
}

// scrubReplace opens a replacement connection for a slot the scrubber freed.
// A failed replacement is not retried here: the freed capacity lets the next
// Get open a connection on demand.
func (pool *Pool[C]) scrubReplace() {
	if conn, err := pool.getNew(pool.ctx); err == nil && conn != nil {
		pool.tryReturnConn(conn)
	}
}
