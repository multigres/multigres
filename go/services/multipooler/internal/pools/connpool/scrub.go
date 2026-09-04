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

// The scrubber is the safety net for state the pool trusts without
// verification: the gateway-authoritative session model reuses connections
// with zero SQL (pointer-equal labels, clean-stack reuse), so any backend
// mutation that escaped tracking — a set_config hidden in a routine body, a
// tracking bug, an out-of-band DDL — silently leaks to the next borrower.
// The scrubber periodically runs every registered ConnChecker against idle
// connections, emits metrics when real backend state diverges from tracked
// state, and closes and replaces divergent connections.
//
// It only touches idle connections, so it adds no latency to the query path.
// It is a sampler: it narrows the leak window and raises an alarm, but the
// tracking gates remain the correctness boundary.
//
// Coverage: the scrubber works in passes. Each tick probes, from the next
// non-empty stack, the topmost idle connection not yet probed in the current
// pass, and marks it with the pass number; once every idle connection in
// every stack carries the current pass number, the next pass begins. Probed
// connections return to the top of their stack exactly as a recycled one
// would, so LIFO reuse and idle-timeout shrinking are untouched — simply
// re-probing the top each tick would never reach the connections beneath
// it, since clients also take and return connections at the top.

// scrubProbeTimeout bounds one checker's probe; each checker gets its own
// budget, so a slow early probe cannot starve later checkers into a timeout
// that the fail-closed error path would turn into needless churn. Pool close
// cancels pool.scrubCtx before draining, so in-flight probes never stall
// shutdown; this timeout only bounds probes against a hung backend.
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

// scrubPop takes the topmost idle connection in stack that has not been
// probed in the current pass, marks it probed, and also returns the
// connection's pre-borrow timeUsed value so the scrubber can restore it: a
// scrub must not refresh the idle clock, or a small pool would have every
// connection kept forever-fresh by scrubbing and the idle-timeout worker
// could never shrink it. The stamp read before borrow is reliable: the only
// concurrent writer for an in-stack connection is the expirer, and if it won
// the race the borrow fails and the connection (already closed by the
// expirer) is left for pool.pop to discard. Returns nil when every idle
// connection in the stack has already been probed this pass.
func (pool *Pool[C]) scrubPop(stack *connStack[C]) (*Pooled[C], time.Duration) {
	var stamp time.Duration
	conn, ok := stack.PopFirst(func(c *Pooled[C]) bool {
		if c.scrubPass == pool.scrubPass {
			return false
		}
		stamp = c.timeUsed.get()
		return c.timeUsed.borrow()
	})
	if !ok {
		return nil, 0
	}
	conn.scrubPass = pool.scrubPass
	return conn, stamp
}

// scrubNext finds the next connection to probe: the first unprobed idle
// connection in stack rotation order from *cursor. If every idle connection
// has been probed this pass, it starts the next pass and looks once more, so
// a tick only comes up empty when the pool has no idle connection at all.
func (pool *Pool[C]) scrubNext(cursor *int) (*Pooled[C], time.Duration) {
	for range 2 {
		for i := range scrubStackCount {
			idx := (*cursor + i) % scrubStackCount
			if conn, stamp := pool.scrubPop(pool.scrubStack(idx)); conn != nil {
				*cursor = (idx + 1) % scrubStackCount
				return conn, stamp
			}
		}
		pool.scrubPass++
	}
	return nil, 0
}

// scrubOne runs every registered checker against one idle connection: the
// next one not yet probed in the current pass, rotating the starting stack
// across calls so every bucket gets coverage (see scrubNext). A clean
// connection returns to the pool with its idle clock intact; a divergent
// one, or one whose state could not be verified, is closed and replaced. The
// bool return keeps the runWorker signature; it is always true.
func (pool *Pool[C]) scrubOne(cursor *int) bool {
	if pool.Capacity() == 0 || len(pool.checkers) == 0 {
		return true
	}

	conn, stamp := pool.scrubNext(cursor)
	if conn == nil {
		return true
	}
	// The scrubber holds the connection like a borrower, so Available and
	// the idle-limit math stay accurate while the probe is in flight.
	pool.borrowed.Add(1)

	// Run every checker, collecting per-checker findings. A checker error
	// ends the loop: the connection's state is unverified and it is replaced
	// below (fail closed). A dead connection ends the loop too — later
	// checkers cannot produce verdicts on it.
	type finding struct {
		checker string
		div     Divergence
	}
	var findings []finding
	var checkErr error
	var errChecker string
	for _, checker := range pool.checkers {
		ctx, cancel := context.WithTimeout(pool.scrubCtx, scrubProbeTimeout)
		div, err := checker.Check(ctx, conn.Conn)
		cancel()
		if err != nil {
			checkErr, errChecker = err, checker.Name()
			break
		}
		if div.IsDiverged() {
			findings = append(findings, finding{checker: checker.Name(), div: div})
		}
		if conn.Conn.IsClosed() {
			break
		}
	}

	pool.Metrics.scrubChecked.Add(1)
	pool.scrubMetrics.RecordCheck(pool.ctx, pool.poolType)
	if checkErr != nil {
		pool.Metrics.scrubErrors.Add(1)
		pool.scrubMetrics.RecordError(pool.ctx, pool.poolType, errChecker)
	}
	pool.borrowed.Add(-1)

	switch {
	case len(findings) > 0:
		pool.Metrics.scrubDivergent.Add(1)
		// Divergence means tracking was bypassed, and what a checker sees is
		// only the observable part of what the untracked code did: fail
		// closed and replace the backend rather than reconcile it.
		// Divergence carries names only, never values.
		for _, f := range findings {
			pool.scrubMetrics.RecordDivergence(pool.ctx, pool.poolType, f.checker, f.div)
			pool.logger.Warn("session-state divergence detected; replacing backend",
				"pool", pool.Name,
				"checker", f.checker,
				"untracked", f.div.Untracked,
				"phantom", f.div.Phantom,
				"mismatched", f.div.Mismatched)
		}
		conn.Close()
		pool.closedConn()
		pool.scrubReplace()
	case checkErr != nil || conn.Conn.IsClosed():
		// No verdict: either a check killed the connection (dead socket) or a
		// checker failed (timeout, probe error). An unverified backend may
		// still carry hidden state, and a client could induce probe failures
		// deliberately (e.g. a tiny tracked statement_timeout), so fail
		// closed and replace it. Churn is bounded to one connection per
		// scrub tick.
		if checkErr != nil {
			pool.logger.Warn("connection state check failed; replacing backend",
				"pool", pool.Name, "checker", errChecker, "error", checkErr)
		}
		if !conn.Conn.IsClosed() {
			conn.Close()
		}
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
