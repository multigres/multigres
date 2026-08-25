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

// scrubOne runs every registered checker against one idle connection,
// rotating the starting stack across calls so every bucket gets coverage. A
// clean connection returns to the pool with its idle clock intact; a
// divergent one is closed and replaced. The bool return keeps the runWorker
// signature; it is always true.
func (pool *Pool[C]) scrubOne(cursor *int) bool {
	if pool.Capacity() == 0 || len(pool.checkers) == 0 {
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

	// Run every checker, collecting per-checker findings. A checker error
	// means no verdict from that checker; findings already collected still
	// count (fail closed). A dead connection ends the loop — later checkers
	// cannot produce verdicts on it.
	type finding struct {
		checker string
		div     Divergence
	}
	var findings []finding
	var checkErr error
	var errChecker string
	ctx, cancel := context.WithTimeout(pool.ctx, scrubProbeTimeout)
	for _, checker := range pool.checkers {
		div, err := checker.Check(ctx, conn.Conn)
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
	cancel()

	pool.Metrics.scrubChecked.Add(1)
	pool.scrubMetrics.RecordCheck(pool.ctx, pool.poolType)
	if checkErr != nil {
		pool.Metrics.scrubErrors.Add(1)
		pool.scrubMetrics.RecordError(pool.ctx, pool.poolType, errChecker)
	}

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
	case conn.Conn.IsClosed():
		// A check killed the connection (dead socket); free the slot and
		// replace it, mirroring closeIdleResources.
		pool.closedConn()
		pool.scrubReplace()
	case checkErr != nil:
		// A checker failed but the connection is alive (e.g. timeout);
		// don't punish the connection for a probe problem.
		pool.logger.Warn("connection state check failed",
			"pool", pool.Name, "checker", errChecker, "error", checkErr)
		conn.timeUsed.set(stamp)
		pool.tryReturnConn(conn)
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
