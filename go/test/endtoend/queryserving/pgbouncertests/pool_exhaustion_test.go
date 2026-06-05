// Copyright 2026 Supabase, Inc.
// Portions derived from PgBouncer (ISC License),
// Copyright (c) 2007-2009 Marko Kreen, Skype Technologies OÜ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgbouncertests

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// Group 3 of the PgBouncer port — pool exhaustion / capacity, ported from
// test_limits.py.
//
// PgBouncer rejects connections once a pool is full; multigres instead BLOCKS:
// when the multipooler's per-user regular pool is at capacity, a borrow waits on
// a waitlist until a connection is returned (see
// go/services/multipooler/pools/connpool/{pool.go,waitlist.go}). So the ported
// assertions differ from PgBouncer's — we prove the proxy queues and proceeds
// rather than erroring, and that a waiter whose context is cancelled is removed
// from the waitlist cleanly (no leak, no deadlock).
//
// These are black-box tests over the gateway PostgreSQL wire path. Each runs in
// its own NewIsolated cluster started with a deliberately tiny connpool
// capacity. The multipooler splits global capacity into a regular and a reserved
// sub-pool (reserved-ratio) and a fair-share rebalancer (rebalance-interval)
// settles each user's slice; with --connpool-global-capacity=4
// --connpool-reserved-ratio=0.5 the single test user's regular pool settles to 2
// backends. We never assert the exact number though — only that concurrency is
// capped well below the number of statements launched, which holds for any small
// capacity and decisively catches a pool that fails to limit at all.

const (
	// smallPoolArgs starts a multipooler with a tiny connection pool so a
	// handful of concurrent statements exhausts it. With ratio 0.5 the regular
	// (non-pinned) sub-pool gets half of 4 = 2 backends for the lone test user;
	// the short rebalance interval makes that slice settle within a couple of
	// seconds. Reads default to the primary, so all load lands on one pool.
	connpoolGlobalCapacity = "--connpool-global-capacity=4"
	connpoolReservedRatio  = "--connpool-reserved-ratio=0.5"
	connpoolRebalanceFast  = "--connpool-rebalance-interval=1s"

	// concurrencyCeiling is a loose upper bound on the settled regular-pool
	// capacity for one user under this config (settles to 2; 6 leaves slack for
	// the pre-rebalance initial capacity and any internal user split). The point
	// is that it is far below loadStatements, so a pool that does not limit at
	// all (peak == loadStatements) fails loudly.
	concurrencyCeiling = 6

	// loadStatements is how many concurrent slow statements we launch — many
	// more than the pool can run at once, so most must queue.
	loadStatements = 12

	// holderStatements saturate the pool for the waiter-cancellation test; more
	// than any plausible capacity here so the probe is guaranteed to block.
	holderStatements = 8
)

// TestPoolExhaustionBlocksThenProceeds launches far more concurrent slow
// statements than the pool can serve at once and asserts that (a) every one
// eventually succeeds — multigres queues rather than rejecting — and (b) the
// number actually executing on postgres at any instant stayed capped well below
// the number launched, proving the pool serialized them across a limited set of
// backends. Ported from test_limits.py::test_pool_size.
func TestPoolExhaustionBlocksThenProceeds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
		shardsetup.WithMultipoolerExtraArgs(connpoolGlobalCapacity, connpoolReservedRatio, connpoolRebalanceFast),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	ctx := utils.WithTimeout(t, 120*time.Second)

	gatewayDB := openGatewayDB(t, setup)
	defer gatewayDB.Close()
	monitorDB := openDirectPostgresDB(t, setup)
	defer monitorDB.Close()

	settleConnpool(t, ctx, gatewayDB)

	// Sample, via a connection that bypasses the pool entirely, how many of our
	// slow statements run on postgres concurrently.
	peak := newSleepConcurrencyMonitor(ctx, monitorDB)

	var wg sync.WaitGroup
	errs := make([]error, loadStatements)
	for i := range loadStatements {
		wg.Go(func() {
			_, errs[i] = gatewayDB.ExecContext(ctx, "SELECT pg_sleep(1)")
		})
	}
	wg.Wait()
	observedPeak := peak.stop()

	for i, err := range errs {
		require.NoErrorf(t, err, "statement %d must succeed — multigres queues on a full pool, it does not reject", i)
	}
	t.Logf("peak concurrent backends running pg_sleep: %d (launched %d concurrently)", observedPeak, loadStatements)

	require.GreaterOrEqual(t, observedPeak, 1, "monitor should have observed at least one statement executing")
	require.Less(t, observedPeak, loadStatements,
		"the pool must cap concurrency below the number launched; peak==launched means the pool did not limit at all")
	assert.LessOrEqualf(t, observedPeak, concurrencyCeiling,
		"concurrency (%d) should settle near the tiny configured capacity, not balloon", observedPeak)
}

// TestPoolExhaustionWaiterCancelledCleanly saturates the pool, then issues one
// more statement with a short deadline. That statement must BLOCK on the
// waitlist (not be rejected) and then fail when its context expires; the elapsed
// time proves it waited the full deadline rather than running immediately.
// Finally, after the holders release, a fresh statement must succeed — proving
// the cancelled waiter was removed from the waitlist cleanly, leaking no slot and
// causing no deadlock. Ported from test_limits.py (pool-full waiter behavior).
func TestPoolExhaustionWaiterCancelledCleanly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
		shardsetup.WithMultipoolerExtraArgs(connpoolGlobalCapacity, connpoolReservedRatio, connpoolRebalanceFast),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	ctx := utils.WithTimeout(t, 120*time.Second)

	gatewayDB := openGatewayDB(t, setup)
	defer gatewayDB.Close()
	monitorDB := openDirectPostgresDB(t, setup)
	defer monitorDB.Close()

	settleConnpool(t, ctx, gatewayDB)

	// Saturate the pool with long-running holders. More than any plausible
	// capacity here, so some run and the rest queue — the pool is full.
	holderCtx, cancelHolders := context.WithCancel(ctx)
	var holders sync.WaitGroup
	for range holderStatements {
		holders.Go(func() {
			_, _ = gatewayDB.ExecContext(holderCtx, "SELECT pg_sleep(30)")
		})
	}
	// Wait until statements are actually executing on postgres, then give the
	// remaining holders a moment to pile onto the waitlist.
	waitForActiveSleeps(t, ctx, monitorDB, 1)
	time.Sleep(time.Second)

	// The probe must block on the full pool, then be cancelled by its deadline.
	const probeDeadline = 800 * time.Millisecond
	probeCtx, probeCancel := context.WithTimeout(ctx, probeDeadline)
	start := time.Now()
	_, probeErr := gatewayDB.ExecContext(probeCtx, "SELECT 1")
	elapsed := time.Since(start)
	probeCancel()

	require.Error(t, probeErr, "a statement against a saturated pool must block then fail on its deadline, not return early")
	assert.GreaterOrEqualf(t, elapsed, probeDeadline-200*time.Millisecond,
		"probe should have blocked ~%s waiting for a slot, but returned after %s — suggests it was rejected/served, not queued", probeDeadline, elapsed)
	t.Logf("probe against saturated pool blocked for %s then failed: %v", elapsed, probeErr)

	// Release the holders; the pool must recover and serve the cancelled-waiter's
	// successor cleanly — proof the timed-out waiter was removed from the waitlist.
	cancelHolders()
	holders.Wait()

	require.Eventually(t, func() bool {
		recoverCtx := utils.WithShortDeadline(t)
		_, err := gatewayDB.ExecContext(recoverCtx, "SELECT 1")
		return err == nil
	}, 20*time.Second, 250*time.Millisecond,
		"after the holders release, the pool must serve again — a leaked or deadlocked waiter would block this forever")
}

// --- shared helpers for the pool-exhaustion tests ---

// openGatewayDB opens a database/sql pool against the gateway (primary path).
// database/sql multiplexes many client connections, so the multipooler's pool —
// not the client — is the concurrency limiter under test.
func openGatewayDB(t *testing.T, setup *shardsetup.ShardSetup) *sql.DB {
	t.Helper()
	dsn := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)
	return db
}

// openDirectPostgresDB opens a small database/sql pool straight to the primary's
// postgres, bypassing the gateway and its connection pool. Used as a side channel
// to observe execution concurrency even while the pool under test is saturated.
func openDirectPostgresDB(t *testing.T, setup *shardsetup.ShardSetup) *sql.DB {
	t.Helper()
	dsn := shardsetup.GetTestUserDSN("localhost", setup.GetPrimary(t).Pgctld.PgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(3)
	return db
}

// settleConnpool creates the test user's pool with a warmup query, then waits for
// the fair-share rebalancer (1s interval) to clamp the user's regular slice down
// to its small global allocation before the test measures capacity.
func settleConnpool(t *testing.T, ctx context.Context, gatewayDB *sql.DB) {
	t.Helper()
	_, err := gatewayDB.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err, "warmup query should succeed")
	time.Sleep(5 * time.Second)
}

// activeSleepQuery counts backends on postgres currently executing one of our
// pg_sleep statements (excluding the monitor's own counting query).
const activeSleepQuery = `SELECT count(*) FROM pg_stat_activity ` +
	`WHERE state = 'active' AND query LIKE '%pg_sleep%' AND query NOT LIKE '%pg_stat_activity%'`

// waitForActiveSleeps blocks until at least min of our slow statements are
// executing on postgres, confirming load has reached the backend.
func waitForActiveSleeps(t *testing.T, ctx context.Context, monitorDB *sql.DB, min int) {
	t.Helper()
	require.Eventually(t, func() bool {
		var n int
		sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := monitorDB.QueryRowContext(sctx, activeSleepQuery).Scan(&n); err != nil {
			return false
		}
		return n >= min
	}, 15*time.Second, 100*time.Millisecond, "expected at least %d statements executing on postgres", min)
}

// sleepConcurrencyMonitor polls the direct-postgres connection and records the
// peak number of concurrently executing pg_sleep statements.
type sleepConcurrencyMonitor struct {
	done chan struct{}
	wg   sync.WaitGroup
	peak atomic.Int64
}

// newSleepConcurrencyMonitor starts sampling immediately; call stop() to end
// sampling and read the peak.
func newSleepConcurrencyMonitor(ctx context.Context, monitorDB *sql.DB) *sleepConcurrencyMonitor {
	m := &sleepConcurrencyMonitor{done: make(chan struct{})}
	m.wg.Go(func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-m.done:
				return
			case <-ticker.C:
				var n int64
				sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				err := monitorDB.QueryRowContext(sctx, activeSleepQuery).Scan(&n)
				cancel()
				if err == nil && n > m.peak.Load() {
					m.peak.Store(n)
				}
			}
		}
	})
	return m
}

// stop ends sampling and returns the peak concurrency observed.
func (m *sleepConcurrencyMonitor) stop() int {
	close(m.done)
	m.wg.Wait()
	return int(m.peak.Load())
}
