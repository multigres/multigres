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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// Group 4 of the PgBouncer port — reserved-connection lifecycle & timeout,
// ported from test_timeouts.py (the idle-in-transaction killer).
//
// A "reserved" (pinned) connection keeps one physical backend attached to a
// client session across statements when the work is non-poolable — an open
// transaction, a temp table, a WITH HOLD cursor, COPY, or LISTEN
// (go/common/protoutil/reservation.go). There is no "prepared" reason: a bare
// named Parse does not pin (that is Group 1's concern).
//
// Two behaviors are exercised here:
//   - The reserved-inactivity timeout (--connpool-user-reserved-inactivity-timeout,
//     multigres's idle-in-transaction killer): an idle pinned backend is reaped,
//     and the session's next statement fails cleanly rather than hanging. Client
//     activity resets the timer, so an active session is never reaped.
//   - The pin/release boundary: while pinned, every statement in the session runs
//     on the SAME backend even under pool churn that would otherwise rotate it;
//     once the reservation ends (COMMIT), the session returns to the pool and its
//     statements rotate across backends again.
//
// All assertions are black-box: the session reads pg_backend_pid() from its own
// result rows (a normal value, not a side channel) and observes only query
// success/failure. The timeout tests use NewIsolated clusters started with a
// short inactivity timeout via WithMultipoolerExtraArgs.

// shortReservedInactivityTimeout reaps an idle pinned backend after ~2s (the
// reaper ticks at timeout/10). Short enough to keep the test quick, long enough
// that a sub-second statement cadence comfortably keeps a session alive.
const shortReservedInactivityTimeout = "--connpool-user-reserved-inactivity-timeout=2s"

// TestReservedConnectionInactivityTimeout opens a transaction (pinning a
// backend), lets it sit idle past the inactivity timeout, and asserts the next
// statement fails cleanly — the multigres equivalent of PostgreSQL's
// idle_in_transaction_session_timeout killing an abandoned transaction. It then
// asserts the session recovers: a fresh statement succeeds on a pooled backend,
// proving the reaped reservation was reclaimed rather than wedging the session.
func TestReservedConnectionInactivityTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
		shardsetup.WithMultipoolerExtraArgs(shortReservedInactivityTimeout),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	ctx := utils.WithTimeout(t, 60*time.Second)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	conn := connectGateway(t, ctx, gatewayDSN)
	defer conn.Close(ctx)

	// Pin a backend: BEGIN, then a statement inside the transaction reserves it.
	_, err := conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	pinnedPid := queryBackendPid(t, ctx, conn)
	t.Logf("transaction pinned backend pid %d", pinnedPid)

	// Idle past the inactivity timeout so the reaper kills the pinned backend.
	time.Sleep(4 * time.Second)

	// The next statement on the reaped reservation must fail cleanly (a returned
	// error within the deadline), not hang.
	stmtCtx := utils.WithShortDeadline(t)
	_, err = conn.Exec(stmtCtx, "SELECT 1")
	require.Error(t, err, "a statement on a reservation reaped for inactivity must fail, not succeed or hang")
	t.Logf("statement after inactivity-reap failed cleanly: %v", err)

	// The session must recover. Roll back the dead transaction (best effort) and
	// confirm a fresh statement succeeds — proving the backend was reclaimed.
	_, _ = conn.Exec(ctx, "ROLLBACK")
	require.Eventually(t, func() bool {
		recoverCtx := utils.WithShortDeadline(t)
		var n int
		return conn.QueryRow(recoverCtx, "SELECT 1").Scan(&n) == nil && n == 1
	}, 15*time.Second, 250*time.Millisecond,
		"after the reserved backend is reaped the session must serve again on a pooled connection")
}

// TestReservedConnectionSurvivesWithActivity is the inverse control: a
// transaction that keeps issuing statements faster than the inactivity timeout
// is never reaped, and — because it is pinned — every statement runs on the same
// backend. Proves the timeout is inactivity-based (reset by activity) and that a
// reservation pins one backend for the session's duration.
func TestReservedConnectionSurvivesWithActivity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
		shardsetup.WithMultipoolerExtraArgs(shortReservedInactivityTimeout),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	ctx := utils.WithTimeout(t, 60*time.Second)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	conn := connectGateway(t, ctx, gatewayDSN)
	defer conn.Close(ctx)

	_, err := conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	pinnedPid := queryBackendPid(t, ctx, conn)

	// Six statements at a 1s cadence spans 5s — well past the 2s timeout in
	// aggregate, but no single gap reaches it, so the pinned backend survives and
	// stays the same throughout.
	for i := range 6 {
		time.Sleep(time.Second)
		stmtCtx := utils.WithShortDeadline(t)
		var n int
		require.NoErrorf(t, conn.QueryRow(stmtCtx, "SELECT 1").Scan(&n),
			"statement %d should keep the pinned backend alive (activity resets the inactivity timer)", i)
		require.Equalf(t, pinnedPid, queryBackendPid(t, ctx, conn),
			"a pinned transaction must stay on backend %d across statements", pinnedPid)
	}

	_, err = conn.Exec(ctx, "COMMIT")
	require.NoError(t, err)
}

// TestTransactionPinsBackendThenReleases proves the reservation boundary: inside
// a transaction every statement runs on the same backend even under pool churn
// that would otherwise rotate it (Group 1 shows non-pinned sessions DO rotate);
// after COMMIT the session rejoins the pool and its statements rotate across
// backends again. Uses the shared cluster (no fault injection, default timeouts).
func TestTransactionPinsBackendThenReleases(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 90*time.Second)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	conn := connectGateway(t, ctx, gatewayDSN)
	defer conn.Close(ctx)

	stopChurn := startPoolChurn(t, ctx, gatewayDSN, churnClients)
	defer stopChurn()

	// Pinned: every statement in the transaction must hit the same backend,
	// despite the churn rotating the pool underneath.
	_, err := conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	pinnedPid := queryBackendPid(t, ctx, conn)
	for i := range minExecutes {
		require.Equalf(t, pinnedPid, queryBackendPid(t, ctx, conn),
			"statement %d inside the transaction must stay pinned to backend %d", i, pinnedPid)
	}
	t.Logf("transaction stayed pinned to backend %d across %d statements under churn", pinnedPid, minExecutes)

	_, err = conn.Exec(ctx, "COMMIT")
	require.NoError(t, err)

	// Released: back in the pool, the session's statements rotate across backends
	// again (>=2 distinct), confirming the reservation ended at COMMIT.
	runAcrossBackends(t, ctx, func(ctx context.Context, _ int) int {
		return queryBackendPid(t, ctx, conn)
	})
}

// queryBackendPid reads the backend pid serving the connection's current
// statement, via the connection's own result row.
func queryBackendPid(t *testing.T, ctx context.Context, conn *pgx.Conn) int {
	t.Helper()
	var pid int
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))
	return pid
}
