// Copyright 2026 Supabase, Inc.
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

package queryserving

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// Session-level advisory locks (pg_advisory_lock / pg_advisory_lock_shared) live
// on a specific PostgreSQL backend and survive transaction boundaries. Under the
// gateway's pooled model a client's logical session is not pinned to one
// backend, so without special handling these locks would be acquired on whatever
// backend ran the acquiring statement and then lost (or leaked) on the next
// query. These tests verify the gateway pins the backend for the lifetime of a
// session-level advisory lock and scrubs it on release.

// skipIfNoRealPostgres skips a test when run in short mode or when the PostgreSQL
// binaries needed to stand up a real cluster are unavailable.
func skipIfNoRealPostgres(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("Skipping end-to-end test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries)")
	}
}

// openGatewayConn opens a single *sql.Conn to the multigateway PG port. Forcing a
// single underlying connection means every query goes through the same
// multigateway session (TCP connection), which is what lets us observe pinning.
func openGatewayConn(t *testing.T, setup *shardsetup.ShardSetup) *sql.Conn {
	t.Helper()
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// advisoryLockHeld reports whether the backend currently serving conn holds a
// single-key session advisory lock for key. PostgreSQL maps a one-argument
// bigint key to pg_locks as classid = key>>32, objid = key&0xffffffff,
// objsubid = 1; for the small keys used here classid is always 0. Scoping the
// probe to a specific key keeps assertions precise even though backends are
// shared across the pool (a leaked lock from another test on the same backend
// won't be miscounted).
func advisoryLockHeld(t *testing.T, conn *sql.Conn, key int) bool {
	t.Helper()
	var held bool
	err := conn.QueryRowContext(t.Context(),
		"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND classid = 0 AND objid = $1 AND objsubid = 1)",
		key).Scan(&held)
	require.NoError(t, err, "failed to probe advisory lock")
	return held
}

// ---------- Pinning on acquire ----------

// TestAdvisoryLock_ExclusiveLockPinsSession verifies that pg_advisory_lock pins
// the multigateway session to a single backend. Before the fix the session is
// not pinned, so the backend PID is free to change between queries.
func TestAdvisoryLock_ExclusiveLockPinsSession(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	conn := openGatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "SELECT pg_advisory_lock(101)")
	require.NoError(t, err)

	// After acquiring a session-level advisory lock the session must be pinned.
	utils.RequirePinned(t, conn)

	_, _ = conn.ExecContext(t.Context(), "SELECT pg_advisory_unlock_all()")
}

// TestAdvisoryLock_SharedLockPinsSession verifies pg_advisory_lock_shared pins
// the session.
func TestAdvisoryLock_SharedLockPinsSession(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	conn := openGatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "SELECT pg_advisory_lock_shared(102)")
	require.NoError(t, err)
	utils.RequirePinned(t, conn)

	_, _ = conn.ExecContext(t.Context(), "SELECT pg_advisory_unlock_all()")
}

// TestAdvisoryLock_TryLockPinsSession verifies pg_try_advisory_lock pins the
// session when it succeeds.
func TestAdvisoryLock_TryLockPinsSession(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	conn := openGatewayConn(t, setup)

	var acquired bool
	err := conn.QueryRowContext(t.Context(), "SELECT pg_try_advisory_lock(103)").Scan(&acquired)
	require.NoError(t, err)
	require.True(t, acquired, "try lock on a free key should succeed")
	utils.RequirePinned(t, conn)

	_, _ = conn.ExecContext(t.Context(), "SELECT pg_advisory_unlock_all()")
}

// TestAdvisoryLock_TwoKeyLockPinsSession verifies the two-argument
// pg_advisory_lock(key1, key2) form pins the session.
func TestAdvisoryLock_TwoKeyLockPinsSession(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	conn := openGatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "SELECT pg_advisory_lock(7, 9)")
	require.NoError(t, err)
	utils.RequirePinned(t, conn)

	_, _ = conn.ExecContext(t.Context(), "SELECT pg_advisory_unlock_all()")
}

// ---------- Lock visibility across queries ----------

// TestAdvisoryLock_VisibleAcrossQueries verifies a lock acquired in one query
// remains held (visible in pg_locks) on the same pinned backend in subsequent
// queries.
func TestAdvisoryLock_VisibleAcrossQueries(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	conn := openGatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "SELECT pg_advisory_lock(104)")
	require.NoError(t, err)
	pinnedPID := utils.RequirePinned(t, conn)

	// The advisory lock must remain visible across many subsequent queries, all
	// served by the same backend.
	for i := range 10 {
		require.Equal(t, pinnedPID, utils.GetBackendPID(t, conn), "PID must stay stable on query %d", i)
		require.True(t, advisoryLockHeld(t, conn, 104), "advisory lock must remain held on query %d", i)
	}

	_, _ = conn.ExecContext(t.Context(), "SELECT pg_advisory_unlock_all()")
}

// ---------- Unpin on release ----------

// TestAdvisoryLock_UnlockAllUnpins verifies pg_advisory_unlock_all() releases
// the lock and unpins the session.
func TestAdvisoryLock_UnlockAllUnpins(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	conn := openGatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "SELECT pg_advisory_lock(105)")
	require.NoError(t, err)
	utils.RequirePinned(t, conn)

	var released bool
	err = conn.QueryRowContext(t.Context(), "SELECT pg_advisory_unlock_all() IS NOT NULL").Scan(&released)
	require.NoError(t, err)

	// After unlocking everything, the lock should be gone on whatever backend we
	// land on next.
	require.False(t, advisoryLockHeld(t, conn, 105), "no advisory lock should remain after unlock_all")
}

// TestAdvisoryLock_UnlockReferenceCounted verifies that a key locked twice stays
// pinned after a single unlock (reference counted) and unpins only after the
// matching second unlock.
func TestAdvisoryLock_UnlockReferenceCounted(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	conn := openGatewayConn(t, setup)

	// Acquire the same key twice — PostgreSQL stacks session-level locks.
	_, err := conn.ExecContext(t.Context(), "SELECT pg_advisory_lock(106)")
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), "SELECT pg_advisory_lock(106)")
	require.NoError(t, err)
	pinnedPID := utils.RequirePinned(t, conn)

	// One unlock leaves the lock still held (count 1) — must remain pinned.
	var ok bool
	require.NoError(t, conn.QueryRowContext(t.Context(), "SELECT pg_advisory_unlock(106)").Scan(&ok))
	require.True(t, ok)
	require.Equal(t, pinnedPID, utils.GetBackendPID(t, conn), "must stay pinned while lock still held")
	require.True(t, advisoryLockHeld(t, conn, 106), "lock should still be held after one of two unlocks")

	// Second unlock fully releases the lock; the session should unpin.
	require.NoError(t, conn.QueryRowContext(t.Context(), "SELECT pg_advisory_unlock(106)").Scan(&ok))
	require.True(t, ok)
	require.False(t, advisoryLockHeld(t, conn, 106), "no advisory lock should remain after matching unlocks")
}

// ---------- DISCARD ALL ----------

// TestAdvisoryLock_DiscardAllReleases verifies DISCARD ALL through the gateway
// releases session-level advisory locks (PG's DISCARD ALL runs
// pg_advisory_unlock_all()).
func TestAdvisoryLock_DiscardAllReleases(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	conn := openGatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "SELECT pg_advisory_lock(107)")
	require.NoError(t, err)
	utils.RequirePinned(t, conn)

	_, err = conn.ExecContext(t.Context(), "DISCARD ALL")
	require.NoError(t, err)

	// The lock should be gone on any backend we reach after DISCARD ALL.
	require.False(t, advisoryLockHeld(t, conn, 107), "DISCARD ALL must release session advisory locks")
}

// ---------- No leak to next client ----------

// TestAdvisoryLock_NoLeakToNextClient verifies that when a session holding a
// session-level advisory lock disconnects without explicitly unlocking, the
// backend does not return to the pool still holding the lock — a new client can
// acquire the same key.
func TestAdvisoryLock_NoLeakToNextClient(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	// First client: take the lock and abandon it (disconnect without unlocking).
	db1, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	db1.SetMaxOpenConns(1)
	db1.SetMaxIdleConns(1)
	conn1, err := db1.Conn(t.Context())
	require.NoError(t, err)
	_, err = conn1.ExecContext(t.Context(), "SELECT pg_advisory_lock(108)")
	require.NoError(t, err)
	conn1.Close()
	db1.Close()

	// Second client: must be able to acquire the same key. If the backend leaked
	// the lock back into the pool, every backend still holding key 108 would
	// fail the try-lock; retry across backends to surface a leak deterministically.
	db2, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db2.Close()
	db2.SetMaxOpenConns(1)
	db2.SetMaxIdleConns(1)
	conn2, err := db2.Conn(t.Context())
	require.NoError(t, err)
	defer conn2.Close()

	require.Eventually(t, func() bool {
		var acquired bool
		if err := conn2.QueryRowContext(t.Context(), "SELECT pg_try_advisory_lock(108)").Scan(&acquired); err != nil {
			return false
		}
		if acquired {
			_, _ = conn2.ExecContext(t.Context(), "SELECT pg_advisory_unlock(108)")
		}
		return acquired
	}, 5*time.Second, 100*time.Millisecond, "key 108 must be acquirable — backend leaked the lock")
}

// ---------- Transaction-level locks are out of scope ----------

// TestAdvisoryLock_XactLockDoesNotPin documents that transaction-level advisory
// locks (pg_advisory_xact_lock) are out of scope: outside an explicit
// transaction they are released at statement end, so the session must NOT stay
// pinned.
func TestAdvisoryLock_XactLockDoesNotPin(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	conn := openGatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "SELECT pg_advisory_xact_lock(109)")
	require.NoError(t, err)

	// The xact lock was released at the end of the implicit transaction, so the
	// lock for key 109 should not remain held.
	assert.False(t, advisoryLockHeld(t, conn, 109), "xact advisory lock should not survive the statement")
}

// ---------- Extended query protocol (pgx, bound parameters) ----------
//
// pgx uses the extended protocol with a bound parameter for `pg_advisory_lock($1)`,
// which exercises a different gateway path than lib/pq's simple protocol: the
// portal is planned via resolvePortalPlan → Plan (so the advisory pin is set on
// the plan's ExecInfo) and executed through the reserved-connection portal path.
// These tests guard that path end to end.

// openGatewayPgxConn opens a single pgx connection to the multigateway. A single
// *pgx.Conn means every query rides the same gateway session, so pinning is
// observable via a stable backend PID.
func openGatewayPgxConn(t *testing.T, ctx context.Context, setup *shardsetup.ShardSetup) *pgx.Conn {
	t.Helper()
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
	cfg, err := pgx.ParseConfig(connStr)
	require.NoError(t, err)
	// QueryExecModeExec still uses the extended protocol with a server-bound
	// parameter (the portal path we're testing), but without pgx's client-side
	// prepared-statement cache. That keeps these tests focused on advisory-lock
	// behavior and avoids the orthogonal gotcha that DISCARD ALL deallocates
	// server prepared statements, which would invalidate a client-side cache.
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return conn
}

// pgxBackendPID returns the backend PID serving conn (extended protocol).
func pgxBackendPID(t *testing.T, ctx context.Context, conn *pgx.Conn) int {
	t.Helper()
	var pid int
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))
	return pid
}

// pgxAdvisoryLockHeld reports whether the backend serving conn holds the
// single-key session advisory lock for key. See advisoryLockHeld for the
// pg_locks key mapping.
func pgxAdvisoryLockHeld(t *testing.T, ctx context.Context, conn *pgx.Conn, key int) bool {
	t.Helper()
	var held bool
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND classid = 0 AND objid = $1 AND objsubid = 1)",
		key).Scan(&held))
	return held
}

// TestAdvisoryLock_ExtendedProtocol_PinsAndReleases verifies that acquiring a
// session-level advisory lock over the extended protocol (bound parameter) pins
// the session, keeps the lock visible across queries, and that an
// extended-protocol pg_advisory_unlock releases it.
func TestAdvisoryLock_ExtendedProtocol_PinsAndReleases(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)
	conn := openGatewayPgxConn(t, ctx, setup)

	_, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", 201)
	require.NoError(t, err)

	// Pinned: backend PID is stable, and the lock is visible on it.
	pid1 := pgxBackendPID(t, ctx, conn)
	pid2 := pgxBackendPID(t, ctx, conn)
	require.Equal(t, pid1, pid2, "session should be pinned after extended-protocol advisory lock")
	require.True(t, pgxAdvisoryLockHeld(t, ctx, conn, 201), "lock must be held on the pinned backend")

	// Unlock over the extended protocol; the lock must be gone afterward.
	var released bool
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", 201).Scan(&released))
	require.True(t, released)
	require.False(t, pgxAdvisoryLockHeld(t, ctx, conn, 201), "lock must be released after extended-protocol unlock")
}

// TestAdvisoryLock_ExtendedProtocol_DiscardAllReleases verifies DISCARD ALL
// releases a session advisory lock acquired over the extended protocol.
func TestAdvisoryLock_ExtendedProtocol_DiscardAllReleases(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)
	conn := openGatewayPgxConn(t, ctx, setup)

	_, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", 202)
	require.NoError(t, err)
	require.True(t, pgxAdvisoryLockHeld(t, ctx, conn, 202))

	_, err = conn.Exec(ctx, "DISCARD ALL")
	require.NoError(t, err)
	require.False(t, pgxAdvisoryLockHeld(t, ctx, conn, 202), "DISCARD ALL must release the lock")
}

// TestAdvisoryLock_ExtendedProtocol_SurvivesCommit exercises the
// existing-reservation promotion path: a transaction first establishes a
// reserved connection, then a session-level advisory lock is acquired over the
// extended protocol on top of it. The lock (and the pin) must survive COMMIT —
// session-level advisory locks outlive transactions.
func TestAdvisoryLock_ExtendedProtocol_SurvivesCommit(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)
	conn := openGatewayPgxConn(t, ctx, setup)

	// Open a transaction and run a query so the backend is reserved for the
	// transaction before the advisory lock is taken.
	_, err := conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "SELECT 1")
	require.NoError(t, err)
	pinnedPID := pgxBackendPID(t, ctx, conn)

	// Acquire the session advisory lock over the extended protocol inside the txn.
	_, err = conn.Exec(ctx, "SELECT pg_advisory_lock($1)", 203)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "COMMIT")
	require.NoError(t, err)

	// After COMMIT the transaction reason is gone, but the advisory lock keeps
	// the same backend pinned and the lock held.
	require.Equal(t, pinnedPID, pgxBackendPID(t, ctx, conn), "advisory lock must keep the backend pinned past COMMIT")
	require.True(t, pgxAdvisoryLockHeld(t, ctx, conn, 203), "session advisory lock must survive COMMIT")

	// Cleanup unpins.
	var released bool
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", 203).Scan(&released))
	require.True(t, released)
}

// TestAdvisoryLock_ExtendedProtocol_SurvivesDiscardTemp covers the case that was
// a known limitation before portals could reserve atomically (PR #1114):
// acquiring a session-level advisory lock over the extended protocol while a
// temp table already holds the backend. The advisory reason is now OR'd onto the
// existing reservation atomically, so DISCARD TEMP — which drops only the
// temp-table reason — leaves the lock held and the backend pinned. No leak.
func TestAdvisoryLock_ExtendedProtocol_SurvivesDiscardTemp(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)
	conn := openGatewayPgxConn(t, ctx, setup)

	// Temp table pins the backend (ReasonTempTable).
	_, err := conn.Exec(ctx, "CREATE TEMP TABLE adv_tmp (id int)")
	require.NoError(t, err)
	pinnedPID := pgxBackendPID(t, ctx, conn)

	// Acquire a session advisory lock over the extended protocol on top of it.
	_, err = conn.Exec(ctx, "SELECT pg_advisory_lock($1)", 204)
	require.NoError(t, err)
	require.True(t, pgxAdvisoryLockHeld(t, ctx, conn, 204))

	// DISCARD TEMP drops the temp-table reason; the advisory lock must keep the
	// backend pinned and the lock held.
	_, err = conn.Exec(ctx, "DISCARD TEMP")
	require.NoError(t, err)
	require.Equal(t, pinnedPID, pgxBackendPID(t, ctx, conn), "advisory lock must keep the backend pinned past DISCARD TEMP")
	require.True(t, pgxAdvisoryLockHeld(t, ctx, conn, 204), "session advisory lock must survive DISCARD TEMP")

	// Cleanup unpins.
	var released bool
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", 204).Scan(&released))
	require.True(t, released)
}
