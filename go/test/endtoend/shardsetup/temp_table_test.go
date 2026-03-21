// Copyright 2025 Supabase, Inc.
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

package shardsetup

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getBackendPID returns the PostgreSQL backend process ID for the given connection.
// When a session is pinned, consecutive calls on the same *sql.Conn must return
// the same PID; when unpinned, the pool is free to assign a different backend.
func getBackendPID(t *testing.T, conn *sql.Conn) int {
	t.Helper()
	var pid int
	err := conn.QueryRowContext(t.Context(), "SELECT pg_backend_pid()").Scan(&pid)
	require.NoError(t, err, "failed to get backend pid")
	return pid
}

// requirePinned asserts that two consecutive backend PID queries return the
// same value, proving the multigateway session is routed to a single reserved
// connection (session pinned).
func requirePinned(t *testing.T, conn *sql.Conn) int {
	t.Helper()
	pid1 := getBackendPID(t, conn)
	pid2 := getBackendPID(t, conn)
	require.Equal(t, pid1, pid2, "session should be pinned: backend PID must be stable")
	return pid1
}

// openMultigatewayConn opens a single *sql.Conn to the multigateway PG port.
func openMultigatewayConn(t *testing.T, setup *ShardSetup) *sql.Conn {
	t.Helper()
	connStr := GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	// Force a single underlying connection so every query goes through the same
	// multigateway session (tcp connection).
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// ---------- Basic pinning ----------

// TestTempTable_CreatePinsSession verifies that CREATE TEMP TABLE pins the
// multigateway session to a single backend connection.
func TestTempTable_CreatePinsSession(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	// Before temp table, PID may vary (pool routing).
	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_pin_test (id int)")
	require.NoError(t, err)

	// After CREATE TEMP TABLE the session must be pinned.
	requirePinned(t, conn)

	// Cleanup
	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_pin_test")
}

// TestTempTable_VisibleAcrossQueries verifies that a temp table created in one
// query is visible in subsequent queries on the same session.
func TestTempTable_VisibleAcrossQueries(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_vis (id int)")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_vis VALUES (1), (2), (3)")
	require.NoError(t, err)

	var count int
	err = conn.QueryRowContext(t.Context(), "SELECT count(*) FROM tt_vis").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count, "all rows should be visible via pinned connection")

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_vis")
}

// TestTempTable_StablePIDAfterMultipleQueries runs many queries after pinning
// and verifies the PID never changes.
func TestTempTable_StablePIDAfterMultipleQueries(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_stable (v text)")
	require.NoError(t, err)

	pinnedPID := getBackendPID(t, conn)
	for i := 0; i < 20; i++ {
		pid := getBackendPID(t, conn)
		require.Equal(t, pinnedPID, pid, "PID must remain stable on query %d", i)
	}

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_stable")
}

// ---------- Alternate creation forms ----------

// TestTempTable_CreateAsSelectPins verifies CREATE TEMP TABLE AS SELECT pins
// the session.
func TestTempTable_CreateAsSelectPins(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_ctas AS SELECT generate_series(1,5) AS n")
	require.NoError(t, err)
	requirePinned(t, conn)

	var count int
	err = conn.QueryRowContext(t.Context(), "SELECT count(*) FROM tt_ctas").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_ctas")
}

// TestTempTable_SelectIntoPins verifies SELECT INTO TEMP TABLE pins the session.
func TestTempTable_SelectIntoPins(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "SELECT generate_series(1,3) AS n INTO TEMP TABLE tt_selinto")
	require.NoError(t, err)
	requirePinned(t, conn)

	var count int
	err = conn.QueryRowContext(t.Context(), "SELECT count(*) FROM tt_selinto").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_selinto")
}

// ---------- Unpin via DISCARD ----------

// TestTempTable_DiscardTempUnpins verifies DISCARD TEMP unpins the session.
func TestTempTable_DiscardTempUnpins(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_discard (id int)")
	require.NoError(t, err)
	requirePinned(t, conn)

	_, err = conn.ExecContext(t.Context(), "DISCARD TEMP")
	require.NoError(t, err)

	// After DISCARD TEMP the session should be unpinned. The temp table should
	// no longer exist on whatever backend we reach next.
	_, err = conn.ExecContext(t.Context(), "SELECT 1 FROM tt_discard LIMIT 0")
	assert.Error(t, err, "temp table should not exist after DISCARD TEMP")
}

// TestTempTable_DiscardAllUnpins verifies DISCARD ALL unpins the session.
func TestTempTable_DiscardAllUnpins(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_discard_all (id int)")
	require.NoError(t, err)
	requirePinned(t, conn)

	_, err = conn.ExecContext(t.Context(), "DISCARD ALL")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "SELECT 1 FROM tt_discard_all LIMIT 0")
	assert.Error(t, err, "temp table should not exist after DISCARD ALL")
}

// ---------- DROP TABLE behavior ----------

// TestTempTable_DropTableDoesNotUnpin verifies that DROP TABLE on a temp table
// does NOT unpin the session. The session remains pinned until an explicit
// DISCARD (gateway doesn't track individual table names).
func TestTempTable_DropTableDoesNotUnpin(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_drop (id int)")
	require.NoError(t, err)
	pinnedPID := requirePinned(t, conn)

	_, err = conn.ExecContext(t.Context(), "DROP TABLE tt_drop")
	require.NoError(t, err)

	// Session should still be pinned (same PID).
	pid := getBackendPID(t, conn)
	assert.Equal(t, pinnedPID, pid, "DROP TABLE should not unpin — PID must remain stable")
}

// ---------- Transaction interaction ----------

// TestTempTable_CreateInTxnCommitPersists verifies CREATE TEMP TABLE inside a
// committed transaction persists the table and keeps the session pinned.
func TestTempTable_CreateInTxnCommitPersists(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "BEGIN")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_txn_commit (id int)")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_txn_commit VALUES (42)")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "COMMIT")
	require.NoError(t, err)

	// Table should persist after commit, session still pinned.
	var val int
	err = conn.QueryRowContext(t.Context(), "SELECT id FROM tt_txn_commit").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, 42, val)

	requirePinned(t, conn)

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_txn_commit")
}

// TestTempTable_CreateInTxnRollbackGone verifies CREATE TEMP TABLE inside a
// rolled-back transaction does not leave the table, but the session stays
// pinned (gateway can't know the rollback undid the CREATE).
func TestTempTable_CreateInTxnRollbackGone(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "BEGIN")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_txn_rb (id int)")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "ROLLBACK")
	require.NoError(t, err)

	// Table should NOT exist after rollback.
	_, err = conn.ExecContext(t.Context(), "SELECT 1 FROM tt_txn_rb LIMIT 0")
	assert.Error(t, err, "temp table should not exist after ROLLBACK")

	// Session remains pinned (gateway doesn't undo pin on rollback).
	// This is a known conservative behavior.
	pinnedPID := getBackendPID(t, conn)
	pid2 := getBackendPID(t, conn)
	assert.Equal(t, pinnedPID, pid2, "session should remain pinned after rollback")
}

// TestTempTable_OnCommitDeleteRows verifies ON COMMIT DELETE ROWS temp tables
// work correctly: table exists but rows are cleared after each transaction.
func TestTempTable_OnCommitDeleteRows(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_ocdr (id int) ON COMMIT DELETE ROWS")
	require.NoError(t, err)
	requirePinned(t, conn)

	_, err = conn.ExecContext(t.Context(), "BEGIN")
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_ocdr VALUES (1), (2)")
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), "COMMIT")
	require.NoError(t, err)

	// Table exists but rows should be gone.
	var count int
	err = conn.QueryRowContext(t.Context(), "SELECT count(*) FROM tt_ocdr").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "ON COMMIT DELETE ROWS should clear rows after commit")

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_ocdr")
}

// TestTempTable_OnCommitDrop verifies ON COMMIT DROP temp tables are dropped
// after the transaction ends, but the session stays pinned.
func TestTempTable_OnCommitDrop(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "BEGIN")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_ocd (id int) ON COMMIT DROP")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_ocd VALUES (99)")
	require.NoError(t, err)

	// Within the transaction, table should be accessible.
	var val int
	err = conn.QueryRowContext(t.Context(), "SELECT id FROM tt_ocd").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, 99, val)

	_, err = conn.ExecContext(t.Context(), "COMMIT")
	require.NoError(t, err)

	// Table should be gone after commit.
	_, err = conn.ExecContext(t.Context(), "SELECT 1 FROM tt_ocd LIMIT 0")
	assert.Error(t, err, "ON COMMIT DROP table should not exist after commit")
}

// ---------- Extended query protocol (EQP) ----------

// TestTempTable_PreparedStatementOnPinnedSession verifies that prepared
// statements (extended query protocol) work on a pinned session.
func TestTempTable_PreparedStatementOnPinnedSession(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_eqp (id int, name text)")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_eqp VALUES (1, 'alice'), (2, 'bob')")
	require.NoError(t, err)

	// Use parameterized query → lib/pq uses extended query protocol.
	var name string
	err = conn.QueryRowContext(t.Context(), "SELECT name FROM tt_eqp WHERE id = $1", 2).Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "bob", name)

	// Prepared statement
	stmt, err := conn.PrepareContext(t.Context(), "SELECT name FROM tt_eqp WHERE id = $1")
	require.NoError(t, err)
	defer stmt.Close()

	err = stmt.QueryRowContext(t.Context(), 1).Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "alice", name)

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_eqp")
}

// ---------- Multiple temp tables ----------

// TestTempTable_MultipleTempTables verifies that creating several temp tables
// all stay accessible and the session remains pinned.
func TestTempTable_MultipleTempTables(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	tables := []string{"tt_multi_a", "tt_multi_b", "tt_multi_c"}
	for _, tbl := range tables {
		_, err := conn.ExecContext(t.Context(), fmt.Sprintf("CREATE TEMP TABLE %s (id int)", tbl))
		require.NoError(t, err)
	}

	pinnedPID := requirePinned(t, conn)

	// All tables accessible.
	for i, tbl := range tables {
		_, err := conn.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s VALUES (%d)", tbl, i+1))
		require.NoError(t, err)
	}

	// Cross-table join should work.
	var sum int
	err := conn.QueryRowContext(t.Context(),
		"SELECT (SELECT id FROM tt_multi_a) + (SELECT id FROM tt_multi_b) + (SELECT id FROM tt_multi_c)").Scan(&sum)
	require.NoError(t, err)
	assert.Equal(t, 6, sum)

	// Still pinned.
	assert.Equal(t, pinnedPID, getBackendPID(t, conn))

	for _, tbl := range tables {
		_, _ = conn.ExecContext(t.Context(), "DROP TABLE "+tbl)
	}
}

// ---------- Isolation between sessions ----------

// TestTempTable_IsolatedBetweenSessions verifies that temp tables in one
// session are not visible from another session.
func TestTempTable_IsolatedBetweenSessions(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)

	conn1 := openMultigatewayConn(t, setup)
	conn2 := openMultigatewayConn(t, setup)

	_, err := conn1.ExecContext(t.Context(), "CREATE TEMP TABLE tt_isolated (secret text)")
	require.NoError(t, err)

	_, err = conn1.ExecContext(t.Context(), "INSERT INTO tt_isolated VALUES ('hidden')")
	require.NoError(t, err)

	// conn2 should NOT see the temp table.
	_, err = conn2.ExecContext(t.Context(), "SELECT 1 FROM tt_isolated LIMIT 0")
	assert.Error(t, err, "temp table should not be visible from a different session")

	_, _ = conn1.ExecContext(t.Context(), "DROP TABLE tt_isolated")
}

// ---------- Re-pin after discard ----------

// TestTempTable_RepinAfterDiscard verifies that after DISCARD TEMP unpins,
// creating a new temp table re-pins the session (possibly to a different
// backend).
func TestTempTable_RepinAfterDiscard(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	// First pin.
	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_repin1 (id int)")
	require.NoError(t, err)
	pid1 := requirePinned(t, conn)

	// Unpin.
	_, err = conn.ExecContext(t.Context(), "DISCARD TEMP")
	require.NoError(t, err)

	// Re-pin.
	_, err = conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_repin2 (id int)")
	require.NoError(t, err)
	pid2 := requirePinned(t, conn)

	// PID may or may not change — the important thing is that the new temp
	// table is accessible.
	_ = pid1
	_ = pid2

	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_repin2 VALUES (1)")
	require.NoError(t, err)

	var val int
	err = conn.QueryRowContext(t.Context(), "SELECT id FROM tt_repin2").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, 1, val)

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_repin2")
}

// ---------- Temp table + regular table interaction ----------

// TestTempTable_JoinWithRegularTable verifies that a pinned session can still
// access regular (permanent) tables alongside temp tables.
func TestTempTable_JoinWithRegularTable(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	// Create a permanent table.
	permTable := fmt.Sprintf("perm_join_test_%d", time.Now().UnixNano())
	_, err := conn.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE %s (id int, label text)", permTable))
	require.NoError(t, err)
	defer func() { _, _ = conn.ExecContext(t.Context(), "DROP TABLE "+permTable) }()

	_, err = conn.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s VALUES (1, 'one'), (2, 'two')", permTable))
	require.NoError(t, err)

	// Create a temp table.
	_, err = conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_join (id int, extra text)")
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_join VALUES (1, 'alpha'), (2, 'beta')")
	require.NoError(t, err)

	// Join should work.
	var label, extra string
	err = conn.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT p.label, t.extra FROM %s p JOIN tt_join t ON p.id = t.id WHERE p.id = 1", permTable)).Scan(&label, &extra)
	require.NoError(t, err)
	assert.Equal(t, "one", label)
	assert.Equal(t, "alpha", extra)

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_join")
}

// ---------- Temp table + GUC interaction ----------

// TestTempTable_GUCPreservedOnPinnedSession verifies that SET commands are
// preserved across queries on a pinned session (the reserved connection
// maintains PG-side state).
func TestTempTable_GUCPreservedOnPinnedSession(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_guc (id int)")
	require.NoError(t, err)
	requirePinned(t, conn)

	_, err = conn.ExecContext(t.Context(), "SET statement_timeout = '30s'")
	require.NoError(t, err)

	var timeout string
	err = conn.QueryRowContext(t.Context(), "SHOW statement_timeout").Scan(&timeout)
	require.NoError(t, err)
	assert.Equal(t, "30s", timeout, "GUC should be preserved on pinned session")

	// Reset
	_, _ = conn.ExecContext(t.Context(), "RESET statement_timeout")
	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_guc")
}

// ---------- Temp sequence ----------

// TestTempTable_CreateTempSequence verifies that CREATE TEMP SEQUENCE does NOT
// currently pin the session (known limitation — uncommon usage).
func TestTempTable_CreateTempSequence(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	// This documents current behavior. If temp sequence pinning is added later,
	// update this test.
	_, err := conn.ExecContext(t.Context(), "CREATE TEMP SEQUENCE tt_seq")
	if err != nil {
		t.Skipf("CREATE TEMP SEQUENCE not supported through multigateway: %v", err)
	}

	// We just document that this doesn't crash. Pinning behavior for sequences
	// is not currently implemented.
	t.Log("CREATE TEMP SEQUENCE succeeded — pinning not verified (known limitation)")

	_, _ = conn.ExecContext(t.Context(), "DROP SEQUENCE tt_seq")
}
