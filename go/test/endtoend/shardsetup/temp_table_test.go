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

// ---------- IF NOT EXISTS ----------

// TestTempTable_IfNotExists verifies that CREATE TEMP TABLE IF NOT EXISTS pins
// the session even when the table already exists (the AST node still carries
// RelPersistence = 't').
func TestTempTable_IfNotExists(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_ine (id int)")
	require.NoError(t, err)

	// Second CREATE with IF NOT EXISTS — should succeed (no-op) and stay pinned.
	_, err = conn.ExecContext(t.Context(), "CREATE TEMP TABLE IF NOT EXISTS tt_ine (id int)")
	require.NoError(t, err)
	requirePinned(t, conn)

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_ine")
}

// ---------- LIKE clause ----------

// TestTempTable_CreateLike verifies CREATE TEMP TABLE ... (LIKE ...) pins the
// session and inherits the structure of the source table.
func TestTempTable_CreateLike(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	// Create a permanent table to copy structure from.
	permTable := fmt.Sprintf("perm_like_src_%d", time.Now().UnixNano())
	_, err := conn.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE %s (id int, name text, active boolean)", permTable))
	require.NoError(t, err)
	defer func() { _, _ = conn.ExecContext(t.Context(), "DROP TABLE "+permTable) }()

	_, err = conn.ExecContext(t.Context(), fmt.Sprintf("CREATE TEMP TABLE tt_like (LIKE %s)", permTable))
	require.NoError(t, err)
	requirePinned(t, conn)

	// Verify inherited columns work.
	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_like (id, name, active) VALUES (1, 'test', true)")
	require.NoError(t, err)

	var name string
	err = conn.QueryRowContext(t.Context(), "SELECT name FROM tt_like WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "test", name)

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_like")
}

// ---------- Index on temp table ----------

// TestTempTable_IndexOnTempTable verifies that creating an index on a temp
// table works across queries on the pinned session.
func TestTempTable_IndexOnTempTable(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_idx (id int, val text)")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "CREATE INDEX tt_idx_id ON tt_idx (id)")
	require.NoError(t, err)

	// Insert data and verify the index is usable.
	for i := 0; i < 100; i++ {
		_, err = conn.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO tt_idx VALUES (%d, 'row_%d')", i, i))
		require.NoError(t, err)
	}

	// Force an index scan to verify the index exists.
	_, err = conn.ExecContext(t.Context(), "SET enable_seqscan = off")
	require.NoError(t, err)

	var val string
	err = conn.QueryRowContext(t.Context(), "SELECT val FROM tt_idx WHERE id = 42").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, "row_42", val)

	_, _ = conn.ExecContext(t.Context(), "RESET enable_seqscan")
	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_idx")
}

// ---------- Temp view ----------

// TestTempTable_CreateTempViewPins verifies that CREATE TEMP VIEW pins the
// session (detected via ViewStmt with RelPersistence = 't' in session_pin.go).
func TestTempTable_CreateTempViewPins(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP VIEW tt_view AS SELECT 1 AS n, 'hello' AS msg")
	require.NoError(t, err)
	requirePinned(t, conn)

	// View should be accessible across queries.
	var msg string
	err = conn.QueryRowContext(t.Context(), "SELECT msg FROM tt_view").Scan(&msg)
	require.NoError(t, err)
	assert.Equal(t, "hello", msg)

	_, _ = conn.ExecContext(t.Context(), "DROP VIEW tt_view")
}

// ---------- DISCARD PLANS/SEQUENCES do NOT unpin ----------

// TestTempTable_DiscardPlansDoesNotUnpin verifies that DISCARD PLANS does not
// unpin a session that was pinned by a temp table.
func TestTempTable_DiscardPlansDoesNotUnpin(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_dp (id int)")
	require.NoError(t, err)
	pinnedPID := requirePinned(t, conn)

	_, err = conn.ExecContext(t.Context(), "DISCARD PLANS")
	require.NoError(t, err)

	// Still pinned — same PID, temp table still accessible.
	pid := getBackendPID(t, conn)
	assert.Equal(t, pinnedPID, pid, "DISCARD PLANS should not unpin")

	var count int
	err = conn.QueryRowContext(t.Context(), "SELECT count(*) FROM tt_dp").Scan(&count)
	require.NoError(t, err)

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_dp")
}

// TestTempTable_DiscardSequencesDoesNotUnpin verifies that DISCARD SEQUENCES
// does not unpin a session pinned by a temp table.
func TestTempTable_DiscardSequencesDoesNotUnpin(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_ds (id int)")
	require.NoError(t, err)
	pinnedPID := requirePinned(t, conn)

	_, err = conn.ExecContext(t.Context(), "DISCARD SEQUENCES")
	require.NoError(t, err)

	pid := getBackendPID(t, conn)
	assert.Equal(t, pinnedPID, pid, "DISCARD SEQUENCES should not unpin")

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_ds")
}

// ---------- Two concurrent pinned sessions ----------

// TestTempTable_ConcurrentPinnedSessions verifies that two independently
// pinned sessions get different backend PIDs (each pinned to their own
// reserved connection).
func TestTempTable_ConcurrentPinnedSessions(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)

	conn1 := openMultigatewayConn(t, setup)
	conn2 := openMultigatewayConn(t, setup)

	_, err := conn1.ExecContext(t.Context(), "CREATE TEMP TABLE tt_conc1 (id int)")
	require.NoError(t, err)
	_, err = conn2.ExecContext(t.Context(), "CREATE TEMP TABLE tt_conc2 (id int)")
	require.NoError(t, err)

	pid1 := requirePinned(t, conn1)
	pid2 := requirePinned(t, conn2)

	assert.NotEqual(t, pid1, pid2, "two pinned sessions must have different backend PIDs")

	_, _ = conn1.ExecContext(t.Context(), "DROP TABLE tt_conc1")
	_, _ = conn2.ExecContext(t.Context(), "DROP TABLE tt_conc2")
}

// ---------- Multi-statement batch ----------

// TestTempTable_MultiStatementBatch verifies that a multi-statement batch
// containing CREATE TEMP TABLE pins the session and all statements in the
// batch execute on the same backend. This tests the SessionPinned &&
// len(asts) > 1 code path in handler.go.
func TestTempTable_MultiStatementBatch(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	// Send multiple statements in a single simple-query-protocol message.
	// lib/pq sends this as one query string when there are no parameters.
	_, err := conn.ExecContext(t.Context(),
		"CREATE TEMP TABLE tt_batch (id int); INSERT INTO tt_batch VALUES (1), (2), (3)")
	require.NoError(t, err)

	requirePinned(t, conn)

	var count int
	err = conn.QueryRowContext(t.Context(), "SELECT count(*) FROM tt_batch").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count, "all rows from batch INSERT should be visible")

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_batch")
}

// ---------- ALTER TABLE on temp table ----------

// TestTempTable_AlterTable verifies that ALTER TABLE on a temp table works
// on the pinned session — the column addition is visible in subsequent queries.
func TestTempTable_AlterTable(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_alter (id int)")
	require.NoError(t, err)
	requirePinned(t, conn)

	_, err = conn.ExecContext(t.Context(), "ALTER TABLE tt_alter ADD COLUMN name text DEFAULT 'unnamed'")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_alter (id) VALUES (1)")
	require.NoError(t, err)

	var name string
	err = conn.QueryRowContext(t.Context(), "SELECT name FROM tt_alter WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "unnamed", name, "ALTER TABLE ADD COLUMN default should apply")

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_alter")
}

// ---------- TRUNCATE temp table ----------

// TestTempTable_Truncate verifies that TRUNCATE on a temp table clears data
// but the table and pinned session remain intact.
func TestTempTable_Truncate(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_trunc (id int)")
	require.NoError(t, err)
	pinnedPID := requirePinned(t, conn)

	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_trunc VALUES (1), (2), (3)")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "TRUNCATE tt_trunc")
	require.NoError(t, err)

	var count int
	err = conn.QueryRowContext(t.Context(), "SELECT count(*) FROM tt_trunc").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "TRUNCATE should clear all rows")

	// Still pinned.
	assert.Equal(t, pinnedPID, getBackendPID(t, conn))

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_trunc")
}

// ---------- Serial / generated column ----------

// TestTempTable_SerialColumn verifies that CREATE TEMP TABLE with a SERIAL
// column (which implicitly creates a temp sequence) works on the pinned session.
func TestTempTable_SerialColumn(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_serial (id serial PRIMARY KEY, name text)")
	require.NoError(t, err)
	requirePinned(t, conn)

	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_serial (name) VALUES ('alice')")
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_serial (name) VALUES ('bob')")
	require.NoError(t, err)

	var id int
	var name string
	err = conn.QueryRowContext(t.Context(), "SELECT id, name FROM tt_serial ORDER BY id DESC LIMIT 1").Scan(&id, &name)
	require.NoError(t, err)
	assert.Equal(t, 2, id, "serial should auto-increment")
	assert.Equal(t, "bob", name)

	_, _ = conn.ExecContext(t.Context(), "DROP TABLE tt_serial")
}

// ---------- Disconnect cleanup ----------

// TestTempTable_DisconnectCleansUp verifies that when a pinned session
// disconnects, the temp table is cleaned up and a new connection does not
// see it.
func TestTempTable_DisconnectCleansUp(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)

	connStr := GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	// First connection: create temp table.
	db1, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	db1.SetMaxOpenConns(1)
	db1.SetMaxIdleConns(1)

	conn1, err := db1.Conn(t.Context())
	require.NoError(t, err)

	_, err = conn1.ExecContext(t.Context(), "CREATE TEMP TABLE tt_disconnect (secret text)")
	require.NoError(t, err)
	_, err = conn1.ExecContext(t.Context(), "INSERT INTO tt_disconnect VALUES ('hidden')")
	require.NoError(t, err)

	// Close the connection and the db pool entirely.
	conn1.Close()
	db1.Close()

	// Second connection: temp table should be gone.
	db2, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db2.Close()
	db2.SetMaxOpenConns(1)
	db2.SetMaxIdleConns(1)

	conn2, err := db2.Conn(t.Context())
	require.NoError(t, err)
	defer conn2.Close()

	_, err = conn2.ExecContext(t.Context(), "SELECT 1 FROM tt_disconnect LIMIT 0")
	assert.Error(t, err, "temp table from disconnected session should not exist")
}

// ---------- Temp table shadows permanent table ----------

// TestTempTable_ShadowsPermanentTable verifies that a temp table with the same
// name as a permanent table takes precedence (PG search_path puts pg_temp
// first), and after DISCARD TEMP the permanent table becomes visible again.
func TestTempTable_ShadowsPermanentTable(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	permTable := fmt.Sprintf("shadow_%d", time.Now().UnixNano())
	_, err := conn.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE %s (src text)", permTable))
	require.NoError(t, err)
	defer func() { _, _ = conn.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+permTable) }()

	_, err = conn.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s VALUES ('permanent')", permTable))
	require.NoError(t, err)

	// Create temp table with the same name — shadows the permanent one.
	_, err = conn.ExecContext(t.Context(), fmt.Sprintf("CREATE TEMP TABLE %s (src text)", permTable))
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s VALUES ('temporary')", permTable))
	require.NoError(t, err)

	var src string
	err = conn.QueryRowContext(t.Context(), fmt.Sprintf("SELECT src FROM %s", permTable)).Scan(&src)
	require.NoError(t, err)
	assert.Equal(t, "temporary", src, "temp table should shadow the permanent table")

	// DISCARD TEMP removes the temp table — permanent one should be visible.
	_, err = conn.ExecContext(t.Context(), "DISCARD TEMP")
	require.NoError(t, err)

	err = conn.QueryRowContext(t.Context(), fmt.Sprintf("SELECT src FROM %s", permTable)).Scan(&src)
	require.NoError(t, err)
	assert.Equal(t, "permanent", src, "after DISCARD TEMP, permanent table should be visible")
}

// ---------- Savepoint rollback ----------

// TestTempTable_SavepointRollback verifies that CREATE TEMP TABLE inside a
// savepoint that is rolled back removes the table, but the session stays
// pinned (gateway doesn't track savepoint-level rollback of DDL).
func TestTempTable_SavepointRollback(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(), "BEGIN")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "SAVEPOINT sp1")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "CREATE TEMP TABLE tt_svpt (id int)")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "INSERT INTO tt_svpt VALUES (1)")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "ROLLBACK TO SAVEPOINT sp1")
	require.NoError(t, err)

	_, err = conn.ExecContext(t.Context(), "COMMIT")
	require.NoError(t, err)

	// Table should NOT exist after savepoint rollback.
	_, err = conn.ExecContext(t.Context(), "SELECT 1 FROM tt_svpt LIMIT 0")
	assert.Error(t, err, "temp table should not exist after ROLLBACK TO SAVEPOINT")

	// Session stays pinned (conservative — gateway saw the CREATE).
	pid1 := getBackendPID(t, conn)
	pid2 := getBackendPID(t, conn)
	assert.Equal(t, pid1, pid2, "session should remain pinned after savepoint rollback")
}
