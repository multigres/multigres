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

package pgregresstest

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// openGUCNoticeConn opens a pgx connection that starts at
// client_min_messages=NOTICE (mirroring PostGIS run_test.pl's PGOPTIONS) and
// records every server NOTICE it receives.
func openGUCNoticeConn(ctx context.Context, t *testing.T, port int) (*pgx.Conn, func() []string, func()) {
	t.Helper()
	connStr := shardsetup.GetTestUserDSN("localhost", port, "sslmode=disable", "connect_timeout=5")
	cfg, err := pgx.ParseConfig(connStr)
	require.NoError(t, err)
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["options"] = "-c client_min_messages=NOTICE"
	var mu sync.Mutex
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		mu.Lock()
		notices = append(notices, n.Message)
		mu.Unlock()
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	get := func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), notices...) }
	return conn, get, func() { _ = conn.Close(ctx) }
}

// createLeakNoticeFn installs public.leak_notice(int), which RAISEs a NOTICE that
// client_min_messages=error must suppress.
func createLeakNoticeFn(t *testing.T, directPgPort int) {
	t.Helper()
	require.NoError(t, execOnPrimary(directPgPort, shardsetup.TestPostgresPassword,
		`CREATE OR REPLACE FUNCTION public.leak_notice(x int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'leak-%', x; RETURN x; END $$`))
	t.Cleanup(func() {
		_ = execOnPrimary(directPgPort, shardsetup.TestPostgresPassword, `DROP FUNCTION IF EXISTS public.leak_notice(int)`)
	})
}

// runGUCNoticeLeakRounds runs `rounds` poison+victim pairs against the given port.
// Each round runs the poison SQL on one connection (a transaction whose
// session-GUC change PostgreSQL reverts on rollback) and then a victim that sets
// client_min_messages=error and expects leak_notice()'s NOTICE to be suppressed.
// It returns how many victim rounds leaked the NOTICE and how many observed a
// reserved backend whose client_min_messages != error (SHOW inside the txn).
func runGUCNoticeLeakRounds(ctx context.Context, t *testing.T, port int, poison []string) (leaks, mismatches int) {
	t.Helper()
	const rounds = 40
	m := pgx.QueryExecModeSimpleProtocol
	for range rounds {
		pc, _, pclose := openGUCNoticeConn(ctx, t, port)
		for _, sql := range poison {
			_, _ = pc.Exec(ctx, sql, m)
		}
		pclose()

		vc, getNotices, vclose := openGUCNoticeConn(ctx, t, port)
		_, _ = vc.Exec(ctx, "SET search_path TO public", m)
		_, _ = vc.Exec(ctx, "SET client_min_messages TO error", m)
		_, _ = vc.Exec(ctx, "BEGIN", m)
		var inTxn string
		_ = vc.QueryRow(ctx, "SHOW client_min_messages", m).Scan(&inTxn)
		_, _ = vc.Exec(ctx, "SELECT public.leak_notice(1)", m)
		_, _ = vc.Exec(ctx, "COMMIT", m)
		got := getNotices()
		vclose()

		if inTxn != "error" {
			mismatches++
		}
		if slices.Contains(got, "leak-1") {
			leaks++
			t.Logf("NOTICE leaked; reserved backend client_min_messages=%q (wanted error)", inTxn)
		}
	}
	return leaks, mismatches
}

// TestCurrentSchemaPinsImplicitTempNamespace verifies that current_schema()
// keeps the backend whose implicit pg_temp namespace it may instantiate.
func TestCurrentSchemaPinsImplicitTempNamespace(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 5*time.Minute)
	directPgPort := setup.GetPrimary(t).Pgctld.PgPort
	gatewayPgPort := setup.MultigatewayPgPort

	run := func(t *testing.T, port int, gid string) {
		t.Helper()
		conn, err := pgx.Connect(ctx, shardsetup.GetTestUserDSN("localhost", port, "sslmode=disable", "connect_timeout=5"))
		require.NoError(t, err)
		defer func() { _ = conn.Close(ctx) }()
		t.Cleanup(func() {
			_ = execOnPrimary(directPgPort, shardsetup.TestPostgresPassword, "ROLLBACK PREPARED '"+gid+"'")
		})

		_, err = conn.Exec(ctx, "SET search_path TO 'pg_temp'")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "BEGIN")
		require.NoError(t, err)
		var schema string
		require.NoError(t, conn.QueryRow(ctx, "SELECT current_schema()").Scan(&schema))
		require.Contains(t, schema, "pg_temp")
		_, err = conn.Exec(ctx, "PREPARE TRANSACTION '"+gid+"'")
		require.ErrorContains(t, err, "cannot PREPARE a transaction that has operated on temporary objects")
	}

	t.Run("direct_primary", func(t *testing.T) { run(t, directPgPort, "temp_namespace_direct") })
	t.Run("multigateway", func(t *testing.T) { run(t, gatewayPgPort, "temp_namespace_gateway") })
}

// TestCopyFreezeAfterTruncateOnTempReservation verifies that affinity does not
// add transaction activity between TRUNCATE and COPY FREEZE.
func TestCopyFreezeAfterTruncateOnTempReservation(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 5*time.Minute)
	directPgPort := setup.GetPrimary(t).Pgctld.PgPort
	gatewayPgPort := setup.MultigatewayPgPort

	run := func(t *testing.T, port int) {
		conn, err := pgx.Connect(ctx, shardsetup.GetTestUserDSN("localhost", port, "sslmode=disable", "connect_timeout=5"))
		require.NoError(t, err)
		defer func() { _ = conn.Close(ctx) }()

		_, err = conn.Exec(ctx, "CREATE TEMP TABLE copy_freeze_test(a text)")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "BEGIN")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "TRUNCATE copy_freeze_test")
		require.NoError(t, err)
		_, err = conn.PgConn().CopyFrom(ctx, strings.NewReader("a\n"), "COPY copy_freeze_test FROM STDIN FREEZE")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "ROLLBACK")
		require.NoError(t, err)
	}

	t.Run("direct_primary", func(t *testing.T) { run(t, directPgPort) })
	t.Run("multigateway", func(t *testing.T) { run(t, gatewayPgPort) })
}

// TestOpaqueReservationPreservesRepeatedRollback verifies that a nontransaction
// reservation survives transaction boundaries without weakening ROLLBACK.
func TestOpaqueReservationPreservesRepeatedRollback(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 5*time.Minute)
	gatewayPgPort := setup.MultigatewayPgPort
	conn, err := pgx.Connect(ctx, shardsetup.GetTestUserDSN("localhost", gatewayPgPort, "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, "DO $$ BEGIN SET work_mem = '8MB'; END $$")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE TABLE opaque_rollback_test (value int)")
	require.NoError(t, err)
	defer func() { _, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS opaque_rollback_test") }()
	_, err = conn.Exec(ctx, "INSERT INTO opaque_rollback_test VALUES (100)")
	require.NoError(t, err)
	for range 5 {
		_, err = conn.Exec(ctx, "BEGIN")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "UPDATE opaque_rollback_test SET value = value + 1")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "ROLLBACK")
		require.NoError(t, err)
	}
	var value int
	require.NoError(t, conn.QueryRow(ctx, "SELECT value FROM opaque_rollback_test").Scan(&value))
	require.Equal(t, 100, value)
}

// TestDeferredSETOnReservedConn verifies tracked SET inside a transaction is
// applied to the reserved backend on the next query (deferred apply), not at
// release.
func TestDeferredSETOnReservedConn(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 5*time.Minute)
	directPgPort := setup.GetPrimary(t).Pgctld.PgPort
	gatewayPgPort := setup.MultigatewayPgPort
	createLeakNoticeFn(t, directPgPort)
	m := pgx.QueryExecModeSimpleProtocol

	run := func(t *testing.T, port int) {
		t.Helper()
		conn, getNotices, close := openGUCNoticeConn(ctx, t, port)
		defer close()

		_, err := conn.Exec(ctx, "BEGIN", m)
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET client_min_messages TO error", m)
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SELECT public.leak_notice(1)", m)
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "COMMIT", m)
		require.NoError(t, err)

		require.NotContains(t, getNotices(), "leak-1",
			"deferred SET must apply before the next query on the reserved connection")
	}

	t.Run("direct_primary", func(t *testing.T) { run(t, directPgPort) })
	t.Run("multigateway", func(t *testing.T) { run(t, gatewayPgPort) })
}

// TestGUCDivergenceRepro covers the plain BEGIN -> SET -> ROLLBACK case: a session
// GUC set inside a transaction is reverted on the backend by ROLLBACK, and the
// pool must revert its cached connstate in lock-step so the recycled connection
// is not reused with stale settings.
func TestGUCDivergenceRepro(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 5*time.Minute)
	directPgPort := setup.GetPrimary(t).Pgctld.PgPort
	gatewayPgPort := setup.MultigatewayPgPort
	createLeakNoticeFn(t, directPgPort)

	poison := []string{
		"SET search_path TO public",
		"BEGIN",
		"SET client_min_messages TO error",
		"SELECT 1",
		"ROLLBACK",
	}

	t.Run("direct_primary", func(t *testing.T) {
		leaks, mism := runGUCNoticeLeakRounds(ctx, t, directPgPort, poison)
		t.Logf("DIRECT:  leaks=%d/40 backend-mismatches=%d/40", leaks, mism)
		require.Zero(t, leaks, "sanity: direct PostgreSQL must never leak")
	})
	t.Run("multigateway", func(t *testing.T) {
		leaks, mism := runGUCNoticeLeakRounds(ctx, t, gatewayPgPort, poison)
		t.Logf("GATEWAY: leaks=%d/40 backend-mismatches=%d/40", leaks, mism)
		require.Zero(t, leaks, "transaction rollback must not leave stale pooled GUC state")
	})
}

// TestGUCSavepointDivergenceRepro covers the ROLLBACK TO SAVEPOINT -> COMMIT hole:
// a session GUC set inside a sub-transaction is reverted on the backend by
// ROLLBACK TO SAVEPOINT, but the transaction-level snapshot only restores on a
// full ROLLBACK, so without the COMMIT-time reconcile the committed connection is
// recycled with a connstate that disagrees with the backend.
func TestGUCSavepointDivergenceRepro(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 5*time.Minute)
	directPgPort := setup.GetPrimary(t).Pgctld.PgPort
	gatewayPgPort := setup.MultigatewayPgPort
	createLeakNoticeFn(t, directPgPort)

	poison := []string{
		"SET search_path TO public",
		"BEGIN",
		"SAVEPOINT sp",
		"SET client_min_messages TO error",
		"ROLLBACK TO SAVEPOINT sp",
		"COMMIT",
	}

	t.Run("direct_primary", func(t *testing.T) {
		leaks, mism := runGUCNoticeLeakRounds(ctx, t, directPgPort, poison)
		t.Logf("DIRECT:  leaks=%d/40 backend-mismatches=%d/40", leaks, mism)
		require.Zero(t, leaks, "sanity: direct PostgreSQL must never leak")
	})
	t.Run("multigateway", func(t *testing.T) {
		leaks, mism := runGUCNoticeLeakRounds(ctx, t, gatewayPgPort, poison)
		t.Logf("GATEWAY: leaks=%d/40 backend-mismatches=%d/40", leaks, mism)
		require.Zero(t, leaks, "savepoint rollback-to + commit must not leave stale pooled GUC state")
	})
}
