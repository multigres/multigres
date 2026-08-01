// Copyright 2026 Supabase, Inc.
//
// One-off repro: does set_config(..., false) without a follow-up query leave a
// pooled backend with session GUCs that connstate does not describe?

package queryserving

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

func TestSetConfigWithoutFollowUpQuery_CrossClientLeak(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	// Connection A: set_config then disconnect with no SHOW/SELECT after.
	connA, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	_, err = connA.ExecContext(ctx, "RESET ALL")
	require.NoError(t, err)
	_, err = connA.ExecContext(ctx, "SELECT set_config('work_mem', '256MB', false)")
	require.NoError(t, err)
	require.NoError(t, connA.Close())

	// Connection B: fresh client expecting default work_mem.
	connB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer connB.Close()
	require.NoError(t, connB.PingContext(ctx))

	var workMem string
	require.NoError(t, connB.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem))
	t.Logf("connection B SHOW work_mem = %q", workMem)
	require.NotEqual(t, "256MB", workMem,
		"set_config on connection A must not leak work_mem to a fresh connection B")
}

func TestSetConfigInTxnCommitWithoutFollowUp_CrossClientLeak(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	connA, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	_, err = connA.ExecContext(ctx, "RESET ALL")
	require.NoError(t, err)
	tx, err := connA.BeginTx(ctx, nil)
	require.NoError(t, err)
	var ignored string
	require.NoError(t, tx.QueryRowContext(ctx,
		"SELECT set_config('client_min_messages', 'error', false)").Scan(&ignored))
	require.NoError(t, tx.Commit())
	require.NoError(t, connA.Close())

	connB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer connB.Close()
	require.NoError(t, connB.PingContext(ctx))

	var clientMin string
	require.NoError(t, connB.QueryRowContext(ctx, "SHOW client_min_messages").Scan(&clientMin))
	t.Logf("connection B SHOW client_min_messages = %q", clientMin)
	require.NotEqual(t, "error", clientMin,
		"set_config in txn on connection A must not leak client_min_messages to connection B")
}

func TestSetConfigWithoutFollowUpQuery_SameClientNextQuery(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := context.Background()
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	conn, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "RESET ALL")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "SELECT set_config('work_mem', '256MB', false)")
	require.NoError(t, err)

	var workMem string
	require.NoError(t, conn.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem))
	require.Equal(t, "256MB", workMem, "same client next query should observe set_config via gateway tracker")
}

// TestPreparedExecuteSetConfig_TrackedAndIsolated covers the SQL
// PREPARE/EXECUTE path of the ReasonSetConfig capture flow: the body's
// session-persisting set_config executes verbatim on a reserved backend, the
// gateway tracks the value after success and releases with the updated map.
// The same session's next query must observe the value (map replay), and a
// fresh client must not (the released backend's label is truthful).
func TestPreparedExecuteSetConfig_TrackedAndIsolated(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	connA, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	connA.SetMaxOpenConns(1)
	_, err = connA.ExecContext(ctx, "PREPARE setapp(text) AS SELECT set_config('application_name', $1, false)")
	require.NoError(t, err)
	_, err = connA.ExecContext(ctx, "EXECUTE setapp('prepared_app')")
	require.NoError(t, err)

	var appName string
	require.NoError(t, connA.QueryRowContext(ctx, "SHOW application_name").Scan(&appName))
	require.Equal(t, "prepared_app", appName,
		"the same session must observe the GUC its EXECUTE applied")
	require.NoError(t, connA.Close())

	connB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer connB.Close()
	require.NoError(t, connB.QueryRowContext(ctx, "SHOW application_name").Scan(&appName))
	require.NotEqual(t, "prepared_app", appName,
		"a fresh client must not inherit the GUC an EXECUTE applied for another session")
}

// TestFailedCommitDoesNotStampAbandonedSettings reproduces the
// outcome-conditional conclude bug: COMMIT on a failed transaction concludes
// as ROLLBACK, so the settings changed inside that transaction were reverted
// by PostgreSQL and must appear neither in the same session's view nor on the
// released backend's label (which a later client requesting the same settings
// would trust without replaying).
func TestFailedCommitDoesNotStampAbandonedSettings(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	connA, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	connA.SetMaxOpenConns(1)
	_, err = connA.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = connA.ExecContext(ctx, "SET work_mem = '64MB'")
	require.NoError(t, err)
	_, execErr := connA.ExecContext(ctx, "SELECT 1/0")
	require.Error(t, execErr, "the transaction must be failed before COMMIT")
	// COMMIT on a failed transaction: PostgreSQL concludes it as ROLLBACK.
	_, _ = connA.ExecContext(ctx, "COMMIT")

	var workMem string
	require.NoError(t, connA.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem))
	require.NotEqual(t, "64MB", workMem,
		"the same session must see the rolled-back value after COMMIT-on-failed")
	require.NoError(t, connA.Close())

	// A fresh client explicitly requesting the abandoned value: if the failed
	// commit stamped the in-transaction map onto the released backend, this
	// client's bucket hit would skip the replay and silently run without it.
	connB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer connB.Close()
	connB.SetMaxOpenConns(1)
	_, err = connB.ExecContext(ctx, "SET work_mem = '64MB'")
	require.NoError(t, err)
	require.NoError(t, connB.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem))
	require.Equal(t, "64MB", workMem,
		"a client that requested the settings must actually have them applied")
}

// TestDynamicSetConfigGatewayManaged_DoesNotPersistOnBackend pins the GMV
// containment for the dynamic pg_settings shape: statement_timeout is a
// gateway-managed variable whose value lives only in gateway state, so the
// synthesized apply must not leave a real timer on the pooled backend — a
// leaked backend statement_timeout would silently abort an unrelated client's
// queries.
func TestDynamicSetConfigGatewayManaged_DoesNotPersistOnBackend(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	connA, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	connA.SetMaxOpenConns(1)
	var applied string
	require.NoError(t, connA.QueryRowContext(ctx,
		"SELECT set_config(name, '50ms', false) FROM pg_settings WHERE name = 'statement_timeout'").Scan(&applied))
	require.NoError(t, connA.Close())

	connB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	defer connB.Close()
	connB.SetMaxOpenConns(1)
	var one int
	require.NoError(t, connB.QueryRowContext(ctx, "SELECT 1 FROM pg_sleep(0.2)").Scan(&one),
		"a fresh client must not be aborted by a statement_timeout left on the pooled backend")
}

// TestPinnedResetRestoresStartupParam pins real-PG RESET semantics for
// startup-packet GUCs on a pinned session: startup params reach pooled
// backends via replayed SET, so a raw routed RESET would revert the backend
// to the server default while the gateway map (and the release label built
// from it) keeps the startup value — the same session would observe the
// wrong value mid-transaction and a bucket-sharing client would inherit a
// backend missing a labelled GUC. The fix routes a synthesized SET of the
// startup value instead (and restores startup params after RESET ALL).
func TestPinnedResetRestoresStartupParam(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)
	dsn := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort,
		"sslmode=disable", "connect_timeout=5", "application_name=mtg_reset_e2e")

	connA, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer connA.Close()
	connA.SetMaxOpenConns(1)

	// RESET inside a transaction: the same session must observe the startup
	// value afterwards, matching a direct PostgreSQL connection.
	txn, err := connA.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = txn.ExecContext(ctx, "RESET application_name")
	require.NoError(t, err)
	var inTxn string
	require.NoError(t, txn.QueryRowContext(ctx, "SELECT current_setting('application_name')").Scan(&inTxn))
	assert.Equal(t, "mtg_reset_e2e", inTxn,
		"RESET of a startup-packet GUC must restore the startup value on the pinned backend")
	require.NoError(t, txn.Commit())

	// RESET ALL inside a transaction: reconciliation must restore startup
	// params after PostgreSQL wiped the applied session state.
	txn, err = connA.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = txn.ExecContext(ctx, "RESET ALL")
	require.NoError(t, err)
	var afterAll string
	require.NoError(t, txn.QueryRowContext(ctx, "SELECT current_setting('application_name')").Scan(&afterAll))
	assert.Equal(t, "mtg_reset_e2e", afterAll,
		"RESET ALL must leave startup params restored on the pinned backend")
	require.NoError(t, txn.Commit())

	// A second client with the same startup params shares the settings
	// bucket; a mislabelled backend from the transactions above would hand it
	// a connection missing the GUC without any replay to repair it.
	connB, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer connB.Close()
	connB.SetMaxOpenConns(1)
	var fresh string
	require.NoError(t, connB.QueryRowContext(ctx, "SELECT current_setting('application_name')").Scan(&fresh))
	assert.Equal(t, "mtg_reset_e2e", fresh,
		"a bucket-sharing client must not inherit a backend missing a labelled GUC")
}

// TestMidTxnDisconnectDoesNotStampAbandonedSettings exercises the disconnect
// release path end to end: a client that vanishes mid-transaction has its
// backend rolled back, relabelled and recycled, and a following client with
// the abandoned session's settings map must observe its requested values.
// The label-correctness contract itself is pinned at each seam by unit
// tests (TestReleaseAll_MidTransactionStampsPreBeginMap gateway-side,
// TestReleaseReservedConnection_StampsOptionsMapVerbatim pooler-side); this
// test guards the plumbing between them — a disconnect release that started
// tainting backends, stranding reservations, or leaking open transactions
// surfaces here.
func TestMidTxnDisconnectDoesNotStampAbandonedSettings(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)

	// A raw protocol client so the session can be torn down with the
	// transaction genuinely open — database/sql will not close a connection
	// held by an open Tx, so it cannot drive the disconnect-release path.
	raw := connectClientToGateway(t, ctx, setup.MultigatewayPgPort)
	_, err := raw.Query(ctx, "SET work_mem = '7MB'")
	require.NoError(t, err)
	_, err = raw.Query(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = raw.Query(ctx, "SET work_mem = '9MB'")
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	// Give the gateway's disconnect release a moment to land the backend in
	// its settings bucket before the probing client checks out.
	time.Sleep(1 * time.Second)

	// A fresh client that asks for exactly the abandoned in-transaction map
	// would bucket-hit a mislabelled backend and skip replay. The rollback
	// restored work_mem to 7MB there, so a stale stamp surfaces as 7MB. The
	// probing client must share the abandoned session's startup params (raw
	// client, none) so the two settings maps intern to the same bucket.
	probe := connectClientToGateway(t, ctx, setup.MultigatewayPgPort)
	defer probe.Close()
	_, err = probe.Query(ctx, "SET work_mem = '9MB'")
	require.NoError(t, err)
	results, err := probe.Query(ctx, "SELECT current_setting('work_mem')")
	require.NoError(t, err)
	require.NotEmpty(t, results)
	require.NotEmpty(t, results[0].Rows)
	assert.Equal(t, "9MB", string(results[0].Rows[0].Values[0]),
		"a backend released after a mid-transaction disconnect must not carry the abandoned transaction's label")
}
