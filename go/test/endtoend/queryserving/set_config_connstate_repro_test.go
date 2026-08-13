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

	"github.com/multigres/multigres/go/common/sqltypes"
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
// PREPARE/EXECUTE path: on an unpinned session the body's session-persisting
// set_config is rewritten to is_local := true so the pooled backend reverts it,
// while the gateway tracks the value. The same session's next query must
// observe the value (map replay), and a fresh client must not (the backend was
// left clean).
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
	var afterResetAll string
	require.NoError(t, txn.QueryRowContext(ctx, "SELECT current_setting('application_name')").Scan(&afterResetAll))
	assert.Equal(t, "mtg_reset_e2e", afterResetAll,
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

// TestPrepareGatewayManagedSetConfigRejected pins the PREPARE-time gate end
// to end: a prepared body executes verbatim on the backend, so a
// gateway-managed set_config inside one must be rejected up front — executed,
// it would leave a real statement_timeout on a pooled backend that the
// release label can never describe, aborting an unrelated client's queries.
func TestPrepareGatewayManagedSetConfigRejected(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)
	conn := connectClientToGateway(t, ctx, setup.MultigatewayPgPort)
	defer conn.Close()

	_, err := conn.Query(ctx, "PREPARE leak AS SELECT set_config('statement_timeout', '50ms', false)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "gateway-managed")

	// The session must remain healthy and nothing registered under the name.
	_, err = conn.Query(ctx, "EXECUTE leak")
	require.Error(t, err, "the rejected statement must not exist")

	// No backend picked up a timer: a query longer than the attempted timeout
	// still succeeds for a fresh client.
	probe := connectClientToGateway(t, ctx, setup.MultigatewayPgPort)
	defer probe.Close()
	results, err := probe.Query(ctx, "SELECT 1 FROM pg_sleep(0.2)")
	require.NoError(t, err, "no backend may carry a leaked statement_timeout")
	require.NotEmpty(t, results)
}

// TestSuspendedSetConfigPortalAbandoned exercises the full lifecycle of a
// row-limited portal whose statement carries a persisting set_config: the
// portal suspends (reserving the backend for portal + capture), the client
// abandons it with Close, keeps using the session, and disconnects. The
// gateway must have tracked the value at suspension (same session reads it
// back), the abandoned Close must drain the reservation rather than strand a
// pinned backend, and a bucket-sharing client afterwards must observe a
// truthful label. The reservation-drain mechanics are pinned by pooler unit
// tests; this guards the end-to-end plumbing.
func TestSuspendedSetConfigPortalAbandoned(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)
	conn := connectClientToGateway(t, ctx, setup.MultigatewayPgPort)
	defer conn.Close()

	require.NoError(t, conn.Parse(ctx, "s1",
		"SELECT set_config('work_mem', '64MB', false), g FROM generate_series(1, 100) g", nil))
	completed, err := conn.BindAndExecute(ctx, "p1", "s1", nil, nil, nil, 1,
		func(context.Context, *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	require.False(t, completed, "maxRows=1 over 100 rows must suspend the portal")

	// Abandon the suspended portal.
	require.NoError(t, conn.ClosePortal(ctx, "p1"))

	// The session lives on; the value was tracked at suspension.
	results, err := conn.Query(ctx, "SELECT current_setting('work_mem')")
	require.NoError(t, err)
	require.NotEmpty(t, results)
	require.NotEmpty(t, results[0].Rows)
	assert.Equal(t, "64MB", string(results[0].Rows[0].Values[0]),
		"the suspended portal's set_config must be tracked by the gateway")
	require.NoError(t, conn.Close())

	// A bucket-sharing client must see a truthful label on whatever backend
	// the abandoned flow released.
	probe := connectClientToGateway(t, ctx, setup.MultigatewayPgPort)
	defer probe.Close()
	_, err = probe.Query(ctx, "SET work_mem = '64MB'")
	require.NoError(t, err)
	results, err = probe.Query(ctx, "SELECT current_setting('work_mem')")
	require.NoError(t, err)
	require.NotEmpty(t, results)
	require.NotEmpty(t, results[0].Rows)
	assert.Equal(t, "64MB", string(results[0].Rows[0].Values[0]))
}

// TestMidStreamErrorThenBackendReuse guards the ReleaseStatementError
// assumption: a clean PostgreSQL error is only safe to recycle on if the
// socket is drained to ReadyForQuery even when the error surfaces mid-result,
// after DataRows have already streamed. Both flavors are exercised — a plain
// pooled statement, and one carrying an (unpinned, is_local-reverted)
// set_config — followed by immediate reuse of the session and of the pool by a
// second client.
func TestMidStreamErrorThenBackendReuse(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)
	conn := connectClientToGateway(t, ctx, setup.MultigatewayPgPort)
	defer conn.Close()

	// Plain pooled statement erroring after ~499 streamed rows.
	_, err := conn.Query(ctx, "SELECT 1/(500-x) FROM generate_series(1, 10000) x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "division by zero")

	// The session must be immediately reusable, repeatedly.
	for range 3 {
		results, qerr := conn.Query(ctx, "SELECT 42")
		require.NoError(t, qerr, "session must stay synchronized after a mid-stream error")
		require.NotEmpty(t, results)
		require.Equal(t, "42", string(results[0].Rows[0].Values[0]))
	}

	// Same shape with a leading set_config: it runs (reverting, is_local := true)
	// for streamed rows, then the statement aborts atomically — the backend must
	// be recycled clean, not closed, and must not carry the value.
	_, err = conn.Query(ctx, "SELECT set_config('work_mem', '99MB', false), 1/(500-x) FROM generate_series(1, 10000) x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "division by zero")

	results, err := conn.Query(ctx, "SELECT current_setting('work_mem')")
	require.NoError(t, err)
	require.NotEmpty(t, results)
	assert.NotEqual(t, "99MB", string(results[0].Rows[0].Values[0]),
		"the aborted statement's set_config must not be tracked or persist")
	require.NoError(t, conn.Close())

	// A fresh client sees a healthy, uncorrupted pool.
	probe := connectClientToGateway(t, ctx, setup.MultigatewayPgPort)
	defer probe.Close()
	results, err = probe.Query(ctx, "SELECT current_setting('work_mem')")
	require.NoError(t, err)
	require.NotEmpty(t, results)
	assert.NotEqual(t, "99MB", string(results[0].Rows[0].Values[0]),
		"no pooled backend may carry the aborted statement's value")
}

// TestPinnedDateStyleTracksCanonicalAcrossRotation pins the composite-GUC
// capture end to end: an in-transaction (pinned) SET datestyle = 'dmy' is a
// RELATIVE literal — the backend combines it with its current style — and
// the gateway must track PostgreSQL's canonical report ('SQL, DMY' here),
// not the bare literal, or a pool-rotation replay of 'dmy' against a fresh
// backend's ISO default silently drops the style the client had set.
func TestPinnedDateStyleTracksCanonicalAcrossRotation(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Minute)
	conn := connectClientToGateway(t, ctx, setup.MultigatewayPgPort)
	defer conn.Close()

	readDateStyle := func() string {
		results, err := conn.Query(ctx, "SELECT current_setting('datestyle')")
		require.NoError(t, err)
		require.NotEmpty(t, results)
		require.NotEmpty(t, results[0].Rows)
		return string(results[0].Rows[0].Values[0])
	}

	_, err := conn.Query(ctx, "SET datestyle = 'SQL, MDY'")
	require.NoError(t, err)

	_, err = conn.Query(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = conn.Query(ctx, "SET datestyle = 'dmy'")
	require.NoError(t, err)
	require.Equal(t, "SQL, DMY", readDateStyle(),
		"inside the transaction the pinned backend combines the relative literal with its current style")
	_, err = conn.Query(ctx, "COMMIT")
	require.NoError(t, err)

	// Change the desired map so the next checkout cannot pointer-hit the
	// just-released backend's bucket and must replay the tracked map onto a
	// different (or reset) connection — the rotation that exposes a
	// literal-tracked 'dmy' as 'ISO, DMY'.
	_, err = conn.Query(ctx, "SET work_mem = '13MB'")
	require.NoError(t, err)
	require.Equal(t, "SQL, DMY", readDateStyle(),
		"after rotation the replayed map must reproduce the canonical composite value")
}

// TestExtendedPortalSetConfig_CrossClientLeak reproduces, deterministically, a
// cross-client GUC leak on the extended (portal) query protocol.
//
// The gateway plans an unpinned `SELECT set_config(name, val, false)` as a
// SessionStateBranch whose unpinned route rewrites is_local to true so the value
// reverts on the pooled backend. But Route.PortalStreamExecute reissues the
// client's ORIGINAL portal (portalInfo) rather than the rewritten route query,
// so the rewrite is a no-op on the extended protocol: the set_config runs
// is_local=false and PERSISTS on the pooled backend. That backend returns to the
// regular pool labelled with the request map (which predates tracking the
// value), so its label omits the GUC it now carries.
//
// The collision is made deterministic despite a large pool:
//   - connection A uses a distinctive settings bucket (a non-default
//     lock_timeout) so its released backend sits alone in that bucket, and its
//     portal returns pg_backend_pid() so we know exactly which backend it was;
//   - connection B requests the identical bucket and keeps running plain queries
//     (regular pool) until it lands on that exact backend, then reads work_mem.
//     Connection B never set work_mem, so a non-default value means the label
//     lied and the bucket pointer-hit skipped the reset — the leak.
//
// This reproduced the bug before the fix; Route.PortalStreamExecute now reissues
// the rewritten route query (r.Query) instead of the client's original portal, so
// the unpinned is_local:=true revert reaches the backend and the test passes.
func TestExtendedPortalSetConfig_CrossClientLeak(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 3*time.Minute)

	const bucketSQL = "SET lock_timeout = '7331ms'" // distinctive, non-gateway-managed bucket
	const leakVal = "256MB"

	// Connection A: distinctive bucket, then a completing row-limited portal that
	// persists work_mem; capture the reserved backend's pid from the portal row.
	connA := connectClientToGateway(t, ctx, setup.MultigatewayPgPort)
	_, err := connA.Query(ctx, bucketSQL)
	require.NoError(t, err)
	require.NoError(t, connA.Parse(ctx, "s1",
		"SELECT set_config('work_mem', '256MB', false), pg_backend_pid()", nil))
	var portalPID string
	// maxRows = 0 (fetch-all): the portal completes on a REGULAR pooled backend.
	// The gateway plans the unpinned (revert) branch, but Route.PortalStreamExecute
	// reissues the client's original portal (is_local=false), so the value
	// persists on that regular backend regardless.
	completed, err := connA.BindAndExecute(ctx, "p1", "s1", nil, nil, nil, 0,
		func(_ context.Context, r *sqltypes.Result) error {
			if len(r.Rows) > 0 && len(r.Rows[0].Values) > 1 {
				portalPID = string(r.Rows[0].Values[1])
			}
			return nil
		})
	require.NoError(t, err)
	require.True(t, completed)
	require.NotEmpty(t, portalPID, "must capture the portal backend pid")
	require.NoError(t, connA.Close())
	t.Logf("connA portal ran on backend pid %s", portalPID)

	// Connection B: same bucket; keep running plain queries (regular pool) until
	// it reuses connA's exact backend, then verify it does not carry connA's
	// work_mem — a value connB never set.
	connB := connectClientToGateway(t, ctx, setup.MultigatewayPgPort)
	defer connB.Close()
	_, err = connB.Query(ctx, bucketSQL)
	require.NoError(t, err)

	const maxTries = 400
	hit := false
	for i := 0; i < maxTries && !hit; i++ {
		results, err := connB.Query(ctx, "SELECT pg_backend_pid()::text, current_setting('work_mem')")
		require.NoError(t, err)
		require.NotEmpty(t, results)
		require.NotEmpty(t, results[0].Rows)
		pid := string(results[0].Rows[0].Values[0])
		workMem := string(results[0].Rows[0].Values[1])
		if pid == portalPID {
			hit = true
			assert.NotEqual(t, leakVal, workMem,
				"reused connA's portal backend (pid %s) but it reports work_mem=%s, which connB never set", pid, workMem)
		}
	}
	require.True(t, hit, "connB never reused connA's portal backend (pid %s) within %d queries", portalPID, maxTries)
}
