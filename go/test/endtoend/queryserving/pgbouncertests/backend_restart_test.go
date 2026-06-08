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
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Group 2 of the PgBouncer port — continuous-load-under-disruption (the
// stress.py ethos), ported from test_operations.py::test_database_restart and
// ::test_reconnect.
//
// PgBouncer's scenario: drive traffic while the backend database is restarted
// underneath the proxy, and assert that (a) in-flight statements fail cleanly
// rather than hanging or returning a torn packet, and (b) new statements
// transparently land on a fresh backend connection once the database is back.
//
// These are black-box tests: they speak only the PostgreSQL wire protocol to
// the multigateway and observe only what a normal client can (query results and
// errors, plus pg_backend_pid() read from the client's own result rows). The
// fault is injected with KillPostgres (SIGKILL — a hard crash), after which the
// multipooler's postgres monitor auto-restarts the same backend (crash
// recovery; the primary stays the primary — there is no failover here).
//
// Each test runs in its own NewIsolated cluster so the deliberate backend crash
// cannot leak into the shared-cluster tests in this package.

// TestBackendCrashRecoveryUnderLoad drives continuous writes through the gateway
// while the primary's postgres is hard-killed, then asserts the pool transparently
// reconnects: writes resume on their own (no client reconnect, no hang) and every
// acknowledged write survived the crash. This is the stress.py ethos —
// throughput recovers after the disruption — combined with a durability check.
func TestBackendCrashRecoveryUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	// Continuous load: several workers INSERTing through the gateway. database/sql
	// pools client connections, so a connection broken by the crash is discarded
	// and a fresh one opened transparently — exactly the recovery we want to see.
	validator, validatorCleanup, err := shardsetup.NewWriterValidator(t, db,
		shardsetup.WithWorkerCount(6),
		shardsetup.WithWriteInterval(5*time.Millisecond),
		shardsetup.WithQueryTimeout(10*time.Second),
	)
	require.NoError(t, err)
	defer validatorCleanup()
	validator.Start(t)

	// Establish a baseline so we know load is flowing before the crash.
	waitForSuccessfulWrites(t, validator, 50, 30*time.Second)
	preCrash, _ := validator.Stats()
	t.Logf("baseline established: %d successful writes before crash", preCrash)

	// Hard-crash postgres on the primary. Restarts stay enabled, so the
	// multipooler monitor auto-restarts the same backend.
	setup.KillPostgres(t, setup.PrimaryName)
	waitForPostgresReady(t, setup, 60*time.Second)
	t.Log("postgres auto-restarted by the monitor")

	// Transparent recovery: load must resume on its own. Wait for the success
	// count to climb well past where it was at recovery time, with no client
	// reconnect or intervention.
	atRecovery, _ := validator.Stats()
	require.Eventually(t, func() bool {
		s, _ := validator.Stats()
		return s >= atRecovery+30
	}, 30*time.Second, 200*time.Millisecond,
		"writes should resume on their own after the backend is back (transparent reconnect)")

	validator.Stop()
	successful, failed := validator.Stats()
	t.Logf("after recovery: %d successful, %d failed writes (failures expected during the outage window)", successful, failed)
	require.Greater(t, successful, preCrash, "more writes must have succeeded after recovery than before the crash")

	// Durability: every acknowledged write must have survived crash recovery.
	// A SIGKILL mid-commit can leave a row committed on disk whose ack never
	// reached the client (counted as failed), so the table may hold a few more
	// rows than we counted successful — hence GreaterOrEqual, not Equal.
	var count int
	require.Eventually(t, func() bool {
		ctx := utils.WithShortDeadline(t)
		// #nosec G202 -- tableName comes from test setup, not user input.
		return db.QueryRowContext(ctx, "SELECT count(*) FROM "+validator.TableName()).Scan(&count) == nil
	}, 15*time.Second, 500*time.Millisecond, "count(*) should be readable after recovery")
	assert.GreaterOrEqualf(t, count, successful,
		"every acknowledged write (%d) must survive crash recovery; table holds %d rows", successful, count)
}

// TestBackendCrashTransparentReconnect is the test_reconnect analog: a single
// client session reads its backend pid, the backend is hard-killed and
// auto-restarted, and the same client session must keep working — its next
// statement lands on a fresh backend (a different pid), proving the gateway
// reconnected underneath the client rather than handing back a dead connection.
func TestBackendCrashTransparentReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	ctx := utils.WithTimeout(t, 120*time.Second)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	conn := connectGateway(t, ctx, gatewayDSN)
	defer conn.Close(ctx)

	var pid1 int
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid1))
	t.Logf("client session started on backend pid %d", pid1)

	setup.KillPostgres(t, setup.PrimaryName)
	waitForPostgresReady(t, setup, 60*time.Second)
	t.Log("postgres auto-restarted by the monitor")

	// The same client session must recover. The first statement after the crash
	// may error cleanly (the borrowed backend is gone) — that is the "clean
	// error, not a hang" property, enforced by the per-attempt deadline. It must
	// then succeed on a fresh backend.
	var pid2 int
	require.Eventually(t, func() bool {
		attemptCtx := utils.WithShortDeadline(t)
		err := conn.QueryRow(attemptCtx, "SELECT pg_backend_pid()").Scan(&pid2)
		if err != nil {
			t.Logf("post-crash statement still failing (expected during recovery): %v", err)
			return false
		}
		return true
	}, 30*time.Second, 500*time.Millisecond,
		"the same client session must recover and serve queries after the backend restarts")

	assert.NotEqual(t, pid1, pid2,
		"after a crash the session must land on a fresh backend; got the same pid %d, suggesting a stale pooled connection", pid1)
	t.Logf("session recovered transparently onto fresh backend pid %d", pid2)
}

// waitForSuccessfulWrites blocks until the validator has recorded at least min
// successful writes, or fails the test after timeout.
func waitForSuccessfulWrites(t *testing.T, v *shardsetup.WriterValidator, minWrites int, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		s, _ := v.Stats()
		return s >= minWrites
	}, timeout, 100*time.Millisecond, "expected at least %d successful writes", minWrites)
}

// waitForPostgresReady polls the primary's multipooler manager until it reports
// postgres ready again (after the monitor auto-restarts a crashed backend).
func waitForPostgresReady(t *testing.T, setup *shardsetup.ShardSetup, timeout time.Duration) {
	t.Helper()
	client := setup.NewPrimaryClient(t)
	defer client.Close()
	require.Eventually(t, func() bool {
		ctx := utils.WithShortDeadline(t)
		status, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			return false
		}
		return status.Status.PostgresReady
	}, timeout, 500*time.Millisecond, "postgres should be auto-restarted by the monitor")
}
