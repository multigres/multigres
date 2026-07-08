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
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestTransactionAbortedOnFailoverGraceExpiry is a regression test for an MTF01
// leak. When a client holds a transaction open (idle-in-transaction) across a
// planned failover for longer than the pooler's connection-drain grace period,
// the old primary force-closes the reserved connection (rolling the transaction
// back) and then demotes to REPLICA. A subsequent statement on that transaction
// used to surface MTF01 ("planned failover in progress, will be retried
// automatically") — which is wrong on two counts: the transaction no longer
// exists, and nothing retries it.
//
// After the fix the pooler admits the in-flight reserved-connection operation
// (the connection's existence is the real gate) and the executor returns an
// honest 08006 (connection_failure) so the client knows to open a new
// connection — the reservation (and the backend behind it) is gone, not just
// the transaction.
//
// Before the fix this test FAILS: the post-failover statement returns SQLSTATE
// "MTF01" instead of "08006".
func TestTransactionAbortedOnFailoverGraceExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping transaction-failover test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping transaction-failover test")
	}

	// A short drain grace period guarantees the idle-in-transaction reserved
	// connection is force-closed quickly once failover demotes its pooler. It must
	// be > 0 — a 0 grace period means an unbounded drain that would wait for our
	// COMMIT and never reproduce the bug.
	setup, cleanup := newFailoverTxnTestCluster(t, "--connpool-drain-grace-period=1s")
	defer cleanup()

	// Create the test table up front via a throwaway connection.
	gatewayDB := openGatewayDB(t, setup)
	_, err := gatewayDB.Exec("CREATE TABLE IF NOT EXISTS txn_failover_test (id INT PRIMARY KEY, val TEXT)")
	require.NoError(t, err)
	gatewayDB.Close()

	// Dedicated single connection so the whole transaction stays pinned to one
	// reserved backend on the current primary.
	ctx := context.Background()
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Open a transaction and run a statement. This reserves a backend on the
	// primary and leaves it idle-in-transaction.
	_, err = conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO txn_failover_test (id, val) VALUES (1, 'before-failover')")
	require.NoError(t, err)

	t.Logf("transaction open and idle on primary %s; triggering failover", setup.PrimaryName)

	// Fail over away from the current primary. The old primary drains for its (1s)
	// grace period; because our connection sits idle-in-transaction it keeps the
	// drain blocked until the grace period expires, then the connection is
	// force-closed and the transaction rolled back. triggerFailover's recovery
	// wait is far longer than the grace period, so by the time it returns the
	// reserved connection is gone and the old pooler is a demoted REPLICA.
	triggerFailover(t, setup)

	// Belt-and-suspenders: ensure the grace period has elapsed (it already has,
	// given the recovery wait above) before probing the dead transaction.
	time.Sleep(2 * time.Second)

	// The next statement on the now-dead transaction must surface an honest,
	// retryable error — 08006 (connection_failure), not MTF01.
	_, err = conn.Exec(ctx, "INSERT INTO txn_failover_test (id, val) VALUES (2, 'after-failover')")
	require.Error(t, err, "a statement on a transaction killed by failover must return an error")

	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T: %v", err, err)
	t.Logf("post-failover error: SQLSTATE=%s severity=%s message=%q", pgErr.Code, pgErr.Severity, pgErr.Message)

	assert.NotEqual(t, mterrors.MTF01.ID, pgErr.Code,
		"MTF01 must not leak to the client for a transaction aborted by failover (regression)")
	assert.Equal(t, mterrors.PgSSConnectionFailure, pgErr.Code,
		"transaction aborted by failover should surface 08006 connection_failure so the client opens a new connection")
}

// newFailoverTxnTestCluster creates a 3-node cluster with buffering enabled on
// the gateway and custom flags applied to every multipooler (e.g. a short
// connpool-drain-grace-period). Buffering is enabled to demonstrate that it does
// NOT hide this error — reserved-connection operations bypass the gateway buffer.
func newFailoverTxnTestCluster(t *testing.T, poolerArgs ...string) (*shardsetup.ShardSetup, func()) {
	t.Helper()
	bufferArgs := []string{
		"--buffer-enabled",
		"--buffer-window", "10s",
		"--buffer-size", "1000",
		"--buffer-max-failover-duration", "20s",
		"--buffer-min-time-between-failovers", "0s",
		"--buffer-drain-concurrency", "5",
	}
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(3),
		shardsetup.WithMultigateway(),
		func(c *shardsetup.SetupConfig) {
			c.MultigatewayExtraArgs = append(c.MultigatewayExtraArgs, bufferArgs...)
		},
		shardsetup.WithMultipoolerExtraArgs(poolerArgs...),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithLeaderFailoverGracePeriod("0s", "0s"),
	)
	setup.StartMultiorchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	require.NotNil(t, setup.GetPrimary(t), "primary should exist after bootstrap")
	t.Logf("Initial primary: %s", setup.PrimaryName)

	return setup, cleanup
}
