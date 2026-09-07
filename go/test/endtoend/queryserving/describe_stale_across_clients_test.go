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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// describeStalePoolCapacity settles the lone test user's regular sub-pool to a
// single backend (global 2 x (1 - reserved ratio 0.5)), making backend reuse
// across two different client connections deterministic instead of a race
// against however many idle backends the pool happens to have. Mirrors
// cancelPoolCapacity in pgbouncertests/cancel_race_test.go.
const (
	describeStalePoolCapacity  = "--connpool-global-capacity=2"
	describeStaleReservedRatio = "--connpool-reserved-ratio=0.5"
	describeStaleRebalanceFast = "--connpool-rebalance-interval=1s"
)

// TestDescribeStaleAcrossClients is the regression for: multipooler's pooled
// backend connections share an already-PREPARE'd statement (keyed by query
// text + param types, see preparedstatement.PoolerConsolidator) across
// unrelated client sessions. A bare Describe('S', name) on a row-returning
// statement DOES revalidate against the current schema, same as Bind/Execute
// — postgres.c exec_describe_statement_message calls CachedPlanGetTargetList,
// which calls RevalidateCachedQuery (plancache.c) — so it can raise the same
// SQLSTATE 0A000 "cached plan must not change result type" a stale
// Bind/Execute would. The bug is that multipooler's Describe RPC, unlike its
// portal-execute paths (see cachedPlanRetry), had no retry/heal logic for
// that error at all. So a client that never touched the original statement,
// but happens to be handed a backend that has it cached from before a DDL
// changed the table's shape, gets hit with a raw 0A000 it has no way to
// recover from — it doesn't know a shared backend-level statement is even
// involved.
//
// This only reproduces through multigateway/multipooler's pooling — a direct
// connection to PostgreSQL always gets a fresh backend with no cached
// statement, so there is nothing stale to inherit. Unlike
// TestCachedPlanReprepareAfterDDL (the sibling regression for the
// Bind/Execute side of this same bug class, which PostgreSQL's own plan
// revalidation catches and multipooler already heals via 0A000), this test
// does not run against setup.GetComparisonTargets — the bug is specific to
// the pooled path.
func TestDescribeStaleAcrossClients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
		shardsetup.WithMultipoolerExtraArgs(describeStalePoolCapacity, describeStaleReservedRatio, describeStaleRebalanceFast),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	ctx := utils.WithTimeout(t, 60*time.Second)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	// Settle the user's regular pool down to a single backend so the two
	// client connections below are forced to share it.
	settleDB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	_, err = settleDB.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err, "warmup query should succeed")
	time.Sleep(5 * time.Second)
	settleDB.Close()

	// Client A: creates the table and warms the canonical prepared statement.
	connA := connectLowLevelToPort(t, ctx, setup.MultigatewayPgPort)
	defer connA.Close()

	_, err = connA.Query(ctx, "DROP TABLE IF EXISTS desctest")
	require.NoError(t, err)
	_, err = connA.Query(ctx, "CREATE TABLE desctest (a int, b int)")
	require.NoError(t, err)
	// A plain defer, not t.Cleanup: t.Cleanup callbacks run after the test
	// function's own defers (including shardsetup's cluster teardown below),
	// so a t.Cleanup here would try to connect after the cluster is gone.
	defer func() {
		c := connectLowLevelToPort(t, context.Background(), setup.MultigatewayPgPort)
		defer c.Close()
		_, _ = c.Query(context.Background(), "DROP TABLE IF EXISTS desctest")
	}()

	require.NoError(t, connA.Parse(ctx, "a_s1", "SELECT * FROM desctest", nil))
	descBefore, err := connA.DescribePrepared(ctx, "a_s1")
	require.NoError(t, err)
	require.Len(t, descBefore.Fields, 2, "before DDL: two columns")
	require.NoError(t, connA.CloseStatement(ctx, "a_s1"))

	// DDL changes the table's shape.
	_, err = connA.Query(ctx, "ALTER TABLE desctest DROP COLUMN b")
	require.NoError(t, err)

	// Client B: a brand-new connection that never touched this statement.
	// Same query text + param types (nil) as client A, so it maps to the same
	// canonical statement at the pooler, and the settled 1-backend pool means
	// it is handed the same backend client A used — the one with a stale
	// PREPARE cached from before the DDL.
	connB := connectLowLevelToPort(t, ctx, setup.MultigatewayPgPort)
	defer connB.Close()

	require.NoError(t, connB.Parse(ctx, "b_s1", "SELECT * FROM desctest", nil))
	descAfter, err := connB.DescribePrepared(ctx, "b_s1")
	require.NoError(t, err)
	require.NoError(t, connB.CloseStatement(ctx, "b_s1"))

	assert.Len(t, descAfter.Fields, 1,
		"a brand-new client's Describe must reflect the post-DDL shape, not a stale shape inherited from a different client's cached statement")
	if len(descAfter.Fields) == 1 {
		assert.Equal(t, "a", descAfter.Fields[0].Name)
	}
}

// TestReprepareParamTypeAfterDDLInTransaction is the regression for the
// in-transaction half of the PostgREST notify_reloading_catalog_cache class of
// bug. After a DDL recreates a table changing the $1 column's type (uuid →
// bigint), a client that re-Parses the same query and Binds a value must run
// against the CURRENT schema, not a backend statement the pooler cached with the
// old parameter type. Reusing the stale plan makes Bind decode the new value
// against the old type and raise SQLSTATE 22P02 (invalid_text_representation).
//
// This is the case the reactive cachedPlanRetry heal CANNOT recover: the error
// lands inside the transaction, which is now aborted, so the heal's re-Parse +
// retry would run in a failed block and is skipped. The gateway's force_reparse
// (a client Parse re-Parses the consolidated backend statement) must prevent the
// staleness proactively. Everything runs inside one transaction on purpose: a
// transaction is pinned to a single backend, so the warm, the DDL, and the
// reprepare all share the backend carrying the stale statement — deterministic
// without settling the pool. A non-canonical query text (extra whitespace) makes
// the gateway's normalized route query differ from the client's, exercising the
// portal rewrite path that must still carry the force-reparse signal.
// Gateway-only: a direct PostgreSQL connection never inherits a stale pooled
// statement.
func TestReprepareParamTypeAfterDDLInTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tests in short mode")
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

	ctx := utils.WithTimeout(t, 60*time.Second)

	// Extra whitespace so the gateway's normalized route query differs from the
	// client's text, exercising the portal rewrite path.
	const reQuery = "SELECT  *  FROM  reptest  WHERE  id = $1"

	conn := connectLowLevelToPort(t, ctx, setup.MultigatewayPgPort)
	defer conn.Close()

	_, err := conn.Query(ctx, "DROP TABLE IF EXISTS reptest")
	require.NoError(t, err)
	_, err = conn.Query(ctx, "CREATE TABLE reptest (id uuid, a int)")
	require.NoError(t, err)
	_, err = conn.Query(ctx, "INSERT INTO reptest VALUES ('11111111-1111-1111-1111-111111111111', 10)")
	require.NoError(t, err)
	defer func() {
		c := connectLowLevelToPort(t, context.Background(), setup.MultigatewayPgPort)
		defer c.Close()
		_, _ = c.Query(context.Background(), "DROP TABLE IF EXISTS reptest")
	}()

	// Everything below runs in one transaction, which pins a single backend.
	_, err = conn.Query(ctx, "BEGIN")
	require.NoError(t, err)
	defer func() { _, _ = conn.Query(ctx, "ROLLBACK") }()

	// Warm the canonical backend statement on the pinned backend: $1 is uuid.
	require.NoError(t, conn.Parse(ctx, "rp_s1", reQuery, nil))
	_, err = conn.BindAndExecute(ctx, "", "rp_s1",
		[][]byte{[]byte("11111111-1111-1111-1111-111111111111")}, nil, nil, 0,
		func(context.Context, *sqltypes.Result) error { return nil })
	require.NoError(t, err, "warm execute with the uuid value should succeed")

	// Recreate the table with id as bigint inside the same transaction (the
	// notify_reloading_catalog_cache DDL), so the pinned backend's cached
	// statement is now stale.
	_, err = conn.Query(ctx, "DROP TABLE reptest")
	require.NoError(t, err)
	_, err = conn.Query(ctx, "CREATE TABLE reptest (id bigint, a int)")
	require.NoError(t, err)
	_, err = conn.Query(ctx, "INSERT INTO reptest VALUES (1, 20)")
	require.NoError(t, err)

	// Re-Parse under a fresh name and Bind the bigint value "1". Pre-fix: the
	// pooler reuses the stale $1::uuid statement on the pinned backend, Bind fails
	// 22P02, the transaction aborts, and the heal cannot retry inside it.
	require.NoError(t, conn.CloseStatement(ctx, "rp_s1"))
	require.NoError(t, conn.Parse(ctx, "rp_s2", reQuery, nil))

	var got *sqltypes.Result
	_, err = conn.BindDescribeAndExecute(ctx, "", "rp_s2",
		[][]byte{[]byte("1")}, nil, nil, 0,
		func(_ context.Context, r *sqltypes.Result) error {
			if r != nil && len(r.Rows) > 0 {
				got = r
			}
			return nil
		})
	require.NoError(t, err,
		"re-Parse + Bind of the bigint value after the DDL must run against the current schema, not the stale uuid-typed cached statement (22P02)")
	require.NotNil(t, got, "the row with id = 1 should be returned")
	require.Len(t, got.Rows, 1)
	require.NotEmpty(t, got.Rows[0].Values)
	assert.Equal(t, "1", string(got.Rows[0].Values[0]), "id column of the matched row")
}
