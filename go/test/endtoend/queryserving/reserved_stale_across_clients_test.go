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

// reservedStalePoolCapacity settles the lone test user's pool down to a
// single backend in *both* the regular and reserved sub-pools (global 2,
// reserved ratio 0.5 -> 1 and 1), making backend reuse across two different
// client connections' reservations deterministic. Mirrors
// describeStalePoolCapacity / cancelPoolCapacity in
// pgbouncertests/cancel_race_test.go.
const (
	reservedStalePoolCapacity  = "--connpool-global-capacity=2"
	reservedStaleReservedRatio = "--connpool-reserved-ratio=0.5"
	reservedStaleRebalanceFast = "--connpool-rebalance-interval=1s"
)

// TestReservedExecuteStaleAcrossClients is portalExecuteWithReserved's
// counterpart to TestDescribeStaleAcrossClients. A reserved connection is
// pinned for the lifetime of an explicit transaction and released back to
// the pool on COMMIT; multipooler shares its already-PREPARE'd statement
// (keyed by query text + param types, see preparedstatement.PoolerConsolidator)
// with whichever client reserves that same backend next.
//
// After a DDL changes the table's shape and client A's transaction releases
// the backend, a brand-new client B — who never touched the original
// statement — reserves that same backend for its own transaction and freshly
// Parses the same query. It must see the CURRENT schema, not the stale plan
// cached from before the DDL, matching direct PostgreSQL where a fresh Parse
// always re-plans.
//
// This works inside client B's transaction because a client Parse forces the
// pooler to re-Parse the consolidated backend statement (force_reparse): the
// transaction is pinned to the one reserved backend, so re-Parsing it up front
// is sufficient. The reactive cachedPlanRetry heal could not help here — any
// error inside an explicit transaction aborts it, and the close+re-Parse+retry
// would run in the already-failed block — so before force_reparse this same
// path surfaced a stale-plan 0A000 that client B had to ROLLBACK and retry.
// Now it just returns the post-DDL shape.
func TestReservedExecuteStaleAcrossClients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
		shardsetup.WithMultipoolerExtraArgs(reservedStalePoolCapacity, reservedStaleReservedRatio, reservedStaleRebalanceFast),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	ctx := utils.WithTimeout(t, 60*time.Second)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	// Settle the user's pool (both sub-pools) down to a single backend each,
	// so the two client connections' reservations below are forced to share
	// the one reserved backend.
	settleDB, err := sql.Open("postgres", gatewayDSN)
	require.NoError(t, err)
	_, err = settleDB.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err, "warmup query should succeed")
	time.Sleep(5 * time.Second)
	settleDB.Close()

	// collectFields runs the fused Bind+Describe+Execute path (so the
	// portal's RowDescription rides back through the callback — plain
	// BindAndExecute never sends one) and returns both the results and any
	// error, leaving the error check to the caller.
	collectFields := func(t *testing.T, conn interface {
		BindDescribeAndExecute(context.Context, string, string, [][]byte, []int16, []int16, int32, func(context.Context, *sqltypes.Result) error) (bool, error)
	}, stmtName string,
	) ([]*sqltypes.Result, error) {
		t.Helper()
		var results []*sqltypes.Result
		_, err := conn.BindDescribeAndExecute(ctx, "", stmtName, nil, nil, nil, 0,
			func(_ context.Context, r *sqltypes.Result) error {
				results = append(results, r)
				return nil
			})
		return results, err
	}
	fieldsOf := func(results []*sqltypes.Result) []*sqltypes.Result {
		for _, r := range results {
			if r.Fields != nil {
				return []*sqltypes.Result{r}
			}
		}
		return nil
	}

	// Client A: creates the table, then reserves a backend for an explicit
	// transaction and warms the canonical prepared statement on it.
	connA := connectLowLevelToPort(t, ctx, setup.MultigatewayPgPort)
	defer connA.Close()

	_, err = connA.Query(ctx, "DROP TABLE IF EXISTS restest")
	require.NoError(t, err)
	_, err = connA.Query(ctx, "CREATE TABLE restest (a int, b int)")
	require.NoError(t, err)
	_, err = connA.Query(ctx, "INSERT INTO restest VALUES (1, 2)")
	require.NoError(t, err)
	// A plain defer, not t.Cleanup: t.Cleanup callbacks run after the test
	// function's own defers (including shardsetup's cluster teardown below),
	// so a t.Cleanup here would try to connect after the cluster is gone.
	defer func() {
		c := connectLowLevelToPort(t, context.Background(), setup.MultigatewayPgPort)
		defer c.Close()
		_, _ = c.Query(context.Background(), "DROP TABLE IF EXISTS restest")
	}()

	_, err = connA.Query(ctx, "BEGIN")
	require.NoError(t, err)
	require.NoError(t, connA.Parse(ctx, "a_s1", "SELECT * FROM restest", nil))
	rawA, err := collectFields(t, connA, "a_s1")
	require.NoError(t, err)
	resA := fieldsOf(rawA)
	require.Len(t, resA, 1, "expected one field-bearing result")
	require.Len(t, resA[0].Fields, 2, "before DDL: two columns")
	require.NoError(t, connA.CloseStatement(ctx, "a_s1"))
	_, err = connA.Query(ctx, "COMMIT")
	require.NoError(t, err)

	// DDL changes the table's shape.
	_, err = connA.Query(ctx, "ALTER TABLE restest DROP COLUMN b")
	require.NoError(t, err)

	// Client B: a brand-new connection that never touched this statement.
	// Same query text + param types (nil) as client A, so it maps to the
	// same canonical statement at the pooler, and the settled 1-backend
	// reserved pool means its own transaction reserves the same backend
	// client A used — the one with a stale PREPARE cached from before the
	// DDL. Its fresh Parse forces a re-Parse of that backend statement, so the
	// Execute returns the current post-DDL shape rather than the stale one.
	connB := connectLowLevelToPort(t, ctx, setup.MultigatewayPgPort)
	defer connB.Close()

	_, err = connB.Query(ctx, "BEGIN")
	require.NoError(t, err)
	require.NoError(t, connB.Parse(ctx, "b_s1", "SELECT * FROM restest", nil))
	rawB, err := collectFields(t, connB, "b_s1")
	require.NoError(t, err,
		"a fresh client Parse after the DDL must run against the current schema, not the stale cached statement on the shared reserved backend")
	resB := fieldsOf(rawB)
	require.NoError(t, connB.CloseStatement(ctx, "b_s1"))
	_, err = connB.Query(ctx, "COMMIT")
	require.NoError(t, err)

	require.Len(t, resB, 1, "expected one field-bearing result")
	assert.Len(t, resB[0].Fields, 1, "after DDL: one column")
	if len(resB[0].Fields) == 1 {
		assert.Equal(t, "a", resB[0].Fields[0].Name)
	}
}
