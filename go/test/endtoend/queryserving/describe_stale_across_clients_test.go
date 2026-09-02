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
// unrelated client sessions. PostgreSQL revalidates a prepared statement's
// result shape on Bind/Execute, but NOT on a bare Describe('S', name)
// (postgres.c exec_describe_statement_message just replays whatever
// resultDesc was recorded at the last successful Parse). So a client that
// never touched the original statement, but happens to be handed a backend
// that has it cached from before a DDL changed the table's shape, can be
// told the pre-DDL shape on Describe with no error at all.
//
// This only reproduces through multigateway/multipooler's pooling — a direct
// connection to PostgreSQL always gets a fresh backend with no cached
// statement, so there is nothing stale to inherit. Unlike
// TestCachedPlanReprepareAfterDDL (the sibling regression for the
// Bind/Execute side of this same bug class, which PostgreSQL's own plan
// revalidation catches and the gateway heals via 0A000), this test does not
// run against setup.GetComparisonTargets — the bug is specific to the
// pooled/gateway path.
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
