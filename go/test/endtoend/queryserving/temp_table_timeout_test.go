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
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultigateway_TempTableSurvivesStatementTimeout is a regression test for a
// reserved-connection bug: a session temp table created via CREATE TABLE AS
// pins a reserved ("temp_table") connection, but a statement_timeout
// cancellation on that connection caused the gateway to drop the reservation
// (the cancelled stream returns a zero reserved state, which applyReservedState
// treated as "connection gone"). Subsequent statements then ran on a different
// pooled backend and the session temp table vanished — reproducing the PostGIS
// interrupt tests' "relation \"_time\"/\"_inputs\" does not exist".
//
// PostgreSQL keeps session temp tables across a statement-level cancellation, so
// the temp table must remain visible after the timeout.
func TestMultigateway_TempTableSurvivesStatementTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	ctx := utils.WithTimeout(t, 120*time.Second)

	// Pin a single client session so the temp table and the later read share one
	// gateway connection.
	c, err := db.Conn(ctx)
	require.NoError(t, err)
	defer c.Close()

	// CREATE TABLE AS (CTAS) temp table — reserves a temp-table connection.
	_, err = c.ExecContext(ctx, "CREATE TEMP TABLE _inputs AS SELECT 1::int AS id")
	require.NoError(t, err, "failed to create temp table")

	// A statement the server cancels via statement_timeout.
	_, err = c.ExecContext(ctx, "SET statement_timeout TO 100")
	require.NoError(t, err)
	_, err = c.ExecContext(ctx, "SELECT pg_sleep(5)")
	require.Error(t, err, "pg_sleep should be cancelled by statement_timeout")
	_, err = c.ExecContext(ctx, "SET statement_timeout TO 0")
	require.NoError(t, err)

	// The session temp table must still be there.
	var id int
	err = c.QueryRowContext(ctx, "SELECT id FROM _inputs").Scan(&id)
	require.NoError(t, err, "temp table must survive a statement_timeout cancellation")
	require.Equal(t, 1, id)
}

// TestMultigateway_StatementTimeoutInTransaction guards that the temp-table fix
// does not change transaction semantics: a statement_timeout inside an explicit
// transaction must abort the transaction (subsequent statements error until the
// block ends), and ROLLBACK must recover the session.
func TestMultigateway_StatementTimeoutInTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	ctx := utils.WithTimeout(t, 120*time.Second)

	c, err := db.Conn(ctx)
	require.NoError(t, err)
	defer c.Close()

	_, err = c.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = c.ExecContext(ctx, "SET statement_timeout TO 100")
	require.NoError(t, err)
	_, err = c.ExecContext(ctx, "SELECT pg_sleep(5)")
	require.Error(t, err, "pg_sleep should be cancelled by statement_timeout")

	// The transaction is now aborted: a normal statement must fail until the
	// block is closed (PostgreSQL: "current transaction is aborted").
	_, err = c.ExecContext(ctx, "SELECT 1")
	require.Error(t, err, "transaction must be aborted after a statement_timeout inside it")

	// ROLLBACK ends the aborted block and the session recovers.
	_, err = c.ExecContext(ctx, "ROLLBACK")
	require.NoError(t, err)

	var n int
	err = c.QueryRowContext(ctx, "SELECT 1").Scan(&n)
	require.NoError(t, err, "session must work normally after ROLLBACK")
	require.Equal(t, 1, n)
}
