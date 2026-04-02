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
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestSimpleProtocolPreparedStatements tests PREPARE/EXECUTE/DEALLOCATE via the
// simple query protocol, verifying that the gateway handles them locally using
// the prepared statement consolidator (analogous to the extended query protocol).
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestSimpleProtocolPreparedStatements(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping prepared statement test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
			db, err := sql.Open("postgres", connStr)
			require.NoError(t, err)
			defer db.Close()

			// Force a single connection so all statements go to the same session.
			db.SetMaxOpenConns(1)

			ctx := utils.WithTimeout(t, 30*time.Second)

			tableName := fmt.Sprintf("prep_test_%d", time.Now().UnixNano())
			_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, value TEXT)", tableName))
			require.NoError(t, err)
			defer func() {
				_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName)
			}()

			_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'hello'), (2, 'world'), (3, 'foo')", tableName))
			require.NoError(t, err)

			t.Run("prepare_and_execute_no_params", func(t *testing.T) {
				_, err := db.ExecContext(ctx, fmt.Sprintf("PREPARE allrows AS SELECT id, value FROM %s ORDER BY id", tableName))
				require.NoError(t, err)

				rows, err := db.QueryContext(ctx, "EXECUTE allrows")
				require.NoError(t, err)
				defer rows.Close()

				var ids []int
				var vals []string
				for rows.Next() {
					var id int
					var val string
					require.NoError(t, rows.Scan(&id, &val))
					ids = append(ids, id)
					vals = append(vals, val)
				}
				require.NoError(t, rows.Err())
				assert.Equal(t, []int{1, 2, 3}, ids)
				assert.Equal(t, []string{"hello", "world", "foo"}, vals)

				_, err = db.ExecContext(ctx, "DEALLOCATE allrows")
				require.NoError(t, err)
			})

			t.Run("prepare_and_execute_with_params", func(t *testing.T) {
				_, err := db.ExecContext(ctx, fmt.Sprintf("PREPARE byid (int) AS SELECT value FROM %s WHERE id = $1", tableName))
				require.NoError(t, err)

				var value string
				err = db.QueryRowContext(ctx, "EXECUTE byid(1)").Scan(&value)
				require.NoError(t, err)
				assert.Equal(t, "hello", value)

				err = db.QueryRowContext(ctx, "EXECUTE byid(2)").Scan(&value)
				require.NoError(t, err)
				assert.Equal(t, "world", value)

				_, err = db.ExecContext(ctx, "DEALLOCATE byid")
				require.NoError(t, err)
			})

			t.Run("prepare_and_execute_with_string_param", func(t *testing.T) {
				_, err := db.ExecContext(ctx, fmt.Sprintf("PREPARE byval (text) AS SELECT id FROM %s WHERE value = $1", tableName))
				require.NoError(t, err)

				var id int
				err = db.QueryRowContext(ctx, "EXECUTE byval('foo')").Scan(&id)
				require.NoError(t, err)
				assert.Equal(t, 3, id)

				_, err = db.ExecContext(ctx, "DEALLOCATE byval")
				require.NoError(t, err)
			})

			t.Run("execute_nonexistent_fails", func(t *testing.T) {
				_, err := db.ExecContext(ctx, "EXECUTE nonexistent")
				require.Error(t, err)
				var pqErr *pq.Error
				require.True(t, errors.As(err, &pqErr), "expected *pq.Error, got %T", err)
				assert.Equal(t, pq.ErrorCode(mterrors.PgSSInvalidSQLStatementName), pqErr.Code)
			})

			t.Run("deallocate_nonexistent_fails", func(t *testing.T) {
				_, err := db.ExecContext(ctx, "DEALLOCATE nonexistent")
				require.Error(t, err)
				var pqErr *pq.Error
				require.True(t, errors.As(err, &pqErr), "expected *pq.Error, got %T", err)
				assert.Equal(t, pq.ErrorCode(mterrors.PgSSInvalidSQLStatementName), pqErr.Code)
			})

			t.Run("deallocate_all", func(t *testing.T) {
				_, err := db.ExecContext(ctx, fmt.Sprintf("PREPARE plan1 AS SELECT 1 FROM %s", tableName)) //nolint:perfsprint // gosec G202 flags string concatenation
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, fmt.Sprintf("PREPARE plan2 AS SELECT 2 FROM %s", tableName)) //nolint:perfsprint // gosec G202 flags string concatenation
				require.NoError(t, err)

				_, err = db.ExecContext(ctx, "DEALLOCATE ALL")
				require.NoError(t, err)

				// Both should be gone
				_, err = db.ExecContext(ctx, "EXECUTE plan1")
				require.Error(t, err)
				_, err = db.ExecContext(ctx, "EXECUTE plan2")
				require.Error(t, err)
			})

			t.Run("prepare_reuse", func(t *testing.T) {
				_, err := db.ExecContext(ctx, fmt.Sprintf("PREPARE reusable AS SELECT id, value FROM %s ORDER BY id LIMIT 1", tableName))
				require.NoError(t, err)

				// Execute multiple times - should work each time
				for i := range 3 {
					var id int
					var val string
					err = db.QueryRowContext(ctx, "EXECUTE reusable").Scan(&id, &val)
					require.NoError(t, err, "EXECUTE attempt %d", i+1)
					assert.Equal(t, 1, id)
					assert.Equal(t, "hello", val)
				}

				_, err = db.ExecContext(ctx, "DEALLOCATE reusable")
				require.NoError(t, err)
			})
		})
	}
}

// TestMultiGateway_MigrationPattern reproduces the failure seen when running
// Miniflux against the multigateway. Miniflux uses database/sql with lib/pq
// and runs schema migrations that mix simple and extended query protocols:
//
//  1. DDL (CREATE TABLE) via simple protocol (no params) — including the
//     schema_version table itself, all within the same transaction.
//  2. A parameterized INSERT to record the migration version, which causes pq
//     to switch to extended query protocol (Parse → Describe → Bind → Execute).
//
// The Describe step fails because the multipooler's Executor.Describe() uses a
// regular pool connection (GetRegularConnWithSettings) instead of the reserved
// transactional connection. The regular connection cannot see the schema_version
// table because it was created inside the uncommitted transaction on the reserved
// connection. The fix is for Executor.Describe() to check options.ReservedConnectionId
// and use the reserved connection, like StreamExecute and ExecuteQuery already do.
func TestMultiGateway_MigrationPattern(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping migration pattern test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 60*time.Second)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	// Single connection — pq must reuse it for the entire transaction.
	db.SetMaxOpenConns(1)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	schemaVersionTable := "schema_version_" + suffix
	usersTable := "mig_users_" + suffix

	defer func() {
		// Clean up outside any transaction.
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+usersTable)
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+schemaVersionTable)
	}()

	// --- Reproduce Miniflux migration v1 ---
	//
	// The schema_version table is created INSIDE the transaction as part of the
	// DDL block. Then a parameterized INSERT records the version. The INSERT
	// triggers extended protocol (Parse → Describe → Bind → Execute). The
	// Describe must use the reserved transactional connection because
	// schema_version only exists within the uncommitted transaction.
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Multi-statement DDL including schema_version — all via simple protocol.
	migrationSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			version TEXT NOT NULL
		);
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			username TEXT NOT NULL UNIQUE,
			password TEXT,
			is_admin BOOLEAN DEFAULT FALSE
		);
	`, schemaVersionTable, usersTable)

	_, err = tx.ExecContext(ctx, migrationSQL)
	require.NoError(t, err, "DDL migration via simple protocol should succeed")

	_, err = tx.ExecContext(ctx, "TRUNCATE "+schemaVersionTable)
	require.NoError(t, err, "TRUNCATE should succeed")

	// This is the failing step: pq sends Parse → Describe → Sync for the
	// parameterized query. The Describe must use the reserved connection
	// because schema_version only exists within this transaction.
	_, err = tx.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %s (version) VALUES ($1)", schemaVersionTable), "1")
	require.NoError(t, err,
		"parameterized INSERT into transaction-local table should succeed "+
			"(Describe must use reserved connection, not regular pool)")

	err = tx.Commit()
	require.NoError(t, err, "commit should succeed")

	// Verify the migration was recorded.
	var version string
	err = db.QueryRowContext(ctx,
		"SELECT version FROM "+schemaVersionTable).Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, "1", version)
}
