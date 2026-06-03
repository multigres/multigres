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
	"errors"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPreparedStatementResetSemantics verifies that both DEALLOCATE ALL and
// DISCARD ALL drop the session's prepared statements, identically on direct
// PostgreSQL and through the multigateway. Ported from
// test_prepared.py::test_deallocate_all and ::test_discard_all.
//
// For each reset we assert two things, with PostgreSQL as the oracle (both
// targets run the same sequence): an EXECUTE of a previously-prepared statement
// fails with invalid_sql_statement_name, and the statement name is free to
// re-PREPARE.
func TestPreparedStatementResetSemantics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			for _, reset := range []string{"DEALLOCATE ALL", "DISCARD ALL"} {
				t.Run(reset, func(t *testing.T) {
					connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
					db, err := sql.Open("postgres", connStr)
					require.NoError(t, err)
					defer db.Close()
					db.SetMaxOpenConns(1) // one client session for all statements

					_, err = db.ExecContext(ctx, "PREPARE rs1 AS SELECT 1")
					require.NoError(t, err)
					_, err = db.ExecContext(ctx, "PREPARE rs2 AS SELECT 2")
					require.NoError(t, err)

					// Sanity: both prepared statements execute before the reset.
					var n int
					require.NoError(t, db.QueryRowContext(ctx, "EXECUTE rs1").Scan(&n))
					require.Equal(t, 1, n)
					require.NoError(t, db.QueryRowContext(ctx, "EXECUTE rs2").Scan(&n))
					require.Equal(t, 2, n)

					_, err = db.ExecContext(ctx, reset)
					require.NoError(t, err, "%s should succeed", reset)

					// After the reset, both statements must be gone.
					_, err = db.ExecContext(ctx, "EXECUTE rs1")
					assertInvalidStatementName(t, err, "EXECUTE rs1 after %s", reset)
					_, err = db.ExecContext(ctx, "EXECUTE rs2")
					assertInvalidStatementName(t, err, "EXECUTE rs2 after %s", reset)

					// ...and the names are free again, so re-PREPARE must succeed.
					_, err = db.ExecContext(ctx, "PREPARE rs1 AS SELECT 1")
					require.NoError(t, err,
						"re-PREPARE after %s should succeed — the reset must drop the session's prepared statements", reset)
				})
			}
		})
	}
}

// assertInvalidStatementName asserts that err is a pq error with the
// invalid_sql_statement_name SQLSTATE (what PostgreSQL returns for EXECUTE of a
// non-existent prepared statement).
func assertInvalidStatementName(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()
	require.Error(t, err, msgAndArgs...)
	var pqErr *pq.Error
	require.True(t, errors.As(err, &pqErr), "expected *pq.Error, got %T (%v)", err, err)
	assert.Equal(t, pq.ErrorCode(mterrors.PgSSInvalidSQLStatementName), pqErr.Code, msgAndArgs...)
}
