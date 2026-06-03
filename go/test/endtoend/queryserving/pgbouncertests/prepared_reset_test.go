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
	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPreparedStatementResetSemantics verifies that DEALLOCATE ALL makes
// previously-prepared statements unavailable, identically on direct PostgreSQL
// and through the multigateway. Ported from test_prepared.py::test_deallocate_all.
//
// After DEALLOCATE ALL, an EXECUTE of a previously-prepared statement must fail
// with invalid_sql_statement_name; running both targets makes PostgreSQL the
// oracle. DISCARD ALL (test_discard_all) is intentionally NOT covered here — see
// TestPreparedStatementDiscardAll for why it is quarantined.
func TestPreparedStatementResetSemantics(t *testing.T) {
	suiteutil.SkipUnlessEnabled(t, suiteutil.EnvRunExtendedQueryServingTests)
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
			for _, reset := range []string{"DEALLOCATE ALL"} {
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
				})
			}
		})
	}
}

// TestPreparedStatementDiscardAll is the DISCARD ALL counterpart of
// TestPreparedStatementResetSemantics, ported from test_prepared.py::test_discard_all.
//
// It currently FAILS on the multigateway target: this is a real, unfixed
// multigres bug, not a test problem. PostgreSQL's DISCARD ALL includes
// DEALLOCATE ALL, so after it the prepared-statement name is free and PREPARE of
// the same name succeeds again. Through the gateway, DISCARD ALL does NOT clear
// the prepared-statement consolidator, so the name stays taken and re-PREPARE
// fails with "prepared statement already exists". (The same un-cleared state
// also leaves the multipooler's per-connection tracking stale, poisoning the
// pooled backend for later sessions — a separate symptom of the same gap.)
//
// We assert the re-PREPARE outcome rather than "EXECUTE fails" because the
// latter is an unreliable signal: on a single connection the post-DISCARD
// EXECUTE lands on the very backend DISCARD ALL ran on, whose now-stale tracking
// makes Bind fail with the same SQLSTATE PostgreSQL returns for a genuinely
// dropped statement — so it would pass for the wrong reason. The re-PREPARE
// check is deterministic and independent of backend routing.
//
// The fix is deferred pending a design decision — notably whether the proxy
// should forward DISCARD ALL to a shared pooled backend at all (PgBouncer
// special-cases it). The test runs in its OWN isolated cluster so the poisoned
// pooled connection cannot corrupt the other tests in this package: this test
// fails, the rest stay green.
func TestPreparedStatementDiscardAll(t *testing.T) {
	suiteutil.SkipUnlessEnabled(t, suiteutil.EnvRunExtendedQueryServingTests)
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultigateway(),
	)
	defer cleanup()

	ctx := utils.WithTimeout(t, 60*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
			db, err := sql.Open("postgres", connStr)
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1) // one client session for all statements

			_, err = db.ExecContext(ctx, "PREPARE rs1 AS SELECT 1")
			require.NoError(t, err)

			// Sanity: the prepared statement executes before the reset.
			var n int
			require.NoError(t, db.QueryRowContext(ctx, "EXECUTE rs1").Scan(&n))
			require.Equal(t, 1, n)

			_, err = db.ExecContext(ctx, "DISCARD ALL")
			require.NoError(t, err, "DISCARD ALL should succeed")

			// DISCARD ALL includes DEALLOCATE ALL, so the name is free again and
			// re-PREPARE must succeed (it does on PostgreSQL; it fails through the
			// gateway today because the consolidator was not cleared).
			_, err = db.ExecContext(ctx, "PREPARE rs1 AS SELECT 1")
			require.NoError(t, err,
				"re-PREPARE after DISCARD ALL should succeed — DISCARD ALL must drop the session's prepared statements")
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
