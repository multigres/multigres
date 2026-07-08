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
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestSelfTerminateBackendPreservesDiagnostic guards against a regression in
// go/common/pgprotocol/client's processQueryResponses: PostgreSQL sends a
// FATAL 57P01 ErrorResponse for pg_terminate_backend(pg_backend_pid()) and
// then closes the connection without ever sending ReadyForQuery. The client
// must return that diagnostic as-is instead of discarding it in favor of the
// EOF that follows, and multipooler must propagate it end-to-end instead of
// masking it with the synthesized reserved-connection-terminated (08006)
// fallback. Matches vanilla PostgreSQL's behavior for the same statement.
func TestSelfTerminateBackendPreservesDiagnostic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	var port int
	for _, target := range setup.GetComparisonTargets(t) {
		if target.Name == "multigateway" {
			port = target.Port
		}
	}
	require.NotZero(t, port, "multigateway target port")

	connStr := shardsetup.GetTestUserDSN("localhost", port, "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)

	res, err := conn.Query(ctx, "SELECT pg_backend_pid()")
	require.NoError(t, err)
	var pid int
	require.True(t, res.Next())
	require.NoError(t, res.Scan(&pid))
	res.Close()
	t.Logf("backend pid = %d", pid)

	_, err = conn.Exec(ctx, "SELECT pg_terminate_backend(pg_backend_pid())")

	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "expected a *pgconn.PgError, got %T: %v", err, err)
	t.Logf("pgErr Code=%s Severity=%s Message=%q Detail=%q",
		pgErr.Code, pgErr.Severity, pgErr.Message, pgErr.Detail)

	require.Equal(t, "57P01", pgErr.Code, "expected the real admin_shutdown SQLSTATE to be preserved end-to-end")
}
