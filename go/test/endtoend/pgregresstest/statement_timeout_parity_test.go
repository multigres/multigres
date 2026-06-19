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

package pgregresstest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// setOutcome is the result of running a SET statement: the SQLSTATE and message
// when it errors, or zero values when it succeeds.
type setOutcome struct {
	errored  bool
	sqlState string
	message  string
}

// runSet opens a fresh connection to port and runs stmt, returning its outcome.
func runSet(ctx context.Context, t *testing.T, port int, stmt string) setOutcome {
	t.Helper()
	connStr := shardsetup.GetTestUserDSN("localhost", port, "sslmode=disable", "connect_timeout=5")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, stmt, pgx.QueryExecModeSimpleProtocol)
	if err == nil {
		return setOutcome{}
	}
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "expected *pgconn.PgError, got %T: %v", err, err)
	return setOutcome{errored: true, sqlState: pgErr.Code, message: pgErr.Message}
}

// TestStatementTimeoutErrorParity verifies that `SET statement_timeout` behaves
// identically through the multigateway and against the backing PostgreSQL — the
// same success/failure, SQLSTATE, and (for errors) byte-for-byte message.
//
// statement_timeout is gateway-managed: the multigateway parses and validates it
// itself rather than forwarding the SET to PostgreSQL, so its error text is
// hand-written (handler/statement_timeout.go). This test guards that the
// hand-written text stays exactly aligned with PostgreSQL's, comparing against a
// live PostgreSQL instead of a hardcoded string.
func TestStatementTimeoutErrorParity(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 2*time.Minute)
	directPgPort := setup.GetPrimary(t).Pgctld.PgPort
	gatewayPgPort := setup.MultigatewayPgPort

	// Values exercised on both engines. Mixes valid settings with out-of-range
	// ones (plain ms and unit-suffixed). Avoids the exact half-millisecond
	// rounding boundary and over-INT_MAX values, where PostgreSQL's integer GUC
	// overflow/rounding behavior is genuinely undefined-ish and not worth pinning.
	values := []string{
		"0",     // valid: disables the timeout
		"5000",  // valid: 5s as plain milliseconds
		"30s",   // valid: unit-suffixed
		"250ms", // valid: unit-suffixed
		"-1",    // out of range: negative milliseconds
		"-100",  // out of range: negative milliseconds
		"-5s",   // out of range: negative, unit-suffixed (-> -5000 ms)
	}

	for _, v := range values {
		t.Run(v, func(t *testing.T) {
			stmt := fmt.Sprintf("SET statement_timeout TO '%s'", v)
			direct := runSet(ctx, t, directPgPort, stmt)
			gateway := runSet(ctx, t, gatewayPgPort, stmt)

			t.Logf("direct:  errored=%v sqlstate=%s msg=%q", direct.errored, direct.sqlState, direct.message)
			t.Logf("gateway: errored=%v sqlstate=%s msg=%q", gateway.errored, gateway.sqlState, gateway.message)

			require.Equal(t, direct.errored, gateway.errored,
				"multigateway must error iff PostgreSQL errors")
			require.Equal(t, direct.sqlState, gateway.sqlState,
				"multigateway SQLSTATE must match PostgreSQL")
			require.Equal(t, direct.message, gateway.message,
				"multigateway error message must match PostgreSQL byte-for-byte")
		})
	}
}
