// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestGatewayEval covers the gateway answering constant-only SELECTs itself,
// through the real wire protocol: the simple protocol, the extended protocol
// with text and binary parameters and results (as a client declares them and
// as pgx negotiates them), PostgreSQL's own evaluation errors, and the plan
// type the query log records. The plan type is the end-to-end proof that the
// statement took the local path; the engine primitive has no backend call
// (see engine.GatewayEval and its unit tests).
func TestGatewayEval(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn := connectToMultigateway(ctx, t, setup)
	defer conn.Close()

	rowValues := func(results []*sqltypes.Result) [][]byte {
		var row *sqltypes.Row
		for _, r := range results {
			if len(r.Rows) > 0 {
				require.Nil(t, row, "expected a single row")
				row = r.Rows[0]
			}
		}
		require.NotNil(t, row, "expected a row")
		out := make([][]byte, len(row.Values))
		for i, v := range row.Values {
			out[i] = []byte(v)
		}
		return out
	}
	pgError := func(t *testing.T, err error, code, message string) {
		t.Helper()
		require.Error(t, err)
		assert.Equal(t, code, mterrors.ExtractSQLSTATE(err), "%v", err)
		assert.Contains(t, err.Error(), message)
	}
	gatewayEvalEntries := func(protocol string) int {
		n := 0
		for _, e := range readQueryLogs(t, setup.GatewayLogFile()) {
			if e.PlanType == "GatewayEval" && e.Protocol == protocol {
				n++
			}
		}
		return n
	}
	waitForGatewayEvalEntry := func(t *testing.T, protocol string, before int) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if gatewayEvalEntries(protocol) > before {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("no new %s GatewayEval entry in the query log", protocol)
	}

	t.Run("simple protocol", func(t *testing.T) {
		before := gatewayEvalEntries("simple")
		results, err := conn.Query(ctx, "SELECT 1 + 2 AS three, true, 7 / 2, -5::int8, NULL::int4, abs(-4)")
		require.NoError(t, err)

		var fields *sqltypes.Result
		for _, r := range results {
			if r.Fields != nil {
				fields = r
			}
		}
		require.NotNil(t, fields)
		names := make([]string, len(fields.Fields))
		oids := make([]uint32, len(fields.Fields))
		for i, f := range fields.Fields {
			names[i], oids[i] = f.Name, f.DataTypeOid
		}
		assert.Equal(t, []string{"three", "?column?", "?column?", "?column?", "int4", "abs"}, names)
		assert.Equal(t, []uint32{23, 16, 23, 20, 23, 23}, oids)
		assert.Equal(t, [][]byte{[]byte("3"), []byte("t"), []byte("3"), []byte("-5"), nil, []byte("4")}, rowValues(results))
		for _, r := range results {
			if r.CommandTag != "" {
				assert.Equal(t, "SELECT 1", r.CommandTag)
			}
		}
		waitForGatewayEvalEntry(t, "simple", before)
	})

	t.Run("evaluation errors are PostgreSQL's", func(t *testing.T) {
		_, err := conn.Query(ctx, "SELECT 1 / 0")
		pgError(t, err, mterrors.PgSSDivisionByZero, "division by zero")

		_, err = conn.Query(ctx, "SELECT 2147483647 + 1")
		pgError(t, err, mterrors.PgSSNumericValueOutOfRange, "integer out of range")

		// The session is intact afterwards.
		results, err := conn.Query(ctx, "SELECT 2")
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("2")}, rowValues(results))
	})

	t.Run("extended protocol", func(t *testing.T) {
		before := gatewayEvalEntries("extended")
		const sql = "SELECT $1::int4 + $2 AS sum, $2 > 3"
		require.NoError(t, conn.Parse(ctx, "ge", sql, []uint32{0, 0}))

		desc, err := conn.DescribePrepared(ctx, "ge")
		require.NoError(t, err)
		require.Len(t, desc.Parameters, 2)
		assert.Equal(t, uint32(23), desc.Parameters[0].DataTypeOid)
		assert.Equal(t, uint32(23), desc.Parameters[1].DataTypeOid, "deduced from the operator")
		require.Len(t, desc.Fields, 2)
		assert.Equal(t, "sum", desc.Fields[0].Name)
		assert.Equal(t, uint32(23), desc.Fields[0].DataTypeOid)
		assert.Equal(t, "?column?", desc.Fields[1].Name)
		assert.Equal(t, uint32(16), desc.Fields[1].DataTypeOid)

		var results []*sqltypes.Result
		collect := func(_ context.Context, r *sqltypes.Result) error {
			results = append(results, r)
			return nil
		}

		// Text parameters and results.
		_, err = conn.BindAndExecute(ctx, "", "ge", [][]byte{[]byte("3"), []byte("4")}, []int16{0}, []int16{0}, 0, collect)
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("7"), []byte("t")}, rowValues(results))

		// Binary parameters and results, with the Describe('P') reporting the
		// binary format codes.
		results = nil
		desc, err = conn.BindAndDescribe(ctx, "ge", [][]byte{{0, 0, 0, 3}, {0, 0, 0, 4}}, []int16{1}, []int16{1})
		require.NoError(t, err)
		require.Len(t, desc.Fields, 2)
		assert.Equal(t, int32(1), desc.Fields[0].Format)
		_, err = conn.BindAndExecute(ctx, "", "ge", [][]byte{{0, 0, 0, 3}, {0, 0, 0, 4}}, []int16{1}, []int16{1}, 0, collect)
		require.NoError(t, err)
		assert.Equal(t, [][]byte{{0, 0, 0, 7}, {1}}, rowValues(results))

		// A NULL parameter.
		results = nil
		_, err = conn.BindAndExecute(ctx, "", "ge", [][]byte{[]byte("3"), nil}, []int16{0}, []int16{0}, 0, collect)
		require.NoError(t, err)
		assert.Equal(t, [][]byte{nil, nil}, rowValues(results))

		// Bind-time errors are PostgreSQL's.
		_, err = conn.BindAndExecute(ctx, "", "ge", [][]byte{[]byte("abc"), []byte("4")}, []int16{0}, []int16{0}, 0, collect)
		pgError(t, err, mterrors.PgSSInvalidTextRepresentation, `invalid input syntax for type integer: "abc"`)
		_, err = conn.BindAndExecute(ctx, "", "ge", [][]byte{[]byte("3")}, []int16{0}, []int16{0}, 0, collect)
		pgError(t, err, mterrors.PgSSProtocolViolation, "bind message supplies 1 parameters")

		waitForGatewayEvalEntry(t, "extended", before)
	})

	// pgx prepares, describes and binds int4 in binary by default, so it
	// exercises the negotiated formats a real driver picks.
	t.Run("pgx", func(t *testing.T) {
		cfg, err := pgx.ParseConfig(shardsetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable"))
		require.NoError(t, err)
		pgxConn, err := pgx.ConnectConfig(ctx, cfg)
		require.NoError(t, err)
		defer pgxConn.Close(ctx)

		var sum int32
		var yes bool
		require.NoError(t, pgxConn.QueryRow(ctx, "select $1::int4 + $2, $2 > 3", 3, 4).Scan(&sum, &yes))
		assert.Equal(t, int32(7), sum)
		assert.True(t, yes)

		var big int64
		require.NoError(t, pgxConn.QueryRow(ctx, "select 2147483648 * $1", int64(2)).Scan(&big))
		assert.Equal(t, int64(4294967296), big)

		var pgErr *pgconn.PgError
		err = pgxConn.QueryRow(ctx, "select $1::int4 / $2", 1, 0).Scan(&sum)
		require.True(t, errors.As(err, &pgErr), "%v", err)
		assert.Equal(t, mterrors.PgSSDivisionByZero, pgErr.Code)
	})

	// Inside a transaction block the statement goes to the backend, where the
	// aborted-transaction state lives; the answer is the same.
	t.Run("transaction block routes", func(t *testing.T) {
		_, err := conn.Query(ctx, "BEGIN")
		require.NoError(t, err)
		results, err := conn.Query(ctx, "SELECT 1 + 2")
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("3")}, rowValues(results))
		_, err = conn.Query(ctx, "SELECT 1 / 0")
		pgError(t, err, mterrors.PgSSDivisionByZero, "division by zero")
		_, err = conn.Query(ctx, "SELECT 1")
		pgError(t, err, mterrors.PgSSInFailedTransaction, "current transaction is aborted")
		_, err = conn.Query(ctx, "ROLLBACK")
		require.NoError(t, err)
	})

	// A session-level advisory lock reserves a backend; while it is held,
	// even `SELECT 1` goes there, so the statement after a backend loss ends
	// the session (TestReservationLoss) rather than succeeding locally.
	t.Run("reserved backend routes", func(t *testing.T) {
		_, err := conn.Query(ctx, "SELECT pg_advisory_lock(7143)")
		require.NoError(t, err)
		before := gatewayEvalEntries("simple")
		results, err := conn.Query(ctx, "SELECT 1 + 1")
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("2")}, rowValues(results))
		_, err = conn.Query(ctx, "SELECT pg_advisory_unlock(7143)")
		require.NoError(t, err)
		assert.Equal(t, before, gatewayEvalEntries("simple"), "routed while the lock was held")

		before = gatewayEvalEntries("simple")
		_, err = conn.Query(ctx, "SELECT 1 + 1")
		require.NoError(t, err)
		waitForGatewayEvalEntry(t, "simple", before)
	})

	// A prepared statement outlives transaction boundaries, and each Execute
	// decides afresh: parsed and described locally outside a block, it must
	// still run on the backend inside one (where the gateway's lazy Parse has
	// never sent it), and come back to the gateway afterwards.
	t.Run("prepared statement across a transaction boundary", func(t *testing.T) {
		require.NoError(t, conn.Parse(ctx, "span", "SELECT $1::int4 + 1", nil))
		desc, err := conn.DescribePrepared(ctx, "span")
		require.NoError(t, err)
		require.Len(t, desc.Fields, 1)

		var results []*sqltypes.Result
		collect := func(_ context.Context, r *sqltypes.Result) error {
			results = append(results, r)
			return nil
		}
		execute := func(t *testing.T, arg string) [][]byte {
			t.Helper()
			results = nil
			_, err := conn.BindAndExecute(ctx, "", "span", [][]byte{[]byte(arg)}, []int16{0}, []int16{0}, 0, collect)
			require.NoError(t, err)
			return rowValues(results)
		}

		before := gatewayEvalEntries("extended")
		assert.Equal(t, [][]byte{[]byte("2")}, execute(t, "1"))
		waitForGatewayEvalEntry(t, "extended", before)

		_, err = conn.Query(ctx, "BEGIN")
		require.NoError(t, err)
		routedBefore := gatewayEvalEntries("extended")
		assert.Equal(t, [][]byte{[]byte("3")}, execute(t, "2"))
		_, err = conn.Query(ctx, "SELECT 1 / 0")
		pgError(t, err, mterrors.PgSSDivisionByZero, "division by zero")
		_, err = conn.BindAndExecute(ctx, "", "span", [][]byte{[]byte("3")}, []int16{0}, []int16{0}, 0, collect)
		pgError(t, err, mterrors.PgSSInFailedTransaction, "current transaction is aborted")
		_, err = conn.Query(ctx, "ROLLBACK")
		require.NoError(t, err)
		assert.Equal(t, routedBefore, gatewayEvalEntries("extended"), "inside the block the statement was routed")

		before = gatewayEvalEntries("extended")
		assert.Equal(t, [][]byte{[]byte("5")}, execute(t, "4"))
		waitForGatewayEvalEntry(t, "extended", before)
	})

	// What the engine declines still works through the backend.
	// PostgreSQL checks a Bind when it arrives: parameter count, formats, and
	// every value's decoding. The gateway does the same for a statement it
	// answers itself, so a client that sends Bind and Describe without Execute
	// gets the error, not BindComplete and a RowDescription, and a rejected
	// Bind leaves no portal. Text values are verified as UTF-8 first, as
	// pg_client_to_server does, so a NUL cannot silently end a value.
	t.Run("invalid bind fails at Bind", func(t *testing.T) {
		require.NoError(t, conn.Parse(ctx, "gb", "SELECT $1::int4", nil))
		_, err := conn.BindAndDescribe(ctx, "gb", [][]byte{[]byte("abc")}, nil, nil)
		pgError(t, err, mterrors.PgSSInvalidTextRepresentation, `invalid input syntax for type integer: "abc"`)
		_, err = conn.BindAndDescribe(ctx, "gb", [][]byte{[]byte("1\x00junk")}, nil, nil)
		pgError(t, err, mterrors.PgSSCharacterNotInRepertoire, `invalid byte sequence for encoding "UTF8": 0x00`)
		_, err = conn.BindAndDescribe(ctx, "gb", [][]byte{{0xc3, 0x28}}, nil, nil)
		pgError(t, err, mterrors.PgSSCharacterNotInRepertoire, `invalid byte sequence for encoding "UTF8": 0xc3 0x28`)
		_, err = conn.BindAndDescribe(ctx, "gb", nil, nil, nil)
		pgError(t, err, mterrors.PgSSProtocolViolation, `bind message supplies 0 parameters, but prepared statement "gb" requires 1`)

		desc, err := conn.BindAndDescribe(ctx, "gb", [][]byte{[]byte("7")}, nil, nil)
		require.NoError(t, err, "the session is usable after the rejected Binds")
		require.Len(t, desc.Fields, 1)
		var results []*sqltypes.Result
		_, err = conn.Execute(ctx, "gb", 0, func(_ context.Context, r *sqltypes.Result) error {
			results = append(results, r)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("7")}, rowValues(results))
		require.NoError(t, conn.CloseStatement(ctx, "gb"))
	})

	// The engine reads and writes UTF-8 only, so a session that asked for
	// another client encoding keeps every statement on the backend.
	t.Run("client_encoding routes", func(t *testing.T) {
		_, err := conn.Query(ctx, "SET client_encoding = 'LATIN1'")
		require.NoError(t, err)
		before := gatewayEvalEntries("simple")
		results, err := conn.Query(ctx, "SELECT 1 + 1")
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("2")}, rowValues(results))
		_, err = conn.Query(ctx, "RESET client_encoding")
		require.NoError(t, err)
		assert.Equal(t, before, gatewayEvalEntries("simple"), "routed while client_encoding was LATIN1")

		before = gatewayEvalEntries("simple")
		_, err = conn.Query(ctx, "SELECT 1 + 1")
		require.NoError(t, err)
		waitForGatewayEvalEntry(t, "simple", before)
	})

	t.Run("declined statements route", func(t *testing.T) {
		for _, sql := range []string{"SELECT 'x'", "SELECT now() > '2000-01-01'", "SELECT ARRAY[1, 2]", "SELECT GREATEST(1, 2)"} {
			_, err := conn.Query(ctx, sql)
			assert.NoError(t, err, sql)
		}
	})
}
