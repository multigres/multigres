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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// connectLowLevelToPort creates a low-level pgprotocol client connection to the given port.
func connectLowLevelToPort(t *testing.T, ctx context.Context, port int) *client.Conn {
	t.Helper()
	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "localhost",
		Port:        port,
		User:        shardsetup.DefaultTestUser,
		Password:    shardsetup.TestPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	return conn
}

// TestExtendedQueryProtocol_DescribePortal tests that Describe('P') for a portal
// does NOT send ParameterDescription, matching the PostgreSQL protocol spec.
// Previously, the proxy sent ParameterDescription for portal describes, which
// confused libpq and caused "server sent data without prior row description" errors
// when using \bind \g in psql or PQsendQueryParams in libpq.
//
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestExtendedQueryProtocol_DescribePortal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")

			t.Run("simple query via extended protocol", func(t *testing.T) {
				conn, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer conn.Close(ctx)

				var val int
				err = conn.QueryRow(ctx, "SELECT 1").Scan(&val)
				require.NoError(t, err)
				assert.Equal(t, 1, val)
			})

			t.Run("parameterized query via extended protocol", func(t *testing.T) {
				conn, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer conn.Close(ctx)

				var val string
				err = conn.QueryRow(ctx, "SELECT $1::text", "hello").Scan(&val)
				require.NoError(t, err)
				assert.Equal(t, "hello", val)
			})

			t.Run("multi-column parameterized query", func(t *testing.T) {
				conn, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer conn.Close(ctx)

				var a, b string
				err = conn.QueryRow(ctx, "SELECT $1::text, $2::text", "foo", "bar").Scan(&a, &b)
				require.NoError(t, err)
				assert.Equal(t, "foo", a)
				assert.Equal(t, "bar", b)
			})

			t.Run("describe prepared statement returns ParameterDescription", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				err := conn.Parse(ctx, "desc_test", "SELECT $1::int, $2::text", []uint32{23, 25})
				require.NoError(t, err)

				desc, err := conn.DescribePrepared(ctx, "desc_test")
				require.NoError(t, err)
				require.NotNil(t, desc)
				assert.Len(t, desc.Parameters, 2, "statement describe should return parameter descriptions")
				assert.Len(t, desc.Fields, 2, "statement describe should return field descriptions")

				require.NoError(t, conn.CloseStatement(ctx, "desc_test"))
			})

			t.Run("describe portal returns fields without parameters", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				err := conn.Parse(ctx, "portal_desc", "SELECT $1::int AS val", []uint32{23})
				require.NoError(t, err)

				desc, err := conn.BindAndDescribe(ctx, "portal_desc", [][]byte{[]byte("42")}, []int16{0}, []int16{0})
				require.NoError(t, err)
				require.NotNil(t, desc)

				assert.Len(t, desc.Fields, 1, "portal describe should return field descriptions")
				assert.Equal(t, "val", desc.Fields[0].Name)
				assert.Empty(t, desc.Parameters, "portal describe should NOT return parameter descriptions")

				require.NoError(t, conn.CloseStatement(ctx, "portal_desc"))
			})

			t.Run("bind and execute after describe", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				err := conn.Parse(ctx, "exec_test", "SELECT $1::int + $2::int AS sum", []uint32{23, 23})
				require.NoError(t, err)

				_, err = conn.BindAndDescribe(ctx, "exec_test", [][]byte{[]byte("3"), []byte("4")}, []int16{0}, []int16{0})
				require.NoError(t, err)

				var results []*sqltypes.Result
				_, err = conn.BindAndExecute(ctx, "", "exec_test", [][]byte{[]byte("3"), []byte("4")}, []int16{0}, []int16{0}, 0,
					func(ctx context.Context, result *sqltypes.Result) error {
						results = append(results, result)
						return nil
					})
				require.NoError(t, err)
				require.NotEmpty(t, results)

				var sum string
				for _, r := range results {
					if len(r.Rows) > 0 {
						sum = string(r.Rows[0].Values[0])
						break
					}
				}
				assert.Equal(t, "7", sum)

				require.NoError(t, conn.CloseStatement(ctx, "exec_test"))
			})

			// Two consecutive Describe('P') calls (no Execute between them).
			// Each is its own batch (BindAndDescribe sends B+D+S), so the deferred-describe
			// flush runs at the first Sync. Both must return the correct portal description.
			t.Run("two portal describes back to back", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				require.NoError(t, conn.Parse(ctx, "two_d", "SELECT $1::int AS v", []uint32{23}))

				desc1, err := conn.BindAndDescribe(ctx, "two_d", [][]byte{[]byte("1")}, []int16{0}, []int16{0})
				require.NoError(t, err)
				require.NotNil(t, desc1)
				require.Len(t, desc1.Fields, 1)
				assert.Equal(t, "v", desc1.Fields[0].Name)

				desc2, err := conn.BindAndDescribe(ctx, "two_d", [][]byte{[]byte("2")}, []int16{0}, []int16{0})
				require.NoError(t, err)
				require.NotNil(t, desc2)
				require.Len(t, desc2.Fields, 1)
				assert.Equal(t, "v", desc2.Fields[0].Name)

				require.NoError(t, conn.CloseStatement(ctx, "two_d"))
			})

			// Statement describe immediately after a portal describe. The portal
			// describe must produce its own RowDescription before the statement
			// describe's ParameterDescription+RowDescription, in that order.
			t.Run("statement describe after portal describe", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				require.NoError(t, conn.Parse(ctx, "ps_after_pp", "SELECT $1::int AS v", []uint32{23}))

				portalDesc, err := conn.BindAndDescribe(ctx, "ps_after_pp", [][]byte{[]byte("9")}, []int16{0}, []int16{0})
				require.NoError(t, err)
				require.Len(t, portalDesc.Fields, 1)
				assert.Empty(t, portalDesc.Parameters, "portal describe must not return ParameterDescription")

				stmtDesc, err := conn.DescribePrepared(ctx, "ps_after_pp")
				require.NoError(t, err)
				require.Len(t, stmtDesc.Parameters, 1, "statement describe must return ParameterDescription")
				require.Len(t, stmtDesc.Fields, 1)

				require.NoError(t, conn.CloseStatement(ctx, "ps_after_pp"))
			})

			// Inverse direction: statement describe first, then portal describe.
			// Different code paths because Describe('S') goes straight to the
			// handler while Describe('P') is held pending.
			t.Run("portal describe after statement describe", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				require.NoError(t, conn.Parse(ctx, "pp_after_ps", "SELECT $1::int AS v", []uint32{23}))

				stmtDesc, err := conn.DescribePrepared(ctx, "pp_after_ps")
				require.NoError(t, err)
				require.Len(t, stmtDesc.Parameters, 1)
				require.Len(t, stmtDesc.Fields, 1)

				portalDesc, err := conn.BindAndDescribe(ctx, "pp_after_ps", [][]byte{[]byte("4")}, []int16{0}, []int16{0})
				require.NoError(t, err)
				require.Len(t, portalDesc.Fields, 1)
				assert.Empty(t, portalDesc.Parameters)

				require.NoError(t, conn.CloseStatement(ctx, "pp_after_ps"))
			})

			// Describe('P') followed by Execute targeting a *different* portal name.
			// The held describe must flush before the unrelated Execute runs, and the
			// Execute itself must not carry a fused describe — it returns no Fields.
			t.Run("execute on different portal after describe", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				require.NoError(t, conn.Parse(ctx, "diff_portal", "SELECT $1::int AS v", []uint32{23}))

				descA, err := conn.BindAndDescribe(ctx, "diff_portal", [][]byte{[]byte("11")}, []int16{0}, []int16{0})
				require.NoError(t, err)
				require.Len(t, descA.Fields, 1)

				var got string
				_, err = conn.BindAndExecute(ctx, "portalB", "diff_portal", [][]byte{[]byte("22")}, []int16{0}, []int16{0}, 0,
					func(ctx context.Context, result *sqltypes.Result) error {
						if len(result.Rows) > 0 {
							got = string(result.Rows[0].Values[0])
						}
						return nil
					})
				require.NoError(t, err)
				assert.Equal(t, "22", got)

				require.NoError(t, conn.CloseStatement(ctx, "diff_portal"))
			})

			// DML prepared statement: portal describe yields NoData (Fields is nil)
			// and a follow-up Execute completes successfully with the right CommandTag.
			t.Run("describe DML portal returns NoData", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS describe_dml_t")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE describe_dml_t (id int)")
				require.NoError(t, err)
				t.Cleanup(func() {
					_, _ = conn.Query(ctx, "DROP TABLE IF EXISTS describe_dml_t")
				})

				require.NoError(t, conn.Parse(ctx, "ins", "INSERT INTO describe_dml_t (id) VALUES ($1)", []uint32{23}))

				desc, err := conn.BindAndDescribe(ctx, "ins", [][]byte{[]byte("1")}, []int16{0}, []int16{0})
				require.NoError(t, err)
				require.NotNil(t, desc)
				assert.Empty(t, desc.Fields, "DML portal describe should produce NoData (no fields)")

				var tag string
				_, err = conn.BindAndExecute(ctx, "", "ins", [][]byte{[]byte("2")}, []int16{0}, []int16{0}, 0,
					func(ctx context.Context, result *sqltypes.Result) error {
						if result.CommandTag != "" {
							tag = result.CommandTag
						}
						return nil
					})
				require.NoError(t, err)
				assert.Equal(t, "INSERT 0 1", tag)

				require.NoError(t, conn.CloseStatement(ctx, "ins"))
			})

			// Empty-result-set SELECT through a prepared portal: Fields are present
			// (column descriptors), Rows is empty, and CommandTag reflects 0 rows.
			t.Run("describe portal for empty result set", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				require.NoError(t, conn.Parse(ctx, "empty_sel", "SELECT $1::int AS v WHERE false", []uint32{23}))

				desc, err := conn.BindAndDescribe(ctx, "empty_sel", [][]byte{[]byte("1")}, []int16{0}, []int16{0})
				require.NoError(t, err)
				require.Len(t, desc.Fields, 1, "SELECT-with-no-rows still reports its column descriptors")
				assert.Equal(t, "v", desc.Fields[0].Name)

				var rowCount int
				var tag string
				_, err = conn.BindAndExecute(ctx, "", "empty_sel", [][]byte{[]byte("1")}, []int16{0}, []int16{0}, 0,
					func(ctx context.Context, result *sqltypes.Result) error {
						rowCount += len(result.Rows)
						if result.CommandTag != "" {
							tag = result.CommandTag
						}
						return nil
					})
				require.NoError(t, err)
				assert.Equal(t, 0, rowCount)
				assert.Equal(t, "SELECT 0", tag)

				require.NoError(t, conn.CloseStatement(ctx, "empty_sel"))
			})

			// Repeated Bind+Execute on the same prepared statement (no Describe).
			// Verifies that the absence of describe state on one batch does not
			// bleed into the next, and that the Execute path produces consistent
			// CommandTags + row data across iterations.
			t.Run("repeated bind+execute without describe", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				require.NoError(t, conn.Parse(ctx, "rep", "SELECT $1::int AS v", []uint32{23}))

				for _, in := range []string{"10", "20", "30"} {
					var got string
					_, err := conn.BindAndExecute(ctx, "", "rep", [][]byte{[]byte(in)}, []int16{0}, []int16{0}, 0,
						func(ctx context.Context, result *sqltypes.Result) error {
							if len(result.Rows) > 0 {
								got = string(result.Rows[0].Values[0])
							}
							return nil
						})
					require.NoError(t, err)
					assert.Equal(t, in, got, "iteration with input %q", in)
				}

				require.NoError(t, conn.CloseStatement(ctx, "rep"))
			})

			// BindDescribeAndExecute: the fused single-batch path. Verifies the
			// streaming callback surfaces Fields on the row chunk just like a
			// standalone Describe('P') would have done.
			t.Run("fused bind describe and execute", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				require.NoError(t, conn.Parse(ctx, "fused", "SELECT $1::int AS v, $2::text AS t", []uint32{23, 25}))

				var sawFields []*query.Field
				var rowVal0, rowVal1 string
				_, err := conn.BindDescribeAndExecute(ctx, "", "fused",
					[][]byte{[]byte("7"), []byte("hi")},
					[]int16{0, 0}, []int16{0, 0}, 0,
					func(ctx context.Context, result *sqltypes.Result) error {
						if len(result.Fields) > 0 && sawFields == nil {
							sawFields = result.Fields
						}
						if len(result.Rows) > 0 {
							rowVal0 = string(result.Rows[0].Values[0])
							rowVal1 = string(result.Rows[0].Values[1])
						}
						return nil
					})
				require.NoError(t, err)
				require.Len(t, sawFields, 2, "fused path must surface the portal's RowDescription via Fields")
				assert.Equal(t, "v", sawFields[0].Name)
				assert.Equal(t, "t", sawFields[1].Name)
				assert.Equal(t, "7", rowVal0)
				assert.Equal(t, "hi", rowVal1)

				require.NoError(t, conn.CloseStatement(ctx, "fused"))
			})

			// Two BindDescribeAndExecute calls on the same statement, back to back.
			// Confirms the within-batch fused path resets cleanly across batches.
			t.Run("repeated fused bind describe and execute", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				require.NoError(t, conn.Parse(ctx, "fused_rep", "SELECT $1::int AS v", []uint32{23}))

				for _, in := range []string{"100", "200", "300"} {
					var got string
					var sawFields []*query.Field
					_, err := conn.BindDescribeAndExecute(ctx, "", "fused_rep",
						[][]byte{[]byte(in)}, []int16{0}, []int16{0}, 0,
						func(ctx context.Context, result *sqltypes.Result) error {
							if len(result.Fields) > 0 && sawFields == nil {
								sawFields = result.Fields
							}
							if len(result.Rows) > 0 {
								got = string(result.Rows[0].Values[0])
							}
							return nil
						})
					require.NoError(t, err)
					require.Len(t, sawFields, 1, "iteration with input %q must carry Fields", in)
					assert.Equal(t, in, got)
				}

				require.NoError(t, conn.CloseStatement(ctx, "fused_rep"))
			})

			// Describe(P) then Close(P) — the deferred describe must flush before
			// Close runs, and Close must not produce a stray RowDescription.
			t.Run("close portal after describe", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				require.NoError(t, conn.Parse(ctx, "close_p", "SELECT $1::int AS v", []uint32{23}))

				desc, err := conn.BindAndDescribe(ctx, "close_p", [][]byte{[]byte("5")}, []int16{0}, []int16{0})
				require.NoError(t, err)
				require.Len(t, desc.Fields, 1)

				require.NoError(t, conn.ClosePortal(ctx, "close_p"))

				// Statement is still parsed; a fresh Bind+Execute should still work.
				var got string
				_, err = conn.BindAndExecute(ctx, "", "close_p", [][]byte{[]byte("6")}, []int16{0}, []int16{0}, 0,
					func(ctx context.Context, result *sqltypes.Result) error {
						if len(result.Rows) > 0 {
							got = string(result.Rows[0].Values[0])
						}
						return nil
					})
				require.NoError(t, err)
				assert.Equal(t, "6", got)

				require.NoError(t, conn.CloseStatement(ctx, "close_p"))
			})
		})
	}
}

// TestZeroColumnResults tests that queries returning zero columns are handled
// correctly. PostgreSQL allows queries like "SELECT FROM table" or "SELECT UNION
// SELECT" which return rows with zero columns. The proxy must send a RowDescription
// with 0 fields before the DataRow messages. Previously, the proxy skipped
// RowDescription when the field count was 0, causing libpq to error with
// "server sent data without prior row description".
//
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestZeroColumnResults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	zeroColQueries := []struct {
		name     string
		query    string
		wantRows int
	}{
		{name: "select union select", query: "SELECT UNION SELECT", wantRows: 1},
		{name: "select intersect select", query: "SELECT INTERSECT SELECT", wantRows: 1},
		{name: "select from generate_series", query: "SELECT FROM generate_series(1, 3)", wantRows: 3},
		{name: "empty select", query: "SELECT", wantRows: 1},
	}

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			t.Run("simple protocol zero-column queries", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				for _, tt := range zeroColQueries {
					t.Run(tt.name, func(t *testing.T) {
						results, err := conn.Query(ctx, tt.query)
						require.NoError(t, err, "query %q should not error", tt.query)
						require.NotEmpty(t, results, "should have at least one result")

						found := false
						for _, r := range results {
							if r.CommandTag != "" {
								assert.NotNil(t, r.Fields, "Fields should be non-nil (zero-column result set)")
								assert.Empty(t, r.Fields, "Fields should be empty (zero columns)")
								assert.Equal(t, tt.wantRows, len(r.Rows), "unexpected row count")
								found = true
								break
							}
						}
						assert.True(t, found, "should have found a result with CommandTag")
					})
				}
			})

			t.Run("pgx zero-column query", func(t *testing.T) {
				connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
				conn, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer conn.Close(ctx)

				rows, err := conn.Query(ctx, "SELECT FROM generate_series(1, 3)")
				require.NoError(t, err)
				defer rows.Close()

				count := 0
				for rows.Next() {
					count++
				}
				require.NoError(t, rows.Err())
				assert.Equal(t, 3, count, "should get 3 rows from generate_series")
			})

			t.Run("zero-column table", func(t *testing.T) {
				conn := connectLowLevelToPort(t, ctx, target.Port)
				defer conn.Close()

				tableName := "test_nocols_ext_query"
				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS "+tableName)
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE "+tableName+" ()")
				require.NoError(t, err)
				t.Cleanup(func() {
					cleanupConn := connectLowLevelToPort(t, context.Background(), target.Port)
					defer cleanupConn.Close()
					_, _ = cleanupConn.Query(context.Background(), "DROP TABLE IF EXISTS "+tableName)
				})

				_, err = conn.Query(ctx, "INSERT INTO "+tableName+" DEFAULT VALUES")
				require.NoError(t, err)

				results, err := conn.Query(ctx, "SELECT * FROM "+tableName)
				require.NoError(t, err)
				require.NotEmpty(t, results)

				for _, r := range results {
					if r.CommandTag != "" {
						assert.NotNil(t, r.Fields, "Fields should be non-nil for zero-column table")
						assert.Empty(t, r.Fields, "Fields should be empty for zero-column table")
						assert.Equal(t, 1, len(r.Rows), "should have 1 row")
						break
					}
				}
			})
		})
	}
}

// TestExtendedProtocol_TransactionIsolation is the end-to-end regression for
// extended-protocol BEGIN/COMMIT/ROLLBACK handling in the gateway planner.
// Before the fix, sending BEGIN over the extended protocol fell through to
// the default "execute portal on backend" path, so the BEGIN ran on a pooled
// backend connection instead of opening a reserved gateway transaction. The
// subsequent INSERT routed through the normal autocommit path — committing
// immediately — and a ROLLBACK issued over the same session had no effect.
// This test issues every control statement over the extended protocol (the
// same pattern pgbench -M extended uses) and confirms ROLLBACK actually
// discards the write.
func TestExtendedProtocol_TransactionIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	// Run against both direct postgres and multigateway so the test pins the
	// expected behavior (postgres) and catches the proxy regression.
	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			// Unique table per target — direct postgres and multigateway share
			// the same backend, so both subtests would otherwise race on the
			// same name via concurrent cleanup.
			tableName := "test_ext_txn_iso_" + target.Name

			setupConn := connectLowLevelToPort(t, ctx, target.Port)
			_, err := setupConn.Query(ctx, "DROP TABLE IF EXISTS "+tableName)
			require.NoError(t, err)
			_, err = setupConn.Query(ctx, "CREATE TABLE "+tableName+" (id int)")
			require.NoError(t, err)
			setupConn.Close()

			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				c := connectLowLevelToPort(t, cleanupCtx, target.Port)
				defer c.Close()
				_, _ = c.Query(cleanupCtx, "DROP TABLE IF EXISTS "+tableName)
			})

			conn := connectLowLevelToPort(t, ctx, target.Port)
			defer conn.Close()

			// Each PrepareAndExecute issues Parse + Bind + Execute + Sync,
			// i.e. the full extended-protocol cycle, which is exactly how
			// pgbench -M extended sends BEGIN / DML / COMMIT.
			noopCb := func(context.Context, *sqltypes.Result) error { return nil }

			require.NoError(t, conn.PrepareAndExecute(ctx, "", "BEGIN", nil, noopCb))
			// After BEGIN, the backend must report 'T' on its ReadyForQuery.
			// Without the fix, the gateway's pooled BEGIN closes with the next
			// Sync and we'd see 'I' here instead.
			assert.Equal(t, protocol.TxnStatusInBlock, conn.TxnStatus(),
				"ReadyForQuery after BEGIN should report 'T' (in transaction)")

			require.NoError(t, conn.PrepareAndExecute(ctx,
				"", "INSERT INTO "+tableName+" VALUES (1)", nil, noopCb))
			assert.Equal(t, protocol.TxnStatusInBlock, conn.TxnStatus(),
				"ReadyForQuery after INSERT in transaction should still report 'T'")

			require.NoError(t, conn.PrepareAndExecute(ctx, "", "ROLLBACK", nil, noopCb))
			assert.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus(),
				"ReadyForQuery after ROLLBACK should report 'I' (idle)")

			// The real assertion: ROLLBACK must have discarded the INSERT.
			// Without the fix this reads "1" on multigateway because the
			// INSERT ran in its own autocommit transaction, outside the
			// phantom BEGIN that never updated gateway-side txn state.
			results, err := conn.Query(ctx, "SELECT COUNT(*) FROM "+tableName)
			require.NoError(t, err)
			require.NotEmpty(t, results)
			require.NotEmpty(t, results[0].Rows)
			got := string(results[0].Rows[0].Values[0])
			assert.Equal(t, "0", got,
				"ROLLBACK should discard the INSERT; got %s row(s) — the gateway is not honoring BEGIN over the extended protocol", got)
		})
	}
}
