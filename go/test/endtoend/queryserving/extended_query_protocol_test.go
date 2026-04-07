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
	"github.com/multigres/multigres/go/common/sqltypes"
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
