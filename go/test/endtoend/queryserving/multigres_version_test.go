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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultigateway_MultigresVersion verifies both version surfaces:
//
//   - SHOW multigres.server_version   → short release version (like server_version),
//     answered locally by the gateway.
//   - SELECT multigres.version() → full build string (like version()), folded to
//     a literal before it reaches PostgreSQL, so it works in any expression
//     position and in both the simple and extended query protocols.
func TestMultigateway_MultigresVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multigres.server_version test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping multigres.server_version test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "failed to open database connection")
	defer db.Close()

	ctx := utils.WithTimeout(t, 30*time.Second)
	require.NoError(t, db.PingContext(ctx), "failed to ping database - multigateway may not be ready")

	// simpleQuery runs sql via the simple query protocol; preparedQuery runs it
	// via the extended protocol (Parse/Bind/Execute).
	simpleQuery := func(t *testing.T, sql string) string {
		t.Helper()
		var out string
		require.NoError(t, db.QueryRowContext(ctx, sql).Scan(&out), "simple query %q failed", sql)
		return out
	}
	preparedQuery := func(t *testing.T, sql string) string {
		t.Helper()
		stmt, err := db.PrepareContext(ctx, sql)
		require.NoError(t, err, "failed to prepare %q", sql)
		defer stmt.Close()
		var out string
		require.NoError(t, stmt.QueryRowContext(ctx).Scan(&out), "prepared query %q failed", sql)
		return out
	}
	bothProtocols := map[string]func(*testing.T, string) string{"simple": simpleQuery, "extended": preparedQuery}

	t.Run("SHOW multigres.server_version returns the short version", func(t *testing.T) {
		for _, run := range bothProtocols {
			version := run(t, "SHOW multigres.server_version")
			assert.NotEmpty(t, version, "version should not be empty")
			assert.NotContains(t, version, "built with", "SHOW should return the short version, got %q", version)
		}
	})

	t.Run("SELECT multigres.version() returns the full build string", func(t *testing.T) {
		for _, run := range bothProtocols {
			version := run(t, "SELECT multigres.version()")
			assert.Contains(t, version, "Multigres", "function should return the full build string, got %q", version)
		}
	})

	// The function is folded to a literal before reaching PostgreSQL, so it works
	// inside a larger expression that PostgreSQL then evaluates — proving it is
	// not limited to a standalone target.
	t.Run("multigres.version() works inside an expression", func(t *testing.T) {
		for _, run := range bothProtocols {
			out := run(t, "SELECT length(multigres.version()) > 0")
			assert.Equal(t, "true", out, "expected the folded literal to be evaluated by PostgreSQL")
		}
	})

	// A bare version() must still route to PostgreSQL and report the backend
	// version — the gateway function is namespaced so it does not shadow this.
	t.Run("bare version() still reports PostgreSQL", func(t *testing.T) {
		version := simpleQuery(t, "SELECT version()")
		assert.True(t, strings.Contains(version, "PostgreSQL"), "bare version() should report PostgreSQL, got %q", version)
	})
}

// versionShapes are the positions version() can take, each a format string
// with %[1]s standing for the function call. Several of them PostgreSQL names
// specially when the function is a FROM item: the table alias names the column,
// a column alias overrides it, ROWS FROM takes each column from its function.
var versionShapes = []string{
	"SELECT %[1]s",
	"SELECT pg_typeof(%[1]s)",
	"SELECT %[1]s || 'x'",
	"SELECT * FROM %[1]s",
	"SELECT * FROM %[1]s AS v",
	"SELECT * FROM %[1]s AS v(x)",
	"SELECT v FROM %[1]s v",
	"SELECT version.version FROM %[1]s",
	"SELECT pg_typeof(version) FROM %[1]s",
	"SELECT * FROM %[1]s WITH ORDINALITY",
	"SELECT * FROM %[1]s WITH ORDINALITY AS t",
	"SELECT * FROM ROWS FROM (%[1]s, generate_series(1, 2))",
	"SELECT * FROM ROWS FROM (generate_series(1, 2), %[1]s) AS r",
	"SELECT * FROM ROWS FROM (%[1]s) AS r",
	"SELECT * FROM generate_series(1, 2) g, LATERAL %[1]s",
	"SELECT * FROM %[1]s JOIN pg_class ON false",
	"SELECT * FROM (SELECT * FROM %[1]s) s",
	"WITH c AS (SELECT * FROM %[1]s) SELECT * FROM c",
	"SELECT * FROM %[1]s UNION ALL SELECT 'x'",
	"SELECT %[1]s AS a, v FROM %[1]s v WHERE v = %[1]s",
	// PostgreSQL rejects a column definition list for a non-record function.
	"SELECT * FROM %[1]s AS v(x text)",
}

// versionResult is what a client sees of one query: column names and types,
// rows (with the version string itself masked, since PostgreSQL's and
// multigres's differ), or the error.
type versionResult struct {
	Cols  []string
	Types []string
	Rows  []string
	Err   string
}

// maskVersion renders one row, replacing either version string with a marker.
func maskVersion(vals []any) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		s := fmt.Sprint(v)
		if b, ok := v.([]byte); ok {
			s = string(b)
		}
		if strings.HasPrefix(s, "PostgreSQL ") || strings.HasPrefix(s, "Multigres ") {
			s = "<version>"
		}
		parts[i] = s
	}
	return strings.Join(parts, "|")
}

func versionErr(err error) versionResult {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return versionResult{Err: pgErr.Code + ": " + pgErr.Message}
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return versionResult{Err: string(pqErr.Code) + ": " + pqErr.Message}
	}
	return versionResult{Err: err.Error()}
}

func pgxVersionQuery(ctx context.Context, conn *pgx.Conn, mode pgx.QueryExecMode, query string) versionResult {
	rows, err := conn.Query(ctx, query, mode)
	if err != nil {
		return versionErr(err)
	}
	defer rows.Close()
	var r versionResult
	for _, fd := range rows.FieldDescriptions() {
		r.Cols = append(r.Cols, fd.Name)
		r.Types = append(r.Types, strconv.FormatUint(uint64(fd.DataTypeOID), 10))
	}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return versionErr(err)
		}
		r.Rows = append(r.Rows, maskVersion(vals))
	}
	if err := rows.Err(); err != nil {
		return versionErr(err)
	}
	return r
}

func pqVersionQuery(ctx context.Context, db *sql.DB, prepared bool, query string) versionResult {
	var rows *sql.Rows
	var err error
	if prepared {
		stmt, perr := db.PrepareContext(ctx, query)
		if perr != nil {
			return versionErr(perr)
		}
		defer stmt.Close()
		rows, err = stmt.QueryContext(ctx)
	} else {
		rows, err = db.QueryContext(ctx, query)
	}
	if err != nil {
		return versionErr(err)
	}
	defer rows.Close()
	var r versionResult
	cts, err := rows.ColumnTypes()
	if err != nil {
		return versionErr(err)
	}
	for _, ct := range cts {
		r.Cols = append(r.Cols, ct.Name())
		r.Types = append(r.Types, ct.DatabaseTypeName())
	}
	for rows.Next() {
		vals := make([]any, len(cts))
		ptrs := make([]any, len(cts))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return versionErr(err)
		}
		r.Rows = append(r.Rows, maskVersion(vals))
	}
	if err := rows.Err(); err != nil {
		return versionErr(err)
	}
	return r
}

// TestMultigateway_MultigresVersionMatchesPostgres checks that
// multigres.version() through the gateway looks to clients exactly like
// PostgreSQL's own version() run directly on the backend — same column names,
// column types, rows and errors — in every shape of versionShapes, over the
// simple and extended protocols of pgx and lib/pq.
func TestMultigateway_MultigresVersionMatchesPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multigres.version() comparison in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping multigres.version() comparison")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	type client func(t *testing.T, port int) func(query string) versionResult
	pgxClient := func(mode pgx.QueryExecMode) client {
		return func(t *testing.T, port int) func(string) versionResult {
			conn, err := pgx.Connect(ctx, shardsetup.GetTestUserDSN("localhost", port, "sslmode=disable", "connect_timeout=5"))
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close(context.Background()) })
			return func(q string) versionResult { return pgxVersionQuery(ctx, conn, mode, q) }
		}
	}
	pqClient := func(prepared bool) client {
		return func(t *testing.T, port int) func(string) versionResult {
			db, err := sql.Open("postgres", shardsetup.GetTestUserDSN("localhost", port, "sslmode=disable", "connect_timeout=5"))
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			return func(q string) versionResult { return pqVersionQuery(ctx, db, prepared, q) }
		}
	}
	clients := []struct {
		name string
		open client
	}{
		{"pgx/simple", pgxClient(pgx.QueryExecModeSimpleProtocol)},
		{"pgx/describe_exec", pgxClient(pgx.QueryExecModeDescribeExec)},
		{"pgx/cache_statement", pgxClient(pgx.QueryExecModeCacheStatement)},
		{"pq/simple", pqClient(false)},
		{"pq/prepared", pqClient(true)},
	}

	var pgPort, gwPort int
	for _, target := range setup.ComparisonTargets(t) {
		switch target.Name {
		case "postgres":
			pgPort = target.Port
		case "multigateway":
			gwPort = target.Port
		}
	}
	require.NotZero(t, pgPort, "no raw PostgreSQL comparison target")
	require.NotZero(t, gwPort, "no multigateway comparison target")

	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			onPostgres := c.open(t, pgPort)
			onGateway := c.open(t, gwPort)
			for _, shape := range versionShapes {
				want := onPostgres(fmt.Sprintf(shape, "version()"))
				got := onGateway(fmt.Sprintf(shape, "multigres.version()"))
				assert.Equal(t, want, got, "shape %q", shape)
				if !strings.Contains(shape, "x text") {
					assert.Empty(t, want.Err, "shape %q should succeed on PostgreSQL", shape)
				}
			}
		})
	}
}

// TestMultigateway_MultigresVersionStatements covers multigres.version() as a
// FROM item inside statements other than a plain SELECT. The gateway folds the
// version into the statement text, so a view or table built from it keeps the
// version of the gateway that created it.
func TestMultigateway_MultigresVersionStatements(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multigres.version() statements test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping multigres.version() statements test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	conn, err := pgx.Connect(ctx, shardsetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	queryVersion := func(t *testing.T, query string) string {
		t.Helper()
		var v string
		require.NoError(t, conn.QueryRow(ctx, query).Scan(&v), "query %q", query)
		require.True(t, strings.HasPrefix(v, "Multigres "), "query %q returned %q", query, v)
		return v
	}

	t.Run("Describe reports one text column", func(t *testing.T) {
		sd, err := conn.Prepare(ctx, "mv_describe", "SELECT * FROM multigres.version()")
		require.NoError(t, err)
		defer func() { _ = conn.Deallocate(ctx, "mv_describe") }()
		require.Len(t, sd.Fields, 1)
		assert.Equal(t, "version", sd.Fields[0].Name)
		assert.Equal(t, uint32(pgtype.TextOID), sd.Fields[0].DataTypeOID)
	})

	t.Run("CREATE TABLE AS and INSERT SELECT", func(t *testing.T) {
		_, err := conn.Exec(ctx, "CREATE TABLE mv_ctas AS SELECT * FROM multigres.version()")
		require.NoError(t, err)
		defer func() { _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS mv_ctas") }()
		_, err = conn.Exec(ctx, "INSERT INTO mv_ctas SELECT * FROM multigres.version()")
		require.NoError(t, err)
		var n int
		require.NoError(t, conn.QueryRow(ctx, "SELECT count(DISTINCT version) + count(*) FROM mv_ctas").Scan(&n))
		assert.Equal(t, 3, n, "expected two rows of one version")
		queryVersion(t, "SELECT version FROM mv_ctas LIMIT 1")
	})

	t.Run("CREATE VIEW", func(t *testing.T) {
		_, err := conn.Exec(ctx, "CREATE VIEW mv_view AS SELECT * FROM multigres.version()")
		require.NoError(t, err)
		defer func() { _, _ = conn.Exec(context.Background(), "DROP VIEW IF EXISTS mv_view") }()
		queryVersion(t, "SELECT version FROM mv_view")
	})

	t.Run("COPY TO STDOUT", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := conn.PgConn().CopyTo(ctx, &buf, "COPY (SELECT * FROM multigres.version()) TO STDOUT")
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(buf.String(), "Multigres "), "COPY returned %q", buf.String())
	})

	t.Run("SQL PREPARE and EXECUTE", func(t *testing.T) {
		_, err := conn.Exec(ctx, "PREPARE mv_p AS SELECT * FROM multigres.version()", pgx.QueryExecModeSimpleProtocol)
		require.NoError(t, err)
		defer func() { _, _ = conn.Exec(context.Background(), "DEALLOCATE mv_p", pgx.QueryExecModeSimpleProtocol) }()
		var v string
		require.NoError(t, conn.QueryRow(ctx, "EXECUTE mv_p", pgx.QueryExecModeSimpleProtocol).Scan(&v))
		assert.True(t, strings.HasPrefix(v, "Multigres "), "EXECUTE returned %q", v)
	})

	// CALL names a procedure; the gateway leaves it for PostgreSQL to reject
	// rather than folding it, and the connection stays usable.
	t.Run("CALL is rejected by PostgreSQL", func(t *testing.T) {
		_, err := conn.Exec(ctx, "CALL multigres.version()")
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "expected a PostgreSQL error, got %v", err)
		queryVersion(t, "SELECT multigres.version()")
	})
}
