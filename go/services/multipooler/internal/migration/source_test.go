// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
)

func TestConnectGrantSQL(t *testing.T) {
	// An empty role list with includePublic false yields no statements — truly
	// nothing to do.
	require.Nil(t, connectGrantSQL(false, false, "appdb", nil))
	require.Nil(t, connectGrantSQL(true, false, "appdb", []string{}))

	// An empty role list with includePublic true still yields the PUBLIC
	// statement: PUBLIC normally holds CONNECT regardless of whether any named
	// roles are configured, so it must not be skipped just because the role
	// list happens to be empty.
	require.Equal(t, []string{"REVOKE CONNECT ON DATABASE appdb FROM PUBLIC"}, connectGrantSQL(false, true, "appdb", nil))

	// REVOKE fences PUBLIC first (so a role relying only on PUBLIC's grant is still
	// cut), then each named role; identifiers are quoted per part.
	revoke := connectGrantSQL(false, true, "appdb", []string{"app", "reporting"})
	require.Equal(t, []string{
		"REVOKE CONNECT ON DATABASE appdb FROM PUBLIC",
		"REVOKE CONNECT ON DATABASE appdb FROM app",
		"REVOKE CONNECT ON DATABASE appdb FROM reporting",
	}, revoke)

	// GRANT restores PUBLIC and each named role symmetrically when includePublic.
	grant := connectGrantSQL(true, true, "appdb", []string{"app"})
	require.Equal(t, []string{
		"GRANT CONNECT ON DATABASE appdb TO PUBLIC",
		"GRANT CONNECT ON DATABASE appdb TO app",
	}, grant)

	// GRANT omits PUBLIC when it didn't have CONNECT beforehand (includePublic
	// false) — only the named roles are restored.
	grantNoPublic := connectGrantSQL(true, false, "appdb", []string{"app"})
	require.Equal(t, []string{
		"GRANT CONNECT ON DATABASE appdb TO app",
	}, grantNoPublic)

	// Mixed-case / reserved database and role names are quoted.
	q := connectGrantSQL(false, true, "AppDB", []string{"User"})
	require.Equal(t, []string{
		`REVOKE CONNECT ON DATABASE "AppDB" FROM PUBLIC`,
		`REVOKE CONNECT ON DATABASE "AppDB" FROM "User"`,
	}, q)
}

func TestTerminateClientBackendsSQL(t *testing.T) {
	// The hard-quiesce write-cut must target only ordinary client backends, spare
	// the caller's own backend, and exclude the migrator's connections (parameter
	// $1) -- gated on BOTH application_name and usename = current_user, since
	// application_name alone is client-spoofable (an ordinary application could
	// otherwise `SET application_name = 'multigres_migrator'` and dodge
	// termination). It must never touch walsenders/replication.
	require.Contains(t, terminateClientBackendsSQL, "pg_terminate_backend(pid)")
	require.Contains(t, terminateClientBackendsSQL, "backend_type = 'client backend'")
	require.Contains(t, terminateClientBackendsSQL, "pid <> pg_backend_pid()")
	require.Contains(t, terminateClientBackendsSQL, "NOT (application_name = $1 AND usename = current_user)")
	require.Contains(t, terminateClientBackendsSQL, "datname = current_database()")
	require.NotContains(t, strings.ToLower(terminateClientBackendsSQL), "walsender")
}

// TestDumpSchemaArgs_QuotesTableNamesAgainstPatternExpansion is the regression
// test for the finding this guards against: pg_dump's --table interprets its
// argument as a pattern (like psql's \d), not a literal name. An unquoted
// table name containing a pattern metacharacter could therefore match beyond
// the exact table this migration resolved, pulling unintended schema into a
// dump that ApplySchema then runs on the target as admin.
func TestDumpSchemaArgs_QuotesTableNamesAgainstPatternExpansion(t *testing.T) {
	args := dumpSchemaArgs([]string{"public.orders", "public.ord*rs"})
	require.Contains(t, args, "--table=public.orders", "an ordinary identifier needs no quoting")
	require.Contains(t, args, `--table=public."ord*rs"`,
		"the metacharacter must be inside a quoted, literal identifier, never an unquoted pattern")
}

// TestStripConnInfoPassword is the regression test for the finding this
// guards against: pg_dump's connection argument is visible in this process's
// argv (e.g. /proc/<pid>/cmdline, `ps`) to any co-resident process, so the
// password must never travel there — it is stripped and returned separately
// for the caller to supply via PGPASSWORD instead.
func TestStripConnInfoPassword(t *testing.T) {
	t.Run("password is stripped and returned separately", func(t *testing.T) {
		dsn, password := stripConnInfoPassword("host=src user=app password=supersecret dbname=appdb")
		require.NotContains(t, dsn, "supersecret", "the sanitized conninfo must never contain the password")
		require.Equal(t, "supersecret", password)
		// The sanitized conninfo must still be a valid, complete conninfo for
		// pg_dump to actually connect with (host/user/dbname preserved).
		require.Contains(t, dsn, "host")
		require.Contains(t, dsn, "app")
		require.Contains(t, dsn, "appdb")
	})

	t.Run("no password: dsn returned unchanged", func(t *testing.T) {
		dsn, password := stripConnInfoPassword("host=src user=app dbname=appdb")
		require.Equal(t, "host=src user=app dbname=appdb", dsn)
		require.Empty(t, password)
	})

	t.Run("quoted password with special characters round-trips", func(t *testing.T) {
		dsn, password := stripConnInfoPassword(`host=src password='a b\'c'`)
		require.NotContains(t, dsn, "a b")
		require.Equal(t, `a b'c`, password)
	})

	// Regression test for the finding this guards against: a pgx/libpq URI DSN
	// (postgres://user:password@host/db) is valid input (pgx.Connect accepts it
	// same as keyword=value), but ParseConnInfo understands only keyword=value
	// and returns an empty map for a URI — silently leaving its password
	// unstripped and reaching pg_dump's argv verbatim.
	t.Run("postgres:// URI password is stripped and returned separately", func(t *testing.T) {
		dsn, password := stripConnInfoPassword("postgres://app:supersecret@src:5432/appdb")
		require.NotContains(t, dsn, "supersecret", "the sanitized DSN must never contain the password")
		require.Equal(t, "supersecret", password)
		require.Contains(t, dsn, "app")
		require.Contains(t, dsn, "src")
		require.Contains(t, dsn, "appdb")
	})

	t.Run("postgresql:// URI password is stripped and returned separately", func(t *testing.T) {
		dsn, password := stripConnInfoPassword("postgresql://app:supersecret@src:5432/appdb")
		require.NotContains(t, dsn, "supersecret")
		require.Equal(t, "supersecret", password)
	})

	t.Run("URI with no password: dsn returned unchanged", func(t *testing.T) {
		dsn, password := stripConnInfoPassword("postgres://app@src:5432/appdb")
		require.Equal(t, "postgres://app@src:5432/appdb", dsn)
		require.Empty(t, password)
	})

	// Regression test for the finding this guards against: libpq's own
	// connection-URI format also accepts "password" as a query parameter, not
	// just embedded in userinfo — stripURIPassword only checked userinfo,
	// leaving a query-parameter password completely unstripped in what the
	// caller treats as the sanitized DSN, reaching pg_dump's argv verbatim.
	t.Run("URI password given as a query parameter is stripped and returned separately", func(t *testing.T) {
		dsn, password := stripConnInfoPassword("postgres://app@src:5432/appdb?password=supersecret&sslmode=require")
		require.NotContains(t, dsn, "supersecret", "the sanitized DSN must never contain the password")
		require.Equal(t, "supersecret", password)
		require.Contains(t, dsn, "sslmode=require", "other query parameters must survive")
	})

	t.Run("URI with no userinfo at all but a query-parameter password is still caught", func(t *testing.T) {
		dsn, password := stripConnInfoPassword("postgres://src:5432/appdb?user=app&password=supersecret")
		require.NotContains(t, dsn, "supersecret")
		require.Equal(t, "supersecret", password)
	})

	t.Run("URI password with special characters round-trips", func(t *testing.T) {
		dsn, password := stripConnInfoPassword("postgres://app:a%20b%2Fc@src:5432/appdb")
		require.NotContains(t, dsn, "a%20b%2Fc")
		require.Equal(t, "a b/c", password)
	})
}

// TestNewSource_RejectsUnixSocketHost is the regression test for the HIGH
// finding this guards against: the gateway's CREATE CONNECTION DDL has no
// authorization check restricting who can set a connection's host, so any
// client reachable through the gateway could otherwise point a migration's
// source DSN at the co-located target Postgres's own local socket —
// typically trust-authenticated for local connections — and use ordinary
// migration source-side operations against the target's own database
// instead of a genuine external source, bypassing both network isolation and
// the target's real authentication. The check must catch a Unix-socket host
// given in either DSN form the registry and --source-dsn both accept, not
// just keyword=value.
func TestNewSource_RejectsUnixSocketHost(t *testing.T) {
	for _, dsn := range []string{
		"host=/data/pg_sockets user=postgres dbname=postgres",
		"postgresql://postgres@/postgres?host=%2Fdata%2Fpg_sockets",
		// Regression case for the finding this guards against: a Unix-socket
		// path hidden as a *fallback* host (second in a comma-separated list)
		// behind a deliberately unreachable first host must be rejected too —
		// cfg.Host alone only ever holds the first entry.
		"host=unreachable.invalid,/data/pg_sockets user=postgres dbname=postgres",
	} {
		_, err := newSource(context.Background(), dsn)
		require.ErrorContains(t, err, "Unix-domain socket", "dsn: %s", dsn)
	}
}

// TestNewSource_AllowsNetworkHost confirms the Unix-socket check does not
// also reject an ordinary network host: it must fail only once newSource
// actually tries (and, in this offline unit test, fails) to dial it, not on
// the host-validation check itself.
func TestNewSource_AllowsNetworkHost(t *testing.T) {
	_, err := newSource(context.Background(), "host=src.example.invalid user=postgres dbname=postgres")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "Unix-domain socket")
}

// TestCheckQualifiedNamePartsSupported_RejectsDot is the regression test for
// the finding this guards against: the canonical "schema.table" name joins
// both parts with a bare dot, and quoteQualifiedName later splits on that
// same single dot to rebuild DDL. A literal dot inside either part (valid in
// a quoted PostgreSQL identifier) would make that split ambiguous — e.g.
// schema "my.schema" with table "orders" joins to "my.schema.orders" and
// splits back as schema "my", table "schema.orders", silently misidentifying
// the relation. Rejecting it here, before the ambiguous name is ever built,
// is what prevents DropTables's DROP TABLE ... CASCADE from later targeting
// the wrong table.
func TestCheckQualifiedNamePartsSupported_RejectsDot(t *testing.T) {
	require.NoError(t, checkQualifiedNamePartsSupported("public", "orders"))

	err := checkQualifiedNamePartsSupported("my.schema", "orders")
	require.Error(t, err)
	require.Equal(t, "0A000", mterrors.ExtractSQLSTATE(err), "must be the typed feature_not_supported error")

	err = checkQualifiedNamePartsSupported("public", "my.table")
	require.Error(t, err)
}

// TestCheckQualifiedNamePartsSupported_RejectsNewline is the regression test
// for the CRITICAL finding this guards against: a source table owner can name
// their table (or schema) with an embedded newline, which pg_dump then emits
// verbatim, splitting that table's own "ALTER TABLE ... ENABLE ALWAYS|REPLICA
// TRIGGER" statement across two lines so DumpSchema's line-based
// isUnsafeTriggerFireModeStatement filter never sees the ALTER TABLE prefix
// and the ENABLE ALWAYS/REPLICA TRIGGER keyword on the same line — letting an
// ENABLE ALWAYS trigger calling a SECURITY DEFINER function survive into the
// dump and fire with the target admin's privileges once replication starts
// applying rows. Rejecting the newline here, before the table is ever
// resolved for migration, closes the bypass before any adversarially-named
// source table can reach DumpSchema at all.
func TestCheckQualifiedNamePartsSupported_RejectsNewline(t *testing.T) {
	err := checkQualifiedNamePartsSupported("public", "evil\ntable")
	require.Error(t, err)
	require.Equal(t, "0A000", mterrors.ExtractSQLSTATE(err), "must be the typed feature_not_supported error")

	err = checkQualifiedNamePartsSupported("evil\nschema", "orders")
	require.Error(t, err)

	err = checkQualifiedNamePartsSupported("public", "evil\rtable")
	require.Error(t, err, "a bare carriage return must also be rejected")
}
