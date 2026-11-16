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

	"github.com/multigres/multigres/go/common/backingconn"
	"github.com/multigres/multigres/go/common/mterrors"
)

func TestTerminateClientBackendsSQL(t *testing.T) {
	// The write-cut must target only ordinary client backends, spare
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
		"host=/data/pg_sockets user=postgres password=p dbname=postgres",
		"postgresql://postgres:p@/postgres?host=%2Fdata%2Fpg_sockets",
		// Regression case for the finding this guards against: a Unix-socket
		// path hidden as a *fallback* host (second in a comma-separated list)
		// behind a deliberately unreachable first host must be rejected too —
		// cfg.Host alone only ever holds the first entry.
		"host=unreachable.invalid,/data/pg_sockets user=postgres password=p dbname=postgres",
	} {
		// A connection that is validated names a network host, so the connection is made
		// without Validate to reach the check of newSource, which does not rely on it.
		_, err := backingconn.FromDSN(dsn)
		require.ErrorContains(t, err, "a network host is required", "dsn: %s", dsn)
		conn, err := backingconn.ParseDSN(dsn)
		require.NoError(t, err, "dsn: %s", dsn)
		conn.Complete()
		_, err = newSource(context.Background(), conn)
		require.ErrorContains(t, err, "Unix-domain socket", "dsn: %s", dsn)
	}
}

// TestNewSource_AllowsNetworkHost confirms the Unix-socket check does not
// also reject an ordinary network host: it must fail only once newSource
// actually tries (and, in this offline unit test, fails) to dial it, not on
// the host-validation check itself.
func TestNewSource_AllowsNetworkHost(t *testing.T) {
	conn, err := backingconn.FromDSN("host=src.example.invalid user=postgres password=p dbname=postgres")
	require.NoError(t, err)
	_, err = newSource(context.Background(), conn)
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
// for the finding this guards against: a source table owner can name their
// table (or schema) with an embedded newline, which pg_dump then emits
// verbatim, splitting a dumped DDL statement across lines in a way
// DumpSchema's own line-based processing (stripping psql meta-commands) could
// mishandle. Rejecting the newline here, before the table is ever resolved
// for migration, closes the bypass before any adversarially-named source
// table can reach DumpSchema at all.
func TestCheckQualifiedNamePartsSupported_RejectsNewline(t *testing.T) {
	err := checkQualifiedNamePartsSupported("public", "evil\ntable")
	require.Error(t, err)
	require.Equal(t, "0A000", mterrors.ExtractSQLSTATE(err), "must be the typed feature_not_supported error")

	err = checkQualifiedNamePartsSupported("evil\nschema", "orders")
	require.Error(t, err)

	err = checkQualifiedNamePartsSupported("public", "evil\rtable")
	require.Error(t, err, "a bare carriage return must also be rejected")
}
