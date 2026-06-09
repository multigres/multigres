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
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// Group 7 of the PgBouncer port — small differential deltas (PG-as-oracle),
// ported from test_no_database.py.
//
// Unlike the rest of the suite, these fit the differential model: run the same
// connection attempt against direct PostgreSQL and the multigateway (via
// GetComparisonTargets). The two cases are connecting to a nonexistent database
// and the auth-vs-database error ordering.
//
// The headline finding is a genuine **divergence**, documented and guarded
// below: direct PostgreSQL validates the database during startup and rejects a
// nonexistent one at connect time (3D000), whereas the multigateway authenticates
// the client and routes queries to the database its backend serves *without
// validating the requested name* — so an unknown database is silently accepted.
// The auth-ordering case, by contrast, matches between the two.

// nonexistentDB is a database name that is not registered in the test topology
// nor created in postgres.
const nonexistentDB = "no_such_db_xyz"

// TestNonexistentDatabaseConnectBehavior documents how the two targets diverge on
// a nonexistent connection database: PostgreSQL rejects it at connect with
// invalid_catalog_name (3D000); the multigateway accepts the connection (auth
// only) and serves queries against its backend database regardless of the
// requested name. The multigateway branch guards that current behavior — if the
// gateway later validates the connection database, this test will flag it for an
// intended update.
func TestNonexistentDatabaseConnectBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)
	targets := setup.GetComparisonTargets(t)

	// Direct PostgreSQL: the catalog is validated during startup, so the
	// connection is rejected before ReadyForQuery with 3D000.
	code, stage, err := probeConnect(t, ctx, targetPort(t, targets, "postgres"), nonexistentDB, shardsetup.TestPostgresPassword)
	require.Error(t, err, "direct postgres must reject a nonexistent database")
	t.Logf("postgres: failed at %s stage with SQLSTATE %q", stage, code)
	assert.Equal(t, "3D000", code, "direct postgres should reject a nonexistent database with invalid_catalog_name")
	assert.Equal(t, "connect", stage, "direct postgres rejects the database at connect time")

	// Multigateway (DIVERGENCE): authentication succeeds and the gateway never
	// validates the requested database, so the connection comes up and queries
	// run against the served database (postgres). This guards the current
	// behavior; it is a known divergence from PostgreSQL, not necessarily the
	// desired end state.
	gwDSN := buildDSN(targetPort(t, targets, "multigateway"), nonexistentDB, shardsetup.TestPostgresPassword)
	gwConn, err := pgx.Connect(ctx, gwDSN)
	require.NoError(t, err,
		"DIVERGENCE: the multigateway does not validate the connection database, so a nonexistent name is accepted")
	defer gwConn.Close(ctx)

	var served string
	require.NoError(t, gwConn.QueryRow(ctx, "SELECT current_database()").Scan(&served))
	t.Logf("multigateway accepted database %q and served %q instead", nonexistentDB, served)
	assert.Equal(t, "postgres", served,
		"the gateway serves its backend database regardless of the requested (nonexistent) name")
}

// TestAuthErrorPrecedesDatabaseError asserts that when both the password is wrong
// AND the database does not exist, both targets report the authentication error
// (28P01) at connect time — authentication is checked before the database is
// resolved, so the bad-database error never surfaces. This case matches between
// PostgreSQL and the multigateway.
func TestAuthErrorPrecedesDatabaseError(t *testing.T) {
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
			code, stage, err := probeConnect(t, ctx, target.Port, nonexistentDB, "definitely_the_wrong_password")
			require.Error(t, err, "a wrong password must fail the connection")
			t.Logf("%s: failed at %s stage with SQLSTATE %q", target.Name, stage, code)
			assert.Equalf(t, "28P01", code,
				"auth failure (28P01) must win over the nonexistent-database error, got %q", code)
			assert.Equalf(t, "connect", stage,
				"the auth failure must be reported at connect time, got %s", stage)
		})
	}
}

// buildDSN builds a libpq DSN for the test user against the given port/database.
func buildDSN(port int, dbname, password string) string {
	return fmt.Sprintf("host=localhost port=%d user=%s password=%s dbname=%s sslmode=disable connect_timeout=5",
		port, shardsetup.DefaultTestUser, password, dbname)
}

// probeConnect connects to a target with the given database and password, then
// runs a trivial query, and returns the SQLSTATE and stage ("connect" or
// "query") of the first error encountered — or ("", "none", nil) on success.
// This normalizes the connect-time-vs-first-query timing difference between
// direct PostgreSQL and the multigateway.
func probeConnect(t *testing.T, ctx context.Context, port int, dbname, password string) (code, stage string, err error) {
	t.Helper()
	conn, cerr := pgx.Connect(ctx, buildDSN(port, dbname, password))
	if cerr != nil {
		return sqlState(cerr), "connect", cerr
	}
	defer conn.Close(ctx)

	var n int
	if qerr := conn.QueryRow(ctx, "SELECT 1").Scan(&n); qerr != nil {
		return sqlState(qerr), "query", qerr
	}
	return "", "none", nil
}

// sqlState extracts the PostgreSQL SQLSTATE from an error, or "" if the error is
// not a PostgreSQL error response.
func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// targetPort returns the port of the named comparison target.
func targetPort(t *testing.T, targets []shardsetup.TestTarget, name string) int {
	t.Helper()
	for _, target := range targets {
		if target.Name == name {
			return target.Port
		}
	}
	t.Fatalf("comparison target %q not found", name)
	return 0
}
