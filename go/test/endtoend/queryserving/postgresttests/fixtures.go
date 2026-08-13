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

package postgresttests

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/multigres/multigres/go/tools/executil"
)

const (
	// authenticatorRole is the login role PostgREST connects as. Upstream's
	// harness uses this exact mixed-case name deliberately, to exercise
	// identifier quoting in the schema-cache queries; keep it verbatim. It is a
	// minimally privileged role (no superuser, no inherit) that SET ROLEs into
	// the per-request impersonated roles granted to it by roles.sql.
	authenticatorRole = "Postgrest_Test_Authenticator"

	// authenticatorPassword is the password PostgREST authenticates with. The
	// cluster/standalone require password auth (scram/md5), unlike upstream's
	// trust-over-socket setup, so the authenticator gets an explicit password.
	authenticatorPassword = "postgrest_authenticator_pw"

	// fixtureSchema is the schema PostgREST exposes (PGRST_DB_SCHEMAS).
	fixtureSchema = "test"
)

// pgConn describes how to reach a PostgreSQL instance for host-side psql. Host
// is either 127.0.0.1 (TCP standalone) or a unix-socket directory (the cluster
// primary, which is socket-only); libpq treats an absolute path as a socket dir.
type pgConn struct {
	Host      string // 127.0.0.1 or a socket directory
	Port      int
	SuperUser string // bootstrap superuser (e.g. "postgres")
	SuperPass string
	Database  string
	SSLMode   string // "disable" for TCP; "" leaves libpq default (socket)
}

// psqlEnv returns the libpq env for connecting to c as the superuser.
func (c pgConn) psqlEnv() []string {
	env := []string{
		"PGHOST=" + c.Host,
		"PGPORT=" + strconv.Itoa(c.Port),
		"PGUSER=" + c.SuperUser,
		"PGPASSWORD=" + c.SuperPass,
		"PGDATABASE=" + c.Database,
		"PGCONNECT_TIMEOUT=10",
	}
	if c.SSLMode != "" {
		env = append(env, "PGSSLMODE="+c.SSLMode)
	}
	return env
}

// loadFixtures creates the authenticator role and loads PostgREST's fixture
// schema into the database at conn, as the bootstrap superuser. srcDir is a
// PostgREST checkout; fixtures live under test/spec/fixtures. This mirrors the
// upstream withPg flow: createuser (non-superuser, non-inherit, login) then
// `psql -v PGUSER=<authenticator> -f load.sql`. roles.sql creates a SUPERUSER
// role and grants all test roles to the authenticator, so the loader must be a
// real superuser.
func loadFixtures(t *testing.T, ctx context.Context, conn pgConn, srcDir string) error {
	t.Helper()

	fixturesDir := filepath.Join(srcDir, "test", "spec", "fixtures")
	loadSQL := filepath.Join(fixturesDir, "load.sql")

	// Force the database default timezone to UTC. Upstream's withPg bakes UTC into
	// the server via `TZ=utc initdb`; our built PostgreSQL inherits the host tz,
	// which makes timestamptz→text (and PostgREST's data-representation casts)
	// render in local time and diverge from the expected fixtures. PostgREST
	// resets the session to the DB default per request, so a database-scoped
	// default (not a session PGOPTIONS) is what takes effect. On the cluster PG
	// this is already UTC, so it is a harmless no-op there.
	if err := runPsql(ctx, conn, []string{"-v", "ON_ERROR_STOP=1", "-c", "ALTER DATABASE postgres SET timezone TO 'UTC';"}); err != nil {
		return fmt.Errorf("set database timezone to UTC: %w", err)
	}

	// Reset the query-planner GUCs to PostgreSQL defaults for this database. The
	// multigres cluster's pgctld tunes them (work_mem=1092kB, random_page_cost=1.1,
	// effective_cache_size=192MB, max_parallel_workers_per_gather=1), which changes
	// EXPLAIN cost estimates and makes PostgREST's PlanSpec (which asserts exact
	// costs) diverge from the default-config direct baseline. Setting the defaults
	// at the database level (PostgREST resets each request's session to it) makes
	// the planner behave identically on both paths, so those become non-divergences.
	for _, guc := range []string{
		"work_mem TO '4MB'",
		"random_page_cost TO 4",
		"effective_cache_size TO '4GB'",
		"max_parallel_workers_per_gather TO 2",
		// max_parallel_workers is GUC_EXPLAIN, so a non-default value (the cluster
		// sets 2) shows up in EXPLAIN (SETTINGS) output and breaks the PlanSpec
		// "outputs the search path when using the settings option" test.
		"max_parallel_workers TO 8",
	} {
		if err := runPsql(ctx, conn, []string{"-v", "ON_ERROR_STOP=1", "-c", "ALTER DATABASE postgres SET " + guc + ";"}); err != nil {
			return fmt.Errorf("reset planner GUC (%s): %w", guc, err)
		}
	}

	// Create the authenticator role first (idempotently), with a password so it
	// can authenticate over TCP/scram. NOINHERIT + no elevated attributes match
	// upstream; SET ROLE relies on the grants in roles.sql.
	createRole := fmt.Sprintf(
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='%s') THEN `+
			`CREATE ROLE "%s" LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD '%s'; `+
			`END IF; END $$;`,
		authenticatorRole, authenticatorRole, authenticatorPassword,
	)
	if err := runPsql(ctx, conn, []string{"-v", "ON_ERROR_STOP=1", "-c", createRole}); err != nil {
		return fmt.Errorf("create authenticator role: %w", err)
	}

	// Load the fixture schema. \ir includes in load.sql resolve relative to the
	// file, so -f with the absolute path finds the sibling SQL files.
	loadArgs := []string{
		"-v", "ON_ERROR_STOP=1",
		"-v", "PGUSER=" + authenticatorRole,
		"-f", loadSQL,
	}
	if err := runPsql(ctx, conn, loadArgs); err != nil {
		return fmt.Errorf("load fixtures from %s: %w", loadSQL, err)
	}

	// Refresh planner statistics on the tables the suite ANALYZEs before the
	// RangeSpec group ("to get accurate results from EXPLAIN" — Main.hs /
	// SpecHelper.analyzeTable). Upstream does this by shelling out to
	// `psql -U postgres` from inside the spec container, but our harness has no
	// in-container superuser psql and no `postgres` role reachable through the
	// proxy, so we run the ANALYZE here as the loader's superuser instead. The
	// container's psql is a no-op shim so the (now redundant) hook still exits 0
	// — see testdata/Dockerfile.spec.
	analyze := fmt.Sprintf(`ANALYZE %[1]s."items"; ANALYZE %[1]s."child_entities";`, fixtureSchema)
	if err := runPsql(ctx, conn, []string{"-v", "ON_ERROR_STOP=1", "-c", analyze}); err != nil {
		return fmt.Errorf("analyze fixture tables: %w", err)
	}

	t.Logf("Loaded PostgREST fixtures into %s:%d (schema %q, authenticator %q)",
		conn.Host, conn.Port, fixtureSchema, authenticatorRole)
	return nil
}

// runPsql runs the built psql (found on PATH) against conn with the given args.
func runPsql(ctx context.Context, conn pgConn, args []string) error {
	cmd := executil.Command(ctx, "psql", args...)
	cmd.AddEnv(conn.psqlEnv()...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w\n%s", err, tail(out.String(), 30))
	}
	return nil
}
