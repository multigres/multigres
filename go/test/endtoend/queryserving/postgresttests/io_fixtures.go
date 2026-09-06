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
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// ioLoginRoles are the additional LOGIN authenticator roles the io fixtures
// create (test/io/fixtures/db_config.sql). Upstream runs postgres with trust
// auth over a socket, so it never gives them passwords; the multigres cluster
// authenticates each client role by SCRAM against pg_authid, so every role
// PostgREST logs in as needs a password. We give them all the same shared
// password (authenticatorPassword) so one PGPASSWORD serves every per-test
// PGUSER override. The impersonated roles (postgrest_test_*) are NOT here: they
// are reached via SET ROLE inside the authenticator's session, never a login.
var ioLoginRoles = []string{
	"db_config_authenticator",
	"other_authenticator",
	"timeout_authenticator",
	"meta_authenticator",
}

// loadIOFixtures creates the authenticator role and loads PostgREST's test/io
// fixture schema into the database at conn, as the bootstrap superuser. This
// mirrors the upstream nix `withPg -f test/io/fixtures/load.sql` flow: the
// authenticator (PGUSER) is created first (roles.sql GRANTs to and ALTERs it),
// then load.sql is run as the superuser (it CREATEs roles + a database and a
// SECURITY DEFINER function that ALTER ROLEs, so a real superuser is required).
// Finally the extra login roles get the shared password so they can authenticate
// through the gateway over TCP.
func loadIOFixtures(t *testing.T, ctx context.Context, conn pgConn, srcDir string) error {
	t.Helper()

	loadSQL := filepath.Join(srcDir, "test", "io", "fixtures", "load.sql")

	// Reset work_mem to the PostgreSQL default at the database level. The
	// multigres cluster's pgctld tunes work_mem (to 1092kB), but several io tests
	// assert the *default* work_mem a request sees when nothing hoists it (e.g.
	// test_second_hoisted_setting_is_applied expects "4MB"). Upstream's direct PG
	// uses the stock 4MB, so without this the cluster's tuned value leaks into the
	// assertion and shows as a spurious divergence. PostgREST resets each request's
	// session to the DB default, so a database-scoped default is what takes effect.
	// (Mirrors the hspec harness's planner-GUC neutralization in fixtures.go.) On a
	// direct-PG baseline this is a harmless no-op — its default is already 4MB.
	if err := runPsql(ctx, conn, []string{"-v", "ON_ERROR_STOP=1", "-c", "ALTER DATABASE postgres SET work_mem TO '4MB';"}); err != nil {
		return fmt.Errorf("reset work_mem to PostgreSQL default: %w", err)
	}

	// Create the authenticator role first, with a password so it can SCRAM
	// through the gateway. NOINHERIT + no elevated attributes match upstream's
	// createuser; SET ROLE relies on the grants in roles.sql. Idempotent.
	createRole := fmt.Sprintf(
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='%s') THEN `+
			`CREATE ROLE "%s" LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD '%s'; `+
			`END IF; END $$;`,
		authenticatorRole, authenticatorRole, authenticatorPassword,
	)
	if err := runPsql(ctx, conn, []string{"-v", "ON_ERROR_STOP=1", "-c", createRole}); err != nil {
		return fmt.Errorf("create authenticator role: %w", err)
	}

	// Load the io fixture schema. \ir includes in load.sql resolve relative to
	// the file, so -f with the absolute path finds the sibling SQL files. PGUSER
	// is substituted into roles.sql (GRANT ... TO :"PGUSER").
	loadArgs := []string{
		"-v", "ON_ERROR_STOP=1",
		"-v", "PGUSER=" + authenticatorRole,
		"-f", loadSQL,
	}
	if err := runPsql(ctx, conn, loadArgs); err != nil {
		return fmt.Errorf("load io fixtures from %s: %w", loadSQL, err)
	}

	// Give the fixture-created login roles the shared password so PostgREST can
	// authenticate as each through the gateway (they are created LOGIN but
	// passwordless upstream).
	for _, role := range ioLoginRoles {
		alter := fmt.Sprintf(`ALTER ROLE "%s" PASSWORD '%s';`, role, authenticatorPassword)
		if err := runPsql(ctx, conn, []string{"-v", "ON_ERROR_STOP=1", "-c", alter}); err != nil {
			return fmt.Errorf("set password on io login role %q: %w", role, err)
		}
	}

	t.Logf("Loaded PostgREST io fixtures into %s:%d (authenticator %q + %d extra login roles)",
		conn.Host, conn.Port, authenticatorRole, len(ioLoginRoles))
	return nil
}
