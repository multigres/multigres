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
	"database/sql"
	"fmt"
	"os/exec"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// TestPgDump runs pg_dump through the multigateway end to end.
//
// pg_dump on PostgreSQL 17+ performs a startup probe that pins a hostile GUC
// before introspecting the catalog:
//
//	SELECT set_config(name, 'view, foreign-table', false)
//	FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'
//
// The name argument is a column reference, not a literal or bound parameter, so
// it is evaluated per row by PostgreSQL. The multigateway's session-scoped
// set_config path currently rejects anything other than a bare literal or bound
// parameter ("set_config name argument must be a literal constant or a bound
// parameter"), which aborts pg_dump before it emits any schema.
//
// This test exercises the real binary so we know exactly what breaks; it is
// expected to fail until the gateway supports evaluating the name argument on
// the session-scoped path.
func TestPgDump(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pg_dump test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping pg_dump test")
	}

	pgDumpPath, err := exec.LookPath("pg_dump")
	if err != nil {
		t.Skip("pg_dump not found on PATH, skipping pg_dump test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 150*time.Second)

	// Seed a small schema so the dump has something to emit. Routed through the
	// gateway to keep setup on the same pooled path the dump will use.
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "failed to open database connection")
	defer db.Close()
	require.NoError(t, db.PingContext(ctx), "failed to ping multigateway")

	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS pg_dump_probe (id int PRIMARY KEY, name text)`)
	require.NoError(t, err, "failed to create seed table")
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS pg_dump_probe`)
	})

	// Subtest 1: reproduce the exact PG17+ startup probe directly, so we pin the
	// precise statement that breaks independently of pg_dump's other queries.
	t.Run("set_config startup probe", func(t *testing.T) {
		_, err := db.ExecContext(ctx,
			`SELECT set_config(name, 'view, foreign-table', false) FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'`)
		require.NoError(t, err, "pg_dump's restrict_nonsystem_relation_kind probe should succeed through the gateway")
	})

	// Subtest 2: run the real pg_dump binary against the gateway.
	t.Run("schema-only dump", func(t *testing.T) {
		cmd := executil.Command(ctx, pgDumpPath,
			"--schema-only",
			"--no-owner",
			"--no-privileges",
			"-d", "postgres")
		cmd.AddEnv(
			"PGHOST=localhost",
			fmt.Sprintf("PGPORT=%d", setup.MultigatewayPgPort),
			"PGUSER="+shardsetup.DefaultTestUser,
			"PGPASSWORD="+shardsetup.TestPostgresPassword,
			"PGSSLMODE=disable",
		)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "pg_dump through the gateway should succeed\noutput:\n%s", string(out))
		require.Contains(t, string(out), "pg_dump_probe", "dump should contain the seeded table")
	})
}
