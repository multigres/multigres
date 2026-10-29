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

package multipooler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMigrationForeignKeyIsEnforced verifies, on a freshly bootstrapped catalog shard, that
// the migrator's schema ensure adds the foreign key from multigres.migration into
// multigres.connections and that it holds: a connection a migration uses cannot be deleted,
// and a migration cannot point at a connection that does not exist.
func TestMigrationForeignKeyIsEnforced(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()
	primary := setup.GetPrimary(t)
	primaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
	defer primaryDB.Close()

	requireMigrationForeignKey(t, primaryDB)

	var connectionID int64
	require.NoError(t, primaryDB.QueryRow(`INSERT INTO multigres.connections (name, dsn)
		VALUES ('fk-src', 'host=h dbname=app') RETURNING connection_id`).Scan(&connectionID))
	// The migration is EXPORTING so the pooler does not hold serving for it.
	_, err := primaryDB.Exec(`INSERT INTO multigres.migration (migration_id, migration_phase, migration_target, connection_id)
		VALUES (1, 'EXPORTING', ROW('app', 'default', '0-inf')::multigres.shard_ref, $1)`, connectionID)
	require.NoError(t, err)

	_, err = primaryDB.Exec(`DELETE FROM multigres.connections WHERE connection_id = $1`, connectionID)
	require.Error(t, err, "deleting a connection that a migration references must fail")
	assert.Contains(t, err.Error(), "migration_connections_fkey")

	_, err = primaryDB.Exec(`INSERT INTO multigres.migration (migration_id, migration_phase, migration_target, connection_id)
		VALUES (2, 'EXPORTING', ROW('app', 'default', '0-inf')::multigres.shard_ref, $1)`, connectionID+1000)
	require.Error(t, err, "a migration must not reference a missing connection")
	assert.Contains(t, err.Error(), "migration_connections_fkey")

	// Once nothing references it the connection can go.
	_, err = primaryDB.Exec(`DELETE FROM multigres.migration WHERE migration_id = 1`)
	require.NoError(t, err)
	_, err = primaryDB.Exec(`DELETE FROM multigres.connections WHERE connection_id = $1`, connectionID)
	require.NoError(t, err)
}

// TestCatalogSchema_CompletesWithoutAMigrationTable: the catalog ensure must complete on a
// shard that has no multigres.migration, and not depend on the migrator's tables (the foreign
// key into connections is the migrator's, added once its tables exist).
//
// Uses an isolated shard: it removes catalog and migrator tables and restarts the pooler.
func TestCatalogSchema_CompletesWithoutAMigrationTable(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	// Two poolers: a shard needs a standby to bootstrap.
	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()
	primary := setup.GetPrimary(t)
	primaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
	defer primaryDB.Close()
	requireCatalogFormat1(t, primaryDB)

	// No migrator tables, and no connections table either: the ensure creates the latter again.
	_, err := primaryDB.Exec(`
DROP VIEW IF EXISTS multigres.stat_migration;
DROP TABLE IF EXISTS multigres.migration_journal, multigres.migration_tables, multigres.migration;
DROP TABLE multigres.connections;`)
	require.NoError(t, err)

	restartMultipooler(t, primary)
	require.Eventually(t, func() bool {
		var exists bool
		return primaryDB.QueryRow(`SELECT to_regclass('multigres.connections') IS NOT NULL`).Scan(&exists) == nil && exists
	}, 30*time.Second, 200*time.Millisecond, "the catalog ensure should complete with no migration table")
}
