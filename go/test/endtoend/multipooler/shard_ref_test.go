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

package multipooler

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	migratorpb "github.com/multigres/multigres/go/pb/migrator"
)

// multigresTypeKind returns what the pg_type row named multigres.<name> is:
// "composite" for a standalone composite type, "relation:<relkind>" for a
// relation's row type, "" if there is no such type.
func multigresTypeKind(t *testing.T, db *sql.DB, name string) string {
	t.Helper()
	var kind sql.NullString
	err := db.QueryRow(`SELECT CASE WHEN c.relkind = 'c' THEN 'composite' ELSE 'relation:' || c.relkind::text END
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_class c ON c.oid = t.typrelid
		WHERE n.nspname = 'multigres' AND t.typname = $1`, name).Scan(&kind)
	if err == sql.ErrNoRows {
		return ""
	}
	require.NoError(t, err)
	return kind.String
}

// columnType returns the formatted type of a column, e.g. "multigres.shard_ref".
func columnType(t *testing.T, db *sql.DB, relation, column string) string {
	t.Helper()
	var typ string
	require.NoError(t, db.QueryRow(`SELECT format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a
		WHERE a.attrelid = $1::regclass AND a.attname = $2 AND NOT a.attisdropped`, relation, column).Scan(&typ))
	return typ
}

// requireShardRefLayout asserts the catalog shard's shard-identity type and
// the catalog's shard_key table coexist: multigres.shard_ref is a standalone
// composite, multigres.shard_key is the catalog table, and the migration
// store's migration_target column is typed shard_ref.
func requireShardRefLayout(t *testing.T, db *sql.DB) {
	t.Helper()
	assert.Equal(t, "composite", multigresTypeKind(t, db, "shard_ref"), "multigres.shard_ref should be a standalone composite")
	assert.Equal(t, "relation:r", multigresTypeKind(t, db, "shard_key"), "multigres.shard_key should be the catalog table")
	assert.Equal(t, "multigres.shard_ref", columnType(t, db, "multigres.migration", "migration_target"))
}

// ensureMigrationStoreSchema drives the migration store's EnsureSchema through
// its real code path: the first Migrator RPC on a primary ensures the store's
// schema (including the shared shard_ref type) before serving.
func ensureMigrationStoreSchema(t *testing.T, grpcPort int) {
	t.Helper()
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	_, err = migratorpb.NewMigratorClient(conn).ListMigrations(utils.WithShortDeadline(t), &migratorpb.ListMigrationsRequest{})
	require.NoError(t, err, "ListMigrations ensures the migration store's schema")
}

// TestShardRef_FreshCatalogShard verifies that on a freshly bootstrapped
// catalog shard the shared multigres.shard_ref type and the catalog's
// multigres.shard_key table coexist once the migration store has ensured its
// schema, and that the standby has the same layout through replication.
func TestShardRef_FreshCatalogShard(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	ensureMigrationStoreSchema(t, setup.PrimaryMultipooler.GrpcPort)

	primaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(setup.PrimaryPgctld.PoolerDir), setup.PrimaryPgctld.PgPort)
	defer primaryDB.Close()
	requireShardRefLayout(t, primaryDB)
	requireCatalogFormat1(t, primaryDB)

	standbyDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(setup.StandbyPgctld.PoolerDir), setup.StandbyPgctld.PgPort)
	defer standbyDB.Close()
	requireShardRefLayout(t, standbyDB)
}

// TestShardRef_UpgradeFromLegacyShardKeyType verifies that a catalog shard
// built from main while the migration store still named its type
// multigres.shard_key (and so had no catalog shard_key table) is upgraded on
// the next primary open: the composite is renamed to shard_ref, columns of
// that type follow the rename with their data intact, and the catalog's
// shard_key tables are created.
//
// Uses an isolated shard: it rewrites the multigres schema and restarts the
// primary pooler.
func TestShardRef_UpgradeFromLegacyShardKeyType(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()

	primary := setup.GetPrimary(t)
	standbys := setup.GetStandbys()
	require.Len(t, standbys, 1)
	standby := standbys[0]

	primaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
	defer primaryDB.Close()
	standbyDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(standby.Pgctld.PoolerDir), standby.Pgctld.PgPort)
	defer standbyDB.Close()

	ensureMigrationStoreSchema(t, primary.Multipooler.GrpcPort)
	requireShardRefLayout(t, primaryDB)

	// Recreate a main-built catalog shard: no catalog shard_key tables, and the
	// migration store's composite named multigres.shard_key. Renaming the
	// bootstrap-created shard_ref back retypes migration.migration_target to
	// the legacy type, exactly as such a cluster has it. A probe table with a
	// row of the legacy type checks data survives the rename.
	_, err := primaryDB.Exec(`BEGIN;
DROP TABLE multigres.shard_key_column, multigres.shard_key;
ALTER TYPE multigres.shard_ref RENAME TO shard_key;
CREATE TABLE public.shard_ref_upgrade_probe (target multigres.shard_key);
INSERT INTO public.shard_ref_upgrade_probe VALUES (ROW('postgres', 'commerce', '0-80')::multigres.shard_key);
COMMIT`)
	require.NoError(t, err, "recreate the legacy shard_key type")
	require.Equal(t, "composite", multigresTypeKind(t, primaryDB, "shard_key"))
	require.Empty(t, multigresTypeKind(t, primaryDB, "shard_ref"))
	require.Equal(t, "multigres.shard_key", columnType(t, primaryDB, "multigres.migration", "migration_target"))

	// Reopening the primary pooler runs ensureSidecarSchemas, which renames the
	// legacy type before the catalog ensure recreates the shard_key tables.
	restartMultipooler(t, primary)
	require.Eventually(t, func() bool {
		var exists bool
		err := primaryDB.QueryRow(`SELECT to_regclass('multigres.shard_key_column') IS NOT NULL`).Scan(&exists)
		return err == nil && exists
	}, 30*time.Second, 200*time.Millisecond, "open should recreate the catalog shard_key tables")

	requireShardRefLayout(t, primaryDB)
	requireCatalogFormat1(t, primaryDB)
	assert.Equal(t, "multigres.shard_ref", columnType(t, primaryDB, "public.shard_ref_upgrade_probe", "target"))
	var tableGroup string
	require.NoError(t, primaryDB.QueryRow(`SELECT (target).table_group FROM public.shard_ref_upgrade_probe`).Scan(&tableGroup))
	assert.Equal(t, "commerce", tableGroup, "rows of the legacy type survive the rename")

	// The migration store still works against the renamed type: the restarted
	// process ensures the store's schema, including the stat_migration view,
	// whose migration_target follows the renamed type.
	ensureMigrationStoreSchema(t, primary.Multipooler.GrpcPort)
	requireShardRefLayout(t, primaryDB)
	assert.Equal(t, "multigres.shard_ref", columnType(t, primaryDB, "multigres.stat_migration", "migration_target"))

	// The upgrade replicates to the standby.
	require.Eventually(t, func() bool {
		return multigresTypeKind(t, standbyDB, "shard_ref") == "composite" &&
			multigresTypeKind(t, standbyDB, "shard_key") == "relation:r"
	}, 30*time.Second, 200*time.Millisecond, "standby should get the renamed type and the catalog tables")
	requireShardRefLayout(t, standbyDB)
}
