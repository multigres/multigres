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
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// catalogSchemaLockKey mirrors the multipooler manager's advisory lock key
// for the catalog schema ensure ("multigrs" in ASCII).
const catalogSchemaLockKey int64 = 0x6d756c7469677273

// catalogFormat1Columns are the catalog tables format 1 creates, each with its
// columns in declaration order.
var catalogFormat1Columns = map[string][]string{
	// definition_hash is an additive column, not part of format 1.
	"catalog_state":    {"singleton", "format_version", "routing_version", "plan_epoch", "definition_hash"},
	"tablegroup":       {"tablegroup_id", "name", "state", "created_at"},
	"shard":            {"shard_id", "tablegroup_id", "shard_name", "range_start", "range_end", "state"},
	"tablegroup_table": {"table_id", "schema_name", "table_name", "tablegroup_id"},
	"shard_index":      {"shard_index_id", "name", "type", "params", "state"},
	"shard_key":        {"shard_key_id", "table_id", "shard_index_id", "name", "role", "state"},
	"shard_key_column": {"shard_key_id", "position", "column_name"},
	"operation": {
		"operation_id", "kind", "state", "request", "initiating_role", "cancel_requested",
		"last_error", "created_at", "updated_at",
	},
	"routing_rule": {
		"rule_id", "operation_id", "scope", "from_tablegroup_id", "from_table_id", "to_tablegroup_id",
		"to_shard_id", "to_shard_key_id", "created_at", "created_version",
	},
	"connections":     {"connection_id", "name", "dsn", "owner_role", "created_at", "conninfo"},
	"admission_fence": {"fence_id", "operation_id", "tablegroup_id", "shard_id", "state", "version"},
}

// legacyCatalogSQL replaces the format 1 catalog with the pre-versioning
// layout and rows that older binaries bootstrapped.
const legacyCatalogSQL = `BEGIN;
DROP VIEW IF EXISTS multigres.stat_migration;
ALTER TABLE multigres.migration DROP CONSTRAINT IF EXISTS migration_connections_fkey;
DROP TABLE multigres.admission_fence_ack, multigres.admission_fence, multigres.routing_rule, multigres.shard_key_column, multigres.shard_key, multigres.shard_index,
	multigres.tablegroup_table, multigres.shard, multigres.tablegroup, multigres.operation,
	multigres.connections, multigres.catalog_state;
CREATE TABLE multigres.tablegroup (
	oid BIGSERIAL PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	type TEXT NOT NULL
);
CREATE TABLE multigres.tablegroup_table (
	oid BIGSERIAL PRIMARY KEY,
	tablegroup_oid BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
	name TEXT NOT NULL,
	UNIQUE (tablegroup_oid, name)
);
CREATE TABLE multigres.shard (
	oid BIGSERIAL PRIMARY KEY,
	tablegroup_oid BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
	shard_name TEXT NOT NULL,
	key_range_start BYTEA NULL,
	key_range_end BYTEA NULL,
	UNIQUE (tablegroup_oid, shard_name)
);
INSERT INTO multigres.tablegroup (name, type) VALUES ('default', 'unsharded');
INSERT INTO multigres.shard (tablegroup_oid, shard_name)
	SELECT oid, '0-inf' FROM multigres.tablegroup WHERE name = 'default';
COMMIT`

// catalogSnapshot renders the catalog's rows (including generated ids and
// timestamps) as text, so two snapshots are equal only if nothing changed.
func catalogSnapshot(t *testing.T, db *sql.DB) string {
	t.Helper()
	var state, tablegroups, shards sql.NullString
	err := db.QueryRow(`SELECT
		(SELECT json_agg(c)::text FROM multigres.catalog_state c),
		(SELECT json_agg(tg ORDER BY tablegroup_id)::text FROM multigres.tablegroup tg),
		(SELECT json_agg(s ORDER BY shard_id)::text FROM multigres.shard s)`).Scan(&state, &tablegroups, &shards)
	require.NoError(t, err)
	return strings.Join([]string{state.String, tablegroups.String, shards.String}, "\n")
}

// catalogFormatVersion returns multigres.catalog_state.format_version, or 0 if
// the table does not exist yet.
func catalogFormatVersion(t *testing.T, db *sql.DB) int {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRow(`SELECT to_regclass('multigres.catalog_state') IS NOT NULL`).Scan(&exists))
	if !exists {
		return 0
	}
	var version int
	require.NoError(t, db.QueryRow(`SELECT format_version FROM multigres.catalog_state`).Scan(&version))
	return version
}

// currentCatalogFormat is the format this binary writes. Format 2 deleted
// rows only, so its tables keep the format 1 columns.
const currentCatalogFormat = 2

// requireCurrentCatalog asserts db holds exactly the current catalog, with
// the format 1 tables and their initial rows, and none of the legacy column
// layout.
func requireCurrentCatalog(t *testing.T, db *sql.DB) {
	t.Helper()

	// Each table has exactly its format 1 columns: this also rules out the
	// legacy layout (tablegroup.type, oid keys, key_range_*) and the dropped
	// pre-RFC columns (distribution, kind, function, component_function,
	// prefix_bytes).
	for table, want := range catalogFormat1Columns {
		var cols sql.NullString
		require.NoError(t, db.QueryRow(`SELECT string_agg(column_name, ',' ORDER BY ordinal_position)
			FROM information_schema.columns WHERE table_schema = 'multigres' AND table_name = $1`, table).Scan(&cols))
		assert.Equal(t, strings.Join(want, ","), cols.String, "multigres.%s columns", table)
	}

	// multigres.migration references the connections table by a foreign key, which the
	// migrator's schema ensure adds on this (the catalog) shard, right after the catalog.
	requireMigrationForeignKey(t, db)

	var formatVersion int
	var routingVersion, planEpoch int64
	require.NoError(t, db.QueryRow(`SELECT format_version, routing_version, plan_epoch FROM multigres.catalog_state`).
		Scan(&formatVersion, &routingVersion, &planEpoch))
	assert.Equal(t, currentCatalogFormat, formatVersion)
	assert.Zero(t, routingVersion)
	assert.Zero(t, planEpoch)

	var tgName, tgState string
	require.NoError(t, db.QueryRow(`SELECT name, state FROM multigres.tablegroup`).Scan(&tgName, &tgState))
	assert.Equal(t, "default", tgName)
	assert.Equal(t, "active", tgState)

	var shardName, shardState, shardTG string
	var rangeStartNull, rangeEndNull bool
	require.NoError(t, db.QueryRow(`SELECT s.shard_name, s.state, tg.name, s.range_start IS NULL, s.range_end IS NULL
		FROM multigres.shard s JOIN multigres.tablegroup tg USING (tablegroup_id)`).
		Scan(&shardName, &shardState, &shardTG, &rangeStartNull, &rangeEndNull))
	assert.Equal(t, "0-inf", shardName)
	assert.Equal(t, "active", shardState)
	assert.Equal(t, "default", shardTG)
	assert.True(t, rangeStartNull, "0-inf range_start should be NULL")
	assert.True(t, rangeEndNull, "0-inf range_end should be NULL")

	// shard_index.type is unconstrained so users can define their own index
	// types.
	var typeChecks int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM pg_constraint
		WHERE conrelid = 'multigres.shard_index'::regclass AND contype = 'c'
		  AND pg_get_constraintdef(oid) LIKE '%type%'`).Scan(&typeChecks))
	assert.Zero(t, typeChecks, "shard_index.type must not be limited to a fixed list")

	for _, table := range []string{"tablegroup_table", "shard_index", "shard_key", "shard_key_column", "operation", "connections", "routing_rule", "admission_fence"} {
		var n int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM multigres.`+table).Scan(&n))
		assert.Zero(t, n, "multigres.%s should start empty", table)
	}
}

// requireMigrationForeignKey waits until multigres.migration has exactly one foreign key,
// to multigres.connections(connection_id). The migrator's schema ensure adds it right after
// the catalog ensure: at bootstrap, before the first backup, and on every full ensure. It
// waits because a standby gets it by replication, and after a restart the pooler adds it
// when it opens.
func requireMigrationForeignKey(t *testing.T, db *sql.DB) {
	t.Helper()
	const want = "FOREIGN KEY (connection_id) REFERENCES multigres.connections(connection_id)"
	var defs sql.NullString
	require.Eventually(t, func() bool {
		defs = sql.NullString{}
		if err := db.QueryRow(`SELECT string_agg(pg_get_constraintdef(oid), '; ' ORDER BY oid)
			FROM pg_constraint WHERE conrelid = to_regclass('multigres.migration') AND contype = 'f'`).Scan(&defs); err != nil {
			return false
		}
		return strings.TrimSuffix(defs.String, " NOT VALID") == want
	}, 45*time.Second, 250*time.Millisecond, "multigres.migration should reference multigres.connections (have %q)", defs.String)
}

// waitForCatalogFormat waits until db reports the given catalog format.
func waitForCatalogFormat(t *testing.T, db *sql.DB, want int, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var exists bool
		if err := db.QueryRow(`SELECT to_regclass('multigres.catalog_state') IS NOT NULL`).Scan(&exists); err != nil || !exists {
			return false
		}
		var version int
		if err := db.QueryRow(`SELECT format_version FROM multigres.catalog_state`).Scan(&version); err != nil {
			return false
		}
		return version == want
	}, 30*time.Second, 200*time.Millisecond, msg)
}

// waitForCatalogDefinition waits until db's catalog_state records a
// definition hash, which the pooler writes once a full catalog ensure, the
// migrator's schema included, has finished.
func waitForCatalogDefinition(t *testing.T, db *sql.DB, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var recorded bool
		err := db.QueryRow(`SELECT definition_hash IS NOT NULL FROM multigres.catalog_state`).Scan(&recorded)
		return err == nil && recorded
	}, 30*time.Second, 200*time.Millisecond, msg)
}

// forgetCatalogDefinition clears the definition hash catalog_state records,
// as on a catalog an older binary last ensured. Without it, a pooler that
// becomes the serving primary finds its own hash and skips the full ensure, so
// a test that changes the catalog by hand must clear it for the pooler to
// repair the change.
func forgetCatalogDefinition(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`UPDATE multigres.catalog_state SET definition_hash = NULL`)
	require.NoError(t, err)
}

// killMultipooler SIGKILLs inst's multipooler process. Unlike a graceful
// shutdown, which also stops postgres (it then comes back as a standby), this
// leaves postgres running as the primary, so a restarted manager runs
// openLocked against a live, already-bootstrapped primary.
func killMultipooler(t *testing.T, inst *shardsetup.MultipoolerInstance) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, stopped := inst.Multipooler.Process.Kill(ctx)
	require.True(t, stopped, "multipooler %s should exit after SIGKILL", inst.Name)
}

// catalogIdentityValues returns the last values of the catalog's seeded
// identity sequences. Read it on the primary only: a standby sees sequence
// values as logged ahead in WAL, not the primary's last value.
func catalogIdentityValues(t *testing.T, db *sql.DB) string {
	t.Helper()
	var values sql.NullString
	require.NoError(t, db.QueryRow(`SELECT concat_ws(',',
		pg_sequence_last_value(pg_get_serial_sequence('multigres.tablegroup', 'tablegroup_id')),
		pg_sequence_last_value(pg_get_serial_sequence('multigres.shard', 'shard_id')))`).Scan(&values))
	return values.String
}

// restartMultipooler kills and restarts inst's multipooler process (see
// killMultipooler), so the restarted manager reopens against its running
// primary.
func restartMultipooler(t *testing.T, inst *shardsetup.MultipoolerInstance) {
	t.Helper()
	killMultipooler(t, inst)
	require.NoError(t, inst.Multipooler.Start(t.Context(), t))
	shardsetup.WaitForManagerReady(t, inst.Multipooler)
}

// TestCatalogSchema_FreshBootstrap verifies that a freshly bootstrapped catalog
// shard has the current catalog, and that its standby received it through
// replication.
func TestCatalogSchema_FreshBootstrap(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	primaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(setup.PrimaryPgctld.PoolerDir), setup.PrimaryPgctld.PgPort)
	defer primaryDB.Close()
	requireCurrentCatalog(t, primaryDB)

	standbyDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(setup.StandbyPgctld.PoolerDir), setup.StandbyPgctld.PgPort)
	defer standbyDB.Close()
	requireCurrentCatalog(t, standbyDB)
	assert.Equal(t, catalogSnapshot(t, primaryDB), catalogSnapshot(t, standbyDB),
		"standby catalog should match the primary's")
}

// TestCatalogSchema_UpgradeFromLegacy verifies that a catalog shard holding
// the legacy (pre-versioning) catalog tables is upgraded to the current format when the
// primary pooler opens, that a second open leaves it unchanged, that the
// upgrade replicates to the standby, and that a later open recreates a missing
// catalog table without a format change (the additive path an existing
// catalog takes to pick up a table added by a later release).
//
// Uses an isolated shard: it rewrites the catalog and restarts the primary
// pooler.
func TestCatalogSchema_UpgradeFromLegacy(t *testing.T) {
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

	requireCurrentCatalog(t, primaryDB)

	// Recreate the legacy layout, as an older binary would have left it.
	_, err := primaryDB.Exec(legacyCatalogSQL)
	require.NoError(t, err, "recreate legacy catalog")
	require.Zero(t, catalogFormatVersion(t, primaryDB), "legacy catalog has no catalog_state")
	var legacyType string
	require.NoError(t, primaryDB.QueryRow(`SELECT type FROM multigres.tablegroup WHERE name = 'default'`).Scan(&legacyType))
	require.Equal(t, "unsharded", legacyType)

	// The reopened primary pooler upgrades the catalog before it serves as the
	// primary.
	restartMultipooler(t, primary)
	waitForCatalogFormat(t, primaryDB, currentCatalogFormat, "primary catalog should be upgraded to the current format on open")
	waitForCatalogDefinition(t, primaryDB, "the full ensure should record its definition hash")
	requireCurrentCatalog(t, primaryDB)
	upgraded := catalogSnapshot(t, primaryDB)
	identityValues := catalogIdentityValues(t, primaryDB)

	// Running the full ensure again leaves the catalog unchanged. Forget the
	// hash so the reopened pooler runs it, and hold the catalog lock across the
	// restart so the test observes it actually run (it blocks on the lock)
	// before comparing.
	forgetCatalogDefinition(t, primaryDB)
	lockConn, err := primaryDB.Conn(t.Context())
	require.NoError(t, err)
	defer lockConn.Close()
	_, err = lockConn.ExecContext(t.Context(), `SELECT pg_advisory_lock($1)`, catalogSchemaLockKey)
	require.NoError(t, err)

	killMultipooler(t, primary)
	startErr := make(chan error, 1)
	go func() { startErr <- primary.Multipooler.Start(t.Context(), t) }()
	require.Eventually(t, func() bool {
		var waiting int
		err := primaryDB.QueryRow(`SELECT count(*) FROM pg_locks
			WHERE locktype = 'advisory' AND NOT granted AND objsubid = 1
			  AND ((classid::bigint << 32) | objid::bigint) = $1`, catalogSchemaLockKey).Scan(&waiting)
		return err == nil && waiting > 0
	}, 30*time.Second, 100*time.Millisecond, "reopened pooler should run the catalog ensure")
	_, err = lockConn.ExecContext(t.Context(), `SELECT pg_advisory_unlock($1)`, catalogSchemaLockKey)
	require.NoError(t, err)
	require.NoError(t, <-startErr)
	shardsetup.WaitForManagerReady(t, primary.Multipooler)
	require.Eventually(t, func() bool {
		var held int
		err := primaryDB.QueryRow(`SELECT count(*) FROM pg_locks
			WHERE locktype = 'advisory' AND objsubid = 1
			  AND ((classid::bigint << 32) | objid::bigint) = $1`, catalogSchemaLockKey).Scan(&held)
		return err == nil && held == 0
	}, 30*time.Second, 100*time.Millisecond, "catalog ensure should finish")
	waitForCatalogDefinition(t, primaryDB, "the second full ensure should record its definition hash")
	assert.Equal(t, upgraded, catalogSnapshot(t, primaryDB), "a second ensure must not change the catalog")
	// The seed re-runs on every full ensure; its existing rows must not consume ids.
	assert.Equal(t, identityValues, catalogIdentityValues(t, primaryDB), "a second ensure must not consume identity values")
	requireCurrentCatalog(t, primaryDB)

	// The upgrade replicates to the standby.
	waitForCatalogFormat(t, standbyDB, currentCatalogFormat, "standby catalog should reach the current format through replication")
	require.Eventually(t, func() bool {
		return catalogSnapshot(t, standbyDB) == upgraded
	}, 30*time.Second, 200*time.Millisecond, "standby catalog should match the upgraded primary's")
	requireCurrentCatalog(t, standbyDB)

	// A catalog table missing from an existing catalog, last ensured by an
	// older binary (no hash), is recreated on the next open, with no format
	// change. routing_rule has no dependents.
	_, err = primaryDB.Exec(`DROP TABLE multigres.routing_rule`)
	require.NoError(t, err)
	forgetCatalogDefinition(t, primaryDB)
	restartMultipooler(t, primary)
	require.Eventually(t, func() bool {
		var exists bool
		err := primaryDB.QueryRow(`SELECT to_regclass('multigres.routing_rule') IS NOT NULL`).Scan(&exists)
		return err == nil && exists
	}, 30*time.Second, 200*time.Millisecond, "open should recreate the missing routing_rule table")
	waitForCatalogDefinition(t, primaryDB, "the ensure should record its definition hash again")
	assert.Equal(t, currentCatalogFormat, catalogFormatVersion(t, primaryDB), "recreating a table must not change the format")
	assert.Equal(t, upgraded, catalogSnapshot(t, primaryDB), "recreating a table must not touch the seeded rows")
	requireCurrentCatalog(t, primaryDB)
	require.Eventually(t, func() bool {
		var exists bool
		err := standbyDB.QueryRow(`SELECT to_regclass('multigres.routing_rule') IS NOT NULL`).Scan(&exists)
		return err == nil && exists
	}, 30*time.Second, 200*time.Millisecond, "the recreated table should replicate to the standby")
	requireCurrentCatalog(t, standbyDB)
}

// TestCatalogSchema_Format2DeletesFencesOfFinishedOperations verifies the
// format 2 data migration against real postgres: a catalog at format 1 with a
// fence its finished operation never lifted loses that fence on the next open,
// keeps a fence of an unfinished operation, and advances routing_version.
func TestCatalogSchema_Format2DeletesFencesOfFinishedOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()
	primary := setup.GetPrimary(t)
	db := connectToPostgresViaSocket(t, getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
	defer db.Close()
	ctx := t.Context()

	const doneOp, liveOp = "aaaaaaaa-0000-0000-0000-00000000c2d0", "aaaaaaaa-0000-0000-0000-00000000c2d1"
	// A fence on default's shard left by a finished operation, as an older
	// pooler could leave it, and one on the whole tablegroup of an
	// unfinished operation.
	for _, stmt := range []string{
		`INSERT INTO multigres.operation (operation_id, kind, state) VALUES ('` + doneOp + `', 'reshard', 'done'), ('` + liveOp + `', 'reshard', 'preparing')`,
		`INSERT INTO multigres.admission_fence (operation_id, tablegroup_id, shard_id, state, version)
SELECT '` + doneOp + `', tablegroup_id, shard_id, 'FENCED', 1 FROM multigres.shard`,
		`INSERT INTO multigres.admission_fence (operation_id, tablegroup_id, shard_id, state, version)
SELECT '` + liveOp + `', tablegroup_id, NULL, 'FENCED', 1 FROM multigres.tablegroup`,
		`UPDATE multigres.catalog_state SET format_version = 1`,
	} {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	var before int64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT routing_version FROM multigres.catalog_state`).Scan(&before))

	restartMultipooler(t, primary)
	waitForCatalogFormat(t, db, currentCatalogFormat, "the open should upgrade the catalog to the current format")

	var ops []string
	rows, err := db.QueryContext(ctx, `SELECT operation_id::text FROM multigres.admission_fence ORDER BY fence_id`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var op string
		require.NoError(t, rows.Scan(&op))
		ops = append(ops, op)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{liveOp}, ops, "only the unfinished operation's fence is left")
	var after int64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT routing_version FROM multigres.catalog_state`).Scan(&after))
	assert.Equal(t, before+1, after, "the deletion is a routing change")
}
