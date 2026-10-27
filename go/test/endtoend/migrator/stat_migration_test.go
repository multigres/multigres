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

package migrator

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestStatMigrationView is the regression test for multigres.stat_migration:
// the real Postgres view every migration's status is queryable through,
// directly against the target (this test's path) as well as via the gateway's
// pseudo-view SQL interface (a separate mechanism built on top, not exercised
// here). Proves the view's composite/enum columns (migration_target,
// migration_phase, active_direction) and the join-derived columns
// (connection_name, total_relations/ready_relations, lag_bytes/lag_seconds)
// all come back with sane values for a freshly created, not-yet-started
// migration.
func TestStatMigrationView(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()
	primary := setup.GetPrimary(t)
	shardsetup.WaitForManagerReady(t, primary.Multipooler)
	targetDB := waitForPrimaryDB(t, ctx, setup)

	srcPort := startStandaloneSource(t)
	seedSource(t, ctx, srcPort)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	connName := createTestConnection(t, ctx, mt, sourceDSN(srcPort))

	createResp, err := mt.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{
		Migration: &migratorpb.MigrationRecord{
			Name:           "statview",
			Target:         &clustermetadatapb.ShardKey{Database: targetDB},
			ConnectionName: connName,
			Objects:        objs("public.orders"),
		},
	})
	require.NoError(t, err)
	id := createResp.GetMigration().GetId()

	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()

	res, err := tc.Query(ctx, `SELECT migration_id, migration_name, connection_name,
		(migration_target).database, (migration_target).table_group, (migration_target).shard,
		migration_phase, active_direction, total_relations, ready_relations,
		lag_bytes, lag_seconds, last_error
		FROM multigres.stat_migration WHERE migration_id = `+strconv.FormatInt(id, 10))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Len(t, res[0].Rows, 1, "exactly one row for a migration that exists")
	v := res[0].Rows[0].Values
	require.Equal(t, strconv.FormatInt(id, 10), string(v[0]), "migration_id")
	require.Equal(t, "statview", string(v[1]), "migration_name")
	require.Equal(t, connName, string(v[2]), "connection_name")
	require.Equal(t, targetDB, string(v[3]), "migration_target.database")
	require.Equal(t, "", string(v[4]), "migration_target.table_group (unset in this request)")
	require.Equal(t, "", string(v[5]), "migration_target.shard (unset in this request)")
	require.Equal(t, "CREATED", string(v[6]), "migration_phase")
	require.Equal(t, "IMPORT", string(v[7]), "active_direction defaults to IMPORT")
	require.Equal(t, "0", string(v[8]), "total_relations: no subscription yet")
	require.Equal(t, "0", string(v[9]), "ready_relations: no subscription yet")
	require.Equal(t, "0", string(v[10]), "lag_bytes: no local subscription or slot yet")
	require.Equal(t, "0", string(v[11]), "lag_seconds: no local subscription or slot yet")
	require.Equal(t, "", string(v[12]), "last_error: no failure")

	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Ref: idRef(id), Force: true})
	require.NoError(t, err)
}
