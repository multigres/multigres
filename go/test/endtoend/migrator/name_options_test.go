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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestNamedMigrationAndOptions exercises the CreateMigration deltas that back the
// gateway SQL surface:
//   - a user-supplied unique name that addresses the migration (start/get/drop by
//     name) in place of the generated id, and a duplicate-name rejection;
//   - skip_copy_data + skip_schema_copy: the target schema is seeded out-of-band
//     and no initial COPY runs, so only streamed changes apply.
func TestNamedMigrationAndOptions(t *testing.T) {
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
	seedSource(t, ctx, srcPort) // source has 3 rows in public.orders

	// Seed the target schema out-of-band (empty table) so skip_copy_data +
	// skip_schema_copy has a table to stream into. Only the id PK is needed on the
	// subscriber; streamed rows carry the source-generated id.
	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()
	_, err := tc.Query(ctx, "CREATE TABLE public.orders (id bigint PRIMARY KEY, v text)")
	require.NoError(t, err)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	createResp, err := mt.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{
		Migration: &migratorpb.MigrationRecord{
			Target:         &clustermetadatapb.ShardKey{Database: targetDB},
			Name:           "nightly",
			ConnectionName: createTestConnection(t, ctx, mt, sourceDSN(srcPort)),
			Objects:        objs("public.orders"),
		},
		Options: &migratorpb.MigrationOptions{
			SkipCopyData:   true,
			SkipSchemaCopy: true,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "nightly", createResp.GetMigration().GetName())
	id := createResp.GetMigration().GetId()
	require.NotEmpty(t, id)

	// A second migration reusing the name is rejected.
	_, err = mt.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{
		Migration: &migratorpb.MigrationRecord{
			Target:         &clustermetadatapb.ShardKey{Database: targetDB},
			Name:           "nightly",
			ConnectionName: createTestConnection(t, ctx, mt, sourceDSN(srcPort)),
			Objects:        objs("public.orders"),
		},
	})
	require.ErrorContains(t, err, "already exists")

	// Address the migration by name for the rest of the workflow.
	_, err = mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{Ref: nameRef("nightly"), Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, err := mt.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: nameRef("nightly")})
		if err != nil {
			return false
		}
		require.Equal(t, id, resp.GetMigration().GetId(), "lookup by name must resolve to the same migration")
		return resp.GetStatus().GetCaughtUp()
	}, 60*time.Second, 500*time.Millisecond, "migration must catch up")

	// skip_copy_data: the 3 pre-existing source rows were NOT copied.
	n, ok := countRows(t, ctx, tc, "public.orders")
	require.True(t, ok)
	require.Equal(t, 0, n, "skip_copy_data must not run the initial COPY")

	// Streaming still works: a new source row reaches the target.
	sc := dialSource(t, ctx, srcPort)
	_, err = sc.Query(ctx, "INSERT INTO public.orders (v) VALUES ('d')")
	require.NoError(t, err)
	_ = sc.Close()
	require.Eventually(t, func() bool {
		n, ok := countRows(t, ctx, tc, "public.orders")
		return ok && n == 1
	}, 30*time.Second, 500*time.Millisecond, "streamed insert must reach the target")

	// Drop by name (force: this test does not exercise the drain barrier).
	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Ref: nameRef("nightly"), Force: true})
	require.NoError(t, err)
	_, err = mt.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: nameRef("nightly")})
	require.Error(t, err, "migration must be gone after drop")
}
