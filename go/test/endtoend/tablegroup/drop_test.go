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

package tablegroup

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestTableGroupDrop drives a tablegroup through DropTableGroup on real poolers:
// the refusals, a drop of an active tablegroup that the provisioner tears
// down, the operations view, and cancellation of a creation through SQL and
// through DropTableGroup.
func TestTableGroupDrop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tablegroup test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}
	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultiadmin())
	defer cleanup()
	primary := setup.NewPrimaryClient(t)
	defer primary.Close()
	manager := primary.Manager
	prov := newProvisioner(t, setup)
	ctx := t.Context()

	t.Run("refusals that need no tablegroup", func(t *testing.T) {
		_, err := manager.DropTableGroup(ctx, &multipoolermanagerdatapb.DropTableGroupRequest{Name: "default"})
		requireCode(t, err, codes.FailedPrecondition)
		_, err = manager.DropTableGroup(ctx, &multipoolermanagerdatapb.DropTableGroupRequest{Name: "missing"})
		requireCode(t, err, codes.NotFound)
		resp, err := manager.DropTableGroup(ctx, &multipoolermanagerdatapb.DropTableGroupRequest{Name: "missing", IfExists: true})
		require.NoError(t, err)
		assert.True(t, resp.GetNotFound())
		assert.Empty(t, resp.GetOperationId())
	})

	// Create and activate a single-shard tablegroup. The processes provisioned
	// here are stopped by remove below, and by this test's cleanup otherwise.
	created, err := manager.CreateTableGroup(ctx, &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
	require.NoError(t, err)
	tg := prov.find(t, "commerce")
	require.NotNil(t, tg)
	prov.provision(t, tg)
	require.Eventually(t, func() bool {
		return getOperation(t, manager, created.GetOperationId()).GetState() == multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE
	}, provisionTimeout, time.Second, "the creation never completed")

	t.Run("a tablegroup with tables cannot be dropped", func(t *testing.T) {
		_, err := primary.Pooler.ExecuteQuery(ctx, fmt.Sprintf(
			"INSERT INTO multigres.tablegroup_table (schema_name, table_name, tablegroup_id) VALUES ('public', 'orders', %d)", tg.GetTablegroupId()), 1)
		require.NoError(t, err)
		_, err = manager.DropTableGroup(ctx, &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
		requireCode(t, err, codes.FailedPrecondition)
		_, err = primary.Pooler.ExecuteQuery(ctx, fmt.Sprintf("DELETE FROM multigres.tablegroup_table WHERE tablegroup_id = %d", tg.GetTablegroupId()), 1)
		require.NoError(t, err)
	})

	var dropOp string
	t.Run("dropping an active tablegroup records the intent", func(t *testing.T) {
		resp, err := manager.DropTableGroup(ctx, &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
		require.NoError(t, err)
		assert.False(t, resp.GetCancelledCreation())
		assert.Equal(t, tg.GetTablegroupId(), resp.GetTablegroupId())
		dropOp = resp.GetOperationId()

		again, err := manager.DropTableGroup(ctx, &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
		require.NoError(t, err)
		assert.Equal(t, dropOp, again.GetOperationId(), "a repeated drop returns the same operation")

		listed := prov.find(t, "commerce")
		require.NotNil(t, listed, "a dropping tablegroup is listed, as the signal to remove it")
		assert.Equal(t, shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_DROPPING, listed.GetState())

		op := getOperation(t, manager, dropOp)
		assert.Equal(t, multipoolermanagerdatapb.OperationKind_OPERATION_KIND_DROP_TABLEGROUP, op.GetKind())
		assert.Equal(t, multipoolermanagerdatapb.OperationState_OPERATION_STATE_DROPPING, op.GetState())
		assert.Equal(t, "commerce", op.GetTablegroupName())
		assert.Equal(t, int32(2), op.GetPoolersRegistered(), "both poolers of the shard still hold it")
		assert.True(t, op.GetClaimHeld(), "the provisioner still holds it")

		rows, err := primary.Pooler.ExecuteQuery(ctx,
			"SELECT state, tablegroup_name FROM multigres.operations WHERE operation_id = '"+dropOp+"'", 1)
		require.NoError(t, err)
		require.Len(t, rows.Rows, 1)
		assert.Equal(t, "dropping", string(rows.Rows[0].Values[0]))
		assert.Equal(t, "commerce", string(rows.Rows[0].Values[1]))
	})

	t.Run("the provisioner's teardown completes the drop", func(t *testing.T) {
		prov.remove(t, tg.GetTablegroupId())
		require.Eventually(t, func() bool {
			return getOperation(t, manager, dropOp).GetState() == multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE
		}, provisionTimeout, time.Second, "the drop never completed")
		assert.Nil(t, prov.find(t, "commerce"), "a dropped tablegroup is gone from the list")
		rows, err := primary.Pooler.ExecuteQuery(ctx, "SELECT count(*) FROM multigres.tablegroup WHERE name = 'commerce'", 1)
		require.NoError(t, err)
		assert.Equal(t, "0", string(rows.Rows[0].Values[0]))
		rows, err = primary.Pooler.ExecuteQuery(ctx, "SELECT state FROM multigres.operations WHERE operation_id = '"+dropOp+"'", 1)
		require.NoError(t, err)
		require.Len(t, rows.Rows, 1, "the operation stays readable after the rows are gone")
		assert.Equal(t, "done", string(rows.Rows[0].Values[0]))

		// The shard's initialization claim, taken by the dropped incarnation's
		// multiorch and keyed by name, went with it: a fresh coordinator wins.
		shardRef := &clustermetadatapb.ShardRef{Database: "postgres", TableGroup: "commerce", Shard: "0-inf"}
		won, _, err := setup.TopoServer.ClaimShardInitialization(ctx, shardRef,
			&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: setup.CellName, Name: "fresh-orch"}, nil)
		require.NoError(t, err)
		assert.True(t, won, "the initialization claim was deleted with the tablegroup")
		require.NoError(t, setup.TopoServer.DeleteShardInitClaim(ctx, shardRef))
	})

	t.Run("the name is free again and a creation can be cancelled from SQL", func(t *testing.T) {
		second, err := manager.CreateTableGroup(ctx, &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
		require.NoError(t, err)
		assert.NotEqual(t, tg.GetTablegroupId(), second.GetTablegroupId(), "a new incarnation gets a new id")

		rows, err := primary.Pooler.ExecuteQuery(ctx, "SELECT multigres.cancel_operation('"+second.GetOperationId()+"')", 1)
		require.NoError(t, err)
		assert.Equal(t, "t", string(rows.Rows[0].Values[0]))
		rows, err = primary.Pooler.ExecuteQuery(ctx, "SELECT multigres.cancel_operation('"+second.GetOperationId()+"')", 1)
		require.NoError(t, err)
		assert.Equal(t, "f", string(rows.Rows[0].Values[0]), "already cancelled")

		require.Eventually(t, func() bool {
			return getOperation(t, manager, second.GetOperationId()).GetState() == multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED
		}, provisionTimeout, time.Second, "the cancelled creation never failed")
		assert.Equal(t, "cancelled", getOperation(t, manager, second.GetOperationId()).GetLastError())
		require.Eventually(t, func() bool { return prov.find(t, "commerce") == nil },
			provisionTimeout, time.Second, "a never-claimed tablegroup is deleted as soon as its creation fails")
	})

	t.Run("dropping a creating tablegroup cancels the creation", func(t *testing.T) {
		third, err := manager.CreateTableGroup(ctx, &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
		require.NoError(t, err)
		resp, err := manager.DropTableGroup(ctx, &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
		require.NoError(t, err)
		assert.True(t, resp.GetCancelledCreation())
		assert.Equal(t, third.GetOperationId(), resp.GetOperationId(), "the caller follows the creation")
		require.Eventually(t, func() bool {
			return getOperation(t, manager, third.GetOperationId()).GetState() == multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED
		}, provisionTimeout, time.Second, "the cancelled creation never failed")
		require.Eventually(t, func() bool { return prov.find(t, "commerce") == nil }, provisionTimeout, time.Second)
		_, err = manager.DropTableGroup(ctx, &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
		requireCode(t, err, codes.NotFound)
	})
}
