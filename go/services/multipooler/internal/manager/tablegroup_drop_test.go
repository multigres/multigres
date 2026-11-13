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

package manager

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
)

// dropLookup answers DropTableGroup's name lookup with one row, or none.
func (env *tableGroupTestEnv) dropLookup(rows ...[]any) {
	env.expect("FROM multigres.tablegroup WHERE name = \\$1 FOR UPDATE", []string{"tablegroup_id", "state"}, rows)
}

// expectDropCounts answers the two checks DropTableGroup makes of an active
// tablegroup: how many tables it contains and how many routing rules name it.
func (env *tableGroupTestEnv) expectDropCounts(tables, rules int64) {
	env.expect("SELECT count\\(\\*\\) FROM multigres.tablegroup_table", []string{"count"}, [][]any{{tables}})
	env.expect("SELECT count\\(\\*\\) FROM multigres.routing_rule", []string{"count"}, [][]any{{rules}})
}

func TestDropTableGroup_ActiveRecordsADrop(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectLock()
	env.dropLookup([]any{testTableGroupID, tableGroupStateActive})
	env.expectDropCounts(0, 0)
	env.expect("UPDATE multigres.tablegroup SET state", nil, nil)
	env.expect("INSERT INTO multigres.operation", nil, nil)
	env.expectCatalogRows()
	env.expectAdvance(5)
	env.expectCommit()

	resp, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
	require.NoError(t, err)
	assert.Equal(t, testTableGroupID, resp.TablegroupId)
	assert.False(t, resp.NotFound)
	assert.False(t, resp.CancelledCreation)
	_, err = uuid.Parse(resp.OperationId)
	require.NoError(t, err, "operation id is a UUID")
	// Moving the tablegroup to dropping changes the snapshot, so the write is
	// checked and the routing version advances, as for a creation.
	assert.Equal(t, []string{
		"SELECT pg_is_in_recovery()",
		"SELECT pg_is_in_recovery()",
		"SELECT routing_version, plan_epoch FROM multigres.catalog_state FOR UPDATE",
		"SELECT tablegroup_id, state FROM multigres.tablegroup WHERE name = $1 FOR UPDATE",
		"SELECT count(*) FROM multigres.tablegroup_table WHERE tablegroup_id = $1",
		"SELECT count(*) FROM multigres.routing_rule WHERE from_tablegroup_id = $1 OR to_tablegroup_id = $1",
		"UPDATE multigres.tablegroup SET state = $2 WHERE tablegroup_id = $1 AND state = $3",
		"INSERT INTO multigres.operation (operation_id, kind, state, request) VALUES ($1, $2, $3, $4::jsonb)",
		snapshotTableGroupsQuery, snapshotShardsQuery, snapshotTablesQuery,
		snapshotShardIndicesQuery, snapshotShardKeysQuery, snapshotShardKeyColumnsQuery, selectRoutingRulesSQL,
		"UPDATE multigres.catalog_state SET routing_version = routing_version + 1 RETURNING routing_version, plan_epoch",
		"COMMIT",
	}, env.queries)
}

func TestDropTableGroup_ActiveInUseIsRefused(t *testing.T) {
	// A tablegroup holding tables, or one a routing rule still sends traffic
	// from or to, stays active: dropping it would tear down shards in use, and
	// the rows referencing it would keep the drop from ever finishing.
	cases := []struct {
		name          string
		tables, rules int64
		contains      string
	}{
		{"with tables", 3, 0, "contains 3 table(s)"},
		{"named by routing rules", 0, 2, "is the source or target of 2 routing rule(s)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newCatalogTestEnv(t)
			env.expectLock()
			env.dropLookup([]any{testTableGroupID, tableGroupStateActive})
			env.expectDropCounts(c.tables, c.rules)

			_, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
			require.Error(t, err)
			assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
			assert.Contains(t, err.Error(), c.contains)
			assert.Empty(t, env.queriesMatching("UPDATE multigres"), "nothing is changed")
			assert.Empty(t, env.queriesMatching("COMMIT"), "nothing is committed")
		})
	}
}

func TestDropTableGroup_CreatingCancelsTheCreation(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectLock()
	env.dropLookup([]any{testTableGroupID, tableGroupStateCreating})
	env.expect("UPDATE multigres.operation SET cancel_requested = true", []string{"operation_id"}, [][]any{{testOpID}})

	resp, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
	require.NoError(t, err)
	assert.True(t, resp.CancelledCreation)
	assert.Equal(t, testOpID, resp.OperationId, "the caller follows the creation")
	assert.Equal(t, testTableGroupID, resp.TablegroupId)
	assert.Empty(t, env.queriesMatching("INSERT"), "no drop operation is recorded")
	assert.Empty(t, env.queriesMatching("routing_version + 1"), "only operation rows change: no version advance")
}

func TestDropTableGroup_DroppingReturnsTheExistingOperation(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectLock()
	env.dropLookup([]any{testTableGroupID, tableGroupStateDropping})
	env.expect("SELECT operation_id FROM multigres.operation", []string{"operation_id"}, [][]any{{testOpID}})

	resp, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
	require.NoError(t, err)
	assert.Equal(t, testOpID, resp.OperationId)
	assert.Empty(t, env.queriesMatching("INSERT"))
}

func TestDropTableGroup_DroppingAfterAFailedCreationRecordsOne(t *testing.T) {
	// A failed creation leaves a dropping tablegroup with no drop operation.
	env := newCatalogTestEnv(t)
	env.expectLock()
	env.dropLookup([]any{testTableGroupID, tableGroupStateDropping})
	env.expect("SELECT operation_id FROM multigres.operation", []string{"operation_id"}, nil)
	env.expect("INSERT INTO multigres.operation", nil, nil)

	resp, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
	require.NoError(t, err)
	_, err = uuid.Parse(resp.OperationId)
	require.NoError(t, err)
	assert.Len(t, env.queriesMatching("INSERT"), 1, "a drop operation is recorded for the caller to wait on")
	assert.Empty(t, env.queriesMatching("routing_version + 1"), "the tablegroup was already dropping: no version advance")
}

func TestDropTableGroup_Missing(t *testing.T) {
	t.Run("is not found", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectLock()
		env.dropLookup()
		_, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_NOT_FOUND, mterrors.Code(err))
	})
	t.Run("is a no-op with IF EXISTS", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectLock()
		env.dropLookup()
		resp, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce", IfExists: true})
		require.NoError(t, err)
		assert.True(t, resp.NotFound)
		assert.Empty(t, resp.OperationId)
	})
	t.Run("a pinned drop treats another incarnation of the name as missing", func(t *testing.T) {
		// The name was recreated by someone else since the caller's first
		// drop: the pinned drop leaves the new tablegroup alone.
		env := newCatalogTestEnv(t)
		env.expectLock()
		env.dropLookup([]any{testTableGroupID + 1, tableGroupStateCreating})
		_, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce", TablegroupId: testTableGroupID})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_NOT_FOUND, mterrors.Code(err))
		assert.Empty(t, env.queriesMatching("UPDATE multigres"), "the other incarnation's creation is not cancelled")

		resp, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce", IfExists: true, TablegroupId: testTableGroupID})
		require.NoError(t, err)
		assert.True(t, resp.NotFound)
	})
	t.Run("a pinned drop of the right incarnation proceeds", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectLock()
		env.dropLookup([]any{testTableGroupID, tableGroupStateCreating})
		env.expect("UPDATE multigres.operation SET cancel_requested = true", []string{"operation_id"}, [][]any{{testOpID}})
		resp, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce", TablegroupId: testTableGroupID})
		require.NoError(t, err)
		assert.True(t, resp.CancelledCreation)
		assert.Equal(t, testOpID, resp.OperationId)
	})
}

func TestDropTableGroup_Rejects(t *testing.T) {
	t.Run("the default tablegroup, before any catalog access", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		_, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "default"})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
		assert.Equal(t, []string{"SELECT pg_is_in_recovery()"}, env.queries, "no catalog transaction is opened")
	})
	t.Run("not the catalog shard", func(t *testing.T) {
		env := newTableGroupTestEnv(t, "commerce", "0-80")
		_, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "other"})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	})
	t.Run("not the elected primary", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.pm.stateManager = NewStateManager(env.pm.logger, env.pm.record, nilConsensusStatus)
		env.pm.stateManager.pgMode = pgmode.Primary
		env.expectPrimary()
		_, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	})
}

func droppingOperationRow(state string) []any {
	return []any{
		testOpID, operationKindDropTableGroup, state,
		`{"tablegroup_id":7,"name":"commerce"}`, false, "", testCreatedAt, testCreatedAt,
	}
}

func TestGetOperation_Drop(t *testing.T) {
	t.Run("reports the poolers still registered and the claim", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expect("FROM multigres.operation WHERE operation_id", operationColumnsList(), [][]any{droppingOperationRow(operationStateDropping)})
		// A shut-down record of this incarnation still counts; another incarnation's does not.
		env.addPooler(t, "cell1", "commerce-0-inf-old", "commerce", "0-inf", testTableGroupID,
			clustermetadatapb.RoutingRole_ROUTING_ROLE_UNKNOWN, clustermetadatapb.PoolerServingStatus_DISABLED, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN)
		env.addServingPrimary(t, "cell2", "commerce", "0-inf", testTableGroupID+1)
		require.NoError(t, env.ts.ClaimTableGroupProvisioning(t.Context(), testTableGroupDB,
			&clustermetadatapb.TableGroupProvisionClaim{Provisioner: "operator", TablegroupId: testTableGroupID, TablegroupName: "commerce"}))

		op, err := env.pm.GetOperation(t.Context(), testOpID)
		require.NoError(t, err)
		assert.Equal(t, multipoolermanagerdatapb.OperationKind_OPERATION_KIND_DROP_TABLEGROUP, op.Kind)
		assert.Equal(t, multipoolermanagerdatapb.OperationState_OPERATION_STATE_DROPPING, op.State)
		assert.Equal(t, testTableGroupID, op.TablegroupId)
		assert.Equal(t, "commerce", op.TablegroupName)
		assert.Equal(t, int32(1), op.PoolersRegistered)
		assert.True(t, op.ClaimHeld)
		assert.Zero(t, op.ShardsDesired)
	})
	t.Run("done reads nothing from topology", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expect("FROM multigres.operation WHERE operation_id", operationColumnsList(), [][]any{droppingOperationRow(operationStateDone)})
		op, err := env.pm.GetOperation(t.Context(), testOpID)
		require.NoError(t, err)
		assert.Equal(t, multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE, op.State)
		assert.Zero(t, op.PoolersRegistered)
		assert.False(t, op.ClaimHeld)
	})
}

func TestCatalogObjects_OperationsViewAndCancel(t *testing.T) {
	var view, fn string
	for _, s := range catalogTables {
		switch {
		case strings.Contains(s, "CREATE VIEW multigres.operations"):
			view = s
		case strings.Contains(s, "CREATE FUNCTION multigres.cancel_operation"):
			fn = s
		}
	}
	require.NotEmpty(t, view)
	require.NotEmpty(t, fn)
	assert.Contains(t, view, "IF to_regclass('multigres.operations') IS NULL", "created once; CREATE OR REPLACE locks the view on every open")
	assert.Contains(t, fn, "IF to_regprocedure('multigres.cancel_operation(uuid)') IS NULL")
	assert.Contains(t, fn, "REVOKE EXECUTE ON FUNCTION multigres.cancel_operation(uuid) FROM PUBLIC", "functions are executable by PUBLIC by default")
	for _, col := range []string{"tablegroup_id", "tablegroup_name", "shards", "cancel_requested", "last_error"} {
		assert.Contains(t, view, col)
	}
	assert.NotContains(t, view, "GRANT", "superusers only, like the statements")
	assert.Contains(t, fn, "SET cancel_requested = true")
	assert.Contains(t, fn, "kind = 'create_tablegroup' AND state = 'creating'", "only a pending creation can be cancelled")
	assert.Contains(t, fn, "RETURNS boolean")
}

func TestDropTableGroup_EveryStatementIsChecked(t *testing.T) {
	// Each statement of the drop transaction can fail; the failure is
	// returned and nothing commits.
	cases := []struct {
		name, state, failing string
	}{
		{"lookup", tableGroupStateActive, "FROM multigres.tablegroup WHERE name"},
		{"table count", tableGroupStateActive, "FROM multigres.tablegroup_table"},
		{"routing rule count", tableGroupStateActive, "FROM multigres.routing_rule"},
		{"marking dropping", tableGroupStateActive, "UPDATE multigres.tablegroup SET state"},
		{"recording the drop", tableGroupStateActive, "INSERT INTO multigres.operation"},
		{"cancelling the creation", tableGroupStateCreating, "SET cancel_requested = true"},
		{"finding the drop operation", tableGroupStateDropping, "SELECT operation_id FROM multigres.operation"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newCatalogTestEnv(t)
			env.expectLock()
			env.qs.AddQueryPatternWithError(c.failing, errors.New("lock timeout"))
			env.dropLookup([]any{testTableGroupID, c.state})
			env.expectDropCounts(0, 0)
			env.expect("UPDATE multigres.tablegroup SET state", nil, nil)
			env.expect("INSERT INTO multigres.operation", nil, nil)
			_, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
			require.ErrorContains(t, err, "lock timeout")
			assert.Empty(t, env.queriesMatching("COMMIT"))
		})
	}
}

func TestDropTableGroup_ACorruptCatalogIsAnInternalError(t *testing.T) {
	t.Run("a creating tablegroup without its creation", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectLock()
		env.dropLookup([]any{testTableGroupID, tableGroupStateCreating})
		env.expect("UPDATE multigres.operation SET cancel_requested = true", []string{"operation_id"}, nil)
		_, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
		assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(err))
	})
	t.Run("a dropping tablegroup with two drops", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectLock()
		env.dropLookup([]any{testTableGroupID, tableGroupStateDropping})
		env.expect("SELECT operation_id FROM multigres.operation", []string{"operation_id"}, [][]any{{testOpID}, {testDropOpID}})
		_, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
		assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(err))
	})
	t.Run("an unknown state", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectLock()
		env.dropLookup([]any{testTableGroupID, "hibernating"})
		_, err := env.pm.DropTableGroup(t.Context(), &multipoolermanagerdatapb.DropTableGroupRequest{Name: "commerce"})
		assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(err))
	})
}
