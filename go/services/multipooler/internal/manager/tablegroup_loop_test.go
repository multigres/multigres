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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// dropCols is the column list of the loop's dropping-tablegroups query.
var dropCols = []string{"tablegroup_id", "name"}

// expectCreations answers the loop's pending-creations query with rows.
func (env *tableGroupTestEnv) expectCreations(rows ...[]any) {
	env.expect("FROM multigres.operation WHERE kind = \\$1 AND state = \\$2 ORDER BY created_at", operationColumnsList(), rows)
}

// expectDroppings answers the loop's dropping-tablegroups query with rows.
func (env *tableGroupTestEnv) expectDroppings(rows ...[]any) {
	env.expect("FROM multigres.tablegroup WHERE state = \\$1 ORDER BY tablegroup_id", dropCols, rows)
}

// queriesMatching returns env.queries that contain substr, in order.
func (env *tableGroupTestEnv) queriesMatching(substr string) []string {
	var out []string
	for _, q := range env.queries {
		if strings.Contains(q, substr) {
			out = append(out, q)
		}
	}
	return out
}

func TestReconcileTableGroups_CompletesACreation(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectCreations(creatingOperationRow(time.Now().UnixMicro(), false))
	env.addServingPrimary(t, "cell1", "commerce", "0-80", testTableGroupID)
	env.addServingPrimary(t, "cell2", "commerce", "80-inf", testTableGroupID)
	env.expect("UPDATE multigres.operation SET state", []string{"operation_id"}, [][]any{{testOpID}})
	env.expect("UPDATE multigres.shard SET state", nil, nil)
	env.expect("UPDATE multigres.tablegroup SET state", nil, nil)
	env.expect("UPDATE multigres.catalog_state SET routing_version", nil, nil)
	env.expectDroppings()

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Zero(t, pending)
	assert.Equal(t, []string{
		"UPDATE multigres.operation SET state = $2, last_error = NULL, updated_at = now() WHERE operation_id = $1 AND state = $3 RETURNING operation_id",
		"UPDATE multigres.shard SET state = $2 WHERE tablegroup_id = $1 AND state = $3",
		"UPDATE multigres.tablegroup SET state = $2 WHERE tablegroup_id = $1 AND state = $3",
		"UPDATE multigres.catalog_state SET routing_version = routing_version + 1",
	}, env.queriesMatching("UPDATE"))
}

func TestReconcileTableGroups_WaitsForEveryShard(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectCreations(creatingOperationRow(time.Now().UnixMicro(), false))
	env.addServingPrimary(t, "cell1", "commerce", "0-80", testTableGroupID)
	// The other shard has a replica only, and a primary of another incarnation.
	env.addPooler(t, "cell2", "commerce-80-inf-replica", "commerce", "80-inf", testTableGroupID,
		clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE)
	env.addPooler(t, "cell2", "commerce-80-inf-stale", "commerce", "80-inf", testTableGroupID+1,
		clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE)
	env.expectDroppings()

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, pending)
	assert.Empty(t, env.queriesMatching("UPDATE"), "nothing is written while a shard is missing")
}

func TestReconcileTableGroups_ACompletedOperationIsLeftAlone(t *testing.T) {
	// The compare-and-set finds the operation already done or failed by
	// another pass: no further write, no error.
	env := newCatalogTestEnv(t)
	env.expectCreations(creatingOperationRow(time.Now().UnixMicro(), false))
	env.addServingPrimary(t, "cell1", "commerce", "0-80", testTableGroupID)
	env.addServingPrimary(t, "cell1", "commerce", "80-inf", testTableGroupID)
	env.expect("UPDATE multigres.operation SET state", []string{"operation_id"}, nil)
	env.expectDroppings()

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Zero(t, pending)
	assert.Len(t, env.queriesMatching("UPDATE"), 1)
}

func TestReconcileTableGroups_FailsACancelledCreationAndDropsIt(t *testing.T) {
	// A cancelled creation fails and its tablegroup moves to dropping; in the
	// same pass, with no pooler record and no claim holding it, the dropping
	// tablegroup is deleted. No drop operation exists for it.
	env := newCatalogTestEnv(t)
	env.expectCreations(creatingOperationRow(time.Now().UnixMicro(), true))
	env.expect("UPDATE multigres.operation SET state", []string{"operation_id"}, [][]any{{testOpID}})
	env.expect("UPDATE multigres.tablegroup SET state", nil, nil)
	env.expectDroppings([]any{testTableGroupID, "commerce"})
	env.expect("DELETE FROM multigres.shard", nil, nil)
	env.expect("DELETE FROM multigres.tablegroup", []string{"tablegroup_id"}, [][]any{{testTableGroupID}})
	env.expect("UPDATE multigres.operation SET state = \\$3", nil, nil)
	env.expect("UPDATE multigres.catalog_state SET routing_version", nil, nil)

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Zero(t, pending)
	writes := env.queriesMatching("UPDATE")
	writes = append(writes, env.queriesMatching("DELETE")...)
	assert.Contains(t, writes[0], "state = $2, last_error = $3", "the operation fails with a reason")
	assert.Contains(t, writes[1], "UPDATE multigres.tablegroup SET state", "the tablegroup moves to dropping")
	assert.Contains(t, env.queries, "DELETE FROM multigres.tablegroup WHERE tablegroup_id = $1 AND state = $2 RETURNING tablegroup_id")
	assert.Contains(t, env.queries, "UPDATE multigres.catalog_state SET routing_version = routing_version + 1")
}

func TestReconcileTableGroups_ATimedOutCreationThatServesCompletes(t *testing.T) {
	// The serving check comes before the timeout: a slow creation whose last
	// shard came up is activated, not torn down.
	env := newCatalogTestEnv(t)
	old := time.Now().Add(-createTableGroupTimeout - time.Minute).UnixMicro()
	env.expectCreations(creatingOperationRow(old, false))
	env.addServingPrimary(t, "cell1", "commerce", "0-80", testTableGroupID)
	env.addServingPrimary(t, "cell2", "commerce", "80-inf", testTableGroupID)
	env.expect("UPDATE multigres.operation SET state", []string{"operation_id"}, [][]any{{testOpID}})
	env.expect("UPDATE multigres.shard SET state", nil, nil)
	env.expect("UPDATE multigres.tablegroup SET state", nil, nil)
	env.expect("UPDATE multigres.catalog_state SET routing_version", nil, nil)
	env.expectDroppings()

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Zero(t, pending)
	assert.Contains(t, env.queries[1], "last_error = NULL", "completed, not failed")
}

func TestReconcileTableGroups_ATopologyErrorIsRecordedThenCleared(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.topo.SetError(errors.New("etcd unreachable"))
	env.expectCreations(creatingOperationRow(time.Now().UnixMicro(), false))
	env.expect("UPDATE multigres.operation SET last_error = \\$2", nil, nil)
	env.expectDroppings([]any{testTableGroupID + 1, "old"})
	env.expect("UPDATE multigres.operation SET last_error = \\$3", nil, nil)

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, pending, "nothing completes or is deleted on a failed topology read")
	assert.Len(t, env.queriesMatching("SET last_error ="), 2, "the error is recorded on the creation and on the drop")
	assert.Empty(t, env.queriesMatching("DELETE"))

	// Topology is back, the shards are still not all serving: the recorded
	// error is cleared and the creation stays pending. (A fresh environment:
	// the mock answers a query with the first pattern registered for it.)
	env = newCatalogTestEnv(t)
	row := creatingOperationRow(time.Now().UnixMicro(), false)
	row[5] = "list cells: etcd unreachable"
	env.expectCreations(row)
	env.expect("UPDATE multigres.operation SET last_error = NULL", nil, nil)
	env.expectDroppings()
	pending, err = env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, pending)
	assert.Contains(t, env.queries, "UPDATE multigres.operation SET last_error = NULL, updated_at = now() WHERE operation_id = $1 AND state = $2 AND last_error IS NOT NULL")
}

func TestReconcileTableGroups_FailsATimedOutCreation(t *testing.T) {
	env := newCatalogTestEnv(t)
	old := time.Now().Add(-createTableGroupTimeout - time.Minute).UnixMicro()
	env.expectCreations(creatingOperationRow(old, false))
	env.expect("UPDATE multigres.operation SET state", []string{"operation_id"}, [][]any{{testOpID}})
	env.expect("UPDATE multigres.tablegroup SET state", nil, nil)
	env.expectDroppings()

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Zero(t, pending)
	require.Len(t, env.queriesMatching("UPDATE multigres.operation"), 1)
}

func TestReconcileTableGroups_ADroppingTableGroupWaitsForItsPoolers(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectCreations()
	env.expectDroppings([]any{testTableGroupID, "commerce"})
	// A record of this incarnation, even shut down, still holds it; a record of
	// another incarnation of the name does not count.
	env.addPooler(t, "cell1", "commerce-old", "commerce", "0-inf", testTableGroupID,
		clustermetadatapb.RoutingRole_ROUTING_ROLE_UNKNOWN, clustermetadatapb.PoolerServingStatus_DISABLED, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN)

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, pending)
	assert.Empty(t, env.queriesMatching("DELETE"))
}

func TestReconcileTableGroups_ADroppingTableGroupWaitsForTheClaim(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectCreations()
	env.expectDroppings([]any{testTableGroupID, "commerce"})
	require.NoError(t, env.ts.ClaimTableGroupProvisioning(t.Context(), testTableGroupDB,
		&clustermetadatapb.TableGroupProvisionClaim{Provisioner: "operator", TablegroupId: testTableGroupID, TablegroupName: "commerce"}))

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, pending, "the provisioner still holds the tablegroup")
	assert.Empty(t, env.queriesMatching("DELETE"))

	// Once released, the next pass deletes it and completes its drop operation.
	// The listing patterns from above still answer, which is what we want.
	require.NoError(t, env.ts.ReleaseTableGroupProvisioning(t.Context(), testTableGroupDB, testTableGroupID))
	env.expect("DELETE FROM multigres.shard", nil, nil)
	env.expect("DELETE FROM multigres.tablegroup", []string{"tablegroup_id"}, [][]any{{testTableGroupID}})
	env.expect("UPDATE multigres.operation SET state = \\$3", nil, nil)
	env.expect("UPDATE multigres.catalog_state SET routing_version", nil, nil)
	pending, err = env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Zero(t, pending)
	assert.Len(t, env.queriesMatching("DELETE"), 2)
	assert.Contains(t, env.queries, "UPDATE multigres.operation SET state = $3, last_error = NULL, updated_at = now() WHERE kind = $1 AND state = $2 AND (request->>'tablegroup_id')::bigint = $4")
}

func TestReconcileTableGroups_ADeletedTableGroupIsLeftAlone(t *testing.T) {
	// Another pass deleted the tablegroup between the listing and the delete:
	// the conditional delete removes nothing and nothing else is written.
	env := newCatalogTestEnv(t)
	env.expectCreations()
	env.expectDroppings([]any{testTableGroupID, "commerce"})
	env.expect("DELETE FROM multigres.shard", nil, nil)
	env.expect("DELETE FROM multigres.tablegroup", []string{"tablegroup_id"}, nil)

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Zero(t, pending)
	assert.Empty(t, env.queriesMatching("routing_version"))
}

func TestTableGroupOperationsPass_OnlyTheElectedPrimaryRuns(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.pm.stateManager = NewStateManager(env.pm.logger, env.pm.record, nilConsensusStatus)
	assert.Zero(t, env.pm.tableGroupOperationsPass(t.Context()))
	assert.Empty(t, env.queries, "a non-primary reads nothing")
}
