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
	"context"
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
		"UPDATE multigres.operation SET state = $2, last_error = NULL, updated_at = now() WHERE operation_id = $1 AND state = $3 AND NOT cancel_requested RETURNING operation_id",
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
	// another pass, or cancelled since this pass read it: no further write,
	// no error. A cancelled one is failed by the next pass.
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
	// A cancelled creation fails and its tablegroup moves to dropping. A pass
	// that lists it dropping (the next one, in practice) deletes it once no
	// pooler record and no claim hold it. No drop operation exists for it.
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
	ops := env.queriesMatching("UPDATE multigres.operation")
	require.Len(t, ops, 1)
	assert.Contains(t, ops[0], "last_error = NULL", "completed, not failed")
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
	require.NoError(t, env.pm.tableGroupOperationsPass(t.Context()))
	assert.Empty(t, env.queries, "a non-primary reads nothing")
}

func TestRunTableGroupOperations_KeepsGoingPastAFailedPass(t *testing.T) {
	// The catalog is unreachable: every pass fails and the loop keeps trying
	// at its interval until it is cancelled.
	env := newCatalogTestEnv(t)
	env.qs.AddQueryPatternWithError("FROM multigres.operation WHERE kind", errors.New("catalog unavailable"))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		env.pm.runTableGroupOperations(ctx)
	}()
	require.Eventually(t, func() bool { return env.qs.AdminQueryCount() >= 1 }, 5*time.Second, 10*time.Millisecond)
	cancel()
	<-done
}

func TestReconcileTableGroups_IdlePassReadsNoTopology(t *testing.T) {
	// With nothing pending the pass is the two listings and nothing else:
	// topology is not read, so a broken topology costs an idle catalog nothing.
	env := newCatalogTestEnv(t)
	env.topo.SetError(errors.New("etcd unreachable"))
	env.expectCreations()
	env.expectDroppings()

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Zero(t, pending)
	assert.Len(t, env.queries, 2)
}

func TestReconcileTableGroups_UnreadableRowsAreSkipped(t *testing.T) {
	// A row the loop cannot read is logged and left out, not counted as
	// pending and not acted on; the readable rows still are.
	env := newCatalogTestEnv(t)
	bad := creatingOperationRow(time.Now().UnixMicro(), false)
	bad[3] = "{not json"
	env.expectCreations(bad)
	env.expectDroppings([]any{"not-a-number", "commerce"})

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err)
	assert.Zero(t, pending)
	assert.Len(t, env.queries, 2, "nothing written for rows that cannot be read")
}

func TestReconcileTableGroups_AListingFailureEndsThePass(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.qs.AddQueryPatternWithError("FROM multigres.operation WHERE kind", errors.New("catalog unavailable"))
	env.expectDroppings([]any{testTableGroupID, "commerce"})

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.ErrorContains(t, err, "failed to list pending tablegroup creations")
	assert.Zero(t, pending)
	assert.Empty(t, env.queriesMatching("DELETE"), "a pass that cannot see the whole catalog deletes nothing")
}

func TestReconcileTableGroups_ACommitFailureKeepsTheCreationPending(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectCreations(creatingOperationRow(time.Now().UnixMicro(), false))
	env.addServingPrimary(t, "cell1", "commerce", "0-80", testTableGroupID)
	env.addServingPrimary(t, "cell2", "commerce", "80-inf", testTableGroupID)
	env.expect("UPDATE multigres.operation SET state", []string{"operation_id"}, [][]any{{testOpID}})
	env.expect("UPDATE multigres.shard SET state", nil, nil)
	env.expect("UPDATE multigres.tablegroup SET state", nil, nil)
	env.expect("UPDATE multigres.catalog_state SET routing_version", nil, nil)
	env.qs.AddQueryPatternWithError("COMMIT", errors.New("connection lost"))
	env.expectDroppings()

	pending, err := env.pm.reconcileTableGroups(t.Context())
	require.NoError(t, err, "one creation's failure does not fail the pass")
	assert.Equal(t, 1, pending, "the creation is retried next pass")
}

func TestReconcileTableGroups_AStatementFailureRollsBackAndRetries(t *testing.T) {
	t.Run("creation", func(t *testing.T) {
		// The operation flipped but the shard update fails: the transaction
		// rolls back, nothing after it runs, and the creation stays pending.
		env := newCatalogTestEnv(t)
		env.expectCreations(creatingOperationRow(time.Now().UnixMicro(), false))
		env.addServingPrimary(t, "cell1", "commerce", "0-80", testTableGroupID)
		env.addServingPrimary(t, "cell2", "commerce", "80-inf", testTableGroupID)
		env.expect("UPDATE multigres.operation SET state", []string{"operation_id"}, [][]any{{testOpID}})
		env.qs.AddQueryPatternWithError("UPDATE multigres.shard SET state", errors.New("lock timeout"))
		env.expect("ROLLBACK", nil, nil)
		env.expectDroppings()

		pending, err := env.pm.reconcileTableGroups(t.Context())
		require.NoError(t, err)
		assert.Equal(t, 1, pending)
		assert.Empty(t, env.queriesMatching("routing_version"), "nothing past the failed statement ran")
		assert.Len(t, env.queriesMatching("ROLLBACK"), 1)
	})
	t.Run("deletion", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectCreations()
		env.expectDroppings([]any{testTableGroupID, "commerce"})
		env.qs.AddQueryPatternWithError("DELETE FROM multigres.shard", errors.New("lock timeout"))
		env.expect("ROLLBACK", nil, nil)

		pending, err := env.pm.reconcileTableGroups(t.Context())
		require.NoError(t, err)
		assert.Equal(t, 1, pending)
		assert.Empty(t, env.queriesMatching("DELETE FROM multigres.tablegroup"))
	})
}

func TestRunTableGroupOperations_PassesUntilCancelled(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectCreations()
	env.expectDroppings()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		env.pm.runTableGroupOperations(ctx)
	}()
	// The first pass runs immediately: both listings, nothing else.
	require.Eventually(t, func() bool { return env.qs.AdminQueryCount() >= 2 }, 5*time.Second, 10*time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop on cancel")
	}
}

func TestStartTableGroupOperations(t *testing.T) {
	t.Run("the catalog shard runs a pass", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectCreations()
		env.expectDroppings()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		env.pm.ctx = ctx
		env.pm.StartTableGroupOperations()
		env.pm.StartTableGroupOperations() // idempotent
		assert.True(t, env.pm.tableGroupOpsEnabled)
		require.Eventually(t, func() bool { return env.qs.AdminQueryCount() >= 2 }, 5*time.Second, 10*time.Millisecond)
	})
	t.Run("another shard has no catalog to run against", func(t *testing.T) {
		env := newTableGroupTestEnv(t, "commerce", "0-80")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		env.pm.ctx = ctx
		env.pm.StartTableGroupOperations()
		assert.True(t, env.pm.tableGroupOpsEnabled, "the opt-in is remembered for a later open")
		time.Sleep(50 * time.Millisecond)
		assert.Zero(t, env.qs.AdminQueryCount(), "no pass ran")
	})
}
