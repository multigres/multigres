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

package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/tools/telemetry"
)

const routingTestDatabase = "db1"

// newRoutingTestManager returns a manager of routingTestDatabase's shard
// tableGroup/shard whose role is PRIMARY when primary is set, and its mock
// query service.
func newRoutingTestManager(t *testing.T, tableGroup, shard string, primary bool) (*MultipoolerManager, *mock.QueryService) {
	t.Helper()
	pm, m := newTestManagerWithMock(t, tableGroup, shard)
	pm.record = newRecordFromProto(&clustermetadatapb.Multipooler{
		Id:       pm.serviceID,
		ShardRef: &clustermetadatapb.ShardRef{Database: routingTestDatabase, TableGroup: tableGroup, Shard: shard},
	})
	pm.stateManager = NewStateManager(pm.logger, pm.record, selfLeaderConsensusStatus)
	if primary {
		pm.stateManager.pgMode = pgmode.Primary
	}
	return pm, m
}

func TestRequireCatalogPrimary(t *testing.T) {
	tests := []struct {
		name       string
		tableGroup string
		primary    bool
		database   string
		code       mtrpcpb.Code
	}{
		{name: "catalog primary", tableGroup: constants.DefaultTableGroup, primary: true, database: routingTestDatabase},
		{name: "no database", tableGroup: constants.DefaultTableGroup, primary: true, code: mtrpcpb.Code_INVALID_ARGUMENT},
		{name: "another database", tableGroup: constants.DefaultTableGroup, primary: true, database: "db2", code: mtrpcpb.Code_INVALID_ARGUMENT},
		{name: "catalog standby", tableGroup: constants.DefaultTableGroup, database: routingTestDatabase, code: mtrpcpb.Code_UNAVAILABLE},
		{name: "primary of another tablegroup", tableGroup: "tg1", primary: true, database: routingTestDatabase, code: mtrpcpb.Code_UNAVAILABLE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, _ := newRoutingTestManager(t, tt.tableGroup, constants.DefaultShard, tt.primary)
			err := pm.requireCatalogPrimary(tt.database)
			if tt.code == mtrpcpb.Code_OK {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tt.code, mterrors.Code(err), "error: %v", err)
		})
	}
}

// Every rule RPC is refused by a pooler that is not the catalog primary
// before it opens a transaction.
func TestRoutingRPCs_OnlyOnCatalogPrimary(t *testing.T) {
	pm, m := newRoutingTestManager(t, constants.DefaultTableGroup, constants.DefaultShard, false)
	began := false
	m.AddQueryPatternWithCallback("^BEGIN$", mock.MakeQueryResult(nil, nil), func(string) { began = true })
	ctx := context.Background()

	_, err := pm.SetRoutingRules(ctx, &multipoolermanagerdatapb.SetRoutingRulesRequest{
		Database: routingTestDatabase, OperationId: importOp,
		Rules: []*multipoolermanagerdatapb.ShardRoutingRuleSpec{{FromTablegroup: "default", ToTablegroup: "srctg", ToShard: "0-inf"}},
	})
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "SetRoutingRules: %v", err)
	_, err = pm.DeleteRoutingRules(ctx, &multipoolermanagerdatapb.DeleteRoutingRulesRequest{Database: routingTestDatabase, OperationId: importOp})
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "DeleteRoutingRules: %v", err)

	assert.False(t, began, "a refused routing RPC must not open a transaction")
}

// A pooler whose routing role is still PRIMARY but whose postgres is already
// in recovery, as during a demotion, passes requireCatalogPrimary and is then
// refused by writeCatalog. It answers UNAVAILABLE, like any other pooler that
// is not the catalog primary, so the caller retries on the new primary rather
// than treating it as a refused change.
func TestRoutingRPCs_DemotedMidRequestIsUnavailable(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expect("SELECT pg_is_in_recovery", []string{"pg_is_in_recovery"}, [][]any{{"t"}})

	_, err := env.pm.SetRoutingRules(t.Context(), &multipoolermanagerdatapb.SetRoutingRulesRequest{
		Database: testTableGroupDB, OperationId: importOp, Rules: importRuleSpec, ExpectedRoutingVersion: 5,
	})
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "error: %v", err)
	assert.Equal(t, []string{"SELECT pg_is_in_recovery()"}, env.queries, "refused by writeCatalog's check, before anything is locked")
}

// importRuleSpec is the import's rule from the default tablegroup to srctg.
var importRuleSpec = []*multipoolermanagerdatapb.ShardRoutingRuleSpec{{FromTablegroup: "default", ToTablegroup: "srctg", ToShard: "0-inf"}}

func TestSetRoutingRules_WritesThroughWriteCatalog(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectRoutingCatalog()
	env.expect("SET routing_version = routing_version \\+ 1 RETURNING", []string{"routing_version", "plan_epoch"}, [][]any{{int64(6), int64(2)}})

	resp, err := env.pm.SetRoutingRules(t.Context(), &multipoolermanagerdatapb.SetRoutingRulesRequest{
		Database: testTableGroupDB, OperationId: importOp, Rules: importRuleSpec, ExpectedRoutingVersion: 5,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(6), resp.GetRoutingVersion())
	assert.Equal(t, "SELECT pg_is_in_recovery()", env.queries[0], "writeCatalog checks for the catalog primary first")
	assert.Contains(t, env.queries[1], "FROM multigres.catalog_state FOR UPDATE", "the catalog_state lock comes before any other catalog row")
	assert.Len(t, env.queriesMatching("INSERT INTO multigres.routing_rule"), 1)
	assert.Equal(t, "COMMIT", env.queries[len(env.queries)-1])
}

func TestSetRoutingRules_RejectedChangeDoesNotCommit(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectRoutingCatalog()

	_, err := env.pm.SetRoutingRules(t.Context(), &multipoolermanagerdatapb.SetRoutingRulesRequest{
		Database: testTableGroupDB, OperationId: importOp, Rules: importRuleSpec, ExpectedRoutingVersion: 4,
	})
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "error: %v", err)
	assert.Empty(t, env.queriesMatching("INSERT INTO"))
	assert.Empty(t, env.queriesMatching("COMMIT"))
}

func TestSetRoutingRules_RequiresRules(t *testing.T) {
	pm, _ := newRoutingTestManager(t, constants.DefaultTableGroup, constants.DefaultShard, true)
	_, err := pm.SetRoutingRules(context.Background(), &multipoolermanagerdatapb.SetRoutingRulesRequest{
		Database: routingTestDatabase, OperationId: importOp,
	})
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
}

func TestDeleteRoutingRules_DeletesAndAdvances(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectRoutingCatalog([]any{7, importOp, routingScopeTablegroup, 1, nil, 2, 2, nil, 5})
	env.expect("SET routing_version = routing_version \\+ 1 RETURNING", []string{"routing_version", "plan_epoch"}, [][]any{{int64(6), int64(2)}})

	resp, err := env.pm.DeleteRoutingRules(t.Context(), &multipoolermanagerdatapb.DeleteRoutingRulesRequest{
		Database: testTableGroupDB, OperationId: importOp, ExpectedRoutingVersion: 5,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(6), resp.GetRoutingVersion())
	assert.Len(t, env.queriesMatching("DELETE FROM multigres.routing_rule"), 1)
	assert.Empty(t, env.queriesMatching("INSERT INTO multigres.routing_rule"))
	assert.Equal(t, "COMMIT", env.queries[len(env.queries)-1])
}

// The catalog primary records the rule count whenever it serves a full
// snapshot.
func TestGetRoutingSnapshot_RecordsTheRuleCount(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))
	env := newCatalogTestEnv(t)
	var err error
	env.pm.metrics, err = newManagerMetrics()
	require.NoError(t, err)
	env.expectPrimary()
	env.expectSnapshotRead(9, 2)
	env.expectCatalogRows()

	_, err = env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(8))
	require.NoError(t, err)

	gauge, ok := findMetric(t, setup.MetricReader, "mg.pooler.routing.rules").Data.(metricdata.Gauge[int64])
	require.True(t, ok)
	require.Len(t, gauge.DataPoints, 1)
	assert.Equal(t, testTableGroupDB, attrValue(t, gauge.DataPoints[0].Attributes, "db.namespace"))
	assert.Equal(t, int64(2), gauge.DataPoints[0].Value)
}
