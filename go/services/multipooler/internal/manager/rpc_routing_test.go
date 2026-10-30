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

// Every routing RPC is refused by a pooler that is not the catalog primary
// before it opens a transaction.
func TestRoutingRPCs_OnlyOnCatalogPrimary(t *testing.T) {
	pm, m := newRoutingTestManager(t, constants.DefaultTableGroup, constants.DefaultShard, false)
	began := false
	m.AddQueryPatternWithCallback("^BEGIN$", mock.MakeQueryResult(nil, nil), func(string) { began = true })
	ctx := context.Background()

	_, err := pm.GetRoutingSnapshot(ctx, &multipoolermanagerdatapb.GetRoutingSnapshotRequest{Database: routingTestDatabase})
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "GetRoutingSnapshot: %v", err)
	_, err = pm.SetRoutingRules(ctx, &multipoolermanagerdatapb.SetRoutingRulesRequest{
		Database: routingTestDatabase, OperationId: importOp,
		Rules: []*multipoolermanagerdatapb.ShardRoutingRuleSpec{{FromTablegroup: "default", ToTablegroup: "srctg", ToShard: "0-inf"}},
	})
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "SetRoutingRules: %v", err)
	_, err = pm.DeleteRoutingRules(ctx, &multipoolermanagerdatapb.DeleteRoutingRulesRequest{Database: routingTestDatabase, OperationId: importOp})
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "DeleteRoutingRules: %v", err)
	_, err = pm.EnsureImportTarget(ctx, &multipoolermanagerdatapb.EnsureImportTargetRequest{Database: routingTestDatabase, OperationId: importOp, Tablegroup: "srctg"})
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "EnsureImportTarget: %v", err)

	assert.False(t, began, "a refused routing RPC must not open a transaction")
}

func TestGetRoutingSnapshot_OneRepeatableReadSnapshot(t *testing.T) {
	pm, m := newRoutingTestManager(t, constants.DefaultTableGroup, constants.DefaultShard, true)
	var queries []string
	record := func(q string) { queries = append(queries, q) }
	m.AddQueryPatternWithCallback(exactPattern(selectRoutingStateSQL),
		mock.MakeQueryResult([]string{"routing_version", "plan_epoch"}, [][]any{{9, 3}}), record)
	m.AddQueryPatternWithCallback(exactPattern(selectSnapshotRulesSQL), mock.MakeQueryResult([]string{
		"rule_id", "operation_id", "scope", "from_tg", "from_schema", "from_table",
		"to_tg", "to_shard", "to_shard_key", "created_version",
	}, [][]any{
		{4, importOp, routingScopeTablegroup, "commerce", nil, nil, "commerce", "40-80", nil, 8},
		{5, otherOp, routingScopeTable, "commerce", "public", "orders", "orders", nil, "by_id", 9},
	}), record)
	m.AddQueryPatternWithCallback(".*", mock.MakeQueryResult(nil, nil), record)

	resp, err := pm.GetRoutingSnapshot(context.Background(), &multipoolermanagerdatapb.GetRoutingSnapshotRequest{Database: routingTestDatabase})
	require.NoError(t, err)

	assert.Equal(t, []string{"BEGIN", setSnapshotIsolationSQL, selectRoutingStateSQL, selectSnapshotRulesSQL, "COMMIT"}, queries)
	assert.Equal(t, int64(9), resp.GetRoutingVersion())
	assert.Equal(t, int64(3), resp.GetPlanEpoch())
	assert.True(t, resp.GetFull())
	require.Len(t, resp.GetRules(), 2)

	shardRule := resp.GetRules()[0]
	assert.Equal(t, int64(4), shardRule.GetRuleId())
	assert.Equal(t, importOp, shardRule.GetOperationId())
	assert.Equal(t, multipoolermanagerdatapb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP, shardRule.GetScope())
	assert.Equal(t, "commerce", shardRule.GetFromTablegroup())
	assert.Nil(t, shardRule.GetFromTable())
	assert.Equal(t, "40-80", shardRule.GetToShard())
	assert.Equal(t, []byte{0x40}, shardRule.GetToKeyRange().GetStart())
	assert.Equal(t, []byte{0x80}, shardRule.GetToKeyRange().GetEnd())
	assert.Equal(t, int64(8), shardRule.GetCreatedVersion())

	tableRule := resp.GetRules()[1]
	assert.Equal(t, multipoolermanagerdatapb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLE, tableRule.GetScope())
	assert.Equal(t, "public", tableRule.GetFromTable().GetSchema())
	assert.Equal(t, "orders", tableRule.GetFromTable().GetName())
	assert.Equal(t, "orders", tableRule.GetToTablegroup())
	assert.Empty(t, tableRule.GetToShard())
	assert.Nil(t, tableRule.GetToKeyRange())
	assert.Equal(t, "by_id", tableRule.GetToShardKey())
}

// addRoutingCatalog registers the catalog reads of a rule write: routing
// version 5, the default and srctg tablegroups with their 0-inf shards, no
// rules, no default tables and the import operation.
func addRoutingCatalog(m *mock.QueryService, record func(string)) {
	tx := newFakeRoutingTx()
	for _, sql := range []string{lockRoutingStateSQL, selectTablegroupsSQL, selectShardsSQL, selectRoutingRulesSQL, countDefaultTablesSQL, selectOperationSQL} {
		m.AddQueryPatternWithCallback(exactPattern(sql), tx.results[sql], record)
	}
}

func TestSetRoutingRules_ReadCommittedAndCommits(t *testing.T) {
	pm, m := newRoutingTestManager(t, constants.DefaultTableGroup, constants.DefaultShard, true)
	var queries []string
	record := func(q string) { queries = append(queries, q) }
	addRoutingCatalog(m, record)
	m.AddQueryPatternWithCallback(".*", mock.MakeQueryResult(nil, nil), record)

	resp, err := pm.SetRoutingRules(context.Background(), &multipoolermanagerdatapb.SetRoutingRulesRequest{
		Database:               routingTestDatabase,
		OperationId:            importOp,
		Rules:                  []*multipoolermanagerdatapb.ShardRoutingRuleSpec{{FromTablegroup: "default", ToTablegroup: "srctg", ToShard: "0-inf"}},
		ExpectedRoutingVersion: 5,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(6), resp.GetRoutingVersion())

	require.GreaterOrEqual(t, len(queries), 3)
	assert.Equal(t, []string{"BEGIN", setReadCommittedSQL, lockRoutingStateSQL}, queries[:3],
		"the isolation level is set first, then the catalog_state lock")
	assert.Equal(t, []string{deleteOperationRulesSQL, insertRoutingRuleSQL, bumpRoutingVersionSQL, "COMMIT"}, queries[len(queries)-4:])
}

func TestSetRoutingRules_RecordsTheRuleCount(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))
	pm, m := newRoutingTestManager(t, constants.DefaultTableGroup, constants.DefaultShard, true)
	var err error
	pm.metrics, err = newManagerMetrics()
	require.NoError(t, err)
	addRoutingCatalog(m, func(string) {})
	m.AddQueryPatternWithCallback(".*", mock.MakeQueryResult(nil, nil), func(string) {})

	_, err = pm.SetRoutingRules(context.Background(), &multipoolermanagerdatapb.SetRoutingRulesRequest{
		Database:               routingTestDatabase,
		OperationId:            importOp,
		Rules:                  []*multipoolermanagerdatapb.ShardRoutingRuleSpec{{FromTablegroup: "default", ToTablegroup: "srctg", ToShard: "0-inf"}},
		ExpectedRoutingVersion: 5,
	})
	require.NoError(t, err)

	gauge, ok := findMetric(t, setup.MetricReader, "mg.pooler.routing.rules").Data.(metricdata.Gauge[int64])
	require.True(t, ok)
	require.Len(t, gauge.DataPoints, 1)
	assert.Equal(t, routingTestDatabase, attrValue(t, gauge.DataPoints[0].Attributes, "db.namespace"))
	assert.Equal(t, int64(1), gauge.DataPoints[0].Value)
}

func TestSetRoutingRules_RejectedChangeDoesNotCommit(t *testing.T) {
	pm, m := newRoutingTestManager(t, constants.DefaultTableGroup, constants.DefaultShard, true)
	var queries []string
	record := func(q string) { queries = append(queries, q) }
	addRoutingCatalog(m, record)
	m.AddQueryPatternWithCallback(".*", mock.MakeQueryResult(nil, nil), record)

	_, err := pm.SetRoutingRules(context.Background(), &multipoolermanagerdatapb.SetRoutingRulesRequest{
		Database:               routingTestDatabase,
		OperationId:            importOp,
		Rules:                  []*multipoolermanagerdatapb.ShardRoutingRuleSpec{{FromTablegroup: "default", ToTablegroup: "srctg", ToShard: "0-inf"}},
		ExpectedRoutingVersion: 4,
	})
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	assert.Contains(t, err.Error(), "routing version is 5, expected 4")
	assert.NotContains(t, queries, "COMMIT")
	assert.Contains(t, queries, "ROLLBACK")
}

func TestSetRoutingRules_RequiresRules(t *testing.T) {
	pm, _ := newRoutingTestManager(t, constants.DefaultTableGroup, constants.DefaultShard, true)
	_, err := pm.SetRoutingRules(context.Background(), &multipoolermanagerdatapb.SetRoutingRulesRequest{
		Database: routingTestDatabase, OperationId: importOp, ExpectedRoutingVersion: 5,
	})
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
}

func TestDeleteRoutingRules_DeletesAndBumps(t *testing.T) {
	pm, m := newRoutingTestManager(t, constants.DefaultTableGroup, constants.DefaultShard, true)
	var queries []string
	record := func(q string) { queries = append(queries, q) }
	tx := newFakeRoutingTx([]any{7, importOp, routingScopeTablegroup, 1, nil, 2, 2, nil, 5})
	for _, sql := range []string{lockRoutingStateSQL, selectTablegroupsSQL, selectShardsSQL, selectRoutingRulesSQL, countDefaultTablesSQL, selectOperationSQL} {
		m.AddQueryPatternWithCallback(exactPattern(sql), tx.results[sql], record)
	}
	m.AddQueryPatternWithCallback(".*", mock.MakeQueryResult(nil, nil), record)

	resp, err := pm.DeleteRoutingRules(context.Background(), &multipoolermanagerdatapb.DeleteRoutingRulesRequest{
		Database: routingTestDatabase, OperationId: importOp, ExpectedRoutingVersion: 5,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(6), resp.GetRoutingVersion())
	assert.Equal(t, []string{deleteOperationRulesSQL, bumpRoutingVersionSQL, "COMMIT"}, queries[len(queries)-3:])
}

func TestEnsureImportTarget_ReadCommittedAndCommits(t *testing.T) {
	pm, m := newRoutingTestManager(t, constants.DefaultTableGroup, constants.DefaultShard, true)
	var queries []string
	record := func(q string) { queries = append(queries, q) }
	m.AddQueryPatternWithCallback(exactPattern(insertTablegroupSQL),
		mock.MakeQueryResult([]string{"tablegroup_id"}, [][]any{{2}}), record)
	m.AddQueryPatternWithCallback(".*", mock.MakeQueryResult(nil, nil), record)

	_, err := pm.EnsureImportTarget(context.Background(), &multipoolermanagerdatapb.EnsureImportTargetRequest{
		Database: routingTestDatabase, OperationId: importOp, Tablegroup: "srctg",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"BEGIN", setReadCommittedSQL, lockRoutingStateSQL,
		selectOperationForUpdateSQL, insertOperationSQL, selectOtherOperationOnTablegroupSQL,
		selectTablegroupByNameSQL, insertTablegroupSQL,
		selectShardNamesSQL, insertFullShardSQL,
		"COMMIT",
	}, queries)
}
