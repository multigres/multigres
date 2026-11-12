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
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// fakeRoutingTx answers the routing store's reads from canned results and
// records every statement with its arguments.
type fakeRoutingTx struct {
	results map[string]*sqltypes.Result
	calls   []fakeRoutingCall
}

type fakeRoutingCall struct {
	sql  string
	args []any
}

func (f *fakeRoutingTx) Query(ctx context.Context, sql string) (*sqltypes.Result, error) {
	return f.QueryArgs(ctx, sql)
}

func (f *fakeRoutingTx) QueryArgs(_ context.Context, sql string, args ...any) (*sqltypes.Result, error) {
	f.calls = append(f.calls, fakeRoutingCall{sql: sql, args: args})
	if r, ok := f.results[sql]; ok {
		return r, nil
	}
	return mock.MakeQueryResult(nil, nil), nil
}

func (f *fakeRoutingTx) Commit(context.Context) error   { return nil }
func (f *fakeRoutingTx) Rollback(context.Context) error { return nil }

// writes returns the recorded statements after the catalog reads.
func (f *fakeRoutingTx) writes() []fakeRoutingCall {
	var out []fakeRoutingCall
	for _, c := range f.calls {
		switch c.sql {
		case selectTablegroupsSQL, selectShardsSQL, selectRoutingRulesSQL,
			countDefaultTablesSQL, selectOperationSQL, selectOtherOperationsSQL, selectFencesSQL:
			continue
		}
		out = append(out, c)
	}
	return out
}

// testRoutingCurrent is the catalog versions the routing store tests write
// at, as writeCatalog would pass them.
var testRoutingCurrent = catalogVersions{routingVersion: 5, planEpoch: 2}

// testRoutingNext is the routing_version writeCatalog would commit a change
// at when it found testRoutingCurrent.
const testRoutingNext = int64(6)

// newFakeRoutingTx returns the catalog reads of a catalog with the default
// tablegroup, an import target srctg and a two-shard commerce, the import
// operation with FENCED fences on all three tablegroups, and the given rule
// rows.
func newFakeRoutingTx(rules ...[]any) *fakeRoutingTx {
	return &fakeRoutingTx{results: routingCatalogResults(rules...)}
}

// routingCatalogResults returns newFakeRoutingTx's canned results, keyed by
// the query that reads them.
func routingCatalogResults(rules ...[]any) map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		selectTablegroupsSQL: mock.MakeQueryResult([]string{"tablegroup_id", "name", "state"}, [][]any{
			{1, "default", "active"}, {2, "srctg", "active"}, {3, "commerce", "active"},
		}),
		selectShardsSQL: mock.MakeQueryResult([]string{"shard_id", "tablegroup_id", "shard_name", "state"}, [][]any{
			{1, 1, "0-inf", "active"},
			{2, 2, "0-inf", "active"},
			{11, 3, "0-80", "active"},
			{12, 3, "80-inf", "active"},
		}),
		selectRoutingRulesSQL: mock.MakeQueryResult([]string{
			"rule_id", "operation_id", "scope", "from_tablegroup_id", "from_table_id",
			"to_tablegroup_id", "to_shard_id", "to_shard_key_id", "created_version",
		}, rules),
		countDefaultTablesSQL: mock.MakeQueryResult([]string{"count"}, [][]any{{0}}),
		selectOperationSQL:    mock.MakeQueryResult([]string{"kind", "state"}, [][]any{{importOperationKind, "preparing"}}),
		selectFencesSQL: fenceResult(
			[]any{100, importOp, 1, nil, fenceStateFenced, 5, "preparing"},
			[]any{101, importOp, 2, nil, fenceStateFenced, 5, "preparing"},
			[]any{102, importOp, 3, nil, fenceStateFenced, 5, "preparing"},
		),
	}
}

// fenceResult is selectFencesSQL's result with the given rows.
func fenceResult(rows ...[]any) *sqltypes.Result {
	return mock.MakeQueryResult([]string{
		"fence_id", "operation_id", "tablegroup_id", "shard_id", "state", "version", "operation_state",
	}, rows)
}

func TestLoadRoutingCatalog(t *testing.T) {
	tx := newFakeRoutingTx([]any{7, importOp, routingScopeTablegroup, 1, nil, 2, 2, nil, 5})
	cat, err := loadRoutingCatalog(context.Background(), tx, testRoutingCurrent, importOp)
	require.NoError(t, err)

	for _, c := range tx.calls {
		assert.NotContains(t, c.sql, "catalog_state", "writeCatalog owns the catalog_state lock and versions")
	}
	assert.Equal(t, int64(5), cat.routingVersion)
	assert.Equal(t, int64(2), cat.planEpoch)
	assert.Equal(t, "srctg", cat.tablegroups[2].name)
	assert.Equal(t, "0-80", keyrange.ShardName(cat.shards[11].keyRange))
	assert.Equal(t, "80-inf", keyrange.ShardName(cat.shards[12].keyRange))
	assert.Equal(t, []routingRule{{
		ruleID: 7, operationID: importOp, scope: routingScopeTablegroup,
		fromTablegroupID: 1, toTablegroupID: 2, toShardID: 2, createdVersion: 5,
	}}, cat.rules)
	assert.Equal(t, &routingOperation{kind: importOperationKind, state: "preparing"}, cat.operation)
	assert.Empty(t, cat.otherOperations)

	var countArgs, otherArgs []any
	for _, c := range tx.calls {
		switch c.sql {
		case countDefaultTablesSQL:
			countArgs = c.args
		case selectOtherOperationsSQL:
			otherArgs = c.args
		}
	}
	assert.Equal(t, []any{constants.DefaultTableGroup}, countArgs)
	assert.Equal(t, []any{importOp}, otherArgs)
}

func TestLoadRoutingCatalog_OtherOperations(t *testing.T) {
	tx := newFakeRoutingTx()
	tx.results[selectOtherOperationsSQL] = mock.MakeQueryResult([]string{"operation_id", "tablegroup_id"}, [][]any{
		{otherOp, 2}, {"33333333-3333-3333-3333-333333333333", 2}, {"44444444-4444-4444-4444-444444444444", 3},
	})
	cat, err := loadRoutingCatalog(context.Background(), tx, testRoutingCurrent, importOp)
	require.NoError(t, err)
	assert.Equal(t, map[int64]string{2: otherOp, 3: "44444444-4444-4444-4444-444444444444"}, cat.otherOperations,
		"each tablegroup keeps the first operation the ordered read returns")
}

func TestLoadRoutingCatalog_RejectsANonCanonicalShardName(t *testing.T) {
	tx := newFakeRoutingTx()
	tx.results[selectShardsSQL] = mock.MakeQueryResult([]string{"shard_id", "tablegroup_id", "shard_name", "state"}, [][]any{
		{11, 3, "00-80", "active"},
	})
	_, err := loadRoutingCatalog(context.Background(), tx, testRoutingCurrent, importOp)
	require.ErrorContains(t, err, "shard 11 has an invalid name")
}

func TestApplyRoutingRuleChange_WritesRules(t *testing.T) {
	// A next that is not current+1 shows the rules are stamped with the
	// version writeCatalog passes, not one the store works out itself.
	const next = int64(9)
	tx := newFakeRoutingTx()
	change, err := applyRoutingRuleChange(context.Background(), tx, testRoutingCurrent, next, routingRuleChange{
		operationID:            importOp,
		rules:                  []shardRuleSpec{{fromTablegroup: "default", toTablegroup: "srctg", toShard: "0-inf"}},
		expectedRoutingVersion: 5,
	})
	require.NoError(t, err)
	assert.Equal(t, catalogRoutingChange, change)

	var nilID *int64
	shardID := int64(2)
	assert.Equal(t, []fakeRoutingCall{
		{sql: deleteOperationRulesSQL, args: []any{importOp}},
		{sql: insertRoutingRuleSQL, args: []any{importOp, routingScopeTablegroup, int64(1), nilID, int64(2), &shardID, nilID, next}},
	}, tx.writes(), "rules are stamped with the version writeCatalog commits them at")
}

func TestApplyRoutingRuleChange_InvalidatesPlans(t *testing.T) {
	// default has one shard; spreading it over commerce's two changes how a
	// query on it may be planned.
	tx := newFakeRoutingTx()
	change, err := applyRoutingRuleChange(context.Background(), tx, testRoutingCurrent, testRoutingNext, routingRuleChange{
		operationID:            importOp,
		rules:                  []shardRuleSpec{{"default", "commerce", "0-80"}, {"default", "commerce", "80-inf"}},
		expectedRoutingVersion: 5,
	})
	require.NoError(t, err)
	assert.Equal(t, catalogInvalidatesPlans, change)
	assert.Len(t, tx.writes(), 3)
}

func TestApplyRoutingRuleChange_Delete(t *testing.T) {
	tx := newFakeRoutingTx([]any{7, importOp, routingScopeTablegroup, 1, nil, 2, 2, nil, 5})
	change, err := applyRoutingRuleChange(context.Background(), tx, testRoutingCurrent, testRoutingNext, routingRuleChange{operationID: importOp, expectedRoutingVersion: 5})
	require.NoError(t, err)
	assert.Equal(t, catalogRoutingChange, change)
	assert.Equal(t, []fakeRoutingCall{
		{sql: deleteOperationRulesSQL, args: []any{importOp}},
	}, tx.writes())
}

func TestApplyRoutingRuleChange_WritesNothing(t *testing.T) {
	existing := []any{7, importOp, routingScopeTablegroup, 1, nil, 2, 2, nil, 5}
	for _, tt := range []struct {
		name   string
		rules  [][]any
		change routingRuleChange
		code   mtrpcpb.Code
	}{
		{
			name:   "already in effect",
			rules:  [][]any{existing},
			change: routingRuleChange{operationID: importOp, rules: []shardRuleSpec{{"default", "srctg", "0-inf"}}, expectedRoutingVersion: 4},
		},
		{
			name:   "stale version",
			change: routingRuleChange{operationID: importOp, rules: []shardRuleSpec{{"default", "srctg", "0-inf"}}, expectedRoutingVersion: 4},
			code:   mtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			name:   "invalid",
			change: routingRuleChange{operationID: importOp, rules: []shardRuleSpec{{"default", "srctg", "0-80"}}, expectedRoutingVersion: 5},
			code:   mtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			name:   "operation id not a uuid",
			change: routingRuleChange{operationID: "nope", expectedRoutingVersion: 5},
			code:   mtrpcpb.Code_INVALID_ARGUMENT,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tx := newFakeRoutingTx(tt.rules...)
			change, err := applyRoutingRuleChange(context.Background(), tx, testRoutingCurrent, testRoutingNext, tt.change)
			if tt.code != mtrpcpb.Code_OK {
				require.Error(t, err)
				assert.Equal(t, tt.code, mterrors.Code(err), "error: %v", err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, catalogNoSnapshotChange, change)
			}
			assert.Empty(t, tx.writes(), tt.name+" must not write")
			if tt.change.operationID == "nope" {
				for _, c := range tx.calls {
					assert.NotEqual(t, selectOperationSQL, c.sql, "a non-uuid operation id must not reach the ::uuid cast")
				}
			}
		})
	}
}

// expectRoutingCatalog registers writeCatalog's catalog primary check, the
// catalog_state lock at testRoutingCurrent, the routing store's catalog reads with the given rule
// rows, its rule writes, the snapshot reads writeCatalog validates the write
// with (the same catalog, whose rules read as the given rows), and COMMIT,
// all recorded in env.queries.
func (env *tableGroupTestEnv) expectRoutingCatalog(rules ...[]any) {
	env.expectCatalogWrite(routingCatalogResults(rules...))
}

// expectCatalogWrite is expectRoutingCatalog with the routing store's reads
// answered from results, and the fence writes registered too.
func (env *tableGroupTestEnv) expectCatalogWrite(results map[string]*sqltypes.Result) {
	env.expectPrimary()
	env.expect("FROM multigres.catalog_state FOR UPDATE", []string{"routing_version", "plan_epoch"},
		[][]any{{testRoutingCurrent.routingVersion, testRoutingCurrent.planEpoch}})
	for sql, res := range results {
		env.qs.AddQueryPatternWithCallback("^"+regexp.QuoteMeta(sql)+"$", res, func(q string) {
			env.queries = append(env.queries, q)
		})
	}
	env.expect("^"+regexp.QuoteMeta(selectOtherOperationsSQL)+"$", []string{"operation_id", "tablegroup_id"}, nil)
	env.expect("^"+regexp.QuoteMeta(deleteOperationRulesSQL)+"$", nil, nil)
	env.expect("^"+regexp.QuoteMeta(insertRoutingRuleSQL)+"$", nil, nil)
	env.expect("^"+regexp.QuoteMeta(insertFenceSQL)+"$", nil, nil)
	env.expect("^"+regexp.QuoteMeta(updateFenceSQL)+"$", nil, nil)
	env.expect("^"+regexp.QuoteMeta(snapshotTableGroupsQuery)+"$", []string{"tablegroup_id", "name", "state"}, [][]any{
		{int64(1), "default", "active"}, {int64(2), "srctg", "active"}, {int64(3), "commerce", "active"},
	})
	env.expect("^"+regexp.QuoteMeta(snapshotShardsQuery)+"$", []string{"shard_id", "tablegroup_id", "shard_name", "range_start", "range_end", "state"}, [][]any{
		{int64(1), int64(1), "0-inf", "", "", "active"},
		{int64(2), int64(2), "0-inf", "", "", "active"},
		{int64(11), int64(3), "0-80", "", "80", "active"},
		{int64(12), int64(3), "80-inf", "80", "", "active"},
	})
	for _, q := range []string{snapshotTablesQuery, snapshotShardIndicesQuery, snapshotShardKeysQuery, snapshotShardKeyColumnsQuery} {
		env.expect("^"+regexp.QuoteMeta(q)+"$", nil, nil)
	}
	env.expectCommit()
}

func TestChangeRoutingRules(t *testing.T) {
	importRule := []shardRuleSpec{{"default", "srctg", "0-inf"}}

	t.Run("a rule change advances routing_version by one", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
		env.expectRoutingCatalog()
		env.expect("SET routing_version = routing_version \\+ 1 RETURNING", []string{"routing_version", "plan_epoch"}, [][]any{{int64(6), int64(2)}})

		v, err := env.pm.changeRoutingRules(t.Context(), routingRuleChange{operationID: importOp, rules: importRule, expectedRoutingVersion: 5})
		require.NoError(t, err)
		assert.Equal(t, catalogVersions{routingVersion: 6, planEpoch: 2}, v)
		assert.Len(t, env.queriesMatching("INSERT INTO multigres.routing_rule"), 1)
		assert.Len(t, env.queriesMatching("UPDATE multigres.catalog_state"), 1, "advanced once, by writeCatalog")
		assert.Equal(t, "COMMIT", env.queries[len(env.queries)-1])
		assert.Equal(t, int64(6), hs.getState().RoutingVersion)
	})
	t.Run("a change that invalidates plans advances plan_epoch too", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectRoutingCatalog()
		env.expect("SET routing_version = routing_version \\+ 1, plan_epoch = plan_epoch \\+ 1",
			[]string{"routing_version", "plan_epoch"}, [][]any{{int64(6), int64(3)}})

		v, err := env.pm.changeRoutingRules(t.Context(), routingRuleChange{
			operationID:            importOp,
			rules:                  []shardRuleSpec{{"default", "commerce", "0-80"}, {"default", "commerce", "80-inf"}},
			expectedRoutingVersion: 5,
		})
		require.NoError(t, err)
		assert.Equal(t, catalogVersions{routingVersion: 6, planEpoch: 3}, v)
		assert.Len(t, env.queriesMatching("plan_epoch = plan_epoch + 1"), 1)
	})
	t.Run("a no-op writes nothing and returns the current versions", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectRoutingCatalog([]any{7, importOp, routingScopeTablegroup, 1, nil, 2, 2, nil, 5})

		// A retry carries the version it expected before its first attempt.
		v, err := env.pm.changeRoutingRules(t.Context(), routingRuleChange{operationID: importOp, rules: importRule, expectedRoutingVersion: 4})
		require.NoError(t, err)
		assert.Equal(t, testRoutingCurrent, v)
		assert.Empty(t, env.queriesMatching("routing_rule WHERE"), "no rule deleted")
		assert.Empty(t, env.queriesMatching("INSERT INTO"))
		assert.Empty(t, env.queriesMatching("UPDATE multigres.catalog_state"))
		assert.Empty(t, env.queriesMatching("ORDER BY tablegroup_id"), "nothing in the snapshot changed, so it is not revalidated")
	})
	t.Run("a stale expected version rolls back", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectRoutingCatalog()

		_, err := env.pm.changeRoutingRules(t.Context(), routingRuleChange{operationID: importOp, rules: importRule, expectedRoutingVersion: 4})
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "error: %v", err)
		assert.Empty(t, env.queriesMatching("INSERT INTO"))
		assert.Empty(t, env.queriesMatching("UPDATE multigres.catalog_state"))
		assert.Empty(t, env.queriesMatching("COMMIT"))
	})
}
