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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
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
		case lockRoutingStateSQL, selectTablegroupsSQL, selectShardsSQL, selectRoutingRulesSQL,
			countDefaultTablesSQL, selectOperationSQL:
			continue
		}
		out = append(out, c)
	}
	return out
}

// newFakeRoutingTx returns a catalog at routing version 5 with the default
// tablegroup, an import target srctg and a two-shard commerce, the import
// operation, and the given rule rows.
func newFakeRoutingTx(rules ...[]any) *fakeRoutingTx {
	return &fakeRoutingTx{results: map[string]*sqltypes.Result{
		lockRoutingStateSQL: mock.MakeQueryResult([]string{"routing_version", "plan_epoch"}, [][]any{{5, 2}}),
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
	}}
}

func TestLoadRoutingCatalog(t *testing.T) {
	tx := newFakeRoutingTx([]any{7, importOp, routingScopeTablegroup, 1, nil, 2, 2, nil, 5})
	cat, err := loadRoutingCatalog(context.Background(), tx, importOp)
	require.NoError(t, err)

	assert.Equal(t, lockRoutingStateSQL, tx.calls[0].sql, "the catalog_state lock comes first")
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

	var countArgs []any
	for _, c := range tx.calls {
		if c.sql == countDefaultTablesSQL {
			countArgs = c.args
		}
	}
	assert.Equal(t, []any{constants.DefaultTableGroup}, countArgs)
}

func TestLoadRoutingCatalog_RejectsANonCanonicalShardName(t *testing.T) {
	tx := newFakeRoutingTx()
	tx.results[selectShardsSQL] = mock.MakeQueryResult([]string{"shard_id", "tablegroup_id", "shard_name", "state"}, [][]any{
		{11, 3, "00-80", "active"},
	})
	_, err := loadRoutingCatalog(context.Background(), tx, importOp)
	require.ErrorContains(t, err, "shard 11 has an invalid name")
}

func TestApplyRoutingRuleChange_WritesRulesAndBumpsVersion(t *testing.T) {
	tx := newFakeRoutingTx()
	version, err := applyRoutingRuleChange(context.Background(), tx, routingRuleChange{
		operationID:            importOp,
		rules:                  []shardRuleSpec{{fromTablegroup: "default", toTablegroup: "srctg", toShard: "0-inf"}},
		expectedRoutingVersion: 5,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(6), version)

	var nilID *int64
	shardID := int64(2)
	assert.Equal(t, []fakeRoutingCall{
		{sql: deleteOperationRulesSQL, args: []any{importOp}},
		{sql: insertRoutingRuleSQL, args: []any{importOp, routingScopeTablegroup, int64(1), nilID, int64(2), &shardID, nilID, int64(6)}},
		{sql: bumpRoutingVersionSQL, args: []any{int64(6), int64(0)}},
	}, tx.writes())
}

func TestApplyRoutingRuleChange_DeleteBumpsVersion(t *testing.T) {
	tx := newFakeRoutingTx([]any{7, importOp, routingScopeTablegroup, 1, nil, 2, 2, nil, 5})
	version, err := applyRoutingRuleChange(context.Background(), tx, routingRuleChange{operationID: importOp, expectedRoutingVersion: 5})
	require.NoError(t, err)
	assert.Equal(t, int64(6), version)
	assert.Equal(t, []fakeRoutingCall{
		{sql: deleteOperationRulesSQL, args: []any{importOp}},
		{sql: bumpRoutingVersionSQL, args: []any{int64(6), int64(0)}},
	}, tx.writes())
}

func TestApplyRoutingRuleChange_WritesNothing(t *testing.T) {
	existing := []any{7, importOp, routingScopeTablegroup, 1, nil, 2, 2, nil, 5}
	for _, tt := range []struct {
		name    string
		rules   [][]any
		change  routingRuleChange
		version int64
		code    mtrpcpb.Code
	}{
		{
			name:    "already in effect",
			rules:   [][]any{existing},
			change:  routingRuleChange{operationID: importOp, rules: []shardRuleSpec{{"default", "srctg", "0-inf"}}, expectedRoutingVersion: 4},
			version: 5,
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
			version, err := applyRoutingRuleChange(context.Background(), tx, tt.change)
			if tt.code != mtrpcpb.Code_OK {
				require.Error(t, err)
				assert.Equal(t, tt.code, mterrors.Code(err), "error: %v", err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.version, version)
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
