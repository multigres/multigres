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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

const (
	importOp = "11111111-1111-1111-1111-111111111111"
	otherOp  = "22222222-2222-2222-2222-222222222222"
)

// Tablegroup and shard ids of testRoutingCatalog.
const (
	tgDefault  = 1
	tgSrc      = 2
	tgCommerce = 3
	tgOrders   = 4

	shardDefault  = 1  // default 0-inf, active
	shardSrc      = 2  // srctg 0-inf, active
	shardC0080    = 11 // commerce 0-80, active
	shardC80Inf   = 12 // commerce 80-inf, active
	shardC0040    = 13 // commerce 0-40, preparing
	shardC4080    = 14 // commerce 40-80, preparing
	shardOrders   = 41 // orders 0-inf, active
	shardO0080    = 42 // orders 0-80, preparing
	shardO80Inf   = 43 // orders 80-inf, preparing
	testRoutingV0 = 5
)

// testRoutingCatalog is a catalog with no rules: the default tablegroup, an
// import target srctg, commerce (two active shards and a split of the first in
// preparation) and orders (one active shard and a split in preparation).
// operation is an import that may switch.
func testRoutingCatalog() *routingCatalog {
	cat := &routingCatalog{
		routingVersion: testRoutingV0,
		tablegroups: map[int64]*catalogTablegroup{
			tgDefault:  {id: tgDefault, name: "default", state: "active"},
			tgSrc:      {id: tgSrc, name: "srctg", state: "active"},
			tgCommerce: {id: tgCommerce, name: "commerce", state: "active"},
			tgOrders:   {id: tgOrders, name: "orders", state: "active"},
		},
		shards:    map[int64]*catalogShard{},
		operation: &routingOperation{kind: importOperationKind, state: "preparing"},
	}
	for _, s := range []*catalogShard{
		{id: shardDefault, tablegroupID: tgDefault, name: "0-inf", state: "active"},
		{id: shardSrc, tablegroupID: tgSrc, name: "0-inf", state: "active"},
		{id: shardC0080, tablegroupID: tgCommerce, name: "0-80", state: "active"},
		{id: shardC80Inf, tablegroupID: tgCommerce, name: "80-inf", state: "active"},
		{id: shardC0040, tablegroupID: tgCommerce, name: "0-40", state: "preparing"},
		{id: shardC4080, tablegroupID: tgCommerce, name: "40-80", state: "preparing"},
		{id: shardOrders, tablegroupID: tgOrders, name: "0-inf", state: "active"},
		{id: shardO0080, tablegroupID: tgOrders, name: "0-80", state: "preparing"},
		{id: shardO80Inf, tablegroupID: tgOrders, name: "80-inf", state: "preparing"},
	} {
		kr, err := keyrange.ParseShardName(s.name)
		if err != nil {
			panic(err)
		}
		s.keyRange = kr
		cat.shards[s.id] = s
	}
	return cat
}

// shardRule is a stored shard rule of op.
func shardRule(op string, from, to, toShard int64) routingRule {
	return routingRule{operationID: op, scope: routingScopeTablegroup, fromTablegroupID: from, toTablegroupID: to, toShardID: toShard}
}

func change(op string, rules ...shardRuleSpec) routingRuleChange {
	return routingRuleChange{operationID: op, rules: rules, expectedRoutingVersion: testRoutingV0}
}

func spec(from, to, toShard string) shardRuleSpec {
	return shardRuleSpec{fromTablegroup: from, toTablegroup: to, toShard: toShard}
}

func requireRoutingCode(t *testing.T, err error, code mtrpcpb.Code, contains string) {
	t.Helper()
	require.Error(t, err)
	assert.Equal(t, code, mterrors.Code(err), "error: %v", err)
	assert.Contains(t, err.Error(), contains)
}

func TestPlanRoutingRuleChange_ImportRoutesDefault(t *testing.T) {
	cat := testRoutingCatalog()
	plan, err := planRoutingRuleChange(cat, change(importOp, spec("default", "srctg", "0-inf")))
	require.NoError(t, err)
	assert.False(t, plan.noop)
	assert.Equal(t, []routingRule{shardRule(importOp, tgDefault, tgSrc, shardSrc)}, plan.insert)
	assert.False(t, plan.bumpPlanEpoch, "an execute-time rule from one shard to one shard keeps plans valid")
}

func TestPlanRoutingRuleChange_DefaultSourceGuard(t *testing.T) {
	t.Run("not an import", func(t *testing.T) {
		cat := testRoutingCatalog()
		cat.operation.kind = "reshard"
		_, err := planRoutingRuleChange(cat, change(importOp, spec("default", "srctg", "0-inf")))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "only an import may route the default tablegroup")
	})
	t.Run("default has tables", func(t *testing.T) {
		cat := testRoutingCatalog()
		cat.defaultTableCount = 2
		_, err := planRoutingRuleChange(cat, change(importOp, spec("default", "srctg", "0-inf")))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "has 2 registered tables")
	})
	t.Run("default as a target is not restricted", func(t *testing.T) {
		cat := testRoutingCatalog()
		cat.operation.kind = "reshard"
		cat.defaultTableCount = 2
		_, err := planRoutingRuleChange(cat, change(importOp, spec("srctg", "default", "0-inf")))
		require.NoError(t, err)
	})
}

func TestPlanRoutingRuleChange_AlreadyInEffectIsNoop(t *testing.T) {
	cat := testRoutingCatalog()
	cat.rules = []routingRule{shardRule(importOp, tgDefault, tgSrc, shardSrc)}
	cat.rules[0].ruleID, cat.rules[0].createdVersion = 7, testRoutingV0

	// A retry after the change committed carries the version from before it.
	retry := change(importOp, spec("default", "srctg", "0-inf"))
	retry.expectedRoutingVersion = testRoutingV0 - 1
	plan, err := planRoutingRuleChange(cat, retry)
	require.NoError(t, err)
	assert.True(t, plan.noop)

	// So does deleting the rules of an operation that has none.
	plan, err = planRoutingRuleChange(cat, change(otherOp))
	require.NoError(t, err)
	assert.True(t, plan.noop)
}

func TestPlanRoutingRuleChange_StaleVersion(t *testing.T) {
	cat := testRoutingCatalog()
	c := change(importOp, spec("default", "srctg", "0-inf"))
	c.expectedRoutingVersion = testRoutingV0 - 1
	_, err := planRoutingRuleChange(cat, c)
	requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "routing version is 5, expected 4")
}

func TestPlanRoutingRuleChange_MalformedRequest(t *testing.T) {
	for name, c := range map[string]routingRuleChange{
		"operation id not a uuid": change("op-1", spec("default", "srctg", "0-inf")),
		"missing target shard":    change(importOp, spec("default", "srctg", "")),
		"duplicate rule":          change(importOp, spec("default", "srctg", "0-inf"), spec("default", "srctg", "0-inf")),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := planRoutingRuleChange(testRoutingCatalog(), c)
			require.Error(t, err)
			assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err), "error: %v", err)
		})
	}
}

func TestPlanRoutingRuleChange_UnusableCatalog(t *testing.T) {
	tests := []struct {
		name     string
		modify   func(cat *routingCatalog)
		spec     shardRuleSpec
		contains string
	}{
		{name: "unknown source", spec: spec("nope", "srctg", "0-inf"), contains: `tablegroup "nope" does not exist`},
		{name: "unknown target", spec: spec("default", "nope", "0-inf"), contains: `tablegroup "nope" does not exist`},
		{name: "unknown shard", spec: spec("default", "srctg", "0-80"), contains: `shard "0-80" of tablegroup "srctg" does not exist`},
		{
			name:     "no operation",
			modify:   func(cat *routingCatalog) { cat.operation = nil },
			spec:     spec("default", "srctg", "0-inf"),
			contains: "operation " + importOp + " does not exist",
		},
		{
			name:     "finished operation",
			modify:   func(cat *routingCatalog) { cat.operation.state = "completed" },
			spec:     spec("default", "srctg", "0-inf"),
			contains: "is completed and may not change routing",
		},
		{
			name:     "target being dropped",
			modify:   func(cat *routingCatalog) { cat.tablegroups[tgSrc].state = "dropping" },
			spec:     spec("default", "srctg", "0-inf"),
			contains: `tablegroup "srctg" is being dropped`,
		},
		{
			name:     "retired target shard",
			modify:   func(cat *routingCatalog) { cat.shards[shardSrc].state = "retired" },
			spec:     spec("default", "srctg", "0-inf"),
			contains: `shard "0-inf" of tablegroup "srctg" is retired`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cat := testRoutingCatalog()
			if tt.modify != nil {
				tt.modify(cat)
			}
			_, err := planRoutingRuleChange(cat, change(importOp, tt.spec))
			requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, tt.contains)
		})
	}
}

func TestPlanRoutingRuleChange_RuleToItself(t *testing.T) {
	_, err := planRoutingRuleChange(testRoutingCatalog(), change(importOp, spec("default", "default", "0-inf")))
	requireRoutingCode(t, err, mtrpcpb.Code_INVALID_ARGUMENT, `routes shard "0-inf" of tablegroup "default" to itself`)

	// A shard of the same tablegroup that is not active yet is a reshard target.
	_, err = planRoutingRuleChange(testRoutingCatalog(),
		change(importOp, spec("commerce", "commerce", "0-40"), spec("commerce", "commerce", "40-80")))
	require.NoError(t, err)
}

func TestPlanRoutingRuleChange_ExactCover(t *testing.T) {
	t.Run("split covers its source", func(t *testing.T) {
		plan, err := planRoutingRuleChange(testRoutingCatalog(),
			change(importOp, spec("commerce", "commerce", "0-40"), spec("commerce", "commerce", "40-80")))
		require.NoError(t, err)
		assert.Len(t, plan.insert, 2)
		assert.False(t, plan.bumpPlanEpoch, "commerce already has several shards")
	})
	t.Run("gap", func(t *testing.T) {
		_, err := planRoutingRuleChange(testRoutingCatalog(), change(importOp, spec("commerce", "commerce", "0-40")))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "do not exactly cover the shards [0-80]")
	})
	t.Run("target overlaps another source shard", func(t *testing.T) {
		// srctg's 0-inf overlaps both of commerce's active shards; together
		// they are covered, so this is a whole-tablegroup move.
		_, err := planRoutingRuleChange(testRoutingCatalog(), change(importOp, spec("commerce", "srctg", "0-inf")))
		require.NoError(t, err)
		// srctg 0-inf and orders 0-80 would overlap each other.
		_, err = planRoutingRuleChange(testRoutingCatalog(),
			change(importOp, spec("commerce", "srctg", "0-inf"), spec("commerce", "orders", "0-80")))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "do not exactly cover")
	})
	t.Run("single-shard source spread over two shards invalidates plans", func(t *testing.T) {
		plan, err := planRoutingRuleChange(testRoutingCatalog(),
			change(importOp, spec("orders", "orders", "0-80"), spec("orders", "orders", "80-inf")))
		require.NoError(t, err)
		assert.True(t, plan.bumpPlanEpoch)
	})
	t.Run("no active source shard", func(t *testing.T) {
		cat := testRoutingCatalog()
		cat.shards[shardOrders].state = "retired"
		_, err := planRoutingRuleChange(cat, change(importOp, spec("orders", "orders", "0-80"), spec("orders", "orders", "80-inf")))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, `no active shard of tablegroup "orders"`)
	})
}

func TestPlanRoutingRuleChange_RuleSetInvariants(t *testing.T) {
	t.Run("overlap with another operation", func(t *testing.T) {
		cat := testRoutingCatalog()
		cat.rules = []routingRule{shardRule(otherOp, tgCommerce, tgCommerce, shardC0040), shardRule(otherOp, tgCommerce, tgCommerce, shardC4080)}
		_, err := planRoutingRuleChange(cat, change(importOp, spec("commerce", "srctg", "0-inf")))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, `overlap on tablegroup "commerce"`)
	})
	t.Run("target is routed elsewhere", func(t *testing.T) {
		cat := testRoutingCatalog()
		cat.rules = []routingRule{shardRule(otherOp, tgSrc, tgOrders, shardOrders)}
		_, err := planRoutingRuleChange(cat, change(importOp, spec("default", "srctg", "0-inf")))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "rules do not chain")
	})
	t.Run("source is another rule's target", func(t *testing.T) {
		cat := testRoutingCatalog()
		cat.rules = []routingRule{shardRule(otherOp, tgOrders, tgSrc, shardSrc)}
		_, err := planRoutingRuleChange(cat, change(importOp, spec("srctg", "default", "0-inf")))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "rules do not chain")
	})
	t.Run("tablegroup moved by a table rule", func(t *testing.T) {
		cat := testRoutingCatalog()
		cat.rules = []routingRule{{operationID: otherOp, scope: routingScopeTable, fromTablegroupID: tgCommerce, fromTableID: 9, toTablegroupID: tgOrders}}
		_, err := planRoutingRuleChange(cat, change(importOp, spec("commerce", "srctg", "0-inf")))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, `tablegroup "commerce" is already moved by a table rule`)
	})
}

// The import's switch replaces default → srctg with srctg → default in one
// change. Against the rules it removes, the new rule would chain (srctg is
// the old rule's target); against the result it is valid.
func TestPlanRoutingRuleChange_SwapIsValidatedOnTheResult(t *testing.T) {
	cat := testRoutingCatalog()
	cat.rules = []routingRule{shardRule(importOp, tgDefault, tgSrc, shardSrc)}

	plan, err := planRoutingRuleChange(cat, change(importOp, spec("srctg", "default", "0-inf")))
	require.NoError(t, err)
	assert.Equal(t, []routingRule{shardRule(importOp, tgSrc, tgDefault, shardDefault)}, plan.insert)
	assert.False(t, plan.bumpPlanEpoch)

	// A second operation could not add the reverse rule next to it.
	_, err = planRoutingRuleChange(cat, change(otherOp, spec("srctg", "default", "0-inf")))
	requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "rules do not chain")
}

func TestPlanRoutingRuleChange_DeleteIsAlwaysAllowed(t *testing.T) {
	cat := testRoutingCatalog()
	cat.rules = []routingRule{shardRule(importOp, tgDefault, tgSrc, shardSrc)}
	// Since the rule was written the catalog has changed in ways that would
	// reject writing it again.
	cat.defaultTableCount = 3
	cat.operation.state = "failed"
	cat.shards[shardSrc].state = "retired"

	plan, err := planRoutingRuleChange(cat, change(importOp))
	require.NoError(t, err)
	assert.False(t, plan.noop)
	assert.Empty(t, plan.insert)
}
