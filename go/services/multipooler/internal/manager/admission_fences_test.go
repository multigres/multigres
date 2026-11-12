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
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// unfencedCatalog is testRoutingCatalog with no fences.
func unfencedCatalog(fences ...catalogFence) *routingCatalog {
	cat := testRoutingCatalog()
	cat.fences = fences
	return cat
}

// fence fences one scope: shard of tablegroup, or the whole tablegroup when
// shard is empty.
func fence(op, tablegroup, shard string) fenceChange {
	return fences(op, fenceScopeSpec{tablegroup: tablegroup, shard: shard})
}

// fences fences every scope in one change.
func fences(op string, scopes ...fenceScopeSpec) fenceChange {
	return fenceChange{operationID: op, scopes: scopes, expectedRoutingVersion: testRoutingV0}
}

func fenceSpec(tablegroup, shard string) fenceScopeSpec {
	return fenceScopeSpec{tablegroup: tablegroup, shard: shard}
}

// onlyWrite returns the single write of plan.
func onlyWrite(t *testing.T, plan fencePlan) fenceWrite {
	t.Helper()
	require.Len(t, plan.writes, 1)
	return plan.writes[0]
}

func unfence(op, tablegroup, shard string) fenceChange {
	c := fence(op, tablegroup, shard)
	c.lift = true
	return c
}

func TestPlanFenceChange_Fence(t *testing.T) {
	t.Run("a new scope inserts a FENCED fence", func(t *testing.T) {
		plan, err := planFenceChange(unfencedCatalog(), fence(importOp, "commerce", "0-80"))
		require.NoError(t, err)
		assert.Equal(t, fenceWrite{scope: fenceScope{tablegroupID: tgCommerce, shardID: shardC0080}, state: fenceStateFenced}, onlyWrite(t, plan))

		plan, err = planFenceChange(unfencedCatalog(), fence(importOp, "commerce", ""))
		require.NoError(t, err)
		assert.Equal(t, fenceScope{tablegroupID: tgCommerce}, onlyWrite(t, plan).scope, "no shard fences the whole tablegroup")
	})
	t.Run("an existing fence is a no-op whatever the version", func(t *testing.T) {
		c := fence(importOp, "commerce", "0-80")
		c.expectedRoutingVersion = testRoutingV0 - 1
		plan, err := planFenceChange(unfencedCatalog(fenceRow(7, importOp, tgCommerce, shardC0080, fenceStateFenced)), c)
		require.NoError(t, err)
		assert.Empty(t, plan.writes)
	})
	t.Run("the operation's own FENCING or UNFENCED fence is updated in place", func(t *testing.T) {
		for _, state := range []string{fenceStateFencing, fenceStateUnfenced} {
			plan, err := planFenceChange(unfencedCatalog(fenceRow(7, importOp, tgCommerce, shardC0080, state)), fence(importOp, "commerce", "0-80"))
			require.NoError(t, err, state)
			w := onlyWrite(t, plan)
			assert.Equal(t, int64(7), w.fenceID, state)
			assert.Equal(t, fenceStateFenced, w.state, state)
		}
	})
	t.Run("a stale expected version is refused", func(t *testing.T) {
		c := fence(importOp, "commerce", "0-80")
		c.expectedRoutingVersion = testRoutingV0 - 1
		_, err := planFenceChange(unfencedCatalog(), c)
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "routing version is 5, expected 4")
	})
	t.Run("a tablegroup being dropped is refused", func(t *testing.T) {
		cat := unfencedCatalog()
		cat.tablegroups[tgCommerce].state = "dropping"
		_, err := planFenceChange(cat, fence(importOp, "commerce", ""))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, `tablegroup "commerce" is being dropped`)
	})
}

func TestPlanFenceChange_Malformed(t *testing.T) {
	for name, tt := range map[string]struct {
		change fenceChange
		code   mtrpcpb.Code
		want   string
	}{
		"operation id not a uuid": {fence("op-1", "commerce", ""), mtrpcpb.Code_INVALID_ARGUMENT, "invalid operation id"},
		"no tablegroup":           {fence(importOp, "", ""), mtrpcpb.Code_INVALID_ARGUMENT, "needs a tablegroup"},
		"unknown tablegroup":      {fence(importOp, "nope", ""), mtrpcpb.Code_FAILED_PRECONDITION, `tablegroup "nope" does not exist`},
		"shard of another tablegroup": {
			fence(importOp, "srctg", "0-80"), mtrpcpb.Code_FAILED_PRECONDITION, `shard "0-80" of tablegroup "srctg" does not exist`,
		},
		"no scopes": {fences(importOp), mtrpcpb.Code_INVALID_ARGUMENT, "no fence scopes"},
		"the same scope twice": {
			fences(importOp, fenceSpec("commerce", "0-80"), fenceSpec("srctg", ""), fenceSpec("commerce", "0-80")),
			mtrpcpb.Code_INVALID_ARGUMENT, `fence scopes shard "0-80" of tablegroup "commerce" and shard "0-80" of tablegroup "commerce" overlap`,
		},
		"a whole tablegroup and one of its shards": {
			fences(importOp, fenceSpec("commerce", "80-inf"), fenceSpec("commerce", "")),
			mtrpcpb.Code_INVALID_ARGUMENT, `fence scopes shard "80-inf" of tablegroup "commerce" and tablegroup "commerce" overlap`,
		},
		"a scope that does not resolve fails the list": {
			fences(importOp, fenceSpec("commerce", "0-80"), fenceSpec("nope", "")),
			mtrpcpb.Code_FAILED_PRECONDITION, `tablegroup "nope" does not exist`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := planFenceChange(unfencedCatalog(), tt.change)
			requireRoutingCode(t, err, tt.code, tt.want)
		})
	}
}

func TestPlanFenceChange_List(t *testing.T) {
	split := fences(importOp, fenceSpec("commerce", "0-80"), fenceSpec("commerce", "0-40"), fenceSpec("commerce", "40-80"))

	t.Run("every scope is written", func(t *testing.T) {
		plan, err := planFenceChange(unfencedCatalog(), split)
		require.NoError(t, err)
		assert.Equal(t, []fenceWrite{
			{scope: onShard(tgCommerce, shardC0080), state: fenceStateFenced},
			{scope: onShard(tgCommerce, shardC0040), state: fenceStateFenced},
			{scope: onShard(tgCommerce, shardC4080), state: fenceStateFenced},
		}, plan.writes)
	})
	t.Run("scopes already fenced are skipped", func(t *testing.T) {
		cat := unfencedCatalog(fenceRow(7, importOp, tgCommerce, shardC0040, fenceStateFenced))
		plan, err := planFenceChange(cat, split)
		require.NoError(t, err)
		assert.Equal(t, []fenceWrite{
			{scope: onShard(tgCommerce, shardC0080), state: fenceStateFenced},
			{scope: onShard(tgCommerce, shardC4080), state: fenceStateFenced},
		}, plan.writes)

		// With anything left to write, the version is checked.
		stale := split
		stale.expectedRoutingVersion = testRoutingV0 - 1
		_, err = planFenceChange(cat, stale)
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "routing version is 5, expected 4")
	})
	t.Run("a list already in effect is a no-op whatever the version", func(t *testing.T) {
		cat := unfencedCatalog(
			fenceRow(7, importOp, tgCommerce, shardC0080, fenceStateFenced),
			fenceRow(8, importOp, tgCommerce, shardC0040, fenceStateFenced),
			fenceRow(9, importOp, tgCommerce, shardC4080, fenceStateFenced),
		)
		retry := split
		retry.expectedRoutingVersion = 1
		plan, err := planFenceChange(cat, retry)
		require.NoError(t, err)
		assert.Empty(t, plan.writes)
	})
	t.Run("one refused scope refuses the list", func(t *testing.T) {
		cat := unfencedCatalog(fenceRow(7, otherOp, tgCommerce, shardC4080, fenceStateFenced))
		_, err := planFenceChange(cat, split)
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION,
			`shard "40-80" of tablegroup "commerce" is fenced by operation `+otherOp)
	})
	t.Run("both sides of an import in one change", func(t *testing.T) {
		plan, err := planFenceChange(unfencedCatalog(), fences(importOp, fenceSpec("default", ""), fenceSpec("srctg", "")))
		require.NoError(t, err)
		assert.Len(t, plan.writes, 2)
	})
	t.Run("unfence lifts only the scopes the operation holds", func(t *testing.T) {
		cat := unfencedCatalog(
			fenceRow(7, importOp, tgCommerce, shardC0080, fenceStateFenced),
			fenceRow(8, importOp, tgCommerce, shardC0040, fenceStateUnfenced),
		)
		c := split
		c.lift = true
		plan, err := planFenceChange(cat, c)
		require.NoError(t, err)
		assert.Equal(t, []fenceWrite{{fenceID: 7, scope: onShard(tgCommerce, shardC0080), state: fenceStateUnfenced}}, plan.writes)
	})
}

func TestPlanFenceChange_Operation(t *testing.T) {
	for _, change := range []fenceChange{fence(importOp, "commerce", ""), unfence(importOp, "commerce", "")} {
		t.Run(map[bool]string{false: "fence", true: "unfence"}[change.lift], func(t *testing.T) {
			raised := fenceRow(7, importOp, tgCommerce, 0, fenceStateFenced)

			cat := unfencedCatalog(raised)
			cat.operation = nil
			_, err := planFenceChange(cat, change)
			if change.lift {
				requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "operation "+importOp+" does not exist")
			} else {
				// Fencing a scope the operation already holds is a no-op.
				require.NoError(t, err)
				cat.fences = nil
				_, err = planFenceChange(cat, change)
				requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "operation "+importOp+" does not exist")
			}

			for _, state := range terminalOperationStates {
				cat := unfencedCatalog()
				if change.lift {
					cat.fences = []catalogFence{raised}
				}
				cat.operation.state = state
				_, err := planFenceChange(cat, change)
				requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "is "+state+" and may not change fences")
			}
		})
	}
}

func TestPlanFenceChange_Overlap(t *testing.T) {
	refused := []struct {
		name  string
		held  catalogFence
		scope fenceChange
	}{
		{"a shard inside another operation's whole-tablegroup fence", fenceRow(7, otherOp, tgCommerce, 0, fenceStateFenced), fence(importOp, "commerce", "0-80")},
		{"a whole tablegroup with another operation's shard fence", fenceRow(7, otherOp, tgCommerce, shardC80Inf, fenceStateFenced), fence(importOp, "commerce", "")},
		{"the same shard", fenceRow(7, otherOp, tgCommerce, shardC0080, fenceStateFenced), fence(importOp, "commerce", "0-80")},
		{"a fence still being raised", fenceRow(7, otherOp, tgCommerce, shardC0080, fenceStateFencing), fence(importOp, "commerce", "0-80")},
	}
	for _, tt := range refused {
		t.Run(tt.name, func(t *testing.T) {
			_, err := planFenceChange(unfencedCatalog(tt.held), tt.scope)
			requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "is fenced by operation "+otherOp+", which has not finished")
		})
	}

	t.Run("another shard of the tablegroup is not an overlap", func(t *testing.T) {
		plan, err := planFenceChange(unfencedCatalog(fenceRow(7, otherOp, tgCommerce, shardC80Inf, fenceStateFenced)), fence(importOp, "commerce", "0-80"))
		require.NoError(t, err)
		assert.Zero(t, onlyWrite(t, plan).fenceID, "a new row")
	})
	t.Run("another tablegroup is not an overlap", func(t *testing.T) {
		_, err := planFenceChange(unfencedCatalog(fenceRow(7, otherOp, tgOrders, 0, fenceStateFenced)), fence(importOp, "commerce", ""))
		require.NoError(t, err)
	})

	taken := []struct {
		name string
		held catalogFence
	}{
		{"a lifted fence of another operation", fenceRow(7, otherOp, tgCommerce, shardC0080, fenceStateUnfenced)},
		{"a fence of a finished operation", func() catalogFence {
			f := fenceRow(7, otherOp, tgCommerce, shardC0080, fenceStateFenced)
			f.operationState = opStateDone
			return f
		}()},
	}
	for _, tt := range taken {
		t.Run(tt.name+" is taken over", func(t *testing.T) {
			plan, err := planFenceChange(unfencedCatalog(tt.held), fence(importOp, "commerce", "0-80"))
			require.NoError(t, err)
			w := onlyWrite(t, plan)
			assert.Equal(t, int64(7), w.fenceID, "the scope keeps its one row")
			assert.Equal(t, fenceStateFenced, w.state)
		})
	}
}

func TestPlanFenceChange_Unfence(t *testing.T) {
	t.Run("the operation's raised fence becomes UNFENCED", func(t *testing.T) {
		for _, state := range []string{fenceStateFencing, fenceStateFenced} {
			plan, err := planFenceChange(unfencedCatalog(fenceRow(7, importOp, tgCommerce, shardC0080, state)), unfence(importOp, "commerce", "0-80"))
			require.NoError(t, err, state)
			assert.Equal(t, fenceWrite{fenceID: 7, scope: fenceScope{tablegroupID: tgCommerce, shardID: shardC0080}, state: fenceStateUnfenced}, onlyWrite(t, plan), state)
		}
	})
	t.Run("a tablegroup being dropped can still be unfenced", func(t *testing.T) {
		cat := unfencedCatalog(fenceRow(7, importOp, tgCommerce, 0, fenceStateFenced))
		cat.tablegroups[tgCommerce].state = "dropping"
		_, err := planFenceChange(cat, unfence(importOp, "commerce", ""))
		require.NoError(t, err)
	})
	for name, cat := range map[string]*routingCatalog{
		"no fence":                        unfencedCatalog(),
		"already unfenced":                unfencedCatalog(fenceRow(7, importOp, tgCommerce, shardC0080, fenceStateUnfenced)),
		"another operation's fence":       unfencedCatalog(fenceRow(7, otherOp, tgCommerce, shardC0080, fenceStateFenced)),
		"a fence on another scope":        unfencedCatalog(fenceRow(7, importOp, tgCommerce, 0, fenceStateFenced)),
		"no fence and no operation":       func() *routingCatalog { c := unfencedCatalog(); c.operation = nil; return c }(),
		"no fence and a failed operation": func() *routingCatalog { c := unfencedCatalog(); c.operation.state = opStateFailed; return c }(),
	} {
		t.Run(name+" is a no-op whatever the version", func(t *testing.T) {
			c := unfence(importOp, "commerce", "0-80")
			c.expectedRoutingVersion = 1
			plan, err := planFenceChange(cat, c)
			require.NoError(t, err)
			assert.Empty(t, plan.writes)
		})
	}
}

// fenced returns testRoutingCatalog's catalog with FENCED fences of importOp
// on the given scopes only.
func fenced(scopes ...fenceScope) *routingCatalog {
	cat := unfencedCatalog()
	for i, s := range scopes {
		cat.fences = append(cat.fences, fenceRow(int64(200+i), importOp, s.tablegroupID, s.shardID, fenceStateFenced))
	}
	return cat
}

func whole(tg int64) fenceScope          { return fenceScope{tablegroupID: tg} }
func onShard(tg, shard int64) fenceScope { return fenceScope{tablegroupID: tg, shardID: shard} }

func TestPlanRoutingRuleChange_RequiresFences(t *testing.T) {
	importRule := change(importOp, spec("default", "srctg", "0-inf"))
	tests := []struct {
		name   string
		cat    *routingCatalog
		change routingRuleChange
		// missing is the scope the error names, empty when the change is
		// accepted.
		missing string
	}{
		{"no fences", fenced(), importRule, `shard "0-inf" of tablegroup "default" is not fenced by operation ` + importOp},
		{"source only", fenced(whole(tgDefault)), importRule, `shard "0-inf" of tablegroup "srctg" is not fenced`},
		{"target only", fenced(whole(tgSrc)), importRule, `shard "0-inf" of tablegroup "default" is not fenced`},
		{"whole tablegroups", fenced(whole(tgDefault), whole(tgSrc)), importRule, ""},
		{"shards", fenced(onShard(tgDefault, shardDefault), onShard(tgSrc, shardSrc)), importRule, ""},

		// commerce's 0-80 and 80-inf both overlap srctg's 0-inf.
		{
			"every overlapping source shard, one missing",
			fenced(onShard(tgCommerce, shardC0080), whole(tgSrc)),
			change(importOp, spec("commerce", "srctg", "0-inf")),
			`shard "80-inf" of tablegroup "commerce" is not fenced`,
		},
		{
			"every overlapping source shard",
			fenced(onShard(tgCommerce, shardC0080), onShard(tgCommerce, shardC80Inf), whole(tgSrc)),
			change(importOp, spec("commerce", "srctg", "0-inf")),
			"",
		},
		// A split of commerce's 0-80 overlaps only that source shard; each
		// target shard needs its own fence.
		{
			"a split needs only the shard it splits",
			fenced(onShard(tgCommerce, shardC0080), onShard(tgCommerce, shardC0040), onShard(tgCommerce, shardC4080)),
			change(importOp, spec("commerce", "commerce", "0-40"), spec("commerce", "commerce", "40-80")),
			"",
		},
		{
			"a split with a target shard unfenced",
			fenced(onShard(tgCommerce, shardC0080), onShard(tgCommerce, shardC0040)),
			change(importOp, spec("commerce", "commerce", "0-40"), spec("commerce", "commerce", "40-80")),
			`shard "40-80" of tablegroup "commerce" is not fenced`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := planRoutingRuleChange(tt.cat, tt.change)
			if tt.missing == "" {
				require.NoError(t, err)
				return
			}
			requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, tt.missing)
		})
	}

	t.Run("only a FENCED fence of the same operation counts", func(t *testing.T) {
		for name, f := range map[string]catalogFence{
			"FENCING":           fenceRow(1, importOp, tgDefault, 0, fenceStateFencing),
			"UNFENCED":          fenceRow(1, importOp, tgDefault, 0, fenceStateUnfenced),
			"another operation": fenceRow(1, otherOp, tgDefault, 0, fenceStateFenced),
		} {
			t.Run(name, func(t *testing.T) {
				cat := fenced(whole(tgSrc))
				cat.fences = append(cat.fences, f)
				_, err := planRoutingRuleChange(cat, importRule)
				requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, `of tablegroup "default" is not fenced`)
			})
		}
	})
}

func TestPlanRoutingRuleChange_RemovedRulesNeedFences(t *testing.T) {
	existing := []routingRule{shardRule(importOp, tgDefault, tgSrc, shardSrc)}

	t.Run("delete", func(t *testing.T) {
		cat := fenced()
		cat.rules = existing
		_, err := planRoutingRuleChange(cat, change(importOp))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, `shard "0-inf" of tablegroup "default" is not fenced`)

		cat = fenced(whole(tgDefault), whole(tgSrc))
		cat.rules = existing
		plan, err := planRoutingRuleChange(cat, change(importOp))
		require.NoError(t, err)
		assert.Empty(t, plan.insert)
	})
	t.Run("a replaced rule's target", func(t *testing.T) {
		// default → srctg becomes default → orders: srctg loses its traffic.
		cat := fenced(whole(tgDefault), whole(tgOrders))
		cat.rules = existing
		_, err := planRoutingRuleChange(cat, change(importOp, spec("default", "orders", "0-inf")))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, `shard "0-inf" of tablegroup "srctg" is not fenced`)

		cat.fences = append(cat.fences, fenceRow(300, importOp, tgSrc, shardSrc, fenceStateFenced))
		_, err = planRoutingRuleChange(cat, change(importOp, spec("default", "orders", "0-inf")))
		require.NoError(t, err)
	})
}

// A change whose result already holds needs no fence, so a retry after the
// operation unfenced still succeeds.
func TestPlanRoutingRuleChange_NoopNeedsNoFences(t *testing.T) {
	cat := fenced()
	cat.rules = []routingRule{shardRule(importOp, tgDefault, tgSrc, shardSrc)}
	retry := change(importOp, spec("default", "srctg", "0-inf"))
	retry.expectedRoutingVersion = testRoutingV0 - 1
	plan, err := planRoutingRuleChange(cat, retry)
	require.NoError(t, err)
	assert.True(t, plan.noop)

	plan, err = planRoutingRuleChange(fenced(), change(importOp))
	require.NoError(t, err)
	assert.True(t, plan.noop, "deleting the rules of an operation that has none")
}

func TestApplyFenceChange(t *testing.T) {
	// A next that is not current+1 shows the fence is stamped with the
	// version writeCatalog passes.
	const next = int64(9)
	shardID := int64(11)

	t.Run("insert", func(t *testing.T) {
		tx := newFakeRoutingTx()
		tx.results[selectFencesSQL] = fenceResult()
		got, err := applyFenceChange(context.Background(), tx, testRoutingCurrent, next, fence(importOp, "commerce", "0-80"))
		require.NoError(t, err)
		assert.Equal(t, catalogRoutingChange, got)
		assert.Equal(t, []fakeRoutingCall{
			{sql: insertFenceSQL, args: []any{importOp, int64(3), &shardID, fenceStateFenced, next}},
		}, tx.writes())
	})
	t.Run("update", func(t *testing.T) {
		tx := newFakeRoutingTx()
		tx.results[selectFencesSQL] = fenceResult([]any{70, importOp, 3, 11, fenceStateFenced, 4, "preparing"})
		got, err := applyFenceChange(context.Background(), tx, testRoutingCurrent, next, unfence(importOp, "commerce", "0-80"))
		require.NoError(t, err)
		assert.Equal(t, catalogRoutingChange, got)
		assert.Equal(t, []fakeRoutingCall{
			{sql: updateFenceSQL, args: []any{int64(70), importOp, fenceStateUnfenced, next}},
		}, tx.writes())
	})
	for name, c := range map[string]fenceChange{
		"already fenced": fence(importOp, "commerce", ""),
		"never fenced":   unfence(importOp, "commerce", "0-80"),
	} {
		t.Run(name+" writes nothing", func(t *testing.T) {
			tx := newFakeRoutingTx()
			got, err := applyFenceChange(context.Background(), tx, testRoutingCurrent, next, c)
			require.NoError(t, err)
			assert.Equal(t, catalogNoSnapshotChange, got)
			assert.Empty(t, tx.writes())
		})
	}
	t.Run("a list writes every row at one version", func(t *testing.T) {
		tx := newFakeRoutingTx()
		tx.results[selectFencesSQL] = fenceResult([]any{70, importOp, 3, 11, fenceStateFenced, 4, "preparing"})
		got, err := applyFenceChange(context.Background(), tx, testRoutingCurrent, next,
			fences(importOp, fenceSpec("commerce", "0-80"), fenceSpec("commerce", "80-inf"), fenceSpec("srctg", "")))
		require.NoError(t, err)
		assert.Equal(t, catalogRoutingChange, got)
		var nilID *int64
		shard12 := int64(12)
		assert.Equal(t, []fakeRoutingCall{
			{sql: insertFenceSQL, args: []any{importOp, int64(3), &shard12, fenceStateFenced, next}},
			{sql: insertFenceSQL, args: []any{importOp, int64(2), nilID, fenceStateFenced, next}},
		}, tx.writes(), "the 0-80 fence already holds and is skipped")
	})
	t.Run("a refused change writes nothing", func(t *testing.T) {
		tx := newFakeRoutingTx()
		tx.results[selectFencesSQL] = fenceResult([]any{70, otherOp, 3, nil, fenceStateFenced, 4, "preparing"})
		_, err := applyFenceChange(context.Background(), tx, testRoutingCurrent, next, fence(importOp, "commerce", "0-80"))
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, `tablegroup "commerce" is fenced by operation `+otherOp)
		assert.Empty(t, tx.writes())
	})
}

func TestLoadRoutingCatalog_Fences(t *testing.T) {
	tx := newFakeRoutingTx()
	tx.results[selectFencesSQL] = fenceResult(
		[]any{70, importOp, 3, 11, fenceStateFenced, 4, "preparing"},
		[]any{71, otherOp, 2, nil, fenceStateUnfenced, 5, opStateDone},
	)
	cat, err := loadRoutingCatalog(context.Background(), tx, testRoutingCurrent, importOp)
	require.NoError(t, err)
	assert.Equal(t, []catalogFence{
		{fenceID: 70, operationID: importOp, scope: fenceScope{tablegroupID: 3, shardID: 11}, state: fenceStateFenced, version: 4, operationState: "preparing"},
		{fenceID: 71, operationID: otherOp, scope: fenceScope{tablegroupID: 2}, state: fenceStateUnfenced, version: 5, operationState: opStateDone},
	}, cat.fences)
}

// fenceWriteResults is the routing store's reads with the given fence rows.
func fenceWriteResults(fences ...[]any) map[string]*sqltypes.Result {
	results := routingCatalogResults()
	results[selectFencesSQL] = fenceResult(fences...)
	return results
}

func TestChangeFence(t *testing.T) {
	t.Run("a fence advances routing_version by one", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		hs := env.withHealthStreamer(t, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
		env.expectCatalogWrite(fenceWriteResults())
		env.expectAdvance(6)

		v, err := env.pm.changeFences(t.Context(), fence(importOp, "commerce", "0-80"))
		require.NoError(t, err)
		assert.Equal(t, int64(6), v.routingVersion)
		assert.Len(t, env.queriesMatching("INSERT INTO multigres.admission_fence"), 1)
		assert.Len(t, env.queriesMatching("UPDATE multigres.catalog_state"), 1, "advanced once, by writeCatalog")
		assert.Len(t, env.queriesMatching("ORDER BY tablegroup_id"), 1, "the resulting snapshot is validated")
		assert.Equal(t, "COMMIT", env.queries[len(env.queries)-1])
		assert.Equal(t, int64(6), hs.getState().RoutingVersion)
	})
	t.Run("an unfence advances routing_version by one", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectCatalogWrite(fenceWriteResults([]any{70, importOp, 3, 11, fenceStateFenced, 4, "preparing"}))
		env.expectAdvance(6)

		v, err := env.pm.changeFences(t.Context(), unfence(importOp, "commerce", "0-80"))
		require.NoError(t, err)
		assert.Equal(t, int64(6), v.routingVersion)
		assert.Len(t, env.queriesMatching("UPDATE multigres.admission_fence"), 1)
		assert.Len(t, env.queriesMatching("UPDATE multigres.catalog_state"), 1)
	})
	t.Run("a no-op writes nothing and returns the current versions", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectCatalogWrite(fenceWriteResults([]any{70, importOp, 3, 11, fenceStateFenced, 4, "preparing"}))

		c := fence(importOp, "commerce", "0-80")
		c.expectedRoutingVersion = 2
		v, err := env.pm.changeFences(t.Context(), c)
		require.NoError(t, err)
		assert.Equal(t, testRoutingCurrent, v)
		assert.Empty(t, env.queriesMatching("admission_fence ("), "no fence inserted")
		assert.Empty(t, env.queriesMatching("UPDATE multigres"))
	})
	t.Run("a stale expected version rolls back", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectCatalogWrite(fenceWriteResults())

		c := fence(importOp, "commerce", "0-80")
		c.expectedRoutingVersion = 4
		_, err := env.pm.changeFences(t.Context(), c)
		requireRoutingCode(t, err, mtrpcpb.Code_FAILED_PRECONDITION, "routing version is 5, expected 4")
		assert.Empty(t, env.queriesMatching("INSERT INTO"))
		assert.Empty(t, env.queriesMatching("UPDATE multigres"))
		assert.Empty(t, env.queriesMatching("COMMIT"))
	})
}

// The fence RPCs are refused by a pooler that is not the catalog primary
// before it opens a transaction.
func TestFenceRPCs_OnlyOnCatalogPrimary(t *testing.T) {
	pm, m := newRoutingTestManager(t, constants.DefaultTableGroup, constants.DefaultShard, false)
	began := false
	m.AddQueryPatternWithCallback("^BEGIN$", mock.MakeQueryResult(nil, nil), func(string) { began = true })
	ctx := context.Background()
	scopes := []*multipoolermanagerdatapb.FenceScopeSpec{{Tablegroup: "commerce"}}

	_, err := pm.FenceScopes(ctx, &multipoolermanagerdatapb.FenceScopesRequest{Database: routingTestDatabase, OperationId: importOp, Scopes: scopes})
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "FenceScopes: %v", err)
	_, err = pm.UnfenceScopes(ctx, &multipoolermanagerdatapb.UnfenceScopesRequest{Database: routingTestDatabase, OperationId: importOp, Scopes: scopes})
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "UnfenceScopes: %v", err)
	_, err = pm.FenceScopes(ctx, &multipoolermanagerdatapb.FenceScopesRequest{Database: "db2", OperationId: importOp, Scopes: scopes})
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err), "another database: %v", err)

	assert.False(t, began, "a refused fence RPC must not open a transaction")
}

func TestFenceScopes_WritesThroughWriteCatalog(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectCatalogWrite(fenceWriteResults())
	env.expectAdvance(6)

	resp, err := env.pm.FenceScopes(t.Context(), &multipoolermanagerdatapb.FenceScopesRequest{
		Database: testTableGroupDB, OperationId: importOp, ExpectedRoutingVersion: 5,
		Scopes: []*multipoolermanagerdatapb.FenceScopeSpec{{Tablegroup: "commerce", Shard: "80-inf"}, {Tablegroup: "srctg"}},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(6), resp.GetRoutingVersion())
	assert.Contains(t, env.queries[2], "FROM multigres.catalog_state FOR UPDATE", "the catalog_state lock comes before any other catalog row")
	assert.Len(t, env.queriesMatching("INSERT INTO multigres.admission_fence"), 2)
	assert.Len(t, env.queriesMatching("UPDATE multigres.catalog_state"), 1, "every fence of the call takes one version")
}
