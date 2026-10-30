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
	"slices"

	"github.com/google/uuid"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// ============================================================================
// Routing rules (catalog shard only)
//
// A routing rule overrides where the catalog sends traffic while a placement
// operation moves data: traffic the catalog would send to one shard is served
// by another, and every rule belongs to the operation that created it. This
// file holds the write-time validation, as pure functions over a routingCatalog loaded under
// the catalog_state row lock; routing_store.go reads and writes the rows.
//
// Every rule write replaces the whole rule set of one operation: setting rules,
// a switch that swaps them, and deleting them (an empty set) are the same
// change. Validation looks at the rule set the change leaves behind, so a swap
// is checked against its result rather than against the rules it removes.
// ============================================================================

// Rule scopes, as stored in multigres.routing_rule.scope.
const (
	routingScopeTablegroup = "tablegroup"
	routingScopeTable      = "table"
)

// importOperationKind is the multigres.operation.kind of an import. It is the
// only kind that may route the default tablegroup (see validateRuleChange).
const importOperationKind = "import"

// terminalOperationStates are the multigres.operation states that may no
// longer change routing. selectOtherOperationOnTablegroupSQL
// (routing_store.go) lists them too, since the internal query service takes no
// array arguments; keep the two in sync.
var terminalOperationStates = []string{"completed", "cancelled", "failed"}

// catalogTablegroup is a multigres.tablegroup row.
type catalogTablegroup struct {
	id    int64
	name  string
	state string
}

// catalogShard is a multigres.shard row.
type catalogShard struct {
	id           int64
	tablegroupID int64
	name         string
	state        string
	keyRange     *clustermetadatapb.KeyRange
}

// routingOperation is the multigres.operation row of the operation a change
// belongs to.
type routingOperation struct {
	kind  string
	state string
}

// routingRule is a multigres.routing_rule row. Zero ids are NULL. A rule that
// is not yet stored has ruleID 0.
type routingRule struct {
	ruleID           int64
	operationID      string
	scope            string
	fromTablegroupID int64
	fromTableID      int64
	toTablegroupID   int64
	toShardID        int64
	toShardKeyID     int64
	createdVersion   int64
}

// routingCatalog is the part of the catalog that rule validation reads, as
// loaded inside the writing transaction (see loadRoutingCatalog).
type routingCatalog struct {
	routingVersion int64
	planEpoch      int64

	tablegroups map[int64]*catalogTablegroup
	shards      map[int64]*catalogShard
	rules       []routingRule

	// defaultTableCount is the number of tablegroup_table rows in the default
	// tablegroup.
	defaultTableCount int

	// operation is the changed operation's row, or nil if it does not exist.
	operation *routingOperation
}

// shardRuleSpec is a requested shard rule: the routing identifiers of
// fromTablegroup in toShard's range are served by toShard of toTablegroup.
type shardRuleSpec struct {
	fromTablegroup string
	toTablegroup   string
	toShard        string
}

// routingRuleChange replaces every rule of operationID with rules. An empty
// rules deletes them.
type routingRuleChange struct {
	operationID            string
	rules                  []shardRuleSpec
	expectedRoutingVersion int64
}

// routingRulePlan is what a validated change writes.
type routingRulePlan struct {
	// noop is set when the operation already has exactly the requested rules:
	// nothing is written and the routing version does not change.
	noop bool
	// insert are the operation's new rules. The operation's existing rules
	// are deleted first.
	insert []routingRule
	// bumpPlanEpoch is set when the change alters what plans may assume
	// (see needsPlanEpochBump).
	bumpPlanEpoch bool
}

// planRoutingRuleChange validates change against cat and returns what to
// write. Malformed requests fail with INVALID_ARGUMENT; a stale
// expectedRoutingVersion or a change the catalog does not allow fails with
// FAILED_PRECONDITION.
//
// A change whose result already holds returns a noop plan whatever its
// expectedRoutingVersion, so a retry of a change that committed succeeds.
func planRoutingRuleChange(cat *routingCatalog, change routingRuleChange) (routingRulePlan, error) {
	if err := checkRuleChangeRequest(change); err != nil {
		return routingRulePlan{}, err
	}

	insert := make([]routingRule, 0, len(change.rules))
	for _, spec := range change.rules {
		rule, err := cat.resolveShardRule(change.operationID, spec)
		if err != nil {
			return routingRulePlan{}, err
		}
		insert = append(insert, rule)
	}

	var kept, removed []routingRule
	for _, r := range cat.rules {
		if r.operationID == change.operationID {
			removed = append(removed, r)
		} else {
			kept = append(kept, r)
		}
	}
	if sameRuleTargets(removed, insert) {
		return routingRulePlan{noop: true}, nil
	}

	if cat.routingVersion != change.expectedRoutingVersion {
		return routingRulePlan{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"routing version is %d, expected %d", cat.routingVersion, change.expectedRoutingVersion)
	}

	if len(insert) > 0 {
		if err := cat.checkNewRules(insert); err != nil {
			return routingRulePlan{}, err
		}
	}
	if err := cat.checkRuleSet(kept, insert); err != nil {
		return routingRulePlan{}, err
	}

	return routingRulePlan{
		insert:        insert,
		bumpPlanEpoch: cat.needsPlanEpochBump(removed) || cat.needsPlanEpochBump(insert),
	}, nil
}

// checkRuleChangeRequest checks the shape of a change, before it is resolved
// against the catalog.
func checkRuleChangeRequest(change routingRuleChange) error {
	if _, err := uuid.Parse(change.operationID); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "invalid operation id %q: %v", change.operationID, err)
	}
	seen := make(map[shardRuleSpec]bool, len(change.rules))
	for _, spec := range change.rules {
		if spec.fromTablegroup == "" || spec.toTablegroup == "" || spec.toShard == "" {
			return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
				"a routing rule needs a source tablegroup, a target tablegroup and a target shard, got %+v", spec)
		}
		if seen[spec] {
			return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
				"duplicate routing rule from tablegroup %q to %s/%s", spec.fromTablegroup, spec.toTablegroup, spec.toShard)
		}
		seen[spec] = true
	}
	return nil
}

// resolveShardRule turns a spec's names into the ids of a shard rule of
// operationID.
func (cat *routingCatalog) resolveShardRule(operationID string, spec shardRuleSpec) (routingRule, error) {
	from := cat.tablegroupByName(spec.fromTablegroup)
	if from == nil {
		return routingRule{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %q does not exist", spec.fromTablegroup)
	}
	to := cat.tablegroupByName(spec.toTablegroup)
	if to == nil {
		return routingRule{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %q does not exist", spec.toTablegroup)
	}
	shard := cat.shardByName(to.id, spec.toShard)
	if shard == nil {
		return routingRule{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"shard %q of tablegroup %q does not exist", spec.toShard, spec.toTablegroup)
	}
	// A rule to one of its source's own active shards would redirect that
	// shard's traffic to where the catalog already sends it.
	if from.id == to.id && shard.state == "active" {
		return routingRule{}, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"the rule routes shard %q of tablegroup %q to itself", spec.toShard, spec.toTablegroup)
	}
	return routingRule{
		operationID:      operationID,
		scope:            routingScopeTablegroup,
		fromTablegroupID: from.id,
		toTablegroupID:   to.id,
		toShardID:        shard.id,
	}, nil
}

// checkNewRules checks what only the rules being written must satisfy: the
// operation may switch, the placements involved are usable, and the default
// tablegroup is routed only by an import of a database with no tables.
func (cat *routingCatalog) checkNewRules(insert []routingRule) error {
	op := cat.operation
	opID := insert[0].operationID
	if op == nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "operation %s does not exist", opID)
	}
	if slices.Contains(terminalOperationStates, op.state) {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"operation %s is %s and may not change routing", opID, op.state)
	}

	for _, r := range insert {
		from, to := cat.tablegroups[r.fromTablegroupID], cat.tablegroups[r.toTablegroupID]
		for _, tg := range []*catalogTablegroup{from, to} {
			if tg.state == "dropping" {
				return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %q is being dropped", tg.name)
			}
		}
		if shard := cat.shards[r.toShardID]; shard.state == "retired" {
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"shard %q of tablegroup %q is retired", shard.name, to.name)
		}

		// The default tablegroup holds the catalog and is never a rule's
		// source, with one exception: an import. Before it switches,
		// all of a database's traffic, including statements that name no
		// table, must reach the source database, and the gateway resolves no
		// objects yet. It is allowed only while no table is registered in the
		// default tablegroup, so no table's placement is overridden by it.
		//
		// Two obligations on other catalog writers keep this sound:
		//   - Everything this validation reads (tablegroup and shard states,
		//     operation states, tablegroup_table rows) must be written under
		//     the catalog_state row lock (lockRoutingStateSQL), as rule
		//     writes and ensureImportTarget are, or a concurrent write can
		//     slip past a check.
		//   - The reverse of this guard: while a rule from the default
		//     tablegroup exists, registering a table in the default
		//     tablegroup must be refused, or the rule silently overrides the
		//     new table's placement.
		if from.name == constants.DefaultTableGroup {
			if op.kind != importOperationKind {
				return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
					"only an import may route the %s tablegroup; operation %s is a %s",
					constants.DefaultTableGroup, opID, op.kind)
			}
			if cat.defaultTableCount > 0 {
				return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
					"the %s tablegroup has %d registered tables and cannot be routed",
					constants.DefaultTableGroup, cat.defaultTableCount)
			}
		}
	}

	return cat.checkExactCover(insert)
}

// checkExactCover checks that, for each source tablegroup, the target shards
// of an operation's rules are contiguous and cover exactly the source's
// active shards they overlap: no gap and no overlap (the exact-cover
// invariant).
func (cat *routingCatalog) checkExactCover(rules []routingRule) error {
	targetsByFrom := map[int64][]*catalogShard{}
	for _, r := range rules {
		targetsByFrom[r.fromTablegroupID] = append(targetsByFrom[r.fromTablegroupID], cat.shards[r.toShardID])
	}
	for fromID, targets := range targetsByFrom {
		from := cat.tablegroups[fromID]
		var sources []*catalogShard
		for _, s := range cat.shards {
			if s.tablegroupID != fromID || s.state != "active" {
				continue
			}
			if slices.ContainsFunc(targets, func(t *catalogShard) bool { return keyrange.Overlaps(s.keyRange, t.keyRange) }) {
				sources = append(sources, s)
			}
		}
		if len(sources) == 0 {
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"no active shard of tablegroup %q overlaps the rules' target shards", from.name)
		}
		sourceSpan, ok := keyrange.Contiguous(shardKeyRanges(sources))
		if !ok {
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"the active shards of tablegroup %q that the rules cover are not contiguous", from.name)
		}
		if targetSpan, ok := keyrange.Contiguous(shardKeyRanges(targets)); !ok || !keyrange.Equal(targetSpan, sourceSpan) {
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"the rules' target shards %s do not exactly cover the shards %s of tablegroup %q",
				shardNames(targets), shardNames(sources), from.name)
		}
	}
	return nil
}

// checkRuleSet checks the invariants of the whole rule set a change leaves
// behind, kept plus insert. A violation is reported only when it involves a
// rule being inserted: the kept rules were valid when written, and a change
// that only removes rules (a revert) must always be possible.
func (cat *routingCatalog) checkRuleSet(kept, insert []routingRule) error {
	all := append(slices.Clone(kept), insert...)
	isNew := func(i int) bool { return i >= len(kept) }
	name := func(id int64) string { return cat.tablegroups[id].name }

	tableRuleTablegroups := map[int64]bool{}
	for _, r := range all {
		if r.scope == routingScopeTable {
			tableRuleTablegroups[r.fromTablegroupID] = true
			tableRuleTablegroups[r.toTablegroupID] = true
		}
	}

	for i, r := range all {
		if r.scope != routingScopeTablegroup {
			continue
		}
		// A tablegroup is not moved by shard rules and table rules at once.
		if isNew(i) {
			for _, id := range []int64{r.fromTablegroupID, r.toTablegroupID} {
				if tableRuleTablegroups[id] {
					return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
						"tablegroup %q is already moved by a table rule", name(id))
				}
			}
		}
		for j, s := range all {
			if j == i || s.scope != routingScopeTablegroup || !(isNew(i) || isNew(j)) {
				continue
			}
			// At most one rule applies to any routing identifier.
			if j > i && s.fromTablegroupID == r.fromTablegroupID &&
				keyrange.Overlaps(cat.shards[r.toShardID].keyRange, cat.shards[s.toShardID].keyRange) {
				return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
					"routing rules of operations %s and %s overlap on tablegroup %q",
					r.operationID, s.operationID, name(r.fromTablegroupID))
			}
			// No chain: a rule's target is never routed again. A rule
			// within one tablegroup (a reshard) sends traffic to shards
			// of its own source, which is not a chain.
			if r.toTablegroupID != r.fromTablegroupID && s.fromTablegroupID == r.toTablegroupID {
				return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
					"tablegroup %q is the target of a routing rule from %q and the source of another; rules do not chain",
					name(r.toTablegroupID), name(r.fromTablegroupID))
			}
		}
	}
	return nil
}

// needsPlanEpochBump reports whether adding or removing rules changes what
// cached plans may assume. Shard rules apply at execute time, except that the
// planner may route a single-shard tablegroup without computing routing
// identifiers, so rules that spread a single-shard tablegroup over several
// shards invalidate those plans. Table rules apply at plan time.
func (cat *routingCatalog) needsPlanEpochBump(rules []routingRule) bool {
	targets := map[int64]int{}
	for _, r := range rules {
		if r.scope == routingScopeTable {
			return true
		}
		targets[r.fromTablegroupID]++
	}
	for fromID, n := range targets {
		if n > 1 && cat.activeShardCount(fromID) == 1 {
			return true
		}
	}
	return false
}

func (cat *routingCatalog) tablegroupByName(name string) *catalogTablegroup {
	for _, tg := range cat.tablegroups {
		if tg.name == name {
			return tg
		}
	}
	return nil
}

func (cat *routingCatalog) shardByName(tablegroupID int64, name string) *catalogShard {
	for _, s := range cat.shards {
		if s.tablegroupID == tablegroupID && s.name == name {
			return s
		}
	}
	return nil
}

func (cat *routingCatalog) activeShardCount(tablegroupID int64) int {
	n := 0
	for _, s := range cat.shards {
		if s.tablegroupID == tablegroupID && s.state == "active" {
			n++
		}
	}
	return n
}

// sameRuleTargets reports whether two rule sets route the same way: the same
// (scope, source, target) triples, ignoring ids, versions and order.
func sameRuleTargets(a, b []routingRule) bool {
	if len(a) != len(b) {
		return false
	}
	type key struct {
		scope                                   string
		fromTablegroup, fromTable, toTablegroup int64
		toShard, toShardKey                     int64
	}
	keyOf := func(r routingRule) key {
		return key{r.scope, r.fromTablegroupID, r.fromTableID, r.toTablegroupID, r.toShardID, r.toShardKeyID}
	}
	counts := map[key]int{}
	for _, r := range a {
		counts[keyOf(r)]++
	}
	for _, r := range b {
		k := keyOf(r)
		if counts[k] == 0 {
			return false
		}
		counts[k]--
	}
	return true
}

func shardKeyRanges(shards []*catalogShard) []*clustermetadatapb.KeyRange {
	ranges := make([]*clustermetadatapb.KeyRange, len(shards))
	for i, s := range shards {
		ranges[i] = s.keyRange
	}
	return ranges
}

// shardNames returns the shards' names, sorted, for error messages.
func shardNames(shards []*catalogShard) []string {
	names := make([]string, len(shards))
	for i, s := range shards {
		names[i] = s.name
	}
	slices.Sort(names)
	return names
}
