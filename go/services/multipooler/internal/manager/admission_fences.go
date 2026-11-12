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
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// ============================================================================
// Admission fences (catalog shard only)
//
// An admission fence closes a scope (a whole tablegroup, or one shard of it)
// to application work while the operation that owns it changes where the
// scope's traffic goes. A routing rule changes only while both its sides are
// fenced by its operation (see checkRuleFences), so no two placements of the
// same data accept writes at once.
//
// A scope has at most one fence row. Raising a fence writes FENCED; lifting
// it writes UNFENCED. Every write stamps the row with the routing_version it
// commits at and keeps the row, so a delayed unfence cannot override a later
// fence. Like rule writes, fence writes are writeCatalog writers, validated
// here against a routingCatalog loaded under the catalog_state row lock. One
// change fences or unfences a list of scopes, all or nothing, at one routing
// version.
//
// Nothing enforces fences yet: no pooler reads them, so FENCED records the
// operation's intent and guarantees nothing. A later change raises a list as
// FENCING, refreshes and drains every pooler of every scope, and only then
// writes FENCED.
// ============================================================================

// Fence states, as stored in multigres.admission_fence.state.
const (
	fenceStateFencing  = "FENCING"
	fenceStateFenced   = "FENCED"
	fenceStateUnfenced = "UNFENCED"
)

const (
	// selectFencesSQL reads every fence with its operation's state, which
	// decides whether the fence still holds its scope.
	selectFencesSQL = `SELECT f.fence_id, f.operation_id::text, f.tablegroup_id, f.shard_id, f.state, f.version, o.state
FROM multigres.admission_fence f JOIN multigres.operation o USING (operation_id) ORDER BY f.fence_id`

	insertFenceSQL = `INSERT INTO multigres.admission_fence (operation_id, tablegroup_id, shard_id, state, version)
VALUES ($1::uuid, $2, $3, $4, $5)`

	updateFenceSQL = `UPDATE multigres.admission_fence SET operation_id = $2::uuid, state = $3, version = $4 WHERE fence_id = $1`
)

// fenceScope is the scope of a fence. A zero shardID (NULL) is the whole
// tablegroup.
type fenceScope struct {
	tablegroupID int64
	shardID      int64
}

// overlaps reports whether two scopes share any traffic: the same tablegroup,
// and the same shard or a whole-tablegroup fence on either side.
func (a fenceScope) overlaps(b fenceScope) bool {
	return a.tablegroupID == b.tablegroupID && (a.shardID == 0 || b.shardID == 0 || a.shardID == b.shardID)
}

// catalogFence is a multigres.admission_fence row, with the state of the
// operation that owns it.
type catalogFence struct {
	fenceID        int64
	operationID    string
	scope          fenceScope
	state          string
	version        int64
	operationState string
}

// holds reports whether the fence holds its scope against other operations:
// it is raised (FENCING or FENCED) and its operation has not finished.
func (f catalogFence) holds() bool {
	return f.state != fenceStateUnfenced && !slices.Contains(terminalOperationStates, f.operationState)
}

// fenceScopeSpec names a fence scope: a tablegroup, and a shard of it or ""
// for the whole tablegroup.
type fenceScopeSpec struct {
	tablegroup string
	shard      string
}

// fenceChange raises (lift unset) or lifts operationID's fences on scopes,
// in one catalog write.
type fenceChange struct {
	operationID            string
	scopes                 []fenceScopeSpec
	lift                   bool
	expectedRoutingVersion int64
}

// fenceWrite is one fence row a validated change writes.
type fenceWrite struct {
	// fenceID is the row at the scope to update, 0 to insert one.
	fenceID int64
	scope   fenceScope
	state   string
}

// fencePlan is what a validated fence change writes: the scopes of the change
// whose fence does not already hold, all at one routing version. An empty plan
// is a no-op: nothing is written and the routing version does not change.
type fencePlan struct {
	writes []fenceWrite
}

// changeFences validates change and, when it alters any fence, writes it
// through writeCatalog. It returns the versions after the change; a change
// that already holds writes nothing and returns the current versions.
func (pm *MultipoolerManager) changeFences(ctx context.Context, change fenceChange) (catalogVersions, error) {
	return pm.writeCatalog(ctx, func(ctx context.Context, tx executor.InternalTx, current catalogVersions, next int64) (catalogChange, error) {
		return applyFenceChange(ctx, tx, current, next, change)
	})
}

// applyFenceChange is the catalogWriter body of a fence write: it validates
// change against the catalog at current and writes its fence rows in tx, all
// stamped with next.
func applyFenceChange(ctx context.Context, tx executor.InternalTx, current catalogVersions, next int64, change fenceChange) (catalogChange, error) {
	cat, err := loadRoutingCatalog(ctx, tx, current, change.operationID)
	if err != nil {
		return catalogNoSnapshotChange, err
	}
	plan, err := planFenceChange(cat, change)
	if err != nil {
		return catalogNoSnapshotChange, err
	}
	if len(plan.writes) == 0 {
		return catalogNoSnapshotChange, nil
	}
	for _, w := range plan.writes {
		if w.fenceID == 0 {
			if _, err := tx.QueryArgs(ctx, insertFenceSQL, change.operationID, w.scope.tablegroupID,
				nullableID(w.scope.shardID), w.state, next); err != nil {
				return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to insert admission fence")
			}
		} else {
			if _, err := tx.QueryArgs(ctx, updateFenceSQL, w.fenceID, change.operationID, w.state, next); err != nil {
				return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to update admission fence")
			}
		}
	}
	return catalogRoutingChange, nil
}

// planFenceChange validates change against cat and returns what to write.
// The change is all or nothing: any scope it refuses refuses the whole change.
// Malformed requests fail with INVALID_ARGUMENT; a stale
// expectedRoutingVersion or a change the catalog does not allow fails with
// FAILED_PRECONDITION.
//
// Each scope whose fence already holds is skipped, and a change in which every
// scope already holds returns an empty plan whatever its
// expectedRoutingVersion, so a retry of a change that committed succeeds.
func planFenceChange(cat *routingCatalog, change fenceChange) (fencePlan, error) {
	if err := checkFenceChangeRequest(change); err != nil {
		return fencePlan{}, err
	}

	type resolved struct {
		scope fenceScope
		tg    *catalogTablegroup
	}
	scopes := make([]resolved, 0, len(change.scopes))
	for _, spec := range change.scopes {
		tg := cat.tablegroupByName(spec.tablegroup)
		if tg == nil {
			return fencePlan{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %q does not exist", spec.tablegroup)
		}
		scope := fenceScope{tablegroupID: tg.id}
		if spec.shard != "" {
			shard := cat.shardByName(tg.id, spec.shard)
			if shard == nil {
				return fencePlan{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
					"shard %q of tablegroup %q does not exist", spec.shard, spec.tablegroup)
			}
			scope.shardID = shard.id
		}
		scopes = append(scopes, resolved{scope: scope, tg: tg})
	}

	var plan fencePlan
	for _, r := range scopes {
		existing := cat.fenceAt(r.scope)
		ours := existing != nil && existing.operationID == change.operationID
		w := fenceWrite{scope: r.scope, state: fenceStateFenced}
		if change.lift {
			// An operation lifts only its own fences; a scope it holds no
			// raised fence on is already unfenced as far as it is concerned.
			if !ours || existing.state == fenceStateUnfenced {
				continue
			}
			w.state = fenceStateUnfenced
		} else if ours && existing.state == fenceStateFenced {
			continue
		}
		if existing != nil {
			w.fenceID = existing.fenceID
		}
		plan.writes = append(plan.writes, w)
	}
	if len(plan.writes) == 0 {
		return plan, nil
	}

	if cat.routingVersion != change.expectedRoutingVersion {
		return fencePlan{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"routing version is %d, expected %d", cat.routingVersion, change.expectedRoutingVersion)
	}

	if cat.operation == nil {
		return fencePlan{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "operation %s does not exist", change.operationID)
	}
	if slices.Contains(terminalOperationStates, cat.operation.state) {
		return fencePlan{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"operation %s is %s and may not change fences", change.operationID, cat.operation.state)
	}
	if change.lift {
		// Lifting a fence, like deleting rules, must always be possible.
		return plan, nil
	}

	for _, r := range scopes {
		if r.tg.state == "dropping" {
			return fencePlan{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %q is being dropped", r.tg.name)
		}
		// A fence of another operation that still holds its scope keeps
		// every overlapping scope from being fenced. One that no longer
		// holds it, at this exact scope, is taken over.
		for _, f := range cat.fences {
			if f.operationID != change.operationID && f.holds() && f.scope.overlaps(r.scope) {
				return fencePlan{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
					"%s is fenced by operation %s, which has not finished", cat.describeFenceScope(f.scope), f.operationID)
			}
		}
	}
	return plan, nil
}

// checkFenceChangeRequest checks the shape of a change, before it is resolved
// against the catalog: a valid operation id, at least one scope, each naming a
// tablegroup, and no two scopes that overlap (the same scope twice, or a whole
// tablegroup and one of its shards).
func checkFenceChangeRequest(change fenceChange) error {
	if err := checkRuleChangeRequest(routingRuleChange{operationID: change.operationID}); err != nil {
		return err
	}
	if len(change.scopes) == 0 {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "no fence scopes")
	}
	for i, a := range change.scopes {
		if a.tablegroup == "" {
			return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "a fence scope needs a tablegroup, got %+v", a)
		}
		for _, b := range change.scopes[:i] {
			if a.tablegroup == b.tablegroup && (a.shard == "" || b.shard == "" || a.shard == b.shard) {
				return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "fence scopes %s and %s overlap", b, a)
			}
		}
	}
	return nil
}

// String names the scope for error messages.
func (s fenceScopeSpec) String() string {
	if s.shard == "" {
		return fmt.Sprintf("tablegroup %q", s.tablegroup)
	}
	return fmt.Sprintf("shard %q of tablegroup %q", s.shard, s.tablegroup)
}

// checkRuleFences checks that every rule a change inserts or removes is
// fenced on both sides by a FENCED fence of operationID:
//
//   - its source tablegroup, by a fence on the whole tablegroup or on every
//     active shard of it that overlaps the rule's target shard;
//   - its target, by a fence on the whole target tablegroup or on the target
//     shard.
//
// A table rule needs whole-tablegroup fences on both sides.
func (cat *routingCatalog) checkRuleFences(operationID string, rules ...[]routingRule) error {
	for _, set := range rules {
		for _, r := range set {
			if err := cat.checkSourceFenced(operationID, r); err != nil {
				return err
			}
			if cat.fenced(operationID, fenceScope{tablegroupID: r.toTablegroupID}) {
				continue
			}
			target := fenceScope{tablegroupID: r.toTablegroupID, shardID: r.toShardID}
			if r.scope != routingScopeTablegroup || !cat.fenced(operationID, target) {
				return cat.notFenced(operationID, target)
			}
		}
	}
	return nil
}

// checkSourceFenced checks the source side of r for checkRuleFences.
func (cat *routingCatalog) checkSourceFenced(operationID string, r routingRule) error {
	whole := fenceScope{tablegroupID: r.fromTablegroupID}
	if cat.fenced(operationID, whole) {
		return nil
	}
	if r.scope != routingScopeTablegroup {
		return cat.notFenced(operationID, whole)
	}
	target := cat.shards[r.toShardID]
	var sources []*catalogShard
	for _, s := range cat.shards {
		if s.tablegroupID == r.fromTablegroupID && s.state == shardStateActive && keyrange.Overlaps(s.keyRange, target.keyRange) {
			sources = append(sources, s)
		}
	}
	slices.SortFunc(sources, func(a, b *catalogShard) int { return cmp.Compare(a.id, b.id) })
	for _, s := range sources {
		scope := fenceScope{tablegroupID: r.fromTablegroupID, shardID: s.id}
		if !cat.fenced(operationID, scope) {
			return cat.notFenced(operationID, scope)
		}
	}
	return nil
}

// fenced reports whether operationID holds a FENCED fence on exactly scope.
func (cat *routingCatalog) fenced(operationID string, scope fenceScope) bool {
	f := cat.fenceAt(scope)
	return f != nil && f.operationID == operationID && f.state == fenceStateFenced
}

func (cat *routingCatalog) notFenced(operationID string, scope fenceScope) error {
	return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
		"%s is not fenced by operation %s; fence it with FenceScopes before changing its routing rules",
		cat.describeFenceScope(scope), operationID)
}

// fenceAt returns the fence row at exactly scope, or nil.
func (cat *routingCatalog) fenceAt(scope fenceScope) *catalogFence {
	for i := range cat.fences {
		if cat.fences[i].scope == scope {
			return &cat.fences[i]
		}
	}
	return nil
}

// describeFenceScope names scope for error messages.
func (cat *routingCatalog) describeFenceScope(scope fenceScope) string {
	desc := fmt.Sprintf("tablegroup %q", cat.tablegroups[scope.tablegroupID].name)
	if s, ok := cat.shards[scope.shardID]; ok {
		desc = fmt.Sprintf("shard %q of %s", s.name, desc)
	}
	return desc
}

// readFences returns every fence, in fence_id order.
func readFences(ctx context.Context, tx executor.InternalTx) ([]catalogFence, error) {
	result, err := tx.Query(ctx, selectFencesSQL)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read admission fences")
	}
	fences := make([]catalogFence, 0, len(result.Rows))
	for _, row := range result.Rows {
		var f catalogFence
		var shard *int64
		if err := executor.ScanRow(row, &f.fenceID, &f.operationID, &f.scope.tablegroupID, &shard,
			&f.state, &f.version, &f.operationState); err != nil {
			return nil, mterrors.Wrap(err, "failed to read admission fence")
		}
		f.scope.shardID = derefID(shard)
		fences = append(fences, f)
	}
	return fences, nil
}
