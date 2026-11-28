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
// A scope has at most one fence row, which exists only while the scope is
// fenced: raising a fence writes FENCED, stamped with the routing_version it
// commits at, and lifting it deletes the row. Like rule writes, fence writes
// are writeCatalog writers, validated here against a routingCatalog loaded
// under the catalog_state row lock. One change fences or unfences a list of
// scopes, all or nothing, at one routing version.
//
// Only an unfinished operation holds fences: a fence can be raised only by an
// operation that has not finished, and every catalog write that finishes an
// operation (moves it to done or failed) deletes its fences in the same
// transaction (deleteOperationFences). Catalog format 2 deleted the fences
// finished operations had left before that. So a fence row in the snapshot
// always means its scope is fenced, in FENCING as in FENCED, and every reader
// uses that one definition (shardingcatalog.TableGroup.Fenced): this
// validation, the pooler's admission gate and the gateway's wait for a fence
// to lift.
//
// The catalog is never fenced against itself. The fence gate guards only
// application work, which reaches postgres through the query service's
// StartRequest; every catalog read and write here (FenceScopes, UnfenceScopes,
// GetRoutingSnapshot, the tablegroup loop, a pooler reading its own fences)
// goes through the internal query service, which never consults the gate. So
// fencing the catalog shard closes it to applications without blocking the
// writes that lift the fence; routing catalog access through the admitted
// query service would deadlock an unfence of the catalog shard.
//
// Primary poolers enforce fences (see the fence gate in poolerserver and
// fence_refresh.go): a pooler whose scope is fenced refuses new application
// work with MTF02 once it has read the fence, which it does on RefreshFences,
// on start and on promotion. FenceScopes does not refresh any pooler yet, so a
// fence it raises is enforced only once each pooler of the scope refreshes. A
// later change raises a list as FENCING, refreshes and drains every pooler of
// every scope, and only then writes FENCED; if that fails, it deletes the rows
// it inserted.
//
// A primary pooler drained for a fence records an ack of it in
// multigres.admission_fence_ack (fence_acks.go), which deleting the fence
// deletes in the same transaction. Nothing reads the acks yet.
// ============================================================================

// Fence states, as stored in multigres.admission_fence.state.
const (
	fenceStateFencing = "FENCING"
	fenceStateFenced  = "FENCED"
)

const (
	selectFencesSQL = `SELECT fence_id, operation_id::text, tablegroup_id, shard_id, state, version
FROM multigres.admission_fence ORDER BY fence_id`

	insertFenceSQL = `INSERT INTO multigres.admission_fence (operation_id, tablegroup_id, shard_id, state, version)
VALUES ($1::uuid, $2, $3, $4, $5)`

	updateFenceSQL = `UPDATE multigres.admission_fence SET operation_id = $2::uuid, state = $3, version = $4 WHERE fence_id = $1`

	deleteFenceSQL = `DELETE FROM multigres.admission_fence WHERE fence_id = $1`

	// deleteOperationFencesSQL deletes every fence of an operation.
	deleteOperationFencesSQL = `DELETE FROM multigres.admission_fence WHERE operation_id = $1::uuid`

	// droppingTableGroupFenceHolderSQL finds the operation of a fence on a
	// dropping tablegroup, if any.
	droppingTableGroupFenceHolderSQL = `SELECT f.operation_id::text FROM multigres.admission_fence f
JOIN multigres.tablegroup t USING (tablegroup_id)
WHERE f.tablegroup_id = $1 AND t.state = $2
ORDER BY f.fence_id LIMIT 1`
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

// catalogFence is a multigres.admission_fence row. Its operation has not
// finished (see the file comment), so it holds its scope.
type catalogFence struct {
	fenceID     int64
	operationID string
	scope       fenceScope
	state       string
	version     int64
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
	// fenceID is the row at the scope to update or, for a lift, delete; 0 to
	// insert one.
	fenceID int64
	scope   fenceScope
	// state is the state to write, unset for a lift.
	state string
	// version is the routing version the existing row was set at, 0 for a
	// new row.
	version int64
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
	change.operationID = canonicalOperationID(change.operationID)
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
		if change.lift {
			if _, err := tx.QueryArgs(ctx, deleteFenceSQL, w.fenceID); err != nil {
				return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to delete admission fence")
			}
			continue
		}
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
//
// A fence is checked against the catalog's routing version; a lift only
// against the versions of the fences it lifts (see the comment at the lift).
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
			// fence on is already unfenced as far as it is concerned.
			if !ours {
				continue
			}
			w.state = ""
		} else if ours && existing.state == fenceStateFenced {
			continue
		}
		if existing != nil {
			w.fenceID, w.version = existing.fenceID, existing.version
		}
		plan.writes = append(plan.writes, w)
	}
	if len(plan.writes) == 0 {
		return plan, nil
	}

	if change.lift {
		// Lifting a fence, like deleting rules, must always be possible,
		// including by an operation that has finished, so its fences can be
		// cleaned up. The only hazard is a delayed unfence lifting a fence
		// raised again after the caller last read it, so the lift is checked
		// against the versions the fences it lifts were set at rather than
		// the catalog's routing version: catalog writes that did not touch
		// them do not force a retry.
		for _, w := range plan.writes {
			if w.version > change.expectedRoutingVersion {
				return fencePlan{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
					"the fence on %s was set at routing version %d, after the expected %d",
					cat.describeFenceScope(w.scope), w.version, change.expectedRoutingVersion)
			}
		}
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

	for _, r := range scopes {
		if r.tg.state == "dropping" {
			return fencePlan{}, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %q is being dropped", r.tg.name)
		}
		// A fence of another operation keeps every overlapping scope from
		// being fenced.
		for _, f := range cat.fences {
			if f.operationID != change.operationID && f.scope.overlaps(r.scope) {
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
			&f.state, &f.version); err != nil {
			return nil, mterrors.Wrap(err, "failed to read admission fence")
		}
		f.scope.shardID = derefID(shard)
		fences = append(fences, f)
	}
	return fences, nil
}

// checkDroppingTableGroupUnfenced refuses, in finishDropTableGroup's
// transaction, to delete a dropping tablegroup that is still fenced: the
// fence's operation may still be changing its routing, so the drop fails with
// FAILED_PRECONDITION and stays pending until the operation lifts the fence or
// finishes, which deletes it.
func checkDroppingTableGroupUnfenced(ctx context.Context, tx executor.InternalTx, name string, tablegroupID int64) error {
	res, err := tx.QueryArgs(ctx, droppingTableGroupFenceHolderSQL, tablegroupID, tableGroupStateDropping)
	if err != nil {
		return mterrors.Wrap(err, "failed to read admission fences")
	}
	if len(res.Rows) == 0 {
		return nil
	}
	var holder string
	if err := executor.ScanRow(res.Rows[0], &holder); err != nil {
		return mterrors.Wrap(err, "failed to read admission fence")
	}
	return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
		"tablegroup %q is fenced by operation %s, which has not finished", name, holder)
}

// deleteOperationFences deletes every fence of operationID, in the
// transaction of a catalog write that finishes the operation: a finished
// operation holds no fence (see the file comment). The fences are in the
// snapshot, so the write is a routing change.
func deleteOperationFences(ctx context.Context, tx executor.InternalTx, operationID string) error {
	if _, err := tx.QueryArgs(ctx, deleteOperationFencesSQL, operationID); err != nil {
		return mterrors.Wrapf(err, "failed to delete the admission fences of operation %s", operationID)
	}
	return nil
}
