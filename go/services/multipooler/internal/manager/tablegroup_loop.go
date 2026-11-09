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
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// The tablegroup completion loop.
//
// A creation is complete when every shard in its layout has a pooler
// registered in topology that carries the tablegroup's catalog id and is a
// serving primary. A dropping tablegroup is gone when no pooler record carries
// its id and no provisioner holds a claim on it. The loop below runs on the
// catalog primary, checks both on a timer, and flips the catalog rows when they
// hold. It is level-triggered on purpose: every pass derives everything from
// the catalog and a topology read, keeps nothing in memory between passes, and
// so resumes after a failover or restart by running again. There is no
// topology watch; the first pass runs immediately on start and the timer is
// short while anything is pending.
//
// Only the primary runs a pass. A pooler demoted between the check and the
// commit cannot commit, because its postgres is read-only, so the role check
// is a courtesy rather than the fence.
const (
	// tableGroupOpsIdleInterval is the pass interval while nothing is pending.
	// An idle pass is two small indexed queries that return nothing.
	tableGroupOpsIdleInterval = 30 * time.Second
	// tableGroupOpsActiveInterval is the pass interval while something is
	// pending. Provisioning takes minutes, so a few seconds of latency on
	// completion is invisible.
	tableGroupOpsActiveInterval = 3 * time.Second
	// createTableGroupTimeout is how long a creation may stay pending before
	// the loop fails it. Provisioning that has not produced serving shards in
	// this long is stuck, and a failed operation is inspectable and frees the
	// name for a retry, where a pending one is not.
	createTableGroupTimeout = time.Hour
)

// StartTableGroupOperations opts this manager into running the tablegroup
// completion loop. Called by the service layer, like StartBackupHealth:
// openLocked relaunches the loop on every re-open so it survives Pause/resume
// cycles, and tests that never opt in run no background catalog queries. An
// unmanaged pooler has no sidecar schema to run it against. Idempotent.
func (pm *MultipoolerManager) StartTableGroupOperations() {
	if pm.IsUnmanaged() {
		return
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.tableGroupOpsEnabled {
		return
	}
	pm.tableGroupOpsEnabled = true
	pm.startTableGroupOperationsLocked()
}

// startTableGroupOperationsLocked launches the loop on the current pm.ctx.
// Caller must hold pm.mu. Only the catalog shard has the tables, so any other
// shard skips it entirely.
func (pm *MultipoolerManager) startTableGroupOperationsLocked() {
	if !pm.isCatalogShard() {
		return
	}
	go pm.runTableGroupOperations(pm.ctx)
}

// runTableGroupOperations runs passes until ctx is cancelled, with the
// interval chosen by whether the last pass found pending work.
func (pm *MultipoolerManager) runTableGroupOperations(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		interval := tableGroupOpsIdleInterval
		if pm.tableGroupOperationsPass(ctx) > 0 {
			interval = tableGroupOpsActiveInterval
		}
		timer.Reset(interval)
	}
}

// tableGroupOperationsPass runs one pass if this pooler is the catalog primary
// and returns how much work is still pending. A pass that could not run at all
// (the catalog unreachable, the manager not open yet) is logged and retried at
// the idle interval, so a persistent failure does not log every few seconds.
func (pm *MultipoolerManager) tableGroupOperationsPass(ctx context.Context) int {
	if pm.stateManager == nil || pm.stateManager.RoutingRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return 0
	}
	pending, err := pm.reconcileTableGroups(ctx)
	if err != nil {
		pm.logger.WarnContext(ctx, "tablegroup operations pass failed", "error", err)
	}
	return pending
}

// reconcileTableGroups is one pass: every pending creation is failed if
// cancelled, completed if every shard serves, failed if it timed out, and left
// pending otherwise; every dropping tablegroup is deleted once nothing holds
// it. Topology is read once per pass, for the whole database, and every
// decision is made against that snapshot. It returns how many are still
// pending. An error on one does not stop the pass for the others.
func (pm *MultipoolerManager) reconcileTableGroups(ctx context.Context) (int, error) {
	records, topoErr := pm.tableGroupPoolerRecords(ctx, "")
	pending, err := pm.reconcileCreations(ctx, records, topoErr)
	if err != nil {
		return pending, err
	}
	dropping, err := pm.reconcileDroppings(ctx, records, topoErr)
	return pending + dropping, err
}

// reconcileCreations advances every create_tablegroup operation in state
// creating. topoErr is the error of this pass's topology read; while it is set
// nothing completes, since a partial read undercounts, and the error is
// recorded on each pending operation for GetOperation to show.
func (pm *MultipoolerManager) reconcileCreations(ctx context.Context, records []*clustermetadatapb.Multipooler, topoErr error) (int, error) {
	qs, err := pm.catalogQueryService()
	if err != nil {
		return 0, err
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	res, err := qs.QueryAdminArgs(execCtx,
		"SELECT "+operationColumns+" FROM multigres.operation WHERE kind = $1 AND state = $2 ORDER BY created_at",
		operationKindCreateTableGroup, operationStateCreating)
	if err != nil {
		return 0, mterrors.Wrap(err, "failed to list pending tablegroup creations")
	}
	pending := 0
	for _, row := range res.Rows {
		op, err := scanOperationRow(row)
		if err != nil {
			// Nothing a later pass can do differently; not counted as pending
			// so it does not keep the loop on its short interval.
			pm.logger.WarnContext(ctx, "skipping unreadable tablegroup operation", "error", err)
			continue
		}
		switch {
		case op.cancelRequested:
			err = pm.failCreateTableGroup(ctx, op, "cancelled")
		case topoErr != nil:
			pm.recordOperationError(ctx, op.id, operationStateCreating, topoErr.Error())
			pending++
		case shardsServing(records, op.request) == len(op.request.Shards):
			err = pm.completeCreateTableGroup(ctx, op)
		case time.Since(op.createdAt) > createTableGroupTimeout:
			err = pm.failCreateTableGroup(ctx, op, fmt.Sprintf("timed out after %s waiting for every shard to serve", createTableGroupTimeout))
		default:
			pm.logger.DebugContext(ctx, "tablegroup creation pending", "operation_id", op.id, "tablegroup", op.request.Name,
				"shards_serving", shardsServing(records, op.request), "shards_desired", len(op.request.Shards), "pooler_records", len(records))
			if op.lastError != "" {
				pm.clearOperationError(ctx, op.id, operationStateCreating)
			}
			pending++
		}
		if err != nil {
			pm.logger.WarnContext(ctx, "tablegroup creation not advanced", "operation_id", op.id, "tablegroup", op.request.Name, "error", err)
			pending++
		}
	}
	return pending, nil
}

// completeCreateTableGroup activates a tablegroup in one transaction: the
// operation to done, its shards to active, the tablegroup to active, and the
// routing version advanced so routing consumers learn the shard map changed.
// The first statement is a compare-and-set on the operation's state, so a
// concurrent or repeated completion finds nothing to do and commits nothing.
func (pm *MultipoolerManager) completeCreateTableGroup(ctx context.Context, op operationRow) error {
	return pm.inCatalogTransaction(ctx, "tablegroup activation", func(execCtx context.Context, tx executor.InternalTx) (bool, error) {
		res, err := tx.QueryArgs(execCtx,
			"UPDATE multigres.operation SET state = $2, last_error = NULL, updated_at = now() WHERE operation_id = $1 AND state = $3 RETURNING operation_id",
			op.id, operationStateDone, operationStateCreating)
		if err != nil {
			return false, mterrors.Wrap(err, "failed to complete operation")
		}
		if len(res.Rows) == 0 {
			return false, nil // already completed or failed by another pass
		}
		if _, err := tx.QueryArgs(execCtx,
			"UPDATE multigres.shard SET state = $2 WHERE tablegroup_id = $1 AND state = $3",
			op.request.TableGroupID, shardStateActive, shardStatePreparing); err != nil {
			return false, mterrors.Wrap(err, "failed to activate shards")
		}
		if _, err := tx.QueryArgs(execCtx,
			"UPDATE multigres.tablegroup SET state = $2 WHERE tablegroup_id = $1 AND state = $3",
			op.request.TableGroupID, tableGroupStateActive, tableGroupStateCreating); err != nil {
			return false, mterrors.Wrap(err, "failed to activate tablegroup")
		}
		if err := advanceRoutingVersion(execCtx, tx); err != nil {
			return false, err
		}
		pm.logger.InfoContext(ctx, "tablegroup is active", "tablegroup", op.request.Name, "tablegroup_id", op.request.TableGroupID, "operation_id", op.id)
		return true, nil
	})
}

// failCreateTableGroup fails a creation in one transaction: the operation to
// failed with the reason, and the tablegroup to dropping, which tells the
// provisioner to remove whatever it provisioned. The rows are deleted by
// reconcileDroppings once no pooler record and no claim remain, and that is
// what frees the name for a retry. The operation keeps its request, so it
// stays inspectable. The first statement is a compare-and-set, as in
// completeCreateTableGroup.
func (pm *MultipoolerManager) failCreateTableGroup(ctx context.Context, op operationRow, reason string) error {
	return pm.inCatalogTransaction(ctx, "tablegroup failure", func(execCtx context.Context, tx executor.InternalTx) (bool, error) {
		res, err := tx.QueryArgs(execCtx,
			"UPDATE multigres.operation SET state = $2, last_error = $3, updated_at = now() WHERE operation_id = $1 AND state = $4 RETURNING operation_id",
			op.id, operationStateFailed, reason, operationStateCreating)
		if err != nil {
			return false, mterrors.Wrap(err, "failed to fail operation")
		}
		if len(res.Rows) == 0 {
			return false, nil
		}
		if _, err := tx.QueryArgs(execCtx,
			"UPDATE multigres.tablegroup SET state = $2 WHERE tablegroup_id = $1 AND state = $3",
			op.request.TableGroupID, tableGroupStateDropping, tableGroupStateCreating); err != nil {
			return false, mterrors.Wrap(err, "failed to mark tablegroup dropping")
		}
		pm.logger.WarnContext(ctx, "tablegroup creation failed", "tablegroup", op.request.Name, "tablegroup_id", op.request.TableGroupID, "operation_id", op.id, "reason", reason)
		return true, nil
	})
}

// reconcileDroppings deletes every dropping tablegroup that nothing holds any
// more: no pooler record in the topology snapshot carries its id, and no
// provisioner holds a claim on it (topoclient.Store.ClaimTableGroupProvisioning).
// A tablegroup is dropping after DropTableGroup or after its creation failed.
// While topoErr is set nothing is deleted: a partial read could miss a record.
func (pm *MultipoolerManager) reconcileDroppings(ctx context.Context, records []*clustermetadatapb.Multipooler, topoErr error) (int, error) {
	qs, err := pm.catalogQueryService()
	if err != nil {
		return 0, err
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	res, err := qs.QueryAdminArgs(execCtx,
		"SELECT tablegroup_id, name FROM multigres.tablegroup WHERE state = $1 ORDER BY tablegroup_id", tableGroupStateDropping)
	if err != nil {
		return 0, mterrors.Wrap(err, "failed to list dropping tablegroups")
	}
	pending := 0
	for _, row := range res.Rows {
		var id int64
		var name string
		if err := executor.ScanRow(row, &id, &name); err != nil {
			pm.logger.WarnContext(ctx, "skipping unreadable dropping tablegroup", "error", err)
			continue
		}
		if topoErr != nil {
			pm.recordDropError(ctx, id, topoErr.Error())
			pending++
			continue
		}
		if poolersRegistered(records, id) > 0 {
			pending++
			continue
		}
		held, err := pm.tableGroupClaimHeld(ctx, id)
		if err != nil {
			pm.recordDropError(ctx, id, err.Error())
			pending++
			continue
		}
		if held {
			pending++
			continue
		}
		if err := pm.finishDropTableGroup(ctx, name, id); err != nil {
			pm.logger.WarnContext(ctx, "dropping tablegroup not deleted", "tablegroup", name, "tablegroup_id", id, "error", err)
			pending++
		}
	}
	return pending, nil
}

// tableGroupClaimHeld reports whether a provisioner holds a claim on a
// tablegroup incarnation.
func (pm *MultipoolerManager) tableGroupClaimHeld(ctx context.Context, id int64) (bool, error) {
	claimCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	_, err := pm.topoClient.GetTableGroupProvisionClaim(claimCtx, pm.record.ShardRef().GetDatabase(), id)
	if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
		return false, nil
	}
	if err != nil {
		return true, fmt.Errorf("read provision claim: %w", err)
	}
	return true, nil
}

// finishDropTableGroup deletes a dropping tablegroup in one transaction: its
// shard rows, its tablegroup row, any drop operation on it to done, and the
// routing version advanced. The tablegroup delete is conditional on the state,
// so a concurrent or repeated pass finds nothing to delete and commits
// nothing. A failed creation has no drop operation; its creation operation
// already reads failed.
func (pm *MultipoolerManager) finishDropTableGroup(ctx context.Context, name string, id int64) error {
	return pm.inCatalogTransaction(ctx, "tablegroup deletion", func(execCtx context.Context, tx executor.InternalTx) (bool, error) {
		if _, err := tx.QueryArgs(execCtx,
			"DELETE FROM multigres.shard WHERE tablegroup_id = $1 AND EXISTS (SELECT 1 FROM multigres.tablegroup WHERE tablegroup_id = $1 AND state = $2)",
			id, tableGroupStateDropping); err != nil {
			return false, mterrors.Wrap(err, "failed to delete shards")
		}
		res, err := tx.QueryArgs(execCtx,
			"DELETE FROM multigres.tablegroup WHERE tablegroup_id = $1 AND state = $2 RETURNING tablegroup_id",
			id, tableGroupStateDropping)
		if err != nil {
			return false, mterrors.Wrap(err, "failed to delete tablegroup")
		}
		if len(res.Rows) == 0 {
			return false, nil // deleted by another pass
		}
		if _, err := tx.QueryArgs(execCtx,
			"UPDATE multigres.operation SET state = $3, last_error = NULL, updated_at = now() WHERE kind = $1 AND state = $2 AND (request->>'tablegroup_id')::bigint = $4",
			operationKindDropTableGroup, operationStateDropping, operationStateDone, id); err != nil {
			return false, mterrors.Wrap(err, "failed to complete drop operation")
		}
		if err := advanceRoutingVersion(execCtx, tx); err != nil {
			return false, err
		}
		pm.logger.InfoContext(ctx, "tablegroup is gone", "tablegroup", name, "tablegroup_id", id)
		return true, nil
	})
}

// inCatalogTransaction runs fn in one admin transaction on the catalog and
// commits when fn asks to. fn returns false to roll back without error, for a
// compare-and-set that found nothing to do.
func (pm *MultipoolerManager) inCatalogTransaction(ctx context.Context, what string, fn func(execCtx context.Context, tx executor.InternalTx) (commit bool, err error)) error {
	qs, err := pm.catalogQueryService()
	if err != nil {
		return err
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	tx, err := qs.BeginAdmin(execCtx)
	if err != nil {
		return mterrors.Wrap(err, "failed to begin "+what+" transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	commit, err := fn(execCtx, tx)
	if err != nil || !commit {
		return err
	}
	if err := tx.Commit(execCtx); err != nil {
		return mterrors.Wrap(err, "failed to commit "+what)
	}
	return nil
}

// advanceRoutingVersion bumps catalog_state.routing_version, which tells
// routing consumers the shard map changed.
func advanceRoutingVersion(ctx context.Context, tx executor.InternalTx) error {
	if _, err := tx.Query(ctx, "UPDATE multigres.catalog_state SET routing_version = routing_version + 1"); err != nil {
		return mterrors.Wrap(err, "failed to advance routing version")
	}
	return nil
}

// recordOperationError stores a transient error on a pending operation so a
// caller polling GetOperation can see why it is not progressing. Best effort:
// a failure here is logged and the next pass tries again.
func (pm *MultipoolerManager) recordOperationError(ctx context.Context, operationID, state, message string) {
	qs, err := pm.catalogQueryService()
	if err != nil {
		return
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	if _, err := qs.QueryAdminArgs(execCtx,
		"UPDATE multigres.operation SET last_error = $2, updated_at = now() WHERE operation_id = $1 AND state = $3 AND last_error IS DISTINCT FROM $2",
		operationID, message, state); err != nil {
		pm.logger.WarnContext(ctx, "failed to record operation error", "operation_id", operationID, "error", err)
	}
}

// clearOperationError removes a recorded transient error from a pending
// operation once the condition has passed, so GetOperation stops showing it.
func (pm *MultipoolerManager) clearOperationError(ctx context.Context, operationID, state string) {
	qs, err := pm.catalogQueryService()
	if err != nil {
		return
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	if _, err := qs.QueryAdminArgs(execCtx,
		"UPDATE multigres.operation SET last_error = NULL, updated_at = now() WHERE operation_id = $1 AND state = $2 AND last_error IS NOT NULL",
		operationID, state); err != nil {
		pm.logger.WarnContext(ctx, "failed to clear operation error", "operation_id", operationID, "error", err)
	}
}

// recordDropError is recordOperationError for the drop operation of a
// tablegroup, found by id. A failed creation has none, and is then only logged.
func (pm *MultipoolerManager) recordDropError(ctx context.Context, tableGroupID int64, message string) {
	qs, err := pm.catalogQueryService()
	if err != nil {
		return
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	if _, err := qs.QueryAdminArgs(execCtx,
		"UPDATE multigres.operation SET last_error = $3, updated_at = now() WHERE kind = $1 AND state = $2 AND (request->>'tablegroup_id')::bigint = $4 AND last_error IS DISTINCT FROM $3",
		operationKindDropTableGroup, operationStateDropping, message, tableGroupID); err != nil {
		pm.logger.WarnContext(ctx, "failed to record drop error", "tablegroup_id", tableGroupID, "error", err)
	}
}
