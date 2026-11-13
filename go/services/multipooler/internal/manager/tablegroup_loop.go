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
// topology watch and no wake-up on a new operation; the first pass runs
// immediately on start.
//
// Only the primary runs a pass, and every write goes through writeCatalog
// (routing_snapshot.go), which checks the primary again, serializes catalog
// writers on the catalog_state lock, advances and publishes routing_version
// for a change the snapshot contains, and commits. A pooler demoted between
// the check and the commit cannot commit, because its postgres is read-only,
// so the role checks are a courtesy rather than the fence.
const (
	// tableGroupOpsInterval is the pass interval. A pass with nothing pending
	// is two small queries over tiny tables and reads no topology, so the
	// interval is set by how long a new operation may go unnoticed, not by
	// cost. Provisioning takes minutes; a few seconds is invisible.
	tableGroupOpsInterval = 3 * time.Second
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

// runTableGroupOperations runs a pass immediately and then every
// tableGroupOpsInterval until ctx is cancelled. A pass that could not run at
// all (the catalog unreachable, the manager not open yet) is tried again next
// interval; it is logged when the failure starts and when it ends, not on
// every pass, so a long outage does not log every few seconds.
func (pm *MultipoolerManager) runTableGroupOperations(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	var lastErr string
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		err := pm.tableGroupOperationsPass(ctx)
		switch msg := errString(err); {
		case msg != "" && msg != lastErr:
			pm.logger.WarnContext(ctx, "tablegroup operations pass failed", "error", err)
		case msg == "" && lastErr != "":
			pm.logger.InfoContext(ctx, "tablegroup operations pass succeeded again")
		}
		lastErr = errString(err)
		timer.Reset(tableGroupOpsInterval)
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// tableGroupOperationsPass runs one pass if this pooler is the catalog primary
// and returns the error that kept the pass from running or from completing
// anything.
func (pm *MultipoolerManager) tableGroupOperationsPass(ctx context.Context) error {
	if pm.stateManager == nil || pm.stateManager.RoutingRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return nil
	}
	pending, err := pm.reconcileTableGroups(ctx)
	if pending > 0 {
		pm.logger.DebugContext(ctx, "tablegroup operations pending", "count", pending)
	}
	return err
}

// reconcileTableGroups is one pass: every pending creation is failed if
// cancelled, completed if every shard serves, failed if it timed out, and left
// pending otherwise; every dropping tablegroup is deleted once nothing holds
// it. The catalog is read first and topology only when it holds something
// pending, once, for the whole database; every decision is made against that
// snapshot. It returns how many are still pending. An error on one does not
// stop the pass for the others.
func (pm *MultipoolerManager) reconcileTableGroups(ctx context.Context) (int, error) {
	creations, err := pm.listPendingCreations(ctx)
	if err != nil {
		return 0, err
	}
	droppings, err := pm.listDroppingTableGroups(ctx)
	if err != nil {
		return 0, err
	}
	if len(creations) == 0 && len(droppings) == 0 {
		return 0, nil
	}
	records, topoErr := pm.tableGroupPoolerRecords(ctx, "")
	pending := pm.reconcileCreations(ctx, creations, records, topoErr) +
		pm.reconcileDroppings(ctx, droppings, records, topoErr)
	// Each pending item recorded the topology error for GetOperation; the
	// pass reports it too, so the loop logs an outage once when it starts
	// and once when it ends.
	return pending, topoErr
}

// listPendingCreations returns every create_tablegroup operation in state
// creating, oldest first. A row that cannot be read is logged and left out:
// nothing a later pass can do differently.
func (pm *MultipoolerManager) listPendingCreations(ctx context.Context) ([]operationRow, error) {
	qs, err := pm.catalogQueryService()
	if err != nil {
		return nil, err
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	res, err := qs.QueryAdminArgs(execCtx,
		"SELECT "+operationColumns+" FROM multigres.operation WHERE kind = $1 AND state = $2 ORDER BY created_at",
		operationKindCreateTableGroup, operationStateCreating)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to list pending tablegroup creations")
	}
	ops := make([]operationRow, 0, len(res.Rows))
	for _, row := range res.Rows {
		op, err := scanOperationRow(row)
		if err != nil {
			pm.logger.WarnContext(ctx, "skipping unreadable tablegroup operation", "error", err)
			continue
		}
		ops = append(ops, op)
	}
	return ops, nil
}

// droppingTableGroup is one row of the loop's dropping-tablegroups query: the
// tablegroup and, when a DropTableGroup recorded one, its drop operation. A
// tablegroup dropping because its creation failed has none.
type droppingTableGroup struct {
	id   int64
	name string
	// dropOpID is the drop operation's id, or "" when there is none.
	dropOpID string
	// lastError is the drop operation's recorded last_error, or "".
	lastError string
}

// listDroppingTableGroups returns every tablegroup in state dropping, with its
// drop operation when it has one. A row that cannot be read is logged and left
// out.
func (pm *MultipoolerManager) listDroppingTableGroups(ctx context.Context) ([]droppingTableGroup, error) {
	qs, err := pm.catalogQueryService()
	if err != nil {
		return nil, err
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	res, err := qs.QueryAdminArgs(execCtx,
		"SELECT t.tablegroup_id, t.name, coalesce(o.operation_id::text, ''), coalesce(o.last_error, '') "+
			"FROM multigres.tablegroup t LEFT JOIN multigres.operation o "+
			"ON o.kind = $2 AND o.state = $3 AND (o.request->>'tablegroup_id')::bigint = t.tablegroup_id "+
			"WHERE t.state = $1 ORDER BY t.tablegroup_id",
		tableGroupStateDropping, operationKindDropTableGroup, operationStateDropping)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to list dropping tablegroups")
	}
	groups := make([]droppingTableGroup, 0, len(res.Rows))
	for _, row := range res.Rows {
		var tg droppingTableGroup
		if err := executor.ScanRow(row, &tg.id, &tg.name, &tg.dropOpID, &tg.lastError); err != nil {
			pm.logger.WarnContext(ctx, "skipping unreadable dropping tablegroup", "error", err)
			continue
		}
		groups = append(groups, tg)
	}
	return groups, nil
}

// reconcileCreations advances every pending creation in ops against the
// topology snapshot records and returns how many are still pending. topoErr
// is the error of this pass's topology read; while it is set nothing
// completes, since a partial read undercounts, and the error is recorded on
// each pending operation for GetOperation to show.
func (pm *MultipoolerManager) reconcileCreations(ctx context.Context, ops []operationRow, records []*clustermetadatapb.Multipooler, topoErr error) int {
	pending := 0
	for _, op := range ops {
		var err error
		switch {
		case op.cancelRequested:
			err = pm.failCreateTableGroup(ctx, op, "cancelled")
		case topoErr != nil:
			pm.recordOperationError(ctx, op.id, operationStateCreating, op.lastError, topoErr.Error())
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
	return pending
}

// completeCreateTableGroup activates a tablegroup in one catalog write: the
// operation to done, its shards to active, the tablegroup to active. The
// first statement is a compare-and-set on the operation's state and
// cancellation flag, so a concurrent or repeated completion finds nothing to
// do and reports no change, and a cancel that landed after this pass read the
// operation wins: the operation stays creating with the flag set, and the next
// pass fails it. writeCatalog advances routing_version and publishes it, so
// gateways learn the shard map changed.
func (pm *MultipoolerManager) completeCreateTableGroup(ctx context.Context, op operationRow) error {
	versions, err := pm.writeCatalog(ctx, func(ctx context.Context, tx executor.InternalTx, _ catalogVersions, _ int64) (catalogChange, error) {
		res, err := tx.QueryArgs(ctx,
			"UPDATE multigres.operation SET state = $2, last_error = NULL, updated_at = now() WHERE operation_id = $1 AND state = $3 AND NOT cancel_requested RETURNING operation_id",
			op.id, operationStateDone, operationStateCreating)
		if err != nil {
			return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to complete operation")
		}
		if len(res.Rows) == 0 {
			return catalogNoSnapshotChange, nil // completed, failed or cancelled by another pass or caller
		}
		if _, err := tx.QueryArgs(ctx,
			"UPDATE multigres.shard SET state = $2 WHERE tablegroup_id = $1 AND state = $3",
			op.request.TableGroupID, shardStateActive, shardStatePreparing); err != nil {
			return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to activate shards")
		}
		if _, err := tx.QueryArgs(ctx,
			"UPDATE multigres.tablegroup SET state = $2 WHERE tablegroup_id = $1 AND state = $3",
			op.request.TableGroupID, tableGroupStateActive, tableGroupStateCreating); err != nil {
			return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to activate tablegroup")
		}
		pm.logger.InfoContext(ctx, "tablegroup is active", "tablegroup", op.request.Name, "tablegroup_id", op.request.TableGroupID, "operation_id", op.id)
		return catalogRoutingChange, nil
	})
	if err != nil {
		return err
	}
	pm.logger.DebugContext(ctx, "tablegroup activation committed", "operation_id", op.id, "routing_version", versions.routingVersion)
	return nil
}

// failCreateTableGroup fails a creation in one catalog write: the operation to
// failed with the reason, and the tablegroup to dropping, which tells the
// provisioner to remove whatever it provisioned. The rows are deleted by
// reconcileDroppings once no pooler record and no claim remain, and that is
// what frees the name for a retry. The operation keeps its request, so it
// stays inspectable. The first statement is a compare-and-set, as in
// completeCreateTableGroup. The tablegroup's state is in the snapshot, so
// this is a routing change.
func (pm *MultipoolerManager) failCreateTableGroup(ctx context.Context, op operationRow, reason string) error {
	_, err := pm.writeCatalog(ctx, func(ctx context.Context, tx executor.InternalTx, _ catalogVersions, _ int64) (catalogChange, error) {
		res, err := tx.QueryArgs(ctx,
			"UPDATE multigres.operation SET state = $2, last_error = $3, updated_at = now() WHERE operation_id = $1 AND state = $4 RETURNING operation_id",
			op.id, operationStateFailed, reason, operationStateCreating)
		if err != nil {
			return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to fail operation")
		}
		if len(res.Rows) == 0 {
			return catalogNoSnapshotChange, nil
		}
		if _, err := tx.QueryArgs(ctx,
			"UPDATE multigres.tablegroup SET state = $2 WHERE tablegroup_id = $1 AND state = $3",
			op.request.TableGroupID, tableGroupStateDropping, tableGroupStateCreating); err != nil {
			return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to mark tablegroup dropping")
		}
		pm.logger.WarnContext(ctx, "tablegroup creation failed", "tablegroup", op.request.Name, "tablegroup_id", op.request.TableGroupID, "operation_id", op.id, "reason", reason)
		return catalogRoutingChange, nil
	})
	return err
}

// reconcileDroppings deletes every dropping tablegroup in groups that nothing
// holds any more: no pooler record in the topology snapshot carries its id,
// and no provisioner holds a claim on it
// (topoclient.Store.ClaimTableGroupProvisioning). It returns how many are
// still held. A tablegroup is dropping after DropTableGroup or after its
// creation failed. While topoErr is set nothing is deleted: a partial read
// could miss a record. An error recorded on the drop operation by an earlier
// pass is cleared once a pass gets past it, as reconcileCreations does for a
// creation.
func (pm *MultipoolerManager) reconcileDroppings(ctx context.Context, groups []droppingTableGroup, records []*clustermetadatapb.Multipooler, topoErr error) int {
	pending := 0
	for _, tg := range groups {
		if topoErr != nil {
			pm.recordDropError(ctx, tg, topoErr.Error())
			pending++
			continue
		}
		if poolersRegistered(records, tg.id) > 0 {
			pm.clearDropError(ctx, tg)
			pending++
			continue
		}
		held, err := pm.tableGroupClaimHeld(ctx, tg.id)
		if err != nil {
			pm.recordDropError(ctx, tg, err.Error())
			pending++
			continue
		}
		if held {
			pm.clearDropError(ctx, tg)
			pending++
			continue
		}
		if err := pm.finishDropTableGroup(ctx, tg.name, tg.id); err != nil {
			// Recorded on the drop operation too, so a drop that cannot finish
			// (a row still referencing the tablegroup, say) is visible to
			// whoever waits on it, not only in this log.
			pm.logger.WarnContext(ctx, "dropping tablegroup not deleted", "tablegroup", tg.name, "tablegroup_id", tg.id, "error", err)
			pm.recordDropError(ctx, tg, err.Error())
			pending++
		}
	}
	return pending
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

// finishDropTableGroup deletes a dropping tablegroup in one catalog write:
// its shard rows, its tablegroup row, any drop operation on it to done, and,
// before that commits, each shard's initialization claim in topology, which
// is keyed by name and would otherwise stop the next tablegroup of that name
// from bootstrapping. Deleting the claims inside the write makes them part of
// the retry: a claim that cannot be deleted rolls the write back, the
// tablegroup stays dropping, and the next pass tries again. Doing it after
// the commit would leave nothing to retry from once the rows are gone. Until
// the commit the dropping row keeps the name from being reused, so no claim
// deleted here can belong to a newer tablegroup of the name; the residual case
// is a former primary that still passes writeCatalog's primary check while the
// real primary has finished this drop and a recreated tablegroup has already
// bootstrapped, which needs a split brain lasting through a whole
// provisioning. The tablegroup delete is conditional on the state, so a
// concurrent or repeated pass finds nothing to delete, touches no claim, and
// reports no change. A failed creation has no drop operation; its creation
// operation already reads failed.
func (pm *MultipoolerManager) finishDropTableGroup(ctx context.Context, name string, id int64) error {
	_, err := pm.writeCatalog(ctx, func(ctx context.Context, tx executor.InternalTx, _ catalogVersions, _ int64) (catalogChange, error) {
		// Every shard the tablegroup ever had, retired ones included: each
		// may have bootstrapped and left a claim.
		res, err := tx.QueryArgs(ctx, "SELECT shard_name FROM multigres.shard WHERE tablegroup_id = $1", id)
		if err != nil {
			return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to read shards")
		}
		shards := make([]string, len(res.Rows))
		for i, row := range res.Rows {
			if err := executor.ScanRow(row, &shards[i]); err != nil {
				return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to read shard name")
			}
		}
		if _, err := tx.QueryArgs(ctx,
			"DELETE FROM multigres.shard WHERE tablegroup_id = $1 AND EXISTS (SELECT 1 FROM multigres.tablegroup WHERE tablegroup_id = $1 AND state = $2)",
			id, tableGroupStateDropping); err != nil {
			return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to delete shards")
		}
		res, err = tx.QueryArgs(ctx,
			"DELETE FROM multigres.tablegroup WHERE tablegroup_id = $1 AND state = $2 RETURNING tablegroup_id",
			id, tableGroupStateDropping)
		if err != nil {
			return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to delete tablegroup")
		}
		if len(res.Rows) == 0 {
			return catalogNoSnapshotChange, nil // deleted by another pass
		}
		if _, err := tx.QueryArgs(ctx,
			"UPDATE multigres.operation SET state = $3, last_error = NULL, updated_at = now() WHERE kind = $1 AND state = $2 AND (request->>'tablegroup_id')::bigint = $4",
			operationKindDropTableGroup, operationStateDropping, operationStateDone, id); err != nil {
			return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to complete drop operation")
		}
		database := pm.record.ShardRef().GetDatabase()
		for _, shard := range shards {
			ref := &clustermetadatapb.ShardRef{Database: database, TableGroup: name, Shard: shard}
			if err := pm.topoClient.DeleteShardInitClaim(ctx, ref); err != nil {
				return catalogNoSnapshotChange, mterrors.Wrapf(err, "failed to delete the initialization claim of shard %s", shard)
			}
		}
		pm.logger.InfoContext(ctx, "tablegroup is gone", "tablegroup", name, "tablegroup_id", id)
		return catalogRoutingChange, nil
	})
	return err
}

// writeOperationRow runs one best-effort update of operation rows through
// writeCatalog. Operation rows are outside the snapshot, so the write commits
// without advancing a version, but it still takes the catalog_state lock:
// every catalog write goes through the one path (see routing_snapshot.go). A
// failure is logged and the next pass tries again.
func (pm *MultipoolerManager) writeOperationRow(ctx context.Context, what, query string, args ...any) {
	_, err := pm.writeCatalog(ctx, func(ctx context.Context, tx executor.InternalTx, _ catalogVersions, _ int64) (catalogChange, error) {
		_, err := tx.QueryArgs(ctx, query, args...)
		return catalogNoSnapshotChange, err
	})
	if err != nil {
		pm.logger.WarnContext(ctx, "failed to "+what, "error", err)
	}
}

// recordOperationError stores a transient error on a pending operation so a
// caller polling GetOperation can see why it is not progressing. recorded is
// the operation's last_error as this pass listed it: an error already recorded
// is not written again, so a lasting outage costs one write per operation,
// not one per pass, and takes the catalog_state lock once.
func (pm *MultipoolerManager) recordOperationError(ctx context.Context, operationID, state, recorded, message string) {
	if recorded == message {
		return
	}
	pm.writeOperationRow(ctx, "record operation error "+operationID,
		"UPDATE multigres.operation SET last_error = $2, updated_at = now() WHERE operation_id = $1 AND state = $3 AND last_error IS DISTINCT FROM $2",
		operationID, message, state)
}

// clearOperationError removes a recorded transient error from a pending
// operation once the condition has passed, so GetOperation stops showing it.
func (pm *MultipoolerManager) clearOperationError(ctx context.Context, operationID, state string) {
	pm.writeOperationRow(ctx, "clear operation error "+operationID,
		"UPDATE multigres.operation SET last_error = NULL, updated_at = now() WHERE operation_id = $1 AND state = $2 AND last_error IS NOT NULL",
		operationID, state)
}

// clearDropError is clearOperationError for a dropping tablegroup's drop
// operation, when an earlier pass recorded an error on it.
func (pm *MultipoolerManager) clearDropError(ctx context.Context, tg droppingTableGroup) {
	if tg.dropOpID != "" && tg.lastError != "" {
		pm.clearOperationError(ctx, tg.dropOpID, operationStateDropping)
	}
}

// recordDropError is recordOperationError for a dropping tablegroup's drop
// operation. A tablegroup dropping because its creation failed has none, and
// nothing is written for it: the pass reports the error instead.
func (pm *MultipoolerManager) recordDropError(ctx context.Context, tg droppingTableGroup, message string) {
	if tg.dropOpID == "" {
		return
	}
	pm.recordOperationError(ctx, tg.dropOpID, operationStateDropping, tg.lastError, message)
}
