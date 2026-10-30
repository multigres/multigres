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
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// routingTxTimeout bounds one routing RPC's catalog transaction, including the
// wait for the catalog_state row lock held by a concurrent rule write.
const routingTxTimeout = 10 * time.Second

const (
	// Rule writes run READ COMMITTED. Each statement after the catalog_state
	// row lock then sees the rows committed before the lock was granted, which
	// is what validation assumes. Under REPEATABLE READ the lock would raise a
	// serialization failure whenever another writer committed in between.
	setReadCommittedSQL = `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`

	// A snapshot reads the version and the rules from one REPEATABLE READ
	// snapshot, so a change is never seen half-applied.
	setSnapshotIsolationSQL = `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`
)

// requireCatalogPrimary checks that this pooler is the writable primary of
// database's default tablegroup, which holds the catalog and is the only
// place routing may be read or written. A standby's copy is never a source of
// truth.
func (pm *MultipoolerManager) requireCatalogPrimary(database string) error {
	if database == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "database is required")
	}
	key := pm.record.ShardRef()
	if database != key.GetDatabase() {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"database %q does not match this pooler's database %q", database, key.GetDatabase())
	}
	// UNAVAILABLE, not FAILED_PRECONDITION: not being the catalog primary (a
	// pooler of another tablegroup, a standby, a demoted primary, one still
	// being promoted) says nothing about the catalog's state, so the caller
	// rediscovers the catalog primary and retries.
	if !constants.IsCatalogShard(key.GetTableGroup(), key.GetShard()) ||
		pm.stateManager == nil ||
		pm.stateManager.RoutingRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "not the catalog primary pooler")
	}
	return nil
}

// GetRoutingSnapshot returns the database's routing version, plan epoch and
// rules, read from one snapshot of the catalog.
func (pm *MultipoolerManager) GetRoutingSnapshot(ctx context.Context, req *multipoolermanagerdatapb.GetRoutingSnapshotRequest) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error) {
	if err := pm.requireCatalogPrimary(req.GetDatabase()); err != nil {
		return nil, err
	}
	var resp *multipoolermanagerdatapb.GetRoutingSnapshotResponse
	err := pm.inRoutingTx(ctx, setSnapshotIsolationSQL, func(ctx context.Context, tx executor.InternalTx) error {
		var err error
		resp, err = readRoutingSnapshot(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	pm.metrics.recordRoutingRules(ctx, req.GetDatabase(), len(resp.GetRules()))
	return resp, nil
}

// SetRoutingRules replaces every routing rule of an operation.
func (pm *MultipoolerManager) SetRoutingRules(ctx context.Context, req *multipoolermanagerdatapb.SetRoutingRulesRequest) (*multipoolermanagerdatapb.SetRoutingRulesResponse, error) {
	if err := pm.requireCatalogPrimary(req.GetDatabase()); err != nil {
		return nil, err
	}
	change := routingRuleChange{operationID: req.GetOperationId(), expectedRoutingVersion: req.GetExpectedRoutingVersion()}
	for _, r := range req.GetRules() {
		change.rules = append(change.rules, shardRuleSpec{
			fromTablegroup: r.GetFromTablegroup(),
			toTablegroup:   r.GetToTablegroup(),
			toShard:        r.GetToShard(),
		})
	}
	if len(change.rules) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "no routing rules; use DeleteRoutingRules to delete an operation's rules")
	}
	version, err := pm.applyRoutingRuleChange(ctx, req.GetDatabase(), change)
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdatapb.SetRoutingRulesResponse{RoutingVersion: version}, nil
}

// DeleteRoutingRules deletes every routing rule of an operation.
func (pm *MultipoolerManager) DeleteRoutingRules(ctx context.Context, req *multipoolermanagerdatapb.DeleteRoutingRulesRequest) (*multipoolermanagerdatapb.DeleteRoutingRulesResponse, error) {
	if err := pm.requireCatalogPrimary(req.GetDatabase()); err != nil {
		return nil, err
	}
	version, err := pm.applyRoutingRuleChange(ctx, req.GetDatabase(), routingRuleChange{
		operationID:            req.GetOperationId(),
		expectedRoutingVersion: req.GetExpectedRoutingVersion(),
	})
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdatapb.DeleteRoutingRulesResponse{RoutingVersion: version}, nil
}

// applyRoutingRuleChange writes change in its own READ COMMITTED transaction
// and returns the routing version after it.
func (pm *MultipoolerManager) applyRoutingRuleChange(ctx context.Context, database string, change routingRuleChange) (int64, error) {
	var write routingWrite
	err := pm.inRoutingTx(ctx, setReadCommittedSQL, func(ctx context.Context, tx executor.InternalTx) error {
		var err error
		write, err = applyRoutingRuleChange(ctx, tx, change)
		return err
	})
	if err != nil {
		return 0, err
	}
	pm.metrics.recordRoutingRules(ctx, database, write.rules)
	pm.logger.InfoContext(ctx, "routing rules changed",
		"operation_id", change.operationID, "rules", len(change.rules), "routing_version", write.version)
	return write.version, nil
}

// inRoutingTx runs fn in a catalog transaction on the admin pool whose first
// statement sets its isolation level, and commits it when fn succeeds.
func (pm *MultipoolerManager) inRoutingTx(ctx context.Context, isolationSQL string, fn func(context.Context, executor.InternalTx) error) error {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "internal query service unavailable for the routing catalog")
	}
	ctx, cancel := context.WithTimeout(ctx, routingTxTimeout)
	defer cancel()

	tx, err := queryService.BeginAdmin(ctx)
	if err != nil {
		return mterrors.Wrap(err, "failed to begin routing catalog transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Query(ctx, isolationSQL); err != nil {
		return mterrors.Wrap(err, "failed to set routing catalog transaction isolation")
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return mterrors.Wrap(err, "failed to commit routing catalog transaction")
	}
	return nil
}
