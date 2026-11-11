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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// The routing store reads and writes routing rules on the catalog shard's
// primary. Every rule write is a writeCatalog writer (routing_snapshot.go), so
// it runs with the catalog_state row already locked: rule writers are
// serialized with every other catalog writer, the validation in
// routing_rules.go sees a catalog no other writer is changing, and
// writeCatalog advances routing_version when the rules change.

const (
	selectTablegroupsSQL = `SELECT tablegroup_id, name, state FROM multigres.tablegroup`

	// A shard's key range is read from its name, which the catalog keeps
	// equal to keyrange.ShardName of range_start and range_end.
	selectShardsSQL = `SELECT shard_id, tablegroup_id, shard_name, state FROM multigres.shard`

	selectRoutingRulesSQL = `SELECT rule_id, operation_id::text, scope, from_tablegroup_id, from_table_id,
	to_tablegroup_id, to_shard_id, to_shard_key_id, created_version
FROM multigres.routing_rule ORDER BY rule_id`

	countDefaultTablesSQL = `SELECT count(*) FROM multigres.tablegroup_table t
JOIN multigres.tablegroup tg USING (tablegroup_id) WHERE tg.name = $1`

	// FOR SHARE keeps the operation's state from changing until the rule
	// write commits.
	selectOperationSQL = `SELECT kind, state FROM multigres.operation WHERE operation_id = $1::uuid FOR SHARE`

	// selectOtherOperationsSQL finds every other unfinished operation that
	// acts on a tablegroup, with that tablegroup's catalog id. An operation
	// names its tablegroup in its request, as request->>'tablegroup_id'.
	selectOtherOperationsSQL = `SELECT operation_id::text, (request->>'tablegroup_id')::bigint FROM multigres.operation
WHERE operation_id <> $1::uuid AND request->>'tablegroup_id' IS NOT NULL AND ` + unfinishedOperationSQL + `
ORDER BY operation_id`

	deleteOperationRulesSQL = `DELETE FROM multigres.routing_rule WHERE operation_id = $1::uuid`

	insertRoutingRuleSQL = `INSERT INTO multigres.routing_rule
	(operation_id, scope, from_tablegroup_id, from_table_id, to_tablegroup_id, to_shard_id, to_shard_key_id, created_version)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8)`
)

// changeRoutingRules validates change and, when it alters the operation's
// rules, writes it through writeCatalog. It returns the versions after the
// change; a change that is already in effect writes nothing and returns the
// current versions. It is the only entry point for rule writes.
func (pm *MultipoolerManager) changeRoutingRules(ctx context.Context, change routingRuleChange) (catalogVersions, error) {
	return pm.writeCatalog(ctx, func(ctx context.Context, tx executor.InternalTx, current catalogVersions) (catalogChange, error) {
		return applyRoutingRuleChange(ctx, tx, current, change)
	})
}

// applyRoutingRuleChange is the catalogWriter body of a rule write: it
// validates change against the catalog at current and replaces the
// operation's rules in tx, stamped with the version writeCatalog commits them
// at. A change that is already in effect writes nothing and reports
// catalogNoSnapshotChange; one whose plan requires it invalidates cached plans.
func applyRoutingRuleChange(ctx context.Context, tx executor.InternalTx, current catalogVersions, change routingRuleChange) (catalogChange, error) {
	cat, err := loadRoutingCatalog(ctx, tx, current, change.operationID)
	if err != nil {
		return catalogNoSnapshotChange, err
	}
	plan, err := planRoutingRuleChange(cat, change)
	if err != nil {
		return catalogNoSnapshotChange, err
	}
	if plan.noop {
		return catalogNoSnapshotChange, nil
	}

	version := current.routingVersion + 1
	if _, err := tx.QueryArgs(ctx, deleteOperationRulesSQL, change.operationID); err != nil {
		return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to delete routing rules")
	}
	for _, r := range plan.insert {
		if _, err := tx.QueryArgs(ctx, insertRoutingRuleSQL,
			r.operationID, r.scope, r.fromTablegroupID, nullableID(r.fromTableID),
			r.toTablegroupID, nullableID(r.toShardID), nullableID(r.toShardKeyID), version); err != nil {
			return catalogNoSnapshotChange, mterrors.Wrap(err, "failed to insert routing rule")
		}
	}
	if plan.bumpPlanEpoch {
		return catalogInvalidatesPlans, nil
	}
	return catalogRoutingChange, nil
}

// loadRoutingCatalog reads what rule validation needs, in a transaction that
// holds the catalog_state lock at current.
func loadRoutingCatalog(ctx context.Context, tx executor.InternalTx, current catalogVersions, operationID string) (*routingCatalog, error) {
	cat := &routingCatalog{
		routingVersion: current.routingVersion,
		planEpoch:      current.planEpoch,
		tablegroups:    map[int64]*catalogTablegroup{},
		shards:         map[int64]*catalogShard{},
	}

	result, err := tx.Query(ctx, selectTablegroupsSQL)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read tablegroups")
	}
	for _, row := range result.Rows {
		tg := &catalogTablegroup{}
		if err := executor.ScanRow(row, &tg.id, &tg.name, &tg.state); err != nil {
			return nil, mterrors.Wrap(err, "failed to read tablegroup")
		}
		cat.tablegroups[tg.id] = tg
	}

	result, err = tx.Query(ctx, selectShardsSQL)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read shards")
	}
	for _, row := range result.Rows {
		s := &catalogShard{}
		if err := executor.ScanRow(row, &s.id, &s.tablegroupID, &s.name, &s.state); err != nil {
			return nil, mterrors.Wrap(err, "failed to read shard")
		}
		if s.keyRange, err = keyrange.ParseShardName(s.name); err != nil {
			return nil, mterrors.Wrapf(err, "shard %d has an invalid name", s.id)
		}
		cat.shards[s.id] = s
	}

	if cat.rules, err = readRoutingRules(ctx, tx); err != nil {
		return nil, err
	}

	result, err = tx.QueryArgs(ctx, countDefaultTablesSQL, constants.DefaultTableGroup)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to count default tablegroup tables")
	}
	if err := executor.ScanSingleRow(result, &cat.defaultTableCount); err != nil {
		return nil, mterrors.Wrap(err, "failed to count default tablegroup tables")
	}

	// An operation id that is not a uuid fails validation before the
	// operation is looked at; don't let the ::uuid cast fail first.
	if checkRuleChangeRequest(routingRuleChange{operationID: operationID}) == nil {
		result, err = tx.QueryArgs(ctx, selectOperationSQL, operationID)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to read operation")
		}
		if len(result.Rows) > 0 {
			op := &routingOperation{}
			if err := executor.ScanRow(result.Rows[0], &op.kind, &op.state); err != nil {
				return nil, mterrors.Wrap(err, "failed to read operation")
			}
			cat.operation = op
		}

		result, err = tx.QueryArgs(ctx, selectOtherOperationsSQL, operationID)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to read operations")
		}
		cat.otherOperations = map[int64]string{}
		for _, row := range result.Rows {
			var id string
			var tablegroupID int64
			if err := executor.ScanRow(row, &id, &tablegroupID); err != nil {
				return nil, mterrors.Wrap(err, "failed to read operation")
			}
			if _, ok := cat.otherOperations[tablegroupID]; !ok {
				cat.otherOperations[tablegroupID] = id
			}
		}
	}
	return cat, nil
}

// readRoutingRules returns every routing rule, in rule_id order.
func readRoutingRules(ctx context.Context, tx executor.InternalTx) ([]routingRule, error) {
	result, err := tx.Query(ctx, selectRoutingRulesSQL)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read routing rules")
	}
	rules := make([]routingRule, 0, len(result.Rows))
	for _, row := range result.Rows {
		var r routingRule
		var fromTable, toShard, toShardKey *int64
		if err := executor.ScanRow(row, &r.ruleID, &r.operationID, &r.scope, &r.fromTablegroupID, &fromTable,
			&r.toTablegroupID, &toShard, &toShardKey, &r.createdVersion); err != nil {
			return nil, mterrors.Wrap(err, "failed to read routing rule")
		}
		r.fromTableID, r.toShardID, r.toShardKeyID = derefID(fromTable), derefID(toShard), derefID(toShardKey)
		rules = append(rules, r)
	}
	return rules, nil
}

// nullableID maps a zero id to NULL.
func nullableID(id int64) *int64 {
	if id == 0 {
		return nil
	}
	return &id
}

// derefID maps NULL to a zero id.
func derefID(id *int64) int64 {
	if id == nil {
		return 0
	}
	return *id
}
