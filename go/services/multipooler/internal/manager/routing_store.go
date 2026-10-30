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
	"encoding/json"
	"slices"

	"github.com/google/uuid"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// The routing store reads and writes routing rules inside a caller's
// transaction on the catalog shard's primary. Every rule write locks the
// catalog_state row first (lockRoutingState), so rule writers are serialized
// and the validation in routing_rules.go sees a catalog no other writer is
// changing.

const (
	lockRoutingStateSQL = `SELECT routing_version, plan_epoch FROM multigres.catalog_state FOR UPDATE`

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

	deleteOperationRulesSQL = `DELETE FROM multigres.routing_rule WHERE operation_id = $1::uuid`

	insertRoutingRuleSQL = `INSERT INTO multigres.routing_rule
	(operation_id, scope, from_tablegroup_id, from_table_id, to_tablegroup_id, to_shard_id, to_shard_key_id, created_version)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8)`

	bumpRoutingVersionSQL = `UPDATE multigres.catalog_state SET routing_version = $1, plan_epoch = plan_epoch + $2`

	selectRoutingStateSQL = `SELECT routing_version, plan_epoch FROM multigres.catalog_state`

	// selectSnapshotRulesSQL reads every rule with the names a gateway routes
	// by, in rule_id order.
	selectSnapshotRulesSQL = `SELECT r.rule_id, r.operation_id::text, r.scope, ftg.name, ft.schema_name, ft.table_name,
	ttg.name, s.shard_name, sk.name, r.created_version
FROM multigres.routing_rule r
JOIN multigres.tablegroup ftg ON ftg.tablegroup_id = r.from_tablegroup_id
JOIN multigres.tablegroup ttg ON ttg.tablegroup_id = r.to_tablegroup_id
LEFT JOIN multigres.tablegroup_table ft ON ft.table_id = r.from_table_id
LEFT JOIN multigres.shard s ON s.shard_id = r.to_shard_id
LEFT JOIN multigres.shard_key sk ON sk.shard_key_id = r.to_shard_key_id
ORDER BY r.rule_id`

	selectOperationForUpdateSQL = `SELECT kind, state, request->>'tablegroup' FROM multigres.operation
WHERE operation_id = $1::uuid FOR UPDATE`

	insertOperationSQL = `INSERT INTO multigres.operation (operation_id, kind, state, request)
VALUES ($1::uuid, $2, $3, $4::jsonb)`

	// selectOtherOperationOnTablegroupSQL finds another unfinished operation,
	// of any kind, whose request names the tablegroup. Its state list is
	// terminalOperationStates.
	selectOtherOperationOnTablegroupSQL = `SELECT operation_id::text FROM multigres.operation
WHERE request->>'tablegroup' = $1 AND operation_id <> $2::uuid
  AND state NOT IN ('completed', 'cancelled', 'failed')
LIMIT 1`

	selectTablegroupByNameSQL = `SELECT tablegroup_id, state FROM multigres.tablegroup WHERE name = $1`

	insertTablegroupSQL = `INSERT INTO multigres.tablegroup (name, state) VALUES ($1, 'active') RETURNING tablegroup_id`

	selectShardNamesSQL = `SELECT shard_name FROM multigres.shard WHERE tablegroup_id = $1 ORDER BY shard_name`

	insertFullShardSQL = `INSERT INTO multigres.shard (tablegroup_id, shard_name, state) VALUES ($1, $2, 'active')`
)

// importOperationInitialState is the state a new import operation starts in.
const importOperationInitialState = "preparing"

// applyRoutingRuleChange validates change and writes it in tx: it replaces
// the operation's rules and increments routing_version (and plan_epoch when
// the plan requires it). It returns the routing version and the database's
// rule count after the change; a change that is already in effect writes
// nothing and returns the current ones. The caller commits tx.
func applyRoutingRuleChange(ctx context.Context, tx executor.InternalTx, change routingRuleChange) (routingWrite, error) {
	cat, err := loadRoutingCatalog(ctx, tx, change.operationID)
	if err != nil {
		return routingWrite{}, err
	}
	plan, err := planRoutingRuleChange(cat, change)
	if err != nil {
		return routingWrite{}, err
	}
	if plan.noop {
		return routingWrite{version: cat.routingVersion, rules: len(cat.rules)}, nil
	}

	write := routingWrite{version: cat.routingVersion + 1, rules: len(plan.insert)}
	for _, r := range cat.rules {
		if r.operationID != change.operationID {
			write.rules++
		}
	}
	version := write.version
	if _, err := tx.QueryArgs(ctx, deleteOperationRulesSQL, change.operationID); err != nil {
		return routingWrite{}, mterrors.Wrap(err, "failed to delete routing rules")
	}
	for _, r := range plan.insert {
		if _, err := tx.QueryArgs(ctx, insertRoutingRuleSQL,
			r.operationID, r.scope, r.fromTablegroupID, nullableID(r.fromTableID),
			r.toTablegroupID, nullableID(r.toShardID), nullableID(r.toShardKeyID), version); err != nil {
			return routingWrite{}, mterrors.Wrap(err, "failed to insert routing rule")
		}
	}
	var planEpochDelta int64
	if plan.bumpPlanEpoch {
		planEpochDelta = 1
	}
	if _, err := tx.QueryArgs(ctx, bumpRoutingVersionSQL, version, planEpochDelta); err != nil {
		return routingWrite{}, mterrors.Wrap(err, "failed to bump routing version")
	}
	return write, nil
}

// routingWrite is the outcome of a rule change: the routing version after it
// and the number of rules the database then has.
type routingWrite struct {
	version int64
	rules   int
}

// loadRoutingCatalog locks catalog_state and reads what rule validation needs.
// It must be the first thing a rule write does in its transaction, so the
// version it compares against is the one it then increments.
func loadRoutingCatalog(ctx context.Context, tx executor.InternalTx, operationID string) (*routingCatalog, error) {
	cat := &routingCatalog{
		tablegroups: map[int64]*catalogTablegroup{},
		shards:      map[int64]*catalogShard{},
	}

	result, err := tx.Query(ctx, lockRoutingStateSQL)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to lock catalog state")
	}
	if err := executor.ScanSingleRow(result, &cat.routingVersion, &cat.planEpoch); err != nil {
		return nil, mterrors.Wrap(err, "failed to read routing version")
	}

	result, err = tx.Query(ctx, selectTablegroupsSQL)
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

// readRoutingSnapshot reads the routing version, the plan epoch and every rule
// in tx, which the caller opens REPEATABLE READ so they are read from one
// snapshot.
func readRoutingSnapshot(ctx context.Context, tx executor.InternalTx) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error) {
	resp := &multipoolermanagerdatapb.GetRoutingSnapshotResponse{Full: true}
	result, err := tx.Query(ctx, selectRoutingStateSQL)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read routing version")
	}
	if err := executor.ScanSingleRow(result, &resp.RoutingVersion, &resp.PlanEpoch); err != nil {
		return nil, mterrors.Wrap(err, "failed to read routing version")
	}

	result, err = tx.Query(ctx, selectSnapshotRulesSQL)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read routing rules")
	}
	for _, row := range result.Rows {
		rule := &multipoolermanagerdatapb.RoutingRule{}
		var scope string
		var fromSchema, fromTable, toShard, toShardKey *string
		if err := executor.ScanRow(row, &rule.RuleId, &rule.OperationId, &scope, &rule.FromTablegroup,
			&fromSchema, &fromTable, &rule.ToTablegroup, &toShard, &toShardKey,
			&rule.CreatedVersion); err != nil {
			return nil, mterrors.Wrap(err, "failed to read routing rule")
		}
		switch scope {
		case routingScopeTablegroup:
			rule.Scope = multipoolermanagerdatapb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP
		case routingScopeTable:
			rule.Scope = multipoolermanagerdatapb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLE
		}
		if fromTable != nil {
			rule.FromTable = &multipoolermanagerdatapb.RoutingTableName{Schema: derefString(fromSchema), Name: *fromTable}
		}
		if toShard != nil {
			rule.ToShard = *toShard
			if rule.ToKeyRange, err = keyrange.ParseShardName(*toShard); err != nil {
				return nil, mterrors.Wrapf(err, "routing rule %d targets a shard with an invalid name", rule.RuleId)
			}
		}
		rule.ToShardKey = derefString(toShardKey)
		resp.Rules = append(resp.Rules, rule)
	}
	return resp, nil
}

// ensureImportTarget creates, in tx, the import operation operationID and the
// tablegroup it imports into with its single full-range shard, each only if it
// does not exist. It changes no routing, so the routing version stays, but it
// locks catalog_state first like a rule write: rule validation reads
// operation, tablegroup and shard rows, which must not change under it.
//
// It backs the EnsureImportTarget RPC, which is temporary, for the first
// version of imports, and will be deprecated: tablegroup creation (the
// equivalent of CREATE TABLEGROUP) plus the import's PREPARE step
// (registering its tables, the default rule and fencing) will replace it.
func ensureImportTarget(ctx context.Context, tx executor.InternalTx, operationID, tablegroup string) error {
	if _, err := uuid.Parse(operationID); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "invalid operation id %q: %v", operationID, err)
	}
	if tablegroup == constants.DefaultTableGroup {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "an import cannot target the %s tablegroup", constants.DefaultTableGroup)
	}
	if err := keyrange.ValidateTableGroupAndShard(tablegroup, constants.DefaultShard); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "invalid tablegroup: %v", err)
	}

	if _, err := tx.Query(ctx, lockRoutingStateSQL); err != nil {
		return mterrors.Wrap(err, "failed to lock catalog state")
	}

	result, err := tx.QueryArgs(ctx, selectOperationForUpdateSQL, operationID)
	if err != nil {
		return mterrors.Wrap(err, "failed to read operation")
	}
	if len(result.Rows) == 0 {
		request, err := json.Marshal(map[string]string{"tablegroup": tablegroup})
		if err != nil {
			return mterrors.Wrap(err, "failed to encode import request")
		}
		if _, err := tx.QueryArgs(ctx, insertOperationSQL,
			operationID, importOperationKind, importOperationInitialState, string(request)); err != nil {
			return mterrors.Wrap(err, "failed to create import operation")
		}
	} else {
		var kind, state string
		var opTablegroup *string
		if err := executor.ScanRow(result.Rows[0], &kind, &state, &opTablegroup); err != nil {
			return mterrors.Wrap(err, "failed to read operation")
		}
		switch {
		case kind != importOperationKind:
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "operation %s is a %s, not an import", operationID, kind)
		case slices.Contains(terminalOperationStates, state):
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "operation %s is %s", operationID, state)
		case derefString(opTablegroup) != tablegroup:
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"operation %s imports into tablegroup %q, not %q", operationID, derefString(opTablegroup), tablegroup)
		}
	}

	// One tablegroup serves one import. A second one fails here rather than
	// later as overlapping rules. Registered tables do not count: an import
	// registers its own tables under the tablegroup, and a re-run of this
	// call must stay idempotent.
	result, err = tx.QueryArgs(ctx, selectOtherOperationOnTablegroupSQL, tablegroup, operationID)
	if err != nil {
		return mterrors.Wrap(err, "failed to read operations")
	}
	if len(result.Rows) > 0 {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"tablegroup %q is already the target of operation %s", tablegroup, string(result.Rows[0].Values[0]))
	}

	var tablegroupID int64
	result, err = tx.QueryArgs(ctx, selectTablegroupByNameSQL, tablegroup)
	if err != nil {
		return mterrors.Wrap(err, "failed to read tablegroup")
	}
	if len(result.Rows) == 0 {
		result, err = tx.QueryArgs(ctx, insertTablegroupSQL, tablegroup)
		if err != nil {
			return mterrors.Wrap(err, "failed to create tablegroup")
		}
		if err := executor.ScanSingleRow(result, &tablegroupID); err != nil {
			return mterrors.Wrap(err, "failed to create tablegroup")
		}
	} else {
		var state string
		if err := executor.ScanRow(result.Rows[0], &tablegroupID, &state); err != nil {
			return mterrors.Wrap(err, "failed to read tablegroup")
		}
		if state == "dropping" {
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %q is being dropped", tablegroup)
		}
	}

	result, err = tx.QueryArgs(ctx, selectShardNamesSQL, tablegroupID)
	if err != nil {
		return mterrors.Wrap(err, "failed to read shards")
	}
	switch {
	case len(result.Rows) == 0:
		if _, err := tx.QueryArgs(ctx, insertFullShardSQL, tablegroupID, constants.DefaultShard); err != nil {
			return mterrors.Wrap(err, "failed to create shard")
		}
	case len(result.Rows) > 1 || string(result.Rows[0].Values[0]) != constants.DefaultShard:
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"tablegroup %q already has shards other than %s", tablegroup, constants.DefaultShard)
	}
	return nil
}
