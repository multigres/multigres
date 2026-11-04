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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// Tablegroup operations: the catalog side of CREATE TABLEGROUP.
//
// The sharding catalog on the catalog shard (default tablegroup, 0-inf) is the
// single source of truth for which tablegroups should exist. This file holds
// the three operations the catalog primary serves for it:
//
//   - CreateTableGroup records the intent in one transaction: a tablegroup row
//     in state creating, its shards in state preparing, and an operation row.
//   - GetOperation reports an operation's state, with progress read from
//     topology while a creation is in flight.
//   - ListTableGroups is what the provisioner reads to learn which tablegroups
//     to provision.
//
// Only the catalog primary writes these tables, through the admin pool, which
// is the same single-writer arrangement ensureCatalogSchema relies on. A
// demoted primary cannot commit because its postgres is read-only, so no
// further fencing is needed.

// Operation kinds and states in multigres.operation. The columns are free
// text in SQL; these are the values this code writes and reads.
const (
	operationKindCreateTableGroup = "create_tablegroup"

	operationStateCreating = "creating"
	operationStateDone     = "done"
	operationStateFailed   = "failed"
)

// Tablegroup and shard states, matching the CHECK constraints in
// pg_catalog_schema.go.
const (
	tableGroupStateCreating = "creating"
	tableGroupStateActive   = "active"
	tableGroupStateDropping = "dropping"

	shardStatePreparing = "preparing"
	shardStateRetired   = "retired"
)

// The catalog stores kinds and states as text; the API carries them as enums.
// A value missing from these maps reads as UNSPECIFIED.
var (
	operationKinds = map[string]multipoolermanagerdatapb.OperationKind{
		operationKindCreateTableGroup: multipoolermanagerdatapb.OperationKind_OPERATION_KIND_CREATE_TABLEGROUP,
	}
	operationStates = map[string]multipoolermanagerdatapb.OperationState{
		operationStateCreating: multipoolermanagerdatapb.OperationState_OPERATION_STATE_CREATING,
		operationStateDone:     multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE,
		operationStateFailed:   multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED,
	}
	tableGroupStates = map[string]multipoolermanagerdatapb.TableGroupState{
		tableGroupStateCreating: multipoolermanagerdatapb.TableGroupState_TABLE_GROUP_STATE_CREATING,
		tableGroupStateActive:   multipoolermanagerdatapb.TableGroupState_TABLE_GROUP_STATE_ACTIVE,
	}
)

// tableGroupOpTimeout bounds each catalog transaction here. They touch a
// handful of rows; anything slower is a stuck lock.
const tableGroupOpTimeout = 10 * time.Second

// maxTableGroupNameLen is the longest tablegroup name accepted. The name is
// used unchanged as a Kubernetes resource name and label value, a pooler flag,
// a topology key, and a backup path, so it is held to the strictest of those
// rules. Lifting this later is an additive change; a mapping layer is not.
const maxTableGroupNameLen = 25

// tableGroupNamePattern is a lowercase DNS label: letters, digits and hyphens,
// starting and ending with a letter or digit.
var tableGroupNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// validateTableGroupName checks a name against the rules above. The default
// tablegroup passes here and is rejected by the catalog instead, since it
// always exists.
func validateTableGroupName(name string) error {
	if name == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "tablegroup name is required")
	}
	if len(name) > maxTableGroupNameLen {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "tablegroup name %q is longer than %d characters", name, maxTableGroupNameLen)
	}
	if !tableGroupNamePattern.MatchString(name) {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "tablegroup name %q must be lowercase letters, digits and hyphens, starting and ending with a letter or digit", name)
	}
	return nil
}

// createTableGroupRequest is the JSON stored in multigres.operation.request
// for a create_tablegroup operation. It is everything needed to report and
// complete the operation without joining other tables, so an operation stays
// inspectable even after its tablegroup rows are gone (a failed creation
// deletes them).
type createTableGroupRequest struct {
	TableGroupID int64    `json:"tablegroup_id"`
	Name         string   `json:"name"`
	Shards       []string `json:"shards"`
}

// checkCatalogShard rejects callers on a pooler that does not serve the
// catalog shard, where the tables do not exist.
func (pm *MultipoolerManager) checkCatalogShard() error {
	if !pm.isCatalogShard() {
		ref := pm.record.ShardRef()
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"tablegroup operations are served by the catalog shard (%s/%s), not %s/%s",
			constants.DefaultTableGroup, constants.DefaultShard, ref.GetTableGroup(), ref.GetShard())
	}
	return nil
}

// catalogQueryService returns the admin query service, or an error when the
// manager is not open yet.
func (pm *MultipoolerManager) catalogQueryService() (executor.InternalQueryService, error) {
	qs := pm.internalQueryService()
	if qs == nil {
		return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "internal query service unavailable for tablegroup operations")
	}
	return qs, nil
}

// CreateTableGroup records the intent to create a tablegroup. See the file
// comment and the RPC's proto comment for the contract.
func (pm *MultipoolerManager) CreateTableGroup(ctx context.Context, req *multipoolermanagerdatapb.CreateTableGroupRequest) (*multipoolermanagerdatapb.CreateTableGroupResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	if err := pm.checkCatalogShard(); err != nil {
		return nil, err
	}
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return nil, err
	}
	if err := validateTableGroupName(req.GetName()); err != nil {
		return nil, err
	}
	shards := req.GetShards()
	if len(shards) == 0 {
		shards = []string{constants.DefaultShard}
	}
	ranges, err := keyrange.ValidateShardLayout(shards)
	if err != nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "invalid shard layout: %v", err)
	}
	layout := make([]string, len(ranges))
	for i, kr := range ranges {
		layout[i] = keyrange.ShardName(kr)
	}

	qs, err := pm.catalogQueryService()
	if err != nil {
		return nil, err
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	tx, err := qs.BeginAdmin(execCtx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to begin tablegroup transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Claim the name. A concurrent CREATE of the same name blocks on the
	// unique index until the other transaction ends; if that one committed,
	// nothing is inserted here and the name resolves against its row instead.
	res, err := tx.QueryArgs(execCtx,
		"INSERT INTO multigres.tablegroup (name, state) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING RETURNING tablegroup_id",
		req.GetName(), tableGroupStateCreating)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to insert tablegroup")
	}
	if len(res.Rows) == 0 {
		res, err = tx.QueryArgs(execCtx,
			"SELECT tablegroup_id, state FROM multigres.tablegroup WHERE name = $1 FOR UPDATE", req.GetName())
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to look up tablegroup")
		}
		if len(res.Rows) == 0 {
			// The conflicting row was deleted after the insert saw it: a
			// creation that failed concurrently. Retrying claims the name.
			return nil, mterrors.Errorf(mtrpcpb.Code_ABORTED, "tablegroup %q changed concurrently; retry", req.GetName())
		}
		var id int64
		var state string
		if err := executor.ScanRow(res.Rows[0], &id, &state); err != nil {
			return nil, mterrors.Wrap(err, "failed to read tablegroup row")
		}
		return pm.createTableGroupExisting(execCtx, tx, req, id, state, layout)
	}
	var id int64
	if err := executor.ScanSingleRow(res, &id); err != nil {
		return nil, mterrors.Wrap(err, "failed to read new tablegroup id")
	}
	for _, kr := range ranges {
		// Bounds travel as hex text and are decoded in SQL: query parameters
		// are sent in text format, so raw bytes cannot be bound to a bytea.
		// A nil parameter is NULL, which is how the catalog spells the start
		// and end of the keyspace.
		if _, err := tx.QueryArgs(execCtx,
			"INSERT INTO multigres.shard (tablegroup_id, shard_name, range_start, range_end, state) VALUES ($1, $2, decode($3, 'hex'), decode($4, 'hex'), $5)",
			id, keyrange.ShardName(kr), hexBound(kr.GetStart()), hexBound(kr.GetEnd()), shardStatePreparing); err != nil {
			return nil, mterrors.Wrap(err, "failed to insert shard")
		}
	}
	opID := uuid.NewString()
	request, err := json.Marshal(createTableGroupRequest{TableGroupID: id, Name: req.GetName(), Shards: layout})
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to encode operation request")
	}
	if _, err := tx.QueryArgs(execCtx,
		"INSERT INTO multigres.operation (operation_id, kind, state, request) VALUES ($1, $2, $3, $4::jsonb)",
		opID, operationKindCreateTableGroup, operationStateCreating, string(request)); err != nil {
		return nil, mterrors.Wrap(err, "failed to insert operation")
	}
	if err := tx.Commit(execCtx); err != nil {
		return nil, mterrors.Wrap(err, "failed to commit tablegroup creation")
	}
	pm.logger.InfoContext(ctx, "recorded tablegroup creation", "tablegroup", req.GetName(), "tablegroup_id", id, "shards", layout, "operation_id", opID)
	return &multipoolermanagerdatapb.CreateTableGroupResponse{OperationId: opID, TablegroupId: id}, nil
}

// createTableGroupExisting resolves a CREATE for a name that already has a
// row, inside the caller's transaction. An active tablegroup is a no-op with
// IF NOT EXISTS and an error without. A creating tablegroup returns its
// in-flight operation, so a retried or concurrent CREATE joins it, when the
// layout matches or IF NOT EXISTS is given (which, as in PostgreSQL, ignores
// the definition); otherwise a different layout is a conflict.
func (pm *MultipoolerManager) createTableGroupExisting(ctx context.Context, tx executor.InternalTx, req *multipoolermanagerdatapb.CreateTableGroupRequest, id int64, state string, layout []string) (*multipoolermanagerdatapb.CreateTableGroupResponse, error) {
	switch state {
	case tableGroupStateActive:
		if req.GetIfNotExists() {
			return &multipoolermanagerdatapb.CreateTableGroupResponse{TablegroupId: id, AlreadyExists: true}, nil
		}
		return nil, mterrors.Errorf(mtrpcpb.Code_ALREADY_EXISTS, "tablegroup %q already exists", req.GetName())
	case tableGroupStateDropping:
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %q is being dropped", req.GetName())
	case tableGroupStateCreating:
	default:
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "tablegroup %q has unknown state %q", req.GetName(), state)
	}

	if !req.GetIfNotExists() {
		existing, err := tableGroupShardNames(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if !slices.Equal(existing, layout) {
			return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"tablegroup %q is already being created with shards %v", req.GetName(), existing)
		}
	}
	res, err := tx.QueryArgs(ctx,
		"SELECT operation_id FROM multigres.operation WHERE kind = $1 AND state = $2 AND (request->>'tablegroup_id')::bigint = $3",
		operationKindCreateTableGroup, operationStateCreating, id)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to look up tablegroup operation")
	}
	if len(res.Rows) != 1 {
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "tablegroup %q is creating with %d pending operations, want 1", req.GetName(), len(res.Rows))
	}
	var opID string
	if err := executor.ScanRow(res.Rows[0], &opID); err != nil {
		return nil, mterrors.Wrap(err, "failed to read tablegroup operation")
	}
	return &multipoolermanagerdatapb.CreateTableGroupResponse{OperationId: opID, TablegroupId: id}, nil
}

// hexBound encodes a key range bound for decode($n, 'hex'). An empty bound is
// the start or end of the keyspace and is stored as NULL.
func hexBound(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return hex.EncodeToString(b)
}

// tableGroupShardNames returns a tablegroup's non-retired shard names in
// canonical order. bytea compares bytewise, which is the order
// keyrange.ValidateShardLayout produces, with NULL (start of keyspace) first.
func tableGroupShardNames(ctx context.Context, tx executor.InternalTx, id int64) ([]string, error) {
	res, err := tx.QueryArgs(ctx,
		"SELECT shard_name FROM multigres.shard WHERE tablegroup_id = $1 AND state <> $2 ORDER BY range_start NULLS FIRST",
		id, shardStateRetired)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to list tablegroup shards")
	}
	names := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		var name string
		if err := executor.ScanRow(row, &name); err != nil {
			return nil, mterrors.Wrap(err, "failed to read shard row")
		}
		names = append(names, name)
	}
	return names, nil
}

// operationRow is multigres.operation as read back for GetOperation.
type operationRow struct {
	id              string
	kind            string
	state           string
	request         createTableGroupRequest
	cancelRequested bool
	lastError       string
	createdAt       time.Time
	updatedAt       time.Time
}

// operationColumns reads the timestamps as microseconds since the epoch, which
// does not depend on the session's TimeZone or DateStyle.
const operationColumns = "operation_id, kind, state, request::text, cancel_requested, coalesce(last_error, ''), " +
	"(extract(epoch FROM created_at) * 1000000)::bigint, (extract(epoch FROM updated_at) * 1000000)::bigint"

func scanOperationRow(row *sqltypes.Row) (operationRow, error) {
	var op operationRow
	var request string
	var createdAt, updatedAt int64
	if err := executor.ScanRow(row, &op.id, &op.kind, &op.state, &request, &op.cancelRequested, &op.lastError, &createdAt, &updatedAt); err != nil {
		return op, err
	}
	op.createdAt = time.UnixMicro(createdAt).UTC()
	op.updatedAt = time.UnixMicro(updatedAt).UTC()
	if op.kind == operationKindCreateTableGroup {
		if err := json.Unmarshal([]byte(request), &op.request); err != nil {
			return op, fmt.Errorf("operation %s has an unreadable request: %w", op.id, err)
		}
	}
	return op, nil
}

// GetOperation returns an operation's current state. For a tablegroup
// creation that is not done, shards_serving is read from topology on each
// call, so a caller polling this sees progress before the catalog rows flip,
// and a failed operation still shows what topology holds for it. Only the
// catalog primary answers, so a caller never reads a replica that lags its
// own write.
func (pm *MultipoolerManager) GetOperation(ctx context.Context, operationID string) (*multipoolermanagerdatapb.Operation, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	if err := pm.checkCatalogShard(); err != nil {
		return nil, err
	}
	parsed, err := uuid.Parse(operationID)
	if err != nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "operation id %q is not a UUID", operationID)
	}
	// Bind the canonical form: uuid.Parse also accepts forms such as
	// urn:uuid:... that PostgreSQL's uuid input rejects.
	operationID = parsed.String()
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return nil, err
	}
	qs, err := pm.catalogQueryService()
	if err != nil {
		return nil, err
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	res, err := qs.QueryAdminArgs(execCtx,
		"SELECT "+operationColumns+" FROM multigres.operation WHERE operation_id = $1", operationID)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read operation")
	}
	if len(res.Rows) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "operation %s not found", operationID)
	}
	op, err := scanOperationRow(res.Rows[0])
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read operation row")
	}
	out := op.toProto()
	if op.kind == operationKindCreateTableGroup && op.state != operationStateDone {
		serving, err := pm.tableGroupShardsServing(ctx, op.request)
		if err != nil {
			// Progress is best effort; the state is still accurate.
			pm.logger.WarnContext(ctx, "failed to read tablegroup readiness from topology", "operation_id", op.id, "error", err)
		}
		out.ShardsServing = int32(serving)
	}
	return out, nil
}

func (op operationRow) toProto() *multipoolermanagerdatapb.Operation {
	out := &multipoolermanagerdatapb.Operation{
		OperationId:     op.id,
		Kind:            operationKinds[op.kind],
		State:           operationStates[op.state],
		LastError:       op.lastError,
		CancelRequested: op.cancelRequested,
		CreatedAt:       timestamppb.New(op.createdAt),
		UpdatedAt:       timestamppb.New(op.updatedAt),
	}
	if op.kind == operationKindCreateTableGroup {
		out.TablegroupId = op.request.TableGroupID
		out.TablegroupName = op.request.Name
		out.ShardsDesired = int32(len(op.request.Shards))
		if op.state == operationStateDone {
			out.ShardsServing = out.ShardsDesired
		}
	}
	return out
}

// ListTableGroups returns every non-default tablegroup that is creating or
// active, with its shard layout, for the provisioner. Dropping tablegroups
// are omitted on purpose: absence from a successful list is the provisioner's
// signal to remove one. That is why only the catalog primary answers: a
// lagging replica would omit a tablegroup it has not replayed yet.
func (pm *MultipoolerManager) ListTableGroups(ctx context.Context) ([]*multipoolermanagerdatapb.TableGroup, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	if err := pm.checkCatalogShard(); err != nil {
		return nil, err
	}
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return nil, err
	}
	qs, err := pm.catalogQueryService()
	if err != nil {
		return nil, err
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	// One statement, so the tablegroups and their shards come from the same
	// snapshot: a creation that fails concurrently is either listed with its
	// whole layout or not at all.
	res, err := qs.QueryAdminArgs(execCtx,
		"SELECT t.tablegroup_id, t.name, t.state, s.shard_name FROM multigres.tablegroup t "+
			"JOIN multigres.shard s ON s.tablegroup_id = t.tablegroup_id AND s.state <> $4 "+
			"WHERE t.name <> $1 AND t.state IN ($2, $3) ORDER BY t.tablegroup_id, s.range_start NULLS FIRST",
		constants.DefaultTableGroup, tableGroupStateCreating, tableGroupStateActive, shardStateRetired)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to list tablegroups")
	}
	groups := []*multipoolermanagerdatapb.TableGroup{}
	for _, row := range res.Rows {
		var id int64
		var name, state, shard string
		if err := executor.ScanRow(row, &id, &name, &state, &shard); err != nil {
			return nil, mterrors.Wrap(err, "failed to read tablegroup row")
		}
		if n := len(groups); n == 0 || groups[n-1].TablegroupId != id {
			groups = append(groups, &multipoolermanagerdatapb.TableGroup{TablegroupId: id, Name: name, State: tableGroupStates[state]})
		}
		last := groups[len(groups)-1]
		last.Shards = append(last.Shards, shard)
	}
	return groups, nil
}

// tableGroupShardsServing counts the shards in req's layout that have a
// serving primary registered in topology for this tablegroup. It reads every
// cell: a database's poolers may live in any of them. A cell that cannot be
// read is reported in the error while the cells that answered still count,
// so the count is never higher than the truth.
func (pm *MultipoolerManager) tableGroupShardsServing(ctx context.Context, req createTableGroupRequest) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	database := pm.record.ShardRef().GetDatabase()
	cells, err := pm.topoClient.GetCellNames(ctx)
	if err != nil {
		return 0, fmt.Errorf("list cells: %w", err)
	}
	serving := make(map[string]bool, len(req.Shards))
	var errs []error
	for _, cell := range cells {
		infos, err := pm.topoClient.GetMultipoolersByCell(ctx, cell, &topoclient.GetMultipoolersByCellOptions{
			DatabaseShard: &topoclient.DatabaseShard{Database: database, TableGroup: req.Name},
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("list poolers in cell %s: %w", cell, err))
			continue
		}
		for _, info := range infos {
			if poolerIsServingPrimaryOf(info.Multipooler, req.TableGroupID) {
				serving[info.Multipooler.GetShardRef().GetShard()] = true
			}
		}
	}
	n := 0
	for _, shard := range req.Shards {
		if serving[shard] {
			n++
		}
	}
	return n, errors.Join(errs...)
}

// poolerIsServingPrimaryOf is the readiness rule for one pooler record: it
// belongs to the tablegroup incarnation being created, it is the routing
// primary of its shard, it is serving, and its process lifecycle is active
// (or unknown, for a record written by an older pooler). The tablegroup id is
// what excludes poolers left over from a dropped tablegroup of the same name.
func poolerIsServingPrimaryOf(mp *clustermetadatapb.Multipooler, tableGroupID int64) bool {
	lifecycle := mp.GetLifecycleStatus().GetStatus()
	return mp.GetTablegroupId() == tableGroupID &&
		mp.GetRoutingState().GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY &&
		mp.GetServingStatus() == clustermetadatapb.PoolerServingStatus_SERVING &&
		(lifecycle == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE ||
			lifecycle == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN)
}
