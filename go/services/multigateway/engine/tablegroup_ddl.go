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

package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// CREATE TABLEGROUP and DROP TABLEGROUP run in the gateway, not in postgres:
// the catalog primary records the intent (the pooler manager's
// CreateTableGroup and DropTableGroup RPCs), a provisioner acts on it, and the
// catalog primary's completion loop finishes the operation once topology
// reflects it. See docs/sharding/tablegroup-operations.md.
//
// The statement waits for the operation to finish, like CREATE DATABASE does.
// Cancelling the statement, a statement timeout, or closing the connection
// only stops the wait: the operation is durable in the catalog and continues.
// The first notice names the operation so the user can follow or cancel it.

// TableGroupAdmin is the catalog primary's tablegroup control plane, as the
// gateway reaches it: database names the database whose catalog shard
// primary serves the call. Errors are gRPC status errors from the pooler.
type TableGroupAdmin interface {
	CreateTableGroup(ctx context.Context, database string, req *multipoolermanagerdatapb.CreateTableGroupRequest) (*multipoolermanagerdatapb.CreateTableGroupResponse, error)
	DropTableGroup(ctx context.Context, database string, req *multipoolermanagerdatapb.DropTableGroupRequest) (*multipoolermanagerdatapb.DropTableGroupResponse, error)
	GetOperation(ctx context.Context, database, operationID string) (*multipoolermanagerdatapb.Operation, error)
}

// tableGroupPollInterval is how often the statement polls its operation.
// Provisioning takes minutes; a second of latency is invisible.
const tableGroupPollInterval = time.Second

// tableGroupPrivilegeSQL is the privilege check, run on the caller's own
// session so current_user is the caller's effective role. Superusers and
// members of multigres_topo (constants.MultigresTopoRole) pass; the catalog's
// multigres.operations view and multigres.cancel_operation admit the same
// roles. A missing role (a catalog shard whose admin user could not create
// it) admits superusers only, rather than failing every statement.
//
// It runs with the caller's search_path, so every function and operator in it
// is schema-qualified: an unqualified = would resolve to an operator the
// caller defined in a schema ahead of pg_catalog, and could match the
// superuser's row instead of the caller's.
const tableGroupPrivilegeSQL = "SELECT COALESCE((SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname OPERATOR(pg_catalog.=) current_user), false)" +
	" OR CASE WHEN pg_catalog.to_regrole('" + constants.MultigresTopoRole + "') IS NULL THEN false" +
	" ELSE pg_catalog.pg_has_role(current_user, '" + constants.MultigresTopoRole + "', 'MEMBER') END"

// TableGroupDDL executes CREATE TABLEGROUP or DROP TABLEGROUP.
type TableGroupDDL struct {
	admin             TableGroupAdmin
	sql               string
	defaultTableGroup string
	defaultShard      string
	pollInterval      time.Duration

	drop        bool
	name        string
	shards      []string // CREATE only; empty means one 0-inf shard
	ifNotExists bool     // CREATE only
	ifExists    bool     // DROP only
}

// NewCreateTableGroup returns the primitive for CREATE TABLEGROUP. The
// privilege check runs on defaultTableGroup/defaultShard.
func NewCreateTableGroup(admin TableGroupAdmin, sql, defaultTableGroup, defaultShard, name string, shards []string, ifNotExists bool) *TableGroupDDL {
	return &TableGroupDDL{
		admin: admin, sql: sql, defaultTableGroup: defaultTableGroup, defaultShard: defaultShard, pollInterval: tableGroupPollInterval,
		name: name, shards: shards, ifNotExists: ifNotExists,
	}
}

// NewDropTableGroup returns the primitive for DROP TABLEGROUP.
func NewDropTableGroup(admin TableGroupAdmin, sql, defaultTableGroup, defaultShard, name string, ifExists bool) *TableGroupDDL {
	return &TableGroupDDL{
		admin: admin, sql: sql, defaultTableGroup: defaultTableGroup, defaultShard: defaultShard, pollInterval: tableGroupPollInterval,
		drop: true, name: name, ifExists: ifExists,
	}
}

func (t *TableGroupDDL) statementName() string {
	if t.drop {
		return "DROP TABLEGROUP"
	}
	return "CREATE TABLEGROUP"
}

func (t *TableGroupDDL) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if conn != nil && conn.IsInTransaction() {
		return mterrors.NewPgError("ERROR", mterrors.PgSSActiveTransaction,
			t.statementName()+" cannot run inside a transaction block", "")
	}
	if err := t.checkPrivilege(ctx, exec, conn, state); err != nil {
		return err
	}
	var database string
	if conn != nil {
		database = conn.Database()
	}
	if t.drop {
		return t.runDrop(ctx, database, callback)
	}
	return t.runCreate(ctx, database, callback)
}

// PortalStreamExecute delegates: the statement takes no parameters.
func (t *TableGroupDDL) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return t.StreamExecute(ctx, exec, conn, state, nil, PlanExecInfo{}, callback)
}

func (t *TableGroupDDL) checkPrivilege(ctx context.Context, exec IExecute, conn *server.Conn, state *handler.MultigatewayConnectionState) error {
	allowed := false
	capture := func(_ context.Context, result *sqltypes.Result) error {
		if len(result.Rows) > 0 && len(result.Rows[0].Values) > 0 {
			v := string(result.Rows[0].Values[0])
			allowed = v == "t" || v == "true"
		}
		return nil
	}
	if err := exec.StreamExecute(ctx, conn, t.defaultTableGroup, t.defaultShard, tableGroupPrivilegeSQL, nil, state, PlanExecInfo{}, true, capture); err != nil {
		return err
	}
	if !allowed {
		verb := "create"
		if t.drop {
			verb = "drop"
		}
		d := mterrors.NewPgError("ERROR", mterrors.PgSSInsufficientPrivilege, "permission denied to "+verb+" tablegroup", "")
		d.Hint = "Must be a member of " + constants.MultigresTopoRole + "."
		return d
	}
	return nil
}

func (t *TableGroupDDL) runCreate(ctx context.Context, database string, callback func(context.Context, *sqltypes.Result) error) error {
	resp, err := t.admin.CreateTableGroup(ctx, database, &multipoolermanagerdatapb.CreateTableGroupRequest{
		Name: t.name, Shards: t.shards, IfNotExists: t.ifNotExists,
	})
	if err != nil {
		return statementError(ctx, err)
	}
	if resp.GetAlreadyExists() {
		return callback(ctx, &sqltypes.Result{
			Notices:    []*mterrors.PgDiagnostic{mterrors.NewPgNotice("NOTICE", mterrors.PgSSDuplicateObject, fmt.Sprintf("tablegroup %q already exists, skipping", t.name), "")},
			CommandTag: t.statementName(),
		})
	}
	opID := resp.GetOperationId()
	if err := callback(ctx, &sqltypes.Result{Notices: []*mterrors.PgDiagnostic{
		operationNotice(fmt.Sprintf("creating tablegroup %q as operation %s", t.name, opID), opID, true),
	}}); err != nil {
		return err
	}
	op, err := t.wait(ctx, database, opID)
	if err != nil {
		return err
	}
	if op.GetState() == multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED {
		return operationFailedError(fmt.Sprintf("tablegroup %q was not created", t.name), op)
	}
	return callback(ctx, &sqltypes.Result{CommandTag: t.statementName()})
}

func (t *TableGroupDDL) runDrop(ctx context.Context, database string, callback func(context.Context, *sqltypes.Result) error) error {
	// A drop issued while the tablegroup is still being created cancels the
	// creation. If the creation completes before the cancel takes effect, the
	// tablegroup is now active and the drop is issued again; that can happen
	// at most once, since an active tablegroup is never creating again.
	for range 2 {
		resp, err := t.admin.DropTableGroup(ctx, database, &multipoolermanagerdatapb.DropTableGroupRequest{Name: t.name, IfExists: t.ifExists})
		if err != nil {
			return statementError(ctx, err)
		}
		if resp.GetNotFound() {
			return callback(ctx, &sqltypes.Result{
				Notices:    []*mterrors.PgDiagnostic{mterrors.NewPgNotice("NOTICE", mterrors.PgSSSuccessfulCompletion, fmt.Sprintf("tablegroup %q does not exist, skipping", t.name), "")},
				CommandTag: t.statementName(),
			})
		}
		opID := resp.GetOperationId()
		msg := fmt.Sprintf("dropping tablegroup %q as operation %s", t.name, opID)
		if resp.GetCancelledCreation() {
			msg = fmt.Sprintf("cancelling the creation of tablegroup %q, operation %s", t.name, opID)
		}
		if err := callback(ctx, &sqltypes.Result{Notices: []*mterrors.PgDiagnostic{operationNotice(msg, opID, false)}}); err != nil {
			return err
		}
		op, err := t.wait(ctx, database, opID)
		if err != nil {
			return err
		}
		// A cancelled creation ends failed, which is the drop succeeding. One
		// that ended done finished first; drop the now-active tablegroup.
		if resp.GetCancelledCreation() && op.GetState() == multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE {
			continue
		}
		return callback(ctx, &sqltypes.Result{CommandTag: t.statementName()})
	}
	return mterrors.NewPgError("ERROR", mterrors.PgSSObjectNotInPrerequisiteState,
		fmt.Sprintf("tablegroup %q changed state while being dropped", t.name), "")
}

// wait polls the operation until it is done or failed. Transient errors
// (the catalog primary failing over, a pooler restarting) are retried until
// ctx ends; ctx ending returns its error, which the server reports as a
// cancel or a statement timeout. Either way the operation itself continues.
func (t *TableGroupDDL) wait(ctx context.Context, database, opID string) (*multipoolermanagerdatapb.Operation, error) {
	ticker := time.NewTicker(t.pollInterval)
	defer ticker.Stop()
	for {
		op, err := t.admin.GetOperation(ctx, database, opID)
		switch {
		case err == nil:
			if op == nil {
				return nil, mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
					fmt.Sprintf("operation %s was reported without its state", opID), "")
			}
			if s := op.GetState(); s == multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE || s == multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED {
				return op, nil
			}
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case !isTransientAdminError(err):
			return nil, tableGroupAdminError(err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// operationNotice is the first notice of a statement: what it started and
// how to follow it. Only a creation can be cancelled.
func operationNotice(message, opID string, cancellable bool) *mterrors.PgDiagnostic {
	n := mterrors.NewPgNotice("NOTICE", mterrors.PgSSSuccessfulCompletion, message,
		"It continues if this statement is cancelled or the connection closes.")
	n.Hint = "Follow it in multigres.operations."
	if cancellable {
		n.Hint = fmt.Sprintf("Follow it in multigres.operations; cancel it with SELECT multigres.cancel_operation('%s').", opID)
	}
	return n
}

// operationFailedError reports a failed creation. A cancel is operator
// intervention; anything else (a provisioning timeout) is outside postgres.
func operationFailedError(message string, op *multipoolermanagerdatapb.Operation) error {
	if op.GetCancelRequested() {
		return mterrors.NewPgError("ERROR", mterrors.PgSSOperatorIntervention, message+": cancelled", "")
	}
	return mterrors.NewPgError("ERROR", mterrors.PgSSSystemError, message+": "+op.GetLastError(), "")
}

// isTransientAdminError reports whether a GetOperation error is worth
// retrying: the catalog primary is unreachable, failing over (a demoted
// pooler answers FAILED_PRECONDITION), or slow. Any other error is a definite
// answer and ends the wait, so a broken pooler is reported rather than polled
// forever when statement_timeout is 0.
func isTransientAdminError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.FailedPrecondition:
		return true
	}
	return false
}

// statementError is the error for a failed RPC: the statement's own cancel or
// timeout when ctx ended, which the server reports as such (57014), else the
// pooler's error mapped by tableGroupAdminError.
func statementError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return tableGroupAdminError(err)
}

// tableGroupAdminError maps a pooler error to the PostgreSQL error a client
// expects, keeping the pooler's message, which already names the tablegroup.
func tableGroupAdminError(err error) error {
	var d *mterrors.PgDiagnostic
	if errors.As(err, &d) {
		return d
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	code := mterrors.PgSSInternalError
	switch st.Code() {
	case codes.InvalidArgument:
		code = mterrors.PgSSInvalidParameterValue
	case codes.AlreadyExists:
		code = mterrors.PgSSDuplicateObject
	case codes.NotFound:
		code = mterrors.PgSSUndefinedObject
	case codes.FailedPrecondition:
		code = mterrors.PgSSObjectNotInPrerequisiteState
	case codes.PermissionDenied:
		code = mterrors.PgSSInsufficientPrivilege
	case codes.Unimplemented:
		code = mterrors.PgSSFeatureNotSupported
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		code = mterrors.PgSSCannotConnectNow
	}
	return mterrors.NewPgError("ERROR", code, st.Message(), "")
}

func (t *TableGroupDDL) GetTableGroup() string { return "" }

func (t *TableGroupDDL) GetQuery() string { return t.sql }

func (t *TableGroupDDL) String() string {
	if t.drop {
		return fmt.Sprintf("TableGroupDDL(%s %s if_exists=%t)", t.statementName(), t.name, t.ifExists)
	}
	return fmt.Sprintf("TableGroupDDL(%s %s shards=%v if_not_exists=%t)", t.statementName(), t.name, t.shards, t.ifNotExists)
}

var _ Primitive = (*TableGroupDDL)(nil)
