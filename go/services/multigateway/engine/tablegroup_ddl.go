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

// tableGroupPrivilegeSQL checks the caller's session before anything is
// recorded. It runs on the caller's own session on the catalog shard and
// returns two columns:
//
//   - transaction_read_only: on for a read-only session
//     (default_transaction_read_only) and on a standby, which is where a
//     connection on the replica-reads port runs it. The statement is then
//     refused, as PostgreSQL refuses CREATE DATABASE.
//   - whether current_user, the caller's effective role, is a superuser.
//     Only superusers may change shard infrastructure.
//
// It runs with the caller's search_path, so every function and operator in it
// is schema-qualified: an unqualified = would resolve to an operator the
// caller defined in a schema ahead of pg_catalog, and could match the
// superuser's row instead of the caller's.
const tableGroupPrivilegeSQL = "SELECT pg_catalog.current_setting('transaction_read_only'), " +
	"COALESCE((SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname OPERATOR(pg_catalog.=) current_user), false)"

// TableGroupDDL executes CREATE TABLEGROUP or DROP TABLEGROUP. Everything it
// runs goes to the catalog shard (constants.IsCatalogShard), never to the
// tablegroup it names.
type TableGroupDDL struct {
	admin        TableGroupAdmin
	sql          string
	pollInterval time.Duration

	drop        bool
	name        string
	shards      []string // CREATE only; empty means one 0-inf shard
	ifNotExists bool     // CREATE only
	ifExists    bool     // DROP only
}

// NewCreateTableGroup returns the primitive for CREATE TABLEGROUP.
func NewCreateTableGroup(admin TableGroupAdmin, sql, name string, shards []string, ifNotExists bool) *TableGroupDDL {
	return &TableGroupDDL{
		admin: admin, sql: sql, pollInterval: tableGroupPollInterval,
		name: name, shards: shards, ifNotExists: ifNotExists,
	}
}

// NewDropTableGroup returns the primitive for DROP TABLEGROUP.
func NewDropTableGroup(admin TableGroupAdmin, sql, name string, ifExists bool) *TableGroupDDL {
	return &TableGroupDDL{
		admin: admin, sql: sql, pollInterval: tableGroupPollInterval,
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
	if err := t.checkSession(ctx, exec, conn, state); err != nil {
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

// checkSession refuses the statement where PostgreSQL refuses CREATE
// DATABASE, in PostgreSQL's order: a read-only session, then a transaction
// block, then a pipeline, then a caller without the privilege. The read-only
// and privilege answers come from tableGroupPrivilegeSQL on the catalog shard.
func (t *TableGroupDDL) checkSession(ctx context.Context, exec IExecute, conn *server.Conn, state *handler.MultigatewayConnectionState) error {
	answered, readOnly, allowed := false, false, false
	capture := func(_ context.Context, result *sqltypes.Result) error {
		if len(result.Rows) > 0 && len(result.Rows[0].Values) > 1 {
			answered = true
			readOnly = string(result.Rows[0].Values[0]) != "off"
			v := string(result.Rows[0].Values[1])
			allowed = v == "t" || v == "true"
		}
		return nil
	}
	if err := exec.StreamExecute(ctx, conn, constants.DefaultTableGroup, constants.DefaultShard, tableGroupPrivilegeSQL, nil, state, PlanExecInfo{}, true, capture); err != nil {
		return err
	}
	if !answered {
		return unansweredPrivilegeCheckError(t.statementName())
	}
	if readOnly {
		return readOnlyTransactionError(t.statementName())
	}
	if conn != nil && conn.IsInTransaction() {
		return mterrors.NewInTransactionBlock(t.statementName())
	}
	if state != nil && state.Pipelining() {
		return mterrors.NewInPipeline(t.statementName())
	}
	if !allowed {
		verb := "create"
		if t.drop {
			verb = "drop"
		}
		return insufficientPrivilegeError(verb)
	}
	return nil
}

func (t *TableGroupDDL) runCreate(ctx context.Context, database string, callback func(context.Context, *sqltypes.Result) error) error {
	resp, err := t.admin.CreateTableGroup(ctx, database, &multipoolermanagerdatapb.CreateTableGroupRequest{
		Name: t.name, Shards: t.shards, IfNotExists: t.ifNotExists,
	})
	if err != nil {
		return t.recordError(ctx, err, callback)
	}
	if resp.GetAlreadyExists() {
		return callback(ctx, &sqltypes.Result{
			Notices:    []*mterrors.PgDiagnostic{alreadyExistsNotice(t.name)},
			CommandTag: t.statementName(),
		})
	}
	opID := resp.GetOperationId()
	if err := callback(ctx, &sqltypes.Result{Notices: []*mterrors.PgDiagnostic{creatingNotice(t.name, opID)}}); err != nil {
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
	resp, err := t.admin.DropTableGroup(ctx, database, &multipoolermanagerdatapb.DropTableGroupRequest{Name: t.name, IfExists: t.ifExists})
	if err != nil {
		return t.recordError(ctx, err, callback)
	}
	if resp.GetNotFound() {
		return callback(ctx, &sqltypes.Result{
			Notices:    []*mterrors.PgDiagnostic{doesNotExistNotice(t.name)},
			CommandTag: t.statementName(),
		})
	}
	if err := callback(ctx, &sqltypes.Result{Notices: []*mterrors.PgDiagnostic{droppingNotice(t.name, resp.GetOperationId(), resp.GetCancelledCreation())}}); err != nil {
		return err
	}
	op, err := t.wait(ctx, database, resp.GetOperationId())
	if err != nil {
		return err
	}
	if resp.GetCancelledCreation() {
		// The drop cancelled a creation still in flight; the second drop is
		// what makes success mean the name is free. It is pinned to the
		// incarnation the first one acted on, so a tablegroup another session
		// created under the name in between reads as not found rather than
		// being dropped. A cancelled creation ends failed and leaves the
		// tablegroup dropping, or already gone, and a drop of that cannot be
		// refused, so a transient error there is retried: the first drop
		// already took effect. A creation that completed first leaves the
		// tablegroup active, and that drop is a plain one, refusable like any
		// other.
		again := &multipoolermanagerdatapb.DropTableGroupRequest{Name: t.name, TablegroupId: resp.GetTablegroupId()}
		cancelled := op.GetState() == multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED
		if cancelled {
			// The rows may already be gone; that is the drop having happened.
			again.IfExists = true
			resp, err = t.dropAgain(ctx, database, again)
		} else {
			// The creation completed first and the tablegroup is active, so
			// this is the user's drop of it, IF EXISTS as they wrote it.
			again.IfExists = t.ifExists
			resp, err = t.admin.DropTableGroup(ctx, database, again)
		}
		if err != nil {
			return t.recordError(ctx, err, callback)
		}
		if resp.GetNotFound() {
			if cancelled {
				return callback(ctx, &sqltypes.Result{CommandTag: t.statementName()})
			}
			return callback(ctx, &sqltypes.Result{
				Notices:    []*mterrors.PgDiagnostic{doesNotExistNotice(t.name)},
				CommandTag: t.statementName(),
			})
		}
		if resp.GetCancelledCreation() {
			return changedStateError(t.name)
		}
		if err := callback(ctx, &sqltypes.Result{Notices: []*mterrors.PgDiagnostic{droppingNotice(t.name, resp.GetOperationId(), false)}}); err != nil {
			return err
		}
		if op, err = t.wait(ctx, database, resp.GetOperationId()); err != nil {
			return err
		}
	}
	// A drop never fails, but a failed one must not report success: its
	// shards and their data would still be there.
	if op.GetState() == multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED {
		return operationFailedError(fmt.Sprintf("tablegroup %q was not dropped", t.name), op)
	}
	return callback(ctx, &sqltypes.Result{CommandTag: t.statementName()})
}

// dropAgain drops a tablegroup whose creation this statement cancelled,
// retrying transient errors as wait does. See runDrop for why that is safe
// here and not for a drop in general. The error is the admin's own, for
// recordError.
func (t *TableGroupDDL) dropAgain(ctx context.Context, database string, req *multipoolermanagerdatapb.DropTableGroupRequest) (*multipoolermanagerdatapb.DropTableGroupResponse, error) {
	var resp *multipoolermanagerdatapb.DropTableGroupResponse
	err := t.pollUntil(ctx, func() (bool, error) {
		var err error
		resp, err = t.admin.DropTableGroup(ctx, database, req)
		return err == nil, err
	})
	return resp, err
}

// wait polls the operation until it is done or failed. Transient errors
// (the catalog primary failing over, a pooler restarting) are retried until
// ctx ends; ctx ending returns its error, which the server reports as a
// cancel or a statement timeout. Either way the operation itself continues.
func (t *TableGroupDDL) wait(ctx context.Context, database, opID string) (*multipoolermanagerdatapb.Operation, error) {
	var op *multipoolermanagerdatapb.Operation
	err := t.pollUntil(ctx, func() (bool, error) {
		var err error
		if op, err = t.admin.GetOperation(ctx, database, opID); err != nil {
			return false, err
		}
		if op == nil {
			return false, missingOperationStateError(opID)
		}
		s := op.GetState()
		return s == multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE || s == multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED, nil
	})
	if err != nil {
		return nil, tableGroupAdminError(err)
	}
	return op, nil
}

// pollUntil calls fn every pollInterval until it reports done, ctx ends, or
// it returns an error that is not transient (isTransientAdminError). The
// error is fn's own, or ctx's when ctx ended.
func (t *TableGroupDDL) pollUntil(ctx context.Context, fn func() (done bool, err error)) error {
	ticker := time.NewTicker(t.pollInterval)
	defer ticker.Stop()
	for {
		done, err := fn()
		switch {
		case err == nil && done:
			return nil
		case err == nil:
		case ctx.Err() != nil:
			return ctx.Err()
		case !isTransientAdminError(err):
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// readOnlyTransactionError refuses the statement in a read-only session,
// including any session on the replica-reads port, as PostgreSQL refuses
// CREATE DATABASE. SQLSTATE 25006 (read_only_sql_transaction).
func readOnlyTransactionError(statement string) *mterrors.PgDiagnostic {
	return mterrors.NewPgError("ERROR", mterrors.PgSSReadOnlyTransaction,
		"cannot execute "+statement+" in a read-only transaction", "")
}

// unansweredPrivilegeCheckError refuses the statement when its session check
// returned no row: it never runs on a check that did not happen.
func unansweredPrivilegeCheckError(statement string) *mterrors.PgDiagnostic {
	return mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
		"the "+statement+" privilege check returned no row", "")
}

// insufficientPrivilegeError rejects a caller that is not a superuser, worded
// as PostgreSQL rejects CREATE EVENT TRIGGER. verb is "create" or "drop".
func insufficientPrivilegeError(verb string) *mterrors.PgDiagnostic {
	d := mterrors.NewPgError("ERROR", mterrors.PgSSInsufficientPrivilege, "permission denied to "+verb+" tablegroup", "")
	d.Hint = "Must be superuser to " + verb + " a tablegroup."
	return d
}

// alreadyExistsNotice is CREATE TABLEGROUP IF NOT EXISTS finding the
// tablegroup active.
func alreadyExistsNotice(name string) *mterrors.PgDiagnostic {
	return mterrors.NewPgNotice("NOTICE", mterrors.PgSSDuplicateObject,
		fmt.Sprintf("tablegroup %q already exists, skipping", name), "")
}

// doesNotExistNotice is DROP TABLEGROUP IF EXISTS finding no tablegroup.
func doesNotExistNotice(name string) *mterrors.PgDiagnostic {
	return mterrors.NewPgNotice("NOTICE", mterrors.PgSSSuccessfulCompletion,
		fmt.Sprintf("tablegroup %q does not exist, skipping", name), "")
}

// creatingNotice announces a creation and how to follow or cancel it.
func creatingNotice(name, opID string) *mterrors.PgDiagnostic {
	return operationNotice(fmt.Sprintf("creating tablegroup %q as operation %s", name, opID), opID, true)
}

// droppingNotice announces a drop, or the cancel of a creation still in
// flight, and how to follow it.
func droppingNotice(name, opID string, cancelledCreation bool) *mterrors.PgDiagnostic {
	msg := fmt.Sprintf("dropping tablegroup %q as operation %s", name, opID)
	if cancelledCreation {
		msg = fmt.Sprintf("cancelling the creation of tablegroup %q, operation %s", name, opID)
	}
	return operationNotice(msg, opID, false)
}

// changedStateError reports a drop that cancelled a creation and cannot finish
// the job: the second drop answered with a cancelled creation, which the
// pooler never does (a cancelled creation ends dropping or gone, a completed
// one active, and neither is creating again).
func changedStateError(name string) *mterrors.PgDiagnostic {
	return mterrors.NewPgError("ERROR", mterrors.PgSSObjectNotInPrerequisiteState,
		fmt.Sprintf("tablegroup %q changed state while being dropped", name), "")
}

// missingOperationStateError reports a GetOperation that answered without an
// operation, which the catalog primary never does.
func missingOperationStateError(opID string) *mterrors.PgDiagnostic {
	return mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
		fmt.Sprintf("operation %s was reported without its state", opID), "")
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

// recordError is the error for a failed CreateTableGroup or DropTableGroup
// call. When its outcome is unknown, the catalog may have recorded the request
// although the caller never saw its operation id, so the error says how to
// find it. When ctx ended, the error is the statement's own cancel or timeout,
// which the server reports as such (57014) and without a hint, so the hint
// goes out first as a notice.
func (t *TableGroupDDL) recordError(ctx context.Context, err error, callback func(context.Context, *sqltypes.Result) error) error {
	if ctx.Err() != nil {
		n := mterrors.NewPgNotice("NOTICE", mterrors.PgSSSuccessfulCompletion,
			"the "+t.statementName()+" request may have been recorded before the statement ended", "")
		n.Hint = t.mayBeRecordedHint()
		// Best effort: a closed connection has no one to tell.
		_ = callback(ctx, &sqltypes.Result{Notices: []*mterrors.PgDiagnostic{n}})
		return ctx.Err()
	}
	mapped := tableGroupAdminError(err)
	var d *mterrors.PgDiagnostic
	if !outcomeUnknown(err) || !errors.As(mapped, &d) {
		return mapped
	}
	hinted := *d
	hinted.Hint = "The request may have been recorded. " + t.mayBeRecordedHint()
	return &hinted
}

// mayBeRecordedHint says how to reach a request that may have been recorded:
// repeating the statement joins it, and the IF [NOT] EXISTS form also
// succeeds if it has already finished.
func (t *TableGroupDDL) mayBeRecordedHint() string {
	clause := "IF NOT EXISTS"
	if t.drop {
		clause = "IF EXISTS"
	}
	return "Run the statement again with " + clause + " to wait for it, or check multigres.operations."
}

// outcomeUnknown reports whether a failed record call may still have
// committed: the call timed out, was cut off, or never got an answer.
// Any other error is the catalog primary refusing the request.
func outcomeUnknown(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Unknown:
		return true
	}
	return false
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

// GetTableGroup returns the catalog shard's tablegroup, where the statement
// runs its session check and whose primary it calls.
func (t *TableGroupDDL) GetTableGroup() string { return constants.DefaultTableGroup }

func (t *TableGroupDDL) GetQuery() string { return t.sql }

func (t *TableGroupDDL) String() string {
	if t.drop {
		return fmt.Sprintf("TableGroupDDL(%s %s if_exists=%t)", t.statementName(), t.name, t.ifExists)
	}
	return fmt.Sprintf("TableGroupDDL(%s %s shards=%v if_not_exists=%t)", t.statementName(), t.name, t.shards, t.ifNotExists)
}

var _ Primitive = (*TableGroupDDL)(nil)
