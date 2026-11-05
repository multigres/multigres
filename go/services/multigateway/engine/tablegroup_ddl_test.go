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
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

const testTableGroupOpID = "6b9c6d1e-9c4e-4f2d-8c0a-1d2e3f4a5b6c"

// fakeTableGroupAdmin scripts the catalog primary. GetOperation answers from
// ops in order and repeats the last answer.
type fakeTableGroupAdmin struct {
	mu        sync.Mutex
	createReq *multipoolermanagerdatapb.CreateTableGroupRequest
	createRes *multipoolermanagerdatapb.CreateTableGroupResponse
	createErr error
	dropRes   []*multipoolermanagerdatapb.DropTableGroupResponse
	dropErr   error
	dropCalls int
	ops       []fakeOpAnswer
	getCalls  int
	databases []string
}

type fakeOpAnswer struct {
	op  *multipoolermanagerdatapb.Operation
	err error
}

func (f *fakeTableGroupAdmin) CreateTableGroup(_ context.Context, database string, req *multipoolermanagerdatapb.CreateTableGroupRequest) (*multipoolermanagerdatapb.CreateTableGroupResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.databases = append(f.databases, database)
	f.createReq = req
	return f.createRes, f.createErr
}

func (f *fakeTableGroupAdmin) DropTableGroup(_ context.Context, database string, _ *multipoolermanagerdatapb.DropTableGroupRequest) (*multipoolermanagerdatapb.DropTableGroupResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.databases = append(f.databases, database)
	if f.dropErr != nil {
		return nil, f.dropErr
	}
	res := f.dropRes[min(f.dropCalls, len(f.dropRes)-1)]
	f.dropCalls++
	return res, nil
}

func (f *fakeTableGroupAdmin) GetOperation(_ context.Context, database, _ string) (*multipoolermanagerdatapb.Operation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.databases = append(f.databases, database)
	a := f.ops[min(f.getCalls, len(f.ops)-1)]
	f.getCalls++
	return a.op, a.err
}

func opIn(state multipoolermanagerdatapb.OperationState, lastError string) fakeOpAnswer {
	return fakeOpAnswer{op: &multipoolermanagerdatapb.Operation{OperationId: testTableGroupOpID, State: state, LastError: lastError}}
}

// privilegeExec answers the privilege query for a read-write session with
// allowed and records every routed query.
func privilegeExec(allowed string) *capturingExec {
	return sessionExec("off", allowed)
}

// sessionExec answers the privilege query with transaction_read_only and
// allowed.
func sessionExec(readOnly, allowed string) *capturingExec {
	return &capturingExec{mockIExecute: mockIExecute{streamExecuteResult: &sqltypes.Result{
		Rows: []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value(readOnly), sqltypes.Value(allowed)}}},
	}}}
}

// runTableGroupDDL executes prim on an idle connection to testdb and returns
// the results it streamed.
func runTableGroupDDL(t *testing.T, ctx context.Context, prim *TableGroupDDL, exec IExecute) ([]*sqltypes.Result, error) {
	t.Helper()
	prim.pollInterval = time.Millisecond
	conn := server.NewTestConn(&bytes.Buffer{}, server.WithTestDatabase("testdb")).Conn
	var results []*sqltypes.Result
	err := prim.StreamExecute(ctx, exec, conn, handler.NewMultigatewayConnectionState(), nil, PlanExecInfo{},
		func(_ context.Context, r *sqltypes.Result) error {
			results = append(results, r)
			return nil
		})
	return results, err
}

func requirePgError(t *testing.T, err error, code string) *mterrors.PgDiagnostic {
	t.Helper()
	require.Error(t, err)
	var d *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &d), "want a PgDiagnostic, got %T: %v", err, err)
	assert.Equal(t, code, d.Code, d.Message)
	return d
}

func newTestCreate(admin TableGroupAdmin, ifNotExists bool) *TableGroupDDL {
	return NewCreateTableGroup(admin, "CREATE TABLEGROUP commerce", "commerce", []string{"0-80", "80-inf"}, ifNotExists)
}

func newTestDrop(admin TableGroupAdmin, ifExists bool) *TableGroupDDL {
	return NewDropTableGroup(admin, "DROP TABLEGROUP commerce", "commerce", ifExists)
}

func TestTableGroupDDL_CreateWaitsForCompletion(t *testing.T) {
	admin := &fakeTableGroupAdmin{
		createRes: &multipoolermanagerdatapb.CreateTableGroupResponse{OperationId: testTableGroupOpID, TablegroupId: 7},
		ops:       []fakeOpAnswer{opIn(multipoolermanagerdatapb.OperationState_OPERATION_STATE_CREATING, ""), opIn(multipoolermanagerdatapb.OperationState_OPERATION_STATE_CREATING, ""), opIn(multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE, "")},
	}
	exec := privilegeExec("t")
	results, err := runTableGroupDDL(t, t.Context(), newTestCreate(admin, false), exec)
	require.NoError(t, err)

	assert.Equal(t, []string{tableGroupPrivilegeSQL}, exec.sqls, "the privilege check runs on the caller's session")
	assert.Equal(t, "commerce", admin.createReq.Name)
	assert.Equal(t, []string{"0-80", "80-inf"}, admin.createReq.Shards)
	assert.Equal(t, 3, admin.getCalls, "polls until done")
	for _, db := range admin.databases {
		assert.Equal(t, "testdb", db, "every call targets the client's database")
	}

	require.Len(t, results, 2)
	require.Len(t, results[0].Notices, 1, "the operation id is announced before the wait")
	notice := results[0].Notices[0]
	assert.Contains(t, notice.Message, `creating tablegroup "commerce" as operation `+testTableGroupOpID)
	assert.Contains(t, notice.Detail, "continues if this statement is cancelled")
	assert.Contains(t, notice.Hint, "SELECT multigres.cancel_operation('"+testTableGroupOpID+"')")
	assert.Equal(t, "CREATE TABLEGROUP", results[1].CommandTag)
}

func TestTableGroupDDL_CreateIfNotExistsAlreadyActive(t *testing.T) {
	admin := &fakeTableGroupAdmin{createRes: &multipoolermanagerdatapb.CreateTableGroupResponse{TablegroupId: 7, AlreadyExists: true}}
	results, err := runTableGroupDDL(t, t.Context(), newTestCreate(admin, true), privilegeExec("t"))
	require.NoError(t, err)
	assert.True(t, admin.createReq.IfNotExists)
	assert.Zero(t, admin.getCalls, "nothing to wait for")
	require.Len(t, results, 1)
	require.Len(t, results[0].Notices, 1)
	assert.Equal(t, mterrors.PgSSDuplicateObject, results[0].Notices[0].Code)
	assert.Equal(t, `tablegroup "commerce" already exists, skipping`, results[0].Notices[0].Message)
	assert.Equal(t, "CREATE TABLEGROUP", results[0].CommandTag)
}

func TestTableGroupDDL_CreateFailed(t *testing.T) {
	t.Run("cancelled is operator intervention", func(t *testing.T) {
		admin := &fakeTableGroupAdmin{
			createRes: &multipoolermanagerdatapb.CreateTableGroupResponse{OperationId: testTableGroupOpID},
			ops: []fakeOpAnswer{{op: &multipoolermanagerdatapb.Operation{
				OperationId: testTableGroupOpID, State: multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED,
				CancelRequested: true, LastError: "cancelled",
			}}},
		}
		_, err := runTableGroupDDL(t, t.Context(), newTestCreate(admin, false), privilegeExec("t"))
		d := requirePgError(t, err, mterrors.PgSSOperatorIntervention)
		assert.Equal(t, `tablegroup "commerce" was not created: cancelled`, d.Message)
	})
	t.Run("a timeout is a system error with the reason", func(t *testing.T) {
		admin := &fakeTableGroupAdmin{
			createRes: &multipoolermanagerdatapb.CreateTableGroupResponse{OperationId: testTableGroupOpID},
			ops:       []fakeOpAnswer{opIn(multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED, "timed out after 1h0m0s waiting for every shard to serve")},
		}
		_, err := runTableGroupDDL(t, t.Context(), newTestCreate(admin, false), privilegeExec("t"))
		d := requirePgError(t, err, mterrors.PgSSSystemError)
		assert.Contains(t, d.Message, "timed out after 1h0m0s")
	})
}

func TestTableGroupDDL_WaitRetriesTransientErrors(t *testing.T) {
	admin := &fakeTableGroupAdmin{
		createRes: &multipoolermanagerdatapb.CreateTableGroupResponse{OperationId: testTableGroupOpID},
		ops: []fakeOpAnswer{
			{err: status.Error(codes.Unavailable, "catalog primary failing over")},
			{err: status.Error(codes.FailedPrecondition, "tablegroup operations are served by the catalog shard")},
			opIn(multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE, ""),
		},
	}
	_, err := runTableGroupDDL(t, t.Context(), newTestCreate(admin, false), privilegeExec("t"))
	require.NoError(t, err)
	assert.Equal(t, 3, admin.getCalls)
}

func TestTableGroupDDL_WaitStopsOnDefiniteErrors(t *testing.T) {
	admin := &fakeTableGroupAdmin{
		createRes: &multipoolermanagerdatapb.CreateTableGroupResponse{OperationId: testTableGroupOpID},
		ops:       []fakeOpAnswer{{err: status.Error(codes.NotFound, "operation not found")}},
	}
	_, err := runTableGroupDDL(t, t.Context(), newTestCreate(admin, false), privilegeExec("t"))
	_ = requirePgError(t, err, mterrors.PgSSUndefinedObject)
	assert.Equal(t, 1, admin.getCalls)
}

func TestTableGroupDDL_WaitStopsOnInternalErrors(t *testing.T) {
	for _, answer := range []fakeOpAnswer{
		{err: status.Error(codes.Internal, "catalog row unreadable")},
		{}, // no operation in the response
	} {
		admin := &fakeTableGroupAdmin{
			createRes: &multipoolermanagerdatapb.CreateTableGroupResponse{OperationId: testTableGroupOpID},
			ops:       []fakeOpAnswer{answer},
		}
		_, err := runTableGroupDDL(t, t.Context(), newTestCreate(admin, false), privilegeExec("t"))
		_ = requirePgError(t, err, mterrors.PgSSInternalError)
		assert.Equal(t, 1, admin.getCalls, "a definite error is reported, not polled forever")
	}
}

func TestTableGroupDDL_TimeoutDuringTheRequestIsAStatementTimeout(t *testing.T) {
	admin := &fakeTableGroupAdmin{createErr: status.Error(codes.DeadlineExceeded, "context deadline exceeded")}
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := runTableGroupDDL(t, ctx, newTestCreate(admin, false), privilegeExec("t"))
	require.ErrorIs(t, err, context.DeadlineExceeded, "the server reports a statement timeout as 57014")
}

func TestTableGroupPrivilegeSQL_QualifiesEveryOperator(t *testing.T) {
	// The check runs with the caller's search_path; a bare = could resolve to
	// an operator the caller defined.
	assert.NotRegexp(t, `[^)]\s=\s`, tableGroupPrivilegeSQL)
	assert.Contains(t, tableGroupPrivilegeSQL, "rolname OPERATOR(pg_catalog.=) current_user")
}

func TestTableGroupDDL_CancelOnlyStopsTheWait(t *testing.T) {
	admin := &fakeTableGroupAdmin{
		createRes: &multipoolermanagerdatapb.CreateTableGroupResponse{OperationId: testTableGroupOpID},
		ops:       []fakeOpAnswer{opIn(multipoolermanagerdatapb.OperationState_OPERATION_STATE_CREATING, "")},
	}
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(20*time.Millisecond, cancel)
	results, err := runTableGroupDDL(t, ctx, newTestCreate(admin, false), privilegeExec("t"))
	require.ErrorIs(t, err, context.Canceled, "the server reports a cancel as 57014")
	require.Len(t, results, 1, "only the announcing notice was sent")
	assert.Zero(t, admin.dropCalls, "the statement never cancels the operation itself")
}

func TestTableGroupDDL_PermissionDenied(t *testing.T) {
	for _, prim := range []*TableGroupDDL{newTestCreate(&fakeTableGroupAdmin{}, false), newTestDrop(&fakeTableGroupAdmin{}, false)} {
		admin := prim.admin.(*fakeTableGroupAdmin)
		_, err := runTableGroupDDL(t, t.Context(), prim, privilegeExec("f"))
		d := requirePgError(t, err, mterrors.PgSSInsufficientPrivilege)
		assert.Contains(t, d.Message, "permission denied to")
		assert.Equal(t, "Must be a member of multigres_topo.", d.Hint)
		assert.Empty(t, admin.databases, "the catalog is never called")
	}
}

func TestTableGroupDDL_RefusedInTransactionBlock(t *testing.T) {
	admin := &fakeTableGroupAdmin{}
	exec := privilegeExec("t")
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	err := newTestCreate(admin, false).StreamExecute(t.Context(), exec, conn, handler.NewMultigatewayConnectionState(), nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	d := requirePgError(t, err, mterrors.PgSSActiveTransaction)
	assert.Equal(t, "CREATE TABLEGROUP cannot run inside a transaction block", d.Message)
	assert.Empty(t, admin.databases)
}

func TestTableGroupDDL_ReadOnlyIsReportedBeforeTheTransactionBlock(t *testing.T) {
	// As PostgreSQL does for CREATE DATABASE in BEGIN READ ONLY.
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	err := newTestCreate(&fakeTableGroupAdmin{}, false).StreamExecute(t.Context(), sessionExec("on", "t"), conn, handler.NewMultigatewayConnectionState(), nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	d := requirePgError(t, err, mterrors.PgSSReadOnlyTransaction)
	assert.Equal(t, "cannot execute CREATE TABLEGROUP in a read-only transaction", d.Message)
}

func TestTableGroupDDL_RefusedInAPipeline(t *testing.T) {
	admin := &fakeTableGroupAdmin{}
	exec := privilegeExec("t")
	state := handler.NewMultigatewayConnectionState()
	state.SetPipelining(true)
	err := newTestDrop(admin, false).StreamExecute(t.Context(), exec, server.NewTestConn(&bytes.Buffer{}).Conn, state, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	d := requirePgError(t, err, mterrors.PgSSActiveTransaction)
	assert.Equal(t, "DROP TABLEGROUP cannot be executed within a pipeline", d.Message)
	assert.Empty(t, admin.databases)
}

func TestTableGroupDDL_RefusedInAReadOnlySession(t *testing.T) {
	// On the replica-reads port the check runs on a standby, where
	// transaction_read_only is always on.
	for _, prim := range []*TableGroupDDL{newTestCreate(&fakeTableGroupAdmin{}, false), newTestDrop(&fakeTableGroupAdmin{}, false)} {
		admin := prim.admin.(*fakeTableGroupAdmin)
		_, err := runTableGroupDDL(t, t.Context(), prim, sessionExec("on", "t"))
		d := requirePgError(t, err, mterrors.PgSSReadOnlyTransaction)
		assert.Equal(t, "cannot execute "+prim.statementName()+" in a read-only transaction", d.Message)
		assert.Empty(t, admin.databases, "the catalog is never called")
	}
}

func TestTableGroupDDL_UnansweredSessionCheckIsRefused(t *testing.T) {
	admin := &fakeTableGroupAdmin{}
	exec := &capturingExec{mockIExecute: mockIExecute{streamExecuteResult: &sqltypes.Result{}}}
	_, err := runTableGroupDDL(t, t.Context(), newTestCreate(admin, false), exec)
	d := requirePgError(t, err, mterrors.PgSSInternalError)
	assert.Equal(t, "the CREATE TABLEGROUP privilege check returned no row", d.Message)
	assert.Empty(t, admin.databases)
}

// targetExec records the tablegroup and shard each query is routed to.
type targetExec struct {
	capturingExec
	targets []string
}

func (e *targetExec) StreamExecute(ctx context.Context, conn *server.Conn, tableGroup, shard, sql string,
	ps *query.PreparedStatement, state *handler.MultigatewayConnectionState, info PlanExecInfo,
	keepStructured bool, callback func(context.Context, *sqltypes.Result) error,
) error {
	e.targets = append(e.targets, tableGroup+"/"+shard)
	return e.capturingExec.StreamExecute(ctx, conn, tableGroup, shard, sql, ps, state, info, keepStructured, callback)
}

func TestTableGroupDDL_SessionCheckRunsOnTheCatalogShard(t *testing.T) {
	admin := &fakeTableGroupAdmin{dropRes: []*multipoolermanagerdatapb.DropTableGroupResponse{{NotFound: true}}}
	exec := &targetExec{capturingExec: *privilegeExec("t")}
	_, err := runTableGroupDDL(t, t.Context(), newTestDrop(admin, true), exec)
	require.NoError(t, err)
	assert.Equal(t, []string{constants.DefaultTableGroup + "/" + constants.DefaultShard}, exec.targets)
}

func TestTableGroupDDL_UnknownOutcomeSaysHowToFindTheRequest(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		admin := &fakeTableGroupAdmin{createErr: status.Error(codes.DeadlineExceeded, "context deadline exceeded")}
		_, err := runTableGroupDDL(t, t.Context(), newTestCreate(admin, false), privilegeExec("t"))
		d := requirePgError(t, err, mterrors.PgSSCannotConnectNow)
		assert.Equal(t, "The request may have been recorded. Run the statement again with IF NOT EXISTS to wait for it, or check multigres.operations.", d.Hint)
	})
	t.Run("drop", func(t *testing.T) {
		admin := &fakeTableGroupAdmin{dropErr: status.Error(codes.Unavailable, "connection reset")}
		_, err := runTableGroupDDL(t, t.Context(), newTestDrop(admin, false), privilegeExec("t"))
		d := requirePgError(t, err, mterrors.PgSSCannotConnectNow)
		assert.Equal(t, "The request may have been recorded. Run the statement again with IF EXISTS to wait for it, or check multigres.operations.", d.Hint)
	})
	t.Run("a refusal has no hint", func(t *testing.T) {
		admin := &fakeTableGroupAdmin{createErr: status.Error(codes.InvalidArgument, "shards overlap")}
		_, err := runTableGroupDDL(t, t.Context(), newTestCreate(admin, false), privilegeExec("t"))
		d := requirePgError(t, err, mterrors.PgSSInvalidParameterValue)
		assert.Empty(t, d.Hint)
	})
	t.Run("a cancel during the request is noticed first", func(t *testing.T) {
		admin := &fakeTableGroupAdmin{createErr: status.Error(codes.Canceled, "context canceled")}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		results, err := runTableGroupDDL(t, ctx, newTestCreate(admin, false), privilegeExec("t"))
		require.ErrorIs(t, err, context.Canceled, "the server reports a cancel as 57014")
		require.Len(t, results, 1)
		n := results[0].Notices[0]
		assert.Equal(t, "the CREATE TABLEGROUP request may have been recorded before the statement ended", n.Message)
		assert.Equal(t, "Run the statement again with IF NOT EXISTS to wait for it, or check multigres.operations.", n.Hint)
	})
}

func TestTableGroupDDL_AdminErrorCodes(t *testing.T) {
	for grpcCode, pgCode := range map[codes.Code]string{
		codes.InvalidArgument:    mterrors.PgSSInvalidParameterValue,
		codes.AlreadyExists:      mterrors.PgSSDuplicateObject,
		codes.NotFound:           mterrors.PgSSUndefinedObject,
		codes.FailedPrecondition: mterrors.PgSSObjectNotInPrerequisiteState,
		codes.Unimplemented:      mterrors.PgSSFeatureNotSupported,
		codes.Unavailable:        mterrors.PgSSCannotConnectNow,
		codes.Internal:           mterrors.PgSSInternalError,
	} {
		admin := &fakeTableGroupAdmin{createErr: status.Error(grpcCode, `tablegroup "commerce" already exists`)}
		_, err := runTableGroupDDL(t, t.Context(), newTestCreate(admin, false), privilegeExec("t"))
		d := requirePgError(t, err, pgCode)
		assert.Equal(t, `tablegroup "commerce" already exists`, d.Message, "the pooler's message is kept")
	}
}

func TestTableGroupDDL_Drop(t *testing.T) {
	t.Run("an active tablegroup waits for teardown", func(t *testing.T) {
		admin := &fakeTableGroupAdmin{
			dropRes: []*multipoolermanagerdatapb.DropTableGroupResponse{{OperationId: testTableGroupOpID, TablegroupId: 7}},
			ops:     []fakeOpAnswer{opIn(multipoolermanagerdatapb.OperationState_OPERATION_STATE_DROPPING, ""), opIn(multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE, "")},
		}
		results, err := runTableGroupDDL(t, t.Context(), newTestDrop(admin, false), privilegeExec("t"))
		require.NoError(t, err)
		require.Len(t, results, 2)
		notice := results[0].Notices[0]
		assert.Contains(t, notice.Message, `dropping tablegroup "commerce" as operation `+testTableGroupOpID)
		assert.Equal(t, "Follow it in multigres.operations.", notice.Hint, "a drop cannot be cancelled")
		assert.Equal(t, "DROP TABLEGROUP", results[1].CommandTag)
	})
	t.Run("a failed drop is an error, never success", func(t *testing.T) {
		admin := &fakeTableGroupAdmin{
			dropRes: []*multipoolermanagerdatapb.DropTableGroupResponse{{OperationId: testTableGroupOpID, TablegroupId: 7}},
			ops:     []fakeOpAnswer{opIn(multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED, "teardown failed")},
		}
		results, err := runTableGroupDDL(t, t.Context(), newTestDrop(admin, false), privilegeExec("t"))
		d := requirePgError(t, err, mterrors.PgSSSystemError)
		assert.Equal(t, `tablegroup "commerce" was not dropped: teardown failed`, d.Message)
		for _, r := range results {
			assert.Empty(t, r.CommandTag, "a failed drop must not send a command tag")
		}
	})
	t.Run("IF EXISTS on a missing tablegroup is a notice", func(t *testing.T) {
		admin := &fakeTableGroupAdmin{dropRes: []*multipoolermanagerdatapb.DropTableGroupResponse{{NotFound: true}}}
		results, err := runTableGroupDDL(t, t.Context(), newTestDrop(admin, true), privilegeExec("t"))
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, `tablegroup "commerce" does not exist, skipping`, results[0].Notices[0].Message)
		assert.Equal(t, "DROP TABLEGROUP", results[0].CommandTag)
		assert.Zero(t, admin.getCalls)
	})
	t.Run("a creation it cancels ending failed is success", func(t *testing.T) {
		admin := &fakeTableGroupAdmin{
			dropRes: []*multipoolermanagerdatapb.DropTableGroupResponse{{OperationId: testTableGroupOpID, CancelledCreation: true}},
			ops:     []fakeOpAnswer{opIn(multipoolermanagerdatapb.OperationState_OPERATION_STATE_CREATING, ""), opIn(multipoolermanagerdatapb.OperationState_OPERATION_STATE_FAILED, "cancelled")},
		}
		results, err := runTableGroupDDL(t, t.Context(), newTestDrop(admin, false), privilegeExec("t"))
		require.NoError(t, err)
		assert.Contains(t, results[0].Notices[0].Message, `cancelling the creation of tablegroup "commerce"`)
		assert.Equal(t, "DROP TABLEGROUP", results[len(results)-1].CommandTag)
		assert.Equal(t, 1, admin.dropCalls)
	})
	t.Run("a creation that completes first is dropped again", func(t *testing.T) {
		admin := &fakeTableGroupAdmin{
			dropRes: []*multipoolermanagerdatapb.DropTableGroupResponse{
				{OperationId: testTableGroupOpID, CancelledCreation: true},
				{OperationId: testTableGroupOpID},
			},
			ops: []fakeOpAnswer{opIn(multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE, "")},
		}
		results, err := runTableGroupDDL(t, t.Context(), newTestDrop(admin, false), privilegeExec("t"))
		require.NoError(t, err)
		assert.Equal(t, 2, admin.dropCalls)
		require.Len(t, results, 3, "two announcing notices and the command tag")
		assert.Contains(t, results[1].Notices[0].Message, `dropping tablegroup "commerce"`)
		assert.Equal(t, "DROP TABLEGROUP", results[2].CommandTag)
	})
	t.Run("pooler refusals keep their message", func(t *testing.T) {
		admin := &fakeTableGroupAdmin{dropErr: status.Error(codes.FailedPrecondition, "the default tablegroup cannot be dropped")}
		_, err := runTableGroupDDL(t, t.Context(), newTestDrop(admin, false), privilegeExec("t"))
		d := requirePgError(t, err, mterrors.PgSSObjectNotInPrerequisiteState)
		assert.Equal(t, "the default tablegroup cannot be dropped", d.Message)
	})
}

func TestTableGroupDDL_PortalDelegates(t *testing.T) {
	admin := &fakeTableGroupAdmin{dropRes: []*multipoolermanagerdatapb.DropTableGroupResponse{{NotFound: true}}}
	prim := newTestDrop(admin, true)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	var tags []string
	err := prim.PortalStreamExecute(t.Context(), privilegeExec("t"), conn, handler.NewMultigatewayConnectionState(), nil, 0, false, PlanExecInfo{},
		func(_ context.Context, r *sqltypes.Result) error {
			tags = append(tags, r.CommandTag)
			return nil
		})
	require.NoError(t, err)
	assert.Equal(t, []string{"DROP TABLEGROUP"}, tags)
}

func TestTableGroupDDL_RunsOnTheDefaultTableGroup(t *testing.T) {
	assert.Equal(t, "default", newTestCreate(nil, false).GetTableGroup())
	assert.Equal(t, "default", newTestDrop(nil, false).GetTableGroup())
}
