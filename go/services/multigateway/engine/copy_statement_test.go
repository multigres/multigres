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

package engine

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	pgClient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// mockIExecute is a mock implementation of IExecute for testing.
type mockIExecute struct {
	// StreamExecute behavior
	streamExecuteErr error
	// lastStreamResv captures the reservation intent of the most recent
	// StreamExecute call so tests can assert what a primitive forwarded.
	lastStreamResv PlanExecInfo

	// CopyInitiate behavior
	copyInitiateErr     error
	copyInitiateFormat  int16
	copyInitiateFormats []int16

	// CopySendData behavior
	copySendDataErr error

	// CopyFinalize behavior
	copyFinalizeErr error

	// Track calls
	copyAbortCalled atomic.Int32
	copyAbortErr    error

	// ReleaseAllReservedConnections behavior
	releaseAllCalled atomic.Int32
	releaseAllErr    error

	// CopyOutInitiate behavior (TO STDOUT direction)
	copyOutInitiateErr     error
	copyOutInitiateFormat  int16
	copyOutInitiateFormats []int16
	copyOutInitiateNotices []*mterrors.PgDiagnostic

	// CopyOutStream behavior — controls what frames the executor
	// "pumps" back via the onMessage callback before returning the
	// final Result.
	copyOutStreamMessages []pgClient.CopyOutMessage
	copyOutStreamResult   *sqltypes.Result
	copyOutStreamErr      error
}

func (m *mockIExecute) StreamExecute(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	sql string,
	preparedStatement *query.PreparedStatement,
	state *handler.MultiGatewayConnectionState,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	m.lastStreamResv = info
	return m.streamExecuteErr
}

func (m *mockIExecute) PortalStreamExecute(
	ctx context.Context,
	tableGroup string,
	shard string,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	includeDescribe bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	m.lastStreamResv = info
	return nil
}

func (m *mockIExecute) Describe(
	ctx context.Context,
	tableGroup string,
	shard string,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	preparedStatementInfo *preparedstatement.PreparedStatementInfo,
) (*query.StatementDescription, error) {
	return nil, nil
}

func (m *mockIExecute) CopyInitiate(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	queryStr string,
	state *handler.MultiGatewayConnectionState,
	callback func(ctx context.Context, result *sqltypes.Result) error,
) (format int16, columnFormats []int16, err error) {
	if m.copyInitiateErr != nil {
		return 0, nil, m.copyInitiateErr
	}
	return m.copyInitiateFormat, m.copyInitiateFormats, nil
}

func (m *mockIExecute) CopySendData(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	state *handler.MultiGatewayConnectionState,
	data []byte,
) error {
	return m.copySendDataErr
}

func (m *mockIExecute) CopyFinalize(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	state *handler.MultiGatewayConnectionState,
	finalData []byte,
	callback func(ctx context.Context, result *sqltypes.Result) error,
) error {
	return m.copyFinalizeErr
}

func (m *mockIExecute) CopyAbort(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	state *handler.MultiGatewayConnectionState,
) error {
	m.copyAbortCalled.Add(1)
	return m.copyAbortErr
}

func (m *mockIExecute) CopyOutInitiate(
	context.Context,
	*server.Conn,
	string,
	string,
	string,
	*handler.MultiGatewayConnectionState,
) (int16, []int16, []*mterrors.PgDiagnostic, error) {
	if m.copyOutInitiateErr != nil {
		return 0, nil, m.copyOutInitiateNotices, m.copyOutInitiateErr
	}
	return m.copyOutInitiateFormat, m.copyOutInitiateFormats, m.copyOutInitiateNotices, nil
}

func (m *mockIExecute) CopyOutStream(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	state *handler.MultiGatewayConnectionState,
	onMessage func(pgClient.CopyOutMessage) error,
) (*sqltypes.Result, error) {
	for _, msg := range m.copyOutStreamMessages {
		if err := onMessage(msg); err != nil {
			return nil, err
		}
	}
	if m.copyOutStreamErr != nil {
		return nil, m.copyOutStreamErr
	}
	return m.copyOutStreamResult, nil
}

func (m *mockIExecute) ConcludeTransaction(
	context.Context,
	*server.Conn,
	*handler.MultiGatewayConnectionState,
	multipoolerpb.TransactionConclusion,
	[]string,
	bool,
	bool,
	func(context.Context, *sqltypes.Result) error,
) error {
	return nil
}

func (m *mockIExecute) DiscardTempTables(ctx context.Context, conn *server.Conn, state *handler.MultiGatewayConnectionState, callback func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *mockIExecute) ReleaseAllReservedConnections(context.Context, *server.Conn, *handler.MultiGatewayConnectionState) error {
	m.releaseAllCalled.Add(1)
	return m.releaseAllErr
}

// Helper to create a CopyStatement for testing
func newTestCopyStatement() *CopyStatement {
	return NewCopyStatement("test_tablegroup", "COPY t FROM STDIN", &ast.CopyStmt{
		IsFrom:   true,
		Relation: &ast.RangeVar{RelName: "t"},
	})
}

// TestCopyStatement_CopyInitiateError tests that CopyAbort is NOT called
// when CopyInitiate fails (defer is set up after CopyInitiate succeeds).
func TestCopyStatement_CopyInitiateError(t *testing.T) {
	mockExec := &mockIExecute{
		copyInitiateErr: errors.New("initiate failed"),
	}

	copyStmt := newTestCopyStatement()

	err := copyStmt.StreamExecute(
		context.Background(),
		mockExec,
		nil, // conn not used when CopyInitiate fails
		&handler.MultiGatewayConnectionState{},
		nil,
		PlanExecInfo{},
		func(ctx context.Context, result *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	// CopyStatement now surfaces the underlying error un-wrapped so the
	// gateway re-emits a verbatim ErrorResponse to the client.
	require.Equal(t, "initiate failed", err.Error())
	// CopyAbort should NOT be called since CopyInitiate failed before defer is set up
	require.Equal(t, int32(0), mockExec.copyAbortCalled.Load())
}

// TestCopyStatement_WriteCopyInResponseError tests that CopyAbort is called
// when WriteCopyInResponse fails (via defer).
func TestCopyStatement_WriteCopyInResponseError(t *testing.T) {
	mockExec := &mockIExecute{
		copyInitiateFormat:  0,
		copyInitiateFormats: []int16{0, 0},
	}

	// Create a conn with empty read buffer - WriteCopyInResponse will fail
	// because the underlying writer has issues (we'll use a closed buffer scenario)
	readBuf := &bytes.Buffer{}
	testConn := server.NewTestConn(readBuf)

	copyStmt := newTestCopyStatement()

	err := copyStmt.StreamExecute(
		context.Background(),
		mockExec,
		testConn.Conn,
		&handler.MultiGatewayConnectionState{},
		nil,
		PlanExecInfo{},
		func(ctx context.Context, result *sqltypes.Result) error { return nil },
	)

	// The error comes from trying to read message type from empty buffer after flush
	require.Error(t, err)
	// CopyAbort should be called via defer
	require.Equal(t, int32(1), mockExec.copyAbortCalled.Load())
}

// TestCopyStatement_CopySendDataError tests that CopyAbort is called
// when CopySendData fails (via defer).
func TestCopyStatement_CopySendDataError(t *testing.T) {
	mockExec := &mockIExecute{
		copyInitiateFormat:  0,
		copyInitiateFormats: []int16{0, 0},
		copySendDataErr:     errors.New("send data failed"),
	}

	// Prepare read buffer with CopyData message
	readBuf := &bytes.Buffer{}
	server.WriteCopyDataMessage(readBuf, []byte("test data"))

	testConn := server.NewTestConn(readBuf)

	copyStmt := newTestCopyStatement()

	err := copyStmt.StreamExecute(
		context.Background(),
		mockExec,
		testConn.Conn,
		&handler.MultiGatewayConnectionState{},
		nil,
		PlanExecInfo{},
		func(ctx context.Context, result *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	// CopySendData errors surface un-wrapped — gateway re-emits the
	// verbatim PG ErrorResponse.
	require.Equal(t, "send data failed", err.Error())
	// CopyAbort should be called via defer
	require.Equal(t, int32(1), mockExec.copyAbortCalled.Load())
}

// TestCopyStatement_CopyFinalizeError tests that CopyAbort is NOT called
// when CopyFinalize fails. CopyFinalize owns the full tail-end lifecycle:
// it drains ReadyForQuery, updates reserved-state tracking via the gateway,
// and either releases the connection or keeps it alive for other reasons
// (e.g., a wrapping transaction). Running CopyAbort on top would either be
// a no-op or actively clobber the surviving reserved-state tracking.
func TestCopyStatement_CopyFinalizeError(t *testing.T) {
	mockExec := &mockIExecute{
		copyInitiateFormat:  0,
		copyInitiateFormats: []int16{0, 0},
		copyFinalizeErr:     errors.New("finalize failed"),
	}

	// Prepare read buffer with CopyDone message
	readBuf := &bytes.Buffer{}
	server.WriteCopyDoneMessage(readBuf)

	testConn := server.NewTestConn(readBuf)

	copyStmt := newTestCopyStatement()

	err := copyStmt.StreamExecute(
		context.Background(),
		mockExec,
		testConn.Conn,
		&handler.MultiGatewayConnectionState{},
		nil,
		PlanExecInfo{},
		func(ctx context.Context, result *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "finalize failed")
	require.Equal(t, int32(0), mockExec.copyAbortCalled.Load())
}

// TestCopyStatement_CopyFailMessage tests that CopyAbort is called
// when client sends CopyFail message.
func TestCopyStatement_CopyFailMessage(t *testing.T) {
	mockExec := &mockIExecute{
		copyInitiateFormat:  0,
		copyInitiateFormats: []int16{0, 0},
	}

	// Prepare read buffer with CopyFail message
	readBuf := &bytes.Buffer{}
	server.WriteCopyFailMessage(readBuf, "client aborted")

	testConn := server.NewTestConn(readBuf)

	copyStmt := newTestCopyStatement()

	err := copyStmt.StreamExecute(
		context.Background(),
		mockExec,
		testConn.Conn,
		&handler.MultiGatewayConnectionState{},
		nil,
		PlanExecInfo{},
		func(ctx context.Context, result *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	// Match PostgreSQL: a client CopyFail surfaces as query_canceled (57014)
	// with "COPY from stdin failed: <msg>".
	require.Contains(t, err.Error(), "COPY from stdin failed: client aborted")
	require.Equal(t, mterrors.PgSSQueryCanceled, mterrors.ExtractSQLSTATE(err))
	// CopyAbort should be called via defer
	require.Equal(t, int32(1), mockExec.copyAbortCalled.Load())
}

// TestCopyStatement_Success tests that CopyAbort is NOT called on success.
func TestCopyStatement_Success(t *testing.T) {
	mockExec := &mockIExecute{
		copyInitiateFormat:  0,
		copyInitiateFormats: []int16{0, 0},
	}

	// Prepare read buffer with CopyData followed by CopyDone
	readBuf := &bytes.Buffer{}
	server.WriteCopyDataMessage(readBuf, []byte("row1\n"))
	server.WriteCopyDataMessage(readBuf, []byte("row2\n"))
	server.WriteCopyDoneMessage(readBuf)

	testConn := server.NewTestConn(readBuf)

	copyStmt := newTestCopyStatement()

	err := copyStmt.StreamExecute(
		context.Background(),
		mockExec,
		testConn.Conn,
		&handler.MultiGatewayConnectionState{},
		nil,
		PlanExecInfo{},
		func(ctx context.Context, result *sqltypes.Result) error { return nil },
	)

	require.NoError(t, err)
	// CopyAbort should NOT be called on success
	require.Equal(t, int32(0), mockExec.copyAbortCalled.Load())
}

// TestCopyStatement_UnexpectedMessageType tests that CopyAbort is called
// when an unexpected message type is received.
func TestCopyStatement_UnexpectedMessageType(t *testing.T) {
	mockExec := &mockIExecute{
		copyInitiateFormat:  0,
		copyInitiateFormats: []int16{0, 0},
	}

	// Prepare read buffer with unexpected message type ('Q' for Query)
	readBuf := &bytes.Buffer{}
	readBuf.WriteByte('Q')                        // message type
	readBuf.Write([]byte{0x00, 0x00, 0x00, 0x05}) // length = 5 (4 + 1 for null terminator)
	readBuf.WriteByte(0)                          // empty query with null terminator

	testConn := server.NewTestConn(readBuf)

	copyStmt := newTestCopyStatement()

	err := copyStmt.StreamExecute(
		context.Background(),
		mockExec,
		testConn.Conn,
		&handler.MultiGatewayConnectionState{},
		nil,
		PlanExecInfo{},
		func(ctx context.Context, result *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected message type during COPY")
	// CopyAbort should be called via defer
	require.Equal(t, int32(1), mockExec.copyAbortCalled.Load())
}

// TestCopyStatement_String tests the String method.
func TestCopyStatement_String(t *testing.T) {
	tests := []struct {
		name     string
		isFrom   bool
		relName  string
		expected string
	}{
		{
			name:     "COPY FROM STDIN",
			isFrom:   true,
			relName:  "users",
			expected: "CopyStatement(users FROM STDIN)",
		},
		{
			name:     "COPY TO STDOUT",
			isFrom:   false,
			relName:  "orders",
			expected: "CopyStatement(orders TO STDOUT)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copyStmt := NewCopyStatement("tg", "query", &ast.CopyStmt{
				IsFrom:   tt.isFrom,
				Relation: &ast.RangeVar{RelName: tt.relName},
			})
			require.Equal(t, tt.expected, copyStmt.String())
		})
	}
}

// TestCopyStatement_GetTableGroup tests the GetTableGroup method.
func TestCopyStatement_GetTableGroup(t *testing.T) {
	copyStmt := NewCopyStatement("my_tablegroup", "query", &ast.CopyStmt{
		IsFrom:   true,
		Relation: &ast.RangeVar{RelName: "t"},
	})
	require.Equal(t, "my_tablegroup", copyStmt.GetTableGroup())
}

// TestCopyStatement_GetQuery tests the GetQuery method.
func TestCopyStatement_GetQuery(t *testing.T) {
	copyStmt := NewCopyStatement("tg", "COPY users FROM STDIN", &ast.CopyStmt{
		IsFrom:   true,
		Relation: &ast.RangeVar{RelName: "users"},
	})
	require.Equal(t, "COPY users FROM STDIN", copyStmt.GetQuery())
}

// newTestCopyToStdoutStatement builds a TO-STDOUT CopyStatement for tests.
func newTestCopyToStdoutStatement() *CopyStatement {
	return NewCopyStatement("test_tablegroup", "COPY t TO STDOUT", &ast.CopyStmt{
		IsFrom:   false,
		Relation: &ast.RangeVar{RelName: "t"},
	})
}

// recordingCallback records every Result the engine emits to the
// higher-level callback so tests can assert ordering / payload.
type recordedResult struct {
	rows         int
	commandTag   string
	noticeCount  int
	rowsAffected uint64
}

func recordingCallback(out *[]recordedResult) func(context.Context, *sqltypes.Result) error {
	return func(_ context.Context, r *sqltypes.Result) error {
		*out = append(*out, recordedResult{
			rows:         len(r.Rows),
			commandTag:   r.CommandTag,
			noticeCount:  len(r.Notices),
			rowsAffected: r.RowsAffected,
		})
		return nil
	}
}

// TestStreamCopyOut_Success exercises the happy path:
//   - CopyOutInitiate returns format + columnFormats + zero initiation notices
//   - CopyOutStream pumps two CopyData chunks and an interleaved Notice
//   - The final Result carries a CommandTag + trailing notices
//
// Verifies that the engine writes CopyOutResponse, forwards each chunk,
// orders the trailing notices AFTER CopyDone (matching upstream PG), and
// hands the CommandTag-bearing Result to the higher-level callback.
func TestStreamCopyOut_Success(t *testing.T) {
	mockExec := &mockIExecute{
		copyOutInitiateFormat:  0,
		copyOutInitiateFormats: []int16{0, 0},
		copyOutStreamMessages: []pgClient.CopyOutMessage{
			{Data: []byte("1\tAlice\n")},
			{Notice: &mterrors.PgDiagnostic{Severity: "NOTICE", Code: "00000", Message: "mid-stream"}},
			{Data: []byte("2\tBob\n")},
		},
		copyOutStreamResult: &sqltypes.Result{
			CommandTag:   "COPY 2",
			RowsAffected: 2,
			Notices: []*mterrors.PgDiagnostic{
				{Severity: "NOTICE", Code: "00000", Message: "after-statement trigger"},
			},
		},
	}

	testConn := server.NewTestConn(&bytes.Buffer{})
	var got []recordedResult

	err := newTestCopyToStdoutStatement().StreamExecute(
		context.Background(),
		mockExec,
		testConn.Conn,
		&handler.MultiGatewayConnectionState{},
		nil,
		PlanExecInfo{},
		recordingCallback(&got),
	)
	require.NoError(t, err)

	// Defer abort must NOT have fired on the success path.
	require.Equal(t, int32(0), mockExec.copyAbortCalled.Load())

	// Two callback invocations: one mid-stream notice forward, one for
	// the trailing-notices result. Plus the CommandTag-bearing result at
	// the end → 3 callbacks total. (CopyData chunks are written directly
	// to conn, not through the callback.)
	require.Len(t, got, 3, "expected mid-stream notice + trailing notices + CommandComplete result")
	assert.Equal(t, 1, got[0].noticeCount, "first callback is the mid-stream NoticeResponse")
	assert.Equal(t, "", got[0].commandTag)

	assert.Equal(t, 1, got[1].noticeCount, "second callback is the trailing notice batch")
	assert.Equal(t, "", got[1].commandTag, "trailing notices come BEFORE the CommandTag callback")

	assert.Equal(t, "COPY 2", got[2].commandTag, "final callback carries CommandTag")
	assert.Equal(t, uint64(2), got[2].rowsAffected)
}

// TestStreamCopyOut_InitiateError surfaces the PG error un-wrapped and
// skips the deferred CopyAbort — CopyOutInitiate failed before any
// reservation was held.
func TestStreamCopyOut_InitiateError(t *testing.T) {
	mockExec := &mockIExecute{
		copyOutInitiateErr: errors.New("column \"foo\" does not exist"),
	}

	testConn := server.NewTestConn(&bytes.Buffer{})
	err := newTestCopyToStdoutStatement().StreamExecute(
		context.Background(),
		mockExec,
		testConn.Conn,
		&handler.MultiGatewayConnectionState{},
		nil,
		PlanExecInfo{},
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)
	require.Error(t, err)
	assert.Equal(t, "column \"foo\" does not exist", err.Error(),
		"PG errors flow back un-wrapped so the gateway re-emits a verbatim ErrorResponse")
	// No reservation was created → no deferred abort to run.
	assert.Equal(t, int32(0), mockExec.copyAbortCalled.Load())
}

// TestStreamCopyOut_DeferredAbortFiresOnPreStreamFailure verifies the
// deferred guard: when the pre-CopyOutResponse notice callback fails
// AFTER CopyOutInitiate succeeded (so the multipooler holds ReasonCopy
// on the reserved conn), the deferred CopyAbort must run so the
// reservation isn't leaked.
func TestStreamCopyOut_DeferredAbortFiresOnPreStreamFailure(t *testing.T) {
	mockExec := &mockIExecute{
		copyOutInitiateFormat:  0,
		copyOutInitiateFormats: []int16{0, 0},
		copyOutInitiateNotices: []*mterrors.PgDiagnostic{
			{Severity: "NOTICE", Code: "00000", Message: "before-statement"},
		},
	}

	testConn := server.NewTestConn(&bytes.Buffer{})
	cbErr := errors.New("client write failed")
	err := newTestCopyToStdoutStatement().StreamExecute(
		context.Background(),
		mockExec,
		testConn.Conn,
		&handler.MultiGatewayConnectionState{},
		nil,
		PlanExecInfo{},
		func(_ context.Context, _ *sqltypes.Result) error {
			// First (and only) callback is the pre-stream initiation
			// notice — make it fail so the engine bails before
			// CopyOutStream is reached.
			return cbErr
		},
	)
	require.ErrorIs(t, err, cbErr)
	assert.Equal(t, int32(1), mockExec.copyAbortCalled.Load(),
		"deferred abort must run when failure happens in the pre-stream window")
}

// TestStreamCopyOut_StreamErrorSkipsDeferredAbort verifies that when
// CopyOutStream returns an error, the engine clears streamCompleted so
// the deferred CopyAbort does NOT fire on top of the executor's own
// cleanup (the executor already released or kept-state the conn via
// abortCopyOut / RemoveReservationReason).
func TestStreamCopyOut_StreamErrorSkipsDeferredAbort(t *testing.T) {
	streamErr := errors.New("PG mid-stream error")
	mockExec := &mockIExecute{
		copyOutInitiateFormat:  0,
		copyOutInitiateFormats: []int16{0, 0},
		copyOutStreamErr:       streamErr,
	}

	testConn := server.NewTestConn(&bytes.Buffer{})
	err := newTestCopyToStdoutStatement().StreamExecute(
		context.Background(),
		mockExec,
		testConn.Conn,
		&handler.MultiGatewayConnectionState{},
		nil,
		PlanExecInfo{},
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)
	require.ErrorIs(t, err, streamErr)
	assert.Equal(t, int32(0), mockExec.copyAbortCalled.Load(),
		"deferred abort must NOT run on top of executor's own CopyOutStream cleanup")
}
