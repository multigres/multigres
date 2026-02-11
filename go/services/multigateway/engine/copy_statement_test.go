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

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// mockIExecute is a mock implementation of IExecute for testing CopyStatement.
type mockIExecute struct {
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
}

func (m *mockIExecute) StreamExecute(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	sql string,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return nil
}

func (m *mockIExecute) PortalStreamExecute(
	ctx context.Context,
	tableGroup string,
	shard string,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	callback func(context.Context, *sqltypes.Result) error,
) error {
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

func (m *mockIExecute) ConcludeTransaction(
	context.Context,
	*server.Conn,
	*handler.MultiGatewayConnectionState,
	multipoolerpb.TransactionConclusion,
	func(context.Context, *sqltypes.Result) error,
) error {
	return nil
}

func (m *mockIExecute) ReleaseAllReservedConnections(context.Context, *server.Conn, *handler.MultiGatewayConnectionState) error {
	return nil
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
		func(ctx context.Context, result *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to initiate COPY")
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
		func(ctx context.Context, result *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to send COPY data")
	// CopyAbort should be called via defer
	require.Equal(t, int32(1), mockExec.copyAbortCalled.Load())
}

// TestCopyStatement_CopyFinalizeError tests that CopyAbort is called
// when CopyFinalize fails (via defer).
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
		func(ctx context.Context, result *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "finalize failed")
	// CopyAbort should be called via defer (even though CopyFinalize already cleaned up map)
	require.Equal(t, int32(1), mockExec.copyAbortCalled.Load())
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
		func(ctx context.Context, result *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "COPY failed: client aborted")
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
