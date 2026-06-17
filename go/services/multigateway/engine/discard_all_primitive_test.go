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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// recordingHandler is a server.Handler that records HandleClose calls. Only
// HandleClose is exercised by DiscardAllPrimitive; the rest are no-ops.
type recordingHandler struct {
	closeCalls []struct {
		typ  byte
		name string
	}
}

func (h *recordingHandler) HandleClose(_ context.Context, _ *server.Conn, typ byte, name string) error {
	h.closeCalls = append(h.closeCalls, struct {
		typ  byte
		name string
	}{typ, name})
	return nil
}

func (h *recordingHandler) HandleQuery(context.Context, *server.Conn, string, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (h *recordingHandler) HandleParse(context.Context, *server.Conn, string, string, []uint32) error {
	return nil
}

func (h *recordingHandler) HandleBind(context.Context, *server.Conn, string, string, [][]byte, []int16, []int16) error {
	return nil
}

func (h *recordingHandler) HandleExecute(context.Context, *server.Conn, string, int32, bool, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (h *recordingHandler) HandleDescribe(context.Context, *server.Conn, byte, string) (*query.StatementDescription, error) {
	return nil, nil
}

func (h *recordingHandler) HandleSync(context.Context, *server.Conn) error { return nil }

func (h *recordingHandler) ConnectionClosed(*server.Conn) {}

func (h *recordingHandler) GetPreparedStatementInfo(uint32, string) *preparedstatement.PreparedStatementInfo {
	return nil
}

func newDiscardTestConn(t *testing.T, h server.Handler) *server.Conn {
	t.Helper()
	return server.NewTestConn(&bytes.Buffer{}, server.WithTestHandler(h)).Conn
}

// TestDiscardAllPrimitive_RejectsInTransaction verifies DISCARD ALL is rejected
// with SQLSTATE 25001 inside a transaction block (matching PG's
// PreventInTransactionBlock) and that it does NOT touch any state or release the
// reserved connection in that case.
func TestDiscardAllPrimitive_RejectsInTransaction(t *testing.T) {
	mockExec := &mockIExecute{}
	h := &recordingHandler{}
	conn := newDiscardTestConn(t, h)
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	state := handler.NewMultiGatewayConnectionState()
	state.SetSessionVariable("work_mem", "64MB")

	prim := NewDiscardAllPrimitive("DISCARD ALL")
	var got *sqltypes.Result
	err := prim.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{},
		func(_ context.Context, r *sqltypes.Result) error { got = r; return nil })

	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSActiveTransaction),
		"expected SQLSTATE %s, got %v", mterrors.PgSSActiveTransaction, err)
	assert.Nil(t, got, "no result should be sent when DISCARD ALL is rejected")
	assert.Empty(t, h.closeCalls, "prepared statements must not be dropped on rejection")
	assert.Equal(t, int32(0), mockExec.releaseAllCalled.Load(),
		"reserved connection must not be released on rejection")
	v, ok := state.GetSessionVariable("work_mem")
	assert.True(t, ok, "session settings must be left intact on rejection")
	assert.Equal(t, "64MB", v)
}

// TestDiscardAllPrimitive_Success verifies the success path resets gateway
// session state, drops all prepared statements, releases the reserved
// connection, and returns the DISCARD ALL command tag — all without forwarding
// the statement to a backend.
func TestDiscardAllPrimitive_Success(t *testing.T) {
	mockExec := &mockIExecute{}
	h := &recordingHandler{}
	conn := newDiscardTestConn(t, h)
	// Idle (not in a transaction) — the default.

	state := handler.NewMultiGatewayConnectionState()
	state.SetSessionVariable("work_mem", "64MB")
	state.AddOpenHoldCursor("c1")

	prim := NewDiscardAllPrimitive("DISCARD ALL")
	var got *sqltypes.Result
	err := prim.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{},
		func(_ context.Context, r *sqltypes.Result) error { got = r; return nil })

	require.NoError(t, err)

	// DEALLOCATE ALL: HandleClose('A') drops every prepared statement.
	require.Len(t, h.closeCalls, 1)
	assert.Equal(t, byte('A'), h.closeCalls[0].typ)
	assert.Empty(t, h.closeCalls[0].name)

	// RESET ALL: session settings cleared.
	_, ok := state.GetSessionVariable("work_mem")
	assert.False(t, ok, "RESET ALL must clear session settings")

	// CLOSE ALL: gateway HOLD-cursor bookkeeping cleared.
	assert.False(t, state.HasAnyOpenHoldCursor(), "DISCARD ALL must clear HOLD cursor tracking")

	// Reserved connection released exactly once.
	assert.Equal(t, int32(1), mockExec.releaseAllCalled.Load())

	require.NotNil(t, got)
	assert.Equal(t, "DISCARD ALL", got.CommandTag)
}

// TestDiscardAllPrimitive_ReleaseError surfaces a reserved-connection release
// failure to the caller and verifies DISCARD ALL is atomic on that failure: the
// release runs first, so when it errors none of the gateway-side resets (prepared
// statements, session GUCs, HOLD-cursor tracking) have run.
func TestDiscardAllPrimitive_ReleaseError(t *testing.T) {
	mockExec := &mockIExecute{releaseAllErr: errors.New("release boom")}
	h := &recordingHandler{}
	conn := newDiscardTestConn(t, h)
	state := handler.NewMultiGatewayConnectionState()
	state.SetSessionVariable("work_mem", "64MB")
	state.AddOpenHoldCursor("c1")

	prim := NewDiscardAllPrimitive("DISCARD ALL")
	err := prim.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })

	require.Error(t, err)
	assert.Contains(t, err.Error(), "release boom")

	// Atomic on failure: the session is left untouched, not half-reset.
	assert.Empty(t, h.closeCalls, "prepared statements must not be dropped when release fails")
	v, ok := state.GetSessionVariable("work_mem")
	assert.True(t, ok, "session settings must be left intact when release fails")
	assert.Equal(t, "64MB", v)
	assert.True(t, state.HasAnyOpenHoldCursor(), "HOLD cursor tracking must be left intact when release fails")
}

// TestDiscardAllPrimitive_PortalStreamExecute exercises the extended-protocol
// delegate path.
func TestDiscardAllPrimitive_PortalStreamExecute(t *testing.T) {
	mockExec := &mockIExecute{}
	conn := newDiscardTestConn(t, &recordingHandler{})
	state := handler.NewMultiGatewayConnectionState()

	prim := NewDiscardAllPrimitive("DISCARD ALL")
	var got *sqltypes.Result
	err := prim.PortalStreamExecute(context.Background(), mockExec, conn, state, nil, 0, false, PlanExecInfo{},
		func(_ context.Context, r *sqltypes.Result) error { got = r; return nil })

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "DISCARD ALL", got.CommandTag)
	assert.Equal(t, int32(1), mockExec.releaseAllCalled.Load())
}

func TestDiscardAllPrimitive_StringAndGetters(t *testing.T) {
	prim := NewDiscardAllPrimitive("DISCARD ALL")
	assert.Equal(t, "DiscardAll(DISCARD ALL)", prim.String())
	assert.Equal(t, "DISCARD ALL", prim.GetQuery())
	assert.Empty(t, prim.GetTableGroup())
}
