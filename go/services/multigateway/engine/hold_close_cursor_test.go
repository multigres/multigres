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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// TestHoldCursorRoute_String verifies the observability label that flows into
// span attributes and query logs.
func TestHoldCursorRoute_String(t *testing.T) {
	r := NewHoldCursorRoute("tg1", "shard1", "DECLARE c CURSOR WITH HOLD FOR SELECT 1", "c")
	assert.Equal(t, "HoldCursorRoute(c: DECLARE c CURSOR WITH HOLD FOR SELECT 1)", r.String())
	assert.Equal(t, "tg1", r.GetTableGroup())
	assert.Equal(t, "DECLARE c CURSOR WITH HOLD FOR SELECT 1", r.GetQuery())
}

func TestCloseCursorRoute_String(t *testing.T) {
	named := NewCloseCursorRoute("tg1", "shard1", "CLOSE c", "c")
	assert.Equal(t, "CloseCursorRoute(c)", named.String())
	assert.False(t, named.CloseAll)
	assert.Equal(t, "c", named.CursorName)
	assert.Equal(t, "tg1", named.GetTableGroup())
	assert.Equal(t, "CLOSE c", named.GetQuery())

	all := NewCloseAllCursorRoute("tg1", "shard1", "CLOSE ALL")
	assert.Equal(t, "CloseCursorRoute(ALL)", all.String())
	assert.True(t, all.CloseAll)
}

// TestHoldCursorRoute_StreamExecute_Success verifies the success path:
// pending pin queued, exec invoked, then OpenHoldCursors marked.
func TestHoldCursorRoute_StreamExecute_Success(t *testing.T) {
	mockExec := &mockIExecute{}
	state := handler.NewMultiGatewayConnectionState()

	route := NewHoldCursorRoute("tg1", "shard1", "DECLARE c WITH HOLD FOR …", "c")
	err := route.StreamExecute(context.Background(), mockExec, nil, state, nil, func(context.Context, *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, state.HasOpenHoldCursor("c"),
		"successful DECLARE WITH HOLD must record the cursor in the open set")
	// ScatterConn would normally Take the pending pin slot — in this unit
	// test there is no ScatterConn, so the slot stays populated. The
	// invariant under test: AppendPendingPinPortals was called.
	require.True(t, state.HasPendingPinPortals(),
		"HoldCursorRoute must enqueue the cursor name as a pending pin")
	assert.Equal(t, []string{"c"}, state.TakePendingPinPortals())
}

// TestHoldCursorRoute_StreamExecute_Failure verifies the gateway does NOT
// record the cursor as open when DECLARE fails.
func TestHoldCursorRoute_StreamExecute_Failure(t *testing.T) {
	mockExec := &mockIExecute{streamExecuteErr: errors.New("syntax error")}
	state := handler.NewMultiGatewayConnectionState()

	route := NewHoldCursorRoute("tg1", "shard1", "DECLARE bad WITH HOLD FOR …", "bad")
	err := route.StreamExecute(context.Background(), mockExec, nil, state, nil, func(context.Context, *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.False(t, state.HasOpenHoldCursor("bad"),
		"failed DECLARE must not populate OpenHoldCursors")
}

// TestCloseCursorRoute_Targets_Named verifies CLOSE <name> resolves to the
// single cursor when it's tracked, or nil when it isn't.
func TestCloseCursorRoute_Targets_Named(t *testing.T) {
	state := handler.NewMultiGatewayConnectionState()
	state.AddOpenHoldCursor("c")

	tracked := NewCloseCursorRoute("tg1", "shard1", "CLOSE c", "c")
	assert.Equal(t, []string{"c"}, tracked.targets(state))

	untracked := NewCloseCursorRoute("tg1", "shard1", "CLOSE other", "other")
	assert.Nil(t, untracked.targets(state),
		"CLOSE on a non-tracked cursor must not enqueue a release — PG handles the error")
}

// TestCloseCursorRoute_Targets_All verifies CLOSE ALL snapshots the open set
// at execution time (not plan time).
func TestCloseCursorRoute_Targets_All(t *testing.T) {
	state := handler.NewMultiGatewayConnectionState()
	state.AddOpenHoldCursor("c1")
	state.AddOpenHoldCursor("c2")

	r := NewCloseAllCursorRoute("tg1", "shard1", "CLOSE ALL")
	assert.ElementsMatch(t, []string{"c1", "c2"}, r.targets(state))

	// Late-bind: target list reflects the state at the moment targets() is
	// called, not the moment the route was constructed.
	state.AddOpenHoldCursor("c3")
	assert.ElementsMatch(t, []string{"c1", "c2", "c3"}, r.targets(state))
}

// TestCloseCursorRoute_StreamExecute_Success verifies CLOSE drops the cursor
// from OpenHoldCursors on success.
func TestCloseCursorRoute_StreamExecute_Success(t *testing.T) {
	mockExec := &mockIExecute{}
	state := handler.NewMultiGatewayConnectionState()
	state.AddOpenHoldCursor("c")

	route := NewCloseCursorRoute("tg1", "shard1", "CLOSE c", "c")
	err := route.StreamExecute(context.Background(), mockExec, nil, state, nil, func(context.Context, *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.False(t, state.HasOpenHoldCursor("c"),
		"successful CLOSE must drop the cursor from OpenHoldCursors")
	assert.Equal(t, []string{"c"}, state.TakePendingReleasePortals())
}

// TestCloseCursorRoute_StreamExecute_Failure verifies the cursor stays
// tracked when CLOSE fails — PG kept it, so the gateway must too.
func TestCloseCursorRoute_StreamExecute_Failure(t *testing.T) {
	mockExec := &mockIExecute{streamExecuteErr: errors.New("boom")}
	state := handler.NewMultiGatewayConnectionState()
	state.AddOpenHoldCursor("c")

	route := NewCloseCursorRoute("tg1", "shard1", "CLOSE c", "c")
	err := route.StreamExecute(context.Background(), mockExec, nil, state, nil, func(context.Context, *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.True(t, state.HasOpenHoldCursor("c"),
		"failed CLOSE must leave the cursor in OpenHoldCursors")
}

// TestCloseCursorRoute_PortalStreamExecute exercises the extended-protocol
// delegate path so it has coverage.
func TestCloseCursorRoute_PortalStreamExecute(t *testing.T) {
	mockExec := &mockIExecute{}
	state := handler.NewMultiGatewayConnectionState()
	state.AddOpenHoldCursor("c")

	route := NewCloseCursorRoute("tg1", "shard1", "CLOSE c", "c")
	err := route.PortalStreamExecute(context.Background(), mockExec, nil, state, nil, 0, false, func(context.Context, *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.False(t, state.HasOpenHoldCursor("c"))
}

func TestHoldCursorRoute_PortalStreamExecute(t *testing.T) {
	mockExec := &mockIExecute{}
	state := handler.NewMultiGatewayConnectionState()

	route := NewHoldCursorRoute("tg1", "shard1", "DECLARE c WITH HOLD FOR …", "c")
	err := route.PortalStreamExecute(context.Background(), mockExec, nil, state, nil, 0, false, func(context.Context, *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, state.HasOpenHoldCursor("c"))
}
