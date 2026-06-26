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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// recordingPrimitive captures which dispatch method was invoked. Used to
// verify Sequence iterates and dispatches per-child.
type recordingPrimitive struct {
	streamCalls int
	portalCalls int
	err         error
}

func (r *recordingPrimitive) StreamExecute(
	context.Context, IExecute, *server.Conn,
	*handler.MultiGatewayConnectionState, []*ast.A_Const,
	PlanExecInfo,
	func(context.Context, *sqltypes.Result) error,
) error {
	r.streamCalls++
	return r.err
}

func (r *recordingPrimitive) PortalStreamExecute(
	context.Context, IExecute, *server.Conn,
	*handler.MultiGatewayConnectionState,
	*preparedstatement.PortalInfo, int32, bool,
	PlanExecInfo,
	func(context.Context, *sqltypes.Result) error,
) error {
	r.portalCalls++
	return r.err
}

func (r *recordingPrimitive) GetTableGroup() string { return "" }
func (r *recordingPrimitive) GetQuery() string      { return "" }
func (r *recordingPrimitive) String() string        { return "recordingPrimitive" }

func silentStatementTimeoutTrack(value string) *ApplySessionState {
	return NewApplySessionStateSilent("SELECT set_config('statement_timeout', '"+value+"', false)", &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "statement_timeout",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: value}}}},
	})
}

func TestSequence_StreamExecute_PrevalidatesSilentGatewayManagedTracking(t *testing.T) {
	route := &recordingPrimitive{}
	seq := NewSequence([]Primitive{route, silentStatementTimeoutTrack("5 seconds")})

	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	state := handler.NewMultiGatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)

	err := seq.StreamExecute(context.Background(), nil, conn, state, nil, PlanExecInfo{}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid value for parameter "statement_timeout": "5 seconds"`)
	assert.Equal(t, 0, route.streamCalls, "Route must not stream a success before tracking validation fails")
	assert.Equal(t, 30*time.Second, state.GetStatementTimeout())
}

func TestSequence_StreamExecute_AppliesPreparedTrackingAfterRouteSuccess(t *testing.T) {
	route := &recordingPrimitive{}
	seq := NewSequence([]Primitive{route, silentStatementTimeoutTrack("5s")})

	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	state := handler.NewMultiGatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)

	err := seq.StreamExecute(context.Background(), nil, conn, state, nil, PlanExecInfo{}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, route.streamCalls)
	assert.Equal(t, 5*time.Second, state.GetStatementTimeout())
}

func TestSequence_StreamExecute_DoesNotApplyPreparedTrackingAfterRouteFailure(t *testing.T) {
	routeErr := errors.New("backend rejected query")
	route := &recordingPrimitive{err: routeErr}
	seq := NewSequence([]Primitive{route, silentStatementTimeoutTrack("5s")})

	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	state := handler.NewMultiGatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)

	err := seq.StreamExecute(context.Background(), nil, conn, state, nil, PlanExecInfo{}, nil)
	require.ErrorIs(t, err, routeErr)
	assert.Equal(t, 1, route.streamCalls)
	assert.Equal(t, 30*time.Second, state.GetStatementTimeout())
}

// TestSequence_PortalStreamExecute_DispatchesPerChild confirms the new
// uniform dispatch: every child of a Sequence gets PortalStreamExecute
// called on it (so each primitive can decide to forward the portal,
// ignore it, or do something else). Earlier the executor reached through
// the Sequence and called StreamExecute on silent tracking while shortcutting
// the Route — both behaviors now live on the primitives themselves.
func TestSequence_PortalStreamExecute_DispatchesPerChild(t *testing.T) {
	a, b, c := &recordingPrimitive{}, &recordingPrimitive{}, &recordingPrimitive{}
	seq := NewSequence([]Primitive{a, b, c})

	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	state := handler.NewMultiGatewayConnectionState()

	err := seq.PortalStreamExecute(context.Background(), nil, conn, state, nil, 0, false, PlanExecInfo{}, nil)
	require.NoError(t, err)

	for i, p := range []*recordingPrimitive{a, b, c} {
		assert.Equal(t, 1, p.portalCalls, "child %d should have received PortalStreamExecute", i)
		assert.Equal(t, 0, p.streamCalls, "child %d should NOT have received StreamExecute", i)
	}
}

// TestSequence_PortalStreamExecute_StopsOnError confirms iteration halts on
// the first error and reports which primitive failed.
func TestSequence_PortalStreamExecute_StopsOnError(t *testing.T) {
	failure := errors.New("boom")
	a := &recordingPrimitive{}
	b := &recordingPrimitive{err: failure}
	c := &recordingPrimitive{}
	seq := NewSequence([]Primitive{a, b, c})

	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	state := handler.NewMultiGatewayConnectionState()

	err := seq.PortalStreamExecute(context.Background(), nil, conn, state, nil, 0, false, PlanExecInfo{}, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, failure)
	assert.Contains(t, err.Error(), "primitive 1 (recordingPrimitive)")

	assert.Equal(t, 1, a.portalCalls)
	assert.Equal(t, 1, b.portalCalls)
	assert.Equal(t, 0, c.portalCalls, "primitives after the failing one must not run")
}
