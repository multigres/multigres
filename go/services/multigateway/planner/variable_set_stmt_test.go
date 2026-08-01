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

package planner

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

func requireRouteThenTrackReset(t *testing.T, plan *engine.Plan) *engine.Sequence {
	t.Helper()
	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 2)
	_, ok = seq.Primitives[0].(*engine.Route)
	require.True(t, ok, "pinned RESET must route through PostgreSQL, got %T", seq.Primitives[0])
	return seq
}

// requireProbeThenTrackReset asserts the unpinned RESET shape: a
// ValidateSetting reset probe (validates the name, reverts instantly) followed
// by a non-silent ApplySessionState that drops the map entry and emits
// CommandComplete("RESET"). No backend session state is touched.
func requireProbeThenTrackReset(t *testing.T, plan *engine.Plan) *engine.Sequence {
	t.Helper()
	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 2)
	probe, ok := seq.Primitives[0].(*engine.ValidateSetting)
	require.True(t, ok, "unpinned RESET must probe-validate the name, got %T", seq.Primitives[0])
	require.True(t, probe.IsReset, "the probe must be a reset probe (set_config(name, NULL, true))")
	track, ok := seq.Primitives[1].(*engine.ApplySessionState)
	require.True(t, ok, "second primitive should track and emit RESET, got %T", seq.Primitives[1])
	require.False(t, track.SilentTracking, "unpinned RESET has no Route sibling; the tracker must emit the tag")
	return seq
}

func TestPlanVariableSetStmt_SET(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "work_mem",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "256MB"}}}},
	}

	plan, err := p.planVariableSetStmt("SET work_mem = '256MB'", stmt, testConn.Conn, nil)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// SET var = value is validated on a backend then tracked locally, so the
	// plan is Sequence[ValidateSetting, ApplySessionState].
	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 2, "expected [ValidateSetting, ApplySessionState]")
	_, ok = seq.Primitives[0].(*engine.ValidateSetting)
	assert.True(t, ok, "first primitive should be ValidateSetting (validate on backend), got %T", seq.Primitives[0])
	_, ok = seq.Primitives[1].(*engine.ApplySessionState)
	assert.True(t, ok, "second primitive should be ApplySessionState (track + emit SET), got %T", seq.Primitives[1])
}

func TestPlanVariableSetStmt_SET_InTransactionRoutesThenTracks(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})
	testConn.Conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state := handler.NewMultigatewayConnectionState()
	state.PendingBeginQuery = "BEGIN"

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "work_mem",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "256MB"}}}},
	}

	plan, err := p.planVariableSetStmt("SET work_mem = '256MB'", stmt, testConn.Conn, state)
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 2, "expected [Route, silent ApplySessionState]")
	_, ok = seq.Primitives[0].(*engine.Route)
	assert.True(t, ok, "first primitive should route the real SET, got %T", seq.Primitives[0])
	track, ok := seq.Primitives[1].(*engine.ApplySessionState)
	require.True(t, ok, "second primitive should track after success, got %T", seq.Primitives[1])
	assert.True(t, track.SilentTracking)
}

// TestPlanVariableSetStmt_SET_OnReservedSessionRoutesThenTracks pins that a
// session holding a reserved connection (temp table, cursor, advisory lock)
// routes the real SET to that backend the same way an in-transaction SET does,
// so the pinned backend genuinely carries the value in lockstep with the
// gateway map.
func TestPlanVariableSetStmt_SET_OnReservedSessionRoutesThenTracks(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})
	// Not in a transaction, but the session owns a reserved connection on the
	// target this statement routes to.
	state := handler.NewMultigatewayConnectionState()
	state.SetReservedConnection(protoutil.NewTarget("", "default", constants.DefaultShard, query.Mode_MODE_UNSPECIFIED),
		&query.ReservedState{ReservedConnectionId: 7})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "work_mem",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "256MB"}}}},
	}

	plan, err := p.planVariableSetStmt("SET work_mem = '256MB'", stmt, testConn.Conn, state)
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 2, "expected [Route, silent ApplySessionState]")
	_, ok = seq.Primitives[0].(*engine.Route)
	assert.True(t, ok, "first primitive should route the real SET, got %T", seq.Primitives[0])
	track, ok := seq.Primitives[1].(*engine.ApplySessionState)
	require.True(t, ok, "second primitive should track after success, got %T", seq.Primitives[1])
	assert.True(t, track.SilentTracking)
}

func TestPlanVariableSetStmt_RESET_RoleAuth_InTransactionRoutesThenTracks(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))

	tests := []struct {
		name string
		sql  string
		stmt *ast.VariableSetStmt
	}{
		{
			name: "RESET ROLE",
			sql:  "RESET ROLE",
			stmt: &ast.VariableSetStmt{Kind: ast.VAR_RESET, Name: "role"},
		},
		{
			name: "RESET SESSION AUTHORIZATION",
			sql:  "RESET SESSION AUTHORIZATION",
			stmt: &ast.VariableSetStmt{Kind: ast.VAR_RESET, Name: "session_authorization"},
		},
		{
			name: "SET ROLE TO DEFAULT",
			sql:  "SET ROLE TO DEFAULT",
			stmt: &ast.VariableSetStmt{Kind: ast.VAR_SET_DEFAULT, Name: "role"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPlanner("default", logger, nil)
			testConn := server.NewTestConn(&bytes.Buffer{})
			testConn.Conn.SetTxnStatus(protocol.TxnStatusInBlock)

			plan, err := p.planVariableSetStmt(tt.sql, tt.stmt, testConn.Conn, nil)
			require.NoError(t, err)
			require.NotNil(t, plan)

			// Inside a transaction, a backend is already pinned for its duration.
			// If an earlier `SET LOCAL ROLE`/`SET LOCAL SESSION AUTHORIZATION` on
			// this same connection changed the backend's real role (SET LOCAL
			// passes straight through untracked — see the IsLocal branch), only
			// routing the real RESET to that same backend can undo it. Gateway-only
			// tracking has nothing to clear and leaves the backend's real role
			// unchanged for the rest of the transaction — the bug this test guards.
			seq := requireRouteThenTrackReset(t, plan)
			track, ok := seq.Primitives[1].(*engine.ApplySessionState)
			require.True(t, ok, "second primitive should track after success, got %T", seq.Primitives[1])
			assert.True(t, track.SilentTracking)
		})
	}
}

func TestPlanVariableSetStmt_RESET_RoleAuth_OutsideTransactionStaysLocal(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})
	// No SetTxnStatus call: testConn defaults to no active transaction.

	stmt := &ast.VariableSetStmt{Kind: ast.VAR_RESET, Name: "role"}

	plan, err := p.planVariableSetStmt("RESET ROLE", stmt, testConn.Conn, nil)
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq := requireProbeThenTrackReset(t, plan)
	_, ok := seq.Primitives[1].(*engine.ApplySessionState)
	assert.True(t, ok, "RESET ROLE should track after the probe, got %T", seq.Primitives[1])
}

func TestPlanVariableSetStmt_SET_IdleSessionTimeoutGatewayManaged(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "idle_session_timeout",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "58s"}}}},
	}

	plan, err := p.planVariableSetStmt("SET idle_session_timeout = '58s'", stmt, testConn.Conn, nil)
	require.NoError(t, err)
	require.NotNil(t, plan)
	_, ok := plan.Primitive.(*engine.GatewaySessionState)
	assert.True(t, ok, "idle_session_timeout should be handled by the gateway, got %T", plan.Primitive)
}

func TestPlanVariableSetStmt_SET_IdleSessionTimeoutInvalidErrors(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "idle_session_timeout",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "not-a-duration"}}}},
	}

	plan, err := p.planVariableSetStmt("SET idle_session_timeout = 'not-a-duration'", stmt, testConn.Conn, nil)
	require.Error(t, err)
	assert.Nil(t, plan)
}

func TestPlanVariableSetStmt_RESET(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_RESET,
		Name: "work_mem",
	}

	plan, err := p.planVariableSetStmt("RESET work_mem", stmt, testConn.Conn, nil)
	require.NoError(t, err)
	require.NotNil(t, plan)

	requireProbeThenTrackReset(t, plan)
}

func TestPlanVariableSetStmt_TransactionOnlyVariablesPassThrough(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	tests := []struct {
		name string
		sql  string
	}{
		{name: "RESET transaction_isolation", sql: "RESET transaction_isolation"},
		{name: "RESET transaction_read_only", sql: "RESET transaction_read_only"},
		{name: "RESET transaction_deferrable", sql: "RESET transaction_deferrable"},
		{name: "SET TRANSACTION SNAPSHOT", sql: "SET TRANSACTION SNAPSHOT 'FFF-FFF-F'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := planPortal(t, p, testConn.Conn, tt.sql)
			require.NoError(t, err)
			require.NotNil(t, plan)
			_, ok := plan.Primitive.(*engine.Route)
			assert.True(t, ok, "transaction-only variable must route to PostgreSQL, got %T", plan.Primitive)
		})
	}
}

func TestPlanVariableSetStmt_RESET_ALL(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_RESET_ALL,
	}

	plan, err := p.planVariableSetStmt("RESET ALL", stmt, testConn.Conn, nil)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// Unpinned RESET ALL cannot fail and touches no backend: a single
	// non-silent ApplySessionState edits the map and emits the tag.
	track, ok := plan.Primitive.(*engine.ApplySessionState)
	require.True(t, ok, "unpinned RESET ALL should be a gateway-only ApplySessionState, got %T", plan.Primitive)
	assert.False(t, track.SilentTracking)
}

func TestPlanVariableSetStmt_SET_LOCAL_PassesThrough(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind:    ast.VAR_SET_VALUE,
		Name:    "work_mem",
		IsLocal: true,
		Args:    &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "256MB"}}}},
	}

	plan, err := p.planVariableSetStmt("SET LOCAL work_mem = '256MB'", stmt, testConn.Conn, nil)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// SET LOCAL should produce a plain Route, not ApplySessionState
	_, ok := plan.Primitive.(*engine.ApplySessionState)
	assert.False(t, ok, "SET LOCAL should not produce ApplySessionState")
}

func TestPlanVariableSetStmt_SET_DEFAULT_TreatedAsReset(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_DEFAULT,
		Name: "work_mem",
	}

	plan, err := p.planVariableSetStmt("SET work_mem TO DEFAULT", stmt, testConn.Conn, nil)
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq := requireProbeThenTrackReset(t, plan)
	track, ok := seq.Primitives[1].(*engine.ApplySessionState)
	require.True(t, ok)
	// The tracker receives the pre-normalization kind so the client gets the
	// "SET" tag PostgreSQL returns for SET ... TO DEFAULT; the reset probe and
	// tracking behavior are unchanged.
	assert.Equal(t, ast.VAR_SET_DEFAULT, track.VariableStmt.Kind)
}

func TestPlanVariableSetStmt_SET_TIME_ZONE_DEFAULT_TreatedAsReset(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_DEFAULT,
		Name: "timezone",
	}

	plan, err := p.planVariableSetStmt("SET TIME ZONE DEFAULT", stmt, testConn.Conn, nil)
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq := requireProbeThenTrackReset(t, plan)
	track, ok := seq.Primitives[1].(*engine.ApplySessionState)
	require.True(t, ok, "expected ApplySessionState second")
	assert.Equal(t, ast.VAR_SET_DEFAULT, track.VariableStmt.Kind)
	assert.Equal(t, "timezone", track.VariableStmt.Name)
}

func TestPlanVariableSetStmt_SET_MULTI_PassesThrough(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_MULTI,
		Name: "TRANSACTION",
	}

	plan, err := p.planVariableSetStmt("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", stmt, testConn.Conn, nil)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// SET TRANSACTION should pass through to PG (Route), not be handled locally
	_, ok := plan.Primitive.(*engine.ApplySessionState)
	assert.False(t, ok, "SET TRANSACTION should not produce ApplySessionState")
}

func TestPlanVariableSetStmt_SET_CURRENT_Rejected(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_CURRENT,
		Name: "search_path",
	}

	// SET var FROM CURRENT resolves its value inside a backend; the gateway
	// cannot track the resulting session state, so it is rejected fail-closed.
	plan, err := p.planVariableSetStmt("SET search_path FROM CURRENT", stmt, testConn.Conn, nil)
	require.Error(t, err)
	assert.Nil(t, plan)
}

// TestPlanVariableSetStmt_SessionCharacteristicsTranslated pins that SET
// SESSION CHARACTERISTICS AS TRANSACTION <mode> is tracked as the
// default_transaction_* GUC it really sets, instead of mutating a pooled
// backend untracked.
func TestPlanVariableSetStmt_SessionCharacteristicsTranslated(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_MULTI,
		Name: "SESSION CHARACTERISTICS AS TRANSACTION",
		Args: &ast.NodeList{Items: []ast.Node{
			ast.NewDefElem("transaction_isolation", ast.NewString("serializable")),
		}},
	}

	plan, err := p.planVariableSetStmt(
		"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		stmt, testConn.Conn, nil)
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 2)
	probe, ok := seq.Primitives[0].(*engine.ValidateSetting)
	require.True(t, ok, "unpinned translated SET should probe-validate, got %T", seq.Primitives[0])
	assert.Equal(t, "default_transaction_isolation", probe.Name)
	assert.Equal(t, "serializable", probe.Value)
	track, ok := seq.Primitives[1].(*engine.ApplySessionState)
	require.True(t, ok)
	assert.Equal(t, "default_transaction_isolation", track.VariableStmt.Name)
}

// TestPlanPortal_SET pins that the extended-protocol path plans SET/RESET the
// same way the simple protocol does: plain SET validates + tracks (Sequence),
// RESET chooses its path from runtime reservation state, and SET LOCAL /
// SET TRANSACTION route as a plain Route
// (which reissues the portal to the authoritative backend). Producing a Sequence
// for a plain SET — rather than a bare Route — is what keeps a raw SET from
// mutating a pooled backend outside multipooler's tracking and skipping
// pool-rotation replay.
func TestPlanPortal_SET(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	t.Run("plain SET is planned (validate + track)", func(t *testing.T) {
		plan, err := planPortal(t, p, testConn.Conn, "SET work_mem = '256MB'")
		require.NoError(t, err)
		require.NotNil(t, plan, "non-gateway SET must be planned, not forwarded raw to a pooled backend")
		seq, ok := plan.Primitive.(*engine.Sequence)
		require.True(t, ok, "expected Sequence, got %T", plan.Primitive)
		require.Len(t, seq.Primitives, 2)
		_, ok = seq.Primitives[0].(*engine.ValidateSetting)
		assert.True(t, ok, "first primitive should be ValidateSetting, got %T", seq.Primitives[0])
	})

	t.Run("RESET is planned", func(t *testing.T) {
		plan, err := planPortal(t, p, testConn.Conn, "RESET work_mem")
		require.NoError(t, err)
		require.NotNil(t, plan)
		requireProbeThenTrackReset(t, plan)
	})

	t.Run("SET LOCAL routes to PG", func(t *testing.T) {
		plan, err := planPortal(t, p, testConn.Conn, "SET LOCAL work_mem = '256MB'")
		require.NoError(t, err)
		require.NotNil(t, plan)
		_, ok := plan.Primitive.(*engine.Route)
		assert.True(t, ok, "SET LOCAL must route as a plain Route to the authoritative backend, got %T", plan.Primitive)
	})

	t.Run("SET TRANSACTION routes to PG", func(t *testing.T) {
		plan, err := planPortal(t, p, testConn.Conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
		require.NoError(t, err)
		require.NotNil(t, plan)
		_, ok := plan.Primitive.(*engine.Route)
		assert.True(t, ok, "SET TRANSACTION must route as a plain Route to the backend, got %T", plan.Primitive)
	})
}

// TestPlanVariableSetStmt_SET_ReservationOnOtherShardIsNotPinned pins the
// target scoping: ScatterConn reuses a reservation only when the shard state
// matches the statement's target, so a reservation on a DIFFERENT shard must
// not make this statement plan as pinned — routing a real SET while the
// executor falls through to a pooled connection would mutate that backend
// untracked.
func TestPlanVariableSetStmt_SET_ReservationOnOtherShardIsNotPinned(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := handler.NewMultigatewayConnectionState()
	state.SetReservedConnection(protoutil.NewTarget("", "other_tablegroup", "0-inf", query.Mode_MODE_UNSPECIFIED),
		&query.ReservedState{ReservedConnectionId: 7})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "work_mem",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "256MB"}}}},
	}

	plan, err := p.planVariableSetStmt("SET work_mem = '256MB'", stmt, testConn.Conn, state)
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)
	_, isProbe := seq.Primitives[0].(*engine.ValidateSetting)
	assert.True(t, isProbe,
		"a reservation on another shard must not pin this statement's target, got %T", seq.Primitives[0])
}

func newPinnedPlannerState(t *testing.T, startup map[string]string) (*Planner, *server.TestConn, *handler.MultigatewayConnectionState) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := handler.NewMultigatewayConnectionState()
	state.StartupParams = startup
	state.SetReservedConnection(protoutil.NewTarget("", "default", constants.DefaultShard, query.Mode_MODE_UNSPECIFIED),
		&query.ReservedState{ReservedConnectionId: 7})
	return p, testConn, state
}

// TestPlanVariableSetStmt_PinnedResetWithStartupFallback pins the divergence
// fix: on a pinned session, RESET of a GUC the client set in its startup
// packet must not route the raw RESET (the pooled backend would revert to the
// server default, while real PostgreSQL — and the gateway map — restore the
// startup value). It routes a synthesized SET of the startup value with
// swallowed output, and the client-visible RESET tag comes from the tracker.
func TestPlanVariableSetStmt_PinnedResetWithStartupFallback(t *testing.T) {
	p, testConn, state := newPinnedPlannerState(t, map[string]string{"search_path": "app_schema, o'brien"})

	stmt := &ast.VariableSetStmt{Kind: ast.VAR_RESET, Name: "search_path"}
	plan, err := p.planVariableSetStmt("RESET search_path", stmt, testConn.Conn, state)
	require.NoError(t, err)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 2)

	restore, ok := seq.Primitives[0].(*engine.SilentRoute)
	require.True(t, ok, "expected SilentRoute restore, got %T", seq.Primitives[0])
	assert.Equal(t, "SET search_path = 'app_schema, o''brien'", restore.GetQuery(),
		"restore must carry the startup value with quote escaping")

	track, ok := seq.Primitives[1].(*engine.ApplySessionState)
	require.True(t, ok, "expected ApplySessionState, got %T", seq.Primitives[1])
	assert.False(t, track.SilentTracking, "the tracker emits the client-visible RESET tag")
}

// TestPlanVariableSetStmt_PinnedResetWithoutStartupFallback pins that a GUC
// absent from the startup packet keeps the raw routed RESET: server default
// on the backend and an absent map entry are already consistent.
func TestPlanVariableSetStmt_PinnedResetWithoutStartupFallback(t *testing.T) {
	p, testConn, state := newPinnedPlannerState(t, map[string]string{"application_name": "probe"})

	stmt := &ast.VariableSetStmt{Kind: ast.VAR_RESET, Name: "work_mem"}
	plan, err := p.planVariableSetStmt("RESET work_mem", stmt, testConn.Conn, state)
	require.NoError(t, err)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence, got %T", plan.Primitive)
	_, isRoute := seq.Primitives[0].(*engine.Route)
	assert.True(t, isRoute, "no startup fallback keeps the raw routed RESET, got %T", seq.Primitives[0])
}

// TestPlanVariableSetStmt_PinnedResetAllRestoresStartupParams pins the
// reconciliation shape: raw RESET ALL first (its tag reaches the client),
// then deterministic silent restores of every startup param except the
// GUC_NO_RESET_ALL pair the backend itself preserves.
func TestPlanVariableSetStmt_PinnedResetAllRestoresStartupParams(t *testing.T) {
	p, testConn, state := newPinnedPlannerState(t, map[string]string{
		"search_path":           "app_schema",
		"application_name":      "probe",
		"session_authorization": "someone",
		"role":                  "someone_else",
	})

	stmt := &ast.VariableSetStmt{Kind: ast.VAR_RESET_ALL}
	plan, err := p.planVariableSetStmt("RESET ALL", stmt, testConn.Conn, state)
	require.NoError(t, err)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 4, "Route + 2 restores + silent track; role/auth must be skipped")

	_, isRoute := seq.Primitives[0].(*engine.Route)
	assert.True(t, isRoute, "raw RESET ALL routes first so its tag reaches the client")
	r1, ok := seq.Primitives[1].(*engine.SilentRoute)
	require.True(t, ok)
	r2, ok := seq.Primitives[2].(*engine.SilentRoute)
	require.True(t, ok)
	assert.Equal(t, "SET application_name = 'probe'", r1.GetQuery(), "restores are name-sorted")
	assert.Equal(t, "SET search_path = 'app_schema'", r2.GetQuery())
	track, ok := seq.Primitives[3].(*engine.ApplySessionState)
	require.True(t, ok)
	assert.True(t, track.SilentTracking)
}

// TestPlanVariableSetStmt_PinnedSetDefaultKeepsSetTag pins that SET var TO
// DEFAULT — normalized to RESET for planning — takes the startup-restore path
// but hands the tracker the ORIGINAL statement kind, so the client still gets
// the "SET" tag real PostgreSQL returns for it.
func TestPlanVariableSetStmt_PinnedSetDefaultKeepsSetTag(t *testing.T) {
	p, testConn, state := newPinnedPlannerState(t, map[string]string{"search_path": "app_schema"})

	stmt := &ast.VariableSetStmt{Kind: ast.VAR_SET_DEFAULT, Name: "search_path"}
	plan, err := p.planVariableSetStmt("SET search_path TO DEFAULT", stmt, testConn.Conn, state)
	require.NoError(t, err)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 2)
	_, isSilentRoute := seq.Primitives[0].(*engine.SilentRoute)
	require.True(t, isSilentRoute, "startup fallback applies to SET TO DEFAULT too, got %T", seq.Primitives[0])
	track, ok := seq.Primitives[1].(*engine.ApplySessionState)
	require.True(t, ok)
	assert.Equal(t, ast.VAR_SET_DEFAULT, track.VariableStmt.Kind,
		"tracker must see the original kind so executeSetDefault emits the SET tag")
}

// TestStartupRestoreStatement_UnsafeNameNeverBuildsSQL pins the injection
// guard: a startup-packet key that is not a plain/dotted GUC identifier must
// never be spliced into synthesized SQL — the raw RESET shape is used instead.
func TestStartupRestoreStatement_UnsafeNameNeverBuildsSQL(t *testing.T) {
	_, ok := startupRestoreStatement(map[string]string{"bad name; DROP TABLE x": "v"}, "bad name; DROP TABLE x")
	assert.False(t, ok)
	assert.Empty(t, startupRestoreStatements(map[string]string{"bad name; DROP TABLE x": "v"}))
}

// TestPlanVariableSetStmt_UnpinnedSetDefaultKeepsSetTag pins the tag parity:
// SET x TO DEFAULT is normalized to RESET for planning, but the tracker must
// see the original kind so executeSetDefault emits the "SET" tag PostgreSQL
// returns — on the unpinned path just like the pinned ones.
func TestPlanVariableSetStmt_UnpinnedSetDefaultKeepsSetTag(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := handler.NewMultigatewayConnectionState()

	stmt := &ast.VariableSetStmt{Kind: ast.VAR_SET_DEFAULT, Name: "work_mem"}
	plan, err := p.planVariableSetStmt("SET work_mem TO DEFAULT", stmt, testConn.Conn, state)
	require.NoError(t, err)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 2)
	_, isProbe := seq.Primitives[0].(*engine.ValidateSetting)
	require.True(t, isProbe, "unpinned SET TO DEFAULT keeps the reset probe, got %T", seq.Primitives[0])
	track, ok := seq.Primitives[1].(*engine.ApplySessionState)
	require.True(t, ok)
	assert.Equal(t, ast.VAR_SET_DEFAULT, track.VariableStmt.Kind,
		"tracker must see the original kind so the client gets the SET tag")
}

// TestStartupRestoreStatement_ScsIndependentQuoting pins the canonical
// quoting: a value containing a backslash must render as an E'...' escape
// string with the backslash doubled, so it parses identically whatever
// standard_conforming_strings the client put in its startup packet — plain
// quote-doubling would let a trailing backslash consume the closing quote
// under scs=off.
func TestStartupRestoreStatement_ScsIndependentQuoting(t *testing.T) {
	sql, ok := startupRestoreStatement(map[string]string{"search_path": `evil\`}, "search_path")
	require.True(t, ok)
	assert.Equal(t, `SET search_path = E'evil\\'`, sql)

	restores := startupRestoreStatements(map[string]string{"application_name": `a\'b`})
	require.Len(t, restores, 1)
	assert.Equal(t, `SET application_name = E'a\\''b'`, restores[0])
}
