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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// buildBoundPortalInfo wires a *preparedstatement.PortalInfo for the given
// prepared SQL + binds via the same factories the real Bind path uses,
// keeping the test path byte-equivalent to a production portal.
func buildBoundPortalInfo(t *testing.T, sql string, paramTypes []uint32, params [][]byte, paramFormats []int16) *preparedstatement.PortalInfo {
	t.Helper()
	psi, err := preparedstatement.NewPreparedStatementInfo(protoutil.NewPreparedStatement("stmt", sql, paramTypes))
	require.NoError(t, err)
	portal := protoutil.NewPortal("", "stmt", params, paramFormats, nil)
	return preparedstatement.NewPortalInfo(psi, portal)
}

// syntheticSetForTest builds a VariableSetStmt placeholder matching what
// planner.syntheticSetStmt would emit when called with the given literal
// fallbacks. Bind-placeholder slots are intentionally distinct strings so
// a leaked placeholder is obvious if executeSetWithBinds forgot to
// override the slot.
func syntheticSetForTest(name, value string) *ast.VariableSetStmt {
	return &ast.VariableSetStmt{
		BaseNode: ast.BaseNode{Tag: ast.T_VariableSetStmt},
		Kind:     ast.VAR_SET_VALUE,
		Name:     name,
		Args:     ast.NewNodeList(ast.NewA_Const(ast.NewString(value), 0)),
	}
}

// runBindExecute executes the primitive's PortalStreamExecute against a
// fresh connection state and reports the resulting tracker map and the
// callback CommandTags it emitted. Returns (sessionSettings, tags, err).
func runBindExecute(t *testing.T, prim *ApplySessionState, portalInfo *preparedstatement.PortalInfo) (map[string]string, []string, error) {
	t.Helper()
	state := &handler.MultiGatewayConnectionState{}
	var tags []string
	err := prim.PortalStreamExecute(context.Background(), nil, nil, state, portalInfo, 0, false,
		func(_ context.Context, r *sqltypes.Result) error {
			tags = append(tags, r.CommandTag)
			return nil
		})
	return state.SessionSettings, tags, err
}

// TestApplySessionState_BoundValueResolves covers the Storage migration
// shape: name and is_local literal, value bound. The text decode hits the
// "TEXT OID, text format" branch — the byte-trivial case.
func TestApplySessionState_BoundValueResolves(t *testing.T) {
	const sql = "SELECT set_config('search_path', $1, false)"
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("public,extensions")}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("search_path", "__bind_$1__"),
		&BoundSetConfigRefs{
			ValueParam: &ast.ParamRef{Number: 1},
		})

	settings, tags, err := runBindExecute(t, prim, portalInfo)
	require.NoError(t, err)
	require.Nil(t, tags, "SilentTracking must suppress the SET CommandComplete; Route owns the response")
	assert.Equal(t, "public,extensions", settings["search_path"])
}

// TestApplySessionState_BoundNameResolves covers the symmetric case: name
// bound, value literal. Confirms the per-slot decode is independent.
func TestApplySessionState_BoundNameResolves(t *testing.T) {
	const sql = "SELECT set_config($1, 'public', false)"
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("search_path")}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("__bind_$1__", "public"),
		&BoundSetConfigRefs{
			NameParam: &ast.ParamRef{Number: 1},
		})

	settings, _, err := runBindExecute(t, prim, portalInfo)
	require.NoError(t, err)
	assert.Equal(t, "public", settings["search_path"])
}

// TestApplySessionState_BoundIsLocalTrueSkipsTracking pins the
// transaction-scoped semantics: when bound is_local resolves to true, the
// gateway must NOT update SessionSettings. PG handles SET LOCAL via the
// trailing Route; mirroring it in the tracker would outlive the
// transaction PG scoped the change to.
func TestApplySessionState_BoundIsLocalTrueSkipsTracking(t *testing.T) {
	const sql = "SELECT set_config('search_path', 'public', $1)"
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.BOOLOID)}, [][]byte{[]byte("true")}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("search_path", "public"),
		&BoundSetConfigRefs{
			IsLocalParam: &ast.ParamRef{Number: 1},
		})

	settings, tags, err := runBindExecute(t, prim, portalInfo)
	require.NoError(t, err)
	assert.Nil(t, tags)
	assert.Empty(t, settings, "is_local=true must leave SessionSettings untouched")
}

// TestApplySessionState_BoundIsLocalFalseTracksNormally pins the opposite
// resolution of the same bound shape: when is_local resolves false, the
// tracker write must fire. Same primitive, same binds shape; only the
// resolved bool changes — proves the conditional branch is value-driven.
func TestApplySessionState_BoundIsLocalFalseTracksNormally(t *testing.T) {
	const sql = "SELECT set_config('search_path', 'public', $1)"
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.BOOLOID)}, [][]byte{[]byte("false")}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("search_path", "public"),
		&BoundSetConfigRefs{
			IsLocalParam: &ast.ParamRef{Number: 1},
		})

	settings, _, err := runBindExecute(t, prim, portalInfo)
	require.NoError(t, err)
	assert.Equal(t, "public", settings["search_path"])
}

// TestApplySessionState_BoundAllThree exercises the full shape: name,
// value, and is_local all resolved from binds. Confirms the resolution
// order (is_local first, then name/value if tracking) doesn't drop
// information when every slot is deferred.
func TestApplySessionState_BoundAllThree(t *testing.T) {
	const sql = "SELECT set_config($1, $2, $3)"
	portalInfo := buildBoundPortalInfo(t, sql,
		[]uint32{uint32(ast.TEXTOID), uint32(ast.TEXTOID), uint32(ast.BOOLOID)},
		[][]byte{[]byte("search_path"), []byte("schema1, schema2"), []byte("false")},
		[]int16{0, 0, 0},
	)

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("__bind_$1__", "__bind_$2__"),
		&BoundSetConfigRefs{
			NameParam:    &ast.ParamRef{Number: 1},
			ValueParam:   &ast.ParamRef{Number: 2},
			IsLocalParam: &ast.ParamRef{Number: 3},
		})

	settings, _, err := runBindExecute(t, prim, portalInfo)
	require.NoError(t, err)
	assert.Equal(t, "schema1, schema2", settings["search_path"])
}

// TestApplySessionState_NullBindRejected — PG's set_config is STRICT,
// NULL input means no-op. If we silently tracked an empty string while PG
// did nothing, gateway tracker and PG state would diverge. Reject
// explicitly so the client sees the contract violation immediately.
func TestApplySessionState_NullBindRejected(t *testing.T) {
	const sql = "SELECT set_config('search_path', $1, false)"
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{nil}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("search_path", "__bind_$1__"),
		&BoundSetConfigRefs{
			ValueParam: &ast.ParamRef{Number: 1},
		})

	settings, _, err := runBindExecute(t, prim, portalInfo)
	require.Error(t, err)
	assertFeatureErrBind(t, err, "cannot be NULL")
	assert.Empty(t, settings, "tracker must not be updated on bind error")
}

// TestApplySessionState_UnsupportedOidRejected — gateway never invents
// type coercion. If the client declares the bound parameter as int4, the
// safe answer is "no" with a message that tells the client how to fix it.
func TestApplySessionState_UnsupportedOidRejected(t *testing.T) {
	const sql = "SELECT set_config('search_path', $1, false)"
	const oidInt4 uint32 = 23
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{oidInt4}, [][]byte{[]byte("123")}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("search_path", "__bind_$1__"),
		&BoundSetConfigRefs{
			ValueParam: &ast.ParamRef{Number: 1},
		})

	_, _, err := runBindExecute(t, prim, portalInfo)
	require.Error(t, err)
	assertFeatureErrBind(t, err, "unsupported type oid=23")
}

// TestApplySessionState_BinaryBool covers the wire-format binary bool: a
// single byte where 0 means false and non-zero means true. Mirrors PG's
// boolrecv.
func TestApplySessionState_BinaryBool(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
		want bool
	}{
		{"binary true", []byte{1}, true},
		{"binary false", []byte{0}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const sql = "SELECT set_config('search_path', 'public', $1)"
			portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.BOOLOID)}, [][]byte{tc.raw}, []int16{1})

			prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("search_path", "public"),
				&BoundSetConfigRefs{
					IsLocalParam: &ast.ParamRef{Number: 1},
				})

			settings, _, err := runBindExecute(t, prim, portalInfo)
			require.NoError(t, err)
			if tc.want {
				assert.Empty(t, settings, "is_local=true (binary 1) must skip tracker write")
			} else {
				assert.Equal(t, "public", settings["search_path"], "is_local=false (binary 0) must populate tracker")
			}
		})
	}
}

// TestApplySessionState_PlanCacheReuseAcrossBinds is the regression for
// the whole reason this is the deferred-resolution shape: the SAME
// primitive (same plan, same BindRefs) must produce different tracker
// writes for different portal binds. A baked-in literal would fail this
// — iteration N would always see iteration 1's value.
func TestApplySessionState_PlanCacheReuseAcrossBinds(t *testing.T) {
	const sql = "SELECT set_config('search_path', $1, false)"
	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("search_path", "__bind_$1__"),
		&BoundSetConfigRefs{
			ValueParam: &ast.ParamRef{Number: 1},
		})

	for _, want := range []string{"first", "second", "third"} {
		portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte(want)}, []int16{0})
		settings, _, err := runBindExecute(t, prim, portalInfo)
		require.NoError(t, err, "iteration %q", want)
		assert.Equal(t, want, settings["search_path"], "iteration %q must reflect that iteration's bind value", want)
	}
}

// TestApplySessionState_OriginalVariableStmtUnmodified pins that
// executeSetWithBinds does NOT mutate the synthetic VariableStmt. The
// primitive is shared across concurrent Executes on the same cached plan;
// a mutation would leak across executions.
func TestApplySessionState_OriginalVariableStmtUnmodified(t *testing.T) {
	const sql = "SELECT set_config('search_path', $1, false)"
	base := syntheticSetForTest("search_path", "__bind_$1__")
	prim := NewApplySessionStateFromBind(sql, base, &BoundSetConfigRefs{
		ValueParam: &ast.ParamRef{Number: 1},
	})
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("public")}, []int16{0})

	_, _, err := runBindExecute(t, prim, portalInfo)
	require.NoError(t, err)

	assert.Equal(t, "search_path", base.Name, "base VariableStmt.Name must not be mutated by execute-time resolution")
	require.NotNil(t, base.Args)
	require.Equal(t, 1, base.Args.Len())
	c, ok := base.Args.Items[0].(*ast.A_Const)
	require.True(t, ok)
	s, ok := c.Val.(*ast.String)
	require.True(t, ok)
	assert.Equal(t, "__bind_$1__", s.SVal, "base VariableStmt.Args[0] placeholder must not be overwritten")
}

// TestApplySessionState_OutOfRangeParamRef pins a defensive error path:
// a ParamRef whose number exceeds the portal's bind count is a malformed
// client request (or planner bug). Surface it explicitly rather than
// panicking on slice access.
func TestApplySessionState_OutOfRangeParamRef(t *testing.T) {
	const sql = "SELECT set_config('search_path', $2, false)"
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("public")}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("search_path", "__bind_$2__"),
		&BoundSetConfigRefs{
			ValueParam: &ast.ParamRef{Number: 2},
		})

	_, _, err := runBindExecute(t, prim, portalInfo)
	require.Error(t, err)
	assertFeatureErrBind(t, err, "but the portal carries 1 values")
}

// ---------- Normalized-binds (simple-protocol) resolution ----------
//
// The tests below cover StreamExecute's BindRefs path: the ParamRefs were
// minted by ast.Normalize (not by the client), so resolution reads the
// normalizer-extracted bindVars instead of a portal's wire Bind values.
// This is the path a cached `SELECT set_config('<gmv>', <value>, true)`
// simple query takes — the value collapses into the plan-cache key, so the
// primitive must re-resolve it on every execution.

// runNormalizedExecute executes the primitive's StreamExecute (simple-
// protocol path) against a fresh connection state with the given conn and
// normalizer-extracted bindVars.
func runNormalizedExecute(t *testing.T, prim *ApplySessionState, conn *server.Conn, bindVars []*ast.A_Const) (*handler.MultiGatewayConnectionState, []string, error) {
	t.Helper()
	state := &handler.MultiGatewayConnectionState{}
	state.InitStatementTimeout(30 * time.Second)
	var tags []string
	err := prim.StreamExecute(context.Background(), nil, conn, state, bindVars,
		func(_ context.Context, r *sqltypes.Result) error {
			tags = append(tags, r.CommandTag)
			return nil
		})
	return state, tags, err
}

// normalizedGMVLocalPrim builds the primitive planSelectStmt mints for
// `SELECT set_config('statement_timeout', <value>, true)` after the
// normalizer parameterized the value: synthetic stmt with IsLocal=true and
// a `__bind_$1__` placeholder, BindRefs carrying the ValueParam.
func normalizedGMVLocalPrim(sql string) *ApplySessionState {
	stmt := syntheticSetForTest("statement_timeout", "__bind_$1__")
	stmt.IsLocal = true
	return NewApplySessionStateFromBind(sql, stmt, &BoundSetConfigRefs{
		ValueParam: &ast.ParamRef{Number: 1},
	})
}

func txnConn(t *testing.T) *server.Conn {
	t.Helper()
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	return conn
}

// TestApplySessionState_NormalizedBindGMVLocalResolves — the gateway-managed
// transaction-local override must be applied with the value resolved from
// THIS execution's bindVars, not the `__bind_$1__` placeholder baked into
// the cached plan's synthetic VariableStmt.
func TestApplySessionState_NormalizedBindGMVLocalResolves(t *testing.T) {
	const sql = "SELECT set_config('statement_timeout', $1, true)"
	prim := normalizedGMVLocalPrim(sql)

	state, tags, err := runNormalizedExecute(t, prim, txnConn(t),
		[]*ast.A_Const{ast.NewA_Const(ast.NewString("250ms"), 0)})
	require.NoError(t, err)
	assert.Nil(t, tags, "SilentTracking must suppress the SET CommandComplete; Route owns the response")
	assert.Equal(t, 250*time.Millisecond, state.GetStatementTimeout())
	_, exists := state.GetSessionVariable("statement_timeout")
	assert.False(t, exists, "GMV must not land in SessionSettings")
}

// TestApplySessionState_NormalizedBindCacheReuseAcrossValues is the engine-
// level regression for the plan-cache staleness report: the SAME primitive
// (same cached plan, same BindRefs) must apply each execution's value. A
// baked-in literal or placeholder would fail every iteration after the first.
func TestApplySessionState_NormalizedBindCacheReuseAcrossValues(t *testing.T) {
	const sql = "SELECT set_config('statement_timeout', $1, true)"
	prim := normalizedGMVLocalPrim(sql)

	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"100ms", 100 * time.Millisecond},
		{"2s", 2 * time.Second},
		{"1m", time.Minute},
	} {
		state, _, err := runNormalizedExecute(t, prim, txnConn(t),
			[]*ast.A_Const{ast.NewA_Const(ast.NewString(tc.value), 0)})
		require.NoError(t, err, "iteration %q", tc.value)
		assert.Equal(t, tc.want, state.GetStatementTimeout(),
			"iteration %q must reflect that iteration's normalized literal", tc.value)
	}
}

// TestApplySessionState_NormalizedBindGMVLocalOutsideTxnIsNoOp — parity with
// the literal path: a transaction-local GMV override outside a transaction
// must not be applied (it would leak for the connection's lifetime; PG scopes
// it to the implicit single-statement transaction).
func TestApplySessionState_NormalizedBindGMVLocalOutsideTxnIsNoOp(t *testing.T) {
	const sql = "SELECT set_config('statement_timeout', $1, true)"
	prim := normalizedGMVLocalPrim(sql)

	idleConn := server.NewTestConn(&bytes.Buffer{}).Conn // idle: not in a transaction
	state, _, err := runNormalizedExecute(t, prim, idleConn,
		[]*ast.A_Const{ast.NewA_Const(ast.NewString("2s"), 0)})
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, state.GetStatementTimeout(), "local override must not leak outside a transaction")
}

// TestApplySessionState_NormalizedBindSessionValueResolves covers the
// is_local=false shape executed via the simple protocol (reachable through
// cross-protocol plan-cache sharing): the resolved value must land in
// SessionSettings under the literal name.
func TestApplySessionState_NormalizedBindSessionValueResolves(t *testing.T) {
	const sql = "SELECT set_config('search_path', $1, false)"
	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("search_path", "__bind_$1__"),
		&BoundSetConfigRefs{
			ValueParam: &ast.ParamRef{Number: 1},
		})

	state, tags, err := runNormalizedExecute(t, prim, server.NewTestConn(&bytes.Buffer{}).Conn,
		[]*ast.A_Const{ast.NewA_Const(ast.NewString("public,extensions"), 0)})
	require.NoError(t, err)
	assert.Nil(t, tags)
	got, ok := state.GetSessionVariable("search_path")
	require.True(t, ok)
	assert.Equal(t, "public,extensions", got)
}

// TestApplySessionState_NormalizedBindOutOfRangeErrors — a ParamRef pointing
// past the extracted literals (user-typed $N in a simple query) must error
// before any gateway state is written; the Sequence aborts before the Route.
func TestApplySessionState_NormalizedBindOutOfRangeErrors(t *testing.T) {
	const sql = "SELECT set_config('statement_timeout', $2, true)"
	stmt := syntheticSetForTest("statement_timeout", "__bind_$2__")
	stmt.IsLocal = true
	prim := NewApplySessionStateFromBind(sql, stmt, &BoundSetConfigRefs{
		ValueParam: &ast.ParamRef{Number: 2},
	})

	state, _, err := runNormalizedExecute(t, prim, txnConn(t),
		[]*ast.A_Const{ast.NewA_Const(ast.NewString("only-one"), 0)})
	require.Error(t, err)
	assertFeatureErrBind(t, err, "carries 1 normalized literal")
	assert.Equal(t, 30*time.Second, state.GetStatementTimeout(), "gateway state must not be updated on bind-resolution error")
}

// assertFeatureErrBind wraps the verbose unwrap-into-PgDiagnostic check.
// All bind-resolution errors are FeatureNotSupported, matching the
// planner's literal-rejection diagnostics so client-visible behavior is
// uniform across plan-time and execute-time errors.
func assertFeatureErrBind(t *testing.T, err error, contains string) {
	t.Helper()
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "expected *mterrors.PgDiagnostic, got %T", err)
	assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
	assert.Contains(t, diag.Message, contains)
}
