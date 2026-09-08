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
	state := &handler.MultigatewayConnectionState{}
	var tags []string
	err := prim.PortalStreamExecute(context.Background(), nil, nil, state, portalInfo, 0, false, PlanExecInfo{},
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

// TestApplySessionState_BoundSearchPathPgTempRejected pins the execute-time
// half of the pg_temp guard: a bound search_path value naming the temp
// namespace must error during bind resolution (aborting the Sequence before
// the paired Route reaches a backend) and leave the tracker untouched.
func TestApplySessionState_BoundSearchPathPgTempRejected(t *testing.T) {
	const sql = "SELECT set_config('search_path', $1, false)"
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("pg_temp, public")}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("search_path", "__bind_$1__"),
		&BoundSetConfigRefs{
			ValueParam: &ast.ParamRef{Number: 1},
		})

	settings, _, err := runBindExecute(t, prim, portalInfo)
	require.ErrorContains(t, err, "pg_temp")
	assert.Empty(t, settings, "rejected search_path must not reach SessionSettings")
}

// TestTrackedSetActionRejectsPgTempSearchPath pins the tracked-settings
// backstop: prepareTrackedSetActionWithBackendPreview is the funnel every
// tracked SET/set_config write passes through, so a pg_temp search_path must
// error there regardless of which resolver produced it.
func TestTrackedSetActionRejectsPgTempSearchPath(t *testing.T) {
	state := &handler.MultigatewayConnectionState{}
	_, err := prepareTrackedSetAction(nil, state, "search_path", "pg_temp, public", false)
	require.ErrorContains(t, err, "pg_temp")

	action, err := prepareTrackedSetAction(nil, state, "search_path", "public", false)
	require.NoError(t, err)
	action()
	assert.Equal(t, "public", state.SessionSettings["search_path"])
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

// TestApplySessionState_BoundNameGatewayManagedLocalApplied pins the MUL-1468
// fix: a parameter-bound name resolving to a gateway-managed variable with
// is_local=true (PostgREST's role-setting form, inside a transaction) is no
// longer rejected — the gateway applies it as a transaction-local override. The
// paired Route runs the real set_config on the backend transaction-locally
// (reverting, so no pool leak), while this primitive owns the value so SHOW and
// the gateway's own deadline enforcement stay correct. The value lives in
// gateway-managed state, never in SessionSettings.
func TestApplySessionState_BoundNameGatewayManagedLocalApplied(t *testing.T) {
	const sql = "SELECT set_config($1, $2, true)"
	portalInfo := buildBoundPortalInfo(t, sql,
		[]uint32{uint32(ast.TEXTOID), uint32(ast.TEXTOID)},
		[][]byte{[]byte("statement_timeout"), []byte("5s")}, []int16{0, 0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetLocalForTest("__bind_$1__", "__bind_$2__"),
		&BoundSetConfigRefs{
			NameParam:  &ast.ParamRef{Number: 1},
			ValueParam: &ast.ParamRef{Number: 2},
		})

	state := &handler.MultigatewayConnectionState{}
	state.InitStatementTimeout(30 * time.Second)
	var tags []string
	err := prim.PortalStreamExecute(context.Background(), nil, txnConn(t), state, portalInfo, 0, false, PlanExecInfo{},
		func(_ context.Context, r *sqltypes.Result) error {
			tags = append(tags, r.CommandTag)
			return nil
		})
	require.NoError(t, err)
	assert.Nil(t, tags, "SilentTracking must suppress the SET CommandComplete; the paired Route owns the response")
	assert.Equal(t, 5*time.Second, state.GetStatementTimeout(), "transaction-local GMV override must be applied to gateway state")
	_, exists := state.GetSessionVariable("statement_timeout")
	assert.False(t, exists, "a gateway-managed variable must never land in SessionSettings")
}

// TestApplySessionState_BoundNameGatewayManagedSessionRejected pins the
// deliberate limitation: a parameter-bound name resolving to a gateway-managed
// variable with is_local=false is rejected. On a pinned session the verbatim
// call would persist on the reserved backend and could drift from the gateway on
// a later change, and forcing the reverted route there needs plumbing we don't
// have yet — so fail closed (a literal name is the supported session-scoped
// spelling). PostgREST always uses is_local=true, so it is unaffected.
func TestApplySessionState_BoundNameGatewayManagedSessionRejected(t *testing.T) {
	const sql = "SELECT set_config($1, '5s', false)"
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("statement_timeout")}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("__bind_$1__", "5s"),
		&BoundSetConfigRefs{
			NameParam: &ast.ParamRef{Number: 1},
		})

	settings, tags, err := runBindExecute(t, prim, portalInfo)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only supported with is_local=true")
	assert.Empty(t, settings)
	assert.Nil(t, tags)
}

// syntheticSetLocalForTest is syntheticSetForTest with IsLocal set, matching
// what planner.syntheticSetStmt emits for a vet-only is_local=true call.
func syntheticSetLocalForTest(name, value string) *ast.VariableSetStmt {
	s := syntheticSetForTest(name, value)
	s.IsLocal = true
	return s
}

// TestApplySessionState_VetOnlyIsLocalTrue pins the vet-only disposition for
// the PostgREST hot path `set_config($1, $2, true)`: the resolved slots are
// vetted during the Sequence's prepare phase — a name resolving to
// search_path gets its value checked for pg_temp, a restricted GUC is
// rejected — and an accepted call tracks nothing (transaction-scoped, owned
// by PG via the paired Route).
func TestApplySessionState_VetOnlyIsLocalTrue(t *testing.T) {
	const sql = "SELECT set_config($1, $2, true)"
	newPrim := func() *ApplySessionState {
		return NewApplySessionStateFromBind(sql, syntheticSetLocalForTest("__bind_$1__", "__bind_$2__"),
			&BoundSetConfigRefs{
				NameParam:  &ast.ParamRef{Number: 1},
				ValueParam: &ast.ParamRef{Number: 2},
			})
	}
	textPair := func(name, value string) *preparedstatement.PortalInfo {
		return buildBoundPortalInfo(t, sql,
			[]uint32{uint32(ast.TEXTOID), uint32(ast.TEXTOID)},
			[][]byte{[]byte(name), []byte(value)}, []int16{0, 0})
	}

	t.Run("benign custom GUC passes untracked", func(t *testing.T) {
		settings, tags, err := runBindExecute(t, newPrim(), textPair("request.jwt.claims", `{"sub":"x"}`))
		require.NoError(t, err)
		assert.Nil(t, tags)
		assert.Empty(t, settings, "vet-only call must not touch SessionSettings")
	})

	t.Run("name resolving to search_path with pg_temp value rejected", func(t *testing.T) {
		settings, _, err := runBindExecute(t, newPrim(), textPair("search_path", "pg_temp, public"))
		require.ErrorContains(t, err, "pg_temp")
		assert.Empty(t, settings)
	})

	t.Run("name resolving to search_path with benign value passes untracked", func(t *testing.T) {
		settings, _, err := runBindExecute(t, newPrim(), textPair("search_path", "public"))
		require.NoError(t, err)
		assert.Empty(t, settings, "is_local=true search_path is PG-scoped, not tracked")
	})

	t.Run("name resolving to restricted GUC rejected", func(t *testing.T) {
		settings, _, err := runBindExecute(t, newPrim(), textPair("synchronous_commit", "off"))
		require.ErrorContains(t, err, "synchronous_commit")
		assert.Empty(t, settings)
	})
}

// TestApplySessionState_VetOnlyBoundSearchPathValue pins the narrower vet-only
// shape: literal search_path name, bound value, literal is_local=true.
func TestApplySessionState_VetOnlyBoundSearchPathValue(t *testing.T) {
	const sql = "SELECT set_config('search_path', $1, true)"
	newPrim := func() *ApplySessionState {
		return NewApplySessionStateFromBind(sql, syntheticSetLocalForTest("search_path", "__bind_$1__"),
			&BoundSetConfigRefs{
				ValueParam: &ast.ParamRef{Number: 1},
			})
	}

	t.Run("pg_temp value rejected", func(t *testing.T) {
		portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("pg_temp")}, []int16{0})
		settings, _, err := runBindExecute(t, newPrim(), portalInfo)
		require.ErrorContains(t, err, "pg_temp")
		assert.Empty(t, settings)
	})

	t.Run("benign value passes untracked", func(t *testing.T) {
		portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("tenant_a")}, []int16{0})
		settings, _, err := runBindExecute(t, newPrim(), portalInfo)
		require.NoError(t, err)
		assert.Empty(t, settings)
	})

	// A NULL value resets search_path to its server/admin default rather than
	// applying a client string, so the pg_temp vet has nothing to check and
	// must not reject the statement.
	t.Run("null value skips the vet and passes untracked", func(t *testing.T) {
		portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{nil}, []int16{0})
		settings, _, err := runBindExecute(t, newPrim(), portalInfo)
		require.NoError(t, err)
		assert.Empty(t, settings)
	})
}

// TestApplySessionState_BoundNameRestrictedGUCRejected pins the execute-time
// restricted-GUC re-check on the tracked (is_local=false) path too: the
// plan-time guard only sees literal names.
func TestApplySessionState_BoundNameRestrictedGUCRejected(t *testing.T) {
	const sql = "SELECT set_config($1, 'off', false)"
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("synchronous_commit")}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("__bind_$1__", "off"),
		&BoundSetConfigRefs{
			NameParam: &ast.ParamRef{Number: 1},
		})

	settings, tags, err := runBindExecute(t, prim, portalInfo)
	require.ErrorContains(t, err, "synchronous_commit")
	assert.Empty(t, settings)
	assert.Nil(t, tags)
}

// TestApplySessionState_BoundIsLocalTrueSkipsTracking pins the
// transaction-scoped semantics: when bound is_local resolves to true, the
// gateway must NOT update SessionSettings. PG handles SET LOCAL via the
// paired Route; mirroring it in the tracker would outlive the transaction PG
// scoped the change to.
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

// TestApplySessionState_NullBindResetsTracking pins PostgreSQL's actual
// set_config NULL semantics. set_config is NOT strict (pg_proc.proisstrict =
// false): set_config(name, NULL, false) clears the parameter and returns the
// restored default — verified against PostgreSQL 17, where it is
// indistinguishable from RESET. So a NULL bind must REMOVE the tracked entry,
// not error: erroring diverges from PG, and tracking an empty string would
// make pool replay assert a value PostgreSQL never set.
func TestApplySessionState_NullBindResetsTracking(t *testing.T) {
	const sql = "SELECT set_config('search_path', $1, false)"
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{nil}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("search_path", "__bind_$1__"),
		&BoundSetConfigRefs{
			ValueParam: &ast.ParamRef{Number: 1},
		})

	state := &handler.MultigatewayConnectionState{}
	state.SetSessionVariable("search_path", "stale_value")
	err := prim.PortalStreamExecute(context.Background(), nil, nil, state, portalInfo, 0, false, PlanExecInfo{},
		func(context.Context, *sqltypes.Result) error { return nil })
	require.NoError(t, err, "a NULL value is a reset, not an error")
	assert.NotContains(t, state.SessionSettings, "search_path",
		"a NULL value must drop the tracked entry so pool replay stops asserting the stale value")
}

// TestApplySessionState_NullBindOnGatewayManagedRejected pins the deliberate
// carve-out: the gateway owns a gateway-managed variable's value and has no
// per-variable reset primitive for it, so a NULL stays fail-closed rather than
// leaving gateway state guessing.
func TestApplySessionState_NullBindOnGatewayManagedRejected(t *testing.T) {
	const sql = "SELECT set_config($1, $2, false)"
	portalInfo := buildBoundPortalInfo(t, sql,
		[]uint32{uint32(ast.TEXTOID), uint32(ast.TEXTOID)},
		[][]byte{[]byte("statement_timeout"), nil}, []int16{0, 0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("__bind_$1__", "__bind_$2__"),
		&BoundSetConfigRefs{
			NameParam:  &ast.ParamRef{Number: 1},
			ValueParam: &ast.ParamRef{Number: 2},
		})

	settings, _, err := runBindExecute(t, prim, portalInfo)
	require.Error(t, err)
	assert.Empty(t, settings)
}

// TestApplySessionState_NullNameRejected — PostgreSQL rejects a NULL name too
// ("SET requires parameter name"), so this is a rejection either way.
func TestApplySessionState_NullNameRejected(t *testing.T) {
	const sql = "SELECT set_config($1, 'public', false)"
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{uint32(ast.TEXTOID)}, [][]byte{nil}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetForTest("__bind_$1__", "public"),
		&BoundSetConfigRefs{
			NameParam: &ast.ParamRef{Number: 1},
		})

	settings, _, err := runBindExecute(t, prim, portalInfo)
	require.Error(t, err)
	assertFeatureErrBind(t, err, "cannot be NULL")
	assert.Empty(t, settings)
}

// TestApplySessionState_UnknownOidIsDecodedAndVetted pins the PostgREST shape:
// an untyped bound parameter arrives as OID 705 (unknown), which PostgreSQL
// coerces to text natively. Refusing it broke real clients — the vet-only
// disposition routes set_config('search_path', $1, true) into the decoder, so
// an unknown-typed value failed the statement outright.
//
// Accepting it is the safe direction, and this test pins that: the value is
// DECODED AND VETTED, not waved through. pg_temp in any position must still be
// rejected; only a benign value passes.
func TestApplySessionState_UnknownOidIsDecodedAndVetted(t *testing.T) {
	const sql = "SELECT set_config('search_path', $1, true)"
	run := func(t *testing.T, value string) error {
		t.Helper()
		prim := NewApplySessionStateFromBind(sql, syntheticSetLocalForTest("search_path", "__bind_$1__"),
			&BoundSetConfigRefs{ValueParam: &ast.ParamRef{Number: 1}})
		portalInfo := buildBoundPortalInfo(t, sql,
			[]uint32{uint32(ast.UNKNOWNOID)}, [][]byte{[]byte(value)}, []int16{0})
		_, _, err := runBindExecute(t, prim, portalInfo)
		return err
	}

	t.Run("benign value is accepted", func(t *testing.T) {
		require.NoError(t, run(t, "public, extensions"))
	})
	t.Run("pg_temp is still rejected", func(t *testing.T) {
		require.ErrorContains(t, run(t, "pg_temp"), "pg_temp")
	})
	t.Run("trailing pg_temp is still rejected", func(t *testing.T) {
		require.ErrorContains(t, run(t, "nosuch, pg_temp"), "pg_temp")
	})
}

// TestApplySessionState_UnsupportedOidStaysFailClosed guards the security
// property behind the OID restriction: the declared parameter OID is
// CLIENT-controlled, so "cannot decode it, let PostgreSQL handle it" would let
// a client bind a policy-relevant argument under an exotic-but-coercible OID
// (NAMEOID here) to skip the gateway's guards while PostgreSQL coerces and
// applies it — reopening the pg_temp bypass. The statement must be refused,
// never passed through unvetted.
func TestApplySessionState_UnsupportedOidStaysFailClosed(t *testing.T) {
	const sql = "SELECT set_config('search_path', $1, true)"
	const oidName uint32 = 19 // NAMEOID — PostgreSQL would happily coerce name -> text
	portalInfo := buildBoundPortalInfo(t, sql, []uint32{oidName}, [][]byte{[]byte("pg_temp")}, []int16{0})

	prim := NewApplySessionStateFromBind(sql, syntheticSetLocalForTest("search_path", "__bind_$1__"),
		&BoundSetConfigRefs{
			ValueParam: &ast.ParamRef{Number: 1},
		})

	settings, _, err := runBindExecute(t, prim, portalInfo)
	require.Error(t, err, "an undecodable OID must abort, never fall through to PostgreSQL unvetted")
	assertFeatureErrBind(t, err, "unsupported type oid=19")
	assert.Empty(t, settings)
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
func runNormalizedExecute(t *testing.T, prim *ApplySessionState, conn *server.Conn, bindVars []*ast.A_Const) (*handler.MultigatewayConnectionState, []string, error) {
	t.Helper()
	state := &handler.MultigatewayConnectionState{}
	state.InitStatementTimeout(30 * time.Second)
	var tags []string
	err := prim.StreamExecute(context.Background(), nil, conn, state, bindVars, PlanExecInfo{},
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
		{"1min", time.Minute},
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
