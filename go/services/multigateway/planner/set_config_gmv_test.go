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

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// TestRewriteGatewayManagedSetConfig_ExpressionValueFailsClosed pins the
// defensive branch: a gateway-managed set_config whose value is neither a literal
// nor a bound param can't be canonicalized or rewritten out. The analyzer normally
// rejects it (mixed → setConfigArgError) or reroutes it (all-set_config →
// DynamicSetConfig) before planning, so this is unreachable via p.Plan — we call
// the rewrite directly to prove that if it ever *were* reached, it fails closed
// rather than leaving the call for the backend (which would leak the real GUC).
func TestRewriteGatewayManagedSetConfig_ExpressionValueFailsClosed(t *testing.T) {
	t.Run("gateway-managed variable fails closed", func(t *testing.T) {
		stmt, ok := parseOne(t, "SELECT set_config('statement_timeout', some_col, false)").(*ast.SelectStmt)
		require.True(t, ok)
		_, _, err := rewriteGatewayManagedSetConfig(stmt)
		require.Error(t, err, "an expression value on a GMV must fail closed, never route to the backend")
	})

	t.Run("ordinary variable is not the rewrite's concern", func(t *testing.T) {
		// work_mem is not gateway-managed, so the rewrite skips it (nothing to rewrite);
		// its expression value is the analyzer's / backend's concern, not a leak here.
		stmt, ok := parseOne(t, "SELECT set_config('work_mem', some_col, false)").(*ast.SelectStmt)
		require.True(t, ok)
		clone, bound, err := rewriteGatewayManagedSetConfig(stmt)
		require.NoError(t, err)
		assert.Nil(t, clone)
		assert.Nil(t, bound)
	})
}

// TestSetConfig_GatewayManagedIsLocalTrueIsTracked verifies the is_local=true
// asymmetry fix: set_config('<gmv>', v, true) is tracked as a transaction-local
// override (so SHOW matches SET LOCAL <gmv>), while an ordinary variable with
// is_local=true is still left untracked for PostgreSQL to execute via the Route.
func TestSetConfig_GatewayManagedIsLocalTrueIsTracked(t *testing.T) {
	t.Run("gateway-managed name is tracked as local", func(t *testing.T) {
		res, err := analyzeStatement(parseOne(t, "SELECT set_config('statement_timeout', '5s', true)"))
		require.NoError(t, err)
		require.Len(t, res.SetConfigs, 1)
		assert.Equal(t, "statement_timeout", res.SetConfigs[0].Name)
		assert.Equal(t, "5s", res.SetConfigs[0].Value)
		assert.True(t, res.SetConfigs[0].IsLocalLiteralTrue)
	})

	t.Run("ordinary name with is_local=true is not tracked", func(t *testing.T) {
		res, err := analyzeStatement(parseOne(t, "SELECT set_config('work_mem', '64MB', true)"))
		require.NoError(t, err)
		assert.Empty(t, res.SetConfigs)
	})
}

// TestPlanSetConfig_GatewayManagedBoundValue verifies the bound-value plan for a
// gateway-managed set_config('statement_timeout', $1, true): the plan is
// Sequence[GatewayManagedValueRoute, ApplySessionState]. The leading route has the
// set_config rewritten out of the backend query (its value stays a $1 bind,
// canonicalized at execute time), and the trailing ApplySessionState carries IsLocal
// so the executor applies a transaction-local gateway override only after the route
// succeeds.
func TestPlanSetConfig_GatewayManagedBoundValue(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	sql := "SELECT set_config('statement_timeout', $1, true)"
	plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 2)

	// Leading route: the gateway-managed set_config is rewritten out; its value
	// stays a $1 bind that GatewayManagedValueRoute canonicalizes at execute time.
	rr, ok := seq.Primitives[0].(*engine.GatewayManagedValueRoute)
	require.True(t, ok, "leading primitive should be GatewayManagedValueRoute, got %T", seq.Primitives[0])
	assert.NotContains(t, rr.GetQuery(), "set_config(", "the gateway-managed set_config call is rewritten out of the routed query")
	assert.Contains(t, rr.GetQuery(), "$1", "its value stays a bind, canonicalized at execute time")

	// Trailing tracker: applied only after the route succeeds, carrying IsLocal.
	ass, ok := seq.Primitives[len(seq.Primitives)-1].(*engine.ApplySessionState)
	require.True(t, ok, "last primitive should be ApplySessionState, got %T", seq.Primitives[len(seq.Primitives)-1])
	assert.Equal(t, "statement_timeout", ass.VariableStmt.Name)
	assert.True(t, ass.VariableStmt.IsLocal, "is_local=true must be carried so the executor applies a transaction-local override")
	assert.True(t, ass.SilentTracking, "the leading route owns the client response")
}

// TestPlanSetConfig_GatewayManagedSharedBoundValue verifies the shared-param plan:
// when the set_config value param ($1) is *also* used elsewhere in the query,
// canonicalizing $1 in place would corrupt the other use. So the gateway-managed
// call is rewritten to a fresh synthetic slot ($2) - sourced from $1 but
// canonicalized independently - while $1 stays put for its other use. Critically,
// the set_config is still rewritten out of the routed query (no backend leak).
func TestPlanSetConfig_GatewayManagedSharedBoundValue(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	// $1 is used both as the statement_timeout value and in abs($1).
	sql := "SELECT set_config('statement_timeout', $1, false), abs($1)"
	plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)

	rr, ok := seq.Primitives[0].(*engine.GatewayManagedValueRoute)
	require.True(t, ok, "shared bound value -> leading GatewayManagedValueRoute, got %T", seq.Primitives[0])
	q := rr.GetQuery()
	assert.NotContains(t, q, "set_config(", "the gateway-managed set_config is rewritten out - never reaches the backend")
	assert.Contains(t, q, "$2", "its value routes through a fresh synthetic slot, canonicalized at execute time")
	assert.Contains(t, q, "abs($1)", "the shared param $1 is left untouched for its other use")
}

// TestPlanSetConfig_GatewayManagedLiteralValue verifies the literal-value plan:
// a session-scoped (is_local=false) value stays literal, so it's canonicalized at
// PLAN time ('1000' -> '1s') and inlined as a constant. No execute-time work is
// needed, so the leading route is a plain Route (no GatewayManagedValueRoute).
func TestPlanSetConfig_GatewayManagedLiteralValue(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	sql := "SELECT set_config('statement_timeout', '1000', false)"
	plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
	require.NoError(t, err)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)

	route := seq.Primitives[0]
	_, isPlainRoute := route.(*engine.Route)
	assert.True(t, isPlainRoute, "literal value -> plain leading Route (constant baked at plan time), got %T", route)
	assert.NotContains(t, route.GetQuery(), "set_config(", "the gateway-managed set_config call is rewritten out")
	assert.Contains(t, route.GetQuery(), "1s", "the canonical constant is inlined at plan time")
}

// TestPlanSetConfig_MixedGatewayManagedRewrittenOutOfRoute verifies the mixed
// case: a session-scoped gateway-managed variable (literal value) alongside an
// ordinary one. The gateway-managed call is rewritten out (canonical constant),
// while the ordinary work_mem call stays in the routed query.
func TestPlanSetConfig_MixedGatewayManagedRewrittenOutOfRoute(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	sql := "SELECT set_config('statement_timeout', '1000', false), set_config('work_mem', '64MB', false)"
	plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)

	q := seq.Primitives[0].GetQuery()
	assert.NotContains(t, q, "statement_timeout", "the gateway-managed call is rewritten out")
	assert.Contains(t, q, "1s", "canonical constant inlined for the gateway-managed call")
	assert.Contains(t, q, "work_mem", "the ordinary set_config still runs on the backend")
}
