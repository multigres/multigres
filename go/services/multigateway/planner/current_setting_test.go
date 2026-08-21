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

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// TestRewriteGatewayManagedCurrentSetting covers the plan-time rewrite in
// isolation: which current_setting calls are recognized as gateway-managed and
// replaced by a synthetic bind slot, and which are left untouched.
func TestRewriteGatewayManagedCurrentSetting(t *testing.T) {
	t.Run("gateway-managed name is rewritten to a synthetic slot", func(t *testing.T) {
		stmt := parseOne(t, "SELECT current_setting('statement_timeout', true)")
		rewritten, reads, err := rewriteGatewayManagedCurrentSetting(stmt)
		require.NoError(t, err)
		require.NotNil(t, rewritten, "a GMV current_setting must be rewritten")
		require.Len(t, reads, 1)
		assert.Equal(t, "statement_timeout", reads[0].Name)
		assert.Equal(t, 1, reads[0].Param, "the first synthetic slot is $1")

		q := rewritten.SqlString()
		assert.NotContains(t, q, "current_setting(", "the call is rewritten out")
		assert.Contains(t, q, "$1", "its value becomes a bind slot filled at execute time")
		assert.Contains(t, q, "AS current_setting", "the output column name is preserved")
	})

	t.Run("one-arg form is rewritten too", func(t *testing.T) {
		stmt := parseOne(t, "SELECT current_setting('statement_timeout')")
		rewritten, reads, err := rewriteGatewayManagedCurrentSetting(stmt)
		require.NoError(t, err)
		require.NotNil(t, rewritten)
		require.Len(t, reads, 1)
		assert.Equal(t, "statement_timeout", reads[0].Name)
	})

	t.Run("ordinary variable is left untouched", func(t *testing.T) {
		stmt := parseOne(t, "SELECT current_setting('search_path')")
		rewritten, reads, err := rewriteGatewayManagedCurrentSetting(stmt)
		require.NoError(t, err)
		assert.Nil(t, rewritten, "a non-GMV current_setting routes to the backend unchanged")
		assert.Nil(t, reads)
	})

	t.Run("bound name is not recognized as a GMV", func(t *testing.T) {
		// current_setting($1): the name is not a literal, so it can't be matched to
		// a GMV at plan time — routes to the backend, same gap as a bound set_config.
		stmt := parseOne(t, "SELECT current_setting($1)")
		rewritten, reads, err := rewriteGatewayManagedCurrentSetting(stmt)
		require.NoError(t, err)
		assert.Nil(t, rewritten)
		assert.Nil(t, reads)
	})

	t.Run("nested call in WHERE is rewritten", func(t *testing.T) {
		stmt := parseOne(t, "SELECT 1 WHERE current_setting('statement_timeout') = '2s'")
		rewritten, reads, err := rewriteGatewayManagedCurrentSetting(stmt)
		require.NoError(t, err)
		require.NotNil(t, rewritten)
		require.Len(t, reads, 1)
		assert.NotContains(t, rewritten.SqlString(), "current_setting(")
	})

	t.Run("synthetic slot is numbered past existing params", func(t *testing.T) {
		// $1 is a client param; the current_setting slot must not collide with it.
		stmt := parseOne(t, "SELECT current_setting('statement_timeout'), $1")
		rewritten, reads, err := rewriteGatewayManagedCurrentSetting(stmt)
		require.NoError(t, err)
		require.Len(t, reads, 1)
		assert.Equal(t, 2, reads[0].Param, "the slot is allocated past the client's $1")
		assert.Contains(t, rewritten.SqlString(), "$2")
	})

	t.Run("multiple GMV calls each get their own slot", func(t *testing.T) {
		stmt := parseOne(t, "SELECT current_setting('statement_timeout'), current_setting('idle_session_timeout')")
		_, reads, err := rewriteGatewayManagedCurrentSetting(stmt)
		require.NoError(t, err)
		require.Len(t, reads, 2)
		assert.Equal(t, "statement_timeout", reads[0].Name)
		assert.Equal(t, "idle_session_timeout", reads[1].Name)
		assert.NotEqual(t, reads[0].Param, reads[1].Param)
	})

	t.Run("RETURNING target keeps the current_setting column name", func(t *testing.T) {
		// The parent-of-the-call is a ResTarget in the RETURNING list, so the
		// output column is named like PostgreSQL's — not just top-level SELECT.
		stmt := parseOne(t, "UPDATE t SET a = 1 RETURNING current_setting('statement_timeout')")
		rewritten, reads, err := rewriteGatewayManagedCurrentSetting(stmt)
		require.NoError(t, err)
		require.Len(t, reads, 1)
		assert.Contains(t, rewritten.SqlString(), "AS current_setting")
	})

	t.Run("repeated same-name reads share one slot", func(t *testing.T) {
		// Two reads of the same variable resolve to one value, so they reuse a
		// single slot (one gateway lookup) — the second projection reuses $1.
		stmt := parseOne(t, "SELECT current_setting('statement_timeout'), current_setting('statement_timeout')")
		rewritten, reads, err := rewriteGatewayManagedCurrentSetting(stmt)
		require.NoError(t, err)
		require.Len(t, reads, 1, "one read shared across both occurrences")
		assert.Equal(t, 1, reads[0].Param)
		q := rewritten.SqlString()
		assert.NotContains(t, q, "$2", "no second slot is allocated for the repeat")
		assert.NotContains(t, q, "current_setting(", "both occurrences are rewritten out")
	})
}

// TestPlanCurrentSetting_GatewayManaged verifies the end-to-end plan for a
// `SELECT current_setting('<gmv>', …)`: the plan is a GatewayManagedValueRoute
// whose routed query has the call rewritten out to a bind slot filled from gateway
// state at execute time.
func TestPlanCurrentSetting_GatewayManaged(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	sql := "SELECT current_setting('statement_timeout', true)"
	plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
	require.NoError(t, err)
	require.NotNil(t, plan)

	gmv, ok := plan.Primitive.(*engine.GatewayManagedValueRoute)
	require.True(t, ok, "expected GatewayManagedValueRoute, got %T", plan.Primitive)
	q := gmv.GetQuery()
	assert.NotContains(t, q, "current_setting(", "the gateway-managed current_setting is rewritten out of the routed query")
	assert.Contains(t, q, "$1", "its value becomes a bind slot filled from gateway state at execute time")
}

// TestPlanCurrentSetting_OrdinaryVariableRoutesToBackend verifies that a
// current_setting for a variable the gateway does NOT manage is left in the query
// and routed to the backend unchanged (a plain Route, no GatewayManagedValueRoute).
func TestPlanCurrentSetting_OrdinaryVariableRoutesToBackend(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	sql := "SELECT current_setting('search_path')"
	plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
	require.NoError(t, err)

	route, ok := plan.Primitive.(*engine.Route)
	require.True(t, ok, "an ordinary current_setting -> plain Route, got %T", plan.Primitive)
	assert.Contains(t, route.GetQuery(), "current_setting", "the call still runs on the backend")
}

// TestPlanCurrentSetting_MaterializesOnce verifies the statements that materialize
// (evaluate once) a gateway-managed current_setting are rewritten: CREATE TABLE AS
// and SELECT INTO. Both store the resolved gateway value, matching native
// PostgreSQL, and preserve the "current_setting" column name (which becomes the new
// table's column).
func TestPlanCurrentSetting_MaterializesOnce(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	for _, sql := range []string{
		"CREATE TABLE t AS SELECT current_setting('statement_timeout')",
		"SELECT current_setting('statement_timeout') INTO t",
	} {
		t.Run(sql, func(t *testing.T) {
			plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
			require.NoError(t, err)
			gmv, ok := plan.Primitive.(*engine.GatewayManagedValueRoute)
			require.True(t, ok, "expected GatewayManagedValueRoute, got %T", plan.Primitive)
			q := gmv.GetQuery()
			assert.NotContains(t, q, "current_setting(", "the call is rewritten out and materialized")
			assert.Contains(t, q, "AS current_setting", "the materialized column keeps its name")
		})
	}
}

// TestPlanCurrentSetting_MaterializedViewNotRewritten verifies the freeze-guard: a
// CREATE MATERIALIZED VIEW shares the CREATE TABLE AS node but stores a refreshable
// query, so its current_setting must NOT be rewritten (a rewrite would freeze the
// value across REFRESH). It routes to the backend unchanged, like CREATE VIEW.
func TestPlanCurrentSetting_MaterializedViewNotRewritten(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	sql := "CREATE MATERIALIZED VIEW mv AS SELECT current_setting('statement_timeout')"
	plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
	require.NoError(t, err)

	route, ok := plan.Primitive.(*engine.Route)
	require.True(t, ok, "a materialized view must not be rewritten -> plain Route, got %T", plan.Primitive)
	assert.Contains(t, route.GetQuery(), "current_setting(",
		"the stored query keeps the live call so REFRESH re-evaluates it")
}

// TestPlanCurrentSetting_WithSetConfig verifies co-occurrence: a SELECT that both
// sets a gateway-managed variable via set_config and reads one via current_setting.
// Both are rewritten out of the single routed query, riding on one
// GatewayManagedValueRoute inside the tracking Sequence.
func TestPlanCurrentSetting_WithSetConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	sql := "SELECT set_config('statement_timeout', '1000', false), current_setting('statement_timeout')"
	plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
	require.NoError(t, err)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence, got %T", plan.Primitive)

	gmv, ok := seq.Primitives[0].(*engine.GatewayManagedValueRoute)
	require.True(t, ok, "leading primitive should be GatewayManagedValueRoute, got %T", seq.Primitives[0])
	q := gmv.GetQuery()
	assert.NotContains(t, q, "set_config(", "the gateway-managed set_config is rewritten out")
	assert.NotContains(t, q, "current_setting(", "the gateway-managed current_setting is rewritten out")
}
