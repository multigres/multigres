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

// TestPlanSetConfig_GatewayManagedIsLocalTruePlan verifies the full plan: a
// Sequence[ApplySessionState, Route] whose ApplySessionState carries IsLocal so
// the executor applies a transaction-local gateway override.
func TestPlanSetConfig_GatewayManagedIsLocalTruePlan(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	sql := "SELECT set_config('statement_timeout', '5s', true)"
	plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)
	require.GreaterOrEqual(t, len(seq.Primitives), 2)

	ass, ok := seq.Primitives[0].(*engine.ApplySessionState)
	require.True(t, ok, "first primitive should be ApplySessionState, got %T", seq.Primitives[0])
	assert.Equal(t, "statement_timeout", ass.VariableStmt.Name)
	assert.True(t, ass.VariableStmt.IsLocal, "is_local=true must be carried so the executor applies a transaction-local override")

	_, ok = seq.Primitives[len(seq.Primitives)-1].(*engine.Route)
	assert.True(t, ok, "last primitive should be Route, got %T", seq.Primitives[len(seq.Primitives)-1])
}
