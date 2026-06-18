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
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// TestPlanVariableShowStmt_QuotedGatewayManagedName is a regression test for the
// case-sensitive SHOW path: a quoted identifier (e.g. SHOW "Statement_Timeout")
// preserves case, so the raw name must be lowercased before reaching the
// executor's case-sensitive switch — otherwise it would miss the switch and
// panic. The fix canonicalizes the name at plan time.
func TestPlanVariableShowStmt_QuotedGatewayManagedName(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	// Quoted identifier preserves case; the parser does not lowercase it.
	stmt := &ast.VariableShowStmt{Name: "Statement_Timeout"}

	plan, err := p.planVariableShowStmt(`SHOW "Statement_Timeout"`, stmt, testConn.Conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	prim, ok := plan.Primitive.(*engine.GatewayShowVariable)
	require.True(t, ok, "expected GatewayShowVariable primitive, got %T", plan.Primitive)

	state := &handler.MultiGatewayConnectionState{}
	state.SetStatementTimeout(5 * time.Second)

	var results []*sqltypes.Result
	require.NotPanics(t, func() {
		err = prim.StreamExecute(context.Background(), nil, testConn.Conn, state, nil, engine.PlanExecInfo{},
			func(_ context.Context, r *sqltypes.Result) error {
				results = append(results, r)
				return nil
			})
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Fields, 1)
	// Column label matches the canonical lowercased GUC name PostgreSQL returns.
	assert.Equal(t, "statement_timeout", results[0].Fields[0].Name)
	require.Len(t, results[0].Rows, 1)
}
