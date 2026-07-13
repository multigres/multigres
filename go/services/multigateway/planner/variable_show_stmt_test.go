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
	"github.com/multigres/multigres/go/common/servenv"
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

	state := &handler.MultigatewayConnectionState{}
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

// TestPlanVariableShowStmt_MultigresVersion verifies that SHOW multigres_version
// is handled locally by the gateway (never routed to a backend) and produces a
// single-row result whose column is labelled multigres_version.
func TestPlanVariableShowStmt_MultigresVersion(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	// The parser lowercases unquoted identifiers; assert the executor also
	// matches a case-preserving quoted spelling.
	for _, name := range []string{"multigres_version", "Multigres_Version"} {
		t.Run(name, func(t *testing.T) {
			stmt := &ast.VariableShowStmt{Name: name}

			plan, err := p.planVariableShowStmt("SHOW "+name, stmt, testConn.Conn)
			require.NoError(t, err)
			require.NotNil(t, plan)

			prim, ok := plan.Primitive.(*engine.GatewayShowVersion)
			require.True(t, ok, "expected GatewayShowVersion primitive, got %T", plan.Primitive)

			var results []*sqltypes.Result
			err = prim.StreamExecute(context.Background(), nil, testConn.Conn, &handler.MultigatewayConnectionState{}, nil, engine.PlanExecInfo{},
				func(_ context.Context, r *sqltypes.Result) error {
					results = append(results, r)
					return nil
				})
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Len(t, results[0].Fields, 1)
			assert.Equal(t, "multigres_version", results[0].Fields[0].Name)
			require.Len(t, results[0].Rows, 1)
			require.Len(t, results[0].Rows[0].Values, 1)
			// The GUC returns the short release version.
			assert.Equal(t, servenv.Version(), string(results[0].Rows[0].Values[0]))
		})
	}
}
