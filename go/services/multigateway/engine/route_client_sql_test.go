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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// parseOne parses a single statement for Route tests.
func parseOne(t *testing.T, sql string) ast.Stmt {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	return stmts[0]
}

// A Route marked IsClientStatement must send the client's exact statement
// text — not the plan's normalized cache template and not a reconstruction —
// so pg_stat_activity, server logs, and error cursor positions match what the
// client wrote (byte-for-byte, including casing and trailing semicolon).
func TestRoute_ClientStatementSendsOriginalText(t *testing.T) {
	norm := ast.Normalize(parseOne(t, "SELECT * FROM t WHERE id = 42"))
	route := NewRoute("tg1", "shard1", norm.NormalizedSQL, norm.NormalizedAST)
	route.IsClientStatement = true

	mockExec := &mockIExecute{}
	clientText := "SeLeCt * FROM t WHERE id = 42 ;"
	err := route.StreamExecute(context.Background(), mockExec, nil,
		handler.NewMultigatewayConnectionState(), norm.BindValues, clientText,
		PlanExecInfo{}, func(context.Context, *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	require.Equal(t, clientText, mockExec.lastStreamSQL,
		"the client's exact text must reach the backend")
}

// A Route whose text is gateway-synthesized (IsClientStatement false, e.g.
// the set_config resolve projection) must keep reconstructing from its
// normalized AST and never adopt the client's statement text.
func TestRoute_SynthesizedRouteIgnoresClientText(t *testing.T) {
	norm := ast.Normalize(parseOne(t, "SELECT * FROM t WHERE id = 42"))
	route := NewRoute("tg1", "shard1", norm.NormalizedSQL, norm.NormalizedAST)

	mockExec := &mockIExecute{}
	err := route.StreamExecute(context.Background(), mockExec, nil,
		handler.NewMultigatewayConnectionState(), norm.BindValues, "SET search_path = evil",
		PlanExecInfo{}, func(context.Context, *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	require.Equal(t, ast.ReconstructSQL(norm.NormalizedAST, norm.BindValues), mockExec.lastStreamSQL,
		"a synthesized route must reconstruct, not adopt the client text")
}

// Without a per-execution client text (e.g. internal delegations), a flagged
// Route falls back to reconstruction, preserving pre-existing behavior.
func TestRoute_ClientStatementFallsBackToReconstruction(t *testing.T) {
	norm := ast.Normalize(parseOne(t, "SELECT * FROM t WHERE id = 42"))
	route := NewRoute("tg1", "shard1", norm.NormalizedSQL, norm.NormalizedAST)
	route.IsClientStatement = true

	mockExec := &mockIExecute{}
	err := route.StreamExecute(context.Background(), mockExec, nil,
		handler.NewMultigatewayConnectionState(), norm.BindValues, "",
		PlanExecInfo{}, func(context.Context, *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	require.Equal(t, ast.ReconstructSQL(norm.NormalizedAST, norm.BindValues), mockExec.lastStreamSQL)
}
