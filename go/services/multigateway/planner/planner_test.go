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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

func TestPrimitiveName(t *testing.T) {
	tests := []struct {
		name      string
		primitive engine.Primitive
		want      string
	}{
		{
			name:      "Route",
			primitive: engine.NewRoute("default", "0-inf", "SELECT 1", nil),
			want:      engine.PlanTypeRoute,
		},
		{
			name:      "Transaction",
			primitive: engine.NewTransactionPrimitive(ast.TRANS_STMT_BEGIN, "BEGIN", "default", nil),
			want:      engine.PlanTypeTransaction,
		},
		{
			name:      "CopyStatement",
			primitive: engine.NewCopyStatement("default", "COPY t FROM STDIN", &ast.CopyStmt{}),
			want:      engine.PlanTypeCopyStatement,
		},
		{
			name:      "ApplySessionState",
			primitive: engine.NewApplySessionState("SET x = 1", &ast.VariableSetStmt{}),
			want:      engine.PlanTypeApplySessionState,
		},
		{
			name:      "GatewaySessionState",
			primitive: engine.NewStatementTimeoutSet("SET statement_timeout = '5s'", 5*time.Second),
			want:      engine.PlanTypeGatewaySessionState,
		},
		{
			name:      "GatewayShowVariable",
			primitive: engine.NewGatewayShowVariable("SHOW statement_timeout", "statement_timeout"),
			want:      engine.PlanTypeGatewayShowVariable,
		},
		{
			name:      "Sequence",
			primitive: engine.NewSequence([]engine.Primitive{engine.NewRoute("default", "0-inf", "SELECT 1", nil)}),
			want:      engine.PlanTypeSequence,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, primitiveName(tt.primitive))
		})
	}
}
