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
	"strings"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// planVariableShowStmt plans SHOW commands.
// Gateway-managed variables are returned directly without a PostgreSQL round-trip.
// All other variables are routed to PostgreSQL via planDefault.
func (p *Planner) planVariableShowStmt(
	sql string,
	stmt *ast.VariableShowStmt,
	conn *server.Conn,
) (*engine.Plan, error) {
	// Canonicalize to the lowercase GUC name. PostgreSQL lowercases unquoted
	// identifiers in the parser, but a quoted name (e.g. SHOW "Statement_Timeout")
	// preserves case. isGatewayManagedVariable already compares
	// case-insensitively; passing the lowercased name to the primitive keeps
	// the executor's case-sensitive switch from missing it (which would panic)
	// and matches the column label PostgreSQL returns.
	name := strings.ToLower(stmt.Name)
	if !isGatewayManagedVariable(name) {
		return p.planDefault(sql, stmt, conn, PlanOptions{})
	}

	p.logger.Debug("planning SHOW gateway-managed variable", "variable", name)
	primitive := engine.NewGatewayShowVariable(sql, name)
	plan := engine.NewPlan(sql, primitive)
	return plan, nil
}
