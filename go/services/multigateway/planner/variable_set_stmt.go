// Copyright 2025 Supabase, Inc.
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
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// planVariableSetStmt plans SET/RESET commands.
// Creates a SessionStateRoute that handles SET (optimistic apply + PG validation)
// and RESET (local state update + synthetic response).
func (p *Planner) planVariableSetStmt(
	sql string,
	stmt *ast.VariableSetStmt,
	conn *server.Conn,
) (*engine.Plan, error) {
	// Just pass through to PostgreSQL
	if stmt.IsLocal {
		p.logger.Debug("SET LOCAL detected, passing through",
			"variable", stmt.Name)
		return p.planDefault(sql, conn)
	}

	// Only track VAR_SET_VALUE, VAR_RESET, VAR_RESET_ALL
	// Other kinds (DEFAULT, CURRENT, MULTI) are passed through
	switch stmt.Kind {
	case ast.VAR_SET_VALUE, ast.VAR_RESET, ast.VAR_RESET_ALL:
		// These are tracked locally
	default:
		// TODO: support VAR_SET_DEFAULT, VAR_SET_CURRENT, VAR_SET_MULTI when needed
		return nil, mterrors.NewFeatureNotSupported(fmt.Sprintf("SET kind %d is not yet supported", stmt.Kind))
	}

	p.logger.Debug("planning SET/RESET command",
		"kind", stmt.Kind,
		"variable", stmt.Name)

	route := engine.NewRoute(p.defaultTableGroup, "", sql)
	sessionStateRoute := engine.NewSessionStateRoute(route, sql, stmt)

	plan := engine.NewPlan(sql, sessionStateRoute)
	p.logger.Debug("created SET/RESET plan", "plan", plan.String())
	return plan, nil
}
