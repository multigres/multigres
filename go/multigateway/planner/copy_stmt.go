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
	"errors"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/multigateway/engine"
	"github.com/multigres/multigres/go/parser/ast"
)

// planCopyStmt plans COPY commands.
// Supports COPY FROM STDIN (streaming), COPY FROM/TO file (pass-through).
// Rejects COPY FROM/TO PROGRAM for security. COPY TO STDOUT not yet supported.
func (p *Planner) planCopyStmt(
	sql string,
	stmt *ast.CopyStmt,
	conn *server.Conn,
) (*engine.Plan, error) {
	// SECURITY: Reject COPY FROM/TO PROGRAM (arbitrary command execution)
	if stmt.IsProgram {
		return nil, errors.New("COPY with PROGRAM not supported for security reasons")
	}

	// Decision tree based on IsFrom and Filename
	if stmt.IsFrom {
		// COPY FROM ...
		if stmt.Filename == "" {
			// COPY FROM STDIN - requires CopyStatement primitive (streaming)
			p.logger.Debug("planning COPY FROM STDIN command",
				"query", sql,
				"tablegroup", p.defaultTableGroup)

			copyPrimitive := engine.NewCopyStatement(p.defaultTableGroup, sql, stmt)
			plan := engine.NewPlan(sql, copyPrimitive)
			p.logger.Debug("created COPY FROM STDIN plan", "plan", plan.String())
			return plan, nil
		} else {
			// COPY FROM file - simple Route (PostgreSQL reads server-side file)
			// TODO(multigateway): Future enhancement - multigateway could intercept
			// this to support client-side file uploads or distributed file access
			// across shards. This would require extending the COPY protocol handling.
			p.logger.Debug("planning COPY FROM file command (pass-through)",
				"query", sql,
				"file", stmt.Filename,
				"tablegroup", p.defaultTableGroup)

			route := engine.NewRoute(p.defaultTableGroup, "", sql)
			plan := engine.NewPlan(sql, route)
			p.logger.Debug("created COPY FROM file plan (pass-through)", "plan", plan.String())
			return plan, nil
		}
	} else {
		// COPY TO ...
		if stmt.Filename == "" {
			// COPY TO STDOUT - not yet supported
			return nil, errors.New("COPY TO STDOUT not yet supported")
		} else {
			// COPY TO file - simple Route (PostgreSQL writes server-side file)
			// TODO(multigateway): Future enhancement - similar to FROM file,
			// multigateway could intercept to support client downloads or
			// distributed writes across shards.
			p.logger.Debug("planning COPY TO file command (pass-through)",
				"query", sql,
				"file", stmt.Filename,
				"tablegroup", p.defaultTableGroup)

			route := engine.NewRoute(p.defaultTableGroup, "", sql)
			plan := engine.NewPlan(sql, route)
			p.logger.Debug("created COPY TO file plan (pass-through)", "plan", plan.String())
			return plan, nil
		}
	}
}
