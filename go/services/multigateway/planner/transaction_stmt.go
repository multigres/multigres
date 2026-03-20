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
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// planTransactionStmt creates a plan for transaction control statements.
//
// Supported statements:
// - BEGIN/START TRANSACTION: Deferred execution (sets state, no backend call)
// - COMMIT: Releases reserved connections with commit
// - ROLLBACK: Releases reserved connections with rollback
// - SAVEPOINT, RELEASE, ROLLBACK TO: Pass through to backend (future enhancement)
func (p *Planner) planTransactionStmt(
	sql string,
	stmt *ast.TransactionStmt,
) (*engine.Plan, error) {
	p.logger.Debug("planning transaction statement",
		"kind", stmt.Kind.String(),
		"sql", sql)

	primitive := engine.NewTransactionPrimitive(stmt.Kind, sql, p.defaultTableGroup)
	plan := engine.NewPlan(sql, primitive)

	p.logger.Debug("created transaction plan",
		"plan", plan.String(),
		"kind", stmt.Kind.String())

	return plan, nil
}
