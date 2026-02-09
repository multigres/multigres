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

package handler

import (
	"context"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// executeWithImplicitTransaction handles multi-statement batches by:
// - Injecting synthetic BEGIN at start and after each COMMIT/ROLLBACK
// - Tracking implicit vs explicit transaction segments
// - Auto-ROLLBACK on failure in implicit segments (matching PostgreSQL behavior)
// - Auto-COMMIT at end of implicit segments
//
// This preserves PostgreSQL's "adoption" semantics where BEGIN mid-batch adopts
// preceding statements into the explicit transaction.
//
// If already in a transaction, it continues without injecting BEGIN but still
// handles mid-batch COMMIT/ROLLBACK that may start new implicit segments.
func (h *MultiGatewayHandler) executeWithImplicitTransaction(
	ctx context.Context,
	conn *server.Conn,
	state *MultiGatewayConnectionState,
	queryStr string,
	stmts []ast.Stmt,
	callback func(ctx context.Context, result *sqltypes.Result) error,
) error {
	execute := func(stmt ast.Stmt) error {
		return h.executor.StreamExecute(ctx, conn, state, stmt.SqlString(), stmt, callback)
	}

	// If already in a transaction, don't inject BEGIN at start
	needsBegin := !state.IsInTransaction()
	isImplicitTx := false

	for _, stmt := range stmts {
		// Inject BEGIN if needed (start of batch or after COMMIT/ROLLBACK)
		if needsBegin {
			if err := execute(ast.NewBeginStmt()); err != nil {
				return err
			}
			needsBegin = false
			isImplicitTx = true
		}

		// User's BEGIN - skip (we already started), mark as explicit
		if ast.IsBeginStatement(stmt) {
			isImplicitTx = false
			continue
		}

		// User's COMMIT/ROLLBACK - execute and prepare for next segment
		if ast.IsCommitStatement(stmt) || ast.IsRollbackStatement(stmt) {
			if err := execute(stmt); err != nil {
				return err
			}
			needsBegin = true
			isImplicitTx = false
			continue
		}

		// Regular statement - execute with error handling
		if err := execute(stmt); err != nil {
			if isImplicitTx {
				// Auto-rollback implicit transaction on failure
				_ = execute(ast.NewRollbackStmt())
			}
			return err
		}
	}

	// Auto-commit if we ended in an implicit transaction
	if isImplicitTx {
		if err := execute(ast.NewCommitStmt()); err != nil {
			return err
		}
	}

	return nil
}
