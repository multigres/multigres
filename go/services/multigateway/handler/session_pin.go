// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package handler

import (
	"github.com/multigres/multigres/go/common/parser/ast"
)

// checkAndPinForTempTable checks if an AST node creates a temporary table
// and pins the session if so. Temp tables are session-local in PostgreSQL
// and require a dedicated connection to remain accessible across queries.
func checkAndPinForTempTable(node ast.Stmt, state *MultiGatewayConnectionState) {
	if state.SessionPinned {
		return
	}

	switch stmt := node.(type) {
	case *ast.CreateStmt:
		if stmt.Relation != nil && stmt.Relation.RelPersistence == 't' {
			state.SessionPinned = true
		}
	case *ast.CreateTableAsStmt:
		// Covers CREATE TEMP TABLE AS SELECT.
		if stmt.Into != nil && stmt.Into.Rel != nil && stmt.Into.Rel.RelPersistence == 't' {
			state.SessionPinned = true
		}
	case *ast.SelectStmt:
		// Covers SELECT INTO TEMPORARY TABLE (parsed as SelectStmt with IntoClause).
		if stmt.IntoClause != nil && stmt.IntoClause.Rel != nil && stmt.IntoClause.Rel.RelPersistence == 't' {
			state.SessionPinned = true
		}
	case *ast.ViewStmt:
		if stmt.View != nil && stmt.View.RelPersistence == 't' {
			state.SessionPinned = true
		}
	}
}
