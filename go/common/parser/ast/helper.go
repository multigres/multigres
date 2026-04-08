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

package ast

// IsBeginStatement returns true if the statement is a BEGIN or START TRANSACTION.
func IsBeginStatement(stmt Stmt) bool {
	txStmt, ok := stmt.(*TransactionStmt)
	if !ok {
		return false
	}
	return txStmt.Kind == TRANS_STMT_BEGIN || txStmt.Kind == TRANS_STMT_START
}

// IsCommitStatement returns true if the statement is a COMMIT.
func IsCommitStatement(stmt Stmt) bool {
	txStmt, ok := stmt.(*TransactionStmt)
	if !ok {
		return false
	}
	return txStmt.Kind == TRANS_STMT_COMMIT
}

// IsRollbackStatement returns true if the statement is a ROLLBACK.
// Note: This does not include ROLLBACK TO SAVEPOINT.
func IsRollbackStatement(stmt Stmt) bool {
	txStmt, ok := stmt.(*TransactionStmt)
	if !ok {
		return false
	}
	return txStmt.Kind == TRANS_STMT_ROLLBACK
}

// IsAllowedInAbortedTransaction returns true if the statement may proceed when
// the transaction is in the aborted (failed) state. PostgreSQL allows ROLLBACK,
// ROLLBACK TO SAVEPOINT, and COMMIT (which it converts to ROLLBACK with a
// WARNING) in this state. All other statements are rejected with SQLSTATE 25P02.
func IsAllowedInAbortedTransaction(stmt Stmt) bool {
	txStmt, ok := stmt.(*TransactionStmt)
	if !ok {
		return false
	}
	switch txStmt.Kind {
	case TRANS_STMT_ROLLBACK, TRANS_STMT_ROLLBACK_TO, TRANS_STMT_COMMIT:
		return true
	default:
		return false
	}
}

// ExtractTablesUsed walks the AST and returns deduplicated, schema-qualified
// table names from all RangeVar nodes. CTE names are excluded since they are
// virtual tables, not real ones. Returns nil for statements that don't
// reference tables (SET, SHOW, BEGIN, etc.).
func ExtractTablesUsed(stmt Stmt) []string {
	if stmt == nil {
		return nil
	}

	// Single pass: collect CTE names and RangeVar references together,
	// then filter CTE references from the result.
	cteNames := make(map[string]struct{})
	seen := make(map[string]struct{})
	var tables []string

	Rewrite(stmt, func(cursor *Cursor) bool {
		switch n := cursor.Node().(type) {
		case *CommonTableExpr:
			if n.Ctename != "" {
				cteNames[n.Ctename] = struct{}{}
			}
		case *RangeVar:
			if n.RelName == "" {
				return true
			}
			name := n.RelName
			if n.SchemaName != "" {
				name = n.SchemaName + "." + name
			}
			if _, exists := seen[name]; !exists {
				seen[name] = struct{}{}
				tables = append(tables, name)
			}
		}
		return true
	}, nil)

	// Remove CTE references (unqualified names that match a CTE).
	if len(cteNames) > 0 {
		filtered := tables[:0]
		for _, name := range tables {
			if _, isCTE := cteNames[name]; !isCTE {
				filtered = append(filtered, name)
			}
		}
		tables = filtered
	}

	return tables
}
