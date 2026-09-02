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

import "strings"

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
// WARNING) in this state. PREPARE TRANSACTION is also admitted so the gateway's
// PREPARE primitive can forward the backend error and clean up the reserved
// transaction state when PostgreSQL has already aborted the transaction before
// PREPARE reaches the backend.
func IsAllowedInAbortedTransaction(stmt Stmt) bool {
	txStmt, ok := stmt.(*TransactionStmt)
	if !ok {
		return false
	}
	switch txStmt.Kind {
	case TRANS_STMT_ROLLBACK, TRANS_STMT_ROLLBACK_TO, TRANS_STMT_COMMIT, TRANS_STMT_PREPARE:
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

// DDLTargetRelations returns the deduplicated, schema-qualified table names a
// DDL statement targets, for the subset of DDL that can change a table's
// result shape (column count, names, or types) as observed through
// PostgreSQL's prepared-plan result-type check: ALTER TABLE, DROP TABLE, and
// ALTER TABLE ... RENAME (table or column rename). Returns nil for anything
// else, including DDL that targets non-table objects (ALTER INDEX, DROP
// FUNCTION, ...) or can't affect a table's shape (CREATE TABLE, COMMENT ON,
// ...).
//
// This is intentionally narrow rather than "every DDL statement type": the
// only thing a caller does with the result is decide which cached prepared
// statements to invalidate, so a DDL type that provably cannot change what a
// SELECT against the table returns doesn't need to be recognized here.
func DDLTargetRelations(stmt Stmt) []string {
	switch n := stmt.(type) {
	case *AlterTableStmt:
		if n.Objtype != OBJECT_TABLE || n.Relation == nil {
			return nil
		}
		return relationNames(n.Relation)
	case *RenameStmt:
		// RenameType == OBJECT_TABLE is "ALTER TABLE t RENAME TO t2" (the
		// table itself); RelationType == OBJECT_TABLE is "ALTER TABLE t
		// RENAME COLUMN a TO b" (renaming something within the table, tagged
		// via RelationType rather than RenameType — see postgres.y's
		// RenameStmt productions).
		if n.Relation == nil || (n.RenameType != OBJECT_TABLE && n.RelationType != OBJECT_TABLE) {
			return nil
		}
		return relationNames(n.Relation)
	case *DropStmt:
		if n.RemoveType != OBJECT_TABLE || n.Objects == nil {
			return nil
		}
		var names []string
		for _, obj := range n.Objects.Items {
			nameList, ok := obj.(*NodeList)
			if !ok {
				continue
			}
			var parts []string
			for _, part := range nameList.Items {
				s, ok := part.(*String)
				if !ok {
					continue
				}
				parts = append(parts, s.SVal)
			}
			if len(parts) > 0 {
				names = append(names, strings.Join(parts, "."))
			}
		}
		return names
	default:
		return nil
	}
}

// relationNames returns rv's schema-qualified name as a single-element slice,
// matching ExtractTablesUsed's naming convention.
func relationNames(rv *RangeVar) []string {
	if rv.RelName == "" {
		return nil
	}
	name := rv.RelName
	if rv.SchemaName != "" {
		name = rv.SchemaName + "." + name
	}
	return []string{name}
}

// MaxParamRef returns the highest parameter number ($N) referenced anywhere in
// node, or 0 if it contains no ParamRef. Callers use it to allocate fresh
// synthetic parameter slots numbered past every existing bind, so they cannot
// collide with a real one.
func MaxParamRef(node Node) int {
	highest := 0
	Rewrite(node, func(cursor *Cursor) bool {
		if pr, ok := cursor.Node().(*ParamRef); ok && pr.Number > highest {
			highest = pr.Number
		}
		return true
	}, nil)
	return highest
}
