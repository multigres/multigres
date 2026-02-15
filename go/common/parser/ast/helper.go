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
