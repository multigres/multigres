// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/multigres/multigres/go/common/sqltypes"
)

// ReadOnlyOverride reports whether stmt would lift the read-only default the
// gateway overlays while a database is read-only (see readonly package). The
// gateway must refuse these before they reach postgres, since postgres itself
// lets a session override default_transaction_read_only:
//
//   - SET [SESSION] transaction_read_only / default_transaction_read_only to
//     anything but on, and RESET / SET ... TO DEFAULT of either
//   - SET TRANSACTION READ WRITE and SET SESSION CHARACTERISTICS AS
//     TRANSACTION READ WRITE
//   - BEGIN / START TRANSACTION ... READ WRITE
//
// set_config('transaction_read_only', 'off', ...) needs no handling: postgres
// rejects it because the enclosing SELECT already took a snapshot, and a
// set_config of the default is re-overridden on the next request. A statement
// that keeps the session read-only (SET transaction_read_only = on) is allowed.
func ReadOnlyOverride(stmt ast.Stmt) bool {
	switch s := stmt.(type) {
	case *ast.VariableSetStmt:
		switch s.Kind {
		case ast.VAR_SET_VALUE:
			return isReadOnlyGUC(s.Name) && !isOn(extractVariableValue(s.Args))
		case ast.VAR_RESET, ast.VAR_SET_DEFAULT:
			return isReadOnlyGUC(s.Name)
		case ast.VAR_SET_MULTI:
			// SET TRANSACTION ... / SET SESSION CHARACTERISTICS AS TRANSACTION ...
			return hasReadWriteOption(s.Args)
		}
	case *ast.TransactionStmt:
		return ast.IsBeginStatement(s) && hasReadWriteOption(s.Options)
	}
	return false
}

// NonAtomicProcedure reports whether stmt is a CALL or DO. Executed outside a
// transaction block, a procedure body may COMMIT, which ends the read-only
// transaction and starts a fresh one it can switch to read-write before any
// query (SET TRANSACTION READ WRITE, or a changed default_transaction_read_only)
// and then write through. Inside a transaction block postgres refuses the
// COMMIT, so the gateway only refuses the non-atomic form while read-only.
func NonAtomicProcedure(stmt ast.Stmt) bool {
	switch stmt.(type) {
	case *ast.CallStmt, *ast.DoStmt:
		return true
	}
	return false
}

func isReadOnlyGUC(name string) bool {
	switch strings.ToLower(name) {
	case "transaction_read_only", "default_transaction_read_only":
		return true
	}
	return false
}

func isOn(value string) bool {
	on, ok := sqltypes.ParseBool(value)
	return ok && on
}

// hasReadWriteOption scans a transaction-mode list (BEGIN options or SET
// TRANSACTION args) for transaction_read_only = false.
func hasReadWriteOption(opts *ast.NodeList) bool {
	if opts == nil {
		return false
	}
	for _, opt := range opts.Items {
		def, ok := opt.(*ast.DefElem)
		if !ok || def.Defname != "transaction_read_only" {
			continue
		}
		if b, ok := def.Arg.(*ast.Boolean); ok && !b.BoolVal {
			return true
		}
	}
	return false
}
