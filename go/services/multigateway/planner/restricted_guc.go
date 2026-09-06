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
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgsettings"
)

// restrictedGUCError reports whether name is a cluster-managed GUC users may
// not assign. The map and message live in pgsettings so the engine's
// execute-time bound-name resolution shares them (see
// pgsettings.RestrictedGUCError).
func restrictedGUCError(name string) error {
	return pgsettings.RestrictedGUCError(name)
}

// checkRestrictedGUCChange rejects statements that assign a value to a
// cluster-managed GUC (see restrictedGUCs) at any gateway-reachable level:
// session/transaction SET (including SET LOCAL and SET ... FROM CURRENT),
// ALTER DATABASE ... SET, and ALTER ROLE ... SET. (ALTER SYSTEM is already
// blocked wholesale as a Tier 2 statement; set_config(...) is handled in the
// expression walker.)
//
// Reverts are allowed because they can only restore the cluster-managed value:
// RESET, RESET ALL, and SET ... TO DEFAULT. SET ... FROM CURRENT is refused
// for every GUC on every surface: its value lives on the backend rather than
// in the statement, so it cannot be vetted (see checkRestrictedSetStmt).
//
// Runs pre-dispatch via analyzeStatement, so it covers both the simple
// and extended query protocols and is short-circuited by the plan cache.
func checkRestrictedGUCChange(stmt ast.Stmt) error {
	switch s := stmt.(type) {
	case *ast.VariableSetStmt:
		return checkRestrictedSetStmt(s)
	case *ast.AlterRoleSetStmt:
		return checkRestrictedSetStmt(s.Setstmt)
	case *ast.AlterDatabaseSetStmt:
		return checkRestrictedSetStmt(s.Setstmt)
	case *ast.CreateFunctionStmt:
		// CREATE/ALTER FUNCTION|PROCEDURE ... SET stores a proconfig entry that
		// PostgreSQL applies on every later call of the function — a persisted
		// assignment like ALTER ROLE ... SET, vetted the same way.
		return checkRestrictedFunctionOptions(s.Options)
	case *ast.AlterFunctionStmt:
		return checkRestrictedFunctionOptions(s.Actions)
	default:
		return nil
	}
}

// checkRestrictedFunctionOptions vets the SET clauses among a CREATE/ALTER
// FUNCTION option list. The grammar encodes each as DefElem{Defname: "set"}
// wrapping the same VariableSetStmt the statement-level SET produces.
func checkRestrictedFunctionOptions(options *ast.NodeList) error {
	if options == nil {
		return nil
	}
	for _, item := range options.Items {
		de, ok := item.(*ast.DefElem)
		if !ok || de.Defname != "set" {
			continue
		}
		if setstmt, ok := de.Arg.(*ast.VariableSetStmt); ok {
			if err := checkRestrictedSetStmt(setstmt); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkRestrictedSetStmt applies the restricted-GUC and search_path value
// checks to a single SET-shaped node, wherever it appeared. Every surface is
// vetted identically: pg_temp is rejected in ANY position, on session SET,
// ALTER ROLE/DATABASE ... SET, and function proconfig alike.
//
// The uniform rule is deliberate. A position-aware check cannot be made sound
// here — the effective creation target is the first EXISTING schema, which the
// gateway cannot determine, so a trailing pg_temp preceded by a nonexistent
// schema ("nosuch, pg_temp") still resolves to the temp namespace. Rather than
// carry a per-surface matrix whose safety depends on schema existence the
// gateway cannot see, no surface may put pg_temp in search_path at all.
func checkRestrictedSetStmt(setstmt *ast.VariableSetStmt) error {
	if setstmt == nil {
		return nil
	}

	// VAR_SET_DEFAULT (SET ... TO DEFAULT), VAR_RESET (RESET), and
	// VAR_RESET_ALL (RESET ALL) revert to the managed global value and are
	// allowed. VAR_SET_VALUE (SET ... = x / SET LOCAL ... = x) pins an explicit
	// value and is vetted below.
	switch setstmt.Kind {
	case ast.VAR_SET_DEFAULT, ast.VAR_RESET, ast.VAR_RESET_ALL:
		return nil
	case ast.VAR_SET_CURRENT:
		// SET ... FROM CURRENT persists whatever the session happens to hold,
		// and that value is nowhere in the statement — Args is empty, so a
		// value-restricted GUC (search_path) cannot be vetted and the guard
		// would pass vacuously on an empty string.
		//
		// It is not enough to observe that the current value once passed a
		// guard: it need not have passed THIS one. It can be inherited from a
		// more lenient surface (ALTER DATABASE ... SET, where a trailing
		// pg_temp is deliberately allowed) or applied natively by PostgreSQL
		// from a role/database default at pooled-backend startup, entirely
		// outside the gateway's session tracking. PostgreSQL resolves FROM
		// CURRENT into a concrete stored value, so accepting it here would pin
		// an unvetted list into pg_db_role_setting or proconfig that no later
		// guard can see or undo.
		//
		// Refused for every GUC, matching planVariableSetStmt, which already
		// rejects the session-level form outright for the same reason. Nothing
		// is lost: pg_dump/pg_dumpall never emit FROM CURRENT (they emit the
		// resolved literal), so dump/restore is unaffected and the workaround
		// is to state the value explicitly.
		return mterrors.NewFeatureNotSupported(fmt.Sprintf(
			"SET %s FROM CURRENT is not supported under connection pooling: "+
				"the value is resolved on the backend and cannot be vetted; specify the value explicitly",
			setstmt.Name))
	}
	if err := restrictedGUCError(setstmt.Name); err != nil {
		return err
	}

	// search_path is value-restricted rather than name-restricted: pg_temp as
	// the effective creation target would make unqualified CREATE land in the
	// temp namespace of whatever pooled backend serves each statement.
	if strings.EqualFold(setstmt.Name, "search_path") {
		return pgsettings.RejectTempSchemaSearchPath(extractVariableValue(setstmt.Args))
	}
	return nil
}
