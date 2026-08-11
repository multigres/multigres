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
	"strings"

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
// RESET, RESET ALL, and SET ... TO DEFAULT.
//
// Runs pre-dispatch via analyzeStatement, so it covers both the simple
// and extended query protocols and is short-circuited by the plan cache.
func checkRestrictedGUCChange(stmt ast.Stmt) error {
	switch s := stmt.(type) {
	case *ast.VariableSetStmt:
		// Session/transaction SET is a client-runtime surface: strict
		// search_path vetting (any pg_temp mention).
		return checkRestrictedSetStmt(s, false)
	case *ast.AlterRoleSetStmt:
		return checkRestrictedSetStmt(s.Setstmt, true)
	case *ast.AlterDatabaseSetStmt:
		return checkRestrictedSetStmt(s.Setstmt, true)
	case *ast.CreateFunctionStmt:
		// CREATE FUNCTION/PROCEDURE ... SET guc = value stores a proconfig
		// entry PostgreSQL applies on every later call of the function — a
		// persisted assignment just like ALTER ROLE ... SET, so it gets the
		// same vetting (search_path = pg_temp here would put every future
		// SELECT f() on an arbitrary pooled backend's temp namespace).
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
			if err := checkRestrictedSetStmt(setstmt, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkRestrictedSetStmt applies the restricted-GUC and search_path value
// checks to a single SET-shaped node. persisted selects the search_path
// guard: admin-authored persisted configuration (ALTER ROLE/DATABASE ... SET,
// function proconfig) allows PostgreSQL's trailing-pg_temp hardening pattern
// and rejects only a leading pg_temp; client-runtime session SET is strict
// (see the two guards in pgsettings for the trust-boundary rationale).
func checkRestrictedSetStmt(setstmt *ast.VariableSetStmt, persisted bool) error {
	if setstmt == nil {
		return nil
	}

	// VAR_SET_DEFAULT (SET ... TO DEFAULT), VAR_RESET (RESET), and
	// VAR_RESET_ALL (RESET ALL) revert to the managed global value and are
	// allowed. Everything else — VAR_SET_VALUE (SET ... = x / SET LOCAL ... = x)
	// and VAR_SET_CURRENT (SET ... FROM CURRENT) — pins an explicit value.
	switch setstmt.Kind {
	case ast.VAR_SET_DEFAULT, ast.VAR_RESET, ast.VAR_RESET_ALL:
		return nil
	}
	if err := restrictedGUCError(setstmt.Name); err != nil {
		return err
	}

	// search_path is value-restricted rather than name-restricted: pg_temp as
	// the effective creation target would make unqualified CREATE land in the
	// temp namespace of whatever pooled backend serves each statement. FROM
	// CURRENT (VAR_SET_CURRENT) carries no args and can only restate a value
	// that already passed this guard.
	if strings.EqualFold(setstmt.Name, "search_path") {
		value := extractVariableValue(setstmt.Args)
		if persisted {
			return pgsettings.RejectLeadingTempSchemaSearchPath(value)
		}
		return pgsettings.RejectTempSchemaSearchPath(value)
	}
	return nil
}
