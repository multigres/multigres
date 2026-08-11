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
		// ALTER ROLE ... SET is NOT an admin-only surface: PostgreSQL lets any
		// non-superuser role alter its OWN role-level USERSET defaults, and
		// search_path is USERSET — so `ALTER ROLE current_user SET search_path
		// = <nonexistent>, pg_temp` is reachable by an ordinary client. Because
		// pooled backends connect directly as the user, PostgreSQL applies the
		// persisted pg_db_role_setting natively at backend startup, entirely
		// outside the gateway's session tracking, so no later guard can see or
		// undo it. A leading-only check would be bypassed by prefixing a
		// nonexistent schema (the creation target is the first EXISTING entry),
		// so this is vetted STRICTLY: pg_temp can never be persisted into a
		// role default through the pooler.
		return checkRestrictedSetStmt(s.Setstmt, false)
	case *ast.AlterDatabaseSetStmt:
		// ALTER DATABASE ... SET requires database ownership, which a hosted
		// client cannot obtain (CREATE DATABASE is blocked as Tier 2), so this
		// stays an admin surface: trailing pg_temp is allowed for PostgreSQL's
		// hardening pattern.
		return checkRestrictedSetStmt(s.Setstmt, true)
	case *ast.CreateFunctionStmt:
		// CREATE/ALTER FUNCTION|PROCEDURE ... SET stores a proconfig entry that
		// PostgreSQL applies (and restores) around each call. The SECURITY
		// DEFINER hardening guidance puts a trailing pg_temp in exactly this
		// clause, so it keeps the lenient (leading-only) guard. The leak it can
		// enable — an unqualified temp CREATE inside the function body — is not
		// visible in this SET clause and is closed by function-body analysis
		// (separate work), not here.
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
			// Function proconfig honors the trailing-pg_temp hardening pattern.
			if err := checkRestrictedSetStmt(setstmt, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkRestrictedSetStmt applies the restricted-GUC and search_path value
// checks to a single SET-shaped node. allowTrailingTemp selects the search_path
// guard: true admits PostgreSQL's trailing-pg_temp hardening pattern (leading
// pg_temp still rejected) and is used ONLY for genuinely admin-restricted
// surfaces a hosted client cannot reach — ALTER DATABASE ... SET and function
// proconfig. Every client-reachable surface (session SET, and ALTER ROLE, which
// a non-superuser can self-target) passes false for strict any-position
// rejection. See the two guards in pgsettings for the trust-boundary rationale.
func checkRestrictedSetStmt(setstmt *ast.VariableSetStmt, allowTrailingTemp bool) error {
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
		if allowTrailingTemp {
			return pgsettings.RejectLeadingTempSchemaSearchPath(value)
		}
		return pgsettings.RejectTempSchemaSearchPath(value)
	}
	return nil
}
