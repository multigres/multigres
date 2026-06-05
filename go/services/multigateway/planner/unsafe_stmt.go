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
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
)

// planUnsupportedStmt rejects Tier 2 statements — server-level operations
// that should not be available through a shared connection pooler in a
// hosted environment: LOAD, ALTER SYSTEM, CREATE LANGUAGE, CREATE
// SUBSCRIPTION, CREATE FOREIGN DATA WRAPPER, CREATE SERVER. These change the
// server itself, affect tenant isolation, or open outbound connections to
// external hosts, and there is no "inspect the body" mitigation that could
// make them safe.
//
// CREATE DATABASE / DROP DATABASE are NOT rejected: the multipooler is
// database-aware (it holds connection pools keyed by (database, user) on a
// single PostgreSQL instance), so a newly created database is immediately
// reachable by reconnecting with that dbname. CREATE/DROP DATABASE run on a
// regular pooled connection like any other statement; PostgreSQL enforces
// the usual constraints (not inside a transaction block, not while connected
// to the database being dropped).
//
// Tier 1 statements (DO, CREATE FUNCTION / PROCEDURE, CREATE TRIGGER,
// CREATE RULE, CREATE EVENT TRIGGER) embed procedural-language code. They
// are NOT rejected here: blocking outright breaks real workloads (migrations,
// ORMs, observability tooling) without closing the actual leak vector, since
// equivalent session-state effects are reachable via SELECT set_config(...)
// at the expression level. Tier 1 will be handled by body analysis once the
// PL/pgSQL parser port lands; see docs/query_serving/unsafe_statement_rejection.md.
//
// Returns a *mterrors.PgDiagnostic with SQLSTATE 0A000 (feature_not_supported)
// if the statement is Tier 2, or nil otherwise.
func planUnsupportedStmt(stmt ast.Stmt) error {
	switch stmt.NodeTag() {
	// -- Tier 2: unsafe for hosted infrastructure --

	case ast.T_LoadStmt:
		return mterrors.NewFeatureNotSupported(
			"LOAD is not supported: loading shared libraries is not permitted through the connection pooler")

	case ast.T_AlterSystemStmt:
		return mterrors.NewFeatureNotSupported(
			"ALTER SYSTEM is not supported: modifying server configuration is not permitted through the connection pooler")

	case ast.T_CreatePLangStmt:
		return mterrors.NewFeatureNotSupported(
			"CREATE LANGUAGE is not supported: installing procedural languages is not permitted through the connection pooler")

	case ast.T_CreateSubscriptionStmt:
		return mterrors.NewFeatureNotSupported(
			"CREATE SUBSCRIPTION is not supported: creating replication subscriptions is not permitted through the connection pooler")

	case ast.T_CreateFdwStmt:
		return mterrors.NewFeatureNotSupported(
			"CREATE FOREIGN DATA WRAPPER is not supported through the connection pooler")

	case ast.T_CreateForeignServerStmt:
		return mterrors.NewFeatureNotSupported(
			"CREATE SERVER is not supported: creating foreign server connections is not permitted through the connection pooler")

	// UNLOGGED tables skip the WAL, so their contents are not replicated to
	// standbys and are truncated on crash recovery — incompatible with a
	// replicated, highly-available deployment. Reject CREATE UNLOGGED TABLE
	// and CREATE UNLOGGED TABLE AS / SELECT INTO UNLOGGED; ordinary tables
	// are unaffected.
	case ast.T_CreateStmt:
		if s, ok := stmt.(*ast.CreateStmt); ok &&
			s.Relation != nil && s.Relation.RelPersistence == ast.RELPERSISTENCE_UNLOGGED {
			return mterrors.NewFeatureNotSupported(
				"UNLOGGED tables are not supported: their contents are not replicated and are lost on crash recovery")
		}
		return nil

	case ast.T_CreateTableAsStmt:
		if s, ok := stmt.(*ast.CreateTableAsStmt); ok &&
			s.Into != nil && s.Into.Rel != nil && s.Into.Rel.RelPersistence == ast.RELPERSISTENCE_UNLOGGED {
			return mterrors.NewFeatureNotSupported(
				"UNLOGGED tables are not supported: their contents are not replicated and are lost on crash recovery")
		}
		return nil

	// SELECT ... INTO UNLOGGED parses as a SelectStmt carrying an IntoClause —
	// it only becomes a CreateTableAsStmt during later parse analysis, which
	// does not run at plan time. The persistence flag therefore lives on
	// SelectStmt.IntoClause.Rel here, not on a CreateTableAsStmt. Plain
	// SELECTs (IntoClause == nil) fall straight through.
	case ast.T_SelectStmt:
		if s, ok := stmt.(*ast.SelectStmt); ok &&
			s.IntoClause != nil && s.IntoClause.Rel != nil && s.IntoClause.Rel.RelPersistence == ast.RELPERSISTENCE_UNLOGGED {
			return mterrors.NewFeatureNotSupported(
				"UNLOGGED tables are not supported: their contents are not replicated and are lost on crash recovery")
		}
		return nil

	// ALTER TABLE ... SET UNLOGGED converts an existing permanent table to
	// UNLOGGED in place — same replication / crash-recovery hazard as creating
	// one. A single ALTER may carry several subcommands (e.g. ADD COLUMN, SET
	// UNLOGGED), so scan every command for the SET UNLOGGED subtype. SET LOGGED
	// (the inverse, restoring WAL logging) is harmless and passes through.
	case ast.T_AlterTableStmt:
		if s, ok := stmt.(*ast.AlterTableStmt); ok && s.Cmds != nil {
			for _, item := range s.Cmds.Items {
				if cmd, ok := item.(*ast.AlterTableCmd); ok && cmd.Subtype == ast.AT_SetUnLogged {
					return mterrors.NewFeatureNotSupported(
						"ALTER TABLE ... SET UNLOGGED is not supported: UNLOGGED tables are not replicated and are lost on crash recovery")
				}
			}
		}
		return nil

	default:
		return nil
	}
}
