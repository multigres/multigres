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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
)

// rejectUnsupportedStatement rejects Tier 2 statements — server-level operations
// that should not be available through a shared connection pooler in a
// hosted environment: LOAD, ALTER SYSTEM, CREATE/DROP DATABASE, CREATE
// LANGUAGE, CREATE SUBSCRIPTION. These change the server itself, affect
// tenant isolation, or open outbound replication connections, and there is
// no "inspect the body" mitigation that could make them safe.
//
// Foreign-data wrappers (CREATE FOREIGN DATA WRAPPER, CREATE SERVER) are
// deliberately not in this list. They are ordinary catalog objects guarded by
// PostgreSQL's own privilege model (superuser for the wrapper, USAGE on it for
// the server), the remote access they enable runs entirely inside the backend,
// and hosted workloads depend on them (postgres_fdw, file_fdw, Supabase
// Wrappers). See docs/query_serving/unsafe_statement_rejection.md for the
// residual postgres_fdw connection-cache note.
//
// Tier 1 statements (DO, CREATE FUNCTION / PROCEDURE, CREATE TRIGGER,
// CREATE RULE, CREATE EVENT TRIGGER) embed procedural-language code and are
// NOT rejected wholesale here — blocking outright breaks real workloads
// (migrations, ORMs, observability tooling). They are instead handled by body
// analysis: analyzeProceduralBody (unsafe_plpgsql.go) parses a DO / CREATE FUNCTION
// PL/pgSQL or SQL body and runs the same expression-level filter over every
// embedded fragment, while the embedded SQL of CREATE RULE / TRIGGER / EVENT
// TRIGGER is already reached by analyzeFunctionCalls' top-level FuncCall walk.
// See docs/query_serving/unsafe_statement_rejection.md.
//
// Returns a *mterrors.PgDiagnostic with SQLSTATE 0A000 (feature_not_supported)
// if the statement is Tier 2, or nil otherwise.
func rejectUnsupportedStatement(stmt ast.Stmt) error {
	switch stmt.NodeTag() {
	// -- Tier 2: unsafe for hosted infrastructure --

	case ast.T_LoadStmt:
		return mterrors.NewFeatureNotSupported(
			"LOAD is not supported: loading shared libraries is not permitted through the connection pooler")

	case ast.T_AlterSystemStmt:
		return mterrors.NewFeatureNotSupported(
			"ALTER SYSTEM is not supported: modifying server configuration is not permitted through the connection pooler")

	case ast.T_CreatedbStmt:
		return mterrors.NewFeatureNotSupported(
			"CREATE DATABASE is not supported through the connection pooler")

	case ast.T_DropdbStmt:
		return mterrors.NewFeatureNotSupported(
			"DROP DATABASE is not supported through the connection pooler")

	case ast.T_CreatePLangStmt:
		return mterrors.NewFeatureNotSupported(
			"CREATE LANGUAGE is not supported: installing procedural languages is not permitted through the connection pooler")

	case ast.T_CreateSubscriptionStmt:
		return mterrors.NewFeatureNotSupported(
			"CREATE SUBSCRIPTION is not supported: creating replication subscriptions is not permitted through the connection pooler")

	// -- Foreign tables: allowed, except file_fdw's `program` option, which runs
	// a shell command on the server — the same capability COPY ... PROGRAM and
	// pg_execute_server_program are refused for, so it is refused here too
	// regardless of the role's pg_execute_server_program membership. `filename`
	// is file_fdw's purpose and stays governed by pg_read_server_files.
	case ast.T_CreateForeignTableStmt:
		return rejectForeignTableProgramOption(stmt.(*ast.CreateForeignTableStmt).Options)

	case ast.T_AlterTableStmt:
		// Checked for every object type: PostgreSQL accepts ALTER TABLE on a
		// foreign table, and an OPTIONS subcommand only exists for one.
		at := stmt.(*ast.AlterTableStmt)
		if at.Cmds == nil {
			return nil
		}
		for _, item := range at.Cmds.Items {
			cmd, ok := item.(*ast.AlterTableCmd)
			if !ok || cmd.Subtype != ast.AT_GenericOptions {
				continue
			}
			opts, _ := cmd.Def.(*ast.NodeList)
			if err := rejectForeignTableProgramOption(opts); err != nil {
				return err
			}
		}
		return nil

	default:
		return nil
	}
}

// rejectForeignTableProgramOption refuses a foreign-table OPTIONS list that
// sets (or adds) the file_fdw `program` option. DROP program is a revert and is
// allowed.
func rejectForeignTableProgramOption(opts *ast.NodeList) error {
	if opts == nil {
		return nil
	}
	for _, item := range opts.Items {
		def, ok := item.(*ast.DefElem)
		if !ok || def.Defaction == ast.DEFELEM_DROP {
			continue
		}
		if strings.EqualFold(def.Defname, "program") {
			return mterrors.NewFeatureNotSupported(
				"foreign table option \"program\" is not supported: executing server programs is not permitted through the connection pooler")
		}
	}
	return nil
}
