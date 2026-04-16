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

// planUnsupportedStmt checks whether a statement is unsafe to execute through
// the connection pooler and returns an appropriate feature_not_supported error.
//
// Two categories of statements are rejected:
//
//  1. Statements containing embedded code that the pooler cannot parse or verify
//     (DO blocks, CREATE FUNCTION/PROCEDURE, CREATE TRIGGER, CREATE RULE,
//     CREATE EVENT TRIGGER).
//
//  2. Statements that are unsafe for hosted infrastructure (LOAD, ALTER SYSTEM,
//     CREATE/DROP DATABASE, CREATE LANGUAGE, CREATE SUBSCRIPTION,
//     CREATE FOREIGN DATA WRAPPER, CREATE FOREIGN SERVER).
//
// Returns a *mterrors.PgDiagnostic with SQLSTATE 0A000 (feature_not_supported)
// if the statement is blocked, or nil if the statement is allowed.
func planUnsupportedStmt(stmt ast.Stmt) error {
	switch stmt.NodeTag() {
	// -- Tier 1: statements containing unparsed/unverifiable code --

	case ast.T_DoStmt:
		return mterrors.NewFeatureNotSupported(
			"DO blocks are not supported: anonymous code blocks execute unparsed queries that cannot be verified by the connection pooler")

	case ast.T_CreateFunctionStmt:
		kind := "FUNCTION"
		if stmt.(*ast.CreateFunctionStmt).IsProcedure {
			kind = "PROCEDURE"
		}
		return mterrors.NewFeatureNotSupported(
			"CREATE " + kind + " is not supported: function and procedure bodies contain code that cannot be verified by the connection pooler")

	case ast.T_CreateTriggerStmt:
		return mterrors.NewFeatureNotSupported(
			"CREATE TRIGGER is not supported: triggers execute functions on data events that cannot be verified by the connection pooler")

	case ast.T_RuleStmt:
		return mterrors.NewFeatureNotSupported(
			"CREATE RULE is not supported: rules rewrite queries in ways that cannot be verified by the connection pooler")

	case ast.T_CreateEventTrigStmt:
		return mterrors.NewFeatureNotSupported(
			"CREATE EVENT TRIGGER is not supported: event triggers execute functions on DDL events that cannot be verified by the connection pooler")

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

	case ast.T_CreateFdwStmt:
		return mterrors.NewFeatureNotSupported(
			"CREATE FOREIGN DATA WRAPPER is not supported through the connection pooler")

	case ast.T_CreateForeignServerStmt:
		return mterrors.NewFeatureNotSupported(
			"CREATE SERVER is not supported: creating foreign server connections is not permitted through the connection pooler")

	default:
		return nil
	}
}
