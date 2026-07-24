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
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// rewriteGatewayManagedCurrentSetting rewrites every gateway-managed
// current_setting call in stmt so it returns the gateway-owned value instead of
// the pooled backend's local GUC. A GMV like statement_timeout is never applied
// to the backend (SET/set_config are handled at the gateway), so a
// current_setting('statement_timeout') evaluated on the backend would see the
// backend default, diverging from SHOW. Rewriting the call fixes that.
//
// Each matching call — current_setting('<gmv>'[, missing_ok]) with a literal
// gateway-managed name — is replaced, in a clone, by a synthetic bind slot ($N
// numbered past the highest existing param). GatewayManagedValueRoute fills that
// slot from gateway connection state at execute time (the same string SHOW
// returns), so the value is resolved per execution and the plan stays cacheable
// across value changes. Repeated reads of the same variable share one slot (and one
// gateway lookup), since current_setting is stable within a statement execution.
// missing_ok is irrelevant: a registered GMV always exists, so current_setting
// returns its value regardless (and never NULL).
//
// The name must be a literal for the call to be recognized as a GMV. A bound name
// (current_setting($1)) is not recognized at plan time and routes to the backend —
// the same documented gap as a bound set_config name. The normalizer keeps the
// name literal by skipping current_setting's arguments (see isPlannerLiteralFunc).
//
// The caller gates this on analysis.NeedsCurrentSettingRewrite, so it runs only
// when a rewrite is actually required — detection already happened on the analyze
// pass's walk (see analyzeFunctionCalls), and this does the single mutating walk
// rather than re-detecting first.
//
// Returns (rewrittenClone, reads, nil) when at least one call was rewritten;
// (nil, nil, nil) when there was nothing to rewrite (defensive — the caller does
// not invoke this unless the analyze flag promised a match).
func rewriteGatewayManagedCurrentSetting(stmt ast.Stmt) (ast.Stmt, []engine.GatewayManagedSettingRead, error) {
	if stmt == nil {
		return nil, nil, nil
	}

	// ast.Rewrite mutates in place, and stmt may be a cached plan's normalized tree
	// shared across executions, so clone before replacing anything.
	clone := ast.CloneNode(stmt).(ast.Stmt)

	// Synthetic slots are numbered past the highest param already in the statement
	// (client binds and any set_config synthetic slots already allocated), so they
	// can't collide with a real bind.
	next := ast.MaxParamRef(clone)

	var reads []engine.GatewayManagedSettingRead
	slotForName := map[string]int{}
	ast.Rewrite(clone, func(cursor *ast.Cursor) bool {
		fc, ok := cursor.Node().(*ast.FuncCall)
		if !ok {
			return true
		}
		name, ok := gatewayManagedCurrentSettingName(fc)
		if !ok {
			return true
		}
		// current_setting is stable within a statement execution, so every read of
		// the same variable resolves to one value — reuse a single slot (and one
		// read, i.e. one gateway lookup) across all occurrences of that name.
		param, seen := slotForName[name]
		if !seen {
			next++
			param = next
			slotForName[name] = param
			reads = append(reads, engine.GatewayManagedSettingRead{Param: param, Name: name})
		}
		// Preserve the output column name. Replacing the call with a bare projection
		// would otherwise report "?column?" for a target like `SELECT current_setting(...)`,
		// where PostgreSQL names the column "current_setting" (clients may key by it,
		// and a CREATE TABLE AS / SELECT INTO takes its column names from here). Only a
		// direct, unnamed target column is renamed: a wrapped call
		// (current_setting(...)::x, f(current_setting(...))) keeps its outer name, and a
		// SET/INSERT target already carries one.
		if rt, ok := cursor.Parent().(*ast.ResTarget); ok && rt.Name == "" {
			rt.Name = "current_setting"
		}
		// Fresh ParamRef node per occurrence so the clone doesn't share a node with
		// the original AST; the shared number makes them resolve to the same value.
		cursor.Replace(ast.NewParamRef(param, 0))
		return false
	}, nil)

	if len(reads) == 0 {
		return nil, nil, nil
	}

	return clone, reads, nil
}

// gatewayManagedCurrentSettingName returns the gateway-managed variable name a
// current_setting call reads, or ("", false) when fc is not a current_setting
// call over a literal gateway-managed name. current_setting takes one or two
// arguments (name[, missing_ok]); only the literal name is inspected.
func gatewayManagedCurrentSettingName(fc *ast.FuncCall) (string, bool) {
	if resolveFuncName(fc.Funcname) != "current_setting" {
		return "", false
	}
	if fc.Args == nil {
		return "", false
	}
	if n := fc.Args.Len(); n != 1 && n != 2 {
		return "", false
	}
	name, ok := constStringArg(fc.Args.Items[0])
	if !ok || !handler.IsGatewayManagedVariable(name) {
		return "", false
	}
	return name, true
}

// stmtRewritableForCurrentSetting reports whether stmt *evaluates* its
// current_setting calls for an immediate result — so rewriting them to the gateway
// value is correct — rather than *storing* the call as a definition that is
// re-evaluated later, where a rewrite would freeze the value to the creating
// session's. Rewritable:
//
//   - SELECT (including SELECT INTO) and INSERT/UPDATE/DELETE — evaluate the call
//     when the statement runs; SELECT INTO materializes the result once.
//   - CREATE TABLE AS — also materializes the result once, EXCEPT a materialized
//     view. CREATE MATERIALIZED VIEW shares this AST node (ObjType OBJECT_MATVIEW),
//     but its query is stored and re-run by REFRESH MATERIALIZED VIEW, so rewriting
//     would freeze it exactly like a plain view.
//
// Everything else returns false — most importantly CREATE VIEW, whose query is
// stored and re-evaluated on every read, so a rewritten current_setting must not be
// frozen into the definition.
func stmtRewritableForCurrentSetting(stmt ast.Stmt) bool {
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		return true
	case *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt:
		return true
	case *ast.CreateTableAsStmt:
		return s.ObjType != ast.OBJECT_MATVIEW
	}
	return false
}
