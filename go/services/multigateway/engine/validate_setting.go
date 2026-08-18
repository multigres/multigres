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

package engine

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/pgsettings"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// ValidateSetting runs a SET's value through PostgreSQL's own set_config() as
// a throwaway validation probe: `SELECT pg_catalog.set_config(name, value,
// true)` on a backend. The is_local := true argument scopes the change to the
// statement's own implicit transaction, so it reverts the instant the
// statement completes, leaving the backend's session state (and therefore
// multipooler's per-backend connstate) exactly as it was. The gateway must
// remain the sole authority on session GUCs; routing a raw `SET name = value`
// here would mutate a pooled backend behind its bookkeeping. set_config still
// validates the value, so an invalid name or out-of-range value raises the
// same error a real SET would, surfacing it at SET time rather than on a
// later unrelated query.
//
// The result row is discarded; this primitive emits nothing to the client. It
// is the first step of Sequence[ValidateSetting, ApplySessionState]: the
// trailing ApplySessionState records the setting (PostgreSQL's own confirmed
// value, captured from set_config's return) for pool-rotation replay and
// emits the synthetic CommandComplete("SET"), and runs only if this step
// succeeded because the Sequence stops on the first child's error.
type ValidateSetting struct {
	TableGroup string
	Shard      string
	// Name and Value are the SET's variable name and value (already unquoted by
	// the parser); they are re-quoted as SQL string literals in validateSQL.
	Name  string
	Value string
	// IsReset validates a RESET rather than a SET: the set_config value argument
	// is NULL, which resets the GUC to its default and returns that default, so a
	// reportable GUC's reverted value is captured the same way a SET's new value
	// is. Value is ignored when IsReset is true.
	IsReset bool
	// Query is the original SQL string, for debug output.
	Query string
}

// NewValidateSetting creates a ValidateSetting primitive for a SET.
func NewValidateSetting(tableGroup, shard, name, value, sql string) *ValidateSetting {
	return &ValidateSetting{
		TableGroup: tableGroup,
		Shard:      shard,
		Name:       name,
		Value:      value,
		Query:      sql,
	}
}

// NewValidateSettingReset creates a ValidateSetting primitive for a RESET. It
// runs set_config(name, NULL, true), which reverts the GUC to its default and
// returns that default, so a reportable GUC's reverted value can be reported.
func NewValidateSettingReset(tableGroup, shard, name, sql string) *ValidateSetting {
	return &ValidateSetting{
		TableGroup: tableGroup,
		Shard:      shard,
		Name:       name,
		IsReset:    true,
		Query:      sql,
	}
}

// validateSQL deparses `SELECT pg_catalog.set_config('<name>', '<value>', true)`
// from an AST rather than formatting a string. Building the tree and letting the
// deparser render it means the name and value are quoted/escaped by the
// canonical path (ast.QuoteStringLiteral), so a single quote in either — a
// hostile variable name or value — cannot break out of the string literal.
func (v *ValidateSetting) validateSQL() string {
	return buildSetConfigSQL(v.Name, v.Value, true, v.IsReset)
}

// buildSetConfigSQL deparses `SELECT pg_catalog.set_config('<name>', '<value>',
// <isLocal>)` from an AST rather than formatting a string, so the name and
// value are quoted/escaped by the canonical path (ast.QuoteStringLiteral) and
// a single quote in either cannot break out of the string literal. isReset
// passes NULL as the value (resets the GUC to its default and returns that
// default) instead of the literal value.
func buildSetConfigSQL(name, value string, isLocal, isReset bool) string {
	funcname := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("set_config"))
	// A RESET passes NULL as the value, which resets the GUC to its default and
	// returns that default; a SET passes the literal value.
	valueArg := ast.NewA_Const(ast.NewString(value), 0)
	if isReset {
		valueArg = ast.NewA_ConstNull(0)
	}
	args := ast.NewNodeList(
		ast.NewA_Const(ast.NewString(name), 0),
		valueArg,
		ast.NewA_Const(ast.NewBoolean(isLocal), 0),
	)
	sel := ast.NewSelectStmt()
	sel.TargetList.Append(ast.NewResTarget("", ast.NewFuncCall(funcname, args, 0)))
	return sel.SqlString()
}

// run executes the set_config query on a backend (validating and reverting,
// or persisting for real, depending on Persist). It captures the scalar the
// query returns: set_config's canonical, effective value for the GUC, and
// always records it on the Sequence exchange as ConfirmedValue so the
// trailing ApplySessionState can record PostgreSQL's actual resolved value
// into SessionSettings instead of the client's literal (e.g. DateStyle 'ISO'
// resolves to 'ISO, MDY'). When the GUC is also one PostgreSQL reports via
// ParameterStatus, the same captured value is additionally recorded under its
// ParameterStatus display name so the client learns the new value too. Nothing
// is emitted to the client here; only an execution error matters.
func (v *ValidateSetting) run(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	info PlanExecInfo,
) error {
	display, reportable := pgsettings.ReportableGUCName(v.Name)

	// captured distinguishes "probe returned a value" (possibly the legitimate
	// empty string, e.g. SET application_name = '') from "no value came back"
	// (zero rows, or a NULL scalar from a reset probe on a GUC with no
	// default). Only a captured value may be confirmed on the exchange —
	// otherwise the tracker falls back to the client's literal, its designed
	// fallback, instead of recording a phantom empty string.
	var effective string
	var captured bool
	capture := func(_ context.Context, result *sqltypes.Result) error {
		if len(result.Rows) > 0 && len(result.Rows[0].Values) > 0 {
			if val := result.Rows[0].Values[0]; !val.IsNull() {
				effective = string(val)
				captured = true
			}
		}
		return nil
	}

	// keepStructured is always true here: the query is a single-row, single-
	// column set_config() result (negligible size either way), and the
	// confirmed value must always be parsed now, not just for reportable GUCs.
	if err := exec.StreamExecute(ctx, conn, v.TableGroup, v.Shard, v.validateSQL(), nil, state, PlanExecInfo{}, true, capture); err != nil {
		return err
	}

	if info.Exchange != nil && captured {
		info.Exchange.SetConfirmedValue(effective)
		if reportable {
			info.Exchange.AddReportedSetting(display, effective)
		}
	}
	return nil
}

// StreamExecute runs the validation query on a backend and propagates any error.
// bindVars are unused: the validation SQL is fully formed from the literal
// name/value. The result row is captured (not emitted) — see validate.
func (v *ValidateSetting) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	info PlanExecInfo,
	_ func(context.Context, *sqltypes.Result) error,
) error {
	return v.run(ctx, exec, conn, state, info)
}

// PortalStreamExecute mirrors StreamExecute. The validation SQL carries no
// parameters, so the portal's binds are irrelevant.
func (v *ValidateSetting) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	info PlanExecInfo,
	_ func(context.Context, *sqltypes.Result) error,
) error {
	return v.run(ctx, exec, conn, state, info)
}

// GetTableGroup returns the target tablegroup.
func (v *ValidateSetting) GetTableGroup() string { return v.TableGroup }

// GetQuery returns the original SQL string.
func (v *ValidateSetting) GetQuery() string { return v.Query }

// String returns a description for debugging.
func (v *ValidateSetting) String() string {
	return fmt.Sprintf("ValidateSetting(%s=%s)", v.Name, v.Value)
}

// Ensure ValidateSetting implements Primitive.
var _ Primitive = (*ValidateSetting)(nil)
