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
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// ValidateSetting validates a SET's value against PostgreSQL without persisting
// it on the pooled backend.
//
// It runs `SELECT pg_catalog.set_config(name, value, true)` on a backend. The
// is_local := true argument scopes the change to the statement's own implicit
// transaction, so it reverts the instant the statement completes — leaving the
// backend's session state (and therefore multipooler's per-backend connstate)
// exactly as it was. multipooler must remain the sole authority on backend
// session GUCs; routing a raw `SET name = value` here would mutate the backend
// behind its back and break a later RESET (the pooler wouldn't know to reset
// the orphaned value). set_config still validates the value, so an invalid name
// or out-of-range value raises the same error a real SET would, surfacing it at
// SET time rather than on a later unrelated query.
//
// The result row is discarded; this primitive emits nothing to the client. It
// is the first step of Sequence[ValidateSetting, ApplySessionState]: the
// trailing ApplySessionState records the setting for pool-rotation replay and
// emits the synthetic CommandComplete("SET"), and runs only if validation
// succeeded because the Sequence stops on the first child's error.
type ValidateSetting struct {
	TableGroup string
	Shard      string
	// Name and Value are the SET's variable name and value (already unquoted by
	// the parser); they are re-quoted as SQL string literals in validateSQL.
	Name  string
	Value string
	// Query is the original SQL string, for debug output.
	Query string
}

// NewValidateSetting creates a ValidateSetting primitive.
func NewValidateSetting(tableGroup, shard, name, value, sql string) *ValidateSetting {
	return &ValidateSetting{
		TableGroup: tableGroup,
		Shard:      shard,
		Name:       name,
		Value:      value,
		Query:      sql,
	}
}

// validateSQL deparses `SELECT pg_catalog.set_config('<name>', '<value>', true)`
// from an AST rather than formatting a string. Building the tree and letting the
// deparser render it means the name and value are quoted/escaped by the
// canonical path (ast.QuoteStringLiteral), so a single quote in either — a
// hostile variable name or value — cannot break out of the string literal.
// is_local is true so the validation reverts when the statement completes.
func (v *ValidateSetting) validateSQL() string {
	funcname := ast.NewNodeList(ast.NewString("pg_catalog"), ast.NewString("set_config"))
	args := ast.NewNodeList(
		ast.NewA_Const(ast.NewString(v.Name), 0),
		ast.NewA_Const(ast.NewString(v.Value), 0),
		ast.NewA_Const(ast.NewBoolean(true), 0),
	)
	sel := ast.NewSelectStmt()
	sel.TargetList.Append(ast.NewResTarget("", ast.NewFuncCall(funcname, args, 0)))
	return sel.SqlString()
}

// discardResults swallows result chunks: only an execution error matters here.
func discardResults(context.Context, *sqltypes.Result) error { return nil }

// StreamExecute runs the validation query on a backend and propagates any error,
// discarding the result row. bindVars/callback are unused: the validation SQL is
// fully formed from the literal name/value, and this step is silent.
func (v *ValidateSetting) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	_ func(context.Context, *sqltypes.Result) error,
) error {
	return exec.StreamExecute(ctx, conn, v.TableGroup, v.Shard, v.validateSQL(), nil, state, discardResults)
}

// PortalStreamExecute mirrors StreamExecute. The validation SQL carries no
// parameters, so the portal's binds are irrelevant.
func (v *ValidateSetting) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	_ func(context.Context, *sqltypes.Result) error,
) error {
	return exec.StreamExecute(ctx, conn, v.TableGroup, v.Shard, v.validateSQL(), nil, state, discardResults)
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
