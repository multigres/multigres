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
	"strings"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// multigresVersionField is the single result column returned by
// `SHOW multigres_version`. Both the executed result and the extended-protocol
// Describe must present identical column metadata, so they share this builder.
func multigresVersionField() *query.Field {
	return &query.Field{
		Name:        constants.MultigresVersionVariable,
		Type:        "text",
		DataTypeOid: uint32(ast.TEXTOID),
	}
}

// IsMultigresVersionShow reports whether stmt is `SHOW multigres_version`, the
// gateway-only pseudo-variable served locally by GatewayShowVersion. The name
// compares case-insensitively, matching PostgreSQL's handling of unquoted
// (lowercased) and quoted (case-preserving) identifiers.
func IsMultigresVersionShow(stmt ast.Stmt) bool {
	show, ok := stmt.(*ast.VariableShowStmt)
	return ok && strings.ToLower(show.Name) == constants.MultigresVersionVariable
}

// MultigresVersionDescription is the extended-protocol Describe response for
// `SHOW multigres_version`: no bind parameters and one text column. It lets the
// gateway answer Describe locally, since postgres has no such GUC to describe
// (forwarding would fail with "unrecognized configuration parameter").
func MultigresVersionDescription() *query.StatementDescription {
	return &query.StatementDescription{
		Parameters: []*query.ParameterDescription{},
		Fields:     []*query.Field{multigresVersionField()},
		HasFields:  true,
	}
}

// GatewayShowVersion handles `SHOW multigres_version`, returning the running
// multigateway's build identity as a single-row result. Unlike
// GatewayShowVariable it reads no per-connection state — the value is a
// process-wide constant — so it answers without a PostgreSQL round-trip. This
// lets a client discover which multigateway build it is connected to without
// shell access to run `--version`.
type GatewayShowVersion struct {
	sql string // Original SQL for debugging
}

// NewGatewayShowVersion creates a primitive that returns the multigateway
// version string.
func NewGatewayShowVersion(sql string) *GatewayShowVersion {
	return &GatewayShowVersion{sql: sql}
}

// versionResult builds the single-row SHOW result. withFields controls whether
// the column metadata is attached: the simple protocol always needs it (one
// RowDescription precedes the row), but in the extended protocol RowDescription
// is delivered by Describe, so Execute must omit Fields unless a Describe('P')
// was folded into it.
func (g *GatewayShowVersion) versionResult(withFields bool) *sqltypes.Result {
	result := &sqltypes.Result{
		Rows: []*sqltypes.Row{
			sqltypes.MakeRow([][]byte{[]byte(servenv.AppVersion())}),
		},
		CommandTag: "SHOW",
	}
	if withFields {
		result.Fields = []*query.Field{multigresVersionField()}
	}
	return result
}

// StreamExecute returns the version string as a single-row result, matching
// PostgreSQL's SHOW output format (one text column named after the variable).
func (g *GatewayShowVersion) StreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	_ *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return callback(ctx, g.versionResult(true))
}

// PortalStreamExecute serves the extended-protocol path. SHOW multigres_version
// carries no parameter binds, so it just emits the row. It attaches column
// metadata only when includeDescribe is set (a folded Describe('P')); otherwise
// the RowDescription was already sent by the separate Describe and emitting
// Fields here would send an illegal second one mid-Execute.
func (g *GatewayShowVersion) PortalStreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	_ *handler.MultigatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	includeDescribe bool,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return callback(ctx, g.versionResult(includeDescribe))
}

// GetTableGroup returns empty string as this primitive doesn't target a tablegroup.
func (g *GatewayShowVersion) GetTableGroup() string {
	return ""
}

// GetQuery returns empty string as this primitive doesn't execute a query.
func (g *GatewayShowVersion) GetQuery() string {
	return ""
}

// String returns a description for logging/debugging.
func (g *GatewayShowVersion) String() string {
	return fmt.Sprintf("GatewayShowVersion(%s)", g.sql)
}

// Ensure GatewayShowVersion implements Primitive interface.
var _ Primitive = (*GatewayShowVersion)(nil)
