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

// The gateway exposes its version two ways, mirroring PostgreSQL:
//
//   - `SHOW multigres_version` returns the short release version (like
//     `server_version`), served locally by the GatewayShowVersion primitive
//     below because postgres has no such GUC.
//   - `SELECT multigres.version()` returns the full build string (like
//     `version()`). It is NOT served here — the handler folds it into a text
//     literal before the query reaches the backend, so it works in any position
//     (see handler/gateway_functions.go).

// textField builds a single text result column with the given label.
func textField(name string) *query.Field {
	return &query.Field{
		Name:        name,
		Type:        "text",
		DataTypeOid: uint32(ast.TEXTOID),
	}
}

// GatewayShowVersion handles `SHOW multigres_version`, returning the gateway's
// short release version as a single-row result. The value is a process-wide
// constant, so it answers without a PostgreSQL round trip.
type GatewayShowVersion struct {
	sql string // Original SQL for debugging
}

// NewGatewayShowVersion creates the primitive for `SHOW multigres_version`.
func NewGatewayShowVersion(sql string) *GatewayShowVersion {
	return &GatewayShowVersion{sql: sql}
}

// result builds the single-row result. withFields controls whether the column
// metadata is attached: the simple protocol always needs it (one RowDescription
// precedes the row), but in the extended protocol RowDescription is delivered by
// Describe, so Execute must omit Fields unless a Describe('P') was folded into it.
func (g *GatewayShowVersion) result(withFields bool) *sqltypes.Result {
	result := &sqltypes.Result{
		Rows: []*sqltypes.Row{
			sqltypes.MakeRow([][]byte{[]byte(servenv.Version())}),
		},
		CommandTag: "SHOW",
	}
	if withFields {
		result.Fields = []*query.Field{textField(constants.MultigresVersionVariable)}
	}
	return result
}

// StreamExecute returns the version as a single-row result (simple protocol).
func (g *GatewayShowVersion) StreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	_ *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return callback(ctx, g.result(true))
}

// PortalStreamExecute serves the extended-protocol path. SHOW carries no
// parameter binds, so it just emits the row. It attaches column metadata only
// when includeDescribe is set (a folded Describe('P')); otherwise the
// RowDescription was already sent by the separate Describe and emitting Fields
// here would send an illegal second one mid-Execute.
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
	return callback(ctx, g.result(includeDescribe))
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

// IsMultigresVersionShow reports whether stmt is `SHOW multigres_version`, the
// gateway-only pseudo-variable served locally. The name compares
// case-insensitively, matching PostgreSQL's handling of unquoted (lowercased)
// and quoted (case-preserving) identifiers.
func IsMultigresVersionShow(stmt ast.Stmt) bool {
	show, ok := stmt.(*ast.VariableShowStmt)
	return ok && strings.ToLower(show.Name) == constants.MultigresVersionVariable
}

// MultigresVersionShowDescription is the extended-protocol Describe response for
// `SHOW multigres_version`: no bind parameters, one text column. It lets the
// gateway answer Describe locally, since postgres has no such GUC to describe.
func MultigresVersionShowDescription() *query.StatementDescription {
	return &query.StatementDescription{
		Parameters: []*query.ParameterDescription{},
		Fields:     []*query.Field{textField(constants.MultigresVersionVariable)},
		HasFields:  true,
	}
}
