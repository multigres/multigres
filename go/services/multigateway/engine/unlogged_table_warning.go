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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// UnloggedTableWarning emits a NoticeResponse warning the client that an unlogged
// table's data is never replicated and is lost on failover. It produces no result
// of its own (no CommandTag) and is composed ahead of the real CREATE route in a
// Sequence, so the client sees the WARNING immediately before the CommandComplete.
type UnloggedTableWarning struct {
	sql string
}

// NewUnloggedTableWarning creates an UnloggedTableWarning primitive.
func NewUnloggedTableWarning(sql string) *UnloggedTableWarning {
	return &UnloggedTableWarning{sql: sql}
}

// notice builds the WARNING-severity diagnostic. SQLSTATE 01000 is the generic
// PostgreSQL warning class.
func (u *UnloggedTableWarning) notice() *mterrors.PgDiagnostic {
	n := mterrors.NewPgNotice("WARNING", "01000",
		"unlogged table data is not replicated and is lost on failover", "")
	n.Hint = "On failover the table is dropped, or left empty if other objects depend on it; " +
		"rebuild it from scratch. See docs/query_serving/unlogged_tables.md."
	return n
}

// StreamExecute emits the warning notice. In the enclosing Sequence the wire order
// is NoticeResponse (here) followed by the CREATE's CommandComplete (next primitive).
func (u *UnloggedTableWarning) StreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	_ *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return callback(ctx, &sqltypes.Result{Notices: []*mterrors.PgDiagnostic{u.notice()}})
}

// PortalStreamExecute satisfies the Primitive interface for the extended-protocol
// path. The warning carries no parameters, so it delegates to StreamExecute.
func (u *UnloggedTableWarning) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return u.StreamExecute(ctx, exec, conn, state, nil, callback)
}

// GetTableGroup returns empty string as this primitive doesn't target a tablegroup.
func (u *UnloggedTableWarning) GetTableGroup() string { return "" }

// GetQuery returns empty string as this primitive doesn't execute a query.
func (u *UnloggedTableWarning) GetQuery() string { return "" }

// String returns a description for logging/debugging.
func (u *UnloggedTableWarning) String() string {
	return fmt.Sprintf("UnloggedTableWarning(%s)", u.sql)
}

// Ensure UnloggedTableWarning implements Primitive interface.
var _ Primitive = (*UnloggedTableWarning)(nil)
