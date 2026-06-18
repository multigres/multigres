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

const unloggedDocHint = "See docs/query_serving/unlogged_tables.md."

// UnloggedWarning emits a NoticeResponse warning the client that an unlogged
// relation's contents are not replicated and are lost on failover. It produces no
// result of its own (no CommandTag) and is composed ahead of the real CREATE route
// in a Sequence, so the client sees the WARNING immediately before the
// CommandComplete. The message/hint differ by relation kind (table vs sequence).
type UnloggedWarning struct {
	sql     string
	message string
	hint    string
}

// NewUnloggedTableWarning creates the warning for an unlogged table. On failover
// the post-promotion sweep drops the table (or leaves it empty if depended upon).
func NewUnloggedTableWarning(sql string) *UnloggedWarning {
	return &UnloggedWarning{
		sql:     sql,
		message: "unlogged table data is not replicated and is lost on failover",
		hint: "On failover the table is dropped, or left empty if other objects depend on it; " +
			"rebuild it from scratch. " + unloggedDocHint,
	}
}

// NewUnloggedSequenceWarning creates the warning for an unlogged sequence. Unlogged
// sequence state is not replicated, so on failover the sequence restarts from its
// initial value (the sweep does not drop sequences).
func NewUnloggedSequenceWarning(sql string) *UnloggedWarning {
	return &UnloggedWarning{
		sql:     sql,
		message: "unlogged sequence is reset to its start value on failover",
		hint:    "Unlogged sequence state is not replicated; a failover restarts it from its initial value. " + unloggedDocHint,
	}
}

// notice builds the WARNING-severity diagnostic. SQLSTATE 01000 is the generic
// PostgreSQL warning class.
func (u *UnloggedWarning) notice() *mterrors.PgDiagnostic {
	n := mterrors.NewPgNotice("WARNING", "01000", u.message, "")
	n.Hint = u.hint
	return n
}

// StreamExecute emits the warning notice. In the enclosing Sequence the wire order
// is NoticeResponse (here) followed by the CREATE's CommandComplete (next primitive).
func (u *UnloggedWarning) StreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	_ *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return callback(ctx, &sqltypes.Result{Notices: []*mterrors.PgDiagnostic{u.notice()}})
}

// PortalStreamExecute satisfies the Primitive interface for the extended-protocol
// path. The warning carries no parameters, so it delegates to StreamExecute.
func (u *UnloggedWarning) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return u.StreamExecute(ctx, exec, conn, state, nil, PlanExecInfo{}, callback)
}

// GetTableGroup returns empty string as this primitive doesn't target a tablegroup.
func (u *UnloggedWarning) GetTableGroup() string { return "" }

// GetQuery returns empty string as this primitive doesn't execute a query.
func (u *UnloggedWarning) GetQuery() string { return "" }

// String returns a description for logging/debugging.
func (u *UnloggedWarning) String() string {
	return fmt.Sprintf("UnloggedWarning(%s)", u.sql)
}

// Ensure UnloggedWarning implements Primitive interface.
var _ Primitive = (*UnloggedWarning)(nil)
