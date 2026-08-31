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

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// EnableDirectConnection is the gateway-local primitive for
// `SET multigres.direct_connection = on`. It latches the connection into
// direct connection (a one-way switch — see server.Conn.LatchDirectConnection)
// and replies with a bare "SET" CommandComplete. It never touches a backend: the
// flag is a gateway control, and every subsequent statement reads it to suppress
// the unsafe-statement rejections and pin+quarantine the backend. The planner
// builds this only after the superuser gate passes.
type EnableDirectConnection struct {
	sql string
}

// NewEnableDirectConnection creates the latch primitive for a validated,
// superuser-authorized `SET multigres.direct_connection = on`.
func NewEnableDirectConnection(sql string) *EnableDirectConnection {
	return &EnableDirectConnection{sql: sql}
}

// StreamExecute latches direct connection on the connection and sends the
// CommandComplete.
func (e *EnableDirectConnection) StreamExecute(
	ctx context.Context,
	_ IExecute,
	conn *server.Conn,
	_ *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	conn.LatchDirectConnection()
	return callback(ctx, &sqltypes.Result{CommandTag: "SET"})
}

// PortalStreamExecute satisfies the Primitive interface for the extended
// protocol; the statement carries no binds, so it delegates to StreamExecute.
func (e *EnableDirectConnection) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return e.StreamExecute(ctx, exec, conn, state, nil, PlanExecInfo{}, callback)
}

// GetTableGroup returns empty: this primitive targets no backend.
func (e *EnableDirectConnection) GetTableGroup() string { return "" }

// GetQuery returns empty: this primitive executes no backend query.
func (e *EnableDirectConnection) GetQuery() string { return "" }

// String returns a description for logging/debugging.
func (e *EnableDirectConnection) String() string {
	return "EnableDirectConnection(" + e.sql + ")"
}
