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

// EnableUnsafeConnection is the gateway-local primitive for
// `SET multigres.unsafe_connection = on`. It latches the connection into
// unsafe connection (a one-way switch — see server.Conn.LatchUnsafeConnection)
// and replies with a bare "SET" CommandComplete. It never touches a backend: the
// flag is a gateway control, and every subsequent statement reads it to suppress
// the unsafe-statement rejections and pin+quarantine the backend.
type EnableUnsafeConnection struct {
	sql string
}

// NewEnableUnsafeConnection creates the latch primitive for a validated
// `SET multigres.unsafe_connection = on`.
func NewEnableUnsafeConnection(sql string) *EnableUnsafeConnection {
	return &EnableUnsafeConnection{sql: sql}
}

// StreamExecute latches unsafe connection on the connection and sends the
// CommandComplete.
func (e *EnableUnsafeConnection) StreamExecute(
	ctx context.Context,
	_ IExecute,
	conn *server.Conn,
	_ *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	conn.LatchUnsafeConnection()
	return callback(ctx, &sqltypes.Result{CommandTag: "SET"})
}

// PortalStreamExecute satisfies the Primitive interface for the extended
// protocol; the statement carries no binds, so it delegates to StreamExecute.
func (e *EnableUnsafeConnection) PortalStreamExecute(
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
func (e *EnableUnsafeConnection) GetTableGroup() string { return "" }

// GetQuery returns empty: this primitive executes no backend query.
func (e *EnableUnsafeConnection) GetQuery() string { return "" }

// String returns a description for logging/debugging.
func (e *EnableUnsafeConnection) String() string {
	return "EnableUnsafeConnection(" + e.sql + ")"
}
