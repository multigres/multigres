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
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// TempTableRoute routes a query that creates temporary objects through a
// reserved connection. It sets PendingTempTableReservation on the state
// so that ScatterConn's StreamExecute creates a reserved connection with
// ReasonTempTable, following the same pattern as transactions.
type TempTableRoute struct {
	TableGroup string
	Shard      string
	Query      string
}

// NewTempTableRoute creates a new TempTableRoute primitive.
func NewTempTableRoute(tableGroup, shard, sql string) *TempTableRoute {
	return &TempTableRoute{TableGroup: tableGroup, Shard: shard, Query: sql}
}

// StreamExecute sets the temp table reservation flag and delegates to
// StreamExecute. ScatterConn will see the flag and create a reserved
// connection with ReasonTempTable.
func (t *TempTableRoute) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	state.PendingTempTableReservation = true
	return exec.StreamExecute(ctx, conn, t.TableGroup, t.Shard, t.Query, state, callback)
}

// GetTableGroup returns the target tablegroup.
func (t *TempTableRoute) GetTableGroup() string { return t.TableGroup }

// GetQuery returns the SQL query.
func (t *TempTableRoute) GetQuery() string { return t.Query }

// String returns a description of the primitive for debugging.
func (t *TempTableRoute) String() string { return fmt.Sprintf("TempTableRoute(%s)", t.Query) }

// Ensure TempTableRoute implements Primitive interface.
var _ Primitive = (*TempTableRoute)(nil)
