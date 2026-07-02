// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/tools/sortedmaps"
)

// PgListeningChannels returns the gateway-managed LISTEN channel set for the
// current logical client session. PostgreSQL's pg_listening_channels() reflects
// backend-local LISTEN state; multigateway owns LISTEN/NOTIFY state at the
// gateway, so routing the function to an arbitrary pooled backend would return
// the wrong answer. This primitive virtualizes the supported simple forms
// without pinning the session to a backend.
type PgListeningChannels struct {
	sql string
}

// NewPgListeningChannels creates a pg_listening_channels() primitive.
func NewPgListeningChannels(sql string) *PgListeningChannels {
	return &PgListeningChannels{sql: sql}
}

// StreamExecute returns the committed gateway LISTEN channels. Pending LISTEN /
// UNLISTEN actions inside an open transaction intentionally do not appear until
// COMMIT, matching PostgreSQL's LISTEN transaction semantics.
func (p *PgListeningChannels) StreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	channels := sortedmaps.Keys(state.GetListenChannels())

	rows := make([]*sqltypes.Row, 0, len(channels))
	for _, ch := range channels {
		rows = append(rows, &sqltypes.Row{Values: []sqltypes.Value{[]byte(ch)}})
	}

	return callback(ctx, &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name:         "pg_listening_channels",
			Type:         "text",
			DataTypeOid:  uint32(ast.TEXTOID),
			DataTypeSize: -1,
			TypeModifier: -1,
			Format:       0,
		}},
		Rows:         rows,
		RowsAffected: uint64(len(rows)),
		CommandTag:   fmt.Sprintf("SELECT %d", len(rows)),
	})
}

// PortalStreamExecute satisfies the Primitive interface for the extended query
// path. The supported pg_listening_channels() forms carry no parameters, so the
// result is identical to simple-protocol execution.
func (p *PgListeningChannels) PortalStreamExecute(
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
	return p.StreamExecute(ctx, exec, conn, state, nil, PlanExecInfo{}, callback)
}

func (p *PgListeningChannels) GetTableGroup() string { return "" }
func (p *PgListeningChannels) GetQuery() string      { return p.sql }
func (p *PgListeningChannels) String() string        { return "PgListeningChannels" }

var _ Primitive = (*PgListeningChannels)(nil)
