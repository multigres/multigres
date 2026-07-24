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
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// ReservationAware chooses the backend-authoritative path only when this
// logical session already owns a reserved backend. Unreserved sessions keep the
// pooling-friendly local/replay path.
type ReservationAware struct {
	Query      string
	Reserved   Primitive
	Unreserved Primitive
}

func NewReservationAware(query string, reserved, unreserved Primitive) *ReservationAware {
	return &ReservationAware{Query: query, Reserved: reserved, Unreserved: unreserved}
}

func (r *ReservationAware) primitive(state *handler.MultigatewayConnectionState) Primitive {
	if state != nil && state.HasReservedConnection() {
		return r.Reserved
	}
	return r.Unreserved
}

func (r *ReservationAware) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	bindVars []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return r.primitive(state).StreamExecute(ctx, exec, conn, state, bindVars, info, callback)
}

func (r *ReservationAware) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	includeDescribe bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return r.primitive(state).PortalStreamExecute(ctx, exec, conn, state, portalInfo, maxRows, includeDescribe, info, callback)
}

func (r *ReservationAware) GetTableGroup() string { return r.Reserved.GetTableGroup() }
func (r *ReservationAware) GetQuery() string      { return r.Query }
func (r *ReservationAware) String() string {
	return fmt.Sprintf("ReservationAware(reserved=%s, unreserved=%s)", r.Reserved.String(), r.Unreserved.String())
}

var _ Primitive = (*ReservationAware)(nil)
