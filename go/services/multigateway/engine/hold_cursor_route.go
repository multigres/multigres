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

// HoldCursorRoute routes a `DECLARE ... WITH HOLD` cursor declaration through
// a reserved connection. It signals ScatterConn (via PlanExecInfo.PinPortals)
// that the cursor name must be pinned on the backend so the cursor survives the
// transaction's COMMIT — at which point the multipooler would otherwise
// release the backend to the pool and the next FETCH would land on a
// different connection.
//
// Non-HOLD `DECLARE` is intentionally left to the default Route: those cursors
// are transaction-scoped and die at COMMIT alongside the existing
// ReasonTransaction reservation, so no portal pinning is required.
type HoldCursorRoute struct {
	TableGroup string
	Shard      string
	Query      string
	CursorName string
}

// NewHoldCursorRoute creates a HoldCursorRoute for the given cursor name.
func NewHoldCursorRoute(tableGroup, shard, sql, cursorName string) *HoldCursorRoute {
	return &HoldCursorRoute{
		TableGroup: tableGroup,
		Shard:      shard,
		Query:      sql,
		CursorName: cursorName,
	}
}

// StreamExecute schedules the cursor name for portal pinning, runs the
// DECLARE on a reserved connection, and on success records the cursor as an
// open HOLD cursor on the gateway session.
//
// Pin lifecycle on failure: the gateway only records the cursor in
// OpenHoldCursors after DECLARE returns nil. The multipooler-side
// streamExecuteOnReservedConn rolls back any pin it just registered when
// QueryStreaming reports an error (releasing the connection if that was
// the last reservation reason), so both sides stay in sync without
// relying on ROLLBACK / session-end cleanup to drain a stale pin.
func (h *HoldCursorRoute) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// info.PinPortals is set by the planner (the cursor name is known at plan
	// time); forward it to the reservation, then record the cursor as open on
	// success.
	if err := exec.StreamExecute(ctx, conn, h.TableGroup, h.Shard, h.Query, nil, state, info, false, callback); err != nil {
		return err
	}
	state.AddOpenHoldCursor(h.CursorName)
	return nil
}

// PortalStreamExecute satisfies the extended-protocol path. DECLARE is rarely
// prepared, but composing HoldCursorRoute into a Sequence shouldn't silently
// lose work — delegate.
func (h *HoldCursorRoute) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return h.StreamExecute(ctx, exec, conn, state, nil, info, callback)
}

func (h *HoldCursorRoute) GetTableGroup() string { return h.TableGroup }

func (h *HoldCursorRoute) GetQuery() string { return h.Query }

func (h *HoldCursorRoute) String() string {
	return fmt.Sprintf("HoldCursorRoute(%s: %s)", h.CursorName, h.Query)
}

var _ Primitive = (*HoldCursorRoute)(nil)
