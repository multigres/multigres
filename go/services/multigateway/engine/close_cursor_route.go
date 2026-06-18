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

// CloseCursorRoute routes a `CLOSE <name>` or `CLOSE ALL` statement. It runs
// the CLOSE on the existing reserved backend (the one pinned by the matching
// `DECLARE ... WITH HOLD`), then asks the multipooler to unpin the cursor
// from its portal set via ReservationOptions.ReleasePortalNames. If the
// release drains the last ReasonPortal bit and no other reasons remain, the
// multipooler returns a zero ReservedState and ScatterConn drops the
// reservation locally.
//
// CLOSE ALL captures the set of tracked HOLD cursors at execution time
// (inside StreamExecute), not at plan time — so a cursor declared after the
// plan was built but before execution is still released. Non-HOLD cursors
// are unaffected: they die at COMMIT alongside ReasonTransaction and never
// appear in the portal set.
//
// If the named cursor is not a tracked HOLD cursor, the statement is still
// forwarded to PostgreSQL (so the server-side cursor — including
// transaction-scoped or extended-protocol ones — is closed properly) but no
// release request is sent: there is no pin to unwind.
type CloseCursorRoute struct {
	TableGroup string
	Shard      string
	Query      string

	// CursorName is the explicit cursor named in `CLOSE <name>`. Empty
	// when CloseAll is true.
	CursorName string

	// CloseAll is true for `CLOSE ALL`. When set, the route releases every
	// HOLD cursor on the session.
	CloseAll bool
}

// NewCloseCursorRoute creates a CloseCursorRoute for `CLOSE <name>`.
func NewCloseCursorRoute(tableGroup, shard, sql, cursorName string) *CloseCursorRoute {
	return &CloseCursorRoute{
		TableGroup: tableGroup,
		Shard:      shard,
		Query:      sql,
		CursorName: cursorName,
	}
}

// NewCloseAllCursorRoute creates a CloseCursorRoute for `CLOSE ALL`.
func NewCloseAllCursorRoute(tableGroup, shard, sql string) *CloseCursorRoute {
	return &CloseCursorRoute{
		TableGroup: tableGroup,
		Shard:      shard,
		Query:      sql,
		CloseAll:   true,
	}
}

// StreamExecute schedules portal releases for any HOLD cursors that match the
// CLOSE target, runs the CLOSE on the backend, and on success drops the
// gateway's HOLD-cursor bookkeeping.
//
// Release lifecycle on failure: the release names ride on this call's
// PlanExecInfo, and the multipooler applies portal releases only after the
// query succeeds (see streamExecuteOnReservedConn). A CLOSE error therefore
// leaves OpenHoldCursors still populated (we only call RemoveOpenHoldCursor
// after a successful CLOSE) and the multipooler's pin set untouched. The
// server-side cursor stays open, so gateway tracking matches reality — no extra
// cleanup is required here.
func (c *CloseCursorRoute) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// The release set is computed from current session state (which the planner
	// doesn't have), so augment the threaded info at exec time rather than at
	// plan time.
	targets := c.targets(state)
	info.ReleasePortals = targets
	if err := exec.StreamExecute(ctx, conn, c.TableGroup, c.Shard, c.Query, nil, state, info, callback); err != nil {
		return err
	}
	for _, name := range targets {
		state.RemoveOpenHoldCursor(name)
	}
	return nil
}

// PortalStreamExecute satisfies the extended-protocol path; delegate.
func (c *CloseCursorRoute) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return c.StreamExecute(ctx, exec, conn, state, nil, info, callback)
}

func (c *CloseCursorRoute) GetTableGroup() string { return c.TableGroup }

func (c *CloseCursorRoute) GetQuery() string { return c.Query }

func (c *CloseCursorRoute) String() string {
	if c.CloseAll {
		return "CloseCursorRoute(ALL)"
	}
	return fmt.Sprintf("CloseCursorRoute(%s)", c.CursorName)
}

// targets resolves the CLOSE statement to the list of currently-tracked HOLD
// cursor names that should be unpinned on the multipooler.
func (c *CloseCursorRoute) targets(state *handler.MultiGatewayConnectionState) []string {
	if c.CloseAll {
		return state.OpenHoldCursorNames()
	}
	if state.HasOpenHoldCursor(c.CursorName) {
		return []string{c.CursorName}
	}
	return nil
}

var _ Primitive = (*CloseCursorRoute)(nil)
