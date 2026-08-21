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

// SessionPinned reports whether a statement routed to tableGroup/shard will
// execute on a session-affine backend: inside an explicit transaction
// (including its deferred-BEGIN first statement, whose reservation is created
// on the routed target) or on a session already holding a reserved connection
// FOR THAT TARGET (temp tables, cursors, advisory locks). A pinned statement
// may mutate that backend's session state for real, because the backend stays
// with the logical session and moves in lockstep with the gateway map; an
// unpinned statement must never leave session state on a pooled backend.
//
// The target scoping matches ScatterConn's reservation reuse (a reservation is
// reused only when the shard state matches the statement's target), so callers
// must pass exactly the tablegroup/shard they hand to the Route they build.
func SessionPinned(conn *server.Conn, state *handler.MultigatewayConnectionState, tableGroup, shard string) bool {
	if conn != nil && conn.IsInTransaction() {
		return true
	}
	return state != nil && state.HasReservedConnectionFor(tableGroup, shard)
}

// StatementReservesBackend reports whether the statement itself reserves its
// backend for the session (via its own PlanExecInfo intent), even when the
// session was not already pinned. This matters for a set_config that shares a
// statement with a backend-pinning call — e.g.
// `SELECT set_config('x','y',false), pg_advisory_lock(1)`: SessionPinned is
// false at statement start, but this one statement reserves the backend, so its
// set_config must persist there for real (a reserved backend has no pool-replay
// path). Treating such a statement as pinned keeps the backend and the gateway
// map in lockstep.
func StatementReservesBackend(info PlanExecInfo) bool {
	return info.AdvisoryLock ||
		info.TempTable ||
		info.LogicalReplicationSlot ||
		info.SetSeed ||
		len(info.PinPortals) > 0
}

// SessionStateBranch chooses between two equivalent sub-plans at execute time
// based on whether the session is pinned to a session-affine backend (see
// SessionPinned). It exists so a plan whose correct shape depends on live
// session state can still be cached: both branches are built at plan time from
// the statement alone (no session state), and the per-execution decision is
// deferred to StreamExecute / PortalStreamExecute where the live conn and state
// are available.
//
// The motivating use is a session-persisting SELECT set_config(..., false),
// which mirrors an unpinned SET: on an unpinned session the routed set_config
// is rewritten to is_local := true so it reverts on the pooled backend and the
// value lives only in the gateway map (replayed at the next checkout); on a
// pinned session it routes for real so the reserved backend — which has no
// replay path — genuinely carries it. Both branches then track the value
// identically.
type SessionStateBranch struct {
	// TableGroup and Shard are the routing target the Pinned/Unpinned children
	// address; SessionPinned is evaluated against exactly this target so the
	// branch decision cannot drift from the reservation ScatterConn would reuse.
	TableGroup string
	Shard      string

	// Query is the original SQL string, for GetQuery/debug output.
	Query string

	// Pinned runs when the session is pinned to a session-affine backend;
	// Unpinned runs otherwise.
	Pinned   Primitive
	Unpinned Primitive
}

// NewSessionStateBranch creates a SessionStateBranch over the two sub-plans.
func NewSessionStateBranch(tableGroup, shard, sql string, pinned, unpinned Primitive) *SessionStateBranch {
	return &SessionStateBranch{
		TableGroup: tableGroup,
		Shard:      shard,
		Query:      sql,
		Pinned:     pinned,
		Unpinned:   unpinned,
	}
}

// choose picks the pinned branch when the set_config will land on a
// session-affine backend: the session is already pinned, OR this statement
// reserves its own backend (see StatementReservesBackend), OR reservesBackend
// is set by the caller for a path that reserves at runtime (a row-limited
// portal — the multipooler reserves any maxRows>0 portal for possible
// resumption). The unpinned branch (revert) is correct only when the set_config
// truly lands on a pooled backend that returns to the pool.
func (b *SessionStateBranch) choose(conn *server.Conn, state *handler.MultigatewayConnectionState, info PlanExecInfo, reservesBackend bool) Primitive {
	if reservesBackend || SessionPinned(conn, state, b.TableGroup, b.Shard) || StatementReservesBackend(info) {
		return b.Pinned
	}
	return b.Unpinned
}

// StreamExecute dispatches to the pinned or unpinned child for the simple-query
// path. A simple query runs to completion on a pooled backend (no runtime
// reservation), so only session/statement pinning forces the pinned branch.
func (b *SessionStateBranch) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	bindVars []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return b.choose(conn, state, info, false).StreamExecute(ctx, exec, conn, state, bindVars, info, callback)
}

// PortalStreamExecute dispatches to the pinned or unpinned child for the
// extended-protocol path. A row-limited portal (maxRows > 0) is reserved by the
// multipooler for possible resumption, so its set_config lands on a reserved
// backend and must persist for real — treat it as pinned.
func (b *SessionStateBranch) PortalStreamExecute(
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
	return b.choose(conn, state, info, maxRows > 0).PortalStreamExecute(ctx, exec, conn, state, portalInfo, maxRows, includeDescribe, info, callback)
}

// GetTableGroup returns the branch's routing target.
func (b *SessionStateBranch) GetTableGroup() string { return b.TableGroup }

// GetQuery returns the original SQL string.
func (b *SessionStateBranch) GetQuery() string { return b.Query }

// String returns a description for debugging.
func (b *SessionStateBranch) String() string {
	return "SessionStateBranch(pinned=" + b.Pinned.String() + ", unpinned=" + b.Unpinned.String() + ")"
}

// Ensure SessionStateBranch implements Primitive.
var _ Primitive = (*SessionStateBranch)(nil)
