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

// AdvisoryLockRoute wraps an ordinary Route for a query that touches
// session-level advisory locks — either acquiring one (pg_advisory_lock /
// pg_advisory_lock_shared and their try-variants) or releasing one
// (pg_advisory_unlock / _shared / _all). Before delegating execution to the
// inner Route it sets gateway state flags that ScatterConn turns into
// reservation options:
//
//   - pin == true (acquire): PendingAdvisoryLockReservation, so ScatterConn
//     creates (or promotes to) a reserved connection with
//     ReasonSessionAdvisoryLock — following the same pattern as TempTableRoute.
//   - always: PendingAdvisoryLockRecheck, so ScatterConn asks the multipooler to
//     re-probe pg_locks after the statement and unpin if no advisory lock
//     remains. This is what lets the (otherwise per-statement) probe run only on
//     statements that actually touch advisory locks.
//
// Wrapping the Route (rather than re-implementing routing) keeps bind-variable
// reconstruction in one place: `SELECT pg_advisory_lock(101)` is normalized to
// `SELECT pg_advisory_lock($1)` for plan caching, and the inner Route
// re-injects the literal at execution time.
//
// Session-level advisory locks live on the specific backend that executed the
// acquiring call and survive transaction boundaries, so the session must stay
// pinned to that backend until every such lock is released. The multipooler
// authoritatively decides when to unpin (by probing pg_locks); this primitive
// only establishes the pin and signals when a recheck is worthwhile.
type AdvisoryLockRoute struct {
	inner *Route
	// pin is true when the statement acquires a lock (reserve the backend);
	// false for a bare release (recheck only).
	pin bool
}

// NewAdvisoryLockRoute wraps an existing Route for a statement that touches
// session-level advisory locks. pin reserves the backend (set for acquisitions);
// a recheck is always requested.
func NewAdvisoryLockRoute(inner *Route, pin bool) *AdvisoryLockRoute {
	return &AdvisoryLockRoute{inner: inner, pin: pin}
}

// signal sets the gateway state flags consumed by ScatterConn before the inner
// Route runs.
func (a *AdvisoryLockRoute) signal(state *handler.MultiGatewayConnectionState) {
	if a.pin {
		state.PendingAdvisoryLockReservation = true
	}
	state.PendingAdvisoryLockRecheck = true
}

// StreamExecute sets the advisory-lock state flags and delegates to the inner
// Route.
func (a *AdvisoryLockRoute) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	bindVars []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	a.signal(state)
	return a.inner.StreamExecute(ctx, exec, conn, state, bindVars, callback)
}

// PortalStreamExecute sets the advisory-lock state flags and delegates to the
// inner Route for the extended-protocol path.
func (a *AdvisoryLockRoute) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	includeDescribe bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	a.signal(state)
	return a.inner.PortalStreamExecute(ctx, exec, conn, state, portalInfo, maxRows, includeDescribe, callback)
}

// Pins reports whether this route reserves the backend (true for an acquire,
// false for a bare release that only requests a recheck). Exposed for
// observability and tests.
func (a *AdvisoryLockRoute) Pins() bool { return a.pin }

// GetTableGroup returns the target tablegroup.
func (a *AdvisoryLockRoute) GetTableGroup() string { return a.inner.GetTableGroup() }

// GetQuery returns the SQL query.
func (a *AdvisoryLockRoute) GetQuery() string { return a.inner.GetQuery() }

// String returns a description of the primitive for debugging.
func (a *AdvisoryLockRoute) String() string {
	return fmt.Sprintf("AdvisoryLockRoute(%s)", a.inner.GetQuery())
}

// Ensure AdvisoryLockRoute implements Primitive interface.
var _ Primitive = (*AdvisoryLockRoute)(nil)
