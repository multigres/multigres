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

// AdvisoryLockRoute wraps an ordinary Route for a query that acquires a
// session-level advisory lock (pg_advisory_lock / pg_advisory_lock_shared and
// their try-variants). Before delegating execution to the inner Route, it sets
// PendingAdvisoryLockReservation on the state so ScatterConn creates (or
// promotes to) a reserved connection with ReasonSessionAdvisoryLock, following
// the same pattern as TempTableRoute.
//
// Wrapping the Route (rather than re-implementing routing) keeps bind-variable
// reconstruction in one place: `SELECT pg_advisory_lock(101)` is normalized to
// `SELECT pg_advisory_lock($1)` for plan caching, and the inner Route
// re-injects the literal at execution time.
//
// Session-level advisory locks live on the specific backend that executed the
// acquiring call and survive transaction boundaries, so the session must stay
// pinned to that backend until every such lock is released. The multipooler
// authoritatively decides when to unpin by probing pg_locks after subsequent
// statements (see the executor), so this primitive only establishes the pin.
type AdvisoryLockRoute struct {
	inner *Route
}

// NewAdvisoryLockRoute wraps an existing Route so the session is pinned for the
// lifetime of the session-level advisory lock the query acquires.
func NewAdvisoryLockRoute(inner *Route) *AdvisoryLockRoute {
	return &AdvisoryLockRoute{inner: inner}
}

// StreamExecute sets the advisory-lock reservation flag and delegates to the
// inner Route. ScatterConn sees the flag and reserves the connection with
// ReasonSessionAdvisoryLock.
func (a *AdvisoryLockRoute) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	bindVars []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	state.PendingAdvisoryLockReservation = true
	return a.inner.StreamExecute(ctx, exec, conn, state, bindVars, callback)
}

// PortalStreamExecute sets the advisory-lock reservation flag and delegates to
// the inner Route for the extended-protocol path.
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
	state.PendingAdvisoryLockReservation = true
	return a.inner.PortalStreamExecute(ctx, exec, conn, state, portalInfo, maxRows, includeDescribe, callback)
}

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
