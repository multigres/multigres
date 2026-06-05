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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// DiscardAllPrimitive handles DISCARD ALL entirely at the gateway.
//
// PostgreSQL documents DISCARD ALL as equivalent to:
//
//	CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ALL;
//	DEALLOCATE ALL; UNLISTEN *; SELECT pg_advisory_unlock_all();
//	DISCARD PLANS; DISCARD TEMP;
//
// and rejects it inside a transaction block (see PreventInTransactionBlock in
// PG's discard.c).
//
// We intentionally do NOT forward DISCARD ALL to a pooled backend. In the
// pooled architecture a client's session state lives at the gateway, not on
// any single backend: prepared statements (the consolidator), session GUCs
// (SessionSettings + gateway-managed variables), and LISTEN subscriptions are
// all gateway-side. The only backend-resident session state is whatever a
// *reserved* connection holds — an open transaction, temp tables, or
// `DECLARE … WITH HOLD` cursors — and that is cleaned up by handing the
// reserved connection back to the pool.
//
// Forwarding the literal DISCARD ALL to a shared pooled backend would run
// DEALLOCATE ALL on it, wiping the prepared statements the multipooler still
// believes are prepared there (its per-connection tracking is not updated),
// poisoning that backend for later sessions. Prepared statements already
// parsed on pooled backends are therefore left in place: they form a
// pool-wide dedup cache keyed by canonical (query, paramTypes), and leaving
// them keeps the multipooler's connState accurate. Only the gateway's
// per-session view (the consolidator) is cleared, which is what frees the
// client's statement names.
//
// Out of scope: the `SELECT pg_advisory_unlock_all()` half of PG's DISCARD ALL
// is NOT handled here. Session-level advisory locks live on whatever backend
// executed pg_advisory_lock, and the reserved-connection release rolls back
// (which only drops transaction-level locks) rather than running
// pg_advisory_unlock_all on the backend. Session-level advisory locks are a
// pre-existing limitation under pooling and are not addressed by this
// primitive.
type DiscardAllPrimitive struct {
	// Query is the original SQL string ("DISCARD ALL").
	Query string
}

// NewDiscardAllPrimitive creates a new DiscardAllPrimitive.
func NewDiscardAllPrimitive(sql string) *DiscardAllPrimitive {
	return &DiscardAllPrimitive{Query: sql}
}

// StreamExecute resets the gateway's session state and releases any reserved
// connection back to the pool, mirroring PG's DISCARD ALL without forwarding
// the statement to a shared pooled backend.
func (d *DiscardAllPrimitive) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// PG rejects DISCARD ALL inside a transaction block — the idea is to
	// catch mistakes that would otherwise leave the transaction uncommitted.
	// conn.IsInTransaction() covers a deferred BEGIN too (the status flips at
	// BEGIN even before the backend call), so the reserved connection below
	// is only ever held for temp tables / WITH HOLD cursors here.
	if conn.IsInTransaction() {
		return mterrors.NewPgError("ERROR", mterrors.PgSSActiveTransaction,
			"DISCARD ALL cannot run inside a transaction block", "")
	}

	// CLOSE ALL + DISCARD TEMP + rollback of a reserved backend — release any
	// reserved connection regardless of reason. ReleaseAllReservedConnections
	// rolls back, discards temp tables, releases portals, returns the backend
	// to the pool clean, and clears local shard state.
	//
	// This is done FIRST because it is the only step that can fail (it makes an
	// RPC to the multipooler). The gateway-side resets below are pure in-memory
	// state mutations that cannot fail, so running the fallible release up front
	// keeps DISCARD ALL effectively atomic: if the release errors we bail with
	// the client's session untouched rather than half-reset.
	if err := exec.ReleaseAllReservedConnections(ctx, conn, state); err != nil {
		return err
	}
	// Clear the gateway's HOLD cursor bookkeeping to match the released backend.
	state.ClearOpenHoldCursors()

	// DEALLOCATE ALL — drop the session's prepared statements from the
	// consolidator (same path DEALLOCATE ALL uses). Pooled backends keep their
	// canonical statements; only the client-facing names are released.
	if err := conn.Handler().HandleClose(ctx, conn, 'A', ""); err != nil {
		return err
	}

	// RESET ALL + SET SESSION AUTHORIZATION DEFAULT — clear every SET-tracked
	// GUC and the gateway-managed variables (e.g. statement_timeout). The next
	// query replays an empty session-settings set, resetting the backend.
	state.ResetAllSessionVariables()
	state.ResetStatementTimeout()

	// UNLISTEN * — drop all LISTEN subscriptions. Not in a transaction here, so
	// apply immediately rather than queueing as a pending action.
	if len(state.GetListenChannels()) > 0 {
		state.ClearListenChannels()
		if state.SubSync != nil {
			state.SubSync.SyncSubscriptions(conn.Context(), conn, state, nil, nil, true)
		}
	}

	return callback(ctx, &sqltypes.Result{CommandTag: "DISCARD ALL"})
}

// PortalStreamExecute satisfies the Primitive interface for the
// extended-protocol path. DISCARD ALL carries no parameter binds and its
// effects are purely on gateway session state and reserved-connection cleanup.
// Delegate to StreamExecute.
func (d *DiscardAllPrimitive) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return d.StreamExecute(ctx, exec, conn, state, nil, callback)
}

// GetTableGroup returns empty string — DISCARD ALL does not target a tablegroup.
func (d *DiscardAllPrimitive) GetTableGroup() string {
	return ""
}

// GetQuery returns the SQL query.
func (d *DiscardAllPrimitive) GetQuery() string {
	return d.Query
}

// String returns a description of the primitive for debugging.
func (d *DiscardAllPrimitive) String() string {
	return fmt.Sprintf("DiscardAll(%s)", d.Query)
}

// Ensure DiscardAllPrimitive implements Primitive interface.
var _ Primitive = (*DiscardAllPrimitive)(nil)
