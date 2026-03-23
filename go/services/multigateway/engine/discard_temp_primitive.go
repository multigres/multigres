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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// DiscardTempPrimitive handles DISCARD TEMP and DISCARD ALL statements.
//
// When the session is pinned (has temp tables), it uses the dedicated
// DiscardTempTables RPC to remove the temp table reservation reason on
// the multipooler side and sends the original SQL to PostgreSQL.
// For DISCARD ALL, it also resets gateway-side session state (GUCs,
// statement timeout) since PG resets those too.
// When the session is not pinned, it falls through to a regular
// StreamExecute since DISCARD on an unpinned session is harmless.
type DiscardTempPrimitive struct {
	// Query is the original SQL string (e.g., "DISCARD TEMP", "DISCARD ALL").
	Query string

	// TableGroup is the target tablegroup for routing.
	TableGroup string

	// Mode is the discard target from the AST (DISCARD_TEMP or DISCARD_ALL).
	Mode ast.DiscardMode
}

// NewDiscardTempPrimitive creates a new DiscardTempPrimitive.
func NewDiscardTempPrimitive(sql, tableGroup string, mode ast.DiscardMode) *DiscardTempPrimitive {
	return &DiscardTempPrimitive{
		Query:      sql,
		TableGroup: tableGroup,
		Mode:       mode,
	}
}

// StreamExecute executes the discard temp primitive.
func (d *DiscardTempPrimitive) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// If the session is pinned (has temp tables), use the dedicated RPC
	// to remove the temp table reason on the multipooler side.
	if state.SessionPinned {
		// Clear any deferred BEGIN — the session is being unpinned, so
		// a pending transaction start should not carry over.
		state.PendingBeginQuery = ""

		err := exec.DiscardTempTables(ctx, conn, state, d.Query, callback)
		if err != nil {
			return err
		}

		// Unpin the session now that the RPC succeeded. This ensures
		// correct behavior even if a later statement in a multi-statement
		// batch fails (the handler's post-execution check is skipped on error).
		state.SessionPinned = false

		// For DISCARD ALL, also reset gateway-side session state.
		// PG's DISCARD ALL runs RESET ALL (clears GUCs) and DEALLOCATE ALL
		// (drops prepared statements), so the gateway must stay in sync.
		if d.Mode == ast.DISCARD_ALL {
			state.ResetAllSessionVariables()
			state.ResetStatementTimeout()
		}

		return nil
	}

	// Session is not pinned — just execute via regular path.
	// DISCARD on an unpinned session is harmless.
	return exec.StreamExecute(ctx, conn, d.TableGroup, constants.DefaultShard, d.Query, state, callback)
}

// GetTableGroup returns the target tablegroup.
func (d *DiscardTempPrimitive) GetTableGroup() string {
	return d.TableGroup
}

// GetQuery returns the SQL query.
func (d *DiscardTempPrimitive) GetQuery() string {
	return d.Query
}

// String returns a description of the primitive for debugging.
func (d *DiscardTempPrimitive) String() string {
	return fmt.Sprintf("DiscardTemp(%s)", d.Query)
}

// Ensure DiscardTempPrimitive implements Primitive interface.
var _ Primitive = (*DiscardTempPrimitive)(nil)
