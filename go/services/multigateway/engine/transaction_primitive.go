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

// TransactionPrimitive handles transaction control statements (BEGIN, COMMIT, ROLLBACK).
//
// Key behaviors:
//   - BEGIN: Deferred execution - sets TxState to InTransaction and returns a synthetic
//     result without making a backend call. The actual BEGIN is sent with the first
//     real query via ReserveStreamExecute.
//   - COMMIT: Releases all reserved connections with COMMIT action, resets transaction state.
//   - ROLLBACK: Releases all reserved connections with ROLLBACK action, resets transaction state.
type TransactionPrimitive struct {
	// Kind is the type of transaction statement (BEGIN, COMMIT, ROLLBACK, etc.)
	Kind ast.TransactionStmtKind

	// Query is the original Query string for this statement.
	Query string

	// TableGroup is the target tablegroup (used for COMMIT/ROLLBACK routing).
	TableGroup string
}

// NewTransactionPrimitive creates a new TransactionPrimitive.
func NewTransactionPrimitive(kind ast.TransactionStmtKind, sql, tableGroup string) *TransactionPrimitive {
	return &TransactionPrimitive{
		Kind:       kind,
		Query:      sql,
		TableGroup: tableGroup,
	}
}

// StreamExecute executes the transaction primitive.
func (t *TransactionPrimitive) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	switch t.Kind {
	case ast.TRANS_STMT_BEGIN, ast.TRANS_STMT_START:
		return t.executeBegin(ctx, state, callback)

	case ast.TRANS_STMT_COMMIT:
		return t.executeCommit(ctx, exec, conn, state, callback)

	case ast.TRANS_STMT_ROLLBACK:
		return t.executeRollback(ctx, exec, conn, state, callback)

	default:
		// For other transaction statements (SAVEPOINT, etc.), pass through to backend
		return exec.StreamExecute(ctx, conn, t.TableGroup, "", t.Query, state, callback)
	}
}

// executeBegin handles BEGIN/START TRANSACTION with deferred execution.
// Sets transaction state but doesn't send to backend - the actual BEGIN
// will be sent atomically with the first real query.
func (t *TransactionPrimitive) executeBegin(
	ctx context.Context,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// Set transaction state (deferred - no backend call yet)
	state.SetTransactionState(handler.TxStateInTransaction)

	// Return synthetic result to client
	return callback(ctx, &sqltypes.Result{
		CommandTag: "BEGIN",
	})
}

// executeCommit handles COMMIT by releasing reserved connections.
func (t *TransactionPrimitive) executeCommit(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// If no reserved connections, just return synthetic result
	// (empty transaction or deferred BEGIN that was never used)
	if len(state.ShardStates) == 0 {
		state.SetTransactionState(handler.TxStateIdle)
		return callback(ctx, &sqltypes.Result{
			CommandTag: "COMMIT",
		})
	}

	// TODO: Phase 5 - Use ReleaseReservedConnection RPC to commit on all shards
	// For now, execute COMMIT through normal path and reset state
	err := exec.StreamExecute(ctx, conn, t.TableGroup, "", "COMMIT", state, callback)

	// Reset transaction state regardless of error
	state.SetTransactionState(handler.TxStateIdle)
	// TODO: Clear reserved connections after Phase 5 implementation

	return err
}

// executeRollback handles ROLLBACK by releasing reserved connections.
func (t *TransactionPrimitive) executeRollback(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// If no reserved connections, just return synthetic result
	if len(state.ShardStates) == 0 {
		state.SetTransactionState(handler.TxStateIdle)
		return callback(ctx, &sqltypes.Result{
			CommandTag: "ROLLBACK",
		})
	}

	// TODO: Phase 5 - Use ReleaseReservedConnection RPC to rollback on all shards
	// For now, execute ROLLBACK through normal path and reset state
	err := exec.StreamExecute(ctx, conn, t.TableGroup, "", "ROLLBACK", state, callback)

	// Reset transaction state regardless of error
	state.SetTransactionState(handler.TxStateIdle)
	// TODO: Clear reserved connections after Phase 5 implementation

	return err
}

// GetTableGroup returns the target tablegroup.
func (t *TransactionPrimitive) GetTableGroup() string {
	return t.TableGroup
}

// GetQuery returns the Query query.
func (t *TransactionPrimitive) GetQuery() string {
	return t.Query
}

// String returns a description of the primitive for debugging.
func (t *TransactionPrimitive) String() string {
	return fmt.Sprintf("Transaction(%s)", t.Kind.String())
}

// Ensure TransactionPrimitive implements Primitive interface.
var _ Primitive = (*TransactionPrimitive)(nil)
