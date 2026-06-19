// Copyright 2025 Supabase, Inc.
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

package executor

import (
	"context"
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
)

// errTxFinished is returned when a query is issued on an InternalTx that has
// already been committed or rolled back.
var errTxFinished = errors.New("transaction already finished")

// InternalQueryService provides a simplified query interface for internal multipooler
// components. It uses the admin connection pool, which authenticates as the configured
// superuser (POSTGRES_USER / POSTGRES_PASSWORD).
type InternalQueryService interface {
	// Query executes a query and returns the result.
	Query(ctx context.Context, query string) (*sqltypes.Result, error)

	// QueryArgs executes a query with arguments and returns the result.
	// This is a convenience method that accepts Go values as arguments and converts
	// them to the appropriate text format for PostgreSQL.
	// Supported argument types: nil, string, []byte, int, int32, int64, *int64, uint32, uint64,
	// float32, float64, bool, and time.Time.
	QueryArgs(ctx context.Context, query string, args ...any) (*sqltypes.Result, error)

	// QueryMultiStatement executes a multi-statement query (e.g. "BEGIN; ...; COMMIT;")
	// using the simple query protocol. Unlike Query, it does not require exactly one result set.
	QueryMultiStatement(ctx context.Context, query string) error

	// Begin starts a transaction on a reserved connection and returns a handle
	// for issuing queries within it. Reserved connections are the mechanism for
	// holding a backend outside the regular pool's checkout/recycle cycle, which
	// is exactly what a multi-statement transaction needs: every query run
	// through the returned InternalTx executes on the same backend inside a
	// single PostgreSQL transaction, so results from one statement can drive the
	// next and the whole sequence commits or rolls back atomically.
	//
	// The reserved connection is held for the lifetime of the transaction. The
	// caller MUST call Commit or Rollback exactly once to release it; the
	// idiomatic pattern is to defer Rollback (a no-op once Commit succeeds):
	//
	//	tx, err := qs.Begin(ctx)
	//	if err != nil {
	//	    return err
	//	}
	//	defer tx.Rollback(ctx)
	//	if _, err := tx.Query(ctx, "..."); err != nil {
	//	    return err
	//	}
	//	return tx.Commit(ctx)
	Begin(ctx context.Context) (InternalTx, error)
}

// InternalTx is a handle to an in-progress transaction held on a reserved
// connection. It is returned by InternalQueryService.Begin. All queries issued
// through it run on the same backend inside one PostgreSQL transaction.
//
// InternalTx is not safe for concurrent use: callers must serialize access, the
// same as they would for a single database connection.
type InternalTx interface {
	// Query executes a query within the transaction and returns its single result.
	Query(ctx context.Context, query string) (*sqltypes.Result, error)

	// QueryArgs executes a parameterized query within the transaction. It accepts
	// the same Go argument types as InternalQueryService.QueryArgs.
	QueryArgs(ctx context.Context, query string, args ...any) (*sqltypes.Result, error)

	// Commit commits the transaction and releases the reserved connection. The
	// InternalTx must not be used afterward.
	Commit(ctx context.Context) error

	// Rollback rolls back the transaction and releases the reserved connection.
	// It is a no-op if the transaction was already committed or rolled back, so
	// it is safe to defer unconditionally.
	Rollback(ctx context.Context) error
}

// Compile-time check that internalTx implements InternalTx.
var _ InternalTx = (*internalTx)(nil)

// Compile-time check that Executor implements InternalQueryService.
var _ InternalQueryService = (*Executor)(nil)

// Query implements InternalQueryService for simple internal queries.
//
// It executes a query using the regular connection pool and returns the first
// result. We use the regular connection pool rather than the admin pool because
// we do not want to exhaust the admin pool.
//
// Internal queries include SQL text in trace spans since they use system
// functions.
func (e *Executor) Query(ctx context.Context, queryStr string) (*sqltypes.Result, error) {
	// Enable SQL text in trace spans for internal queries (safe - no user data)
	ctx = client.WithQueryTracing(ctx, client.QueryTracingConfig{
		IncludeQueryText: true,
	})

	conn, err := e.poolManager.GetRegularConn(ctx, e.poolManager.PgUser(), nil, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	results, err := conn.Conn.QueryWithRetry(ctx, queryStr)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, errors.New("unexpected number of results")
	}
	return results[0], nil
}

// QueryMultiStatement implements InternalQueryService for multi-statement queries.
// It executes a query using the simple query protocol and does not check the number of result sets.
func (e *Executor) QueryMultiStatement(ctx context.Context, queryStr string) error {
	ctx = client.WithQueryTracing(ctx, client.QueryTracingConfig{
		IncludeQueryText: true,
	})

	conn, err := e.poolManager.GetRegularConn(ctx, e.poolManager.PgUser(), nil, nil)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	_, err = conn.Conn.QueryWithRetry(ctx, queryStr)
	return err
}

// QueryArgs implements InternalQueryService for simple internal queries.
// This is a convenience method that accepts Go values as arguments and converts
// them to the appropriate text format for PostgreSQL.
// Supported argument types: nil, string, []byte, int, int32, int64, *int64, uint32, uint64,
// float32, float64, bool, and time.Time.
// Internal queries include SQL text in trace spans since they use system functions.
func (e *Executor) QueryArgs(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error) {
	// Enable SQL text in trace spans for internal queries (safe - no user data)
	ctx = client.WithQueryTracing(ctx, client.QueryTracingConfig{
		IncludeQueryText: true,
	})

	conn, err := e.poolManager.GetRegularConn(ctx, e.poolManager.PgUser(), nil, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	results, err := conn.Conn.QueryArgsWithRetry(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, errors.New("unexpected number of results")
	}
	return results[0], nil
}

// Begin implements InternalQueryService. It reserves a connection (via the
// reserved pool, the mechanism for holding a backend outside normal pooling)
// and starts a transaction on it, returning a handle that owns the reserved
// connection until Commit or Rollback is called.
func (e *Executor) Begin(ctx context.Context) (InternalTx, error) {
	// Enable SQL text in trace spans for internal queries (safe - no user data).
	ctx = client.WithQueryTracing(ctx, client.QueryTracingConfig{
		IncludeQueryText: true,
	})

	// Internal callers connect as the configured superuser and need no session
	// settings or SCRAM passthrough keys.
	reservedConn, err := e.poolManager.NewReservedConn(ctx, nil, e.poolManager.PgUser(), nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create reserved connection: %w", err)
	}

	// Begin runs "BEGIN" and records ReasonTransaction on the reservation. If it
	// fails no transaction state was established; release frees the connection
	// (tainting it if the failure closed the socket).
	if err := reservedConn.Begin(ctx); err != nil {
		reservedConn.Release(reserved.ReleaseError, nil)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &internalTx{conn: reservedConn}, nil
}

// internalTx is the Executor's implementation of InternalTx. It owns a reserved
// connection for the duration of a transaction.
type internalTx struct {
	// conn is the reserved connection carrying the open transaction.
	conn *reserved.Conn

	// finished is set once the transaction has been committed or rolled back and
	// the connection released. Further queries on a finished tx are rejected.
	finished bool
}

// Query implements InternalTx.
func (tx *internalTx) Query(ctx context.Context, queryStr string) (*sqltypes.Result, error) {
	if tx.finished {
		return nil, errTxFinished
	}
	ctx = client.WithQueryTracing(ctx, client.QueryTracingConfig{
		IncludeQueryText: true,
	})

	results, err := tx.conn.Query(ctx, queryStr)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, errors.New("unexpected number of results")
	}
	return results[0], nil
}

// QueryArgs implements InternalTx.
func (tx *internalTx) QueryArgs(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error) {
	if tx.finished {
		return nil, errTxFinished
	}
	ctx = client.WithQueryTracing(ctx, client.QueryTracingConfig{
		IncludeQueryText: true,
	})

	results, err := tx.conn.QueryArgs(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, errors.New("unexpected number of results")
	}
	return results[0], nil
}

// Commit implements InternalTx.
func (tx *internalTx) Commit(ctx context.Context) error {
	if tx.finished {
		return errTxFinished
	}
	tx.finished = true
	ctx = client.WithQueryTracing(ctx, client.QueryTracingConfig{
		IncludeQueryText: true,
	})

	if err := tx.conn.Commit(ctx); err != nil {
		// COMMIT failed: the connection's state is no longer known to be clean,
		// so release it as an error (which taints it) rather than recycling.
		tx.conn.Release(reserved.ReleaseError, nil)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	tx.conn.Release(reserved.ReleaseCommit, nil)
	return nil
}

// Rollback implements InternalTx. It is a no-op once the transaction is
// finished, so callers can defer it unconditionally alongside Commit.
func (tx *internalTx) Rollback(ctx context.Context) error {
	if tx.finished {
		return nil
	}
	tx.finished = true
	ctx = client.WithQueryTracing(ctx, client.QueryTracingConfig{
		IncludeQueryText: true,
	})

	if err := tx.conn.Rollback(ctx); err != nil {
		tx.conn.Release(reserved.ReleaseError, nil)
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}
	tx.conn.Release(reserved.ReleaseRollback, nil)
	return nil
}
