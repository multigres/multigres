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

package executor

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/admin"
)

// The QueryAdmin* methods are the InternalQueryService access path for the
// locked-down multigres sidecar schema. They share the run* helpers with the
// regular Query* methods and differ only in the borrower: they run on the admin
// pool, which authenticates as the true PostgreSQL superuser. The multigres
// schema is created and owned by that superuser and is not reachable by customer
// roles through their per-user regular pool (they get only USAGE on the schema
// and SELECT on multigres.backend_vpid), so any internal read/write of a
// multigres.* table must go through these methods or it would fail once the
// regular pool's role is not the schema owner.

// borrowAdmin borrows a connection from the admin (true-superuser) pool.
func (e *Executor) borrowAdmin(ctx context.Context) (pooledQueryConn, func(), error) {
	conn, err := e.poolManager.GetAdminConn(ctx)
	if err != nil {
		return nil, nil, err
	}
	return conn.Conn, conn.Recycle, nil
}

// QueryAdmin implements InternalQueryService on the admin pool.
func (e *Executor) QueryAdmin(ctx context.Context, queryStr string) (*sqltypes.Result, error) {
	return runSingleQuery(ctx, e.borrowAdmin, queryStr)
}

// QueryAdminArgs implements InternalQueryService on the admin pool.
func (e *Executor) QueryAdminArgs(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error) {
	return runSingleQueryArgs(ctx, e.borrowAdmin, sql, args...)
}

// QueryAdminMultiStatement implements InternalQueryService on the admin pool.
func (e *Executor) QueryAdminMultiStatement(ctx context.Context, queryStr string) error {
	return runMultiStatement(ctx, e.borrowAdmin, queryStr)
}

// BeginAdmin implements InternalQueryService on the admin pool.
func (e *Executor) BeginAdmin(ctx context.Context) (InternalTx, error) {
	pooled, err := e.poolManager.GetAdminConn(ctx)
	if err != nil {
		return nil, err
	}
	conn := adminTxConn{conn: pooled.Conn}
	if _, err := conn.Query(ctx, "BEGIN"); err != nil {
		pooled.Recycle()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &genericTx{
		conn: conn,
		// admin.Conn's own no-retry query methods already close the
		// connection on a genuine connection failure (see QueryNoRetry's doc
		// comment), so Recycle here always sees an accurate IsClosed() and
		// does the right thing either way — no separate error-vs-clean
		// release path is needed, unlike the reserved pool's ReleaseReason.
		onRelease: func(_ txOutcome, _ error) {
			pooled.Recycle()
		},
	}, nil
}

// adminTxConn adapts *admin.Conn to the txConn interface shared with the
// regular pool's transactions (see genericTx). admin.Conn has no native
// transaction-lifecycle methods, so Commit/Rollback are sent as literal
// statements — always via the no-retry query path, never
// QueryWithRetry/QueryArgsWithRetry, which are only safe for stateless calls
// (see their doc comments for why: a silent reconnect mid-transaction would
// lose it without any error to signal that).
type adminTxConn struct {
	conn *admin.Conn
}

func (c adminTxConn) Query(ctx context.Context, sql string) ([]*sqltypes.Result, error) {
	return c.conn.QueryNoRetry(ctx, sql)
}

func (c adminTxConn) QueryArgs(ctx context.Context, sql string, args ...any) ([]*sqltypes.Result, error) {
	return c.conn.QueryArgsNoRetry(ctx, sql, args...)
}

func (c adminTxConn) Commit(ctx context.Context) error {
	_, err := c.conn.QueryNoRetry(ctx, "COMMIT")
	return err
}

func (c adminTxConn) Rollback(ctx context.Context) error {
	_, err := c.conn.QueryNoRetry(ctx, "ROLLBACK")
	return err
}
