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

	"github.com/multigres/multigres/go/common/sqltypes"
)

// The QueryAdmin* methods are the InternalQueryService access path for recovery
// probes that must bypass saturated user pools and for the locked-down multigres
// sidecar schema. They share the run* helpers with the regular Query* methods and
// differ only in the borrower: they run on the admin pool, which authenticates
// as the true PostgreSQL superuser.

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

// BeginAdmin implements InternalQueryService on the admin pool. The returned
// transaction runs on an admin.TxConn, which has no retry methods at all —
// see its doc comment for why that matters mid-transaction.
func (e *Executor) BeginAdmin(ctx context.Context) (InternalTx, error) {
	pooled, err := e.poolManager.GetAdminConn(ctx)
	if err != nil {
		return nil, err
	}
	txConn, err := pooled.Conn.BeginTx(ctx)
	if err != nil {
		pooled.Recycle()
		return nil, err
	}
	return &genericTx{
		conn: txConn,
		// admin.TxConn's query methods already close the connection on a
		// genuine connection failure, so Recycle here always sees an
		// accurate IsClosed() and does the right thing either way — no
		// separate error-vs-clean release path is needed, unlike the
		// reserved pool's ReleaseReason.
		onRelease: func(_ txOutcome, _ error) {
			pooled.Recycle()
		},
	}, nil
}
