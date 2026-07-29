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
