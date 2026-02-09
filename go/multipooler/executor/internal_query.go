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

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// DefaultInternalUser is the default PostgreSQL user for internal queries.
// This can be overridden when creating an Executor instance.
const DefaultInternalUser = "postgres"

// InternalQueryService provides a simplified query interface for internal multipooler
// components. It uses the connection pool with a configurable internal user.
type InternalQueryService interface {
	// Query executes a query and returns the result.
	Query(ctx context.Context, query string) (*sqltypes.Result, error)

	// QueryArgs executes a query with arguments and returns the result.
	// This is a convenience method that accepts Go values as arguments and converts
	// them to the appropriate text format for PostgreSQL.
	// Supported argument types: nil, string, []byte, int, int32, int64, uint32, uint64,
	// float32, float64, bool, and time.Time.
	QueryArgs(ctx context.Context, query string, args ...any) (*sqltypes.Result, error)

	// QueryMultiStatement executes a multi-statement query (e.g. "BEGIN; ...; COMMIT;")
	// using the simple query protocol. Unlike Query, it does not require exactly one result set.
	QueryMultiStatement(ctx context.Context, query string) error
}

// Compile-time check that Executor implements InternalQueryService.
var _ InternalQueryService = (*Executor)(nil)

// Query implements InternalQueryService for simple internal queries.
// It executes a query using the configured internal user and returns the first result.
// Internal queries include SQL text in trace spans since they use system functions.
func (e *Executor) Query(ctx context.Context, queryStr string) (*sqltypes.Result, error) {
	// Enable SQL text in trace spans for internal queries (safe - no user data)
	ctx = client.WithQueryTracing(ctx, client.QueryTracingConfig{
		IncludeQueryText: true,
	})

	conn, err := e.poolManager.GetRegularConn(ctx, e.poolManager.InternalUser())
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	results, err := conn.Conn.Query(ctx, queryStr)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, errors.New("unexepected number of results")
	}
	return results[0], nil
}

// QueryMultiStatement implements InternalQueryService for multi-statement queries.
// It executes a query using the simple query protocol and does not check the number of result sets.
func (e *Executor) QueryMultiStatement(ctx context.Context, queryStr string) error {
	ctx = client.WithQueryTracing(ctx, client.QueryTracingConfig{
		IncludeQueryText: true,
	})

	conn, err := e.poolManager.GetRegularConn(ctx, e.poolManager.InternalUser())
	if err != nil {
		return err
	}
	defer conn.Recycle()

	_, err = conn.Conn.Query(ctx, queryStr)
	return err
}

// QueryArgs implements InternalQueryService for simple internal queries.
// This is a convenience method that accepts Go values as arguments and converts
// them to the appropriate text format for PostgreSQL.
// Supported argument types: nil, string, []byte, int, int32, int64, uint32, uint64,
// float32, float64, bool, and time.Time.
// Internal queries include SQL text in trace spans since they use system functions.
func (e *Executor) QueryArgs(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error) {
	// Enable SQL text in trace spans for internal queries (safe - no user data)
	ctx = client.WithQueryTracing(ctx, client.QueryTracingConfig{
		IncludeQueryText: true,
	})

	conn, err := e.poolManager.GetRegularConn(ctx, e.poolManager.InternalUser())
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	results, err := conn.Conn.QueryArgs(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, errors.New("unexepected number of results")
	}
	return results[0], nil
}
