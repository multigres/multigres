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
	"fmt"

	"github.com/multigres/multigres/go/pb/query"
)

// TODO: We might want to make this a configuration. For now its a constant.
// DefaultInternalUser is the default PostgreSQL user for internal queries.
const DefaultInternalUser = "postgres"

// InternalQueryService provides a simplified query interface for internal multipooler
// components. It uses the connection pool with "postgres" user by default.
type InternalQueryService interface {
	// Query executes a query and returns the result.
	Query(ctx context.Context, query string) (*query.QueryResult, error)

	// Query executes a query with arguments and returns the result.
	// This is a convenience method that accepts Go values as arguments and converts
	// them to the appropriate text format for PostgreSQL.
	// Supported argument types: nil, string, []byte, int, int32, int64, uint32, uint64,
	// float32, float64, bool, and time.Time.
	QueryArgs(ctx context.Context, query string, args ...any) (*query.QueryResult, error)
}

// Compile-time check that Executor implements InternalQueryService.
var _ InternalQueryService = (*Executor)(nil)

// Query implements InternalQueryService for simple internal queries.
// It executes a query using the "postgres" user and returns the first result.
func (e *Executor) Query(ctx context.Context, queryStr string) (*query.QueryResult, error) {
	conn, err := e.poolManager.GetRegularConn(ctx, DefaultInternalUser)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	results, err := conn.Conn.Query(ctx, queryStr)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("unexepected number of results")
	}
	return results[0], nil
}

// QueryArgs implements InternalQueryService for simple internal queries.
// This is a convenience method that accepts Go values as arguments and converts
// them to the appropriate text format for PostgreSQL.
// Supported argument types: nil, string, []byte, int, int32, int64, uint32, uint64,
// float32, float64, bool, and time.Time.
func (e *Executor) QueryArgs(ctx context.Context, sql string, args ...any) (*query.QueryResult, error) {
	conn, err := e.poolManager.GetRegularConn(ctx, DefaultInternalUser)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	results, err := conn.Conn.QueryArgs(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("unexepected number of results")
	}
	return results[0], nil
}
