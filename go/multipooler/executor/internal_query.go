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

	"github.com/multigres/multigres/go/pb/query"
)

// DefaultInternalUser is the default PostgreSQL user for internal queries.
// This can be overridden when creating an Executor instance.
const DefaultInternalUser = "postgres"

// InternalQuerier provides a simplified query interface for internal multipooler
// components. It uses the connection pool with a configurable internal user.
type InternalQuerier interface {
	// Query executes a query and returns the result.
	Query(ctx context.Context, query string) (*query.QueryResult, error)
}

// Compile-time check that Executor implements InternalQuerier.
var _ InternalQuerier = (*Executor)(nil)

// Query implements InternalQuerier for simple internal queries.
// It executes a query using the configured internal user and returns the first result.
func (e *Executor) Query(ctx context.Context, queryStr string) (*query.QueryResult, error) {
	conn, err := e.poolManager.GetRegularConn(ctx, e.internalUser)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	results, err := conn.Conn.Query(ctx, queryStr)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0], nil
}
