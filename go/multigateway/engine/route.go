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

package engine

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/server"
)

// Route is a primitive that routes a query to a specific tablegroup.
// It represents the simplest form of query execution - sending a query
// to a single target tablegroup.
type Route struct {
	// TableGroup is the target tablegroup for this query.
	TableGroup string

	// Query is the SQL query string to execute.
	Query string

	// Shard is the target shard (empty string for unsharded or any shard).
	Shard string
}

// NewRoute creates a new Route primitive.
func NewRoute(tableGroup, shard, query string) *Route {
	return &Route{
		TableGroup: tableGroup,
		Shard:      shard,
		Query:      query,
	}
}

// StreamExecute executes the route by sending the query to the target tablegroup.
// It uses the IExecute interface to perform the actual execution, allowing for
// easy testing and decoupling from concrete execution implementations.
func (r *Route) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	callback func(context.Context, *query.QueryResult) error,
) error {
	// Execute the query through the execution interface
	// This will call ScatterConn in Phase 2+, or a stub/mock in testing
	return exec.StreamExecute(
		conn.Context(),
		r.TableGroup,
		r.Shard,
		r.Query,
		callback,
	)
}

// GetTableGroup returns the target tablegroup.
func (r *Route) GetTableGroup() string {
	return r.TableGroup
}

// GetQuery returns the SQL query.
func (r *Route) GetQuery() string {
	return r.Query
}

// String returns a description of the route for debugging.
func (r *Route) String() string {
	return fmt.Sprintf("Route(tablegroup=%s, query=%s)", r.TableGroup, r.Query)
}

// Ensure Route implements Primitive interface.
var _ Primitive = (*Route)(nil)
