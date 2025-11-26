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

// Package engine contains the query execution primitives for multigateway.
// Primitives are the building blocks of query plans and handle routing,
// execution coordination, and result aggregation.
package engine

import (
	"context"

	"github.com/multigres/multigres/go/multigateway/handler"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/server"
)

// IExecute is the execution interface that provides access to execution
// resources like ScatterConn. It's passed to primitives during execution,
// allowing them to execute queries without directly depending on concrete types.
//
// This interface helps testing Planner while allowing the underlying execution framework
// to be mocked. This interface is implemented by ScatterCon in production code.
type IExecute interface {
	// StreamExecute executes a query on the specified tablegroup and streams results.
	// This is the main execution method that primitives call to actually run queries.
	//
	// Parameters:
	//   ctx: Context for cancellation and timeouts
	//   tableGroup: Target tablegroup for the query
	//   shard: Target shard (empty string for unsharded or any shard)
	//   sql: SQL query to execute
	//   options: Execute options containing session state and settings
	//   callback: Function called for each result chunk
	// TODO: When we support sharded query serving, this method will need to take in
	// Routing parameters instead and figure out which all shards to send queries to.
	StreamExecute(
		ctx context.Context,
		tableGroup string,
		shard string,
		sql string,
		options *handler.ExecuteOptions,
		callback func(context.Context, *query.QueryResult) error,
	) error
}

// Primitive is the building block of the query execution plan.
// Each primitive represents an operation in the query execution tree
// (e.g., route to tablegroup, join, aggregate, etc.).
//
// Primitives receive an IExecute interface during execution, which provides
// access to execution resources without tight coupling.
type Primitive interface {
	// StreamExecute executes the primitive and streams results via callback.
	// The IExecute interface provides access to execution resources.
	StreamExecute(
		ctx context.Context,
		exec IExecute,
		conn *server.Conn,
		options *handler.ExecuteOptions,
		callback func(context.Context, *query.QueryResult) error,
	) error

	// GetTableGroup returns the target tablegroup for this primitive.
	// Returns empty string if primitive doesn't target a specific tablegroup.
	GetTableGroup() string

	// GetQuery returns the SQL query to be executed.
	GetQuery() string

	// String returns a description of the primitive for logging/debugging.
	String() string
}
