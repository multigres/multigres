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

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
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
	//   conn: Database connection
	//   tableGroup: Target tablegroup for the query
	//   shard: Target shard (empty string for unsharded or any shard)
	//   sql: SQL query to execute
	//   state: Connection state containing session information and reserved connections
	//   callback: Function called for each result chunk
	// TODO: When we support sharded query serving, this method will need to take in
	// Routing parameters instead and figure out which all shards to send queries to.
	StreamExecute(
		ctx context.Context,
		conn *server.Conn,
		tableGroup string,
		shard string,
		sql string,
		state *handler.MultiGatewayConnectionState,
		callback func(context.Context, *sqltypes.Result) error,
	) error

	// PortalStreamExecute executes a portal (bound prepared statement) and streams results.
	//
	// Parameters:
	//   ctx: Context for cancellation and timeouts
	//   tableGroup: Target tablegroup for the query
	//   shard: Target shard (empty string for unsharded or any shard)
	//   conn: Database connection
	//   state: Connection state containing session information and reserved connections
	//   portalInfo: Portal information including bound parameters
	//   maxRows: Maximum number of rows to return (0 for unlimited)
	//   callback: Function called for each result chunk
	PortalStreamExecute(
		ctx context.Context,
		tableGroup string,
		shard string,
		conn *server.Conn,
		state *handler.MultiGatewayConnectionState,
		portalInfo *preparedstatement.PortalInfo,
		maxRows int32,
		callback func(context.Context, *sqltypes.Result) error,
	) error

	// Describe returns metadata about a prepared statement or portal.
	//
	// Parameters:
	//   ctx: Context for cancellation and timeouts
	//   tableGroup: Target tablegroup for the query
	//   shard: Target shard (empty string for unsharded or any shard)
	//   conn: Database connection
	//   state: Connection state containing session information and reserved connections
	//   portalInfo: Portal information (nil if describing a prepared statement)
	//   preparedStatementInfo: Prepared statement information (nil if describing a portal)
	Describe(
		ctx context.Context,
		tableGroup string,
		shard string,
		conn *server.Conn,
		state *handler.MultiGatewayConnectionState,
		portalInfo *preparedstatement.PortalInfo,
		preparedStatementInfo *preparedstatement.PreparedStatementInfo,
	) (*query.StatementDescription, error)

	// --- COPY FROM STDIN methods (called by CopyStatement primitive) ---

	// CopyInitiate initiates a COPY FROM STDIN operation using bidirectional streaming.
	// Returns: reservedConnID, poolerID, format, columnFormats, error
	CopyInitiate(
		ctx context.Context,
		conn *server.Conn,
		queryStr string,
		callback func(ctx context.Context, result *sqltypes.Result) error,
	) (reservedConnID uint64, poolerID *clustermetadata.ID, format int16, columnFormats []int16, err error)

	// CopySendData sends a chunk of COPY data via bidirectional stream.
	CopySendData(
		ctx context.Context,
		conn *server.Conn,
		reservedConnID uint64,
		poolerID *clustermetadata.ID,
		data []byte,
	) error

	// CopyFinalize sends the final chunk and CopyDone via bidirectional stream.
	CopyFinalize(
		ctx context.Context,
		conn *server.Conn,
		reservedConnID uint64,
		poolerID *clustermetadata.ID,
		finalData []byte,
		callback func(ctx context.Context, result *sqltypes.Result) error,
	) error

	// CopyAbort aborts the COPY operation via bidirectional stream.
	CopyAbort(
		ctx context.Context,
		conn *server.Conn,
		reservedConnID uint64,
		poolerID *clustermetadata.ID,
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
		state *handler.MultiGatewayConnectionState,
		callback func(context.Context, *sqltypes.Result) error,
	) error

	// GetTableGroup returns the target tablegroup for this primitive.
	// Returns empty string if primitive doesn't target a specific tablegroup.
	GetTableGroup() string

	// GetQuery returns the SQL query to be executed.
	GetQuery() string

	// String returns a description of the primitive for logging/debugging.
	String() string
}
