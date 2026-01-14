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

// Package queryservice contains the interface for the query service definition
// of multipooler.
//
// This interface is implemented by:
// - multipooler instances (actual query execution on PostgreSQL)
// - PoolerGateway (routes to multipooler instances)
// - Mock implementations for testing
package queryservice

import (
	"context"

	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

// ReservedState contains information about a reserved connection.
// This is returned by ReserveStreamExecute and should be stored in the shard state
// to ensure subsequent queries in the same session use the same reserved connection.
type ReservedState struct {
	// ReservedConnectionId is the ID of the reserved connection on the multipooler.
	ReservedConnectionId uint64

	// PoolerID identifies which multipooler instance owns this reserved connection.
	// This is needed to route subsequent queries to the correct pooler.
	PoolerID *clustermetadatapb.ID
}

// QueryService is the interface for executing queries on a multipooler.
// This interface abstracts the communication with multipooler instances
// and can be implemented by different backends (gRPC, local, mock, etc.).
//
// All methods must be safe to be called concurrently.
//
// This interface is implemented by the grpcQueryService which serves as the gRPC client
// to the multipooler, and also by the executor in multipooler which is called by the server of the said
// gRPC connection. It is also implemented by the PoolerGateway to abstract away the complexity of managing
// gRPC pooler connections.
type QueryService interface {
	// ExecuteQuery executes a query and returns the results.
	// This should be used sparingly only when we know the result set is small,
	// otherwise StreamExecute should be used.
	ExecuteQuery(
		ctx context.Context,
		target *query.Target,
		sql string,
		options *query.ExecuteOptions,
	) (*sqltypes.Result, error)

	// StreamExecute executes a query and streams results back via callback.
	// The callback will be called for each Result. If the callback returns
	// an error, streaming stops and that error is returned.
	//
	// The context can be used to cancel the stream.
	StreamExecute(
		ctx context.Context,
		target *query.Target,
		sql string,
		options *query.ExecuteOptions,
		callback func(context.Context, *sqltypes.Result) error,
	) error

	// PortalStreamExecute executes a portal (bound prepared statement) and streams results back via callback.
	// Returns ReservedState containing information about the reserved connection used for this execution.
	// The returned ReservedState should be stored in the connection's shard state to ensure
	// subsequent queries in the same session use the same reserved connection.
	//
	// Parameters:
	//   ctx: Context for cancellation and timeouts
	//   target: Target specifying tablegroup, shard, and pooler type
	//   preparedStatement: The prepared statement to execute
	//   portal: The portal containing bound parameters
	//   options: Execute options including max rows and reserved connection ID
	//   callback: Function called for each result chunk
	PortalStreamExecute(
		ctx context.Context,
		target *query.Target,
		preparedStatement *query.PreparedStatement,
		portal *query.Portal,
		options *query.ExecuteOptions,
		callback func(context.Context, *sqltypes.Result) error,
	) (ReservedState, error)

	// Describe returns metadata about a prepared statement or portal.
	// The target specifies which multipooler to query.
	// Either preparedStatement or portal (or both) should be provided.
	//
	// Parameters:
	//   ctx: Context for cancellation and timeouts
	//   target: Target specifying tablegroup, shard, and pooler type
	//   preparedStatement: The prepared statement to describe (nil if describing a portal)
	//   portal: The portal to describe (nil if describing a prepared statement)
	//   options: Execute options including reserved connection ID
	Describe(
		ctx context.Context,
		target *query.Target,
		preparedStatement *query.PreparedStatement,
		portal *query.Portal,
		options *query.ExecuteOptions,
	) (*query.StatementDescription, error)

	// Close closes the query service and releases resources.
	// After Close is called, no other methods should be called.
	Close() error
}
