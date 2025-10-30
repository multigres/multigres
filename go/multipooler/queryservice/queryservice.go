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

	"github.com/multigres/multigres/go/pb/query"
)

// QueryService is the interface for executing queries on a multipooler.
// This interface abstracts the communication with multipooler instances
// and can be implemented by different backends (gRPC, local, mock, etc.).
//
// All methods must be safe to be called concurrently.
type QueryService interface {
	// ExecuteQuery executes a query and returns the results.
	// This should be used sparingly only when we know the result set is small,
	// otherwise StreamExecute should be used.
	ExecuteQuery(
		ctx context.Context,
		target *query.Target,
		sql string,
		maxRows uint64,
	) (*query.QueryResult, error)

	// StreamExecute executes a query and streams results back via callback.
	// The callback will be called for each QueryResult. If the callback returns
	// an error, streaming stops and that error is returned.
	//
	// The context can be used to cancel the stream.
	StreamExecute(
		ctx context.Context,
		target *query.Target,
		sql string,
		callback func(*query.QueryResult) error,
	) error

	// Close closes the query service and releases resources.
	// After Close is called, no other methods should be called.
	Close(ctx context.Context) error
}
