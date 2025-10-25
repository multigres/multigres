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

package server

import (
	"context"

	"github.com/multigres/multigres/go/pb/query"
)

// Handler defines the interface for query execution.
// This abstracts the actual query processing from the protocol layer,
// allowing the protocol implementation to be decoupled from routing/execution logic.
//
// Implementations of this interface should handle routing queries to the appropriate
// backend (e.g., multipooler via gRPC), and return results using proto-generated types
// that can be efficiently serialized and transmitted.
type Handler interface {
	// HandleQuery processes a simple query protocol message ('Q').
	// Returns the query result or an error.
	HandleQuery(ctx context.Context, query string) (*query.QueryResult, error)

	// HandleParse processes a Parse message ('P') for the extended query protocol.
	// Prepares a statement with the given name and parameter types.
	// An empty name indicates an unnamed statement.
	HandleParse(ctx context.Context, name, queryStr string, paramTypes []uint32) error

	// HandleBind processes a Bind message ('B') for the extended query protocol.
	// Binds parameters to a prepared statement, creating a portal.
	// portalName: name of the portal to create (empty for unnamed portal)
	// stmtName: name of the prepared statement to bind (empty for unnamed statement)
	// params: parameter values in the format specified by paramFormats
	// paramFormats: format codes for parameters (0=text, 1=binary)
	// resultFormats: desired format codes for result columns (0=text, 1=binary)
	HandleBind(ctx context.Context, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error

	// HandleExecute processes an Execute message ('E') for the extended query protocol.
	// Executes a bound portal and returns results.
	// portalName: name of the portal to execute (empty for unnamed portal)
	// maxRows: maximum number of rows to return (0 for no limit)
	HandleExecute(ctx context.Context, portalName string, maxRows int32) (*query.QueryResult, error)

	// HandleDescribe processes a Describe message ('D').
	// Returns description of a prepared statement or portal.
	// typ: 'S' for statement, 'P' for portal
	// name: name of the statement or portal
	HandleDescribe(ctx context.Context, typ byte, name string) (*query.StatementDescription, error)

	// HandleClose processes a Close message ('C').
	// Closes a prepared statement or portal.
	// typ: 'S' for statement, 'P' for portal
	// name: name of the statement or portal to close
	HandleClose(ctx context.Context, typ byte, name string) error

	// HandleSync processes a Sync message ('S').
	// Called at the end of an extended query cycle to indicate transaction boundary.
	HandleSync(ctx context.Context) error
}
