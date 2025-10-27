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
	"github.com/multigres/multigres/go/pb/query"
)

// Handler defines the interface for query execution.
// This abstracts the actual query processing from the protocol layer,
// allowing the protocol implementation to be decoupled from routing/execution logic.
//
// Implementations of this interface should handle routing queries to the appropriate
// backend (e.g., multipooler via gRPC), and stream results back via the callback function.
//
// The callback-based approach allows for efficient streaming of large result sets
// without needing to buffer all results in memory before sending to the client.
//
// Supports multiple statements in a single query (e.g., "SELECT 1; SELECT 2;") where
// each statement can have a large streaming result set, using the CommandTag field
// to indicate result set boundaries.
type Handler interface {
	// HandleQuery processes a simple query protocol message ('Q').
	// The callback function is called with the query result.
	//
	// The handler should set result.CommandTag when a result set is complete:
	// - If CommandTag is empty: More packets coming (continuing current result set)
	// - If CommandTag is set: This is the last packet of this result set, triggers CommandComplete
	//
	// For streaming a single large result set:
	//   callback(chunk1)           // Fields + rows, CommandTag=""
	//   callback(chunk2)           // More rows, CommandTag=""
	//   callback(chunk3)           // Final rows, CommandTag="SELECT 42"
	//
	// For multiple statements with streaming (e.g., "SELECT * FROM big_table1; SELECT * FROM big_table2;"):
	//   callback(chunk1_q1)        // Query 1, chunk 1, CommandTag=""
	//   callback(chunk2_q1)        // Query 1, chunk 2, CommandTag=""
	//   callback(chunk3_q1)        // Query 1, final chunk, CommandTag="SELECT 100" → CommandComplete
	//   callback(chunk1_q2)        // Query 2, chunk 1, CommandTag=""
	//   callback(chunk2_q2)        // Query 2, final chunk, CommandTag="SELECT 200" → CommandComplete
	//
	// After all callbacks complete, ReadyForQuery ('Z') is sent once.
	//
	// Returns an error if query execution or result streaming fails.
	HandleQuery(conn *Conn, query string, callback func(result *query.QueryResult) error) error

	// HandleParse processes a Parse message ('P') for the extended query protocol.
	// Prepares a statement with the given name and parameter types.
	// An empty name indicates an unnamed statement.
	HandleParse(conn *Conn, name, queryStr string, paramTypes []uint32) error

	// HandleBind processes a Bind message ('B') for the extended query protocol.
	// Binds parameters to a prepared statement, creating a portal.
	// portalName: name of the portal to create (empty for unnamed portal)
	// stmtName: name of the prepared statement to bind (empty for unnamed statement)
	// params: parameter values in the format specified by paramFormats
	// paramFormats: format codes for parameters (0=text, 1=binary)
	// resultFormats: desired format codes for result columns (0=text, 1=binary)
	HandleBind(conn *Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error

	// HandleExecute processes an Execute message ('E') for the extended query protocol.
	// Executes a bound portal and returns results.
	// portalName: name of the portal to execute (empty for unnamed portal)
	// maxRows: maximum number of rows to return (0 for no limit)
	HandleExecute(conn *Conn, portalName string, maxRows int32) (*query.QueryResult, error)

	// HandleDescribe processes a Describe message ('D').
	// Returns description of a prepared statement or portal.
	// typ: 'S' for statement, 'P' for portal
	// name: name of the statement or portal
	HandleDescribe(conn *Conn, typ byte, name string) (*query.StatementDescription, error)

	// HandleClose processes a Close message ('C').
	// Closes a prepared statement or portal.
	// typ: 'S' for statement, 'P' for portal
	// name: name of the statement or portal to close
	HandleClose(conn *Conn, typ byte, name string) error

	// HandleSync processes a Sync message ('S').
	// Called at the end of an extended query cycle to indicate transaction boundary.
	HandleSync(conn *Conn) error
}
