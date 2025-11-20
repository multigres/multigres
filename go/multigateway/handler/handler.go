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

package handler

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/parser"
	"github.com/multigres/multigres/go/parser/ast"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/server"
	"github.com/multigres/multigres/go/pools/connpool"
)

// Executor defines the interface for query execution.
type Executor interface {
	StreamExecute(ctx context.Context, conn *server.Conn, queryStr string, astStmt ast.Stmt, callback func(ctx context.Context, result *query.QueryResult) error) error
}

// MultiGatewayHandler implements the pgprotocol Handler interface for multigateway.
// It routes PostgreSQL protocol queries to the appropriate multipooler instances.
type MultiGatewayHandler struct {
	executor Executor
	logger   *slog.Logger
}

// NewMultiGatewayHandler creates a new PostgreSQL protocol handler.
func NewMultiGatewayHandler(executor Executor, logger *slog.Logger) *MultiGatewayHandler {
	return &MultiGatewayHandler{
		executor: executor,
		logger:   logger.With("component", "multigateway_handler"),
	}
}

// HandleQuery processes a simple query protocol message ('Q').
// Routes the query to an appropriate multipooler instance and streams results back.
func (h *MultiGatewayHandler) HandleQuery(ctx context.Context, conn *server.Conn, queryStr string, callback func(ctx context.Context, result *query.QueryResult) error) error {
	h.logger.DebugContext(ctx, "handling query", "query", queryStr, "user", conn.User(), "database", conn.Database())

	asts, err := parser.ParseSQL(queryStr)
	if err != nil {
		return err
	}

	// Handle empty query (e.g., just a semicolon or whitespace).
	// Call callback with nil to signal empty query response.
	if len(asts) == 0 {
		return callback(ctx, nil)
	}

	for _, astStmt := range asts {
		// Route the query through the executor which will eventually call multipooler
		err = h.executor.StreamExecute(ctx, conn, queryStr, astStmt, callback)
		if err != nil {
			return err
		}
	}
	return nil
}

// getConnectionState retrieves and typecasts the connection state for this handler.
// Initializes a new state if it doesn't exist.
func (h *MultiGatewayHandler) getConnectionState(conn *server.Conn) *connpool.ConnectionState {
	state := conn.GetConnectionState()
	if state == nil {
		newState := connpool.NewEmptyConnectionState()
		conn.SetConnectionState(newState)
		return newState
	}
	return state.(*connpool.ConnectionState)
}

// HandleParse processes a Parse message ('P') for the extended query protocol.
// Creates and stores a prepared statement.
func (h *MultiGatewayHandler) HandleParse(ctx context.Context, conn *server.Conn, name, queryStr string, paramTypes []uint32) error {
	h.logger.DebugContext(ctx, "parse", "name", name, "query", queryStr, "param_count", len(paramTypes))

	// Basic validation: query must not be empty.
	if queryStr == "" {
		return fmt.Errorf("query string cannot be empty")
	}

	asts, err := parser.ParseSQL(queryStr)
	if err != nil {
		return err
	}
	if len(asts) != 1 {
		return fmt.Errorf("more than 1 query in prepare statement")
	}

	// Create and store the prepared statement.
	stmt := connpool.NewPreparedStatement(name, asts[0], paramTypes)

	state := h.getConnectionState(conn)
	state.StorePreparedStatement(stmt)

	return nil
}

// HandleBind processes a Bind message ('B') for the extended query protocol.
// Creates and stores a portal for the specified prepared statement with bound parameters.
func (h *MultiGatewayHandler) HandleBind(ctx context.Context, conn *server.Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	h.logger.DebugContext(ctx, "bind", "portal", portalName, "statement", stmtName, "param_count", len(params))

	// Get the connection state.
	state := h.getConnectionState(conn)

	// Get the prepared statement.
	stmt := state.GetPreparedStatement(stmtName)
	if stmt == nil {
		return fmt.Errorf("prepared statement \"%s\" does not exist", stmtName)
	}

	// Create portal with bound parameters and format codes.
	portal := connpool.NewPortal(portalName, stmt, params, paramFormats, resultFormats)
	state.StorePortal(portal)

	return nil
}

// HandleExecute processes an Execute message ('E') for the extended query protocol.
// Executes the specified portal's query with bound parameters and streams results via callback.
func (h *MultiGatewayHandler) HandleExecute(ctx context.Context, conn *server.Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *query.QueryResult) error) error {
	h.logger.DebugContext(ctx, "execute", "portal", portalName, "max_rows", maxRows)

	// Get the connection state.
	state := h.getConnectionState(conn)

	// Get the portal.
	portal := state.GetPortal(portalName)
	if portal == nil {
		return fmt.Errorf("portal \"%s\" does not exist", portalName)
	}

	// Get the query AST from the portal's statement.
	qry := portal.Statement.Query

	// Substitute parameters if any are bound
	if len(portal.Params) > 0 {
		substitutedQuery, err := ast.SubstituteParameters(
			qry,
			portal.Params,
			portal.ParamFormats,
			portal.Statement.ParamTypes,
		)
		if err != nil {
			return fmt.Errorf("parameter substitution failed: %w", err)
		}
		qry = substitutedQuery
		h.logger.DebugContext(ctx, "substituted query", "query", qry.SqlString())
	}

	// TODO: Handle maxRows limitation (cursor support for partial fetches)

	// Stream execute and pass results directly through callback.
	return h.executor.StreamExecute(ctx, conn, qry.SqlString(), qry, callback)
}

// HandleDescribe processes a Describe message ('D').
// Describes either a prepared statement ('S') or a portal ('P').
func (h *MultiGatewayHandler) HandleDescribe(ctx context.Context, conn *server.Conn, typ byte, name string) (*query.StatementDescription, error) {
	h.logger.DebugContext(ctx, "describe", "type", string(typ), "name", name)

	state := h.getConnectionState(conn)

	switch typ {
	case 'S': // Describe prepared statement
		stmt := state.GetPreparedStatement(name)
		if stmt == nil {
			return nil, fmt.Errorf("prepared statement \"%s\" does not exist", name)
		}

		// Convert param types to parameter descriptions.
		params := make([]*query.ParameterDescription, len(stmt.ParamTypes))
		for i, oid := range stmt.ParamTypes {
			params[i] = &query.ParameterDescription{
				DataTypeOid: oid,
			}
		}

		// Return a statement description.
		// TODO: For now, we return empty fields since we don't parse the query to determine result columns.
		// The client will get actual field information when executing.
		return &query.StatementDescription{
			Parameters: params,
			Fields:     nil, // Would require query parsing to determine result columns
		}, nil

	case 'P': // Describe portal
		portal := state.GetPortal(name)
		if portal == nil {
			return nil, fmt.Errorf("portal \"%s\" does not exist", name)
		}

		// Convert param types to parameter descriptions.
		params := make([]*query.ParameterDescription, len(portal.Statement.ParamTypes))
		for i, oid := range portal.Statement.ParamTypes {
			params[i] = &query.ParameterDescription{
				DataTypeOid: oid,
			}
		}

		// Return a statement description for the portal's query.
		// TODO: Similar to above, we don't have field information without executing.
		return &query.StatementDescription{
			Parameters: params,
			Fields:     nil, // Would require query parsing/execution to determine result columns
		}, nil

	default:
		return nil, fmt.Errorf("invalid describe type: %c (expected 'S' or 'P')", typ)
	}
}

// HandleClose processes a Close message ('C').
// Closes either a prepared statement ('S') or a portal ('P').
func (h *MultiGatewayHandler) HandleClose(ctx context.Context, conn *server.Conn, typ byte, name string) error {
	h.logger.DebugContext(ctx, "close", "type", string(typ), "name", name)

	state := h.getConnectionState(conn)

	switch typ {
	case 'S': // Close prepared statement
		state.DeletePreparedStatement(name)
		return nil

	case 'P': // Close portal
		state.DeletePortal(name)
		return nil

	default:
		return fmt.Errorf("invalid close type: %c (expected 'S' or 'P')", typ)
	}
}

// HandleSync processes a Sync message ('S').
func (h *MultiGatewayHandler) HandleSync(ctx context.Context, conn *server.Conn) error {
	h.logger.DebugContext(ctx, "sync")

	// TODO: Handle transaction state
	return nil
}

// Ensure MultiGatewayHandler implements server.Handler interface.
var _ server.Handler = (*MultiGatewayHandler)(nil)
