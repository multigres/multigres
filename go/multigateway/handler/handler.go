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

	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/server"
)

// Executor defines the interface for query execution.
type Executor interface {
	StreamExecute(ctx context.Context, conn *server.Conn, queryStr string, callback func(ctx context.Context, result *query.QueryResult) error) error
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
	h.logger.Debug("handling query", "query", queryStr, "user", conn.User(), "database", conn.Database())

	// Route the query through the executor which will eventually call multipooler
	return h.executor.StreamExecute(ctx, conn, queryStr, callback)
}

// getConnectionState retrieves and typecasts the connection state for this handler.
// Initializes a new state if it doesn't exist.
func (h *MultiGatewayHandler) getConnectionState(conn *server.Conn) *ConnectionState {
	state := conn.GetConnectionState()
	if state == nil {
		newState := NewConnectionState()
		conn.SetConnectionState(newState)
		return newState
	}
	return state.(*ConnectionState)
}

// HandleParse processes a Parse message ('P') for the extended query protocol.
// Creates and stores a prepared statement.
func (h *MultiGatewayHandler) HandleParse(ctx context.Context, conn *server.Conn, name, queryStr string, paramTypes []uint32) error {
	h.logger.Debug("parse", "name", name, "query", queryStr, "param_count", len(paramTypes))

	// Basic validation: query must not be empty.
	if queryStr == "" {
		return fmt.Errorf("query string cannot be empty")
	}

	// Create and store the prepared statement.
	stmt := NewPreparedStatement(name, queryStr, paramTypes)

	state := h.getConnectionState(conn)
	state.StorePreparedStatement(stmt)

	return nil
}

// HandleBind processes a Bind message ('B') for the extended query protocol.
// Creates and stores a portal for the specified prepared statement.
func (h *MultiGatewayHandler) HandleBind(ctx context.Context, conn *server.Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	h.logger.Debug("bind", "portal", portalName, "statement", stmtName, "param_count", len(params))

	// Get the connection state.
	state := h.getConnectionState(conn)

	// Get the prepared statement.
	stmt := state.GetPreparedStatement(stmtName)
	if stmt == nil {
		return fmt.Errorf("prepared statement \"%s\" does not exist", stmtName)
	}

	// TODO: Store parameters.
	portal := NewPortal(portalName, stmt)
	state.StorePortal(portal)

	return nil
}

// HandleExecute processes an Execute message ('E') for the extended query protocol.
// Executes the specified portal's query and streams results via callback.
func (h *MultiGatewayHandler) HandleExecute(ctx context.Context, conn *server.Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *query.QueryResult) error) error {
	h.logger.Debug("execute", "portal", portalName, "max_rows", maxRows)

	// Get the connection state.
	state := h.getConnectionState(conn)

	// Get the portal.
	portal := state.GetPortal(portalName)
	if portal == nil {
		return fmt.Errorf("portal \"%s\" does not exist", portalName)
	}

	// Get the query from the portal's statement.
	queryStr := portal.Statement.Query

	// TODO: Handle Parameters
	// TODO: Handle maxRows limitation

	// Stream execute and pass results directly through callback.
	return h.executor.StreamExecute(ctx, conn, queryStr, callback)
}

// HandleDescribe processes a Describe message ('D').
func (h *MultiGatewayHandler) HandleDescribe(ctx context.Context, conn *server.Conn, typ byte, name string) (*query.StatementDescription, error) {
	h.logger.Debug("describe not yet implemented", "type", string(typ), "name", name)
	return nil, fmt.Errorf("extended query protocol not yet implemented")
}

// HandleClose processes a Close message ('C').
func (h *MultiGatewayHandler) HandleClose(ctx context.Context, conn *server.Conn, typ byte, name string) error {
	h.logger.Debug("close not yet implemented", "type", string(typ), "name", name)
	return fmt.Errorf("extended query protocol not yet implemented")
}

// HandleSync processes a Sync message ('S').
func (h *MultiGatewayHandler) HandleSync(ctx context.Context, conn *server.Conn) error {
	h.logger.Debug("sync")

	// TODO: Handle transaction state
	return nil
}

// Ensure MultiGatewayHandler implements server.Handler interface.
var _ server.Handler = (*MultiGatewayHandler)(nil)
