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
	"errors"
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// Executor defines the interface for query execution.
type Executor interface {
	// StreamExecute is used to run the provided query in streaming mode.
	StreamExecute(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState, queryStr string, astStmt ast.Stmt, callback func(ctx context.Context, result *sqltypes.Result) error) error

	// PortalStreamExecute is used to execute a Portal that was previously created.
	PortalStreamExecute(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState, portalInfo *preparedstatement.PortalInfo, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) error

	// Describe returns metadata about a prepared statement or portal.
	// The options should contain PreparedStatement or Portal information and the reserved connection ID.
	Describe(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState, portalInfo *preparedstatement.PortalInfo, preparedStatementInfo *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error)

	// ValidateStartupParams validates startup parameters by executing a simple
	// query against the backend with those parameters applied as session settings.
	// If any parameter is invalid, the backend will reject the SET and return an error.
	// Returns ParameterStatus values captured from the backend response.
	ValidateStartupParams(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState) (map[string]string, error)
}

// MultiGatewayHandler implements the pgprotocol Handler interface for multigateway.
// It routes PostgreSQL protocol queries to the appropriate multipooler instances.
type MultiGatewayHandler struct {
	executor Executor
	logger   *slog.Logger
	psc      *preparedstatement.Consolidator
}

// NewMultiGatewayHandler creates a new PostgreSQL protocol handler.
func NewMultiGatewayHandler(executor Executor, logger *slog.Logger) *MultiGatewayHandler {
	return &MultiGatewayHandler{
		executor: executor,
		logger:   logger.With("component", "multigateway_handler"),
		psc:      preparedstatement.NewConsolidator(),
	}
}

// Consolidator returns the prepared statement consolidator.
func (h *MultiGatewayHandler) Consolidator() *preparedstatement.Consolidator {
	return h.psc
}

// HandleQuery processes a simple query protocol message ('Q').
// Routes the query to an appropriate multipooler instance and streams results back.
func (h *MultiGatewayHandler) HandleQuery(ctx context.Context, conn *server.Conn, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error {
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
	st := h.getConnectionState(conn)

	for _, astStmt := range asts {
		// Route the query through the executor which will eventually call multipooler
		err = h.executor.StreamExecute(ctx, conn, st, queryStr, astStmt, callback)
		if err != nil {
			return err
		}
	}
	return nil
}

// getConnectionState retrieves and typecasts the connection state for this handler.
// Initializes a new state if it doesn't exist, populating startup parameters from the connection.
func (h *MultiGatewayHandler) getConnectionState(conn *server.Conn) *MultiGatewayConnectionState {
	state := conn.GetConnectionState()
	if state == nil {
		newState := NewMultiGatewayConnectionState()
		newState.StartupParams = conn.GetStartupParams()
		conn.SetConnectionState(newState)
		return newState
	}
	return state.(*MultiGatewayConnectionState)
}

// HandleParse processes a Parse message ('P') for the extended query protocol.
// Creates and stores a prepared statement.
func (h *MultiGatewayHandler) HandleParse(ctx context.Context, conn *server.Conn, name, queryStr string, paramTypes []uint32) error {
	h.logger.DebugContext(ctx, "parse", "name", name, "query", queryStr, "param_count", len(paramTypes))

	// Basic validation: query must not be empty.
	if queryStr == "" {
		return errors.New("query string cannot be empty")
	}

	_, err := h.psc.AddPreparedStatement(conn.ConnectionID(), name, queryStr, paramTypes)
	return err
}

// HandleBind processes a Bind message ('B') for the extended query protocol.
// Creates and stores a portal for the specified prepared statement with bound parameters.
func (h *MultiGatewayHandler) HandleBind(ctx context.Context, conn *server.Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	h.logger.DebugContext(ctx, "bind", "portal", portalName, "statement", stmtName, "param_count", len(params))

	// Get the prepared statement to verify it exists.
	psi := h.psc.GetPreparedStatementInfo(conn.ConnectionID(), stmtName)
	if psi == nil {
		return fmt.Errorf("prepared statement \"%s\" does not exist", stmtName)
	}

	// Get the connection state.
	state := h.getConnectionState(conn)

	// Create portal using protoutil helper.
	portal := protoutil.NewPortal(portalName, psi.Name, params, paramFormats, resultFormats)
	state.StorePortalInfo(portal, psi)

	return nil
}

// HandleExecute processes an Execute message ('E') for the extended query protocol.
// Executes the specified portal's query with bound parameters and streams results via callback.
func (h *MultiGatewayHandler) HandleExecute(ctx context.Context, conn *server.Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	h.logger.DebugContext(ctx, "execute", "portal", portalName, "max_rows", maxRows)

	// Get the connection state.
	state := h.getConnectionState(conn)

	// Get the portal.
	portalInfo := state.GetPortalInfo(portalName)
	if portalInfo == nil {
		return fmt.Errorf("portal \"%s\" does not exist", portalName)
	}

	return h.executor.PortalStreamExecute(ctx, conn, state, portalInfo, maxRows, callback)
}

// HandleDescribe processes a Describe message ('D').
// Describes either a prepared statement ('S') or a portal ('P').
func (h *MultiGatewayHandler) HandleDescribe(ctx context.Context, conn *server.Conn, typ byte, name string) (*query.StatementDescription, error) {
	h.logger.DebugContext(ctx, "describe", "type", string(typ), "name", name)

	// Get the connection state.
	state := h.getConnectionState(conn)

	switch typ {
	case 'S': // Describe prepared statement
		stmt := h.psc.GetPreparedStatementInfo(conn.ConnectionID(), name)
		if stmt == nil {
			return nil, fmt.Errorf("prepared statement \"%s\" does not exist", name)
		}

		// Call executor to get description from multipooler
		return h.executor.Describe(ctx, conn, state, nil, stmt)

	case 'P': // Describe portal
		portalInfo := state.GetPortalInfo(name)
		if portalInfo == nil {
			return nil, fmt.Errorf("portal \"%s\" does not exist", name)
		}

		// Call executor to get description from multipooler
		return h.executor.Describe(ctx, conn, state, portalInfo, nil)

	default:
		return nil, fmt.Errorf("invalid describe type: %c (expected 'S' or 'P')", typ)
	}
}

// HandleClose processes a Close message ('C').
// Closes either a prepared statement ('S') or a portal ('P').
func (h *MultiGatewayHandler) HandleClose(ctx context.Context, conn *server.Conn, typ byte, name string) error {
	h.logger.DebugContext(ctx, "close", "type", string(typ), "name", name)

	switch typ {
	case 'S': // Close prepared statement
		h.psc.RemovePreparedStatement(conn.ConnectionID(), name)
		return nil

	case 'P': // Close portal
		state := h.getConnectionState(conn)
		state.DeletePortalInfo(name)
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

// HandleStartup validates startup parameters by establishing a backend connection.
// If the client has startup parameters (e.g., DateStyle, TimeZone, PGOPTIONS values),
// they are applied to a backend connection via SET SESSION statements. If any parameter
// is invalid, an error is returned and the client connection is rejected.
func (h *MultiGatewayHandler) HandleStartup(ctx context.Context, conn *server.Conn) (map[string]string, error) {
	startupParams := conn.GetStartupParams()
	if len(startupParams) == 0 {
		return nil, nil
	}

	h.logger.DebugContext(ctx, "validating startup parameters", "params", startupParams)

	// Initialize connection state eagerly so startup params are available
	// for the validation query.
	state := NewMultiGatewayConnectionState()
	state.StartupParams = startupParams
	conn.SetConnectionState(state)

	// Validate by executing a simple query with the startup params applied.
	// The executor will merge startup params into session settings, which causes
	// the multipooler to apply them via SET SESSION statements on the backend.
	// If any parameter is invalid, PostgreSQL rejects the SET and returns an error.
	paramStatus, err := h.executor.ValidateStartupParams(ctx, conn, state)
	if err != nil {
		// Clear connection state on failure since the connection will be rejected.
		conn.SetConnectionState(nil)
		return nil, err
	}

	return paramStatus, nil
}

// Ensure MultiGatewayHandler implements server.Handler interface.
var _ server.Handler = (*MultiGatewayHandler)(nil)
