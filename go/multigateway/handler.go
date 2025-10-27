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

package multigateway

import (
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/server"
)

// MultiGatewayHandler implements the pgprotocol Handler interface for multigateway.
// It routes PostgreSQL protocol queries to the appropriate multipooler instances.
type MultiGatewayHandler struct {
	mg     *MultiGateway
	logger *slog.Logger
}

// NewMultiGatewayHandler creates a new PostgreSQL protocol handler.
func NewMultiGatewayHandler(mg *MultiGateway) *MultiGatewayHandler {
	return &MultiGatewayHandler{
		mg:     mg,
		logger: mg.senv.GetLogger().With("component", "multigateway_handler"),
	}
}

// HandleQuery processes a simple query protocol message ('Q').
// Routes the query to an appropriate multipooler instance and streams results back.
func (h *MultiGatewayHandler) HandleQuery(conn *server.Conn, queryStr string, callback func(result *query.QueryResult) error) error {
	h.logger.Debug("handling query", "query", queryStr, "user", conn.User(), "database", conn.Database())

	// Route the query through the executor which will eventually call multipooler
	return h.mg.executor.StreamExecute(conn, queryStr, callback)
}

// HandleParse processes a Parse message ('P') for the extended query protocol.
func (h *MultiGatewayHandler) HandleParse(conn *server.Conn, name, queryStr string, paramTypes []uint32) error {
	h.logger.Debug("parse not yet implemented", "name", name, "query", queryStr)
	return fmt.Errorf("extended query protocol not yet implemented")
}

// HandleBind processes a Bind message ('B') for the extended query protocol.
func (h *MultiGatewayHandler) HandleBind(conn *server.Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	h.logger.Debug("bind not yet implemented", "portal", portalName, "statement", stmtName)
	return fmt.Errorf("extended query protocol not yet implemented")
}

// HandleExecute processes an Execute message ('E') for the extended query protocol.
func (h *MultiGatewayHandler) HandleExecute(conn *server.Conn, portalName string, maxRows int32) (*query.QueryResult, error) {
	h.logger.Debug("execute not yet implemented", "portal", portalName, "max_rows", maxRows)
	return nil, fmt.Errorf("extended query protocol not yet implemented")
}

// HandleDescribe processes a Describe message ('D').
func (h *MultiGatewayHandler) HandleDescribe(conn *server.Conn, typ byte, name string) (*query.StatementDescription, error) {
	h.logger.Debug("describe not yet implemented", "type", string(typ), "name", name)
	return nil, fmt.Errorf("extended query protocol not yet implemented")
}

// HandleClose processes a Close message ('C').
func (h *MultiGatewayHandler) HandleClose(conn *server.Conn, typ byte, name string) error {
	h.logger.Debug("close not yet implemented", "type", string(typ), "name", name)
	return fmt.Errorf("extended query protocol not yet implemented")
}

// HandleSync processes a Sync message ('S').
func (h *MultiGatewayHandler) HandleSync(conn *server.Conn) error {
	h.logger.Debug("sync not yet implemented")
	return fmt.Errorf("extended query protocol not yet implemented")
}

// Ensure MultiGatewayHandler implements server.Handler interface.
var _ server.Handler = (*MultiGatewayHandler)(nil)
