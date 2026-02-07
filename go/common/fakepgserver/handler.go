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

package fakepgserver

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// fakeHandler implements server.Handler to return pre-configured query results.
type fakeHandler struct {
	server *Server

	// preparedStatements stores prepared statements by name.
	// Each connection would normally have its own, but for simplicity
	// we use a shared map (tests typically use a single connection).
	preparedStatements map[string]*preparedStmt

	// portals stores bound portals by name.
	portals map[string]*portal
}

type preparedStmt struct {
	name       string
	query      string
	paramTypes []uint32
}

type portal struct {
	name       string
	stmt       *preparedStmt
	params     [][]byte
	resultFmts []int16
}

// HandleQuery handles a simple query protocol message.
func (h *fakeHandler) HandleQuery(ctx context.Context, conn *server.Conn, queryStr string, callback func(context.Context, *sqltypes.Result) error) error {
	result, err := h.server.handleQuery(queryStr)
	if err != nil {
		return err
	}
	return callback(ctx, result)
}

// HandleParse handles a Parse message for the extended query protocol.
func (h *fakeHandler) HandleParse(ctx context.Context, conn *server.Conn, name, queryStr string, paramTypes []uint32) error {
	if h.preparedStatements == nil {
		h.preparedStatements = make(map[string]*preparedStmt)
	}

	h.preparedStatements[name] = &preparedStmt{
		name:       name,
		query:      queryStr,
		paramTypes: paramTypes,
	}

	return nil
}

// HandleBind handles a Bind message for the extended query protocol.
func (h *fakeHandler) HandleBind(ctx context.Context, conn *server.Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	if h.portals == nil {
		h.portals = make(map[string]*portal)
	}

	stmt, ok := h.preparedStatements[stmtName]
	if !ok {
		return fmt.Errorf("prepared statement %q not found", stmtName)
	}

	h.portals[portalName] = &portal{
		name:       portalName,
		stmt:       stmt,
		params:     params,
		resultFmts: resultFormats,
	}

	return nil
}

// HandleExecute handles an Execute message for the extended query protocol.
func (h *fakeHandler) HandleExecute(ctx context.Context, conn *server.Conn, portalName string, maxRows int32, callback func(context.Context, *sqltypes.Result) error) error {
	p, ok := h.portals[portalName]
	if !ok {
		return fmt.Errorf("portal %q not found", portalName)
	}

	// Execute the query.
	result, err := h.server.handleQuery(p.stmt.query)
	if err != nil {
		return err
	}
	return callback(ctx, result)
}

// HandleDescribe handles a Describe message.
func (h *fakeHandler) HandleDescribe(ctx context.Context, conn *server.Conn, typ byte, name string) (*query.StatementDescription, error) {
	switch typ {
	case 'S': // Statement
		stmt, ok := h.preparedStatements[name]
		if !ok {
			return nil, fmt.Errorf("prepared statement %q not found", name)
		}

		// For the fake server, we return minimal description.
		// In a real server, this would describe the statement's parameters and result columns.
		desc := &query.StatementDescription{
			Parameters: make([]*query.ParameterDescription, len(stmt.paramTypes)),
		}
		for i, oid := range stmt.paramTypes {
			desc.Parameters[i] = &query.ParameterDescription{DataTypeOid: oid}
		}
		return desc, nil

	case 'P': // Portal
		p, ok := h.portals[name]
		if !ok {
			return nil, fmt.Errorf("portal %q not found", name)
		}

		// Get the expected result to determine fields.
		result, err := h.server.handleQuery(p.stmt.query)
		if err != nil {
			return nil, err
		}

		// Use fields directly from the result.
		return &query.StatementDescription{
			Fields: result.Fields,
		}, nil

	default:
		return nil, fmt.Errorf("unknown describe type: %c", typ)
	}
}

// HandleClose handles a Close message.
func (h *fakeHandler) HandleClose(ctx context.Context, conn *server.Conn, typ byte, name string) error {
	switch typ {
	case 'S': // Statement
		delete(h.preparedStatements, name)
	case 'P': // Portal
		delete(h.portals, name)
	}
	return nil
}

// HandleSync handles a Sync message.
func (h *fakeHandler) HandleSync(ctx context.Context, conn *server.Conn) error {
	// Clear unnamed portal after sync (per PostgreSQL protocol).
	delete(h.portals, "")
	return nil
}

// HandleStartup handles connection startup (no-op for fake server).
func (h *fakeHandler) HandleStartup(ctx context.Context, conn *server.Conn) (map[string]string, error) {
	return nil, nil
}

// Ensure fakeHandler implements server.Handler.
var _ server.Handler = (*fakeHandler)(nil)
