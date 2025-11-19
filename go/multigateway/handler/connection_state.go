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
	"sync"

	"github.com/multigres/multigres/go/parser/ast"
)

// PreparedStatement represents a prepared statement in the extended query protocol.
// Prepared statements are created by the Parse message and can be executed
// multiple times with different parameter values.
type PreparedStatement struct {
	// Name is the client-provided name for the prepared statement.
	// An empty name indicates the unnamed statement.
	Name string

	// Query is the SQL query string.
	Query ast.Stmt

	// ParamTypes contains the OIDs of the parameter types.
	// This is sent by the client in the Parse message.
	ParamTypes []uint32
}

// NewPreparedStatement creates a new PreparedStatement.
func NewPreparedStatement(name string, query ast.Stmt, paramTypes []uint32) *PreparedStatement {
	return &PreparedStatement{
		Name:       name,
		Query:      query,
		ParamTypes: paramTypes,
	}
}

// Portal represents a bound prepared statement with parameters.
// Portals are created by the Bind message and can be executed via Execute.
type Portal struct {
	// Name is the client-provided name for the portal.
	// An empty name indicates the unnamed portal.
	Name string

	// Statement is the prepared statement this portal is bound to.
	Statement *PreparedStatement

	// Params contains the parameter values sent by the client in the Bind message.
	// Each parameter is a byte slice, with nil indicating NULL.
	Params [][]byte

	// ParamFormats specifies the format code for each parameter.
	// 0 = text, 1 = binary.
	// If empty, all parameters are text format.
	// If a single element, that format applies to all parameters.
	ParamFormats []int16

	// ResultFormats specifies the format code for each result column.
	// 0 = text, 1 = binary.
	// If empty, all results are text format.
	// If a single element, that format applies to all result columns.
	ResultFormats []int16
}

// NewPortal creates a new Portal.
func NewPortal(name string, statement *PreparedStatement, params [][]byte, paramFormats, resultFormats []int16) *Portal {
	return &Portal{
		Name:          name,
		Statement:     statement,
		Params:        params,
		ParamFormats:  paramFormats,
		ResultFormats: resultFormats,
	}
}

// ConnectionState holds the state for the extended query protocol.
// This includes prepared statements and portals.
// All methods are thread-safe.
type ConnectionState struct {
	// mu protects all fields in this struct.
	mu sync.Mutex

	// preparedStatements stores prepared statements by name.
	// The unnamed statement uses the empty string "" as the key.
	preparedStatements map[string]*PreparedStatement

	// portals stores portals (bound prepared statements) by name.
	// The unnamed portal uses the empty string "" as the key.
	portals map[string]*Portal
}

// NewConnectionState creates a new ConnectionState with initialized maps.
func NewConnectionState() *ConnectionState {
	return &ConnectionState{
		preparedStatements: make(map[string]*PreparedStatement),
		portals:            make(map[string]*Portal),
	}
}

// Close cleans up the connection state.
func (cs *ConnectionState) Close() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.preparedStatements = nil
	cs.portals = nil
}

// StorePreparedStatement stores a prepared statement.
// If a statement with the same name already exists, it is replaced.
func (cs *ConnectionState) StorePreparedStatement(stmt *PreparedStatement) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.preparedStatements[stmt.Name] = stmt
}

// GetPreparedStatement retrieves a prepared statement by name.
// Returns nil if the statement does not exist.
func (cs *ConnectionState) GetPreparedStatement(name string) *PreparedStatement {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	return cs.preparedStatements[name]
}

// StorePortal stores a portal.
// If a portal with the same name already exists, it is replaced.
func (cs *ConnectionState) StorePortal(portal *Portal) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.portals[portal.Name] = portal
}

// GetPortal retrieves a portal by name.
// Returns nil if the portal does not exist.
func (cs *ConnectionState) GetPortal(name string) *Portal {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	return cs.portals[name]
}

// DeletePreparedStatement removes a prepared statement by name.
// Does nothing if the statement doesn't exist (PostgreSQL-compliant behavior).
func (cs *ConnectionState) DeletePreparedStatement(name string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.preparedStatements, name)
}

// DeletePortal removes a portal by name.
// Does nothing if the portal doesn't exist (PostgreSQL-compliant behavior).
func (cs *ConnectionState) DeletePortal(name string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.portals, name)
}
