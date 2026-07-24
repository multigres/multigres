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

package preparedstatement

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/protoutil"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// Consolidator is used to consolidate prepared statements that
// are preparing the same statement but with different names. The intent is to be able to
// use the same connection for both of them to execute this because underlying, they are using
// the same prepared statement.
type Consolidator struct {
	// Mutex to protect the fields
	mu sync.Mutex

	// Map from (query, paramTypes) dedup key to canonical prepared statement
	stmts map[string]*PreparedStatementInfo
	// Map from connection ID and user-visible statement name to logical state.
	incoming map[uint32]map[string]*LogicalPreparedStatement
	// Reference count: number of connections using each prepared statement
	usageCount map[*PreparedStatementInfo]int

	// lastUsedID is the last id of the statement name that we used.
	lastUsedID int
	nextOrder  uint64
}

// ConsolidatorStats contains statistics about the prepared statement consolidator.
type ConsolidatorStats struct {
	// UniqueStatements is the number of unique prepared statements being tracked.
	UniqueStatements int `json:"unique_statements"`
	// TotalReferences is the total number of references across all connections.
	TotalReferences int `json:"total_references"`
	// ConnectionCount is the number of connections that have prepared statements.
	ConnectionCount int `json:"connection_count"`
	// Statements contains details about each unique prepared statement.
	Statements []StatementStats `json:"statements"`
}

// StatementStats contains statistics for a single prepared statement.
type StatementStats struct {
	// Name is the canonical name of the prepared statement.
	Name string `json:"name"`
	// Query is the SQL query of the prepared statement.
	Query string `json:"query"`
	// UsageCount is the number of connections using this prepared statement.
	UsageCount int `json:"usage_count"`
}

type PortalInfo struct {
	*querypb.Portal
	*PreparedStatementInfo
}

type PreparedStatementInfo struct {
	*querypb.PreparedStatement
	astStruct ast.Stmt
}

// LogicalPreparedStatement maps a client-visible name to the shared physical
// statement definition. It exists for backend alias reconciliation only; SQL
// catalog reads continue to observe the selected PostgreSQL backend.
type LogicalPreparedStatement struct {
	Name          string
	Prepared      *PreparedStatementInfo
	sqlAlias      bool
	creationOrder uint64
}

// AstStmt returns the parsed AST statement for this prepared statement.
func (psi *PreparedStatementInfo) AstStmt() ast.Stmt {
	return psi.astStruct
}

// IsEmpty reports whether this prepared statement was created from an empty or
// comment-only query string (zero parsed statements). An empty statement has a
// nil AST; Execute answers it with EmptyQueryResponse, matching PostgreSQL.
func (psi *PreparedStatementInfo) IsEmpty() bool {
	return psi.astStruct == nil
}

// NewPreparedStatementInfo parses the query in the prepared statement and stores it along with the
// prepared statement information for future use.
func NewPreparedStatementInfo(ps *querypb.PreparedStatement) (*PreparedStatementInfo, error) {
	asts, err := parser.ParseSQL(ps.Query)
	if err != nil {
		// ParseSQL only does syntactic parsing, so any error here is a parse-stage
		// error. Surface it as the diagnostic PostgreSQL would send (the parser
		// stays mterrors-free): 42601 unless the parser named a SQLSTATE of its
		// own, carrying the cursor position for the ErrorResponse "P" field.
		var se *parser.ParseSyntaxError
		if errors.As(err, &se) {
			diag := mterrors.NewParseErrorAt(se.Message, se.CursorPosition, se.SQLState)
			diag.Hint = se.Hint
			return nil, diag
		}
		return nil, mterrors.NewParseError(err.Error())
	}
	switch {
	case len(asts) == 0:
		// Empty or comment-only query: PostgreSQL accepts this and answers a
		// subsequent Execute with EmptyQueryResponse. Represent it as a prepared
		// statement with a nil AST (the empty-statement sentinel). The gateway
		// short-circuits empty statements before they reach the planner.
		return &PreparedStatementInfo{
			PreparedStatement: ps,
			astStruct:         nil,
		}, nil
	case len(asts) > 1:
		// PostgreSQL: a Parse message "cannot insert multiple commands into a
		// prepared statement" (SQLSTATE 42601).
		return nil, mterrors.NewParseError("cannot insert multiple commands into a prepared statement")
	}
	return &PreparedStatementInfo{
		PreparedStatement: ps,
		astStruct:         asts[0],
	}, nil
}

// NewPortalInfo creates the PortalInfo.
func NewPortalInfo(psi *PreparedStatementInfo, portal *querypb.Portal) *PortalInfo {
	return &PortalInfo{
		Portal:                portal,
		PreparedStatementInfo: psi,
	}
}

// NewConsolidator gets a new prepared statement consolidator
// used to consolidate and reuse the same prepared statements.
func NewConsolidator() *Consolidator {
	return &Consolidator{
		stmts:      make(map[string]*PreparedStatementInfo),
		incoming:   make(map[uint32]map[string]*LogicalPreparedStatement),
		usageCount: make(map[*PreparedStatementInfo]int),
		lastUsedID: 0,
	}
}

// AddPreparedStatement adds a prepared statement to the consolidator.
// Returns the PreparedStatementInfo (either existing or newly created) and any error.
func (psc *Consolidator) AddPreparedStatement(connId uint32, name, queryStr string, paramTypes []uint32) (*PreparedStatementInfo, error) {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	// Initialize the map for this connection if it doesn't exist
	if psc.incoming[connId] == nil {
		psc.incoming[connId] = make(map[string]*LogicalPreparedStatement)
	}

	// If the name is non-empty and a prepared statement for this name already exists
	// on the connection, replace it. This matches PostgreSQL behavior where re-parsing
	// with an existing name replaces the old statement. This is necessary to handle
	// the case where Parse succeeds (adding to consolidator) but the subsequent
	// Describe fails — the client retries Parse with the same name.
	if existing, exists := psc.incoming[connId][name]; exists && name != "" {
		slog.Debug("replacing existing prepared statement",
			"connId", connId,
			"name", name,
			"oldQuery", existing.Prepared.Query,
			"newQuery", queryStr,
		)
		psc.usageCount[existing.Prepared]--
		if psc.usageCount[existing.Prepared] == 0 {
			delete(psc.stmts, dedupKey(existing.Prepared.Query, existing.Prepared.ParamTypes))
			delete(psc.usageCount, existing.Prepared)
		}
		delete(psc.incoming[connId], name)
	}

	// Let's check if a prepared statement with this (query, paramTypes) already exists.
	key := dedupKey(queryStr, paramTypes)
	existingPs, foundExisting := psc.stmts[key]
	if foundExisting {
		// We found an existing prepared statement, we should be using that.
		psc.usageCount[existingPs] += 1
		psc.storeLogical(connId, name, existingPs)
		return existingPs, nil
	}

	// We didn't find any existing prepared statement with this (query, paramTypes).
	// Create a new one in our stmts list tracking unique prepared statements.
	newName := fmt.Sprintf("stmt%d", psc.lastUsedID)
	psc.lastUsedID += 1
	newPS, err := NewPreparedStatementInfo(protoutil.NewPreparedStatement(newName, queryStr, paramTypes))
	if err != nil {
		return nil, err
	}

	psc.stmts[key] = newPS
	psc.usageCount[newPS] += 1
	psc.storeLogical(connId, name, newPS)
	return newPS, nil
}

func (psc *Consolidator) storeLogical(connID uint32, name string, prepared *PreparedStatementInfo) {
	psc.incoming[connID][name] = &LogicalPreparedStatement{
		Name:          name,
		Prepared:      prepared,
		creationOrder: psc.nextOrder,
	}
	psc.nextOrder++
}

// MarkSQLAlias marks a successfully validated SQL PREPARE name for backend
// reconciliation. Extended-protocol statement names never need SQL EXECUTE.
func (psc *Consolidator) MarkSQLAlias(connID uint32, name string) {
	psc.mu.Lock()
	defer psc.mu.Unlock()
	if logical := psc.incoming[connID][name]; logical != nil {
		logical.sqlAlias = true
	}
}

// LogicalPreparedStatements returns a stable snapshot of SQL PREPARE aliases
// that must be reconciled on the selected backend.
func (psc *Consolidator) LogicalPreparedStatements(connID uint32) []*LogicalPreparedStatement {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	logical := make([]*LogicalPreparedStatement, 0, len(psc.incoming[connID]))
	for name, stmt := range psc.incoming[connID] {
		if name == "" || !stmt.sqlAlias {
			continue
		}
		copy := *stmt
		logical = append(logical, &copy)
	}
	sort.Slice(logical, func(i, j int) bool { return logical[i].creationOrder < logical[j].creationOrder })
	return logical
}

// GetPreparedStatementInfo gets the information for a previously added prepared statement to the consolidator.
func (psc *Consolidator) GetPreparedStatementInfo(connId uint32, name string) *PreparedStatementInfo {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	logical := psc.incoming[connId][name]
	if logical == nil {
		return nil
	}
	return logical.Prepared
}

// RemovePreparedStatement removes prepared statement.
func (psc *Consolidator) RemovePreparedStatement(connId uint32, name string) {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	logical, exists := psc.incoming[connId][name]
	if exists {
		psi := logical.Prepared
		psc.usageCount[psi] -= 1
		if psc.usageCount[psi] == 0 {
			delete(psc.stmts, dedupKey(psi.Query, psi.ParamTypes))
			delete(psc.usageCount, psi)
		}
		delete(psc.incoming[connId], name)
	}
}

// RemoveConnection removes all prepared statements associated with a connection.
// This should be called when a client connection is closed.
func (psc *Consolidator) RemoveConnection(connId uint32) {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	connStmts, exists := psc.incoming[connId]
	if !exists {
		return
	}

	for _, logical := range connStmts {
		psi := logical.Prepared
		psc.usageCount[psi]--
		if psc.usageCount[psi] == 0 {
			delete(psc.stmts, dedupKey(psi.Query, psi.ParamTypes))
			delete(psc.usageCount, psi)
		}
	}
	delete(psc.incoming, connId)
}

// Stats returns statistics about the consolidator's current state.
func (psc *Consolidator) Stats() ConsolidatorStats {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	stats := ConsolidatorStats{
		UniqueStatements: len(psc.stmts),
		TotalReferences:  0,
		ConnectionCount:  len(psc.incoming),
		Statements:       make([]StatementStats, 0, len(psc.stmts)),
	}

	for _, psi := range psc.stmts {
		usageCount := psc.usageCount[psi]
		stats.TotalReferences += usageCount
		stats.Statements = append(stats.Statements, StatementStats{
			Name:       psi.Name,
			Query:      psi.Query,
			UsageCount: usageCount,
		})
	}

	return stats
}
