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
	"sync"

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

	// Map from statement body to canonical prepared statement
	stmts map[string]*PreparedStatementInfo
	// Map from connection ID and statement name to prepared statement reference
	incoming map[uint32]map[string]*PreparedStatementInfo
	// Reference count: number of connections using each prepared statement
	usageCount map[*PreparedStatementInfo]int

	// lastUsedID is the last id of the statement name that we used.
	lastUsedID int
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

// NewPreparedStatementInfo parses the query in the prepared statement and stores it along with the
// prepared statement information for future use.
func NewPreparedStatementInfo(ps *querypb.PreparedStatement) (*PreparedStatementInfo, error) {
	asts, err := parser.ParseSQL(ps.Query)
	if err != nil {
		return nil, err
	}
	if len(asts) != 1 {
		return nil, errors.New("more than 1 query in prepare statement")
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
		incoming:   make(map[uint32]map[string]*PreparedStatementInfo),
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
		psc.incoming[connId] = make(map[string]*PreparedStatementInfo)
	}

	// If the name is non-empty, and a prepared statement for this name already exists on the connection, we throw an error.
	if _, exists := psc.incoming[connId][name]; exists && name != "" {
		return nil, errors.New("Prepared statement with this name exists")
	}

	// Let's check if a prepared statement with this statement already exists.
	existingPs, foundExisting := psc.stmts[queryStr]
	if foundExisting {
		// We found an existing prepared statement, we should be using that.
		psc.usageCount[existingPs] += 1
		psc.incoming[connId][name] = existingPs
		return existingPs, nil
	}

	// We didn't find any existing prepared statement with this sql.
	// Create a new one in our stmts list tracking unique prepared statements.
	newName := fmt.Sprintf("stmt%d", psc.lastUsedID)
	psc.lastUsedID += 1
	newPS, err := NewPreparedStatementInfo(protoutil.NewPreparedStatement(newName, queryStr, paramTypes))
	if err != nil {
		return nil, err
	}

	psc.stmts[queryStr] = newPS
	psc.usageCount[newPS] += 1
	psc.incoming[connId][name] = newPS
	return newPS, nil
}

// GetPreparedStatementInfo gets the information for a previously added prepared statement to the consolidator.
func (psc *Consolidator) GetPreparedStatementInfo(connId uint32, name string) *PreparedStatementInfo {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	return psc.incoming[connId][name]
}

// RemovePreparedStatement removes prepared statement.
func (psc *Consolidator) RemovePreparedStatement(connId uint32, name string) {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	psi, exists := psc.incoming[connId][name]
	if exists {
		psc.usageCount[psi] -= 1
		if psc.usageCount[psi] == 0 {
			delete(psc.stmts, psi.Query)
			delete(psc.usageCount, psi)
		}
		delete(psc.incoming[connId], name)
	}
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
